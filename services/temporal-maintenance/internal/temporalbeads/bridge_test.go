package temporalbeads

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

func TestBridgeDuplicateDeliveryUsesOneWorkflowAndLogicalEvent(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	bridge := ReadyEventBridge{Temporal: gateway, Acker: store}

	first, err := bridge.Deliver(context.Background(), event)
	require.NoError(t, err)
	second, err := bridge.Deliver(context.Background(), event)
	require.NoError(t, err)

	require.Equal(t, first.WorkflowID, second.WorkflowID)
	require.Equal(t, 1, gateway.StartCount())
	require.Equal(t, 1, gateway.SignalExistingCount())
	require.Equal(t, 1, gateway.LogicalEventCount())
	record, err := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, err)
	require.Empty(t, record.ClaimToken)
	require.Empty(t, record.Outcome)
	require.Equal(t, event.Formula, gateway.LastRequest().Input.InitialReady[0].Formula)
}

func TestBridgeCrashAfterTemporalAcknowledgementIsSafe(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	acker := &failFirstAcker{delegate: store}
	bridge := ReadyEventBridge{Temporal: gateway, Acker: acker}

	_, err := bridge.Deliver(context.Background(), event)
	require.ErrorContains(t, err, "checkpoint injected failure")
	pending, err := store.PendingReadyEvents(context.Background())
	require.NoError(t, err)
	require.Len(t, pending, 1)

	_, err = bridge.Deliver(context.Background(), event)
	require.NoError(t, err)
	pending, err = store.PendingReadyEvents(context.Background())
	require.NoError(t, err)
	require.Empty(t, pending)
	require.Equal(t, 1, gateway.LogicalEventCount())
	require.Equal(t, 1, gateway.StartCount())
}

func TestBridgeTemporalUnavailablePreservesBeadsAndOutbox(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	gateway.err = errors.New("temporal unavailable")
	bridge := ReadyEventBridge{Temporal: gateway, Acker: store}

	_, err := bridge.Deliver(context.Background(), event)
	require.ErrorContains(t, err, "temporal unavailable")

	record, inspectErr := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, inspectErr)
	require.Equal(t, BeadStatusReady, record.Status)
	require.Empty(t, record.ClaimToken)
	pending, pendingErr := store.PendingReadyEvents(context.Background())
	require.NoError(t, pendingErr)
	require.Equal(t, []ReadyEvent{event}, pending)
}

func TestBridgeUsesConfiguredHeartbeatTimeout(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	bridge := ReadyEventBridge{
		Temporal: gateway,
		Acker:    store,
		Timing: TimingConfig{
			HeartbeatTimeout:  2 * time.Minute,
			ReconcileInterval: time.Minute,
			Clock: NewManualClock(
				time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC),
			),
		},
	}

	_, err := bridge.Deliver(context.Background(), event)
	require.NoError(t, err)
	require.Equal(t, 2*time.Minute, gateway.LastRequest().Input.HeartbeatTimeout)
}

func TestBridgeSealsRunWithAuthoritativeEventSet(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	bridge := ReadyEventBridge{Temporal: gateway, Acker: store}
	_, err := bridge.Deliver(context.Background(), event)
	require.NoError(t, err)
	request := CloseRequest{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		ExpectedEventIDs: []string{event.EventID},
		ReasonCode:       "run-sealed",
	}

	require.NoError(t, bridge.SealRun(context.Background(), request))
	require.Equal(t, []CloseRequest{request}, gateway.CloseRequests())
}

func TestWorkflowIDIsStableAndRejectsUnsafeSegments(t *testing.T) {
	first, err := WorkflowID("city", "run", "gc-1")
	require.NoError(t, err)
	second, err := WorkflowID("city", "run", "gc-1")
	require.NoError(t, err)
	require.Equal(t, "bead-orchestration/city/run/gc-1", first)
	require.Equal(t, first, second)

	_, err = WorkflowID("../city", "run", "gc-1")
	require.Error(t, err)
}

func TestTemporalGatewayRetainsParentLinkAndStartMemo(t *testing.T) {
	event := validReadyEvent(t)
	event.Formula.ParentWorkflowID = "gascity-maintenance/gascity/dr-gst6"
	event.Formula.ParentRunID = "parent-run-1"
	transport := &recordingTemporalClient{}
	gateway := TemporalClientGateway{
		Client:                        transport,
		EnableFormulaSearchAttributes: true,
	}

	receipt, err := gateway.SignalWithStart(
		context.Background(),
		StartOrSignalRequest{
			WorkflowID: "bead-orchestration/ds-research/goal-gc-636326",
			Event:      event,
			Input: WorkflowInput{
				ContractVersion:  CurrentContractVersion,
				CityID:           event.CityID,
				RunID:            event.RunID,
				BeadID:           event.BeadID,
				InitialReady:     []ReadyEvent{event},
				HeartbeatTimeout: time.Minute,
			},
		},
	)

	require.NoError(t, err)
	require.Equal(t, FormulaMemo(event), transport.startOptions.Memo)
	require.Equal(t, 8, transport.startOptions.TypedSearchAttributes.Size())
	require.True(t, transport.startInput.SearchAttributes)
	require.Equal(t, event.Formula.ParentWorkflowID, transport.signalWorkflowID)
	require.Equal(t, event.Formula.ParentRunID, transport.signalRunID)
	require.Equal(t, SignalParentChildLink, transport.signalName)
	link, ok := transport.signalArg.(ChildWorkflowLink)
	require.True(t, ok)
	require.Equal(t, receipt.WorkflowID, link.ChildWorkflowID)
	require.Equal(t, receipt.RunID, link.ChildRunID)
	require.Equal(t, event.EventID, link.EventID)
}

func onlyPendingEvent(t *testing.T, store ReadyEventSource) ReadyEvent {
	t.Helper()
	events, err := store.PendingReadyEvents(context.Background())
	require.NoError(t, err)
	require.Len(t, events, 1)
	return events[0]
}

type failFirstAcker struct {
	mu       sync.Mutex
	delegate ReadyEventSource
	failed   bool
}

type recordingTemporalClient struct {
	client.Client
	startOptions     client.StartWorkflowOptions
	startInput       WorkflowInput
	signalWorkflowID string
	signalRunID      string
	signalName       string
	signalArg        interface{}
	describeResponse *workflowservice.DescribeWorkflowExecutionResponse
	describeErr      error
	describeCalls    int
	signalStartCalls int
}

func (c *recordingTemporalClient) SignalWithStartWorkflow(
	_ context.Context,
	workflowID string,
	_ string,
	_ interface{},
	options client.StartWorkflowOptions,
	_ interface{},
	workflowArgs ...interface{},
) (client.WorkflowRun, error) {
	c.signalStartCalls++
	c.startOptions = options
	if len(workflowArgs) == 1 {
		c.startInput, _ = workflowArgs[0].(WorkflowInput)
	}
	return fakeWorkflowRun{id: workflowID, runID: "child-run-1"}, nil
}

func (c *recordingTemporalClient) DescribeWorkflowExecution(
	context.Context,
	string,
	string,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.describeCalls++
	return c.describeResponse, c.describeErr
}

func (c *recordingTemporalClient) SignalWorkflow(
	_ context.Context,
	workflowID string,
	runID string,
	signalName string,
	arg interface{},
) error {
	c.signalWorkflowID = workflowID
	c.signalRunID = runID
	c.signalName = signalName
	c.signalArg = arg
	return nil
}

type fakeWorkflowRun struct {
	id    string
	runID string
}

func (r fakeWorkflowRun) GetID() string {
	return r.id
}

func (r fakeWorkflowRun) GetRunID() string {
	return r.runID
}

func (fakeWorkflowRun) Get(context.Context, interface{}) error {
	return nil
}

func (fakeWorkflowRun) GetWithOptions(
	context.Context,
	interface{},
	client.WorkflowRunGetOptions,
) error {
	return nil
}

func (a *failFirstAcker) AcknowledgeReadyEvent(ctx context.Context, eventID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.failed {
		a.failed = true
		return errors.New("checkpoint injected failure")
	}
	return a.delegate.AcknowledgeReadyEvent(ctx, eventID)
}

type fakeWorkflowGateway struct {
	mu               sync.Mutex
	err              error
	receiptErr       error
	postAcceptErr    error
	postAcceptFailed bool
	workflows        map[string]map[string]struct{}
	requests         []StartOrSignalRequest
	closes           []CloseRequest
	starts           int
	signals          int
}

func newFakeWorkflowGateway() *fakeWorkflowGateway {
	return &fakeWorkflowGateway{workflows: make(map[string]map[string]struct{})}
}

func (g *fakeWorkflowGateway) SignalWithStart(
	_ context.Context,
	request StartOrSignalRequest,
) (WorkflowReceipt, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return WorkflowReceipt{}, g.err
	}
	g.requests = append(g.requests, request)
	events, exists := g.workflows[request.WorkflowID]
	if !exists {
		events = make(map[string]struct{})
		g.workflows[request.WorkflowID] = events
		g.starts++
	} else {
		g.signals++
	}
	events[request.Event.EventID] = struct{}{}
	if g.postAcceptErr != nil && !g.postAcceptFailed {
		g.postAcceptFailed = true
		return WorkflowReceipt{}, g.postAcceptErr
	}
	return WorkflowReceipt{
		WorkflowID: request.WorkflowID,
		EventID:    request.Event.EventID,
	}, nil
}

func (g *fakeWorkflowGateway) HasEvent(
	_ context.Context,
	workflowID string,
	eventID string,
) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.receiptErr != nil {
		return false, g.receiptErr
	}
	events, exists := g.workflows[workflowID]
	if !exists {
		return false, nil
	}
	_, exists = events[eventID]
	return exists, nil
}

func (g *fakeWorkflowGateway) SignalClose(
	_ context.Context,
	workflowID string,
	request CloseRequest,
) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return g.err
	}
	if _, exists := g.workflows[workflowID]; !exists {
		return errors.New("workflow not found")
	}
	g.closes = append(g.closes, request)
	return nil
}

func (g *fakeWorkflowGateway) Seed(workflowID, eventID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	events, exists := g.workflows[workflowID]
	if !exists {
		events = make(map[string]struct{})
		g.workflows[workflowID] = events
	}
	events[eventID] = struct{}{}
}

func (g *fakeWorkflowGateway) WorkflowIDs() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	ids := make([]string, 0, len(g.workflows))
	for workflowID := range g.workflows {
		ids = append(ids, workflowID)
	}
	sort.Strings(ids)
	return ids
}

func (g *fakeWorkflowGateway) StartCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.starts
}

func (g *fakeWorkflowGateway) SignalExistingCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.signals
}

func (g *fakeWorkflowGateway) LogicalEventCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	total := 0
	for _, events := range g.workflows {
		total += len(events)
	}
	return total
}

func (g *fakeWorkflowGateway) LastRequest() StartOrSignalRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.requests[len(g.requests)-1]
}

func (g *fakeWorkflowGateway) CloseRequests() []CloseRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]CloseRequest(nil), g.closes...)
}
