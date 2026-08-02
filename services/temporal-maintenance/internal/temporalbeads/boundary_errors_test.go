package temporalbeads

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestContractValidationRejectsMalformedBoundaryValues(t *testing.T) {
	valid := validReadyEvent(t)
	tests := map[string]func() error{
		"event version": func() error {
			event := valid
			event.ContractVersion++
			return event.Validate()
		},
		"event id": func() error {
			event := valid
			event.EventID = "wrong"
			return event.Validate()
		},
		"event generation": func() error {
			event := valid
			event.Generation = 0
			return event.Validate()
		},
		"event ready time": func() error {
			event := valid
			event.ReadyAt = time.Time{}
			return event.Validate()
		},
		"workflow input version": func() error {
			return WorkflowInput{}.validatePayload()
		},
		"workflow input heartbeat": func() error {
			return WorkflowInput{
				ContractVersion: CurrentContractVersion,
				CityID:          "city", RunID: "run",
			}.validatePayload()
		},
		"workflow state phase": func() error {
			return WorkflowState{
				ContractVersion: CurrentContractVersion,
				CityID:          "city", RunID: "run", Phase: "unknown",
			}.validatePayload()
		},
		"heartbeat sequence": func() error {
			return HeartbeatCheckpoint{}.validatePayload()
		},
		"activity result outcome": func() error {
			return ActivityResult{
				EventID: "event-1", SessionID: "session-1", Outcome: "unknown",
			}.validatePayload()
		},
	}
	for name, check := range tests {
		t.Run(name, func(t *testing.T) {
			require.Error(t, check())
		})
	}

	_, err := EncodeWorkflowPayload(nil)
	require.Error(t, err)
}

func TestTimingConfigRejectsMissingValuesAndDefaultClockRuns(t *testing.T) {
	require.Error(t, (TimingConfig{}).Validate())
	require.Error(t, (TimingConfig{
		HeartbeatTimeout: time.Second,
		Clock:            NewManualClock(time.Unix(0, 0)),
	}).Validate())
	require.False(t, ReadyEventReconciler{}.Due(time.Now().Add(time.Hour)))
	require.False(t, realClock{}.Now().IsZero())
	require.False(t, NewSystemClock().Now().IsZero())
}

func TestFormulaTopologyRejectsInvalidParentAndActivityIdentity(t *testing.T) {
	formula := validFormulaRef()
	formula.ParentRunID = "run-without-parent"
	require.ErrorContains(t, formula.Validate(), "requires a parent workflow")

	formula = validFormulaRef()
	formula.ParentWorkflowID = "bad\nworkflow"
	require.ErrorContains(t, formula.Validate(), "workflow identifier")

	_, err := FormulaActivityID(validFormulaRef(), 0)
	require.ErrorContains(t, err, "generation")
	broken := validFormulaRef()
	broken.Hash = "not-a-hash"
	_, err = FormulaActivityID(broken, 1)
	require.ErrorContains(t, err, "formula hash")

	event := validReadyEvent(t)
	_, err = NewChildWorkflowLink(
		event,
		WorkflowReceipt{
			WorkflowID: "child/workflow",
			RunID:      "",
			EventID:    event.EventID,
		},
		ChildWorkflowStarted,
		"",
	)
	require.ErrorContains(t, err, "child run id")
}

func TestFileBeadStoreRejectsInvalidAndStaleOperations(t *testing.T) {
	_, err := OpenFileBeadStore("", NewManualClock(time.Unix(0, 0)))
	require.Error(t, err)
	_, err = OpenFileBeadStore("beads.json", nil)
	require.Error(t, err)

	path := filepath.Join(t.TempDir(), "beads.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":99}`), 0o600))
	_, err = OpenFileBeadStore(path, NewManualClock(time.Unix(0, 0)))
	require.ErrorContains(t, err, "unsupported")

	store := newReadyStore(t)
	event, err := store.TransitionReady(
		context.Background(), "city", "run", "gc-1", 1, validFormulaRef(),
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), event.Generation)
	_, err = store.TransitionReady(
		context.Background(), "city", "run", "gc-1", 0, validFormulaRef(),
	)
	require.Error(t, err)
	_, err = store.Inspect(context.Background(), "missing")
	require.ErrorIs(t, err, ErrBeadNotFound)
	err = store.AcknowledgeReadyEvent(context.Background(), "missing")
	require.Error(t, err)
	lease, err := store.Claim(context.Background(), ClaimRequest{
		BeadID: "missing", Generation: 1, WorkflowID: "workflow-a",
	})
	require.NoError(t, err)
	require.False(t, lease.Acquired)
	err = store.RecordAttemptFailure(context.Background(), AttemptFailure{
		BeadID: "gc-1", Generation: 2, ClaimToken: "wrong",
	})
	require.ErrorIs(t, err, ErrStaleFence)
}

func TestBridgeRejectsInvalidConfigurationAndReceipt(t *testing.T) {
	event := validReadyEvent(t)
	_, err := (ReadyEventBridge{}).Deliver(context.Background(), event)
	require.Error(t, err)
	_, err = (ReadyEventBridge{
		Temporal: newFakeWorkflowGateway(),
	}).Deliver(context.Background(), event)
	require.Error(t, err)
	_, err = (ReadyEventBridge{
		Temporal: mismatchedReceiptGateway{},
		Acker:    &recordingAcker{},
	}).Deliver(context.Background(), event)
	require.ErrorContains(t, err, "mismatched")

	_, err = (TemporalClientGateway{}).SignalWithStart(
		context.Background(),
		StartOrSignalRequest{Event: event},
	)
	require.ErrorContains(t, err, "client is required")
	err = (TemporalClientGateway{}).SignalClose(
		context.Background(),
		"workflow",
		CloseRequest{},
	)
	require.ErrorContains(t, err, "client is required")
}

func TestReconcilerRejectsMissingDependenciesAndSourceFailure(t *testing.T) {
	_, err := (ReadyEventReconciler{}).Reconcile(context.Background())
	require.Error(t, err)
	_, err = (ReadyEventReconciler{
		Source: emptyReadySource{},
	}).Reconcile(context.Background())
	require.ErrorContains(t, err, "receipt checker")
	_, err = (ReadyEventReconciler{
		Source:   failingReadySource{},
		Receipts: newFakeWorkflowGateway(),
	}).Reconcile(context.Background())
	require.ErrorContains(t, err, "durable ready events")
}

func TestActivityWorkerRejectsMissingDependencies(t *testing.T) {
	event := validReadyEvent(t)
	env := newActivityEnvironment(t, &ActivityWorker{})
	_, err := env.ExecuteActivity(
		(&ActivityWorker{}).ExecuteBead,
		ActivityInput{Event: event},
	)
	require.ErrorContains(t, err, "Beads store")

	store := newReadyStore(t)
	worker := &ActivityWorker{Beads: store}
	env = newActivityEnvironment(t, worker)
	_, err = env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.ErrorContains(t, err, "agent executor")
}

func TestNewWorkerSetRejectsMissingDependencies(t *testing.T) {
	_, err := NewWorkerSet(nil, nil, nil)
	require.ErrorContains(t, err, "temporal client")
	_, err = NewShadowWorkerSet(nil)
	require.ErrorContains(t, err, "temporal client")

	_, err = WorkflowID(
		strings.Repeat("c", 128),
		strings.Repeat("r", 128),
		"gc-1",
	)
	require.ErrorContains(t, err, "workflow identifier")
}

type mismatchedReceiptGateway struct{}

func (mismatchedReceiptGateway) SignalWithStart(
	context.Context,
	StartOrSignalRequest,
) (WorkflowReceipt, error) {
	return WorkflowReceipt{
		WorkflowID: "wrong", EventID: "wrong",
	}, nil
}

func (mismatchedReceiptGateway) SignalClose(
	context.Context,
	string,
	CloseRequest,
) error {
	return nil
}

type recordingAcker struct {
	events []string
}

func (a *recordingAcker) AcknowledgeReadyEvent(
	_ context.Context,
	eventID string,
) error {
	a.events = append(a.events, eventID)
	return nil
}

type failingReadySource struct{}

func (s failingReadySource) PendingReadyEvents(context.Context) ([]ReadyEvent, error) {
	return nil, errors.New("source unavailable")
}

func (s failingReadySource) AcknowledgeReadyEvent(context.Context, string) error {
	return nil
}

type emptyReadySource struct{}

func (emptyReadySource) PendingReadyEvents(context.Context) ([]ReadyEvent, error) {
	return nil, nil
}

func (emptyReadySource) AcknowledgeReadyEvent(context.Context, string) error {
	return nil
}
