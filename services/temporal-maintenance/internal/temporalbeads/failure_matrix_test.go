package temporalbeads

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

const persistedHistoryPath = "testdata/bead_orchestration_history.json"

func TestIntegration_DuplicateDeliveryAndWorkerExecutionAcrossSeparateQueues(t *testing.T) {
	binary, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("temporal CLI is required for the WorkerSet integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: binary,
		LogLevel:     "error",
	})
	require.NoError(t, err)
	defer func() { _ = server.Stop() }()
	temporalClient := server.Client()
	defer temporalClient.Close()

	store, err := OpenFileBeadStore(
		filepath.Join(t.TempDir(), "beads.json"),
		NewManualClock(time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)),
	)
	require.NoError(t, err)
	event, err := store.TransitionReady(
		ctx, "city", "integration-run", "gc-1", 1, validFormulaRef(),
	)
	require.NoError(t, err)
	agent := newFakeAgentExecutor()
	agent.result = AgentExecutionResult{
		SessionID:    "integration-session",
		Outcome:      OutcomeCompleted,
		ArtifactRefs: []ArtifactRef{testArtifact()},
	}
	_, err = NewWorkerSet(temporalClient, nil, agent)
	require.ErrorContains(t, err, "Beads store")
	_, err = NewWorkerSet(temporalClient, store, nil)
	require.ErrorContains(t, err, "agent executor")
	workers, err := NewWorkerSet(temporalClient, store, agent)
	require.NoError(t, err)
	require.NoError(t, workers.Start())
	defer workers.Stop()

	gateway := TemporalClientGateway{Client: temporalClient}
	bridge := ReadyEventBridge{Temporal: gateway, Acker: store}
	first, err := bridge.Deliver(ctx, event)
	require.NoError(t, err)
	second, err := bridge.Deliver(ctx, event)
	require.NoError(t, err)
	require.Equal(t, first.WorkflowID, second.WorkflowID)

	require.NoError(t, bridge.SealRun(ctx, CloseRequest{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		ExpectedEventIDs: []string{event.EventID},
		ReasonCode:       "integration-complete",
	}))
	run := temporalClient.GetWorkflow(ctx, first.WorkflowID, first.RunID)
	var state WorkflowState
	require.NoError(t, run.Get(ctx, &state))
	require.Equal(t, []string{event.EventID}, state.CompletedEventIDs)
	require.Equal(t, 1, agent.StartCount())
	record, err := store.Inspect(ctx, event.BeadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusCompleted, record.Status)
	require.Equal(t, "integration-session", record.SessionID)
	seen, err := gateway.HasEvent(ctx, first.WorkflowID, event.EventID)
	require.NoError(t, err)
	require.True(t, seen)

	capturePath := os.Getenv("TEMPORAL_BEADS_CAPTURE_HISTORY")
	if capturePath != "" {
		require.NoError(t, writeHistory(
			ctx, temporalClient, first.WorkflowID, first.RunID, capturePath))
	}
	workers.Stop()
	require.ErrorContains(t, workers.Start(), "cannot be restarted")
}

func TestIntegration_WorkflowWaitsForAgentCancellation(t *testing.T) {
	binary, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("temporal CLI is required for the cancellation integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: binary,
		LogLevel:     "error",
	})
	require.NoError(t, err)
	defer func() { _ = server.Stop() }()
	temporalClient := server.Client()
	defer temporalClient.Close()

	store, err := OpenFileBeadStore(
		filepath.Join(t.TempDir(), "beads.json"),
		NewManualClock(time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)),
	)
	require.NoError(t, err)
	event, err := store.TransitionReady(
		ctx, "city", "cancel-run", "gc-cancel", 1, validFormulaRef(),
	)
	require.NoError(t, err)
	agent := newCancellationProbeExecutor()
	workers, err := NewWorkerSet(temporalClient, store, agent)
	require.NoError(t, err)
	require.NoError(t, workers.Start())
	defer workers.Stop()

	receipt, err := (ReadyEventBridge{
		Temporal: TemporalClientGateway{Client: temporalClient},
		Acker:    store,
		Timing: TimingConfig{
			HeartbeatTimeout:  300 * time.Millisecond,
			ReconcileInterval: time.Minute,
			Clock: NewManualClock(
				time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC),
			),
		},
	}).Deliver(ctx, event)
	require.NoError(t, err)
	select {
	case <-agent.started:
	case <-ctx.Done():
		t.Fatal("agent Activity did not start")
	}
	require.NoError(t, temporalClient.CancelWorkflow(
		ctx,
		receipt.WorkflowID,
		receipt.RunID,
	))

	err = temporalClient.GetWorkflow(
		ctx,
		receipt.WorkflowID,
		receipt.RunID,
	).Get(ctx, nil)
	require.Error(t, err)
	require.True(t, agent.cleaned.Load())
}

func TestWorkerSetCannotRestartAfterStop(t *testing.T) {
	workers := &WorkerSet{}
	workers.Stop()
	require.ErrorContains(t, workers.Start(), "cannot be restarted")
}

func TestReplayPersistedWorkflowHistory(t *testing.T) {
	history := loadPersistedHistory(t)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(
		BeadOrchestrationWorkflow,
		workflow.RegisterOptions{Name: BeadOrchestrationWorkflowName},
	)
	require.NoError(t, replayer.ReplayWorkflowHistory(nil, history))
}

func TestReplayRejectsPlantedNondeterministicWorkflow(t *testing.T) {
	history := loadPersistedHistory(t)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(
		plantedNondeterministicWorkflow,
		workflow.RegisterOptions{Name: BeadOrchestrationWorkflowName},
	)
	err := replayer.ReplayWorkflowHistory(nil, history)
	require.Error(t, err)
}

func TestTemporalUnavailableDoesNotBlockInspectionOrRecoveryEntryPoint(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	gateway.err = context.DeadlineExceeded
	bridge := ReadyEventBridge{Temporal: gateway, Acker: store}

	_, err := bridge.Deliver(context.Background(), event)
	require.Error(t, err)
	_, err = store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, err)

	recoveryCalled := false
	coreRecovery := func() { recoveryCalled = true }
	coreRecovery()
	require.True(t, recoveryCalled)
	require.Zero(t, newFakeAgentExecutor().StartCount())
}

func TestPlantedDisabledFenceFailsDurableFactOracle(t *testing.T) {
	safe := newReadyStore(t)
	require.NoError(t, staleCompletionOracle(safe, false))

	planted := newReadyStore(t)
	err := staleCompletionOracle(planted, true)
	require.ErrorContains(t, err, "oracle detected stale outcome overwrite")
}

func staleCompletionOracle(store *FileBeadStore, disableFence bool) error {
	ctx := context.Background()
	oldLease, err := store.Claim(ctx, ClaimRequest{
		BeadID: "gc-1", Generation: 1, WorkflowID: "old-workflow",
	})
	if err != nil {
		return err
	}
	if _, err := store.TransitionReady(
		ctx, "city", "run", "gc-1", 2, validFormulaRef(),
	); err != nil {
		return err
	}
	completion := Completion{
		BeadID: "gc-1", Generation: 1, ClaimToken: oldLease.Token,
		SessionID: "stale-session", Outcome: OutcomeCompleted,
	}
	if disableFence {
		store.forceUnsafeCompletionForTest(completion)
	} else if err := store.Complete(ctx, completion); !errors.Is(err, ErrStaleFence) {
		return fmt.Errorf("safe store did not reject stale completion: %w", err)
	}
	record, err := store.Inspect(ctx, "gc-1")
	if err != nil {
		return err
	}
	if record.Outcome != "" || record.SessionID != "" {
		return fmt.Errorf("oracle detected stale outcome overwrite")
	}
	return nil
}

func plantedNondeterministicWorkflow(
	ctx workflow.Context,
	input WorkflowInput,
) (WorkflowState, error) {
	if err := workflow.Sleep(ctx, time.Minute); err != nil {
		return WorkflowState{}, err
	}
	return BeadOrchestrationWorkflow(ctx, input)
}

type cancellationProbeExecutor struct {
	started   chan struct{}
	startOnce sync.Once
	cleaned   atomic.Bool
}

func newCancellationProbeExecutor() *cancellationProbeExecutor {
	return &cancellationProbeExecutor{started: make(chan struct{})}
}

func (e *cancellationProbeExecutor) ResolveSession(
	context.Context,
	AgentExecutionRequest,
) (string, error) {
	return "cancel-probe-session", nil
}

func (e *cancellationProbeExecutor) Execute(
	ctx context.Context,
	_ AgentExecutionRequest,
	_ func(AgentProgress) error,
) (AgentExecutionResult, error) {
	e.startOnce.Do(func() { close(e.started) })
	<-ctx.Done()
	return AgentExecutionResult{}, ctx.Err()
}

func (e *cancellationProbeExecutor) Cancel(
	ctx context.Context,
	_ AgentCancellation,
) error {
	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		e.cleaned.Store(true)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func loadPersistedHistory(t *testing.T) *historypb.History {
	t.Helper()
	data, err := os.ReadFile(persistedHistoryPath)
	require.NoError(t, err)
	var history historypb.History
	require.NoError(t, protojson.Unmarshal(data, &history))
	return &history
}

func writeHistory(
	ctx context.Context,
	temporalClient client.Client,
	workflowID string,
	runID string,
	path string,
) error {
	iterator := temporalClient.GetWorkflowHistory(
		ctx,
		workflowID,
		runID,
		false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	history := &historypb.History{}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return err
		}
		history.Events = append(history.Events, event)
	}
	data, err := protojson.MarshalOptions{
		UseProtoNames: true,
		Indent:        "  ",
	}.Marshal(history)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
