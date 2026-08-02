package temporalbeads

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
)

func TestActivityWorkerClaimsHeartbeatsAndCompletesExactGeneration(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.progress = []AgentProgress{{
		SessionID: "session-1",
		Sequence:  1,
		Phase:     "tests",
	}}
	executor.result = AgentExecutionResult{
		SessionID: "session-1",
		Outcome:   OutcomeCompleted,
		ArtifactRefs: []ArtifactRef{
			testArtifact(),
		},
	}
	worker := &ActivityWorker{Beads: store, Agent: executor}
	env := newActivityEnvironment(t, worker)

	var heartbeats []HeartbeatCheckpoint
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		var checkpoint HeartbeatCheckpoint
		require.NoError(t, details.Get(&checkpoint))
		heartbeats = append(heartbeats, checkpoint)
	})

	value, err := env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.NoError(t, err)
	var result ActivityResult
	require.NoError(t, value.Get(&result))
	require.Equal(t, OutcomeCompleted, Outcome(result.Outcome))
	require.Equal(t, "session-1", result.SessionID)
	require.Equal(t, []ArtifactRef{testArtifact()}, result.ArtifactRefs)

	require.NotEmpty(t, heartbeats)
	require.Equal(t, int64(1), heartbeats[0].Sequence)
	for index := 1; index < len(heartbeats); index++ {
		require.Greater(t, heartbeats[index].Sequence, heartbeats[index-1].Sequence)
	}
	record, err := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusCompleted, record.Status)
	require.Equal(t, "session-1", record.SessionID)
	require.Equal(t, []ArtifactRef{testArtifact()}, record.ArtifactRefs)
}

func TestActivityWorkerCarriesExactTemporalExecutionIdentityToCompletion(
	t *testing.T,
) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	capturing := &completionCaptureStore{BeadStore: store}
	executor := newFakeAgentExecutor()
	executor.result = AgentExecutionResult{
		SessionID: "session-source-identity",
		Outcome:   OutcomeCompleted,
	}
	worker := &ActivityWorker{Beads: capturing, Agent: executor}
	env := newActivityEnvironment(t, worker)
	var sourceWorkflowID, sourceWorkflowRunID string
	env.SetOnActivityHeartbeatListener(
		func(info *activity.Info, _ converter.EncodedValues) {
			sourceWorkflowID = info.WorkflowExecution.ID
			sourceWorkflowRunID = info.WorkflowExecution.RunID
		},
	)

	_, err := env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.NoError(t, err)
	require.NotEmpty(t, sourceWorkflowID)
	require.NotEmpty(t, sourceWorkflowRunID)
	require.Equal(t, sourceWorkflowID, capturing.completion.SourceWorkflowID)
	require.Equal(
		t,
		sourceWorkflowRunID,
		capturing.completion.SourceWorkflowRunID,
	)
	require.NotEqual(t, event.RunID, capturing.completion.SourceWorkflowRunID)
}

func TestActivityRetryReusesClaimAndAgentSession(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.result = AgentExecutionResult{
		SessionID: "session-stable",
		Outcome:   OutcomeCompleted,
	}
	worker := &ActivityWorker{Beads: store, Agent: executor}
	firstEnv := newActivityEnvironment(t, worker)

	var final HeartbeatCheckpoint
	firstEnv.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		require.NoError(t, details.Get(&final))
	})
	_, err := firstEnv.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.NoError(t, err)
	require.Equal(t, CheckpointPhaseComplete, final.Phase)

	secondEnv := newActivityEnvironment(t, worker)
	secondEnv.SetHeartbeatDetails(final)
	_, err = secondEnv.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.NoError(t, err)

	require.Equal(t, 1, executor.StartCount())
	require.Equal(t, []string{"session-stable"}, executor.SessionIDs())
	record, err := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusCompleted, record.Status)
}

func TestActivityWorkerCrashResumesFromHeartbeatWithoutSecondSession(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.progress = []AgentProgress{{
		SessionID: "session-stable",
		Sequence:  3,
		Phase:     "authoring",
		ArtifactRefs: []ArtifactRef{
			testArtifact(),
		},
	}}
	executor.err = errors.New("injected worker crash")
	worker := &ActivityWorker{Beads: store, Agent: executor}
	firstEnv := newActivityEnvironment(t, worker)

	var checkpoint HeartbeatCheckpoint
	firstEnv.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		require.NoError(t, details.Get(&checkpoint))
	})
	_, err := firstEnv.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.Error(t, err)
	require.Equal(t, "session-stable", checkpoint.SessionID)
	require.Equal(t, int64(3), checkpoint.Sequence)

	executor.err = nil
	executor.result = AgentExecutionResult{
		SessionID:    "session-stable",
		Outcome:      OutcomeCompleted,
		ArtifactRefs: []ArtifactRef{testArtifact()},
	}
	secondEnv := newActivityEnvironment(t, worker)
	secondEnv.SetHeartbeatDetails(checkpoint)
	_, err = secondEnv.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.NoError(t, err)

	require.Equal(t, 1, executor.StartCount())
	require.Equal(t, &checkpoint, executor.LastRequest().ResumeFrom)
	record, err := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, err)
	workflowID, err := WorkflowID(event.CityID, event.RunID, event.BeadID)
	require.NoError(t, err)
	require.Equal(t, workflowID, record.WorkflowID)
	require.Equal(t, BeadStatusCompleted, record.Status)
	require.Equal(t, []ArtifactRef{testArtifact()}, record.ArtifactRefs)
}

func TestActivityCancellationReachesAttachedSessionAndStaleCompletionFails(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.progress = []AgentProgress{{
		SessionID: "session-cancel",
		Sequence:  1,
		Phase:     "running",
	}}
	executor.err = context.Canceled
	worker := &ActivityWorker{Beads: store, Agent: executor}
	env := newActivityEnvironment(t, worker)

	_, err := env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.Error(t, err)
	require.Equal(t, []string{"session-cancel"}, executor.CanceledSessionIDs())

	oldRecord, err := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, err)
	oldToken := oldRecord.ClaimToken
	_, err = store.TransitionReady(
		context.Background(), "city", "run", event.BeadID, 2, event.Formula,
	)
	require.NoError(t, err)
	err = store.Complete(context.Background(), Completion{
		BeadID: event.BeadID, Generation: 1, ClaimToken: oldToken,
		SessionID: "session-cancel", Outcome: OutcomeCompleted,
	})
	require.ErrorIs(t, err, ErrStaleFence)
}

func TestActivityCancellationBeforeFirstCheckpointUsesResolvedSession(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.resolvedSessionID = "session-before-heartbeat"
	executor.err = context.Canceled
	worker := &ActivityWorker{Beads: store, Agent: executor}
	env := newActivityEnvironment(t, worker)

	_, err := env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.Error(t, err)
	require.Equal(
		t,
		[]string{"session-before-heartbeat"},
		executor.CanceledSessionIDs(),
	)
}

func TestActivityWorkerRejectsStaleGenerationBeforeAgentStarts(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	_, err := store.TransitionReady(
		context.Background(), "city", "run", event.BeadID, 2, event.Formula,
	)
	require.NoError(t, err)
	executor := newFakeAgentExecutor()
	worker := &ActivityWorker{Beads: store, Agent: executor}
	env := newActivityEnvironment(t, worker)

	_, err = env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.ErrorContains(t, err, "stale claim")
	require.Zero(t, executor.StartCount())
	record, inspectErr := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, inspectErr)
	require.Equal(t, int64(2), record.Generation)
	require.Empty(t, record.Outcome)
}

func TestActivityWorkerBoundsTooManyArtifactsAndCompletesCanonically(
	t *testing.T,
) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.result = AgentExecutionResult{
		SessionID:    "session-overflow",
		Outcome:      OutcomeCompleted,
		ArtifactRefs: make([]ArtifactRef, MaxOutcomeEvidenceReferences+1),
	}
	for index := range executor.result.ArtifactRefs {
		executor.result.ArtifactRefs[index] = testArtifact()
	}
	worker := &ActivityWorker{Beads: store, Agent: executor}
	env := newActivityEnvironment(t, worker)

	value, err := env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.NoError(t, err)
	var result ActivityResult
	require.NoError(t, value.Get(&result))
	require.True(t, result.ArtifactRefsTruncated)
	require.Len(t, result.ArtifactRefs, MaxOutcomeEvidenceReferences)

	record, inspectErr := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, inspectErr)
	require.Equal(t, BeadStatusCompleted, record.Status)
	require.Equal(t, OutcomeCompleted, record.Outcome)
	require.Len(t, record.ArtifactRefs, MaxOutcomeEvidenceReferences)
}

func TestActivityRetryPreservesArtifactTruncationFromTerminalHeartbeat(
	t *testing.T,
) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.result = AgentExecutionResult{
		SessionID:    "session-overflow-retry",
		Outcome:      OutcomeCompleted,
		ArtifactRefs: make([]ArtifactRef, MaxOutcomeEvidenceReferences+1),
	}
	for index := range executor.result.ArtifactRefs {
		executor.result.ArtifactRefs[index] = testArtifact()
	}
	worker := &ActivityWorker{Beads: store, Agent: executor}
	firstEnv := newActivityEnvironment(t, worker)
	var terminal HeartbeatCheckpoint
	firstEnv.SetOnActivityHeartbeatListener(
		func(_ *activity.Info, details converter.EncodedValues) {
			var checkpoint HeartbeatCheckpoint
			require.NoError(t, details.Get(&checkpoint))
			if checkpoint.Phase == CheckpointPhaseComplete {
				terminal = checkpoint
			}
		},
	)

	firstValue, err := firstEnv.ExecuteActivity(
		worker.ExecuteBead,
		ActivityInput{Event: event},
	)
	require.NoError(t, err)
	var firstResult ActivityResult
	require.NoError(t, firstValue.Get(&firstResult))
	require.True(t, firstResult.ArtifactRefsTruncated)
	require.True(t, terminal.ArtifactRefsTruncated)

	secondEnv := newActivityEnvironment(t, worker)
	secondEnv.SetHeartbeatDetails(terminal)
	secondValue, err := secondEnv.ExecuteActivity(
		worker.ExecuteBead,
		ActivityInput{Event: event},
	)
	require.NoError(t, err)
	var secondResult ActivityResult
	require.NoError(t, secondValue.Get(&secondResult))
	require.True(t, secondResult.ArtifactRefsTruncated)
	require.Len(t, secondResult.ArtifactRefs, MaxOutcomeEvidenceReferences)
	require.Equal(t, 1, executor.StartCount())
}

func TestActivityWorkerRejectsCheckpointSequenceReuseWithDifferentContent(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.progress = []AgentProgress{
		{SessionID: "session-1", Sequence: 1, Phase: "authoring"},
		{SessionID: "session-1", Sequence: 1, Phase: "review"},
	}
	worker := &ActivityWorker{Beads: store, Agent: executor}
	env := newActivityEnvironment(t, worker)

	_, err := env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.Error(t, err)
	record, inspectErr := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, inspectErr)
	require.Empty(t, record.Outcome)
}

func TestActivityWorkerRejectsTerminalSessionIdentityChange(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.progress = []AgentProgress{{
		SessionID: "session-attached",
		Sequence:  1,
		Phase:     "running",
	}}
	executor.result = AgentExecutionResult{
		SessionID: "session-different",
		Outcome:   OutcomeCompleted,
	}
	worker := &ActivityWorker{Beads: store, Agent: executor}
	env := newActivityEnvironment(t, worker)

	_, err := env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.ErrorContains(t, err, "changed attached session identity")
	record, inspectErr := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, inspectErr)
	require.Equal(t, BeadStatusClaimed, record.Status)
	require.Empty(t, record.Outcome)
}

func TestActivityWorkerRejectsMalformedCheckpointBeforeHeartbeat(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	executor := newFakeAgentExecutor()
	executor.progress = []AgentProgress{{
		SessionID: "session-1",
		Sequence:  1,
		Phase:     "unsafe phase",
	}}
	worker := &ActivityWorker{Beads: store, Agent: executor}
	env := newActivityEnvironment(t, worker)
	heartbeatCount := 0
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, _ converter.EncodedValues) {
		heartbeatCount++
	})

	_, err := env.ExecuteActivity(worker.ExecuteBead, ActivityInput{Event: event})
	require.Error(t, err)
	require.Zero(t, heartbeatCount)
	record, inspectErr := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, inspectErr)
	require.Equal(t, BeadStatusClaimed, record.Status)
	require.Empty(t, record.Outcome)
}

func TestActivityWorkerDoesNotPollBeadsForWork(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("activity.go"))
	require.NoError(t, err)
	require.NotContains(t, string(source), "PendingReadyEvents")
	require.NotContains(t, string(source), "TransitionReady")
	require.Contains(t, string(source), "input.Event")
}

func newActivityEnvironment(
	t *testing.T,
	worker *ActivityWorker,
) *testsuite.TestActivityEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(worker)
	return env
}

type completionCaptureStore struct {
	BeadStore
	completion Completion
}

func (s *completionCaptureStore) Complete(
	ctx context.Context,
	completion Completion,
) error {
	s.completion = completion
	return s.BeadStore.Complete(ctx, completion)
}

type fakeAgentExecutor struct {
	mu                sync.Mutex
	progress          []AgentProgress
	result            AgentExecutionResult
	err               error
	resolvedSessionID string
	sessions          map[string]string
	starts            int
	requests          []AgentExecutionRequest
	canceled          []string
}

func newFakeAgentExecutor() *fakeAgentExecutor {
	return &fakeAgentExecutor{sessions: make(map[string]string)}
}

func (e *fakeAgentExecutor) ResolveSession(
	_ context.Context,
	request AgentExecutionRequest,
) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if sessionID, exists := e.sessions[request.ClaimToken]; exists {
		return sessionID, nil
	}
	e.starts++
	sessionID := e.resolvedSessionID
	if sessionID == "" && len(e.progress) > 0 {
		sessionID = e.progress[0].SessionID
	}
	if sessionID == "" {
		sessionID = e.result.SessionID
	}
	if sessionID == "" {
		sessionID = "session-stable"
	}
	e.sessions[request.ClaimToken] = sessionID
	return sessionID, nil
}

func (e *fakeAgentExecutor) Execute(
	_ context.Context,
	request AgentExecutionRequest,
	heartbeat func(AgentProgress) error,
) (AgentExecutionResult, error) {
	e.mu.Lock()
	e.requests = append(e.requests, request)
	progress := append([]AgentProgress(nil), e.progress...)
	result := e.result
	runErr := e.err
	e.mu.Unlock()

	for _, checkpoint := range progress {
		if err := heartbeat(checkpoint); err != nil {
			return AgentExecutionResult{}, err
		}
	}
	if result.SessionID == "" {
		result.SessionID = request.SessionID
	}
	return result, runErr
}

func (e *fakeAgentExecutor) Cancel(
	_ context.Context,
	request AgentCancellation,
) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.canceled = append(e.canceled, request.SessionID)
	return nil
}

func (e *fakeAgentExecutor) StartCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.starts
}

func (e *fakeAgentExecutor) SessionIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.sessions))
	for _, sessionID := range e.sessions {
		out = append(out, sessionID)
	}
	return out
}

func (e *fakeAgentExecutor) CanceledSessionIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.canceled...)
}

func (e *fakeAgentExecutor) LastRequest() AgentExecutionRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.requests[len(e.requests)-1]
}

func testArtifact() ArtifactRef {
	return ArtifactRef{
		Kind:   ArtifactKindCommit,
		URI:    "git:commit:0123456789abcdef",
		SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
}
