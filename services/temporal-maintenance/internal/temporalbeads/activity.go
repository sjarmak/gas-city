package temporalbeads

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

const (
	ExecuteBeadActivityName  = "ExecuteBeadActivity"
	CheckpointPhaseAttached  = "agent-attached"
	CheckpointPhaseComplete  = "agent-complete"
	cancelPropagationTimeout = 10 * time.Second
	maxHeartbeatInterval     = 5 * time.Second
)

// AgentProgress is a compact checkpoint produced by an attached agent session.
type AgentProgress struct {
	SessionID    string        `json:"session_id"`
	Sequence     int64         `json:"sequence"`
	Phase        string        `json:"phase"`
	ArtifactRefs []ArtifactRef `json:"artifact_refs,omitempty"`
}

// AgentExecutionRequest starts or attaches using the stable claim token.
type AgentExecutionRequest struct {
	Event      ReadyEvent           `json:"event"`
	ClaimToken string               `json:"claim_token"`
	SessionID  string               `json:"session_id,omitempty"`
	ResumeFrom *HeartbeatCheckpoint `json:"resume_from,omitempty"`
}

// AgentExecutionResult contains only a terminal code and artifact references.
type AgentExecutionResult struct {
	SessionID    string        `json:"session_id"`
	Outcome      Outcome       `json:"outcome"`
	ArtifactRefs []ArtifactRef `json:"artifact_refs,omitempty"`
}

// AgentCancellation identifies the exact attached session to cancel.
type AgentCancellation struct {
	BeadID     string `json:"bead_id"`
	Generation int64  `json:"generation"`
	ClaimToken string `json:"claim_token"`
	SessionID  string `json:"session_id"`
}

// AgentExecutor owns the nondeterministic start-or-attach session protocol.
type AgentExecutor interface {
	ResolveSession(context.Context, AgentExecutionRequest) (string, error)
	Execute(
		context.Context,
		AgentExecutionRequest,
		func(AgentProgress) error,
	) (AgentExecutionResult, error)
	Cancel(context.Context, AgentCancellation) error
}

// ActivityWorker performs the only claim and outcome mutations in the design.
type ActivityWorker struct {
	Beads BeadStore
	Agent AgentExecutor
}

// ExecuteBead runs or resumes one exact ready generation.
func (w *ActivityWorker) ExecuteBead(
	ctx context.Context,
	input ActivityInput,
) (ActivityResult, error) {
	if err := w.validate(input); err != nil {
		return ActivityResult{}, err
	}
	lease, err := w.claim(ctx, input.Event)
	if err != nil {
		return ActivityResult{}, err
	}
	resume, err := activityHeartbeat(ctx, input.Event, lease)
	if err != nil {
		return ActivityResult{}, err
	}
	if resume != nil && resume.Phase == CheckpointPhaseComplete {
		return w.complete(ctx, input.Event, lease, AgentExecutionResult{
			SessionID:    resume.SessionID,
			Outcome:      OutcomeCompleted,
			ArtifactRefs: cloneArtifacts(resume.ArtifactRefs),
		}, resume.Sequence, false, resume.ArtifactRefsTruncated)
	}

	execution, err := w.runAgent(ctx, input.Event, lease, resume)
	if err != nil {
		return ActivityResult{}, err
	}
	return w.complete(
		ctx,
		input.Event,
		lease,
		execution.result,
		execution.lastSequence,
		true,
		false,
	)
}

func (w *ActivityWorker) validate(input ActivityInput) error {
	if w.Beads == nil {
		return invalidActivityConfiguration("Beads store is required")
	}
	if w.Agent == nil {
		return invalidActivityConfiguration("agent executor is required")
	}
	if err := input.Event.Validate(); err != nil {
		return temporal.NewNonRetryableApplicationError(
			"invalid ready event", "InvalidReadyEvent", err)
	}
	return nil
}

func (w *ActivityWorker) claim(
	ctx context.Context,
	event ReadyEvent,
) (ClaimLease, error) {
	workflowID, err := WorkflowID(event.CityID, event.RunID, event.BeadID)
	if err != nil {
		return ClaimLease{}, temporal.NewNonRetryableApplicationError(
			"invalid workflow identity", "InvalidWorkflowIdentity", err)
	}
	lease, err := w.Beads.Claim(ctx, ClaimRequest{
		BeadID: event.BeadID, Generation: event.Generation, WorkflowID: workflowID,
	})
	if err != nil {
		return ClaimLease{}, fmt.Errorf("claim ready generation: %w", err)
	}
	if !lease.Acquired || lease.BeadID != event.BeadID ||
		lease.Generation != event.Generation || lease.Token == "" {
		return ClaimLease{}, temporal.NewNonRetryableApplicationError(
			"stale claim rejected", "StaleClaim", ErrStaleFence)
	}
	return lease, nil
}

type agentExecution struct {
	result       AgentExecutionResult
	lastSequence int64
}

func (w *ActivityWorker) runAgent(
	ctx context.Context,
	event ReadyEvent,
	lease ClaimLease,
	resume *HeartbeatCheckpoint,
) (agentExecution, error) {
	request := AgentExecutionRequest{
		Event: event, ClaimToken: lease.Token, ResumeFrom: cloneCheckpoint(resume),
	}
	if resume != nil {
		request.SessionID = resume.SessionID
	} else {
		sessionID, err := w.Agent.ResolveSession(ctx, request)
		if err != nil {
			return agentExecution{}, fmt.Errorf("resolve agent session: %w", err)
		}
		if err := validateSegment("session id", sessionID); err != nil {
			return agentExecution{}, temporal.NewNonRetryableApplicationError(
				"agent resolved an invalid session identity", "InvalidAgentSession", err)
		}
		request.SessionID = sessionID
	}
	tracker := newHeartbeatTracker(ctx, event, lease, resume, request.SessionID)
	stopHeartbeats := startHeartbeatPump(ctx, tracker)
	defer stopHeartbeats()
	executionDone := make(chan struct{})
	cancellationWatchDone := make(chan struct{})
	var cancelOnce sync.Once
	var cancelErr error
	cancelAttached := func() error {
		cancelOnce.Do(func() {
			cancelErr = w.cancelAttachedSession(
				ctx,
				event,
				lease,
				request.SessionID,
			)
		})
		return cancelErr
	}
	go func() {
		defer close(cancellationWatchDone)
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				_ = cancelAttached()
			}
		case <-executionDone:
		}
	}()
	result, runErr := w.Agent.Execute(ctx, request, tracker.Record)
	close(executionDone)
	<-cancellationWatchDone
	if errors.Is(runErr, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		if err := cancelAttached(); err != nil {
			return agentExecution{}, err
		}
		return agentExecution{}, temporal.NewCanceledError("agent execution canceled")
	}
	if runErr == nil {
		attachedSessionID := tracker.SessionID()
		if result.SessionID == "" {
			result.SessionID = attachedSessionID
		}
		if attachedSessionID != "" && result.SessionID != attachedSessionID {
			return agentExecution{}, temporal.NewNonRetryableApplicationError(
				"agent changed attached session identity", "InvalidAgentResult", nil)
		}
		return agentExecution{result: result, lastSequence: tracker.Sequence()}, nil
	}
	activity.GetLogger(ctx).Error("agent execution failed", "error", runErr)
	failure := AttemptFailure{
		BeadID: event.BeadID, Generation: event.Generation, ClaimToken: lease.Token,
		Attempt: activity.GetInfo(ctx).Attempt, Code: "agent-execution-failed",
	}
	if err := w.Beads.RecordAttemptFailure(ctx, failure); err != nil {
		return agentExecution{}, fmt.Errorf("record fenced attempt failure: %w", err)
	}
	return agentExecution{}, temporal.NewApplicationError(
		"agent execution failed", "AgentExecutionFailed")
}

func (w *ActivityWorker) cancelAttachedSession(
	ctx context.Context,
	event ReadyEvent,
	lease ClaimLease,
	sessionID string,
) error {
	if sessionID == "" {
		return temporal.NewNonRetryableApplicationError(
			"canceled agent has no attached session", "MissingSessionIdentity", nil)
	}
	cancelCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		cancelPropagationTimeout,
	)
	defer cancel()
	err := w.Agent.Cancel(cancelCtx, AgentCancellation{
		BeadID: event.BeadID, Generation: event.Generation,
		ClaimToken: lease.Token, SessionID: sessionID,
	})
	if err != nil {
		return fmt.Errorf("cancel attached agent session: %w", err)
	}
	return nil
}

func (w *ActivityWorker) complete(
	ctx context.Context,
	event ReadyEvent,
	lease ClaimLease,
	result AgentExecutionResult,
	lastSequence int64,
	recordFinalHeartbeat bool,
	artifactRefsTruncated bool,
) (ActivityResult, error) {
	if result.Outcome != OutcomeCompleted || result.SessionID == "" {
		return ActivityResult{}, temporal.NewNonRetryableApplicationError(
			"agent returned an invalid terminal result", "InvalidAgentResult", nil)
	}
	if err := validateSegment("session id", result.SessionID); err != nil {
		return ActivityResult{}, temporal.NewNonRetryableApplicationError(
			"agent returned an invalid session identity", "InvalidAgentResult", err)
	}
	workflowExecution := activity.GetInfo(ctx).WorkflowExecution
	if err := validateWorkflowID(
		"source workflow id",
		workflowExecution.ID,
	); err != nil {
		return ActivityResult{}, temporal.NewNonRetryableApplicationError(
			"Temporal Activity has no valid source workflow identity",
			"InvalidSourceWorkflow",
			err,
		)
	}
	if err := validateSegment(
		"source workflow run id",
		workflowExecution.RunID,
	); err != nil {
		return ActivityResult{}, temporal.NewNonRetryableApplicationError(
			"Temporal Activity has no valid source workflow run identity",
			"InvalidSourceWorkflow",
			err,
		)
	}
	artifactRefs := result.ArtifactRefs
	artifactOverflow := artifactRefsTruncated
	if len(artifactRefs) > MaxOutcomeEvidenceReferences {
		artifactOverflow = true
		artifactRefs = artifactRefs[:MaxOutcomeEvidenceReferences]
	}
	for _, artifact := range artifactRefs {
		if err := artifact.Validate(); err != nil {
			return ActivityResult{}, temporal.NewNonRetryableApplicationError(
				"agent returned an invalid artifact reference", "InvalidArtifactRef", err)
		}
	}
	if recordFinalHeartbeat {
		checkpoint := HeartbeatCheckpoint{
			BeadID: event.BeadID, Generation: event.Generation,
			ClaimToken: lease.Token, SessionID: result.SessionID,
			Sequence: lastSequence + 1, Phase: CheckpointPhaseComplete,
			ArtifactRefs:          cloneArtifacts(artifactRefs),
			ArtifactRefsTruncated: artifactOverflow,
		}
		if err := checkpoint.validatePayload(); err != nil {
			return ActivityResult{}, temporal.NewNonRetryableApplicationError(
				"terminal heartbeat is invalid", "InvalidHeartbeat", err)
		}
		activity.RecordHeartbeat(ctx, checkpoint)
	}
	err := w.Beads.Complete(ctx, Completion{
		BeadID: event.BeadID, Generation: event.Generation, ClaimToken: lease.Token,
		SessionID: result.SessionID, Outcome: result.Outcome,
		SourceWorkflowID:    workflowExecution.ID,
		SourceWorkflowRunID: workflowExecution.RunID,
		ArtifactRefs:        cloneArtifacts(artifactRefs),
	})
	if err != nil {
		return ActivityResult{}, fmt.Errorf("write fenced completion: %w", err)
	}
	return ActivityResult{
		EventID:               event.EventID,
		Outcome:               string(result.Outcome),
		SessionID:             result.SessionID,
		ArtifactRefs:          cloneArtifacts(artifactRefs),
		ArtifactRefsTruncated: artifactOverflow,
	}, nil
}

func invalidActivityConfiguration(message string) error {
	return temporal.NewNonRetryableApplicationError(
		message, "InvalidActivityConfiguration", nil)
}

func activityHeartbeat(
	ctx context.Context,
	event ReadyEvent,
	lease ClaimLease,
) (*HeartbeatCheckpoint, error) {
	if !activity.HasHeartbeatDetails(ctx) {
		return nil, nil
	}
	var checkpoint HeartbeatCheckpoint
	if err := activity.GetHeartbeatDetails(ctx, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode Activity heartbeat: %w", err)
	}
	if checkpoint.BeadID != event.BeadID ||
		checkpoint.Generation != event.Generation ||
		checkpoint.ClaimToken != lease.Token ||
		checkpoint.Sequence < 0 {
		return nil, temporal.NewNonRetryableApplicationError(
			"heartbeat belongs to a stale claim", "StaleHeartbeat", ErrStaleFence)
	}
	if err := checkpoint.validatePayload(); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			"heartbeat checkpoint is invalid", "InvalidHeartbeat", err)
	}
	return &checkpoint, nil
}

type heartbeatTracker struct {
	mu        sync.Mutex
	ctx       context.Context
	event     ReadyEvent
	lease     ClaimLease
	sessionID string
	sequence  int64
	phase     string
	artifacts []ArtifactRef
}

func newHeartbeatTracker(
	ctx context.Context,
	event ReadyEvent,
	lease ClaimLease,
	resume *HeartbeatCheckpoint,
	sessionID string,
) *heartbeatTracker {
	tracker := &heartbeatTracker{
		ctx: ctx, event: event, lease: lease, sessionID: sessionID,
		phase: CheckpointPhaseAttached,
	}
	if resume != nil {
		tracker.sessionID = resume.SessionID
		tracker.sequence = resume.Sequence
		tracker.phase = resume.Phase
		tracker.artifacts = cloneArtifacts(resume.ArtifactRefs)
	}
	return tracker
}

func startHeartbeatPump(ctx context.Context, tracker *heartbeatTracker) func() {
	timeout := activity.GetInfo(ctx).HeartbeatTimeout
	if timeout <= 0 {
		return func() {}
	}
	interval := timeout / 3
	if interval <= 0 {
		interval = timeout
	}
	if interval > maxHeartbeatInterval {
		interval = maxHeartbeatInterval
	}
	tracker.Heartbeat()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tracker.Heartbeat()
			case <-ctx.Done():
				return
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func (t *heartbeatTracker) Heartbeat() {
	t.mu.Lock()
	defer t.mu.Unlock()
	activity.RecordHeartbeat(t.ctx, HeartbeatCheckpoint{
		BeadID: t.event.BeadID, Generation: t.event.Generation,
		ClaimToken: t.lease.Token, SessionID: t.sessionID,
		Sequence: t.sequence, Phase: t.phase,
		ArtifactRefs: cloneArtifacts(t.artifacts),
	})
}

func (t *heartbeatTracker) Record(progress AgentProgress) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	checkpoint := HeartbeatCheckpoint{
		BeadID: t.event.BeadID, Generation: t.event.Generation,
		ClaimToken: t.lease.Token, SessionID: progress.SessionID,
		Sequence: progress.Sequence, Phase: progress.Phase,
		ArtifactRefs: cloneArtifacts(progress.ArtifactRefs),
	}
	if err := checkpoint.validatePayload(); err != nil {
		return fmt.Errorf("invalid agent checkpoint: %w", err)
	}
	if t.sessionID != "" && progress.SessionID != t.sessionID {
		return fmt.Errorf("agent checkpoint changed session identity")
	}
	if progress.Sequence == t.sequence {
		if progress.Phase != t.phase ||
			!artifactsEqual(progress.ArtifactRefs, t.artifacts) {
			return fmt.Errorf("agent checkpoint reused sequence with different content")
		}
		return nil
	}
	if progress.Sequence < t.sequence {
		return fmt.Errorf("agent checkpoint sequence must increase monotonically")
	}
	for _, artifact := range progress.ArtifactRefs {
		if err := artifact.Validate(); err != nil {
			return err
		}
	}
	t.sessionID = progress.SessionID
	t.sequence = progress.Sequence
	t.phase = progress.Phase
	t.artifacts = cloneArtifacts(progress.ArtifactRefs)
	activity.RecordHeartbeat(t.ctx, checkpoint)
	return nil
}

func (t *heartbeatTracker) SessionID() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sessionID
}

func (t *heartbeatTracker) Sequence() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sequence
}

func cloneCheckpoint(checkpoint *HeartbeatCheckpoint) *HeartbeatCheckpoint {
	if checkpoint == nil {
		return nil
	}
	cloned := *checkpoint
	cloned.ArtifactRefs = cloneArtifacts(checkpoint.ArtifactRefs)
	return &cloned
}
