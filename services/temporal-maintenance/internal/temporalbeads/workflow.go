package temporalbeads

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	workflowPhaseRunning   = "running"
	workflowPhaseCompleted = "completed"
	workflowPhaseFailed    = "failed"
	formulaObservabilityV1 = 1
)

// BeadOrchestrationWorkflow deterministically maps ready events to Activities.
func BeadOrchestrationWorkflow(
	ctx workflow.Context,
	input WorkflowInput,
) (WorkflowState, error) {
	formulaVersion := workflow.GetVersion(
		ctx,
		"formula-ref-observability-v1",
		workflow.DefaultVersion,
		formulaObservabilityV1,
	)
	requireFormula := formulaVersion != workflow.DefaultVersion
	if err := input.validate(requireFormula); err != nil {
		return WorkflowState{}, temporal.NewNonRetryableApplicationError(
			"invalid orchestration input", "InvalidOrchestrationInput", err)
	}
	state := &WorkflowState{
		ContractVersion: CurrentContractVersion,
		CityID:          input.CityID,
		RunID:           input.RunID,
		BeadID:          input.BeadID,
		Phase:           workflowPhaseRunning,
	}
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (WorkflowState, error) {
		return cloneWorkflowState(*state), nil
	}); err != nil {
		return *state, err
	}

	run := newWorkflowRun(ctx, input, state, requireFormula)
	defer run.stop()
	run.registerSignals()
	for _, event := range input.InitialReady {
		if err := run.schedule(event); err != nil {
			return *state, temporal.NewNonRetryableApplicationError(
				"invalid initial ready event", "InvalidReadyEvent", err)
		}
	}
	run.await()
	return run.finish()
}

type workflowRun struct {
	ctx            workflow.Context
	input          WorkflowInput
	state          *WorkflowState
	activityCtx    workflow.Context
	cancelWork     workflow.CancelFunc
	cancelBase     workflow.CancelFunc
	selector       workflow.Selector
	ready          workflow.ReceiveChannel
	seen           map[string]struct{}
	expected       map[string]struct{}
	eventLimit     int
	inFlight       int
	parentSignals  int
	closing        bool
	sealed         bool
	cancelErr      error
	activities     []*pendingActivity
	requireFormula bool
}

type pendingActivity struct {
	event    ReadyEvent
	future   workflow.Future
	terminal bool
}

func newWorkflowRun(
	ctx workflow.Context,
	input WorkflowInput,
	state *WorkflowState,
	requireFormula bool,
) *workflowRun {
	activityBase, cancelBase := workflow.NewDisconnectedContext(ctx)
	activityCtx, cancelWork := workflow.WithCancel(activityBase)
	eventLimit := input.EventLimit
	if eventLimit == 0 {
		eventLimit = DefaultEventLimit
	}
	return &workflowRun{
		ctx: ctx, input: input, state: state, activityCtx: activityCtx,
		cancelWork:     cancelWork,
		cancelBase:     cancelBase,
		selector:       workflow.NewSelector(ctx),
		seen:           make(map[string]struct{}),
		expected:       make(map[string]struct{}),
		eventLimit:     eventLimit,
		closing:        input.CloseWhenIdle,
		requireFormula: requireFormula,
	}
}

func (r *workflowRun) registerSignals() {
	r.selector.AddReceive(r.ctx.Done(), func(workflow.ReceiveChannel, bool) {
		r.state.Phase = workflowPhaseFailed
		r.state.LastErrorCode = "workflow-canceled"
		r.cancelErr = r.ctx.Err()
		r.cancelWork()
	})
	r.ready = workflow.GetSignalChannel(r.ctx, SignalReady)
	r.selector.AddReceive(r.ready, func(channel workflow.ReceiveChannel, _ bool) {
		var event ReadyEvent
		channel.Receive(r.ctx, &event)
		r.acceptReady(event)
	})
	closeChannel := workflow.GetSignalChannel(r.ctx, SignalClose)
	r.selector.AddReceive(closeChannel, func(channel workflow.ReceiveChannel, _ bool) {
		var request CloseRequest
		channel.Receive(r.ctx, &request)
		if err := request.validatePayload(); err != nil ||
			request.CityID != r.input.CityID ||
			request.RunID != r.input.RunID ||
			(r.requireFormula && request.BeadID != r.input.BeadID) ||
			(!r.requireFormula &&
				request.BeadID != "" &&
				request.BeadID != r.input.BeadID) {
			r.fail("invalid-close-request")
			return
		}
		requested := eventIDSet(request.ExpectedEventIDs)
		if r.sealed {
			if !eventIDSetsEqual(r.expected, requested) {
				r.fail("invalid-close-request")
				return
			}
			r.drainReadySignals()
			return
		}
		r.expected = requested
		r.sealed = true
		r.closing = true
		if !r.seenEventsAreExpected() {
			r.fail("invalid-close-request")
			return
		}
		r.drainReadySignals()
	})
}

func (r *workflowRun) stop() {
	r.cancelWork()
	r.cancelBase()
}

func (r *workflowRun) acceptReady(event ReadyEvent) {
	if err := r.schedule(event); err != nil {
		r.fail("invalid-ready-event")
	}
}

func (r *workflowRun) fail(code string) {
	r.state.Phase = workflowPhaseFailed
	r.state.LastErrorCode = code
	r.closing = true
}

func (r *workflowRun) drainReadySignals() {
	for {
		var event ReadyEvent
		if !r.ready.ReceiveAsync(&event) {
			return
		}
		r.acceptReady(event)
	}
}

func (r *workflowRun) schedule(event ReadyEvent) error {
	if err := validateEventForRun(
		r.input.CityID,
		r.input.RunID,
		r.input.BeadID,
		event,
		r.requireFormula,
	); err != nil {
		return err
	}
	if r.sealed {
		if _, expected := r.expected[event.EventID]; !expected {
			return fmt.Errorf("ready event %s is outside the authoritative seal", event.EventID)
		}
	}
	if _, exists := r.seen[event.EventID]; exists {
		return nil
	}
	if len(r.seen) >= r.eventLimit {
		return fmt.Errorf("orchestration run exceeded event limit %d", r.eventLimit)
	}
	activityID := ""
	if r.requireFormula {
		var err error
		activityID, err = FormulaActivityID(event.Formula, event.Generation)
		if err != nil {
			return err
		}
		if err := workflow.UpsertMemo(r.ctx, FormulaMemo(event)); err != nil {
			return fmt.Errorf("upsert formula memo: %w", err)
		}
		if r.input.SearchAttributes {
			if err := workflow.UpsertTypedSearchAttributes(
				r.ctx,
				FormulaSearchAttributeUpdates(event)...,
			); err != nil {
				return fmt.Errorf("upsert formula search attributes: %w", err)
			}
		}
	}
	r.seen[event.EventID] = struct{}{}
	r.state.ReceivedEventIDs = append(r.state.ReceivedEventIDs, event.EventID)
	r.inFlight++
	activityCtx := workflow.WithActivityOptions(r.activityCtx, workflow.ActivityOptions{
		ActivityID:          activityID,
		TaskQueue:           AgentTaskQueue,
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    r.input.HeartbeatTimeout,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	})
	future := workflow.ExecuteActivity(
		activityCtx,
		ExecuteBeadActivityName,
		ActivityInput{Event: event},
	)
	pending := &pendingActivity{event: event, future: future}
	r.activities = append(r.activities, pending)
	r.selector.AddFuture(future, func(completed workflow.Future) {
		r.completeActivity(r.ctx, pending, completed)
	})
	return nil
}

func (r *workflowRun) completeActivity(
	ctx workflow.Context,
	pending *pendingActivity,
	completed workflow.Future,
) {
	if pending.terminal {
		return
	}
	pending.terminal = true
	r.inFlight--
	var result ActivityResult
	if err := completed.Get(ctx, &result); err != nil {
		if r.cancelErr == nil {
			r.state.FailedEventIDs = append(
				r.state.FailedEventIDs,
				pending.event.EventID,
			)
			r.state.LastErrorCode = "activity-failed"
			r.scheduleParentResult(
				pending.event,
				ChildWorkflowFailed,
				r.state.LastErrorCode,
			)
		}
		return
	}
	if result.EventID != pending.event.EventID ||
		result.Outcome != string(OutcomeCompleted) {
		if r.cancelErr == nil {
			r.state.FailedEventIDs = append(
				r.state.FailedEventIDs,
				pending.event.EventID,
			)
			r.state.LastErrorCode = "invalid-activity-result"
			r.scheduleParentResult(
				pending.event,
				ChildWorkflowFailed,
				r.state.LastErrorCode,
			)
		}
		return
	}
	r.state.CompletedEventIDs = append(
		r.state.CompletedEventIDs,
		pending.event.EventID,
	)
	r.scheduleParentResult(pending.event, ChildWorkflowCompleted, "")
}

func (r *workflowRun) scheduleParentResult(
	event ReadyEvent,
	status string,
	errorCode string,
) {
	if !r.requireFormula || event.Formula.ParentWorkflowID == "" {
		return
	}
	info := workflow.GetInfo(r.ctx)
	link, err := NewChildWorkflowLink(
		event,
		WorkflowReceipt{
			WorkflowID: info.WorkflowExecution.ID,
			RunID:      info.WorkflowExecution.RunID,
			EventID:    event.EventID,
		},
		status,
		errorCode,
	)
	if err != nil {
		r.fail("invalid-parent-link")
		return
	}
	future := workflow.SignalExternalWorkflow(
		r.activityCtx,
		event.Formula.ParentWorkflowID,
		event.Formula.ParentRunID,
		SignalParentChildLink,
		link,
	)
	r.parentSignals++
	r.selector.AddFuture(future, func(completed workflow.Future) {
		r.parentSignals--
		if err := completed.Get(r.ctx, nil); err != nil {
			r.fail("parent-signal-failed")
		}
	})
}

func (r *workflowRun) await() {
	for {
		if r.cancelErr != nil {
			r.awaitActivityCancellation()
			return
		}
		if r.state.Phase == workflowPhaseFailed &&
			r.inFlight == 0 &&
			r.parentSignals == 0 {
			return
		}
		if r.closing &&
			r.inFlight == 0 &&
			r.parentSignals == 0 &&
			r.receivedExpectedEvents() {
			r.drainReadySignals()
			if r.inFlight == 0 &&
				r.parentSignals == 0 &&
				r.receivedExpectedEvents() {
				return
			}
		}
		r.selector.Select(r.ctx)
	}
}

func (r *workflowRun) awaitActivityCancellation() {
	if r.inFlight == 0 {
		return
	}
	waitCtx, cancel := workflow.NewDisconnectedContext(r.ctx)
	defer cancel()
	selector := workflow.NewSelector(waitCtx)
	for _, pending := range r.activities {
		if pending.terminal {
			continue
		}
		current := pending
		selector.AddFuture(current.future, func(completed workflow.Future) {
			r.completeActivity(waitCtx, current, completed)
		})
	}
	for r.inFlight > 0 {
		selector.Select(waitCtx)
	}
}

func (r *workflowRun) receivedExpectedEvents() bool {
	for eventID := range r.expected {
		if _, exists := r.seen[eventID]; !exists {
			return false
		}
	}
	return true
}

func (r *workflowRun) seenEventsAreExpected() bool {
	for eventID := range r.seen {
		if _, expected := r.expected[eventID]; !expected {
			return false
		}
	}
	return true
}

func eventIDSet(eventIDs []string) map[string]struct{} {
	set := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		set[eventID] = struct{}{}
	}
	return set
}

func eventIDSetsEqual(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for eventID := range left {
		if _, exists := right[eventID]; !exists {
			return false
		}
	}
	return true
}

func (r *workflowRun) finish() (WorkflowState, error) {
	if r.cancelErr != nil {
		return cloneWorkflowState(*r.state), r.cancelErr
	}
	if r.state.Phase == workflowPhaseFailed || len(r.state.FailedEventIDs) > 0 {
		r.state.Phase = workflowPhaseFailed
		return cloneWorkflowState(*r.state), fmt.Errorf(
			"bead orchestration failed with code %s", r.state.LastErrorCode)
	}
	r.state.Phase = workflowPhaseCompleted
	return cloneWorkflowState(*r.state), nil
}

func cloneWorkflowState(state WorkflowState) WorkflowState {
	state.ReceivedEventIDs = append([]string(nil), state.ReceivedEventIDs...)
	state.CompletedEventIDs = append([]string(nil), state.CompletedEventIDs...)
	state.FailedEventIDs = append([]string(nil), state.FailedEventIDs...)
	return state
}
