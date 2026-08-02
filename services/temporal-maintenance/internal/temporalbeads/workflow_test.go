package temporalbeads

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestWorkflowDuplicateReadyDeliverySchedulesOneActivity(t *testing.T) {
	event := validReadyEvent(t)
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalReady, event)
		env.SignalWorkflow(SignalReady, event)
		env.SignalWorkflow(SignalClose, CloseRequest{
			ContractVersion:  CurrentContractVersion,
			CityID:           event.CityID,
			RunID:            event.RunID,
			BeadID:           event.BeadID,
			ExpectedEventIDs: []string{event.EventID},
			ReasonCode:       "test-complete",
		})
	}, time.Second)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		InitialReady:     []ReadyEvent{event},
		HeartbeatTimeout: time.Minute,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var state WorkflowState
	require.NoError(t, env.GetWorkflowResult(&state))
	require.Equal(t, "completed", state.Phase)
	require.Equal(t, []string{event.EventID}, state.ReceivedEventIDs)
	require.Equal(t, []string{event.EventID}, state.CompletedEventIDs)
	require.Equal(t, []ReadyEvent{event}, recorder.Events())
}

func TestWorkflowActivityInputSuppliesExactReadyEvent(t *testing.T) {
	event := validReadyEvent(t)
	event.Generation = 9
	event.EventID = readyEventID(event.CityID, event.RunID, event.BeadID, 9)
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		InitialReady:     []ReadyEvent{event},
		CloseWhenIdle:    true,
		HeartbeatTimeout: time.Minute,
	})

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []ReadyEvent{event}, recorder.Events())
}

func TestWorkflowMemoAndActivityIDExposeFormulaStep(t *testing.T) {
	event := validReadyEvent(t)
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)
	var activityID string
	env.SetOnActivityStartedListener(func(
		info *activity.Info,
		_ context.Context,
		_ converter.EncodedValues,
	) {
		activityID = info.ActivityID
	})
	env.OnUpsertMemo(FormulaMemo(event)).Return(nil).Once()
	env.OnUpsertTypedSearchAttributes(
		FormulaSearchAttributes(event),
	).Return(nil).Once()

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		InitialReady:     []ReadyEvent{event},
		CloseWhenIdle:    true,
		SearchAttributes: true,
		HeartbeatTimeout: time.Minute,
	})

	require.NoError(t, env.GetWorkflowError())
	expected, err := FormulaActivityID(event.Formula, event.Generation)
	require.NoError(t, err)
	require.Equal(t, expected, activityID)
}

func TestWorkflowSignalsTerminalResultToMaintenanceParent(t *testing.T) {
	event := validReadyEvent(t)
	event.Formula.ParentWorkflowID = "gascity-maintenance/gascity/dr-gst6"
	event.Formula.ParentRunID = "parent-run-1"
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)
	env.OnSignalExternalWorkflow(
		mock.Anything,
		event.Formula.ParentWorkflowID,
		event.Formula.ParentRunID,
		SignalParentChildLink,
		mock.MatchedBy(func(link ChildWorkflowLink) bool {
			return link.EventID == event.EventID &&
				link.BeadID == event.BeadID &&
				link.Status == ChildWorkflowCompleted
		}),
	).Return(nil).Once()

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		InitialReady:     []ReadyEvent{event},
		CloseWhenIdle:    true,
		HeartbeatTimeout: time.Minute,
	})

	require.NoError(t, env.GetWorkflowError())
}

func TestWorkflowDrainsReadySignalReceivedWithClose(t *testing.T) {
	event := validReadyEvent(t)
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalClose, CloseRequest{
			ContractVersion:  CurrentContractVersion,
			CityID:           event.CityID,
			RunID:            event.RunID,
			BeadID:           event.BeadID,
			ExpectedEventIDs: []string{event.EventID},
			ReasonCode:       "test-complete",
		})
		env.SignalWorkflow(SignalReady, event)
	}, time.Second)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		HeartbeatTimeout: time.Minute,
	})

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []ReadyEvent{event}, recorder.Events())
}

func TestWorkflowRejectsEventForDifferentRunWithoutScheduling(t *testing.T) {
	event := validReadyEvent(t)
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            "different-run",
		BeadID:           event.BeadID,
		InitialReady:     []ReadyEvent{event},
		CloseWhenIdle:    true,
		HeartbeatTimeout: time.Minute,
	})

	require.Error(t, env.GetWorkflowError())
	require.Empty(t, recorder.Events())
}

func TestWorkflowRejectsRunBeyondConfiguredEventLimit(t *testing.T) {
	first := validReadyEvent(t)
	second, err := NewReadyEvent(
		first.CityID,
		first.RunID,
		first.BeadID,
		2,
		first.Formula,
		first.ReadyAt,
	)
	require.NoError(t, err)
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           first.CityID,
		RunID:            first.RunID,
		BeadID:           first.BeadID,
		InitialReady:     []ReadyEvent{first, second},
		CloseWhenIdle:    true,
		HeartbeatTimeout: time.Minute,
		EventLimit:       1,
	})

	require.ErrorContains(t, env.GetWorkflowError(), "event limit")
	require.Empty(t, recorder.Events())
}

func TestWorkflowExitsFailedRunWhenExpectedEventIsMalformed(t *testing.T) {
	event := validReadyEvent(t)
	malformed := event
	malformed.Generation++
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalClose, CloseRequest{
			ContractVersion:  CurrentContractVersion,
			CityID:           event.CityID,
			RunID:            event.RunID,
			BeadID:           event.BeadID,
			ExpectedEventIDs: []string{event.EventID},
			ReasonCode:       "run-sealed",
		})
		env.SignalWorkflow(SignalReady, malformed)
	}, time.Second)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		HeartbeatTimeout: time.Minute,
	})

	require.ErrorContains(t, env.GetWorkflowError(), "invalid-ready-event")
	require.Empty(t, recorder.Events())
}

func TestWorkflowCancellationDoesNotHangWhileActivityStops(t *testing.T) {
	event := validReadyEvent(t)
	env := newWorkflowEnvironment(
		t,
		func(ctx context.Context, _ ActivityInput) (ActivityResult, error) {
			<-ctx.Done()
			return ActivityResult{}, ctx.Err()
		},
	)
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	env.SetTestTimeout(3 * time.Second)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		InitialReady:     []ReadyEvent{event},
		HeartbeatTimeout: time.Minute,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestWorkflowRejectsReadyEventOutsideAuthoritativeSeal(t *testing.T) {
	expected := validReadyEvent(t)
	unexpected, err := NewReadyEvent(
		expected.CityID,
		expected.RunID,
		expected.BeadID,
		2,
		expected.Formula,
		expected.ReadyAt,
	)
	require.NoError(t, err)
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalClose, CloseRequest{
			ContractVersion:  CurrentContractVersion,
			CityID:           expected.CityID,
			RunID:            expected.RunID,
			BeadID:           expected.BeadID,
			ExpectedEventIDs: []string{expected.EventID},
			ReasonCode:       "authoritative-seal",
		})
		env.SignalWorkflow(SignalReady, unexpected)
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalReady, expected)
	}, 2*time.Second)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           expected.CityID,
		RunID:            expected.RunID,
		BeadID:           expected.BeadID,
		HeartbeatTimeout: time.Minute,
	})

	require.Error(t, env.GetWorkflowError())
	require.Empty(t, recorder.Events())
}

func TestWorkflowRejectsSealOmittingPreviouslySeenEvent(t *testing.T) {
	expected := validReadyEvent(t)
	omitted, err := NewReadyEvent(
		expected.CityID,
		expected.RunID,
		expected.BeadID,
		2,
		expected.Formula,
		expected.ReadyAt,
	)
	require.NoError(t, err)
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalClose, CloseRequest{
			ContractVersion:  CurrentContractVersion,
			CityID:           expected.CityID,
			RunID:            expected.RunID,
			BeadID:           expected.BeadID,
			ExpectedEventIDs: []string{expected.EventID},
			ReasonCode:       "authoritative-seal",
		})
		env.SignalWorkflow(SignalReady, expected)
	}, time.Second)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           expected.CityID,
		RunID:            expected.RunID,
		BeadID:           expected.BeadID,
		InitialReady:     []ReadyEvent{omitted},
		HeartbeatTimeout: time.Minute,
	})

	require.Error(t, env.GetWorkflowError())
	require.Equal(t, []ReadyEvent{omitted}, recorder.Events())
}

func TestWorkflowRejectsChangedSealButAcceptsIdenticalRepeat(t *testing.T) {
	first := validReadyEvent(t)
	second, err := NewReadyEvent(
		first.CityID,
		first.RunID,
		first.BeadID,
		2,
		first.Formula,
		first.ReadyAt,
	)
	require.NoError(t, err)
	recorder := &activityRecorder{}
	env := newWorkflowEnvironment(t, recorder.Execute)
	firstSeal := CloseRequest{
		ContractVersion:  CurrentContractVersion,
		CityID:           first.CityID,
		RunID:            first.RunID,
		BeadID:           first.BeadID,
		ExpectedEventIDs: []string{first.EventID},
		ReasonCode:       "authoritative-seal",
	}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalClose, firstSeal)
		env.SignalWorkflow(SignalClose, firstSeal)
		changed := firstSeal
		changed.ExpectedEventIDs = []string{first.EventID, second.EventID}
		env.SignalWorkflow(SignalClose, changed)
		env.SignalWorkflow(SignalReady, first)
		env.SignalWorkflow(SignalReady, second)
	}, time.Second)

	env.ExecuteWorkflow(BeadOrchestrationWorkflow, WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           first.CityID,
		RunID:            first.RunID,
		HeartbeatTimeout: time.Minute,
	})

	require.Error(t, env.GetWorkflowError())
	require.Empty(t, recorder.Events())
}

func newWorkflowEnvironment(
	t *testing.T,
	execute func(context.Context, ActivityInput) (ActivityResult, error),
) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(
		BeadOrchestrationWorkflow,
		workflow.RegisterOptions{Name: BeadOrchestrationWorkflowName},
	)
	env.RegisterActivityWithOptions(
		execute,
		activity.RegisterOptions{Name: ExecuteBeadActivityName},
	)
	return env
}

type activityRecorder struct {
	mu     sync.Mutex
	events []ReadyEvent
}

func (r *activityRecorder) Execute(
	_ context.Context,
	input ActivityInput,
) (ActivityResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, input.Event)
	return ActivityResult{
		EventID:   input.Event.EventID,
		Outcome:   string(OutcomeCompleted),
		SessionID: "session-" + input.Event.EventID,
	}, nil
}

func (r *activityRecorder) Events() []ReadyEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ReadyEvent(nil), r.events...)
}
