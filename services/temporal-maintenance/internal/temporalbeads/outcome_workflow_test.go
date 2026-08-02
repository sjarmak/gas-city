package temporalbeads

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestCoordinatorOutcomeWorkflowRetriesTransportAndRequiresExplicitAck(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	activities := &outcomeActivityRecorder{
		failDeliveries: 1,
		fences:         []string{"mayor-session-1"},
	}
	env := newOutcomeWorkflowEnvironment(t, activities)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCoordinatorAcknowledged, OutcomeAcknowledgement{
			StoreRef:  envelope.StoreRef,
			OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
			CoordinatorFence: "mayor-session-1", AcknowledgedBy: "mayor",
		})
	}, 2*time.Second)

	env.ExecuteWorkflow(CoordinatorOutcomeWorkflow, CoordinatorOutcomeInput{
		Envelope:           envelope,
		RedeliveryInterval: time.Hour,
	})

	require.NoError(t, env.GetWorkflowError())
	var state CoordinatorOutcomeState
	require.NoError(t, env.GetWorkflowResult(&state))
	require.Equal(t, OutcomeCoordinatorAcknowledged, state.Phase)
	require.Equal(t, 2, activities.DeliveryAttempts())
	require.Equal(
		t,
		[]string{"cycle-000001", "cycle-000001"},
		activities.DeliveryCycles(),
	)
	require.Equal(t, 1, activities.Acknowledgements())
}

func TestCoordinatorOutcomeWorkflowStaysNeedsAckWhenSignalIsMissing(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	activities := &outcomeActivityRecorder{fences: []string{"mayor-session-1"}}
	env := newOutcomeWorkflowEnvironment(t, activities)
	var queried CoordinatorOutcomeState
	env.RegisterDelayedCallback(func() {
		values, err := env.QueryWorkflow(QueryCoordinatorOutcomeState)
		require.NoError(t, err)
		require.NoError(t, values.Get(&queried))
		env.CancelWorkflow()
	}, time.Minute)

	env.ExecuteWorkflow(CoordinatorOutcomeWorkflow, CoordinatorOutcomeInput{
		Envelope:           envelope,
		RedeliveryInterval: time.Hour,
	})

	require.Error(t, env.GetWorkflowError())
	require.Equal(t, OutcomeCoordinatorNeedsAck, queried.Phase)
	require.Equal(t, "mayor-session-1", queried.CoordinatorFence)
	require.True(t, queried.AcknowledgedAt.IsZero())
	require.Equal(t, 1, activities.DeliveryAttempts())
	require.Zero(t, activities.Acknowledgements())
}

func TestCoordinatorOutcomeWorkflowRedeliversAcrossCoordinatorFenceChange(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	activities := &outcomeActivityRecorder{
		fences: []string{"mayor-session-old", "mayor-session-new"},
	}
	env := newOutcomeWorkflowEnvironment(t, activities)
	env.RegisterDelayedCallback(func() {
		require.GreaterOrEqual(t, activities.DeliveryAttempts(), 2)
		require.Equal(t, "mayor-session-new", activities.LastFence())
		env.SignalWorkflow(SignalCoordinatorAcknowledged, OutcomeAcknowledgement{
			StoreRef:  envelope.StoreRef,
			OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
			CoordinatorFence: "mayor-session-old", AcknowledgedBy: "mayor",
		})
	}, 3*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCoordinatorAcknowledged, OutcomeAcknowledgement{
			StoreRef:  envelope.StoreRef,
			OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
			CoordinatorFence: "mayor-session-new", AcknowledgedBy: "mayor",
		})
	}, 4*time.Second)

	env.ExecuteWorkflow(CoordinatorOutcomeWorkflow, CoordinatorOutcomeInput{
		Envelope:           envelope,
		RedeliveryInterval: 2 * time.Second,
	})

	require.NoError(t, env.GetWorkflowError())
	var state CoordinatorOutcomeState
	require.NoError(t, env.GetWorkflowResult(&state))
	require.Equal(t, OutcomeCoordinatorAcknowledged, state.Phase)
	require.Equal(t, "mayor-session-new", state.CoordinatorFence)
	require.Equal(t, 1, state.StaleAcknowledgements)
	require.GreaterOrEqual(t, activities.DeliveryAttempts(), 2)
	require.Equal(
		t,
		[]string{"cycle-000001", "cycle-000002"},
		activities.DeliveryCycles()[:2],
	)
	require.Equal(t, 1, activities.Acknowledgements())
}

func TestCoordinatorOutcomeWorkflowDeduplicatesReadySignal(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	activities := &outcomeActivityRecorder{fences: []string{"mayor-session-1"}}
	env := newOutcomeWorkflowEnvironment(t, activities)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalOutcomeReady, envelope)
		env.SignalWorkflow(SignalOutcomeReady, envelope)
		env.SignalWorkflow(SignalCoordinatorAcknowledged, OutcomeAcknowledgement{
			StoreRef:  envelope.StoreRef,
			OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
			CoordinatorFence: "mayor-session-1", AcknowledgedBy: "mayor",
		})
	}, time.Second)

	env.ExecuteWorkflow(CoordinatorOutcomeWorkflow, CoordinatorOutcomeInput{
		Envelope:           envelope,
		RedeliveryInterval: time.Hour,
	})

	require.NoError(t, env.GetWorkflowError())
	var state CoordinatorOutcomeState
	require.NoError(t, env.GetWorkflowResult(&state))
	require.Equal(t, 2, state.DuplicateSignals)
	require.Equal(t, 1, activities.DeliveryAttempts())
}

func TestCoordinatorOutcomeWorkflowUsesNewAckActivityIDAfterRetryExhaustion(
	t *testing.T,
) {
	envelope := validOutcomeEnvelope(t)
	activities := &outcomeActivityRecorder{
		fences:               []string{"mayor-session-1"},
		failAcknowledgements: 5,
	}
	env := newOutcomeWorkflowEnvironment(t, activities)
	var ackActivityIDs []string
	env.SetOnActivityStartedListener(func(
		info *activity.Info,
		_ context.Context,
		_ converter.EncodedValues,
	) {
		if info.ActivityType.Name == AcknowledgeCoordinatorOutcomeActivityName {
			ackActivityIDs = append(ackActivityIDs, info.ActivityID)
		}
	})
	ack := OutcomeAcknowledgement{
		StoreRef:  envelope.StoreRef,
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		CoordinatorFence: "mayor-session-1", AcknowledgedBy: "mayor",
	}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCoordinatorAcknowledged, ack)
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCoordinatorAcknowledged, ack)
	}, 20*time.Second)

	env.ExecuteWorkflow(CoordinatorOutcomeWorkflow, CoordinatorOutcomeInput{
		Envelope: envelope, RedeliveryInterval: 2 * time.Second,
	})
	require.NoError(t, env.GetWorkflowError())
	require.Contains(
		t,
		ackActivityIDs,
		"ack/"+envelope.OutcomeID+"/cycle-000001",
	)
	require.NotEqual(
		t,
		"ack/"+envelope.OutcomeID+"/cycle-000001",
		ackActivityIDs[len(ackActivityIDs)-1],
	)
	require.Regexp(
		t,
		"^ack/"+envelope.OutcomeID+"/cycle-[0-9]{6}$",
		ackActivityIDs[len(ackActivityIDs)-1],
	)
}

func TestCoordinatorOutcomeWorkflowContinuesAsNewWithStateAndCounters(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	activities := &outcomeActivityRecorder{
		fences: []string{"mayor-session-1"},
	}
	env := newOutcomeWorkflowEnvironment(t, activities)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalOutcomeReady, envelope)
		env.SignalWorkflow(SignalCoordinatorAcknowledged, OutcomeAcknowledgement{
			StoreRef:  envelope.StoreRef,
			OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
			CoordinatorFence: "stale-fence", AcknowledgedBy: "mayor",
		})
	}, 500*time.Millisecond)
	env.ExecuteWorkflow(CoordinatorOutcomeWorkflow, CoordinatorOutcomeInput{
		Envelope: envelope, RedeliveryInterval: time.Second,
		ContinueAsNewAfter: 2,
	})
	next := continuedOutcomeInput(t, env.GetWorkflowError())
	require.NotNil(t, next.ResumeState)
	require.Equal(t, 1, next.ResumeState.ContinueAsNewCount)
	require.Equal(t, 2, next.ResumeState.DeliveryAttempts)
	require.Equal(t, 1, next.ResumeState.DuplicateSignals)
	require.Equal(t, 1, next.ResumeState.StaleAcknowledgements)

	nextEnv := newOutcomeWorkflowEnvironment(t, activities)
	nextEnv.RegisterDelayedCallback(func() {
		nextEnv.SignalWorkflow(
			SignalCoordinatorAcknowledged,
			OutcomeAcknowledgement{
				StoreRef:  envelope.StoreRef,
				OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
				CoordinatorFence: "mayor-session-1", AcknowledgedBy: "mayor",
			},
		)
	}, 100*time.Millisecond)
	nextEnv.ExecuteWorkflow(CoordinatorOutcomeWorkflow, next)
	require.NoError(t, nextEnv.GetWorkflowError())
	var state CoordinatorOutcomeState
	require.NoError(t, nextEnv.GetWorkflowResult(&state))
	require.Equal(t, OutcomeCoordinatorAcknowledged, state.Phase)
	require.Equal(t, 1, state.ContinueAsNewCount)
	require.GreaterOrEqual(t, state.DeliveryAttempts, 3)
	require.Equal(t, 1, state.DuplicateSignals)
	require.Equal(t, 1, state.StaleAcknowledgements)
}

func TestCoordinatorOutcomeWorkflowDoesNotLoseAcknowledgementAtHistoryBoundary(
	t *testing.T,
) {
	envelope := validOutcomeEnvelope(t)
	activities := &outcomeActivityRecorder{
		fences: []string{"mayor-session-1", "mayor-session-new"},
	}
	env := newOutcomeWorkflowEnvironment(t, activities)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCoordinatorAcknowledged, OutcomeAcknowledgement{
			StoreRef:  envelope.StoreRef,
			OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
			CoordinatorFence: "mayor-session-1", AcknowledgedBy: "mayor",
		})
	}, time.Second)

	input := CoordinatorOutcomeInput{
		Envelope: envelope, RedeliveryInterval: time.Second,
		ContinueAsNewAfter: 1,
	}
	env.ExecuteWorkflow(CoordinatorOutcomeWorkflow, input)
	input = continuedOutcomeInput(t, env.GetWorkflowError())
	attemptsAtBoundary := activities.DeliveryAttempts()
	env = newOutcomeWorkflowEnvironment(t, activities)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(
			SignalCoordinatorAcknowledged,
			OutcomeAcknowledgement{
				StoreRef:  envelope.StoreRef,
				OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
				CoordinatorFence: "mayor-session-1",
				AcknowledgedBy:   "mayor",
			},
		)
	}, 0)
	env.ExecuteWorkflow(CoordinatorOutcomeWorkflow, input)
	require.NoError(t, env.GetWorkflowError())
	var state CoordinatorOutcomeState
	require.NoError(t, env.GetWorkflowResult(&state))
	require.Equal(t, OutcomeCoordinatorAcknowledged, state.Phase)
	require.Equal(t, 1, activities.Acknowledgements())
	require.Equal(t, attemptsAtBoundary, activities.DeliveryAttempts())
	require.Equal(t, "mayor-session-1", state.CoordinatorFence)
}

func continuedOutcomeInput(
	t *testing.T,
	err error,
) CoordinatorOutcomeInput {
	t.Helper()
	require.Error(t, err)
	var continued *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continued)
	var input CoordinatorOutcomeInput
	require.NoError(
		t,
		converter.GetDefaultDataConverter().FromPayloads(
			continued.Input,
			&input,
		),
	)
	return input
}

func TestCoordinatorOutcomeWorkflowIDIsStable(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	first, err := CoordinatorOutcomeWorkflowID(envelope)
	require.NoError(t, err)
	second, err := CoordinatorOutcomeWorkflowID(envelope)
	require.NoError(t, err)
	require.Equal(t, "coordinator-outcome/"+envelope.OutcomeID, first)
	require.Equal(t, first, second)
}

func TestCoordinatorOutcomeGenerationsCompleteAsDistinctWorkflowHistories(
	t *testing.T,
) {
	firstInput := validOutcomeAdapterInput()
	first, err := DirectWorkerOutcome(firstInput)
	require.NoError(t, err)
	nextInput := firstInput
	nextInput.Fence.ProducerID = "different-project-lead-session"
	nextInput.Fence.Generation = 3
	nextInput.Fence.Token = "generation-three-token"
	nextInput.OccurredAt = nextInput.OccurredAt.Add(time.Minute)
	next, err := ProjectLeadOutcome(nextInput)
	require.NoError(t, err)

	firstWorkflowID, err := CoordinatorOutcomeWorkflowID(first)
	require.NoError(t, err)
	nextWorkflowID, err := CoordinatorOutcomeWorkflowID(next)
	require.NoError(t, err)
	require.NotEqual(t, firstWorkflowID, nextWorkflowID)

	runToAcknowledged := func(envelope OutcomeReady) CoordinatorOutcomeState {
		activities := &outcomeActivityRecorder{
			fences: []string{"mayor-session"},
		}
		env := newOutcomeWorkflowEnvironment(t, activities)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(
				SignalCoordinatorAcknowledged,
				OutcomeAcknowledgement{
					StoreRef:         envelope.StoreRef,
					OutcomeID:        envelope.OutcomeID,
					WorkID:           envelope.WorkID,
					CoordinatorFence: "mayor-session",
					AcknowledgedBy:   "mayor",
				},
			)
		}, time.Second)
		env.ExecuteWorkflow(
			CoordinatorOutcomeWorkflow,
			CoordinatorOutcomeInput{
				Envelope:           envelope,
				RedeliveryInterval: time.Hour,
			},
		)
		require.NoError(t, env.GetWorkflowError())
		var state CoordinatorOutcomeState
		require.NoError(t, env.GetWorkflowResult(&state))
		require.Equal(t, OutcomeCoordinatorAcknowledged, state.Phase)
		require.Equal(t, envelope.OutcomeID, state.Envelope.OutcomeID)
		return state
	}

	firstHistory := runToAcknowledged(first)
	nextHistory := runToAcknowledged(next)
	require.NotEqual(
		t,
		firstHistory.Envelope.OutcomeID,
		nextHistory.Envelope.OutcomeID,
	)
}

func newOutcomeWorkflowEnvironment(
	t *testing.T,
	recorder *outcomeActivityRecorder,
) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(
		CoordinatorOutcomeWorkflow,
		workflow.RegisterOptions{Name: CoordinatorOutcomeWorkflowName},
	)
	env.RegisterActivityWithOptions(
		recorder.Deliver,
		activity.RegisterOptions{Name: DeliverCoordinatorOutcomeActivityName},
	)
	env.RegisterActivityWithOptions(
		recorder.Acknowledge,
		activity.RegisterOptions{Name: AcknowledgeCoordinatorOutcomeActivityName},
	)
	return env
}

type outcomeActivityRecorder struct {
	mu                   sync.Mutex
	failDeliveries       int
	fences               []string
	deliveryAttempts     int
	deliveryCycles       []string
	acknowledgements     int
	failAcknowledgements int
	lastFence            string
}

func (r *outcomeActivityRecorder) Deliver(
	_ context.Context,
	request OutcomeDeliveryRequest,
) (OutcomeDelivery, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := r.deliveryAttempts
	r.deliveryAttempts++
	r.deliveryCycles = append(r.deliveryCycles, request.DeliveryCycle)
	if index < r.failDeliveries {
		return OutcomeDelivery{}, errors.New("coordinator session unavailable")
	}
	fenceIndex := index - r.failDeliveries
	if fenceIndex >= len(r.fences) {
		fenceIndex = len(r.fences) - 1
	}
	r.lastFence = r.fences[fenceIndex]
	return OutcomeDelivery{
		OutcomeID: request.Envelope.OutcomeID, WorkID: request.Envelope.WorkID,
		DeliveryRef:      "local:delivery",
		CoordinatorFence: r.fences[fenceIndex],
	}, nil
}

func (r *outcomeActivityRecorder) Acknowledge(
	_ context.Context,
	ack OutcomeAcknowledgement,
) (OutcomeRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acknowledgements++
	if r.acknowledgements <= r.failAcknowledgements {
		return OutcomeRecord{}, errors.New("canonical acknowledgement unavailable")
	}
	return OutcomeRecord{
		Envelope: OutcomeReady{OutcomeID: ack.OutcomeID, WorkID: ack.WorkID},
		State:    OutcomeCoordinatorAcknowledged,
	}, nil
}

func (r *outcomeActivityRecorder) DeliveryAttempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.deliveryAttempts
}

func (r *outcomeActivityRecorder) DeliveryCycles() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.deliveryCycles...)
}

func (r *outcomeActivityRecorder) Acknowledgements() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acknowledgements
}

func (r *outcomeActivityRecorder) LastFence() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastFence
}

func validOutcomeEnvelope(t *testing.T) OutcomeReady {
	t.Helper()
	envelope, err := FormulaStepOutcome(validOutcomeAdapterInput())
	require.NoError(t, err)
	return envelope
}

func TestCoordinatorOutcomeActivityUsesCanonicalStore(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	store := &recordingOutcomeStore{}
	notifier := &recordingOutcomeNotifier{
		receipt: OutcomeNotificationReceipt{
			DeliveryRef:      "local:delivery",
			CoordinatorFence: "mayor-session-1",
		},
	}
	activities := CoordinatorOutcomeActivities{
		Store: store, Notifier: notifier, StoreRef: envelope.StoreRef,
	}

	delivery, err := activities.Deliver(
		context.Background(),
		OutcomeDeliveryRequest{
			Envelope: envelope, DeliveryCycle: "cycle-000001",
		},
	)
	require.NoError(t, err)
	require.Equal(t, envelope.OutcomeID, delivery.OutcomeID)
	require.Equal(t, delivery, store.delivery)

	ack := OutcomeAcknowledgement{
		StoreRef:  envelope.StoreRef,
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		CoordinatorFence: delivery.CoordinatorFence, AcknowledgedBy: "mayor",
	}
	_, err = activities.Acknowledge(context.Background(), ack)
	require.NoError(t, err)
	require.Equal(t, ack, store.ack)
}

func TestCoordinatorOutcomeActivityFailsClosedAtDeliveryBoundary(t *testing.T) {
	ctx := context.Background()
	envelope := validOutcomeEnvelope(t)
	request := OutcomeDeliveryRequest{
		Envelope: envelope, DeliveryCycle: "cycle-000001",
	}

	_, err := (&CoordinatorOutcomeActivities{}).Deliver(ctx, request)
	require.ErrorContains(t, err, "store is required")
	_, err = (&CoordinatorOutcomeActivities{
		Store: &recordingOutcomeStore{},
	}).Deliver(ctx, request)
	require.ErrorContains(t, err, "notifier is required")
	_, err = (&CoordinatorOutcomeActivities{
		Store: &recordingOutcomeStore{}, Notifier: &recordingOutcomeNotifier{},
		StoreRef: "invalid",
	}).Deliver(ctx, request)
	require.ErrorContains(t, err, "configured outcome store ref")
	_, err = (&CoordinatorOutcomeActivities{
		Store: &recordingOutcomeStore{}, Notifier: &recordingOutcomeNotifier{},
		StoreRef: envelope.StoreRef,
	}).Deliver(ctx, OutcomeDeliveryRequest{})
	require.Error(t, err)

	_, err = (&CoordinatorOutcomeActivities{
		Store: &recordingOutcomeStore{},
		Notifier: failingOutcomeNotifier{
			err: errors.New("injected notifier failure"),
		},
		StoreRef: envelope.StoreRef,
	}).Deliver(ctx, request)
	require.ErrorContains(t, err, "injected notifier failure")

	_, err = (&CoordinatorOutcomeActivities{
		Store: &recordingOutcomeStore{},
		Notifier: &recordingOutcomeNotifier{
			receipt: OutcomeNotificationReceipt{
				CoordinatorFence: "mayor-session-1",
			},
		},
		StoreRef: envelope.StoreRef,
	}).Deliver(ctx, request)
	require.ErrorContains(t, err, "delivery ref")

	_, err = (&CoordinatorOutcomeActivities{
		Store: failingOutcomeStore{
			OutcomeStore: &recordingOutcomeStore{},
			deliveryErr:  errors.New("injected delivery write failure"),
		},
		Notifier: &recordingOutcomeNotifier{
			receipt: OutcomeNotificationReceipt{
				DeliveryRef:      "local:delivery",
				CoordinatorFence: "mayor-session-1",
			},
		},
		StoreRef: envelope.StoreRef,
	}).Deliver(ctx, request)
	require.ErrorContains(t, err, "injected delivery write failure")
}

func TestCoordinatorOutcomeActivityFailsClosedAtAcknowledgementBoundary(
	t *testing.T,
) {
	ctx := context.Background()
	envelope := validOutcomeEnvelope(t)
	ack := OutcomeAcknowledgement{
		StoreRef:  envelope.StoreRef,
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		CoordinatorFence: "mayor-session-1", AcknowledgedBy: "mayor",
	}
	_, err := (&CoordinatorOutcomeActivities{}).Acknowledge(ctx, ack)
	require.ErrorContains(t, err, "store is required")
	_, err = (&CoordinatorOutcomeActivities{
		Store: &recordingOutcomeStore{}, StoreRef: "invalid",
	}).Acknowledge(ctx, ack)
	require.ErrorContains(t, err, "configured outcome store ref")
	_, err = (&CoordinatorOutcomeActivities{
		Store: &recordingOutcomeStore{}, StoreRef: envelope.StoreRef,
	}).Acknowledge(ctx, OutcomeAcknowledgement{})
	require.Error(t, err)

	wrongStore := ack
	wrongStore.StoreRef = "rig:other"
	_, err = (&CoordinatorOutcomeActivities{
		Store: &recordingOutcomeStore{}, StoreRef: envelope.StoreRef,
	}).Acknowledge(ctx, wrongStore)
	require.ErrorContains(t, err, "does not match")

	_, err = (&CoordinatorOutcomeActivities{
		Store: failingOutcomeStore{
			OutcomeStore: &recordingOutcomeStore{},
			ackErr:       errors.New("injected acknowledgement write failure"),
		},
		StoreRef: envelope.StoreRef,
	}).Acknowledge(ctx, ack)
	require.ErrorContains(t, err, "injected acknowledgement write failure")
}

type recordingOutcomeNotifier struct {
	receipt OutcomeNotificationReceipt
	calls   int
}

func (n *recordingOutcomeNotifier) Deliver(
	context.Context,
	OutcomeDeliveryRequest,
) (OutcomeNotificationReceipt, error) {
	n.calls++
	return n.receipt, nil
}

type failingOutcomeNotifier struct {
	err error
}

func (n failingOutcomeNotifier) Deliver(
	context.Context,
	OutcomeDeliveryRequest,
) (OutcomeNotificationReceipt, error) {
	return OutcomeNotificationReceipt{}, n.err
}

type failingOutcomeStore struct {
	OutcomeStore
	deliveryErr error
	ackErr      error
}

func (s failingOutcomeStore) MarkOutcomeDelivered(
	context.Context,
	OutcomeDelivery,
) (OutcomeRecord, error) {
	return OutcomeRecord{}, s.deliveryErr
}

func (s failingOutcomeStore) MarkOutcomeAcknowledged(
	context.Context,
	OutcomeAcknowledgement,
) (OutcomeRecord, error) {
	return OutcomeRecord{}, s.ackErr
}

type recordingOutcomeStore struct {
	delivery   OutcomeDelivery
	ack        OutcomeAcknowledgement
	pending    []OutcomeRecord
	discovered []OutcomeReady
	emitted    []string
	emitErrors map[string]error
}

func (s *recordingOutcomeStore) EmitOutcome(
	_ context.Context,
	envelope OutcomeReady,
) (OutcomeRecord, error) {
	if err := s.emitErrors[envelope.WorkID]; err != nil {
		return OutcomeRecord{}, err
	}
	s.emitted = append(s.emitted, envelope.WorkID)
	return OutcomeRecord{}, nil
}

func (s *recordingOutcomeStore) DiscoverLegacyOutcomes(
	context.Context,
) ([]OutcomeReady, error) {
	return append([]OutcomeReady(nil), s.discovered...), nil
}

func (s *recordingOutcomeStore) PendingOutcomes(context.Context) ([]OutcomeRecord, error) {
	return append([]OutcomeRecord(nil), s.pending...), nil
}

func (s *recordingOutcomeStore) MarkOutcomeDelivered(
	_ context.Context,
	delivery OutcomeDelivery,
) (OutcomeRecord, error) {
	s.delivery = delivery
	return OutcomeRecord{
		Envelope: OutcomeReady{
			OutcomeID: delivery.OutcomeID,
			WorkID:    delivery.WorkID,
		},
		State:            OutcomeCoordinatorNeedsAck,
		DeliveryRef:      delivery.DeliveryRef,
		CoordinatorFence: delivery.CoordinatorFence,
	}, nil
}

func (s *recordingOutcomeStore) MarkOutcomeAcknowledged(
	_ context.Context,
	ack OutcomeAcknowledgement,
) (OutcomeRecord, error) {
	s.ack = ack
	return OutcomeRecord{State: OutcomeCoordinatorAcknowledged}, nil
}

func (*recordingOutcomeStore) InspectOutcome(context.Context, string) (OutcomeRecord, error) {
	return OutcomeRecord{}, nil
}

func (*recordingOutcomeStore) FindSilentOutcomes(context.Context) ([]SilentOutcome, error) {
	return nil, nil
}

func TestCoordinatorOutcomeActivityExposesAttemptMetadata(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	env := newOutcomeWorkflowEnvironment(t, &outcomeActivityRecorder{
		fences: []string{"mayor-session-1"},
	})
	var activityID string
	env.SetOnActivityStartedListener(func(
		info *activity.Info,
		_ context.Context,
		_ converter.EncodedValues,
	) {
		if info.ActivityType.Name == DeliverCoordinatorOutcomeActivityName {
			activityID = info.ActivityID
		}
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCoordinatorAcknowledged, OutcomeAcknowledgement{
			StoreRef:  envelope.StoreRef,
			OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
			CoordinatorFence: "mayor-session-1", AcknowledgedBy: "mayor",
		})
	}, time.Second)
	env.ExecuteWorkflow(CoordinatorOutcomeWorkflow, CoordinatorOutcomeInput{
		Envelope: envelope, RedeliveryInterval: time.Hour,
	})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(
		t,
		"deliver/"+envelope.OutcomeID+"/cycle-000001",
		activityID,
	)
}
