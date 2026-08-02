package temporalbeads

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"
)

func TestCoordinatorOutcomeRuntimeConfigFailsClosed(t *testing.T) {
	err := ValidateCoordinatorOutcomeRuntimeConfig(CoordinatorOutcomeRuntimeConfig{})
	require.ErrorContains(t, err, "store")

	err = ValidateCoordinatorOutcomeRuntimeConfig(CoordinatorOutcomeRuntimeConfig{
		Store: &recordingOutcomeStore{},
	})
	require.ErrorContains(t, err, "notifier")

	err = ValidateCoordinatorOutcomeRuntimeConfig(CoordinatorOutcomeRuntimeConfig{
		Store: &recordingOutcomeStore{}, Notifier: &recordingOutcomeNotifier{},
	})
	require.ErrorContains(t, err, "store ref")

	err = ValidateCoordinatorOutcomeRuntimeConfig(CoordinatorOutcomeRuntimeConfig{
		Store: &recordingOutcomeStore{}, Notifier: &recordingOutcomeNotifier{},
		StoreRef: "city:ds-research",
	})
	require.ErrorContains(t, err, "reconcile")

	err = ValidateCoordinatorOutcomeRuntimeConfig(CoordinatorOutcomeRuntimeConfig{
		Store: &recordingOutcomeStore{}, Notifier: &recordingOutcomeNotifier{},
		StoreRef:          "city:ds-research",
		ReconcileInterval: time.Second, RedeliveryInterval: -time.Second,
		WorkerStopTimeout: time.Second,
	})
	require.ErrorContains(t, err, "redelivery")

	err = ValidateCoordinatorOutcomeRuntimeConfig(CoordinatorOutcomeRuntimeConfig{
		Store: &recordingOutcomeStore{}, Notifier: &recordingOutcomeNotifier{},
		StoreRef:          "city:ds-research",
		ReconcileInterval: time.Second, RedeliveryInterval: time.Minute,
	})
	require.ErrorContains(t, err, "stop timeout")

	require.NoError(t, ValidateCoordinatorOutcomeRuntimeConfig(
		CoordinatorOutcomeRuntimeConfig{
			Store: &recordingOutcomeStore{}, Notifier: &recordingOutcomeNotifier{},
			StoreRef:          "city:ds-research",
			ReconcileInterval: time.Second, RedeliveryInterval: time.Minute,
			WorkerStopTimeout: time.Second,
		},
	))
}

func TestCoordinatorOutcomeRuntimeConstructionFailsClosedAndRegistersWorker(
	t *testing.T,
) {
	_, err := NewCoordinatorOutcomeRuntime(nil, CoordinatorOutcomeRuntimeConfig{})
	require.ErrorContains(t, err, "Temporal client")

	lazyClient, err := temporalclient.NewLazyClient(temporalclient.Options{})
	require.NoError(t, err)
	defer lazyClient.Close()
	_, err = NewCoordinatorOutcomeRuntime(lazyClient, CoordinatorOutcomeRuntimeConfig{})
	require.ErrorContains(t, err, "store")

	runtime, err := NewCoordinatorOutcomeRuntime(
		lazyClient,
		CoordinatorOutcomeRuntimeConfig{
			Store: &recordingOutcomeStore{}, Notifier: &recordingOutcomeNotifier{},
			StoreRef:           "city:ds-research",
			ReconcileInterval:  time.Second,
			RedeliveryInterval: time.Minute,
			WorkerStopTimeout:  time.Second,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, runtime.worker)
	require.NotNil(t, runtime.reconciler)
	require.Equal(t, time.Second, runtime.reconcileInterval)
}

func TestCoordinatorOutcomeRuntimeLifecycleIsIdempotentAndBounded(t *testing.T) {
	store := &recordingOutcomeStore{}
	gateway := &recordingOutcomeGateway{}
	results := make(chan error, 4)
	newRuntime := func(worker temporalworker.Worker) *CoordinatorOutcomeRuntime {
		return &CoordinatorOutcomeRuntime{
			worker: worker,
			reconciler: &CoordinatorOutcomeReconciler{
				Source: store,
				Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
			},
			reconcileInterval: time.Millisecond,
			onResult: func(_ OutcomeReconcileResult, err error) {
				results <- err
			},
		}
	}

	empty := newRuntime(nil)
	require.NoError(t, empty.Stop(context.Background()))
	require.ErrorContains(t, empty.Start(context.Background()), "worker")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	notStarted := &stubOutcomeWorker{}
	require.ErrorIs(t, newRuntime(notStarted).Start(cancelled), context.Canceled)
	require.Zero(t, notStarted.starts)

	startFailure := &stubOutcomeWorker{startErr: errors.New("start failed")}
	require.ErrorContains(
		t,
		newRuntime(startFailure).Start(context.Background()),
		"start failed",
	)
	require.Equal(t, 1, startFailure.starts)

	cancelDuringStart, cancelDuring := context.WithCancel(context.Background())
	interrupted := &stubOutcomeWorker{onStart: cancelDuring}
	require.ErrorIs(
		t,
		newRuntime(interrupted).Start(cancelDuringStart),
		context.Canceled,
	)
	require.Equal(t, 1, interrupted.stops)

	startedWorker := &stubOutcomeWorker{}
	started := newRuntime(startedWorker)
	require.NoError(t, started.Start(context.Background()))
	require.NoError(t, started.Start(context.Background()))
	require.Equal(t, 1, startedWorker.starts)
	select {
	case err := <-results:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reconcile loop did not run")
	}
	require.NoError(t, started.Stop(context.Background()))
	require.NoError(t, started.Stop(context.Background()))
	require.Equal(t, 1, startedWorker.stops)
}

func TestCoordinatorOutcomeRuntimeStopHonorsCallerDeadline(t *testing.T) {
	loopContext, cancelLoop := context.WithCancel(context.Background())
	runtime := &CoordinatorOutcomeRuntime{
		cancel: cancelLoop,
		done:   make(chan struct{}),
	}
	stopContext, cancelStop := context.WithCancel(context.Background())
	cancelStop()
	err := runtime.Stop(stopContext)
	require.ErrorContains(t, err, "stop coordinator outcome reconcile loop")
	require.ErrorIs(t, err, context.Canceled)
	require.Error(t, loopContext.Err())
}

type stubOutcomeWorker struct {
	temporalworker.Worker
	startErr error
	onStart  func()
	starts   int
	stops    int
}

func (w *stubOutcomeWorker) Start() error {
	w.starts++
	if w.onStart != nil {
		w.onStart()
	}
	return w.startErr
}

func (w *stubOutcomeWorker) Stop() {
	w.stops++
}

func TestCoordinatorOutcomeRuntimeReconcileResultIsObservable(t *testing.T) {
	var observed OutcomeReconcileResult
	var observedErr error
	runtime := &CoordinatorOutcomeRuntime{
		reconciler: &CoordinatorOutcomeReconciler{
			Source: &recordingOutcomeStore{},
			Bridge: CoordinatorOutcomeBridge{Temporal: &recordingOutcomeGateway{}},
		},
		onResult: func(result OutcomeReconcileResult, err error) {
			observed, observedErr = result, err
		},
	}
	runtime.reconcileOnce(context.Background())
	require.NoError(t, observedErr)
	require.Equal(t, OutcomeReconcileResult{}, observed)
}

func TestStoreSpecificOutcomeQueuesCannotCrossWrite(t *testing.T) {
	city := newIsolatedDoltBeadStore(t)
	rig := newIsolatedDoltBeadStore(t)
	ctx := context.Background()

	cityInput := validOutcomeAdapterInput()
	cityInput.WorkID = city.beadID
	cityInput.StoreRef = "city:ds-research"
	cityEnvelope, err := DirectWorkerOutcome(cityInput)
	require.NoError(t, err)
	_, err = city.store.EmitOutcome(ctx, cityEnvelope)
	require.NoError(t, err)

	rigInput := validOutcomeAdapterInput()
	rigInput.WorkID = rig.beadID
	rigInput.StoreRef = "rig:codeprobe"
	rigEnvelope, err := DirectWorkerOutcome(rigInput)
	require.NoError(t, err)
	_, err = rig.store.EmitOutcome(ctx, rigEnvelope)
	require.NoError(t, err)

	cityQueue, err := CoordinatorOutcomeTaskQueueForStore(cityInput.StoreRef)
	require.NoError(t, err)
	rigQueue, err := CoordinatorOutcomeTaskQueueForStore(rigInput.StoreRef)
	require.NoError(t, err)
	require.NotEqual(t, cityQueue, rigQueue)

	cityNotifier := &recordingOutcomeNotifier{
		receipt: OutcomeNotificationReceipt{
			DeliveryRef: "local:city", CoordinatorFence: "mayor-city",
		},
	}
	cityActivities := CoordinatorOutcomeActivities{
		Store:    city.store,
		Notifier: cityNotifier,
		StoreRef: cityInput.StoreRef,
	}
	_, err = cityActivities.Deliver(ctx, OutcomeDeliveryRequest{
		Envelope: rigEnvelope, DeliveryCycle: "cycle-000001",
	})
	require.ErrorContains(t, err, "store ref")
	require.Zero(t, cityNotifier.calls)
	cityRecord, err := city.store.InspectOutcome(ctx, city.beadID)
	require.NoError(t, err)
	require.Equal(t, OutcomeCoordinatorPending, cityRecord.State)

	wrongAck := OutcomeAcknowledgement{
		StoreRef:         rigInput.StoreRef,
		OutcomeID:        cityEnvelope.OutcomeID,
		WorkID:           cityEnvelope.WorkID,
		CoordinatorFence: "mayor-city",
		AcknowledgedBy:   "mayor",
	}
	_, err = cityActivities.Acknowledge(ctx, wrongAck)
	require.ErrorContains(t, err, "store ref")
	cityRecord, err = city.store.InspectOutcome(ctx, city.beadID)
	require.NoError(t, err)
	require.Equal(t, OutcomeCoordinatorPending, cityRecord.State)

	rigActivities := CoordinatorOutcomeActivities{
		Store: rig.store,
		Notifier: &recordingOutcomeNotifier{
			receipt: OutcomeNotificationReceipt{
				DeliveryRef: "local:rig", CoordinatorFence: "mayor-rig",
			},
		},
		StoreRef: rigInput.StoreRef,
	}
	_, err = cityActivities.Deliver(ctx, OutcomeDeliveryRequest{
		Envelope: cityEnvelope, DeliveryCycle: "cycle-000001",
	})
	require.NoError(t, err)
	_, err = rigActivities.Deliver(ctx, OutcomeDeliveryRequest{
		Envelope: rigEnvelope, DeliveryCycle: "cycle-000001",
	})
	require.NoError(t, err)

	cityRecord, err = city.store.InspectOutcome(ctx, city.beadID)
	require.NoError(t, err)
	rigRecord, err := rig.store.InspectOutcome(ctx, rig.beadID)
	require.NoError(t, err)
	require.Equal(t, "local:city", cityRecord.DeliveryRef)
	require.Equal(t, "local:rig", rigRecord.DeliveryRef)
}

func TestCoordinatorOutcomeTaskQueueIsStablePerStore(t *testing.T) {
	first, err := CoordinatorOutcomeTaskQueueForStore("city:ds-research")
	require.NoError(t, err)
	second, err := CoordinatorOutcomeTaskQueueForStore("city:ds-research")
	require.NoError(t, err)
	rig, err := CoordinatorOutcomeTaskQueueForStore("rig:codeprobe")
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.NotEqual(t, first, rig)
}

func TestOutcomeStoreFailureIncludesStoreAndStage(t *testing.T) {
	failure := OutcomeStoreFailure{
		StoreRef: "rig:codeprobe",
		Stage:    "scan",
		Err:      errors.New("deadline exceeded"),
	}
	require.Equal(
		t,
		"rig:codeprobe scan: deadline exceeded",
		failure.Error(),
	)
}
