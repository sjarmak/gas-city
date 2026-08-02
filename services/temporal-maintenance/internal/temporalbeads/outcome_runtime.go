package temporalbeads

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type CoordinatorOutcomeRuntimeConfig struct {
	Store              OutcomeStore
	Notifier           OutcomeNotifier
	StoreRef           string
	TaskQueue          string
	ReconcileInterval  time.Duration
	RedeliveryInterval time.Duration
	WorkerStopTimeout  time.Duration
	OnReconcile        func(OutcomeReconcileResult, error)
}

func ValidateCoordinatorOutcomeRuntimeConfig(
	config CoordinatorOutcomeRuntimeConfig,
) error {
	if config.Store == nil {
		return fmt.Errorf("coordinator outcome store is required")
	}
	if config.Notifier == nil {
		return fmt.Errorf("coordinator outcome notifier is required")
	}
	if err := validateStoreRef(config.StoreRef); err != nil {
		return fmt.Errorf("coordinator outcome store ref: %w", err)
	}
	if config.ReconcileInterval <= 0 {
		return fmt.Errorf("coordinator outcome reconcile interval must be positive")
	}
	if config.RedeliveryInterval <= 0 {
		return fmt.Errorf("coordinator outcome redelivery interval must be positive")
	}
	if config.WorkerStopTimeout <= 0 {
		return fmt.Errorf("coordinator outcome worker stop timeout must be positive")
	}
	return nil
}

// CoordinatorOutcomeRuntime owns one local Task Queue poller plus the canonical
// pending-outbox reconciliation loop.
type CoordinatorOutcomeRuntime struct {
	worker            worker.Worker
	reconciler        *CoordinatorOutcomeReconciler
	reconcileInterval time.Duration
	onResult          func(OutcomeReconcileResult, error)

	mu     sync.Mutex
	stopMu sync.Mutex
	cancel context.CancelFunc
	done   <-chan struct{}
}

func NewCoordinatorOutcomeRuntime(
	temporalClient client.Client,
	config CoordinatorOutcomeRuntimeConfig,
) (*CoordinatorOutcomeRuntime, error) {
	if temporalClient == nil {
		return nil, fmt.Errorf("Temporal client is required")
	}
	if err := ValidateCoordinatorOutcomeRuntimeConfig(config); err != nil {
		return nil, err
	}
	activities := &CoordinatorOutcomeActivities{
		Store: config.Store, Notifier: config.Notifier, StoreRef: config.StoreRef,
	}
	outcomeWorker := worker.New(
		temporalClient,
		outcomeTaskQueue(config.TaskQueue),
		worker.Options{WorkerStopTimeout: config.WorkerStopTimeout},
	)
	outcomeWorker.RegisterWorkflowWithOptions(
		CoordinatorOutcomeWorkflow,
		workflow.RegisterOptions{Name: CoordinatorOutcomeWorkflowName},
	)
	outcomeWorker.RegisterActivityWithOptions(
		activities.Deliver,
		activity.RegisterOptions{Name: DeliverCoordinatorOutcomeActivityName},
	)
	outcomeWorker.RegisterActivityWithOptions(
		activities.Acknowledge,
		activity.RegisterOptions{Name: AcknowledgeCoordinatorOutcomeActivityName},
	)
	gateway := CoordinatorOutcomeTemporalGateway{
		Client: temporalClient, RedeliveryInterval: config.RedeliveryInterval,
		TaskQueue: outcomeTaskQueue(config.TaskQueue),
	}
	return &CoordinatorOutcomeRuntime{
		worker: outcomeWorker,
		reconciler: &CoordinatorOutcomeReconciler{
			Source: config.Store,
			Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
		},
		reconcileInterval: config.ReconcileInterval,
		onResult:          config.OnReconcile,
	}, nil
}

func outcomeTaskQueue(configured string) string {
	if configured != "" {
		return configured
	}
	return CoordinatorOutcomeTaskQueue
}

func (r *CoordinatorOutcomeRuntime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return nil
	}
	if r.worker == nil {
		return fmt.Errorf("coordinator outcome worker is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.worker.Start(); err != nil {
		return fmt.Errorf("start coordinator outcome worker: %w", err)
	}
	if err := ctx.Err(); err != nil {
		r.worker.Stop()
		return err
	}
	loopContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	r.cancel = cancel
	r.done = done
	go r.reconcileLoop(loopContext, done)
	return nil
}

func (r *CoordinatorOutcomeRuntime) Stop(ctx context.Context) error {
	r.stopMu.Lock()
	defer r.stopMu.Unlock()
	r.mu.Lock()
	cancel := r.cancel
	done := r.done
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("stop coordinator outcome reconcile loop: %w", ctx.Err())
	}
	if r.worker != nil {
		r.worker.Stop()
	}
	r.mu.Lock()
	r.cancel = nil
	r.done = nil
	r.mu.Unlock()
	return nil
}

func (r *CoordinatorOutcomeRuntime) reconcileLoop(
	ctx context.Context,
	done chan<- struct{},
) {
	defer close(done)
	r.reconcileOnce(ctx)
	ticker := time.NewTicker(r.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcileOnce(ctx)
		}
	}
}

func (r *CoordinatorOutcomeRuntime) reconcileOnce(ctx context.Context) {
	result, err := r.reconciler.Reconcile(ctx)
	if r.onResult != nil {
		r.onResult(result, err)
	}
}
