package temporalbeads

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// DeploymentMode is the explicit production safety gate for Temporal-owned
// Beads execution.
type DeploymentMode string

const (
	// DeploymentModeShadow polls both Task Queues but rejects agent Activities
	// before any canonical Beads mutation or agent dispatch.
	DeploymentModeShadow DeploymentMode = "shadow"
	// DeploymentModeCanary enables the canonical adapter, Signal-With-Start
	// bridge, reconciler, and agent executor for a separately approved canary.
	DeploymentModeCanary DeploymentMode = "canary"
)

// ParseDeploymentMode rejects implicit activation and unknown future modes.
func ParseDeploymentMode(value string) (DeploymentMode, error) {
	switch DeploymentMode(value) {
	case DeploymentModeShadow:
		return DeploymentModeShadow, nil
	case DeploymentModeCanary:
		return DeploymentModeCanary, nil
	case "":
		return "", fmt.Errorf("temporal Beads deployment mode is required")
	default:
		return "", fmt.Errorf(
			"temporal Beads deployment mode must be shadow or canary, got %q",
			value,
		)
	}
}

// CanonicalStore is the complete production boundary used by the Activity and
// ready-event reconciliation paths.
type CanonicalStore interface {
	BeadStore
	ReadyEventSource
}

// ManagedRuntimeConfig composes production workers and reconciliation.
type ManagedRuntimeConfig struct {
	Mode                       DeploymentMode
	Store                      CanonicalStore
	Agent                      AgentExecutor
	Timing                     TimingConfig
	SearchAttributesRegistered bool
	OnReconcile                func(ReconcileResult, error)
}

// ValidateManagedRuntimeConfig keeps shadow independent of every mutating
// dependency and requires the full production boundary only for canary.
func ValidateManagedRuntimeConfig(config ManagedRuntimeConfig) error {
	if config.Mode != DeploymentModeShadow &&
		config.Mode != DeploymentModeCanary {
		return fmt.Errorf("invalid Temporal Beads deployment mode %q", config.Mode)
	}
	if config.Mode == DeploymentModeShadow {
		return nil
	}
	if config.Store == nil {
		return fmt.Errorf("canonical Beads store is required in canary mode")
	}
	if config.Agent == nil {
		return fmt.Errorf("agent executor is required in canary mode")
	}
	if !config.SearchAttributesRegistered {
		return fmt.Errorf(
			"formula search attributes must be registered in canary mode",
		)
	}
	timing := config.Timing
	if timing == (TimingConfig{}) {
		timing = defaultTimingConfig()
	}
	if err := timing.Validate(); err != nil {
		return fmt.Errorf("canary timing: %w", err)
	}
	return nil
}

// ShadowActivityWorker is registered under the real Activity name so the
// managed worker polls the agent Task Queue without accepting side effects.
type ShadowActivityWorker struct{}

// ExecuteBead always fails non-retryably before a Beads claim or agent launch.
func (*ShadowActivityWorker) ExecuteBead(
	context.Context,
	ActivityInput,
) (ActivityResult, error) {
	return ActivityResult{}, temporal.NewNonRetryableApplicationError(
		"Temporal Beads shadow mode cannot mutate Beads or dispatch agents",
		"TemporalBeadsShadowMode",
		nil,
	)
}

// ManagedRuntime owns both Task Queue pollers and the reconciliation loop.
// Shadow binds that loop to an empty fail-closed source; only canary receives
// the canonical ready-event source.
type ManagedRuntime struct {
	mode       DeploymentMode
	workers    *WorkerSet
	reconciler *ReadyEventReconciler
	timing     TimingConfig
	onResult   func(ReconcileResult, error)

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewManagedRuntime composes the mode-specific production runtime.
func NewManagedRuntime(
	temporalClient client.Client,
	config ManagedRuntimeConfig,
) (*ManagedRuntime, error) {
	if temporalClient == nil {
		return nil, fmt.Errorf("temporal client is required")
	}
	if err := ValidateManagedRuntimeConfig(config); err != nil {
		return nil, err
	}

	runtime := &ManagedRuntime{
		mode:     config.Mode,
		timing:   config.Timing,
		onResult: config.OnReconcile,
	}
	if runtime.timing == (TimingConfig{}) {
		runtime.timing = defaultTimingConfig()
	}
	var err error
	if config.Mode == DeploymentModeShadow {
		runtime.workers, err = NewShadowWorkerSet(temporalClient)
		if err != nil {
			return nil, err
		}
		source := shadowReadyEventSource{}
		gateway := TemporalClientGateway{Client: temporalClient}
		runtime.reconciler = &ReadyEventReconciler{
			Source:   source,
			Receipts: gateway,
			Bridge: ReadyEventBridge{
				Temporal: gateway,
				Acker:    source,
				Timing:   runtime.timing,
			},
			Timing: runtime.timing,
		}
		return runtime, nil
	}
	runtime.workers, err = NewWorkerSet(
		temporalClient,
		config.Store,
		config.Agent,
	)
	if err != nil {
		return nil, err
	}
	gateway := TemporalClientGateway{
		Client:                        temporalClient,
		EnableFormulaSearchAttributes: true,
	}
	bridge := ReadyEventBridge{
		Temporal: gateway,
		Acker:    config.Store,
		Timing:   runtime.timing,
	}
	runtime.reconciler = &ReadyEventReconciler{
		Source:   config.Store,
		Receipts: gateway,
		Bridge:   bridge,
		Timing:   runtime.timing,
	}
	return runtime, nil
}

// Mode reports the immutable deployment safety mode.
func (r *ManagedRuntime) Mode() DeploymentMode {
	return r.mode
}

// Start begins both Task Queue pollers and the mode-specific reconciliation.
func (r *ManagedRuntime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return nil
	}
	if err := r.workers.Start(); err != nil {
		return err
	}
	loopContext, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.wg.Add(1)
	go r.reconcileLoop(loopContext)
	return nil
}

// Stop halts reconciliation and both Task Queue pollers.
func (r *ManagedRuntime) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.cancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
	r.workers.Stop()
}

func (r *ManagedRuntime) reconcileLoop(ctx context.Context) {
	defer r.wg.Done()
	r.reconcileOnce(ctx)
	ticker := time.NewTicker(r.timing.ReconcileInterval)
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

func (r *ManagedRuntime) reconcileOnce(ctx context.Context) {
	result, err := r.reconciler.Reconcile(ctx)
	if r.onResult != nil {
		r.onResult(result, err)
	}
}

// shadowReadyEventSource makes the bridge/reconciler runnable without giving
// shadow mode a reference to canonical Beads. An acknowledgement is impossible
// even if a future regression somehow tries to deliver an event through it.
type shadowReadyEventSource struct{}

func (shadowReadyEventSource) PendingReadyEvents(context.Context) ([]ReadyEvent, error) {
	return nil, nil
}

func (shadowReadyEventSource) AcknowledgeReadyEvent(context.Context, string) error {
	return fmt.Errorf("temporal Beads shadow mode cannot acknowledge ready events")
}
