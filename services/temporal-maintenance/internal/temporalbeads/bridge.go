package temporalbeads

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

const (
	OrchestrationTaskQueue        = "gascity-bead-orchestration"
	BeadOrchestrationWorkflowName = "BeadOrchestrationWorkflow"
)

// StartOrSignalRequest is one idempotent Temporal boundary delivery.
type StartOrSignalRequest struct {
	WorkflowID string
	Event      ReadyEvent
	Input      WorkflowInput
}

// WorkflowReceipt identifies the exact accepted event and Workflow.
type WorkflowReceipt struct {
	WorkflowID string
	RunID      string
	EventID    string
}

// WorkflowGateway performs Temporal's atomic Signal-With-Start operation.
type WorkflowGateway interface {
	SignalWithStart(context.Context, StartOrSignalRequest) (WorkflowReceipt, error)
	SignalClose(context.Context, string, CloseRequest) error
}

// ReadyEventAcker records that Temporal accepted an outbox event.
type ReadyEventAcker interface {
	AcknowledgeReadyEvent(context.Context, string) error
}

// ReadyEventBridge delivers durable Beads events and checkpoints acceptance.
type ReadyEventBridge struct {
	Temporal WorkflowGateway
	Acker    ReadyEventAcker
	Timing   TimingConfig
}

// Deliver is safe across duplicate delivery and the acknowledgement crash gap.
func (b ReadyEventBridge) Deliver(
	ctx context.Context,
	event ReadyEvent,
) (WorkflowReceipt, error) {
	if b.Temporal == nil {
		return WorkflowReceipt{}, fmt.Errorf("workflow gateway is required")
	}
	if b.Acker == nil {
		return WorkflowReceipt{}, fmt.Errorf("ready event acker is required")
	}
	if err := event.Validate(); err != nil {
		return WorkflowReceipt{}, err
	}
	timing := b.Timing
	if timing.HeartbeatTimeout == 0 &&
		timing.ReconcileInterval == 0 &&
		timing.Clock == nil {
		timing = defaultTimingConfig()
	}
	if err := timing.Validate(); err != nil {
		return WorkflowReceipt{}, fmt.Errorf("ready event timing: %w", err)
	}
	workflowID, err := WorkflowID(event.CityID, event.RunID, event.BeadID)
	if err != nil {
		return WorkflowReceipt{}, err
	}
	input := WorkflowInput{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		InitialReady:     []ReadyEvent{event},
		HeartbeatTimeout: timing.HeartbeatTimeout,
		EventLimit:       DefaultEventLimit,
	}
	receipt, err := b.Temporal.SignalWithStart(ctx, StartOrSignalRequest{
		WorkflowID: workflowID,
		Event:      event,
		Input:      input,
	})
	if err != nil {
		return WorkflowReceipt{}, fmt.Errorf("signal with start %s: %w", workflowID, err)
	}
	if receipt.WorkflowID != workflowID || receipt.EventID != event.EventID {
		return WorkflowReceipt{}, fmt.Errorf("Temporal returned a mismatched workflow receipt")
	}
	if err := b.Acker.AcknowledgeReadyEvent(ctx, event.EventID); err != nil {
		return WorkflowReceipt{}, fmt.Errorf("checkpoint ready event %s: %w", event.EventID, err)
	}
	return receipt, nil
}

// SealRun closes a finite run only after its authoritative event set is seen.
func (b ReadyEventBridge) SealRun(ctx context.Context, request CloseRequest) error {
	if b.Temporal == nil {
		return fmt.Errorf("workflow gateway is required")
	}
	if err := request.validatePayload(); err != nil {
		return err
	}
	if request.BeadID == "" {
		return fmt.Errorf("bead id is required to seal a per-bead workflow")
	}
	workflowID, err := WorkflowID(
		request.CityID,
		request.RunID,
		request.BeadID,
	)
	if err != nil {
		return err
	}
	if err := b.Temporal.SignalClose(ctx, workflowID, request); err != nil {
		return fmt.Errorf("seal orchestration run %s: %w", workflowID, err)
	}
	return nil
}

// WorkflowID is stable for one bead within the complete orchestration run.
func WorkflowID(cityID, runID, beadID string) (string, error) {
	if err := validateSegment("city id", cityID); err != nil {
		return "", err
	}
	if err := validateSegment("run id", runID); err != nil {
		return "", err
	}
	if err := validateSegment("bead id", beadID); err != nil {
		return "", err
	}
	workflowID := fmt.Sprintf(
		"bead-orchestration/%s/%s/%s",
		cityID,
		runID,
		beadID,
	)
	if err := validateWorkflowID("bead orchestration workflow id", workflowID); err != nil {
		return "", err
	}
	return workflowID, nil
}

// TemporalClientGateway binds the abstract boundary to the Temporal Go client.
type TemporalClientGateway struct {
	Client                        client.Client
	EnableFormulaSearchAttributes bool
}

// SignalWithStart atomically signals a run or starts it under the stable ID.
func (g TemporalClientGateway) SignalWithStart(
	ctx context.Context,
	request StartOrSignalRequest,
) (WorkflowReceipt, error) {
	if g.Client == nil {
		return WorkflowReceipt{}, fmt.Errorf("Temporal client is required")
	}
	input := request.Input
	input.SearchAttributes = g.EnableFormulaSearchAttributes
	options := client.StartWorkflowOptions{
		ID:                       request.WorkflowID,
		TaskQueue:                OrchestrationTaskQueue,
		Memo:                     FormulaMemo(request.Event),
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}
	if g.EnableFormulaSearchAttributes {
		options.TypedSearchAttributes = FormulaSearchAttributes(request.Event)
	}
	run, err := g.Client.SignalWithStartWorkflow(
		ctx,
		request.WorkflowID,
		SignalReady,
		request.Event,
		options,
		BeadOrchestrationWorkflowName,
		input,
	)
	if err != nil {
		return WorkflowReceipt{}, err
	}
	receipt := WorkflowReceipt{
		WorkflowID: run.GetID(),
		RunID:      run.GetRunID(),
		EventID:    request.Event.EventID,
	}
	if request.Event.Formula.ParentWorkflowID != "" {
		link, linkErr := NewChildWorkflowLink(
			request.Event,
			receipt,
			ChildWorkflowStarted,
			"",
		)
		if linkErr != nil {
			return WorkflowReceipt{}, linkErr
		}
		if signalErr := g.Client.SignalWorkflow(
			ctx,
			request.Event.Formula.ParentWorkflowID,
			request.Event.Formula.ParentRunID,
			SignalParentChildLink,
			link,
		); signalErr != nil {
			return WorkflowReceipt{}, fmt.Errorf(
				"retain parent/child workflow link: %w",
				signalErr,
			)
		}
	}
	return receipt, nil
}

// SignalClose delivers the authoritative finite-run seal to the current run.
func (g TemporalClientGateway) SignalClose(
	ctx context.Context,
	workflowID string,
	request CloseRequest,
) error {
	if g.Client == nil {
		return fmt.Errorf("Temporal client is required")
	}
	return g.Client.SignalWorkflow(ctx, workflowID, "", SignalClose, request)
}
