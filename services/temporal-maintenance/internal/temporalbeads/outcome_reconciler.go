package temporalbeads

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type CoordinatorOutcomeSource interface {
	EmitOutcome(context.Context, OutcomeReady) (OutcomeRecord, error)
	DiscoverLegacyOutcomes(context.Context) ([]OutcomeReady, error)
	PendingOutcomes(context.Context) ([]OutcomeRecord, error)
}

type CoordinatorOutcomeGateway interface {
	SignalWithStartOutcome(context.Context, OutcomeReady) (WorkflowReceipt, error)
	OutcomeWorkflowRunning(context.Context, OutcomeReady) (bool, error)
}

type CoordinatorOutcomeBridge struct {
	Temporal CoordinatorOutcomeGateway
}

func (b CoordinatorOutcomeBridge) Deliver(
	ctx context.Context,
	envelope OutcomeReady,
) (WorkflowReceipt, error) {
	if b.Temporal == nil {
		return WorkflowReceipt{}, fmt.Errorf("coordinator outcome Temporal gateway is required")
	}
	return b.Temporal.SignalWithStartOutcome(ctx, envelope)
}

func (b CoordinatorOutcomeBridge) Ensure(
	ctx context.Context,
	envelope OutcomeReady,
) (WorkflowReceipt, bool, error) {
	if b.Temporal == nil {
		return WorkflowReceipt{}, false, fmt.Errorf(
			"coordinator outcome Temporal gateway is required",
		)
	}
	running, err := b.Temporal.OutcomeWorkflowRunning(ctx, envelope)
	if err != nil {
		return WorkflowReceipt{}, false, err
	}
	if running {
		workflowID, err := CoordinatorOutcomeWorkflowID(envelope)
		return WorkflowReceipt{WorkflowID: workflowID}, false, err
	}
	receipt, err := b.Temporal.SignalWithStartOutcome(ctx, envelope)
	return receipt, err == nil, err
}

type CoordinatorOutcomeTemporalGateway struct {
	Client             client.Client
	RedeliveryInterval time.Duration
	TaskQueue          string
}

func (g CoordinatorOutcomeTemporalGateway) SignalWithStartOutcome(
	ctx context.Context,
	envelope OutcomeReady,
) (WorkflowReceipt, error) {
	if g.Client == nil {
		return WorkflowReceipt{}, fmt.Errorf("Temporal client is required")
	}
	workflowID, err := CoordinatorOutcomeWorkflowID(envelope)
	if err != nil {
		return WorkflowReceipt{}, err
	}
	interval := g.RedeliveryInterval
	if interval <= 0 {
		interval = defaultOutcomeRedeliveryPeriod
	}
	run, err := g.Client.SignalWithStartWorkflow(
		ctx,
		workflowID,
		SignalOutcomeReady,
		envelope,
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: outcomeTaskQueue(g.TaskQueue),
			Memo:      CoordinatorOutcomeMemo(envelope),
		},
		CoordinatorOutcomeWorkflowName,
		CoordinatorOutcomeInput{
			Envelope:           envelope,
			RedeliveryInterval: interval,
		},
	)
	if err != nil {
		return WorkflowReceipt{}, fmt.Errorf(
			"signal-with-start coordinator outcome %s: %w",
			envelope.OutcomeID,
			err,
		)
	}
	return WorkflowReceipt{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}

func (g CoordinatorOutcomeTemporalGateway) OutcomeWorkflowRunning(
	ctx context.Context,
	envelope OutcomeReady,
) (bool, error) {
	if g.Client == nil {
		return false, fmt.Errorf("Temporal client is required")
	}
	workflowID, err := CoordinatorOutcomeWorkflowID(envelope)
	if err != nil {
		return false, err
	}
	description, err := g.Client.DescribeWorkflowExecution(
		ctx,
		workflowID,
		"",
	)
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf(
			"describe coordinator outcome workflow %s: %w",
			envelope.OutcomeID,
			err,
		)
	}
	if description == nil || description.WorkflowExecutionInfo == nil {
		return false, fmt.Errorf(
			"describe coordinator outcome workflow %s returned no execution info",
			envelope.OutcomeID,
		)
	}
	switch description.WorkflowExecutionInfo.Status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return true, nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return false, nil
	default:
		return false, fmt.Errorf(
			"coordinator outcome workflow %s is %s while canonical outcome remains unacknowledged",
			envelope.OutcomeID,
			description.WorkflowExecutionInfo.Status,
		)
	}
}

// CoordinatorOutcomeMemo makes the durable result and its source topology
// legible in Temporal Web without namespace-level Search Attribute setup.
func CoordinatorOutcomeMemo(envelope OutcomeReady) map[string]interface{} {
	memo := map[string]interface{}{
		"GasCityOutcomeStoreRef":     envelope.StoreRef,
		"GasCityOutcomeWorkID":       envelope.WorkID,
		"GasCityOutcomeSourceRootID": envelope.SourceRootID,
		"GasCityOutcomeProducer":     string(envelope.Producer),
		"GasCityOutcomeStepKey":      envelope.StepKey,
		"GasCityOutcomeID":           envelope.OutcomeID,
	}
	if envelope.WorkflowID != "" {
		memo["GasCitySourceWorkflowID"] = envelope.WorkflowID
	}
	if envelope.WorkflowRunID != "" {
		memo["GasCitySourceWorkflowRunID"] = envelope.WorkflowRunID
	}
	return memo
}

type OutcomeReconcileResult struct {
	Produced  int `json:"produced"`
	Scanned   int `json:"scanned"`
	Signalled int `json:"signalled"`
}

type CoordinatorOutcomeReconciler struct {
	Source CoordinatorOutcomeSource
	Bridge CoordinatorOutcomeBridge
}

func (r CoordinatorOutcomeReconciler) Reconcile(
	ctx context.Context,
) (OutcomeReconcileResult, error) {
	if r.Source == nil {
		return OutcomeReconcileResult{}, fmt.Errorf("coordinator outcome source is required")
	}
	result := OutcomeReconcileResult{}
	var itemErrors []error
	records, err := r.Source.PendingOutcomes(ctx)
	if err != nil {
		return OutcomeReconcileResult{}, fmt.Errorf(
			"read pending coordinator outcomes: %w",
			err,
		)
	}
	result.Scanned = len(records)
	for _, record := range records {
		_, signalled, deliveryErr := r.Bridge.Ensure(ctx, record.Envelope)
		if deliveryErr != nil {
			itemErrors = append(itemErrors, fmt.Errorf(
				"signal pending coordinator outcome for work %s (%s): %w",
				record.Envelope.WorkID,
				record.Envelope.OutcomeID,
				deliveryErr,
			))
			continue
		}
		if signalled {
			result.Signalled++
		}
	}
	candidates, discoveryErr := r.Source.DiscoverLegacyOutcomes(ctx)
	var candidateErr *legacyOutcomeDiscoveryError
	if discoveryErr != nil && !errors.As(discoveryErr, &candidateErr) {
		storeErr := fmt.Errorf(
			"discover legacy coordinator outcomes: %w",
			discoveryErr,
		)
		return result, errors.Join(append(itemErrors, storeErr)...)
	}
	if discoveryErr != nil {
		itemErrors = append(itemErrors, fmt.Errorf(
			"discover legacy coordinator outcomes: %w",
			discoveryErr,
		))
	}
	for _, envelope := range candidates {
		if _, err := r.Source.EmitOutcome(ctx, envelope); err != nil {
			itemErrors = append(itemErrors, fmt.Errorf(
				"persist legacy coordinator outcome for work %s (%s): %w",
				envelope.WorkID,
				envelope.OutcomeID,
				err,
			))
			continue
		}
		result.Produced++
		result.Scanned++
		if _, err := r.Bridge.Deliver(ctx, envelope); err != nil {
			itemErrors = append(itemErrors, fmt.Errorf(
				"signal legacy coordinator outcome for work %s (%s): %w",
				envelope.WorkID,
				envelope.OutcomeID,
				err,
			))
			continue
		}
		result.Signalled++
	}
	return result, errors.Join(itemErrors...)
}
