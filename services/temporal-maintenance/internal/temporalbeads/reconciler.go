package temporalbeads

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
)

// WorkflowReceiptChecker queries the exact event receipt in Workflow state.
type WorkflowReceiptChecker interface {
	HasEvent(context.Context, string, string) (bool, error)
}

// ReconcileResult reports boundary repair only, never scheduling.
type ReconcileResult struct {
	Scanned      int
	Acknowledged int
	Repaired     int
}

// ReadyEventReconciler repairs missing ready-event delivery.
type ReadyEventReconciler struct {
	Source   ReadyEventSource
	Receipts WorkflowReceiptChecker
	Bridge   ReadyEventBridge
	Timing   TimingConfig
}

// Reconcile redelivers every pending event through the complete idempotent
// bridge, even when the child already has the event. This closes the crash gap
// between child Signal-With-Start, maintenance-parent linkage, and outbox ack.
func (r ReadyEventReconciler) Reconcile(ctx context.Context) (ReconcileResult, error) {
	if r.Source == nil {
		return ReconcileResult{}, fmt.Errorf("ready event source is required")
	}
	if r.Receipts == nil {
		return ReconcileResult{}, fmt.Errorf("workflow receipt checker is required")
	}
	events, err := r.Source.PendingReadyEvents(ctx)
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("read durable ready events: %w", err)
	}
	result := ReconcileResult{Scanned: len(events)}
	for _, event := range events {
		workflowID, err := WorkflowID(
			event.CityID,
			event.RunID,
			event.BeadID,
		)
		if err != nil {
			return result, err
		}
		seen, err := r.Receipts.HasEvent(ctx, workflowID, event.EventID)
		if err != nil {
			return result, fmt.Errorf("query ready event receipt: %w", err)
		}
		if seen {
			if _, err := r.Bridge.Deliver(ctx, event); err != nil {
				return result, fmt.Errorf(
					"complete existing ready event receipt %s: %w",
					event.EventID,
					err,
				)
			}
			result.Acknowledged++
			continue
		}
		if _, err := r.Bridge.Deliver(ctx, event); err != nil {
			return result, fmt.Errorf("repair ready event %s: %w", event.EventID, err)
		}
		result.Repaired++
	}
	return result, nil
}

// Due reports whether the configured repair interval has elapsed.
func (r ReadyEventReconciler) Due(lastRun time.Time) bool {
	config := r.Timing
	if config.Clock == nil {
		config = defaultTimingConfig()
	}
	if err := config.Validate(); err != nil {
		return false
	}
	return !config.Now().Before(lastRun.Add(config.ReconcileInterval))
}

// HasEvent queries the compact Workflow state through the Temporal client.
func (g TemporalClientGateway) HasEvent(
	ctx context.Context,
	workflowID string,
	eventID string,
) (bool, error) {
	if g.Client == nil {
		return false, fmt.Errorf("Temporal client is required")
	}
	value, err := g.Client.QueryWorkflow(ctx, workflowID, "", QueryState)
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	var state WorkflowState
	if err := value.Get(&state); err != nil {
		return false, fmt.Errorf("decode Workflow receipt state: %w", err)
	}
	for _, received := range state.ReceivedEventIDs {
		if received == eventID {
			return true, nil
		}
	}
	return false, nil
}
