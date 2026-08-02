package temporalbeads

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReconcilerRepairsMissingReadyReceiptThroughBridge(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	reconciler := ReadyEventReconciler{
		Source:   store,
		Receipts: gateway,
		Bridge:   ReadyEventBridge{Temporal: gateway, Acker: store},
	}

	result, err := reconciler.Reconcile(context.Background())
	require.NoError(t, err)
	workflowID, err := WorkflowID(event.CityID, event.RunID, event.BeadID)
	require.NoError(t, err)
	require.Equal(t, ReconcileResult{Scanned: 1, Repaired: 1}, result)
	require.Equal(t, []string{workflowID}, gateway.WorkflowIDs())
	require.Equal(t, 1, gateway.LogicalEventCount())
}

func TestReconcilerCompletesExistingReceiptThroughIdempotentBridge(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	workflowID, err := WorkflowID(event.CityID, event.RunID, event.BeadID)
	require.NoError(t, err)
	gateway.Seed(workflowID, event.EventID)
	reconciler := ReadyEventReconciler{
		Source:   store,
		Receipts: gateway,
		Bridge:   ReadyEventBridge{Temporal: gateway, Acker: store},
	}

	result, err := reconciler.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, ReconcileResult{Scanned: 1, Acknowledged: 1}, result)
	require.Zero(t, gateway.StartCount())
	require.Equal(t, 1, gateway.SignalExistingCount())
	record, err := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusReady, record.Status)
	require.Empty(t, record.ClaimToken)
}

func TestReconcilerRepairsChildAcceptedParentLinkCrashGap(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	gateway.postAcceptErr = errors.New("parent link unavailable")
	bridge := ReadyEventBridge{Temporal: gateway, Acker: store}

	_, err := bridge.Deliver(context.Background(), event)
	require.ErrorContains(t, err, "parent link unavailable")
	pending, err := store.PendingReadyEvents(context.Background())
	require.NoError(t, err)
	require.Equal(t, []ReadyEvent{event}, pending)

	result, err := (ReadyEventReconciler{
		Source:   store,
		Receipts: gateway,
		Bridge:   bridge,
	}).Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, ReconcileResult{Scanned: 1, Acknowledged: 1}, result)
	require.Equal(t, 1, gateway.StartCount())
	require.Equal(t, 1, gateway.SignalExistingCount())
	pending, err = store.PendingReadyEvents(context.Background())
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestReconcilerCannotBecomeASecondScheduler(t *testing.T) {
	source := &splitBrainTrapSource{events: []ReadyEvent{validReadyEvent(t)}}
	gateway := newFakeWorkflowGateway()
	reconciler := ReadyEventReconciler{
		Source:   source,
		Receipts: gateway,
		Bridge:   ReadyEventBridge{Temporal: gateway, Acker: source},
	}

	_, err := reconciler.Reconcile(context.Background())
	require.NoError(t, err)
	require.Zero(t, source.claimCalls)
	require.Zero(t, source.agentStarts)
}

func TestReconcilerTemporalUnavailableLeavesOwnershipUntouched(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	gateway := newFakeWorkflowGateway()
	gateway.receiptErr = errors.New("temporal unavailable")
	reconciler := ReadyEventReconciler{
		Source:   store,
		Receipts: gateway,
		Bridge:   ReadyEventBridge{Temporal: gateway, Acker: store},
	}

	_, err := reconciler.Reconcile(context.Background())
	require.ErrorContains(t, err, "temporal unavailable")
	record, inspectErr := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, inspectErr)
	require.Equal(t, BeadStatusReady, record.Status)
	require.Empty(t, record.ClaimToken)
	pending, pendingErr := store.PendingReadyEvents(context.Background())
	require.NoError(t, pendingErr)
	require.Equal(t, []ReadyEvent{event}, pending)
}

func TestReconcilerDueUsesConfiguredDeterministicClock(t *testing.T) {
	start := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	clock := NewManualClock(start)
	reconciler := ReadyEventReconciler{Timing: TimingConfig{
		Clock:             clock,
		HeartbeatTimeout:  time.Minute,
		ReconcileInterval: 5 * time.Minute,
	}}

	require.False(t, reconciler.Due(start))
	clock.Advance(5 * time.Minute)
	require.True(t, reconciler.Due(start))
}

type splitBrainTrapSource struct {
	events      []ReadyEvent
	claimCalls  int
	agentStarts int
}

func (s *splitBrainTrapSource) PendingReadyEvents(context.Context) ([]ReadyEvent, error) {
	return append([]ReadyEvent(nil), s.events...), nil
}

func (s *splitBrainTrapSource) AcknowledgeReadyEvent(
	_ context.Context,
	eventID string,
) error {
	for index, event := range s.events {
		if event.EventID == eventID {
			s.events = append(s.events[:index], s.events[index+1:]...)
			return nil
		}
	}
	return nil
}

func (s *splitBrainTrapSource) Claim() {
	s.claimCalls++
}

func (s *splitBrainTrapSource) StartAgent() {
	s.agentStarts++
}
