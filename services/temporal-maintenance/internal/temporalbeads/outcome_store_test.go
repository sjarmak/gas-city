package temporalbeads

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDoltOutcomeStorePersistsEnvelopeAndOutboxAtomically(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := FormulaStepOutcome(input)
	require.NoError(t, err)

	record, err := fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)
	require.Equal(t, OutcomeCoordinatorPending, record.State)
	require.Equal(t, fixture.clock.Now(), record.PendingAt)
	require.True(t, record.DeliveredAt.IsZero())
	require.True(t, record.AcknowledgedAt.IsZero())

	var metadataJSON []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		envelope.WorkID,
	).Scan(&metadataJSON))
	var metadata map[string]any
	require.NoError(t, json.Unmarshal(metadataJSON, &metadata))
	require.NotEmpty(t, metadata[metadataOutcomeEnvelope])
	require.NotEmpty(t, metadata[metadataOutcomeOutbox])
	require.Equal(t, record.PendingAt.Format(time.RFC3339Nano), metadata[metadataOutcomePendingAt])
	require.Nil(t, metadata[metadataOutcomeDeliveredAt])
	require.Nil(t, metadata[metadataOutcomeAcknowledgedAt])

	pending, err := fixture.store.PendingOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []OutcomeRecord{record}, pending)

	var eventCount int
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND actor = ?",
		envelope.WorkID,
		fixture.config.Actor,
	).Scan(&eventCount))
	require.Equal(t, 1, eventCount)
}

func TestDoltOutcomeStoreRollsBackEnvelopeWhenEventWriteFails(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := DirectWorkerOutcome(input)
	require.NoError(t, err)
	fixture.store.recordEventFault = func(operation string) error {
		require.Equal(t, "outcome-ready", operation)
		return errors.New("injected event write failure")
	}

	_, err = fixture.store.EmitOutcome(ctx, envelope)
	require.ErrorContains(t, err, "injected event write failure")

	var metadataJSON []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		envelope.WorkID,
	).Scan(&metadataJSON))
	var metadata map[string]any
	require.NoError(t, json.Unmarshal(metadataJSON, &metadata))
	require.NotContains(t, metadata, metadataOutcomeEnvelope)
	require.NotContains(t, metadata, metadataOutcomeOutbox)
	var eventCount int
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND actor = ?",
		envelope.WorkID,
		fixture.config.Actor,
	).Scan(&eventCount))
	require.Zero(t, eventCount)
}

func TestDoltOutcomeStoreRollsBackOutboxWhenMetadataWriteFails(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := DirectWorkerOutcome(input)
	require.NoError(t, err)
	fixture.store.outcomeMetadataFault = func() error {
		return errors.New("injected metadata write failure")
	}

	_, err = fixture.store.EmitOutcome(ctx, envelope)
	require.ErrorContains(t, err, "injected metadata write failure")

	var metadataJSON []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		envelope.WorkID,
	).Scan(&metadataJSON))
	var metadata map[string]any
	require.NoError(t, json.Unmarshal(metadataJSON, &metadata))
	require.NotContains(t, metadata, metadataOutcomeEnvelope)
	require.NotContains(t, metadata, metadataOutcomeOutbox)
	var eventCount int
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND actor = ?",
		envelope.WorkID,
		fixture.config.Actor,
	).Scan(&eventCount))
	require.Zero(t, eventCount)
}

func TestDoltOutcomeStoreDeduplicatesAndRejectsConflicts(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := DirectWorkerOutcome(input)
	require.NoError(t, err)

	first, err := fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)
	duplicate, err := fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)
	require.Equal(t, first, duplicate)

	conflict := envelope
	conflict.Summary = "A conflicting duplicate."
	_, err = fixture.store.EmitOutcome(ctx, conflict)
	require.ErrorContains(t, err, "conflicts")
}

func TestDoltOutcomeStoreSupersedesAcknowledgedOutcomeAtNextGeneration(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	firstInput := validOutcomeAdapterInput()
	firstInput.WorkID = fixture.beadID
	first, err := DirectWorkerOutcome(firstInput)
	require.NoError(t, err)
	_, err = fixture.store.EmitOutcome(ctx, first)
	require.NoError(t, err)

	nextInput := firstInput
	nextInput.Fence.ProducerID = "different-project-lead-session"
	nextInput.Fence.Generation = 3
	nextInput.Fence.Token = "claim-token-generation-3"
	nextInput.OccurredAt = nextInput.OccurredAt.Add(time.Minute)
	next, err := ProjectLeadOutcome(nextInput)
	require.NoError(t, err)
	require.NotEqual(t, first.OutcomeID, next.OutcomeID)
	firstWorkflowID, err := CoordinatorOutcomeWorkflowID(first)
	require.NoError(t, err)
	nextWorkflowID, err := CoordinatorOutcomeWorkflowID(next)
	require.NoError(t, err)
	require.NotEqual(t, firstWorkflowID, nextWorkflowID)

	_, err = fixture.store.EmitOutcome(ctx, next)
	require.ErrorContains(t, err, "unacknowledged")

	delivery := OutcomeDelivery{
		OutcomeID: first.OutcomeID, WorkID: first.WorkID,
		DeliveryRef:      "local:generation-1",
		CoordinatorFence: "mayor-generation-1",
	}
	_, err = fixture.store.MarkOutcomeDelivered(ctx, delivery)
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeAcknowledged(
		ctx,
		OutcomeAcknowledgement{
			StoreRef:  first.StoreRef,
			OutcomeID: first.OutcomeID, WorkID: first.WorkID,
			CoordinatorFence: delivery.CoordinatorFence,
			AcknowledgedBy:   "mayor",
		},
	)
	require.NoError(t, err)

	staleInput := firstInput
	staleInput.Fence.Token = "different-generation-1-attempt"
	stale, err := DirectWorkerOutcome(staleInput)
	require.NoError(t, err)
	_, err = fixture.store.EmitOutcome(ctx, stale)
	require.ErrorContains(t, err, "generation")

	staleTimeInput := nextInput
	staleTimeInput.Fence.Generation = 4
	staleTimeInput.Fence.Token = "claim-token-generation-4-stale-time"
	staleTimeInput.OccurredAt = first.OccurredAt
	staleTime, err := ProjectLeadOutcome(staleTimeInput)
	require.NoError(t, err)
	_, err = fixture.store.EmitOutcome(ctx, staleTime)
	require.ErrorContains(t, err, "occurred_at")

	record, err := fixture.store.EmitOutcome(ctx, next)
	require.NoError(t, err)
	require.Equal(t, OutcomeCoordinatorPending, record.State)
	require.Equal(t, next.OutcomeID, record.Envelope.OutcomeID)

	var rawMetadata []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		fixture.beadID,
	).Scan(&rawMetadata))
	metadata, err := decodeMetadata(rawMetadata)
	require.NoError(t, err)
	require.NotContains(t, metadata, "gc.coordinator_outcome.history")

	var oldValue, newValue []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		`SELECT old_value, new_value
		   FROM events
		  WHERE issue_id = ?
		    AND old_value LIKE ?
		    AND new_value LIKE ?`,
		fixture.beadID,
		"%"+first.OutcomeID+"%",
		"%"+next.OutcomeID+"%",
	).Scan(&oldValue, &newValue))
	var oldSnapshot, newSnapshot struct {
		Metadata map[string]any `json:"metadata"`
	}
	require.NoError(t, json.Unmarshal(oldValue, &oldSnapshot))
	require.NoError(t, json.Unmarshal(newValue, &newSnapshot))
	oldRecord, exists, err := outcomeRecordFromMetadata(oldSnapshot.Metadata)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, first.OutcomeID, oldRecord.Envelope.OutcomeID)
	require.Equal(t, OutcomeCoordinatorAcknowledged, oldRecord.State)
	require.False(t, oldRecord.AcknowledgedAt.IsZero())
	newRecord, exists, err := outcomeRecordFromMetadata(newSnapshot.Metadata)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, next.OutcomeID, newRecord.Envelope.OutcomeID)
	require.Equal(t, OutcomeCoordinatorPending, newRecord.State)
	require.True(t, newRecord.AcknowledgedAt.IsZero())
}

func TestDoltOutcomeStoreTracksDeliveredAndExplicitAcknowledgement(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := ProjectLeadOutcome(input)
	require.NoError(t, err)
	record, err := fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)

	fixture.clock.Advance(time.Minute)
	record, err = fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID:        envelope.OutcomeID,
		WorkID:           envelope.WorkID,
		DeliveryRef:      "mail:gc-outcome-message",
		CoordinatorFence: "mayor-session-1",
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeCoordinatorNeedsAck, record.State)
	require.Equal(t, fixture.clock.Now(), record.DeliveredAt)
	require.True(t, record.AcknowledgedAt.IsZero())

	pending, err := fixture.store.PendingOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []OutcomeRecord{record}, pending)

	fixture.clock.Advance(time.Minute)
	record, err = fixture.store.MarkOutcomeAcknowledged(ctx, OutcomeAcknowledgement{
		StoreRef:         envelope.StoreRef,
		OutcomeID:        envelope.OutcomeID,
		WorkID:           envelope.WorkID,
		CoordinatorFence: "mayor-session-1",
		AcknowledgedBy:   "mayor",
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeCoordinatorAcknowledged, record.State)
	require.Equal(t, fixture.clock.Now(), record.AcknowledgedAt)
	pending, err = fixture.store.PendingOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestDoltOutcomeStoreFinalizesExactAcknowledgedGenerationAtomically(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		workID       = "dr-pu0k"
		transitionID = "019fb859-a057-72ae-a479-90c52d03213f"
	)
	seedLegacyOutcomeTransition(
		t,
		fixture,
		workID,
		"in_progress",
		"city-infra-pl",
		transitionID,
		map[string]any{
			"gc.outcome.generation": "3",
			"review_head":           "9e9d4f2769cd0488366580a508e8f929ab442778",
			"review_verdict":        "pass",
			"summary_for_human":     "OutcomeReady implementation passed exact-head review and live canary.",
		},
	)

	reconciler := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: &recordingOutcomeGateway{}},
	}
	result, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Produced)
	record, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	require.Equal(t, int64(3), record.Envelope.Fence.Generation)
	require.Equal(t, transitionID, record.Envelope.Fence.Token)

	const coordinatorFence = "gc-527414"
	acknowledged := acknowledgeOutcomeForFinalization(
		t, fixture, record.Envelope, coordinatorFence,
	)
	require.Equal(t, OutcomeCoordinatorAcknowledged, acknowledged.State)
	var preFinalizationMetadata []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		workID,
	).Scan(&preFinalizationMetadata))
	preFinalization, err := decodeMetadata(preFinalizationMetadata)
	require.NoError(t, err)
	acknowledgedOutbox := metadataString(preFinalization, metadataOutcomeOutbox)

	request := OutcomeFinalizationRequest{
		StoreRef:           acknowledged.Envelope.StoreRef,
		OutcomeID:          acknowledged.Envelope.OutcomeID,
		WorkID:             workID,
		ProducerGeneration: acknowledged.Envelope.Fence.Generation,
		ProducerToken:      acknowledged.Envelope.Fence.Token,
		CoordinatorFence:   coordinatorFence,
		FinalizedBy:        "city-infra-pl",
		CloseReason:        "Reviewed OutcomeReady implementation finalized after exact coordinator acknowledgement.",
	}
	receipt, err := fixture.store.FinalizeAcknowledgedOutcome(ctx, request)
	require.NoError(t, err)
	require.Equal(t, 1, receipt.ContractVersion)
	require.Equal(t, request.StoreRef, receipt.StoreRef)
	require.Equal(t, request.OutcomeID, receipt.OutcomeID)
	require.Equal(t, request.WorkID, receipt.WorkID)
	require.Equal(t, int64(3), receipt.ProducerGeneration)
	require.Equal(t, transitionID, receipt.ProducerToken)
	require.Equal(t, coordinatorFence, receipt.CoordinatorFence)
	require.Equal(t, acknowledged.AcknowledgedAt, receipt.AcknowledgedAt)
	require.Equal(t, "mayor", receipt.AcknowledgedBy)
	require.Equal(t, request.FinalizedBy, receipt.FinalizedBy)
	require.Equal(t, request.CloseReason, receipt.CloseReason)
	require.False(t, receipt.FinalizedAt.IsZero())

	var status string
	var rawMetadata []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT status, metadata FROM issues WHERE id = ?",
		workID,
	).Scan(&status, &rawMetadata))
	require.Equal(t, "closed", status)
	metadata, err := decodeMetadata(rawMetadata)
	require.NoError(t, err)
	require.NotContains(t, metadata, metadataOutcomeNonOutcome)
	require.Equal(t, acknowledgedOutbox, metadataString(metadata, metadataOutcomeOutbox))
	var stored OutcomeFinalizationReceipt
	require.NoError(t, decodeMetadataJSON(
		metadata,
		metadataOutcomeFinalization,
		&stored,
	))
	require.Equal(t, receipt, stored)

	finalRecord, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	require.Equal(t, acknowledged, finalRecord)
	envelopes, err := fixture.store.DiscoverLegacyOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, envelopes)
	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, findings)

	var eventCount int
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM events
		  WHERE issue_id = ? AND event_type = 'closed'`,
		workID,
	).Scan(&eventCount))
	require.Equal(t, 1, eventCount)
	var oldValue, newValue []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		`SELECT old_value, new_value FROM events
		  WHERE issue_id = ? AND event_type = 'closed'`,
		workID,
	).Scan(&oldValue, &newValue))
	var oldSnapshot, newSnapshot struct {
		Status   string         `json:"status"`
		Metadata map[string]any `json:"metadata"`
	}
	require.NoError(t, json.Unmarshal(oldValue, &oldSnapshot))
	require.NoError(t, json.Unmarshal(newValue, &newSnapshot))
	require.Equal(t, "in_progress", oldSnapshot.Status)
	require.Equal(t, "closed", newSnapshot.Status)
	require.NotContains(t, oldSnapshot.Metadata, metadataOutcomeFinalization)
	require.Contains(t, newSnapshot.Metadata, metadataOutcomeFinalization)
	require.Equal(
		t,
		metadataString(oldSnapshot.Metadata, metadataOutcomeOutbox),
		metadataString(newSnapshot.Metadata, metadataOutcomeOutbox),
	)

	duplicate, err := fixture.store.FinalizeAcknowledgedOutcome(ctx, request)
	require.NoError(t, err)
	require.Equal(t, receipt, duplicate)
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM events
		  WHERE issue_id = ? AND event_type = 'closed'`,
		workID,
	).Scan(&eventCount))
	require.Equal(t, 1, eventCount)
}

func TestDoltOutcomeStoreFinalizationDoesNotReproduceCanonicalOutcome(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.StoreRef = fixture.config.OutcomeStoreRef
	input.WorkID = fixture.beadID
	envelope, err := DirectWorkerOutcome(input)
	require.NoError(t, err)
	_, err = fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)
	const coordinatorFence = "gc-mayor-current"
	acknowledged := acknowledgeOutcomeForFinalization(
		t, fixture, envelope, coordinatorFence,
	)

	var beforeMetadataJSON []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		fixture.beadID,
	).Scan(&beforeMetadataJSON))
	beforeMetadata, err := decodeMetadata(beforeMetadataJSON)
	require.NoError(t, err)
	require.Empty(t, metadataString(beforeMetadata, "review_verdict"))
	require.Empty(t, metadataString(beforeMetadata, "gc.outcome"))
	acknowledgedOutbox := metadataString(beforeMetadata, metadataOutcomeOutbox)

	_, err = fixture.store.FinalizeAcknowledgedOutcome(
		ctx,
		OutcomeFinalizationRequest{
			StoreRef:           envelope.StoreRef,
			OutcomeID:          envelope.OutcomeID,
			WorkID:             envelope.WorkID,
			ProducerGeneration: envelope.Fence.Generation,
			ProducerToken:      envelope.Fence.Token,
			CoordinatorFence:   coordinatorFence,
			FinalizedBy:        "city-infra-pl",
			CloseReason:        "Reviewed canonical outcome finalized after acknowledgement.",
		},
	)
	require.NoError(t, err)

	discovered, err := fixture.store.DiscoverLegacyOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, discovered)
	silent, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, silent)

	gateway := &recordingOutcomeGateway{}
	result, err := (CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}).Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, OutcomeReconcileResult{}, result)
	require.Empty(t, gateway.envelopes)

	finalRecord, err := fixture.store.InspectOutcome(ctx, fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, acknowledged, finalRecord)
	var afterMetadataJSON []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		fixture.beadID,
	).Scan(&afterMetadataJSON))
	afterMetadata, err := decodeMetadata(afterMetadataJSON)
	require.NoError(t, err)
	require.Equal(
		t,
		acknowledgedOutbox,
		metadataString(afterMetadata, metadataOutcomeOutbox),
	)

	var tamperedReceipt OutcomeFinalizationReceipt
	require.NoError(t, decodeMetadataJSON(
		afterMetadata,
		metadataOutcomeFinalization,
		&tamperedReceipt,
	))
	tamperedReceipt.CoordinatorFence = "gc-replaced-acknowledgement"
	tamperedRecord := acknowledged
	tamperedRecord.CoordinatorFence = tamperedReceipt.CoordinatorFence
	tamperedMetadata, err := decodeMetadata(afterMetadataJSON)
	require.NoError(t, err)
	require.NoError(t, setOutcomeMetadata(tamperedMetadata, tamperedRecord))
	tamperedReceiptJSON, err := json.Marshal(tamperedReceipt)
	require.NoError(t, err)
	tamperedMetadata[metadataOutcomeFinalization] = string(tamperedReceiptJSON)
	tamperedMetadataJSON, err := encodeMetadata(tamperedMetadata)
	require.NoError(t, err)
	tamperedEventValue, err := json.Marshal(map[string]any{
		"id":       fixture.beadID,
		"status":   "closed",
		"metadata": tamperedMetadata,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(
		ctx,
		"UPDATE issues SET metadata = ? WHERE id = ?",
		tamperedMetadataJSON,
		fixture.beadID,
	)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(
		ctx,
		`UPDATE events SET new_value = ?
		  WHERE issue_id = ? AND event_type = 'closed'`,
		tamperedEventValue,
		fixture.beadID,
	)
	require.NoError(t, err)
	discovered, err = fixture.store.DiscoverLegacyOutcomes(ctx)
	require.Empty(t, discovered)
	require.ErrorContains(t, err, "pre-close acknowledged outbox")
}

func TestDoltOutcomeStoreFinalizationRejectsAlreadyClosedWorkWithoutContract(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := DirectWorkerOutcome(input)
	require.NoError(t, err)
	_, err = fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)
	const coordinatorFence = "gc-mayor-current"
	acknowledgeOutcomeForFinalization(t, fixture, envelope, coordinatorFence)
	_, err = fixture.db.ExecContext(
		ctx,
		"UPDATE issues SET status = 'closed' WHERE id = ?",
		fixture.beadID,
	)
	require.NoError(t, err)

	_, err = fixture.store.FinalizeAcknowledgedOutcome(
		ctx,
		OutcomeFinalizationRequest{
			StoreRef: envelope.StoreRef, OutcomeID: envelope.OutcomeID,
			WorkID:             envelope.WorkID,
			ProducerGeneration: envelope.Fence.Generation,
			ProducerToken:      envelope.Fence.Token,
			CoordinatorFence:   coordinatorFence,
			FinalizedBy:        "city-infra-pl", CloseReason: "Reviewed completion.",
		},
	)
	require.ErrorContains(t, err, "already closed without")

	var rawMetadata []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		fixture.beadID,
	).Scan(&rawMetadata))
	metadata, err := decodeMetadata(rawMetadata)
	require.NoError(t, err)
	require.NotContains(t, metadata, metadataOutcomeFinalization)
}

func TestDoltOutcomeStoreFinalizationFailsClosedBeforeExactAcknowledgement(t *testing.T) {
	for _, test := range []struct {
		name        string
		acknowledge bool
		mutate      func(*OutcomeFinalizationRequest)
		want        string
	}{
		{name: "pending generation", want: "acknowledged"},
		{
			name: "store mismatch", acknowledge: true,
			mutate: func(request *OutcomeFinalizationRequest) {
				request.StoreRef = "rig:gascity"
			},
			want: "exact acknowledged generation",
		},
		{
			name: "outcome mismatch", acknowledge: true,
			mutate: func(request *OutcomeFinalizationRequest) {
				request.OutcomeID = "outcome-0000000000000000"
			},
			want: "exact acknowledged generation",
		},
		{
			name: "fence mismatch", acknowledge: true,
			mutate: func(request *OutcomeFinalizationRequest) {
				request.CoordinatorFence = "gc-replacement"
			},
			want: "exact acknowledged generation",
		},
		{
			name: "producer generation mismatch", acknowledge: true,
			mutate: func(request *OutcomeFinalizationRequest) {
				request.ProducerGeneration++
			},
			want: "exact acknowledged generation",
		},
		{
			name: "producer token mismatch", acknowledge: true,
			mutate: func(request *OutcomeFinalizationRequest) {
				request.ProducerToken = "different-transition"
			},
			want: "exact acknowledged generation",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newIsolatedDoltBeadStore(t)
			ctx := context.Background()
			input := validOutcomeAdapterInput()
			input.WorkID = fixture.beadID
			envelope, err := DirectWorkerOutcome(input)
			require.NoError(t, err)
			_, err = fixture.store.EmitOutcome(ctx, envelope)
			require.NoError(t, err)
			const coordinatorFence = "gc-mayor-current"
			if test.acknowledge {
				acknowledgeOutcomeForFinalization(
					t, fixture, envelope, coordinatorFence,
				)
			}
			request := OutcomeFinalizationRequest{
				StoreRef: envelope.StoreRef, OutcomeID: envelope.OutcomeID,
				WorkID:             envelope.WorkID,
				ProducerGeneration: envelope.Fence.Generation,
				ProducerToken:      envelope.Fence.Token,
				CoordinatorFence:   coordinatorFence,
				FinalizedBy:        "city-infra-pl", CloseReason: "Reviewed completion.",
			}
			if test.mutate != nil {
				test.mutate(&request)
			}
			_, err = fixture.store.FinalizeAcknowledgedOutcome(ctx, request)
			require.ErrorContains(t, err, test.want)

			var status string
			var rawMetadata []byte
			require.NoError(t, fixture.db.QueryRowContext(
				ctx,
				"SELECT status, metadata FROM issues WHERE id = ?",
				fixture.beadID,
			).Scan(&status, &rawMetadata))
			require.NotEqual(t, "closed", status)
			metadata, err := decodeMetadata(rawMetadata)
			require.NoError(t, err)
			require.NotContains(t, metadata, metadataOutcomeFinalization)
		})
	}
}

func acknowledgeOutcomeForFinalization(
	t *testing.T,
	fixture *isolatedDoltBeadStore,
	envelope OutcomeReady,
	coordinatorFence string,
) OutcomeRecord {
	t.Helper()
	ctx := context.Background()
	_, err := fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		DeliveryRef: "mail:gc-outcome", CoordinatorFence: coordinatorFence,
	})
	require.NoError(t, err)
	record, err := fixture.store.MarkOutcomeAcknowledged(
		ctx,
		OutcomeAcknowledgement{
			StoreRef: envelope.StoreRef, OutcomeID: envelope.OutcomeID,
			WorkID: envelope.WorkID, CoordinatorFence: coordinatorFence,
			AcknowledgedBy: "mayor",
		},
	)
	require.NoError(t, err)
	return record
}

func TestDoltOutcomeStoreAllowsDeliveryFenceChangeBeforeAcknowledgement(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := MaintenanceOrderOutcome(input)
	require.NoError(t, err)
	_, err = fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)

	first, err := fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		DeliveryRef: "local:first", CoordinatorFence: "mayor-old",
	})
	require.NoError(t, err)
	fixture.clock.Advance(time.Minute)
	second, err := fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		DeliveryRef: "local:second", CoordinatorFence: "mayor-new",
	})
	require.NoError(t, err)
	require.Equal(t, first.DeliveredAt, second.DeliveredAt)
	require.Equal(t, "mayor-new", second.CoordinatorFence)
	require.Equal(t, "local:second", second.DeliveryRef)
	rediscovered, err := fixture.store.PendingOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []OutcomeRecord{second}, rediscovered)

	_, err = fixture.store.MarkOutcomeAcknowledged(ctx, OutcomeAcknowledgement{
		StoreRef:  envelope.StoreRef,
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		CoordinatorFence: "mayor-old", AcknowledgedBy: "mayor",
	})
	require.ErrorIs(t, err, ErrStaleFence)
}

func TestDeliveredUnacknowledgedOutcomeIsRediscoveredAfterReconcilerRestart(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := MaintenanceOrderOutcome(input)
	require.NoError(t, err)
	_, err = fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		DeliveryRef: "local:old", CoordinatorFence: "mayor-old",
	})
	require.NoError(t, err)

	restartedGateway := &recordingOutcomeGateway{}
	restarted := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: restartedGateway},
	}
	result, err := restarted.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Scanned)
	require.Equal(t, []OutcomeReady{envelope}, restartedGateway.envelopes)

	_, err = fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		DeliveryRef: "local:new", CoordinatorFence: "mayor-new",
	})
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeAcknowledged(ctx, OutcomeAcknowledgement{
		StoreRef:  envelope.StoreRef,
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		CoordinatorFence: "mayor-new", AcknowledgedBy: "mayor",
	})
	require.NoError(t, err)

	afterAckGateway := &recordingOutcomeGateway{}
	afterAck := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: afterAckGateway},
	}
	result, err = afterAck.Reconcile(ctx)
	require.NoError(t, err)
	require.Zero(t, result.Scanned)
	require.Empty(t, afterAckGateway.envelopes)
}

func TestDoltOutcomeStoreDeduplicatesAcknowledgement(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := ProjectLeadOutcome(input)
	require.NoError(t, err)
	_, err = fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		DeliveryRef: "local:delivery", CoordinatorFence: "mayor-session",
	})
	require.NoError(t, err)
	ack := OutcomeAcknowledgement{
		StoreRef:  envelope.StoreRef,
		OutcomeID: envelope.OutcomeID, WorkID: envelope.WorkID,
		CoordinatorFence: "mayor-session", AcknowledgedBy: "mayor",
	}
	first, err := fixture.store.MarkOutcomeAcknowledged(ctx, ack)
	require.NoError(t, err)
	fixture.clock.Advance(time.Minute)
	second, err := fixture.store.MarkOutcomeAcknowledged(ctx, ack)
	require.NoError(t, err)
	require.Equal(t, first, second)

	var eventCount int
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND actor = ? AND event_type = 'updated'",
		envelope.WorkID,
		fixture.config.Actor,
	).Scan(&eventCount))
	require.Equal(t, 3, eventCount)
}

func TestDoltOutcomeStoreFindsTerminalEvidenceWithoutEnvelope(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	_, err := fixture.db.ExecContext(ctx, `
		UPDATE issues
		   SET status = 'closed',
		       metadata = JSON_SET(metadata, '$."evidence.reviewer_verdict"', 'pass')
		 WHERE id = ?`,
		fixture.beadID,
	)
	require.NoError(t, err)
	insertOutcomeTransitionEvent(
		t,
		fixture,
		"55555555-5555-5555-5555-555555555555",
		fixture.beadID,
		"open",
		map[string]any{},
		"closed",
		map[string]any{"evidence.reviewer_verdict": "pass"},
		fixture.clock.Now(),
	)

	silent, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []SilentOutcome{{
		StoreRef:     "city:test",
		WorkID:       fixture.beadID,
		Reason:       "terminal without outcome envelope",
		TransitionID: "55555555-5555-5555-5555-555555555555",
		TransitionAt: fixture.clock.Now(),
	}}, silent)

	input := validOutcomeAdapterInput()
	input.WorkID = fixture.beadID
	envelope, err := DirectWorkerOutcome(input)
	require.NoError(t, err)
	_, err = fixture.store.EmitOutcome(ctx, envelope)
	require.NoError(t, err)
	silent, err = fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, silent)
}

func TestDoltOutcomeStoreFindsVerifiedOpenAndInProgressEvidenceWithoutEnvelope(t *testing.T) {
	for _, status := range []string{"open", "in_progress"} {
		t.Run(status, func(t *testing.T) {
			fixture := newIsolatedDoltBeadStore(t)
			ctx := context.Background()
			_, err := fixture.db.ExecContext(ctx, `
				UPDATE issues
				   SET status = ?,
				       metadata = JSON_SET(
				         metadata,
				         '$."evidence.reviewer_verdict"',
				         'pass'
				       )
				 WHERE id = ?`,
				status,
				fixture.beadID,
			)
			require.NoError(t, err)
			eventID := "66666666-6666-6666-6666-66666666666" + status[:1]
			insertOutcomeTransitionEvent(
				t,
				fixture,
				eventID,
				fixture.beadID,
				status,
				map[string]any{},
				status,
				map[string]any{"evidence.reviewer_verdict": "pass"},
				fixture.clock.Now(),
			)

			silent, err := fixture.store.FindSilentOutcomes(ctx)
			require.NoError(t, err)
			require.Equal(t, []SilentOutcome{{
				StoreRef:     "city:test",
				WorkID:       fixture.beadID,
				Reason:       "verified without outcome envelope",
				TransitionID: eventID,
				TransitionAt: fixture.clock.Now(),
			}}, silent)
		})
	}
}

func TestDoltOutcomeStoreSkipsClosedWorkflowStepButSurfacesSourceWork(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	rootID := "gc-l8wuu"
	stepID := "gc-dkg9y"
	sourceID := "gc-w4oq1"
	stepCloseID := "019fb829-3701-7242-8268-2a16e73bc05c"
	sourceCloseID := "11111111-2222-3333-4444-555555555555"
	rootMetadata, err := json.Marshal(map[string]any{
		"gc.kind":         "workflow",
		"gc.formula_name": "mol-focus-review",
		"gc.var.issue":    sourceID,
	})
	require.NoError(t, err)
	stepMetadata := map[string]any{
		"gc.continuation_group": "pool-workflow",
		"gc.outcome":            "success",
		"gc.root_bead_id":       rootID,
		"gc.root_store_ref":     "rig:gascity",
		"gc.session_affinity":   "require",
		"gc.step_ref":           "mol-focus-review.load-context",
	}
	encodedStepMetadata, err := json.Marshal(stepMetadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 status, close_reason, metadata)
		VALUES
			(?, 'mol-focus-review', '', '', '', '', 'in_progress', '', ?),
			(?, 'Load context and understand assignment', '', '', '', '',
			 'closed', 'Context loaded; precheck passed', ?),
			(?, 'Source work', '', '', '', '', 'open', '', '{}')
	`, rootID, rootMetadata, stepID, encodedStepMetadata, sourceID)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'closed', 'polecat-gc-646827', '',
			'Context loaded; precheck passed', ?)
	`, stepCloseID, stepID, fixture.clock.Now().In(time.Local).Format(doltDateTimeLayout))
	require.NoError(t, err)
	priorStepMetadata := make(map[string]any, len(stepMetadata)-1)
	for key, value := range stepMetadata {
		if key != "gc.outcome" {
			priorStepMetadata[key] = value
		}
	}
	oldStep, err := json.Marshal(map[string]any{
		"id": stepID, "status": "closed", "metadata": priorStepMetadata,
	})
	require.NoError(t, err)
	newStep, err := json.Marshal(map[string]any{
		"metadata": stepMetadata,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES ('22222222-3333-4444-5555-666666666666', ?, 'updated',
			'polecat-gc-646827', ?, ?, ?)
	`, stepID, oldStep, newStep,
		fixture.clock.Now().Add(time.Second).In(time.Local).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, findings)

	fixture.clock.Advance(time.Minute)
	_, err = fixture.db.ExecContext(ctx, `
		UPDATE issues
		   SET status = 'closed', close_reason = 'Source work completed.'
		 WHERE id = ?
	`, sourceID)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'closed', 'polecat-gc-646827', '',
			'Source work completed.', ?)
	`, sourceCloseID, sourceID,
		fixture.clock.Now().In(time.Local).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err = fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []SilentOutcome{{
		StoreRef:     "city:test",
		WorkID:       sourceID,
		Reason:       "terminal without outcome envelope",
		TransitionID: sourceCloseID,
		TransitionAt: fixture.clock.Now(),
	}}, findings)
}

func TestDoltOutcomeStoreSkipsExactNonTerminalPassingFormulaStep(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		rootID             = "gc-o30to"
		stepID             = "gc-q8qzc"
		downstreamStepID   = "gc-3v3sq"
		sourceID           = "gc-8jg7c"
		stepTransitionID   = "019fb8a4-5c77-7ac2-888a-9a5018eb4274"
		sourceTransitionID = "99999999-aaaa-bbbb-cccc-dddddddddddd"
	)
	rootMetadata, err := json.Marshal(map[string]any{
		"gc.kind":          "workflow",
		"gc.formula_name":  "mol-focus-review",
		"gc.var.issue":     sourceID,
		"gc.no_land":       "true",
		"gc.finalize_mode": "verify-only",
	})
	require.NoError(t, err)
	stepMetadata := map[string]any{
		"gc.outcome":        "pass",
		"gc.root_bead_id":   rootID,
		"gc.root_store_ref": "rig:gascity",
		"gc.step_ref":       "mol-focus-review.simplify",
	}
	downstreamMetadata := map[string]any{
		"gc.root_bead_id":   rootID,
		"gc.root_store_ref": "rig:gascity",
		"gc.step_ref":       "mol-focus-review.review",
	}
	encodedStepMetadata, err := json.Marshal(stepMetadata)
	require.NoError(t, err)
	encodedDownstreamMetadata, err := json.Marshal(downstreamMetadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 status, close_reason, metadata)
		VALUES
			(?, 'mol-focus-review', '', '', '', '', 'open', '', ?),
			(?, 'Simplify the diff', '', '', '', '', 'closed',
			 'Simplification complete', ?),
			(?, 'Independent review', '', '', '', '', 'in_progress', '', ?),
			(?, 'Source work', '', '', '', '', 'open', '', '{}')
	`, rootID, rootMetadata, stepID, encodedStepMetadata,
		downstreamStepID, encodedDownstreamMetadata, sourceID)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO dependencies
			(id, issue_id, type, created_by, depends_on_issue_id)
		VALUES ('88888888-9999-aaaa-bbbb-cccccccccccc', ?, 'blocks',
			'gc-formula', ?)
	`, downstreamStepID, stepID)
	require.NoError(t, err)
	priorStepMetadata := map[string]any{
		"gc.root_bead_id":   rootID,
		"gc.root_store_ref": "rig:gascity",
		"gc.step_ref":       "mol-focus-review.simplify",
	}
	oldStep, err := json.Marshal(map[string]any{
		"id": stepID, "status": "in_progress", "metadata": priorStepMetadata,
	})
	require.NoError(t, err)
	newStep, err := json.Marshal(map[string]any{
		"status": "closed", "metadata": stepMetadata,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'closed', 'polecat-gc-648490', ?, ?, ?)
	`, stepTransitionID, stepID, oldStep, newStep,
		fixture.clock.Now().In(time.Local).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, findings)

	fixture.clock.Advance(time.Minute)
	_, err = fixture.db.ExecContext(ctx, `
		UPDATE issues
		   SET status = 'closed', close_reason = 'Source work completed.'
		 WHERE id = ?
	`, sourceID)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'closed', 'polecat-gc-648490', '',
			'Source work completed.', ?)
	`, sourceTransitionID, sourceID,
		fixture.clock.Now().In(time.Local).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err = fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []SilentOutcome{{
		StoreRef:     "city:test",
		WorkID:       sourceID,
		Reason:       "terminal without outcome envelope",
		TransitionID: sourceTransitionID,
		TransitionAt: fixture.clock.Now(),
	}}, findings)
}

func TestDoltOutcomeStoreSurfacesExactReopenedRepairTaskCompletion(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		workID             = "dr-pu0k.1"
		verifiedEventID    = "019fb8a5-7806-7043-b373-37435b284b8f"
		closeEventID       = "019fb8a5-789a-777d-8b10-d780618cdb8c"
		reopenEventID      = "019fb8ab-353f-7e54-8fe1-60f38ae06a5f"
		completionEvidence = "Implemented and independently reviewed exact-transition non-outcome contract plus literal markerless-convoy exclusion; installed observer CLI and passed fresh canonical surface cycle with required positives preserved and dr-rnrl/gc-vhc0l excluded."
	)
	metadata := map[string]any{
		"installed_outcome_ready_sha256": "1828018e1bcc80cbff0cefb4103fce3b8d56ffeec51c2de25e49b1280d0e82b6",
		"review_findings":                0,
		"review_head":                    "279f768e36992a1d5f8d4a2ed9abcfe51cebaf67",
		"review_verdict":                 "pass",
		"surface_canary":                 "pass",
	}
	encodedMetadata, err := json.Marshal(metadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 status, close_reason, metadata)
		VALUES (?, 'Model superseded coordinator closes and stop recursive Outcome health alerts',
			'', '', '', '', 'open', '', ?)
	`, workID, encodedMetadata)
	require.NoError(t, err)
	oldVerified, err := json.Marshal(map[string]any{
		"id": workID, "status": "in_progress", "metadata": map[string]any{},
	})
	require.NoError(t, err)
	newVerified, err := json.Marshal(map[string]any{"metadata": metadata})
	require.NoError(t, err)
	closedSnapshot, err := json.Marshal(map[string]any{
		"id": workID, "status": "closed", "metadata": metadata,
	})
	require.NoError(t, err)
	reopenedSnapshot, err := json.Marshal(map[string]any{
		"status": "open", "metadata": metadata,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES
			(?, ?, 'updated', 'city-infra-pl', ?, ?, ?),
			(?, ?, 'closed', 'city-infra-pl', '', ?, ?),
			(?, ?, 'reopened', 'mayor', ?, ?, ?)
	`, verifiedEventID, workID, oldVerified, newVerified,
		fixture.clock.Now().In(time.Local).Format(doltDateTimeLayout),
		closeEventID, workID, completionEvidence,
		fixture.clock.Now().In(time.Local).Format(doltDateTimeLayout),
		reopenEventID, workID, closedSnapshot, reopenedSnapshot,
		fixture.clock.Now().Add(time.Minute).In(time.Local).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []SilentOutcome{{
		StoreRef:     "city:test",
		WorkID:       workID,
		Reason:       "terminal without outcome envelope",
		TransitionID: closeEventID,
		TransitionAt: fixture.clock.Now(),
	}}, findings)
}

func TestDoltOutcomeStoreSurfacesExactPostRolloutSourceWorkClose(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		workID       = "dr-4mte.2"
		transitionID = "019fb837-e83d-7d4c-962a-e79f538d38d3"
		closeReason  = "S2 Sourcegraph agent access configured and verified"
	)
	transitionAt := time.Date(2026, 7, 31, 12, 48, 19, 0, time.UTC)
	eventLocation, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 status, close_reason, metadata, updated_at, closed_at)
		VALUES (?, 'Replace Gas City agent code search with S2 Sourcegraph',
			'', '', '', '', 'closed', ?, '{}', ?, ?)
	`, workID, closeReason, transitionAt, transitionAt)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'closed', 'sjarmak', '', ?, ?)
	`, transitionID, workID, closeReason,
		transitionAt.In(eventLocation).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []SilentOutcome{{
		StoreRef:     "city:test",
		WorkID:       workID,
		Reason:       "terminal without outcome envelope",
		TransitionID: transitionID,
		TransitionAt: transitionAt,
	}}, findings)
}

func TestDoltOutcomeStoreSkipsExactSyntheticConvoyButSurfacesItsSourceWork(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		sourceID            = "gc-pzamd"
		syntheticID         = "gc-lv6dn"
		sourceTransition    = "019fb85a-2fc9-7a57-9b6f-7efb922169b9"
		syntheticTransition = "019fb85a-58c4-73ab-ac86-d13b6486886f"
		sourceReason        = "Fixed at reviewed branch-ready commit 855e85fb92f79e10d05ffb2619694c220a126193; human pushes."
		syntheticReason     = "convoy autoclose: all children closed"
	)
	transitionAt := time.Date(2026, 7, 31, 13, 25, 46, 0, time.UTC)
	eventLocation, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	sourceMetadata, err := json.Marshal(map[string]any{
		"gc.work_dir": "/srv/city/worktrees/polecat-3",
	})
	require.NoError(t, err)
	syntheticMetadata, err := json.Marshal(map[string]any{
		"gc.synthetic": "true",
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 status, close_reason, metadata, updated_at, closed_at)
		VALUES
			(?, 'Source work', '', '', '', '', 'closed', ?, ?, ?, ?),
			(?, 'Synthetic convoy', '', '', '', '', 'closed', ?, ?, ?, ?)
	`, sourceID, sourceReason, sourceMetadata, transitionAt, transitionAt,
		syntheticID, syntheticReason, syntheticMetadata,
		transitionAt.Add(10*time.Second), transitionAt.Add(10*time.Second))
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES
			(?, ?, 'closed', 'polecat-gc-627935', '', ?, ?),
			(?, ?, 'closed', 'gascity', '', ?, ?)
	`, sourceTransition, sourceID, sourceReason,
		transitionAt.In(eventLocation).Format(doltDateTimeLayout),
		syntheticTransition, syntheticID, syntheticReason,
		transitionAt.Add(10*time.Second).In(eventLocation).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []SilentOutcome{{
		StoreRef:     "city:test",
		WorkID:       sourceID,
		Reason:       "terminal without outcome envelope",
		TransitionID: sourceTransition,
		TransitionAt: transitionAt,
	}}, findings)
}

func TestDoltOutcomeStoreSkipsExactAutocloseConvoyWithoutSyntheticMarker(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		sourceID            = "gc-md9ol"
		syntheticID         = "gc-vhc0l"
		sourceTransition    = "019fb86e-77b6-74d0-82bd-c7b97370f4c3"
		syntheticTransition = "019fb86e-85b5-7654-b571-a2e8f1ecfcf7"
		sourceReason        = "PASS: independent exact-head verification of gc-0ychy at 96f1f2f11b95fab6d426ee9a2ca8b78df76b78ba."
		syntheticReason     = "convoy autoclose: all children closed"
	)
	transitionAt := time.Date(2026, 7, 31, 13, 47, 55, 0, time.UTC)
	eventLocation, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	sourceMetadata, err := json.Marshal(map[string]any{
		"gc.work_dir": "/srv/city/worktrees/polecat-2",
	})
	require.NoError(t, err)
	syntheticMetadata, err := json.Marshal(map[string]any{
		"close_reason": syntheticReason,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 issue_type, status, close_reason, metadata, updated_at, closed_at)
		VALUES
			(?, 'Independent exact-head review', '', '', '', '',
			 'task', 'closed', ?, ?, ?, ?),
			(?, 'Synthetic review convoy', '', '', '', '',
			 'convoy', 'closed', ?, ?, ?, ?)
	`, sourceID, sourceReason, sourceMetadata, transitionAt, transitionAt,
		syntheticID, syntheticReason, syntheticMetadata,
		transitionAt.Add(3*time.Second), transitionAt.Add(3*time.Second))
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES
			(?, ?, 'closed', 'polecat-gc-647533', '', ?, ?),
			(?, ?, 'closed', 'gascity', '', ?, ?)
	`, sourceTransition, sourceID, sourceReason,
		transitionAt.In(eventLocation).Format(doltDateTimeLayout),
		syntheticTransition, syntheticID, syntheticReason,
		transitionAt.Add(3*time.Second).In(eventLocation).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []SilentOutcome{{
		StoreRef:     "city:test",
		WorkID:       sourceID,
		Reason:       "terminal without outcome envelope",
		TransitionID: sourceTransition,
		TransitionAt: transitionAt,
	}}, findings)
}

func TestDoltOutcomeStoreSkipsExactDispositionedSelfAuditTransition(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		workID            = "dr-rnrl"
		firstCloseID      = "019fb873-b2a6-7735-9a5a-31a7940a61a4"
		dispositionNoteID = "019fb875-1841-7886-a5e3-ed5c74465dd2"
		reopenID          = "019fb87a-e40c-7b5d-88e7-c32a8d3b13ec"
		secondCloseID     = "019fb87d-c476-7c46-8965-2a9b0a88db53"
		firstCloseReason  = "Fixed and verified both Codex account launch paths"
		secondCloseReason = "Fixed the actual login-shell launchers and verified both commands end-to-end"
	)
	firstCloseAt := time.Date(2026, 7, 31, 13, 53, 37, 0, time.UTC)
	dispositionAt := time.Date(2026, 7, 31, 13, 55, 9, 0, time.UTC)
	reopenAt := time.Date(2026, 7, 31, 14, 1, 29, 0, time.UTC)
	secondCloseAt := time.Date(2026, 7, 31, 14, 4, 37, 0, time.UTC)
	eventLocation, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 issue_type, status, close_reason, metadata, updated_at, closed_at)
		VALUES (?, 'Repair Sourcegraph launch paths', '', '', '',
			'Second root cause and repair', 'task', 'closed', ?, '{}', ?, ?)
	`, workID, secondCloseReason, secondCloseAt, secondCloseAt)
	require.NoError(t, err)
	closedSnapshot, err := json.Marshal(map[string]any{
		"id": workID, "status": "closed", "metadata": map[string]any{},
	})
	require.NoError(t, err)
	disposition, err := json.Marshal(map[string]any{
		"contract_version": 1,
		"disposition":      "non-outcome",
		"work_id":          workID,
		"transition_id":    secondCloseID,
		"recorded_by":      "mayor",
		"recorded_at":      "2026-07-31T14:05:00Z",
	})
	require.NoError(t, err)
	metadata, err := json.Marshal(map[string]any{
		"gc.coordinator_outcome.non_outcome": string(disposition),
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		UPDATE issues SET metadata = ? WHERE id = ?
	`, metadata, workID)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES
			(?, ?, 'closed', 'sjarmak', '', ?, ?),
			(?, ?, 'updated', 'mayor', ?, '{"notes":"HEALTH DISPOSITION: prior close is a true missing producer"}', ?),
			(?, ?, 'reopened', 'sjarmak', ?, '{"status":"open"}', ?),
			(?, ?, 'closed', 'sjarmak', '', ?, ?)
	`, firstCloseID, workID, firstCloseReason,
		firstCloseAt.In(eventLocation).Format(doltDateTimeLayout),
		dispositionNoteID, workID, closedSnapshot,
		dispositionAt.In(eventLocation).Format(doltDateTimeLayout),
		reopenID, workID, closedSnapshot,
		reopenAt.In(eventLocation).Format(doltDateTimeLayout),
		secondCloseID, workID, secondCloseReason,
		secondCloseAt.In(eventLocation).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, findings)

	var staleDisposition map[string]any
	require.NoError(t, json.Unmarshal(disposition, &staleDisposition))
	staleDisposition["transition_id"] = firstCloseID
	disposition, err = json.Marshal(staleDisposition)
	require.NoError(t, err)
	metadata, err = json.Marshal(map[string]any{
		"gc.coordinator_outcome.non_outcome": string(disposition),
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		UPDATE issues SET metadata = ? WHERE id = ?
	`, metadata, workID)
	require.NoError(t, err)
	findings, err = fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Equal(t, "malformed-outcome-candidate", findings[0].Kind)
	require.Equal(t, secondCloseID, findings[0].TransitionID)
	require.Contains(t, findings[0].Error, "does not match qualifying transition")
}

func TestDoltOutcomeStoreSurfacesExactUnmarkedCoordinatorSupersessionClose(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		workID       = "gc-1xtzg"
		transitionID = "019fb88e-c186-783d-b112-203a2f8642e7"
		closeReason  = "Superseded by routable city coordinator bead dr-9yt0 after cross-store fail-closed"
	)
	transitionAt := time.Date(2026, 7, 31, 14, 23, 11, 0, time.UTC)
	eventLocation, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	metadata, err := json.Marshal(map[string]any{
		"gc.base_ref": "main",
		"gc.work_dir": "/srv/city/worktrees/city-1xtzg",
		"work_dir":    "/srv/city/worktrees/city-1xtzg",
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 issue_type, status, close_reason, metadata,
			 updated_at, closed_at)
		VALUES (?, 'Reconcile de-sequenced polecat ready queue', '', '', '',
			'Cross-store dispatch failed closed; superseded by dr-9yt0.',
			'bug', 'closed', ?, ?, ?, ?)
	`, workID, closeReason, metadata, transitionAt, transitionAt)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'closed', 'mayor', '', ?, ?)
	`, transitionID, workID, closeReason,
		transitionAt.In(eventLocation).Format(doltDateTimeLayout))
	require.NoError(t, err)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []SilentOutcome{{
		StoreRef:     "city:test",
		WorkID:       workID,
		Reason:       "terminal without outcome envelope",
		TransitionID: transitionID,
		TransitionAt: transitionAt,
	}}, findings)
}

func TestDoltOutcomeStoreRolloutEpochExcludesLegacyHistory(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	fixture.store.outcomeRolloutEpoch = time.Date(
		2026, 7, 30, 20, 30, 0, 0, time.UTC,
	)
	_, err := fixture.db.ExecContext(ctx, `
		UPDATE issues
		   SET status = 'closed',
		       updated_at = '2026-07-30 20:00:00',
		       metadata = JSON_SET(metadata, '$."evidence.reviewer_verdict"', 'pass')
		 WHERE id = ?`,
		fixture.beadID,
	)
	require.NoError(t, err)
	insertOutcomeTransitionEvent(
		t,
		fixture,
		"77777777-7777-7777-7777-777777777777",
		fixture.beadID,
		"open",
		map[string]any{},
		"closed",
		map[string]any{"evidence.reviewer_verdict": "pass"},
		time.Date(2026, 7, 30, 20, 0, 0, 0, time.UTC),
	)
	silent, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, silent)

	_, err = fixture.db.ExecContext(ctx, `
		UPDATE issues
		   SET updated_at = '2026-07-30 21:00:00'
		 WHERE id = ?`,
		fixture.beadID,
	)
	require.NoError(t, err)
	insertOutcomeTransitionEvent(
		t,
		fixture,
		"88888888-8888-8888-8888-888888888888",
		fixture.beadID,
		"closed",
		map[string]any{"evidence.reviewer_verdict": "pass"},
		"closed",
		map[string]any{
			"evidence.reviewer_verdict": "pass",
			"unrelated.note":            "legacy note after rollout",
		},
		time.Date(2026, 7, 30, 21, 0, 0, 0, time.UTC),
	)
	silent, err = fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, silent)

	_, err = fixture.db.ExecContext(ctx, `
		UPDATE issues
		   SET updated_at = '2026-07-30 22:00:00'
		 WHERE id = ?`,
		fixture.beadID,
	)
	require.NoError(t, err)
	insertOutcomeTransitionEvent(
		t,
		fixture,
		"99999999-8888-8888-8888-888888888888",
		fixture.beadID,
		"open",
		map[string]any{},
		"closed",
		map[string]any{"evidence.reviewer_verdict": "pass"},
		time.Date(2026, 7, 30, 22, 0, 0, 0, time.UTC),
	)
	silent, err = fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []SilentOutcome{{
		StoreRef:     "city:test",
		WorkID:       fixture.beadID,
		Reason:       "terminal without outcome envelope",
		TransitionID: "99999999-8888-8888-8888-888888888888",
		TransitionAt: time.Date(2026, 7, 30, 22, 0, 0, 0, time.UTC),
	}}, silent)
}

func TestDoltLegacyOutcomeUsesUTCForCandidatesAndHostLocalEvents(
	t *testing.T,
) {
	workerLocation, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	previousLocation := time.Local
	time.Local = workerLocation
	t.Cleanup(func() { time.Local = previousLocation })

	tests := []struct {
		name              string
		rollout           time.Time
		postIssueUTC      string
		postEventLocal    string
		postEventUTC      time.Time
		preEventLocal     string
		preCandidateUTC   string
		preCandidateLocal string
	}{
		{
			name:              "EDT",
			rollout:           time.Date(2026, 7, 31, 3, 15, 0, 0, time.UTC),
			postIssueUTC:      "2026-07-31 06:57:13",
			postEventLocal:    "2026-07-31 02:57:13",
			postEventUTC:      time.Date(2026, 7, 31, 6, 57, 13, 0, time.UTC),
			preEventLocal:     "2026-07-30 23:14:59",
			preCandidateUTC:   "2026-07-31 03:14:59",
			preCandidateLocal: "2026-07-31 02:58:13",
		},
		{
			name:              "EST",
			rollout:           time.Date(2026, 1, 15, 3, 15, 0, 0, time.UTC),
			postIssueUTC:      "2026-01-15 07:57:13",
			postEventLocal:    "2026-01-15 02:57:13",
			postEventUTC:      time.Date(2026, 1, 15, 7, 57, 13, 0, time.UTC),
			preEventLocal:     "2026-01-14 22:14:59",
			preCandidateUTC:   "2026-01-15 03:14:59",
			preCandidateLocal: "2026-01-15 02:58:13",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newIsolatedDoltBeadStoreWithServerTimezone(
				t,
				"America/New_York",
			)
			require.Equal(
				t,
				"America/New_York",
				fixture.store.eventTimeLocation.String(),
			)
			ctx := context.Background()
			seed := func(
				workID string,
				issueUpdatedAt string,
				eventCreatedAt string,
			) {
				seedCanonicalIssue(t, fixture.db, workID)
				_, err := fixture.db.ExecContext(ctx, `
					UPDATE issues
					   SET status = 'closed',
					       updated_at = ?,
					       metadata = JSON_OBJECT(
					         'gc.outcome.generation',
					         '1'
					       )
					 WHERE id = ?`,
					issueUpdatedAt,
					workID,
				)
				require.NoError(t, err)
				oldValue, err := json.Marshal(map[string]any{
					"id": workID, "status": "open",
					"metadata": map[string]any{
						"gc.outcome.generation": "1",
					},
				})
				require.NoError(t, err)
				newValue, err := json.Marshal(map[string]any{
					"id": workID, "status": "closed",
					"metadata": map[string]any{
						"gc.outcome.generation": "1",
					},
				})
				require.NoError(t, err)
				_, err = fixture.db.ExecContext(ctx, `
					INSERT INTO events
						(id, issue_id, event_type, actor,
						 old_value, new_value, created_at)
					VALUES
						(UUID(), ?, 'closed', 'direct-worker',
						 ?, ?, ?)`,
					workID,
					oldValue,
					newValue,
					eventCreatedAt,
				)
				require.NoError(t, err)
			}
			seed("dr-post-rollout", test.postIssueUTC, test.postEventLocal)
			seed("dr-pre-event", test.postIssueUTC, test.preEventLocal)
			seed(
				"dr-pre-candidate",
				test.preCandidateUTC,
				test.preCandidateLocal,
			)

			restarted, err := OpenDoltBeadStore(ctx, fixture.config)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, restarted.Close()) })
			restarted.outcomeRolloutEpoch = test.rollout
			envelopes, err := restarted.DiscoverLegacyOutcomes(ctx)
			require.NoError(t, err)
			require.Len(t, envelopes, 1)
			require.Equal(t, "dr-post-rollout", envelopes[0].WorkID)
			require.Equal(t, test.postEventUTC, envelopes[0].OccurredAt)
		})
	}
}

func TestDoltLegacyOutcomeRejectsWrongConfiguredServerEventTimezone(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStoreWithServerTimezone(
		t,
		"America/New_York",
	)
	config := fixture.config
	config.EventTimeZone = "America/Indiana/Indianapolis"

	store, err := OpenDoltBeadStore(context.Background(), config)
	require.Nil(t, store)
	require.ErrorContains(t, err, "event time zone")
	require.ErrorContains(t, err, "America/Indiana/Indianapolis")
	require.ErrorContains(t, err, "America/New_York")
}

func TestDoltOutcomeStoreUsesTypedMissingOutcomeError(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	_, err := fixture.store.InspectOutcome(context.Background(), fixture.beadID)
	require.ErrorIs(t, err, ErrOutcomeNotFound)
	require.NotErrorIs(t, err, ErrBeadNotFound)
}

func TestDoltTemporalCompletionAtomicallyEmitsOutcomeReady(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	_, err := fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 1, validFormulaRef(),
	)
	require.NoError(t, err)
	workflowID, err := WorkflowID("city", "run", fixture.beadID)
	require.NoError(t, err)
	lease, err := fixture.store.Claim(ctx, ClaimRequest{
		BeadID: fixture.beadID, Generation: 1, WorkflowID: workflowID,
	})
	require.NoError(t, err)
	completion := Completion{
		BeadID: fixture.beadID, Generation: 1, ClaimToken: lease.Token,
		SessionID: "temporal-agent-session", Outcome: OutcomeCompleted,
		SourceWorkflowID: workflowID, SourceWorkflowRunID: "temporal-run-1",
		ArtifactRefs: []ArtifactRef{testArtifact()},
	}

	require.NoError(t, fixture.store.Complete(ctx, completion))

	record, err := fixture.store.InspectOutcome(ctx, fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, OutcomeCoordinatorPending, record.State)
	require.Equal(t, OutcomeProducerTemporal, record.Envelope.Producer)
	require.Equal(t, fixture.config.OutcomeStoreRef, record.Envelope.StoreRef)
	require.Equal(t, fixture.beadID, record.Envelope.WorkID)
	require.Equal(t, validFormulaRef().RootID, record.Envelope.SourceRootID)
	require.Equal(t, validFormulaRef().StepKey, record.Envelope.StepKey)
	require.Equal(t, workflowID, record.Envelope.WorkflowID)
	require.Equal(t, completion.SourceWorkflowRunID, record.Envelope.WorkflowRunID)
	require.NotEqual(t, "run", record.Envelope.WorkflowRunID)
	require.Equal(t, OutcomeEvidenceTerminal, record.Envelope.State)
	require.Equal(t, completion.ArtifactRefs, record.Envelope.Evidence)
	require.Equal(t, OutcomeProducerFence{
		ProducerID: completion.SessionID,
		Generation: completion.Generation,
		Token:      completion.ClaimToken,
	}, record.Envelope.Fence)
	require.Equal(t, fixture.clock.Now(), record.PendingAt)

	var completeEvents int
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = 'closed'",
		fixture.beadID,
	).Scan(&completeEvents))
	require.Equal(t, 1, completeEvents)
	pending, err := fixture.store.PendingOutcomes(ctx)
	require.NoError(t, err)
	require.Equal(t, []OutcomeRecord{record}, pending)
}

func TestDoltTemporalRecompletionPreservesOlderUnacknowledgedOutcome(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	_, err := fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 1, validFormulaRef(),
	)
	require.NoError(t, err)
	firstLease, err := fixture.store.Claim(ctx, ClaimRequest{
		BeadID: fixture.beadID, Generation: 1, WorkflowID: "workflow-first",
	})
	require.NoError(t, err)
	require.NoError(t, fixture.store.Complete(ctx, Completion{
		BeadID: fixture.beadID, Generation: 1, ClaimToken: firstLease.Token,
		SessionID: "session-first", Outcome: OutcomeCompleted,
		SourceWorkflowID: "workflow-first", SourceWorkflowRunID: "temporal-run-first",
		ArtifactRefs: []ArtifactRef{testArtifact()},
	}))
	first, err := fixture.store.InspectOutcome(ctx, fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, OutcomeCoordinatorPending, first.State)

	fixture.clock.Advance(time.Minute)
	_, err = fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 2, validFormulaRef(),
	)
	require.NoError(t, err)
	secondLease, err := fixture.store.Claim(ctx, ClaimRequest{
		BeadID: fixture.beadID, Generation: 2, WorkflowID: "workflow-second",
	})
	require.NoError(t, err)
	err = fixture.store.Complete(ctx, Completion{
		BeadID: fixture.beadID, Generation: 2, ClaimToken: secondLease.Token,
		SessionID: "session-second", Outcome: OutcomeCompleted,
		SourceWorkflowID: "workflow-second", SourceWorkflowRunID: "temporal-run-second",
		ArtifactRefs: []ArtifactRef{testArtifact()},
	})
	require.ErrorContains(t, err, "unacknowledged")

	preserved, err := fixture.store.InspectOutcome(ctx, fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, first, preserved)
	canonical, err := fixture.store.Inspect(ctx, fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusClaimed, canonical.Status)
	require.Equal(t, int64(2), canonical.Generation)
	require.Empty(t, canonical.Outcome)
}

func insertOutcomeTransitionEvent(
	t *testing.T,
	fixture *isolatedDoltBeadStore,
	eventID string,
	workID string,
	oldStatus string,
	oldMetadata map[string]any,
	newStatus string,
	newMetadata map[string]any,
	createdAt time.Time,
) {
	t.Helper()
	oldValue, err := json.Marshal(map[string]any{
		"id": workID, "status": oldStatus, "metadata": oldMetadata,
	})
	require.NoError(t, err)
	newValue, err := json.Marshal(map[string]any{
		"id": workID, "status": newStatus, "metadata": newMetadata,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(context.Background(), `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'updated', 'test-producer', ?, ?, ?)
	`,
		eventID,
		workID,
		oldValue,
		newValue,
		createdAt.In(time.Local).Format(doltDateTimeLayout),
	)
	require.NoError(t, err)
}
