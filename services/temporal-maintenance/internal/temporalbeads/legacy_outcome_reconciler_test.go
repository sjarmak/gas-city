package temporalbeads

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLegacyOutcomeReconcilerEmitsRepresentativeLiveLanes(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	lanes := []struct {
		workID   string
		status   string
		actor    string
		eventID  string
		metadata map[string]any
		producer OutcomeProducerKind
	}{
		{
			workID: "dr-direct", status: "closed",
			actor: "pool-worker", eventID: "11111111-1111-1111-1111-111111111111",
			metadata: map[string]any{
				"gc.outcome": "pass", "summary_for_human": "Direct worker finished.",
			},
			producer: OutcomeProducerDirectWorker,
		},
		{
			workID: "dr-formula-step", status: "in_progress",
			actor: "formula-worker", eventID: "22222222-2222-2222-2222-222222222222",
			metadata: map[string]any{
				"gc.root_bead_id":           "dr-formula-root",
				"gc.root_store_ref":         "city:ds-research",
				"gc.step_ref":               "mol-do-work.verify",
				"evidence.reviewer_verdict": "pass",
				"summary_for_human":         "Formula verification passed.",
			},
			producer: OutcomeProducerFormulaStep,
		},
		{
			workID: "dr-pl-result", status: "open",
			actor: "research-pl", eventID: "33333333-3333-3333-3333-333333333333",
			metadata: map[string]any{
				"gc.outcome.producer": "project-lead",
				"review_verdict":      "pass",
				"summary_for_human":   "Project lead verified the result.",
			},
			producer: OutcomeProducerProjectLead,
		},
		{
			workID: "dr-order-result", status: "closed",
			actor: "order:managed-dolt-maintenance", eventID: "44444444-4444-4444-4444-444444444444",
			metadata: map[string]any{
				"gc.outcome":        "completed",
				"summary_for_human": "Maintenance order completed.",
			},
			producer: OutcomeProducerMaintenanceOrder,
		},
	}
	for _, lane := range lanes {
		seedLegacyOutcomeTransition(
			t,
			fixture,
			lane.workID,
			lane.status,
			lane.actor,
			lane.eventID,
			lane.metadata,
		)
	}
	addLegacyOutcomeNoteEvent(
		t,
		fixture,
		"dr-formula-step",
		"99999999-9999-9999-9999-999999999999",
		lanes[1].metadata,
	)

	gateway := &recordingOutcomeGateway{}
	reconciler := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}
	result, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, result.Produced)
	require.Equal(t, 4, result.Scanned)
	require.Equal(t, 4, result.Signalled)

	for _, lane := range lanes {
		record, err := fixture.store.InspectOutcome(ctx, lane.workID)
		require.NoError(t, err)
		require.Equal(t, lane.producer, record.Envelope.Producer)
		require.Equal(t, lane.eventID, record.Envelope.Fence.Token)
		require.Equal(t, OutcomeCoordinatorPending, record.State)
	}
	var formulaStatus string
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT status FROM issues WHERE id = 'dr-formula-step'",
	).Scan(&formulaStatus))
	require.Equal(t, "in_progress", formulaStatus)
}

func TestLegacyOutcomeReconcilerEmitsExactReviewedP0SourceClose(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		workID       = "dr-q8sv.2.12"
		transitionID = "019fb8c9-95ce-7c76-bc2d-3db0aed5731f"
		closeReason  = "Implemented reviewed additive oomd victim protection and uncontained-memory detection; live-loaded without restart and verified supervisor/Temporal/Qdrant continuity."
	)
	transitionAt := time.Date(2026, 7, 31, 15, 27, 26, 0, time.UTC)
	eventLocation, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	metadata, err := json.Marshal(map[string]any{
		"review_findings":               0,
		"review_verdict":                "pass",
		"reviewed_sampler_sha256":       "73d51e489ee9009e4e8f5115bba6a34715a34da3d7407680030fa970c3b593ad",
		"reviewed_server_dropin_sha256": "eae57f4548f2fb011c2f2415838d23d58a439ecd1eb62918f4293de2454d943d",
		"reviewed_test_sha256":          "aa74ccb1839a6360c4dd1ad63acfd6c9042985e814f2c59356a07bb317be7242",
		"reviewed_worker_dropin_sha256": "a4e840fe3892eaa025f04ed0fef8b021d8d7d9f03e1a5c37cba8b0e0e8f95019",
		"rollout_state":                 "active_no_restart",
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 issue_type, status, close_reason, metadata, updated_at, closed_at)
		VALUES (?, 'Protect maintenance services from unrelated user-session memory pressure',
			'', '', '', '', 'bug', 'closed', ?, ?, ?, ?)
	`, workID, closeReason, metadata, transitionAt, transitionAt)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'closed', 'sjarmak', '', ?, ?)
	`, transitionID, workID, closeReason,
		transitionAt.In(eventLocation).Format(doltDateTimeLayout))
	require.NoError(t, err)

	gateway := &recordingOutcomeGateway{}
	reconciler := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}
	result, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Scanned)
	require.Equal(t, 1, result.Produced)
	require.Equal(t, 1, result.Signalled)
	require.Len(t, gateway.envelopes, 1)

	record, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	require.Equal(t, OutcomeCoordinatorPending, record.State)
	require.Equal(t, OutcomeProducerDirectWorker, record.Envelope.Producer)
	require.Equal(t, OutcomeEvidenceTerminal, record.Envelope.State)
	require.Equal(t, workID, record.Envelope.WorkID)
	require.Equal(t, workID, record.Envelope.SourceRootID)
	require.Equal(t, transitionID, record.Envelope.Fence.Token)
	require.Equal(t, transitionAt, record.Envelope.OccurredAt)
	require.Equal(t, closeReason, record.Envelope.Summary)
	require.Equal(t, "bead:"+workID+"@"+transitionID,
		record.Envelope.Evidence[0].URI)

	second, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, second.Scanned)
	require.Zero(t, second.Produced)
	require.Zero(t, second.Signalled)
	require.Len(t, gateway.envelopes, 1)
}

type websiteSourceCloseFixture struct {
	workID      string
	title       string
	issueType   string
	closeReason string
	eventID     string
	eventAt     time.Time
	updatedAt   time.Time
}

var exactWebsiteSourceCloses = []websiteSourceCloseFixture{
	{
		workID: "sjai-629", title: "Show estimated page count for coding agents book",
		issueType: "task", closeReason: "Estimated 356-page count published and verified.",
		eventID:   "019fb8dd-6df2-7a05-8d41-c05e865bffbb",
		eventAt:   time.Date(2026, 7, 31, 15, 49, 7, 0, time.UTC),
		updatedAt: time.Date(2026, 7, 31, 15, 49, 7, 0, time.UTC),
	},
	{
		workID: "sjai-dzq", title: "Add explorable book and companion knowledge graph",
		issueType: "feature", closeReason: "Published and production-verified",
		eventID:   "019fb8f6-e838-7eac-87ef-294835998f4a",
		eventAt:   time.Date(2026, 7, 31, 16, 16, 56, 0, time.UTC),
		updatedAt: time.Date(2026, 7, 31, 16, 16, 57, 0, time.UTC),
	},
	{
		workID: "sjai-kpr", title: "Rename coding agents book across manuscript and website",
		issueType: "task", closeReason: "Canonical title updated and title-only website release verified in production.",
		eventID:   "019fb8d0-9ec5-7038-859c-ccae114264bd",
		eventAt:   time.Date(2026, 7, 31, 15, 35, 7, 0, time.UTC),
		updatedAt: time.Date(2026, 7, 31, 15, 35, 8, 0, time.UTC),
	},
}

func TestLegacyOutcomeReconcilerRepairsExactWebsiteSourceCloses(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	rollout := time.Date(2026, 7, 31, 3, 15, 0, 0, time.UTC)
	fixture.store.outcomeRolloutEpoch = rollout
	fixture.store.outcomeStoreRef = "rig:website"
	eventLocation, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	for _, close := range exactWebsiteSourceCloses {
		seedWebsiteSourceClose(t, fixture, ctx, eventLocation, close)
	}
	seedPreRolloutWebsiteClose(t, fixture, ctx, eventLocation, rollout)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	requireExactWebsiteSilentFindings(t, findings)

	gateway := &recordingOutcomeGateway{}
	reconciler := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}
	result, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, OutcomeReconcileResult{Produced: 3, Scanned: 3, Signalled: 3}, result)
	require.Len(t, gateway.envelopes, 3)
	requireExactWebsiteOutcomeRecords(t, fixture, ctx)

	findings, err = fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Empty(t, findings)
	second, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, OutcomeReconcileResult{Scanned: 3}, second)
	require.Len(t, gateway.envelopes, 3)
}

func seedWebsiteSourceClose(
	t *testing.T,
	fixture *isolatedDoltBeadStore,
	ctx context.Context,
	eventLocation *time.Location,
	close websiteSourceCloseFixture,
) {
	t.Helper()
	_, err := fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 issue_type, status, close_reason, metadata, updated_at, closed_at)
		VALUES (?, ?, '', '', '', '', ?, 'closed', ?, '{}', ?, ?)
	`, close.workID, close.title, close.issueType, close.closeReason,
		close.updatedAt, close.updatedAt)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'closed', 'sjarmak', '', ?, ?)
	`, close.eventID, close.workID, close.closeReason,
		close.eventAt.In(eventLocation).Format(doltDateTimeLayout))
	require.NoError(t, err)
}

func seedPreRolloutWebsiteClose(
	t *testing.T,
	fixture *isolatedDoltBeadStore,
	ctx context.Context,
	eventLocation *time.Location,
	rollout time.Time,
) {
	t.Helper()
	const workID = "sjai-pre-rollout"
	_, err := fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 issue_type, status, close_reason, metadata, updated_at, closed_at)
		VALUES (?, 'Historical website close', '', '', '', '', 'task', 'closed',
			'Historical deployment.', '{}', ?, ?)
	`, workID, rollout.Add(time.Hour), rollout.Add(time.Hour))
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES ('019fb000-0000-7000-8000-000000000001', ?, 'closed',
			'sjarmak', '', 'Historical deployment.', ?)
	`, workID, rollout.Add(-time.Second).In(eventLocation).Format(doltDateTimeLayout))
	require.NoError(t, err)
}

func requireExactWebsiteSilentFindings(t *testing.T, findings []SilentOutcome) {
	t.Helper()
	require.Len(t, findings, 3)
	findingsByWork := make(map[string]SilentOutcome, len(findings))
	for _, finding := range findings {
		findingsByWork[finding.WorkID] = finding
	}
	for _, close := range exactWebsiteSourceCloses {
		finding, found := findingsByWork[close.workID]
		require.True(t, found)
		require.Equal(t, "rig:website", finding.StoreRef)
		require.Equal(t, close.eventID, finding.TransitionID)
		require.Equal(t, close.eventAt, finding.TransitionAt)
	}
	require.NotContains(t, findingsByWork, "sjai-pre-rollout")
}

func requireExactWebsiteOutcomeRecords(
	t *testing.T,
	fixture *isolatedDoltBeadStore,
	ctx context.Context,
) {
	t.Helper()
	for _, close := range exactWebsiteSourceCloses {
		record, err := fixture.store.InspectOutcome(ctx, close.workID)
		require.NoError(t, err)
		require.Equal(t, "rig:website", record.Envelope.StoreRef)
		require.Equal(t, OutcomeProducerDirectWorker, record.Envelope.Producer)
		require.Equal(t, OutcomeEvidenceTerminal, record.Envelope.State)
		require.Equal(t, close.eventID, record.Envelope.Fence.Token)
		require.Equal(t, int64(1), record.Envelope.Fence.Generation)
		require.Equal(t, close.eventAt, record.Envelope.OccurredAt)
		require.Equal(t, close.closeReason, record.Envelope.Summary)
		require.Equal(t, OutcomeCoordinatorPending, record.State)
		var producedEvents int
		require.NoError(t, fixture.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM events
			 WHERE issue_id = ? AND new_value LIKE ?
		`, close.workID, "%"+record.Envelope.OutcomeID+"%").Scan(&producedEvents))
		require.Equal(t, 1, producedEvents)
	}
}

func TestLegacyOutcomeReconcilerQuarantinesPoisonCandidateWithinStore(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	poisonID := "dr-poison-candidate"
	healthyID := "dr-healthy-candidate"
	poisonEventID := "10101010-1010-1010-1010-101010101010"
	healthyEventID := "20202020-2020-2020-2020-202020202020"
	seedLegacyOutcomeTransition(
		t,
		fixture,
		poisonID,
		"closed",
		"pool-worker",
		poisonEventID,
		map[string]any{
			"gc.outcome.producer": "not-a-producer",
			"summary_for_human":   "Malformed result.",
		},
	)
	seedLegacyOutcomeTransition(
		t,
		fixture,
		healthyID,
		"closed",
		"pool-worker",
		healthyEventID,
		map[string]any{
			"gc.outcome":        "completed",
			"summary_for_human": "Healthy result.",
		},
	)

	gateway := &recordingOutcomeGateway{}
	reconciler := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}
	result, err := reconciler.Reconcile(ctx)
	require.ErrorContains(t, err, poisonID)
	require.ErrorContains(t, err, `explicit legacy producer "not-a-producer" is invalid`)
	require.Equal(t, 1, result.Produced)
	require.Equal(t, 1, result.Signalled)
	require.Len(t, gateway.envelopes, 1)
	require.Equal(t, healthyID, gateway.envelopes[0].WorkID)

	record, err := fixture.store.InspectOutcome(ctx, healthyID)
	require.NoError(t, err)
	require.Equal(t, healthyID, record.Envelope.WorkID)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Equal(t, poisonID, findings[0].WorkID)
	require.Equal(t, poisonEventID, findings[0].TransitionID)
	require.Contains(
		t,
		findings[0].Reason,
		`explicit legacy producer "not-a-producer" is invalid`,
	)
}

func TestLegacyOutcomeReconcilerSupersedesAcknowledgedRecompletionOnce(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	workID := "dr-recompleted"
	firstEventID := "21212121-2121-2121-2121-212121212121"
	nextEventID := "31313131-3131-3131-3131-313131313131"
	seedLegacyOutcomeTransition(
		t,
		fixture,
		workID,
		"closed",
		"worker-session-generation-1",
		firstEventID,
		map[string]any{
			"gc.outcome":            "completed",
			"gc.outcome.generation": "1",
			"summary_for_human":     "First completion.",
		},
	)
	gateway := &recordingOutcomeGateway{}
	reconciler := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}
	firstResult, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, firstResult.Produced)
	first, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: first.Envelope.OutcomeID, WorkID: workID,
		DeliveryRef:      "local:first-generation",
		CoordinatorFence: "mayor-first-generation",
	})
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeAcknowledged(
		ctx,
		OutcomeAcknowledgement{
			StoreRef:  first.Envelope.StoreRef,
			OutcomeID: first.Envelope.OutcomeID, WorkID: workID,
			CoordinatorFence: "mayor-first-generation",
			AcknowledgedBy:   "mayor",
		},
	)
	require.NoError(t, err)

	var currentRaw []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		workID,
	).Scan(&currentRaw))
	currentMetadata, err := decodeMetadata(currentRaw)
	require.NoError(t, err)
	oldMetadata := make(map[string]any, len(currentMetadata))
	for key, value := range currentMetadata {
		oldMetadata[key] = value
	}
	delete(oldMetadata, "gc.outcome")
	delete(oldMetadata, "summary_for_human")
	oldMetadata["gc.outcome.generation"] = "3"
	oldMetadata["gc.outcome.producer"] = "project-lead"
	newMetadata := make(map[string]any, len(oldMetadata)+2)
	for key, value := range oldMetadata {
		newMetadata[key] = value
	}
	newMetadata["gc.outcome"] = "completed"
	newMetadata["summary_for_human"] = "Recompletion at generation three."
	encodedCurrent, err := encodeMetadata(newMetadata)
	require.NoError(t, err)
	nextOccurredAt := first.Envelope.OccurredAt.Add(time.Minute)
	_, err = fixture.db.ExecContext(ctx, `
		UPDATE issues
		   SET status = 'closed', metadata = ?, updated_at = ?
		 WHERE id = ?
	`, encodedCurrent, nextOccurredAt, workID)
	require.NoError(t, err)
	oldSnapshot, err := json.Marshal(map[string]any{
		"id": workID, "status": "open", "metadata": oldMetadata,
	})
	require.NoError(t, err)
	newSnapshot, err := json.Marshal(map[string]any{
		"id": workID, "status": "closed", "metadata": newMetadata,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'closed', 'different-project-lead-session', ?, ?, ?)
	`, nextEventID, workID, oldSnapshot, newSnapshot, nextOccurredAt)
	require.NoError(t, err)

	secondResult, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, secondResult.Produced)
	require.Equal(t, 1, secondResult.Signalled)
	second, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	require.NotEqual(t, first.Envelope.OutcomeID, second.Envelope.OutcomeID)
	require.Equal(t, int64(3), second.Envelope.Fence.Generation)
	require.Equal(t, nextEventID, second.Envelope.Fence.Token)
	require.Equal(t, OutcomeProducerProjectLead, second.Envelope.Producer)

	signalCount := len(gateway.envelopes)
	thirdResult, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Zero(t, thirdResult.Produced)
	require.Zero(t, thirdResult.Signalled)
	require.Len(t, gateway.envelopes, signalCount)
	unchanged, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	require.Equal(t, second.Envelope.OutcomeID, unchanged.Envelope.OutcomeID)
}

func TestLegacyOutcomeReconcilerAdvancesAcknowledgedGenerationForExactNewReview(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		workID       = "dr-pu0k"
		firstEventID = "019fb7fb-c66e-724f-8a7f-ff5cdc25b7b0"
		nextEventID  = "019fb859-a057-72ae-a479-90c52d03213f"
	)
	seedLegacyOutcomeTransition(
		t,
		fixture,
		workID,
		"in_progress",
		"city-infra-pl",
		firstEventID,
		map[string]any{
			"gc.outcome.generation": "2",
			"review_head":           "61affc5c462faee1e5553b2fa62e30bed4382e31",
			"review_verdict":        "pass",
		},
	)
	gateway := &recordingOutcomeGateway{}
	reconciler := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}
	firstResult, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, firstResult.Produced)
	first, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: first.Envelope.OutcomeID, WorkID: workID,
		DeliveryRef:      "mail:first-review",
		CoordinatorFence: "gc-527414",
	})
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeAcknowledged(ctx, OutcomeAcknowledgement{
		StoreRef: first.Envelope.StoreRef, OutcomeID: first.Envelope.OutcomeID,
		WorkID: workID, CoordinatorFence: "gc-527414", AcknowledgedBy: "mayor",
	})
	require.NoError(t, err)

	var currentRaw []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT metadata FROM issues WHERE id = ?",
		workID,
	).Scan(&currentRaw))
	currentMetadata, err := decodeMetadata(currentRaw)
	require.NoError(t, err)
	oldMetadata := make(map[string]any, len(currentMetadata))
	for key, value := range currentMetadata {
		oldMetadata[key] = value
	}
	delete(oldMetadata, "review_head")
	delete(oldMetadata, "review_verdict")
	newMetadata := make(map[string]any, len(oldMetadata)+3)
	for key, value := range oldMetadata {
		newMetadata[key] = value
	}
	newMetadata["gc.outcome.generation"] = "2"
	newMetadata["gc.coordinator_outcome.disposition"] =
		"accepted-reviewed-head-proof-with-live-canary-defects"
	newMetadata["review_head"] = "762da1d0b1c27576501f9ccb852afd3ecb3c3b43"
	newMetadata["review_verdict"] = "pass"
	nextOccurredAt := first.Envelope.OccurredAt.Add(time.Minute)
	encodedCurrent, err := encodeMetadata(newMetadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		UPDATE issues
		   SET status = 'in_progress', metadata = ?, updated_at = ?
		 WHERE id = ?
	`, encodedCurrent, nextOccurredAt, workID)
	require.NoError(t, err)
	oldSnapshot, err := json.Marshal(map[string]any{
		"id": workID, "status": "in_progress", "metadata": oldMetadata,
	})
	require.NoError(t, err)
	newValue, err := json.Marshal(map[string]any{"metadata": newMetadata})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'updated', 'city-infra-pl', ?, ?, ?)
	`, nextEventID, workID, oldSnapshot, newValue, nextOccurredAt)
	require.NoError(t, err)

	secondResult, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, secondResult.Produced)
	require.Equal(t, 1, secondResult.Signalled)
	second, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	require.Equal(t, int64(3), second.Envelope.Fence.Generation)
	require.Equal(t, nextEventID, second.Envelope.Fence.Token)
	require.NotEqual(t, first.Envelope.OutcomeID, second.Envelope.OutcomeID)
}

func TestLegacyOutcomeReconcilerSkipsAcknowledgedMaxGenerationReplay(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		workID  = "dr-max-generation-replay"
		eventID = "41414141-4141-4141-4141-414141414141"
	)
	seedLegacyOutcomeTransition(t, fixture, workID, "closed", "pool-worker", eventID,
		map[string]any{
			"gc.outcome":            "completed",
			"gc.outcome.generation": fmt.Sprint(math.MaxInt64),
		})
	gateway := &recordingOutcomeGateway{}
	reconciler := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}
	firstResult, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, firstResult.Produced)
	first, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: first.Envelope.OutcomeID, WorkID: workID,
		DeliveryRef: "mail:max", CoordinatorFence: "gc-527414",
	})
	require.NoError(t, err)
	acknowledged, err := fixture.store.MarkOutcomeAcknowledged(
		ctx,
		OutcomeAcknowledgement{
			StoreRef: first.Envelope.StoreRef, OutcomeID: first.Envelope.OutcomeID,
			WorkID: workID, CoordinatorFence: "gc-527414", AcknowledgedBy: "mayor",
		},
	)
	require.NoError(t, err)

	replayResult, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Zero(t, replayResult.Produced)
	require.Zero(t, replayResult.Signalled)
	unchanged, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	require.Equal(t, acknowledged, unchanged)
}

func TestLegacyOutcomeReconcilerRejectsNewTransitionAfterMaxGeneration(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	const (
		workID       = "dr-max-generation-new-transition"
		firstEventID = "51515151-5151-5151-5151-515151515151"
		nextEventID  = "61616161-6161-6161-6161-616161616161"
	)
	seedLegacyOutcomeTransition(t, fixture, workID, "in_progress", "project-lead", firstEventID,
		map[string]any{
			"gc.outcome.generation": fmt.Sprint(math.MaxInt64),
			"review_verdict":        "pass",
		})
	gateway := &recordingOutcomeGateway{}
	reconciler := CoordinatorOutcomeReconciler{
		Source: fixture.store,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}
	firstResult, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, firstResult.Produced)
	first, err := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, err)
	_, err = fixture.store.MarkOutcomeDelivered(ctx, OutcomeDelivery{
		OutcomeID: first.Envelope.OutcomeID, WorkID: workID,
		DeliveryRef: "mail:max", CoordinatorFence: "gc-527414",
	})
	require.NoError(t, err)
	acknowledged, err := fixture.store.MarkOutcomeAcknowledged(
		ctx,
		OutcomeAcknowledgement{
			StoreRef: first.Envelope.StoreRef, OutcomeID: first.Envelope.OutcomeID,
			WorkID: workID, CoordinatorFence: "gc-527414", AcknowledgedBy: "mayor",
		},
	)
	require.NoError(t, err)

	var currentRaw []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx, "SELECT metadata FROM issues WHERE id = ?", workID,
	).Scan(&currentRaw))
	currentMetadata, err := decodeMetadata(currentRaw)
	require.NoError(t, err)
	oldMetadata := make(map[string]any, len(currentMetadata))
	for key, value := range currentMetadata {
		oldMetadata[key] = value
	}
	delete(oldMetadata, "review_verdict")
	newMetadata := make(map[string]any, len(oldMetadata)+1)
	for key, value := range oldMetadata {
		newMetadata[key] = value
	}
	newMetadata["review_verdict"] = "pass"
	nextOccurredAt := first.Envelope.OccurredAt.Add(time.Minute)
	encodedCurrent, err := encodeMetadata(newMetadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		UPDATE issues SET metadata = ?, updated_at = ? WHERE id = ?
	`, encodedCurrent, nextOccurredAt, workID)
	require.NoError(t, err)
	oldSnapshot, err := json.Marshal(map[string]any{
		"id": workID, "status": "in_progress", "metadata": oldMetadata,
	})
	require.NoError(t, err)
	newValue, err := json.Marshal(map[string]any{"metadata": newMetadata})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value, created_at)
		VALUES (?, ?, 'updated', 'project-lead', ?, ?, ?)
	`, nextEventID, workID, oldSnapshot, newValue, nextOccurredAt)
	require.NoError(t, err)

	result, err := reconciler.Reconcile(ctx)
	require.ErrorContains(t, err, "acknowledged legacy outcome generation is exhausted")
	require.Zero(t, result.Produced)
	require.Zero(t, result.Signalled)
	unchanged, inspectErr := fixture.store.InspectOutcome(ctx, workID)
	require.NoError(t, inspectErr)
	require.Equal(t, acknowledged, unchanged)
}

func TestLegacyOutcomeDiscoveryQuarantinesMalformedEventWithinStore(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	poisonID := "dr-poison-event"
	healthyID := "dr-healthy-after-event"
	poisonEventID := "30303030-3030-3030-3030-303030303030"
	healthyEventID := "40404040-4040-4040-4040-404040404040"
	metadata, err := json.Marshal(map[string]any{"gc.outcome": "completed"})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes, status, metadata)
		VALUES (?, 'poison event', '', '', '', '', 'closed', ?)
	`, poisonID, metadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value)
		VALUES (?, ?, 'updated', 'pool-worker', '{}', '{not-json')
	`, poisonEventID, poisonID)
	require.NoError(t, err)
	seedLegacyOutcomeTransition(
		t,
		fixture,
		healthyID,
		"closed",
		"pool-worker",
		healthyEventID,
		map[string]any{
			"gc.outcome":        "completed",
			"summary_for_human": "Healthy result after malformed event.",
		},
	)

	envelopes, err := fixture.store.DiscoverLegacyOutcomes(ctx)
	require.ErrorContains(t, err, poisonID)
	require.ErrorContains(t, err, "apply new transition value")
	require.Len(t, envelopes, 1)
	require.Equal(t, healthyID, envelopes[0].WorkID)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Len(t, findings, 2)
	byWork := make(map[string]SilentOutcome, len(findings))
	for _, finding := range findings {
		byWork[finding.WorkID] = finding
	}
	require.Equal(t, healthyEventID, byWork[healthyID].TransitionID)
	require.Equal(t, "malformed-outcome-candidate", byWork[poisonID].Kind)
	require.Equal(t, poisonEventID, byWork[poisonID].TransitionID)
	require.Contains(t, byWork[poisonID].Error, "apply new transition value")
}

func TestLegacyOutcomeDiscoveryAcceptsOpaqueNativeCloseReason(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	workID := "dr-g6ar-shaped-close"
	eventID := "45454545-4545-4545-4545-454545454545"
	metadata, err := json.Marshal(map[string]any{
		"gc.outcome.generation": "1",
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 status, close_reason, metadata)
		VALUES (?, 'native opaque close', '', '', '', '', 'closed',
			'Fresh provider conversation independently verified.', ?)
	`, workID, metadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value)
		VALUES (?, ?, 'closed', 'gascity-maintenance-pl', '',
			'Fresh provider conversation independently verified.')
	`, eventID, workID)
	require.NoError(t, err)

	envelopes, err := fixture.store.DiscoverLegacyOutcomes(ctx)
	require.NoError(t, err)
	require.Len(t, envelopes, 1)
	require.Equal(t, workID, envelopes[0].WorkID)
	require.Equal(t, eventID, envelopes[0].Fence.Token)
}

func TestLegacyNonOutcomeDispositionRequiresExactValidatedTuple(t *testing.T) {
	transitionAt := time.Date(2026, 7, 31, 14, 4, 37, 0, time.UTC)
	valid := legacyNonOutcomeDisposition{
		ContractVersion: 1,
		Disposition:     "non-outcome",
		WorkID:          "dr-rnrl",
		TransitionID:    "019fb87d-c476-7c46-8965-2a9b0a88db53",
		RecordedBy:      "mayor",
		RecordedAt:      transitionAt.Add(time.Minute),
	}
	encode := func(t *testing.T, disposition legacyNonOutcomeDisposition) map[string]any {
		t.Helper()
		raw, err := json.Marshal(disposition)
		require.NoError(t, err)
		return map[string]any{metadataOutcomeNonOutcome: string(raw)}
	}
	event := legacyOutcomeEvent{
		ID: valid.TransitionID, CreatedAt: transitionAt,
	}

	dispositioned, err := legacyTransitionIsDispositioned(legacyOutcomeIssue{
		ID: valid.WorkID, Metadata: encode(t, valid),
	}, event)
	require.NoError(t, err)
	require.True(t, dispositioned)

	tests := []struct {
		name   string
		mutate func(*legacyNonOutcomeDisposition)
		error  string
	}{
		{
			name: "wrong work", error: "does not match candidate",
			mutate: func(value *legacyNonOutcomeDisposition) {
				value.WorkID = "dr-other"
			},
		},
		{
			name: "stale transition", error: "does not match qualifying transition",
			mutate: func(value *legacyNonOutcomeDisposition) {
				value.TransitionID = "019fb873-b2a6-7735-9a5a-31a7940a61a4"
			},
		},
		{
			name: "predates transition", error: "precedes qualifying transition",
			mutate: func(value *legacyNonOutcomeDisposition) {
				value.RecordedAt = transitionAt.Add(-time.Second)
			},
		},
		{
			name: "unsupported disposition", error: "is unsupported",
			mutate: func(value *legacyNonOutcomeDisposition) {
				value.Disposition = "acknowledged"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			dispositioned, err := legacyTransitionIsDispositioned(
				legacyOutcomeIssue{ID: valid.WorkID, Metadata: encode(t, candidate)},
				event,
			)
			require.False(t, dispositioned)
			require.ErrorContains(t, err, test.error)
		})
	}
}

func TestLegacyMarkerlessConvoyExclusionRequiresExactCanonicalTuple(
	t *testing.T,
) {
	tests := []struct {
		name      string
		issueType string
		reason    string
		metadata  map[string]any
		want      bool
	}{
		{
			name: "canonical autoclose", issueType: "convoy",
			reason: legacyConvoyAutocloseReason, want: true,
		},
		{
			name: "explicit synthetic marker", issueType: "task",
			reason:   "ordinary source close",
			metadata: map[string]any{"gc.synthetic": "true"}, want: true,
		},
		{
			name: "uppercase type", issueType: "CONVOY",
			reason: legacyConvoyAutocloseReason,
		},
		{
			name: "uppercase reason", issueType: "convoy",
			reason: "Convoy autoclose: all children closed",
		},
		{
			name: "reason with trailing space", issueType: "convoy",
			reason: legacyConvoyAutocloseReason + " ",
		},
		{
			name: "task with canonical reason", issueType: "task",
			reason: legacyConvoyAutocloseReason,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, legacyIssueIsSynthetic(legacyOutcomeIssue{
				IssueType: test.issueType, CloseReason: test.reason,
				Metadata: test.metadata,
			}))
		})
	}
}

func TestLegacyOutcomeDiscoveryRejectsMalformedStructuredClosePayload(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	workID := "dr-structured-close-poison"
	eventID := "46464646-4646-4646-4646-464646464646"
	metadata, err := json.Marshal(map[string]any{
		"gc.outcome.generation": "1",
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes,
			 status, close_reason, metadata)
		VALUES (?, 'structured close poison', '', '', '', '', 'closed',
			'malformed structured close', ?)
	`, workID, metadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value)
		VALUES (?, ?, 'closed', 'project-lead', '', '{not-json')
	`, eventID, workID)
	require.NoError(t, err)

	envelopes, err := fixture.store.DiscoverLegacyOutcomes(ctx)
	require.ErrorContains(t, err, workID)
	require.ErrorContains(t, err, "apply new transition value")
	require.Empty(t, envelopes)
}

func TestLegacyOutcomeDiscoveryQuarantinesMalformedClosedMetadata(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	poisonID := "dr-poison-metadata"
	healthyID := "dr-healthy-after-metadata"
	healthyEventID := "50505050-5050-5050-5050-505050505050"
	_, err := fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes, status, metadata)
		VALUES (?, 'poison metadata', '', '', '', '', 'closed', '[]')
	`, poisonID)
	require.NoError(t, err)
	seedLegacyOutcomeTransition(
		t,
		fixture,
		healthyID,
		"closed",
		"pool-worker",
		healthyEventID,
		map[string]any{"gc.outcome": "completed"},
	)

	envelopes, err := fixture.store.DiscoverLegacyOutcomes(ctx)
	require.ErrorContains(t, err, poisonID)
	require.ErrorContains(t, err, "decode candidate metadata")
	require.Len(t, envelopes, 1)
	require.Equal(t, healthyID, envelopes[0].WorkID)

	findings, err := fixture.store.FindSilentOutcomes(ctx)
	require.NoError(t, err)
	require.Len(t, findings, 2)
	byWork := make(map[string]SilentOutcome, len(findings))
	for _, finding := range findings {
		byWork[finding.WorkID] = finding
	}
	require.Equal(t, healthyEventID, byWork[healthyID].TransitionID)
	require.Equal(t, "malformed-outcome-candidate", byWork[poisonID].Kind)
	require.Contains(t, byWork[poisonID].Error, "decode candidate metadata")
}

func TestLegacyProducerDoesNotInferProjectLeadFromIdentityNames(t *testing.T) {
	issue := legacyOutcomeIssue{
		ID: "dr-name-only", Title: "name-only result", Status: "closed",
		Metadata: map[string]any{},
	}
	producer, err := classifyLegacyOutcomeProducer(issue, "research-pl")
	require.NoError(t, err)
	require.Equal(t, OutcomeProducerDirectWorker, producer)

	issue.Metadata["gc.outcome.producer"] = "project-lead"
	producer, err = classifyLegacyOutcomeProducer(issue, "unrelated")
	require.NoError(t, err)
	require.Equal(t, OutcomeProducerProjectLead, producer)
}

func TestLegacyOutcomeTransitionUsesNativePartialPatches(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	workID := "dr-native-patch"
	transitionID := "55555555-5555-5555-5555-555555555555"
	noteID := "66666666-6666-6666-6666-666666666666"
	finalMetadata := map[string]any{
		"evidence.reviewer_verdict": "pass",
		"gc.step_ref":               "mol-do-work.verify",
	}
	encoded, err := json.Marshal(finalMetadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes, status, metadata)
		VALUES (?, 'native patch', '', '', '', 'later note', 'in_progress', ?)
	`, workID, encoded)
	require.NoError(t, err)
	oldSnapshot, err := json.Marshal(map[string]any{
		"id": workID, "status": "in_progress", "metadata": map[string]any{},
	})
	require.NoError(t, err)
	metadataPatch, err := json.Marshal(map[string]any{"metadata": finalMetadata})
	require.NoError(t, err)
	verifiedSnapshot, err := json.Marshal(map[string]any{
		"id": workID, "status": "in_progress", "metadata": finalMetadata,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value)
		VALUES
			(?, ?, 'updated', 'formula-worker', ?, ?),
			(?, ?, 'updated', 'note-writer', ?, '{"notes":"later note"}')
	`, transitionID, workID, oldSnapshot, metadataPatch,
		noteID, workID, verifiedSnapshot)
	require.NoError(t, err)

	event, err := fixture.store.legacyOutcomeTransitionEvent(ctx, workID)
	require.NoError(t, err)
	require.Equal(t, transitionID, event.ID)
	require.Equal(t, "in_progress", event.Status)
	require.Equal(t, "pass", metadataString(event.Metadata, "evidence.reviewer_verdict"))
}

func TestLegacyOutcomeTransitionTreatsNativeClosedScalarAsTerminal(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	workID := "dr-native-close"
	eventID := "77777777-7777-7777-7777-777777777777"
	metadata := map[string]any{"gc.outcome.producer": "project-lead"}
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes, status, close_reason, metadata)
		VALUES (?, 'native close', '', '', '', '', 'closed', 'completed', ?)
	`, workID, encoded)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(ctx, `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value)
		VALUES (?, ?, 'closed', 'project-lead', '', '"completed"')
	`, eventID, workID)
	require.NoError(t, err)

	event, err := fixture.store.legacyOutcomeTransitionEvent(ctx, workID)
	require.NoError(t, err)
	require.Equal(t, eventID, event.ID)
	require.Equal(t, "closed", event.Status)

	envelopes, err := fixture.store.DiscoverLegacyOutcomes(ctx)
	require.NoError(t, err)
	require.Len(t, envelopes, 1)
	require.Equal(t, OutcomeProducerProjectLead, envelopes[0].Producer)
	require.Equal(t, eventID, envelopes[0].Fence.Token)
}

func TestNativeCloseWithoutOldSnapshotUsesStableCurrentLaneIdentity(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	lanes := []struct {
		workID     string
		eventID    string
		actor      string
		metadata   map[string]any
		producer   OutcomeProducerKind
		generation int64
	}{
		{
			workID: "dr-native-formula", eventID: "88888888-8888-8888-8888-888888888888",
			actor: "formula-worker",
			metadata: map[string]any{
				"gc.step_ref":           "mol-do-work.verify",
				"gc.outcome.generation": "2",
			},
			producer: OutcomeProducerFormulaStep, generation: 2,
		},
		{
			workID: "dr-native-pl", eventID: "99999999-9999-9999-9999-999999999998",
			actor:    "project-lead",
			metadata: map[string]any{"gc.outcome.producer": "project-lead"},
			producer: OutcomeProducerProjectLead, generation: 1,
		},
		{
			workID: "dr-native-order", eventID: "99999999-9999-9999-9999-999999999997",
			actor:    "order:maintenance-cycle",
			metadata: map[string]any{"gc.order.name": "maintenance-cycle"},
			producer: OutcomeProducerMaintenanceOrder, generation: 1,
		},
	}
	for _, lane := range lanes {
		encoded, err := json.Marshal(lane.metadata)
		require.NoError(t, err)
		_, err = fixture.db.ExecContext(ctx, `
			INSERT INTO issues
				(id, title, description, design, acceptance_criteria, notes, status, close_reason, metadata)
			VALUES (?, ?, '', '', '', '', 'closed', 'completed', ?)
		`, lane.workID, lane.workID, encoded)
		require.NoError(t, err)
		_, err = fixture.db.ExecContext(ctx, `
			INSERT INTO events
				(id, issue_id, event_type, actor, old_value, new_value)
			VALUES (?, ?, 'closed', ?, '', '"completed"')
		`, lane.eventID, lane.workID, lane.actor)
		require.NoError(t, err)
	}

	envelopes, err := fixture.store.DiscoverLegacyOutcomes(ctx)
	require.NoError(t, err)
	require.Len(t, envelopes, len(lanes))
	byWork := make(map[string]OutcomeReady, len(envelopes))
	for _, envelope := range envelopes {
		byWork[envelope.WorkID] = envelope
	}
	for _, lane := range lanes {
		envelope := byWork[lane.workID]
		require.Equal(t, lane.producer, envelope.Producer)
		require.Equal(t, lane.eventID, envelope.Fence.Token)
		require.Equal(t, lane.generation, envelope.Fence.Generation)
	}
}

func seedLegacyOutcomeTransition(
	t *testing.T,
	fixture *isolatedDoltBeadStore,
	workID string,
	status string,
	actor string,
	eventID string,
	metadata map[string]any,
) {
	t.Helper()
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	newValue, err := json.Marshal(map[string]any{
		"id": workID, "status": status, "metadata": metadata,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(context.Background(), `
		INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes, status, metadata)
		VALUES (?, ?, '', '', '', '', ?, ?)
	`, workID, fmt.Sprintf("%s result", workID), status, encoded)
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(context.Background(), `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value)
		VALUES (?, ?, 'updated', ?, '{}', ?)
	`, eventID, workID, actor, newValue)
	require.NoError(t, err)
}

func addLegacyOutcomeNoteEvent(
	t *testing.T,
	fixture *isolatedDoltBeadStore,
	workID string,
	eventID string,
	metadata map[string]any,
) {
	t.Helper()
	previous, err := json.Marshal(map[string]any{
		"id": workID, "status": "in_progress", "metadata": metadata,
	})
	require.NoError(t, err)
	withNote := make(map[string]any, len(metadata)+1)
	for key, value := range metadata {
		withNote[key] = value
	}
	withNote["unrelated.note"] = "added after verification"
	next, err := json.Marshal(map[string]any{
		"id": workID, "status": "in_progress", "metadata": withNote,
	})
	require.NoError(t, err)
	_, err = fixture.db.ExecContext(context.Background(), `
		INSERT INTO events
			(id, issue_id, event_type, actor, old_value, new_value)
		VALUES (?, ?, 'updated', 'note-writer', ?, ?)
	`, eventID, workID, previous, next)
	require.NoError(t, err)
}
