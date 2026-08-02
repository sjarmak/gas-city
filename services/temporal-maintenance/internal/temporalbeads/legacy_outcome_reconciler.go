package temporalbeads

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type legacyOutcomeIssue struct {
	ID                       string
	Title                    string
	IssueType                string
	Status                   string
	CloseReason              string
	Metadata                 map[string]any
	HasDownstreamFormulaStep bool
	Existing                 *OutcomeRecord
}

type legacyOutcomeEvent struct {
	ID          string
	EventType   string
	Actor       string
	NewValue    []byte
	CreatedAt   time.Time
	OldStatus   string
	OldMetadata map[string]any
	Status      string
	Metadata    map[string]any
}

type legacyOutcomeFailure struct {
	WorkID       string
	TransitionID string
	TransitionAt time.Time
	Err          error
}

// legacyNonOutcomeDisposition is the only observer suppression contract. It is
// scoped to one exact result transition; actor names, notes, and close-reason
// prose never imply that a terminal record is coordinator-only.
type legacyNonOutcomeDisposition struct {
	ContractVersion int       `json:"contract_version"`
	Disposition     string    `json:"disposition"`
	WorkID          string    `json:"work_id"`
	TransitionID    string    `json:"transition_id"`
	RecordedBy      string    `json:"recorded_by"`
	RecordedAt      time.Time `json:"recorded_at"`
}

type legacyOutcomeDiscoveryError struct {
	Failures []legacyOutcomeFailure
}

func (e *legacyOutcomeDiscoveryError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return "malformed legacy outcome candidate"
	}
	details := make([]string, 0, len(e.Failures))
	for _, failure := range e.Failures {
		detail := fmt.Sprintf("%s: %v", failure.WorkID, failure.Err)
		if failure.TransitionID != "" {
			detail = fmt.Sprintf(
				"%s transition %s: %v",
				failure.WorkID,
				failure.TransitionID,
				failure.Err,
			)
		}
		details = append(details, detail)
	}
	return "malformed legacy outcome candidate(s): " + strings.Join(details, "; ")
}

var ErrOutcomeTransitionNotFound = errors.New(
	"post-rollout outcome transition not found",
)

const (
	legacyOutcomeEventBatchSize = 256
	legacyConvoyAutocloseReason = "convoy autoclose: all children closed"
)

// DiscoverLegacyOutcomes is the authoritative compatibility boundary for
// non-Temporal writers. It converts post-rollout canonical terminal/verified
// events into lane-specific envelopes; EmitOutcome persists each envelope and
// outbox atomically.
func (s *DoltBeadStore) DiscoverLegacyOutcomes(
	ctx context.Context,
) ([]OutcomeReady, error) {
	if s.outcomeRolloutEpoch.IsZero() {
		return nil, fmt.Errorf("outcome rollout epoch is required")
	}
	if err := validateStoreRef(s.outcomeStoreRef); err != nil {
		return nil, fmt.Errorf("legacy outcome store ref: %w", err)
	}
	if err := s.verifyEventTimeZone(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT candidate.id,
		       candidate.title,
		       candidate.issue_type,
		       candidate.status,
		       COALESCE(candidate.close_reason, ''),
		       candidate.metadata,
		       EXISTS (
		           SELECT 1
		             FROM dependencies AS dependency
		             JOIN issues AS downstream
		               ON downstream.id = dependency.issue_id
		            WHERE dependency.type = 'blocks'
		              AND dependency.depends_on_issue_id = candidate.id
		              AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
		                      downstream.metadata,
		                      '$."gc.root_bead_id"'
		                  )), '') = COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
		                      candidate.metadata,
		                      '$."gc.root_bead_id"'
		                  )), '')
		              AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(
		                      downstream.metadata,
		                      '$."gc.step_ref"'
		                  )), '') <> ''
		       ) AS has_downstream_formula_step
		  FROM issues AS candidate
		 WHERE candidate.updated_at >= ?
		 ORDER BY candidate.id`,
		s.outcomeRolloutEpoch.UTC().Format(doltDateTimeLayout),
	)
	if err != nil {
		return nil, fmt.Errorf("query legacy outcome candidates: %w", err)
	}
	var candidates []legacyOutcomeIssue
	var failures []legacyOutcomeFailure
	for rows.Next() {
		var candidate legacyOutcomeIssue
		var raw []byte
		if err := rows.Scan(
			&candidate.ID,
			&candidate.Title,
			&candidate.IssueType,
			&candidate.Status,
			&candidate.CloseReason,
			&raw,
			&candidate.HasDownstreamFormulaStep,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan legacy outcome candidate: %w", err)
		}
		metadata, err := decodeMetadata(raw)
		if err != nil {
			if candidate.Status == "closed" {
				failures = append(failures, legacyOutcomeFailure{
					WorkID: candidate.ID,
					Err:    fmt.Errorf("decode candidate metadata: %w", err),
				})
			}
			continue
		}
		candidate.Metadata = metadata
		if legacyIssueIsSynthetic(candidate) {
			continue
		}
		existing, exists, err := outcomeRecordFromMetadata(metadata)
		if err != nil {
			failures = append(failures, legacyOutcomeFailure{
				WorkID: candidate.ID,
				Err:    fmt.Errorf("decode existing outcome: %w", err),
			})
			continue
		}
		if exists {
			cloned := cloneOutcomeRecord(existing)
			candidate.Existing = &cloned
		}
		if legacyIssueIsNonTerminalFormulaStep(candidate) {
			continue
		}
		if !legacySnapshotHasOutcome(candidate.Status, metadata) {
			continue
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate legacy outcome candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close legacy outcome candidates: %w", err)
	}

	envelopes := make([]OutcomeReady, 0, len(candidates))
	workIDs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		workIDs = append(workIDs, candidate.ID)
	}
	events, eventFailures, err := s.legacyOutcomeTransitionEvents(ctx, workIDs)
	if err != nil {
		return nil, err
	}
	failures = append(failures, eventFailures...)
	for _, candidate := range candidates {
		event, found := events[candidate.ID]
		if !found {
			continue
		}
		if candidate.Existing != nil &&
			(event.ID == candidate.Existing.Envelope.Fence.Token ||
				!event.CreatedAt.After(
					candidate.Existing.Envelope.OccurredAt,
				)) {
			continue
		}
		finalized, err := legacyTransitionIsFinalization(candidate, event)
		if err != nil {
			failures = append(failures, legacyOutcomeFailure{
				WorkID:       candidate.ID,
				TransitionID: event.ID,
				TransitionAt: event.CreatedAt.UTC(),
				Err: fmt.Errorf(
					"validate acknowledged-generation finalization: %w",
					err,
				),
			})
			continue
		}
		if finalized {
			continue
		}
		dispositioned, err := legacyTransitionIsDispositioned(candidate, event)
		if err != nil {
			failures = append(failures, legacyOutcomeFailure{
				WorkID:       candidate.ID,
				TransitionID: event.ID,
				TransitionAt: event.CreatedAt.UTC(),
				Err: fmt.Errorf(
					"validate non-outcome disposition: %w",
					err,
				),
			})
			continue
		}
		if dispositioned {
			continue
		}
		envelope, err := s.legacyOutcomeEnvelope(candidate, event)
		if err != nil {
			failures = append(failures, legacyOutcomeFailure{
				WorkID:       candidate.ID,
				TransitionID: event.ID,
				TransitionAt: event.CreatedAt.UTC(),
				Err:          err,
			})
			continue
		}
		envelopes = append(envelopes, envelope)
	}
	if len(failures) > 0 {
		return envelopes, &legacyOutcomeDiscoveryError{Failures: failures}
	}
	return envelopes, nil
}

func (s *DoltBeadStore) legacyOutcomeTransitionEvent(
	ctx context.Context,
	workID string,
) (legacyOutcomeEvent, error) {
	events, failures, err := s.legacyOutcomeTransitionEvents(ctx, []string{workID})
	if err != nil {
		return legacyOutcomeEvent{}, err
	}
	if len(failures) > 0 {
		return legacyOutcomeEvent{}, &legacyOutcomeDiscoveryError{
			Failures: failures,
		}
	}
	event, found := events[workID]
	if !found {
		return legacyOutcomeEvent{}, fmt.Errorf(
			"%s: %w",
			workID,
			ErrOutcomeTransitionNotFound,
		)
	}
	return event, nil
}

func (s *DoltBeadStore) legacyOutcomeTransitionEvents(
	ctx context.Context,
	workIDs []string,
) (map[string]legacyOutcomeEvent, []legacyOutcomeFailure, error) {
	selected := make(map[string]legacyOutcomeEvent, len(workIDs))
	var failures []legacyOutcomeFailure
	for start := 0; start < len(workIDs); start += legacyOutcomeEventBatchSize {
		end := start + legacyOutcomeEventBatchSize
		if end > len(workIDs) {
			end = len(workIDs)
		}
		batch, batchFailures, err := s.legacyOutcomeTransitionEventBatch(
			ctx,
			workIDs[start:end],
		)
		if err != nil {
			return nil, nil, err
		}
		failures = append(failures, batchFailures...)
		for workID, event := range batch {
			selected[workID] = event
		}
	}
	return selected, failures, nil
}

func (s *DoltBeadStore) legacyOutcomeTransitionEventBatch(
	ctx context.Context,
	workIDs []string,
) (map[string]legacyOutcomeEvent, []legacyOutcomeFailure, error) {
	selected := make(map[string]legacyOutcomeEvent, len(workIDs))
	var failures []legacyOutcomeFailure
	if len(workIDs) == 0 {
		return selected, failures, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(workIDs)), ",")
	query := `
		SELECT id, issue_id, event_type, actor, old_value, new_value,
		       CAST(created_at AS CHAR)
		  FROM events
		 WHERE created_at >= ?
		   AND issue_id IN (` + placeholders + `)
		 ORDER BY issue_id, created_at, id`
	arguments := make([]any, 0, len(workIDs)+1)
	arguments = append(
		arguments,
		s.outcomeRolloutEpoch.In(s.eventTimeLocation).Format(doltDateTimeLayout),
	)
	for _, workID := range workIDs {
		arguments = append(arguments, workID)
	}
	rows, err := s.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"query exact post-rollout transitions: %w",
			err,
		)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var event legacyOutcomeEvent
		var workID string
		var oldValue []byte
		var createdAt string
		if err := rows.Scan(
			&event.ID,
			&workID,
			&event.EventType,
			&event.Actor,
			&oldValue,
			&event.NewValue,
			&createdAt,
		); err != nil {
			return nil, nil, fmt.Errorf(
				"scan exact post-rollout transition for %s: %w",
				workID,
				err,
			)
		}
		event.CreatedAt, err = time.ParseInLocation(
			doltDateTimeLayout,
			createdAt,
			s.eventTimeLocation,
		)
		if err != nil {
			failures = append(failures, legacyOutcomeFailure{
				WorkID:       workID,
				TransitionID: event.ID,
				Err: fmt.Errorf(
					"parse host-local transition timestamp %q: %w",
					createdAt,
					err,
				),
			})
			continue
		}
		event.CreatedAt = event.CreatedAt.UTC()
		oldStatus, oldMetadata, err := decodeLegacyOutcomeSnapshot(oldValue)
		if err != nil {
			failures = append(failures, legacyOutcomeFailure{
				WorkID:       workID,
				TransitionID: event.ID,
				TransitionAt: event.CreatedAt.UTC(),
				Err: fmt.Errorf(
					"decode old transition snapshot: %w",
					err,
				),
			})
			continue
		}
		event.OldStatus = oldStatus
		event.OldMetadata = oldMetadata
		newStatus, newMetadata, err := applyLegacyOutcomeEvent(
			event.EventType,
			oldStatus,
			oldMetadata,
			event.NewValue,
		)
		if err != nil {
			failures = append(failures, legacyOutcomeFailure{
				WorkID:       workID,
				TransitionID: event.ID,
				TransitionAt: event.CreatedAt.UTC(),
				Err: fmt.Errorf(
					"apply new transition value: %w",
					err,
				),
			})
			continue
		}
		if !legacySnapshotHasOutcome(oldStatus, oldMetadata) &&
			legacySnapshotHasOutcome(newStatus, newMetadata) {
			event.Status = newStatus
			event.Metadata = newMetadata
			selected[workID] = event
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf(
			"iterate exact post-rollout transitions: %w",
			err,
		)
	}
	return selected, failures, nil
}

func (s *DoltBeadStore) legacyOutcomeEnvelope(
	issue legacyOutcomeIssue,
	event legacyOutcomeEvent,
) (OutcomeReady, error) {
	transitionIssue := issue
	transitionIssue.Status = event.Status
	transitionIssue.Metadata = legacyStableTransitionMetadata(
		event.Metadata,
		issue.Metadata,
	)
	producer, err := classifyLegacyOutcomeProducer(transitionIssue, event.Actor)
	if err != nil {
		return OutcomeReady{}, err
	}
	generation, err := legacyOutcomeGeneration(transitionIssue.Metadata)
	if err != nil {
		return OutcomeReady{}, err
	}
	if issue.Existing != nil &&
		issue.Existing.State == OutcomeCoordinatorAcknowledged &&
		generation <= issue.Existing.Envelope.Fence.Generation {
		if issue.Existing.Envelope.Fence.Generation == math.MaxInt64 {
			return OutcomeReady{}, fmt.Errorf(
				"acknowledged legacy outcome generation is exhausted",
			)
		}
		generation = issue.Existing.Envelope.Fence.Generation + 1
	}
	summary := firstLegacyMetadata(
		issue.Metadata,
		"summary_for_human",
		"gc.outcome.summary",
	)
	if summary == "" {
		summary = strings.TrimSpace(issue.CloseReason)
	}
	if summary == "" || len(summary) > 512 {
		summary = strings.TrimSpace(issue.Title)
	}
	sourceRoot := firstLegacyMetadata(
		transitionIssue.Metadata,
		"gc.outcome.source_root_id",
		"gc.root_bead_id",
		"dispatch.root_id",
		"gc.formula.root_id",
	)
	if sourceRoot == "" {
		sourceRoot = issue.ID
	}
	stepKey := firstLegacyMetadata(
		transitionIssue.Metadata,
		"gc.outcome.step_key",
		"gc.step_ref",
		"dispatch.step",
		"gc.formula.step_key",
	)
	if stepKey == "" {
		stepKey = string(producer)
	}
	state := OutcomeEvidenceTerminal
	if event.Status != "closed" {
		state = OutcomeEvidenceVerified
	}
	digest := sha256.Sum256(event.NewValue)
	input := OutcomeAdapterInput{
		StoreRef:     s.outcomeStoreRef,
		WorkID:       issue.ID,
		SourceRootID: sourceRoot,
		StepKey:      stepKey,
		Summary:      summary,
		Impact:       legacyOutcomeImpact(producer),
		State:        state,
		Evidence: []ArtifactRef{{
			Kind:   ArtifactKindReviewRecord,
			URI:    "bead:" + issue.ID + "@" + event.ID,
			SHA256: hex.EncodeToString(digest[:]),
		}},
		Fence: OutcomeProducerFence{
			ProducerID: "legacy-" + string(producer),
			Generation: generation,
			Token:      event.ID,
		},
		OccurredAt: event.CreatedAt.UTC(),
	}
	switch producer {
	case OutcomeProducerDirectWorker:
		return DirectWorkerOutcome(input)
	case OutcomeProducerFormulaStep:
		return FormulaStepOutcome(input)
	case OutcomeProducerProjectLead:
		return ProjectLeadOutcome(input)
	case OutcomeProducerMaintenanceOrder:
		return MaintenanceOrderOutcome(input)
	default:
		return OutcomeReady{}, fmt.Errorf("unsupported legacy producer %q", producer)
	}
}

// Native close events may contain no old snapshot and only a scalar close
// reason. Recover only stable structured lane/attempt identity from the
// canonical issue; outcome evidence and status still come exclusively from the
// exact event transition.
func legacyStableTransitionMetadata(
	eventMetadata map[string]any,
	currentMetadata map[string]any,
) map[string]any {
	merged := make(map[string]any, len(eventMetadata))
	for key, value := range eventMetadata {
		merged[key] = value
	}
	for _, key := range []string{
		"gc.outcome.producer",
		"dispatch.formula",
		"gc.formula_name",
		"gc.formula.name",
		"gc.order.name",
		"order.name",
		"gc.outcome.source_root_id",
		"gc.root_bead_id",
		"dispatch.root_id",
		"gc.formula.root_id",
		"gc.outcome.step_key",
		"gc.step_ref",
		"dispatch.step",
		"gc.formula.step_key",
		"gc.outcome.generation",
		"gc.claim_generation",
		"gc.temporal.generation",
	} {
		if _, exists := merged[key]; exists {
			continue
		}
		if value, exists := currentMetadata[key]; exists {
			merged[key] = value
		}
	}
	return merged
}

func classifyLegacyOutcomeProducer(
	issue legacyOutcomeIssue,
	actor string,
) (OutcomeProducerKind, error) {
	if explicit := firstLegacyMetadata(issue.Metadata, "gc.outcome.producer"); explicit != "" {
		producer := OutcomeProducerKind(explicit)
		switch producer {
		case OutcomeProducerDirectWorker,
			OutcomeProducerFormulaStep,
			OutcomeProducerProjectLead,
			OutcomeProducerMaintenanceOrder:
			return producer, nil
		default:
			return "", fmt.Errorf("explicit legacy producer %q is invalid", explicit)
		}
	}
	if firstLegacyMetadata(
		issue.Metadata,
		"dispatch.formula",
		"gc.formula_name",
		"gc.formula.name",
		"gc.step_ref",
	) != "" {
		return OutcomeProducerFormulaStep, nil
	}
	if firstLegacyMetadata(
		issue.Metadata,
		"gc.order.name",
		"order.name",
	) != "" {
		return OutcomeProducerMaintenanceOrder, nil
	}
	if strings.HasPrefix(actor, "order:") {
		return OutcomeProducerMaintenanceOrder, nil
	}
	return OutcomeProducerDirectWorker, nil
}

func decodeLegacyOutcomeSnapshot(raw []byte) (string, map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", map[string]any{}, nil
	}
	var snapshot struct {
		Status   string         `json:"status"`
		Metadata map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return "", nil, err
	}
	if snapshot.Metadata == nil {
		snapshot.Metadata = map[string]any{}
	}
	return snapshot.Status, snapshot.Metadata, nil
}

// Native Beads events store old_value as a full issue snapshot, while
// new_value is commonly a partial field patch (and close events use the close
// reason scalar). Reconstruct the post-event outcome fields before deciding
// whether this exact event introduced terminal or verified evidence.
func applyLegacyOutcomeEvent(
	eventType string,
	oldStatus string,
	oldMetadata map[string]any,
	raw []byte,
) (string, map[string]any, error) {
	status := oldStatus
	metadata := oldMetadata
	if metadata == nil {
		metadata = map[string]any{}
	}

	var value any
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &value); err != nil {
			switch eventType {
			case "closed", "reopened":
				trimmed := bytes.TrimSpace(raw)
				if len(trimmed) > 0 &&
					(trimmed[0] == '{' || trimmed[0] == '[') {
					return "", nil, err
				}
				// Native Beads stores the human close/reopen reason as an
				// opaque scalar. The event type carries the contract state;
				// non-JSON reason text is not an outcome payload.
				value = nil
			default:
				return "", nil, err
			}
		}
	}
	switch patch := value.(type) {
	case map[string]any:
		if patchedStatus, ok := patch["status"].(string); ok {
			status = patchedStatus
		}
		if patchedMetadata, ok := patch["metadata"].(map[string]any); ok {
			metadata = patchedMetadata
		}
	case string:
		if eventType == "status_changed" {
			status = patch
		}
	case nil:
	default:
		return "", nil, fmt.Errorf(
			"unsupported %s event value type %T",
			eventType,
			value,
		)
	}
	switch eventType {
	case "closed":
		status = "closed"
	case "reopened":
		status = "open"
	}
	return status, metadata, nil
}

func legacySnapshotHasOutcome(status string, metadata map[string]any) bool {
	if strings.EqualFold(
		strings.TrimSpace(metadataString(metadata, "gc.synthetic")),
		"true",
	) {
		return false
	}
	hasVerifiedOutcome := false
	for _, key := range []string{
		"evidence.reviewer_verdict",
		"review_verdict",
		"reviewer_verdict",
	} {
		if strings.EqualFold(metadataString(metadata, key), "pass") {
			hasVerifiedOutcome = true
			break
		}
	}
	if !hasVerifiedOutcome {
		hasVerifiedOutcome =
			isPassingOutcomeValue(strings.ToLower(metadataString(metadata, "gc.outcome"))) ||
				isPassingOutcomeValue(strings.ToLower(metadataString(metadata, "gc.work_outcome")))
	}
	if hasVerifiedOutcome {
		return true
	}
	if status != "closed" {
		return false
	}
	// Graph-v2 formula steps are control-plane bookkeeping, not the source work
	// bead. A bare step close must not become an OutcomeReady result; an
	// explicit passing outcome/review marker above still makes a formula step a
	// legitimate result while its source root remains open.
	return firstLegacyMetadata(metadata, "gc.step_ref") == "" ||
		firstLegacyMetadata(metadata, "gc.root_bead_id") == ""
}

func legacyIssueIsSynthetic(issue legacyOutcomeIssue) bool {
	if strings.EqualFold(
		strings.TrimSpace(metadataString(issue.Metadata, "gc.synthetic")),
		"true",
	) {
		return true
	}
	return issue.IssueType == "convoy" &&
		issue.CloseReason == legacyConvoyAutocloseReason
}

func legacyIssueIsNonTerminalFormulaStep(issue legacyOutcomeIssue) bool {
	return issue.HasDownstreamFormulaStep &&
		firstLegacyMetadata(issue.Metadata, "gc.step_ref") != "" &&
		firstLegacyMetadata(issue.Metadata, "gc.root_bead_id") != ""
}

// legacyTransitionIsFinalization excludes only the close written by the
// acknowledged-generation finalization contract. Both event snapshots and the
// current issue must carry the same valid receipt and byte-equivalent canonical
// outbox; a marker-shaped or stale receipt is an observer error, not suppression.
func legacyTransitionIsFinalization(
	issue legacyOutcomeIssue,
	event legacyOutcomeEvent,
) (bool, error) {
	eventRaw := metadataString(event.Metadata, metadataOutcomeFinalization)
	currentRaw := metadataString(issue.Metadata, metadataOutcomeFinalization)
	if eventRaw == "" && currentRaw == "" {
		return false, nil
	}
	if event.EventType != "closed" || event.Status != "closed" || issue.Status != "closed" {
		return false, fmt.Errorf("finalization must be an exact closed transition")
	}
	if metadataString(event.OldMetadata, metadataOutcomeFinalization) != "" {
		return false, fmt.Errorf("finalization receipt predates the closed transition")
	}
	if strings.TrimSpace(event.OldStatus) == "" || event.OldStatus == "closed" {
		return false, fmt.Errorf("finalization pre-close snapshot is not live work")
	}
	eventReceipt, eventFinalized, err := outcomeFinalizationFromMetadata(event.Metadata)
	if err != nil {
		return false, fmt.Errorf("decode event finalization: %w", err)
	}
	currentReceipt, currentFinalized, err := outcomeFinalizationFromMetadata(issue.Metadata)
	if err != nil {
		return false, fmt.Errorf("decode current finalization: %w", err)
	}
	if !eventFinalized || !currentFinalized {
		return false, fmt.Errorf("finalization receipt is missing from an exact snapshot")
	}
	if eventReceipt.WorkID != issue.ID || currentReceipt.WorkID != issue.ID {
		return false, fmt.Errorf("finalization work id does not match candidate %q", issue.ID)
	}
	eventRecord, eventExists, err := outcomeRecordFromMetadata(event.Metadata)
	if err != nil {
		return false, fmt.Errorf("decode event finalization outbox: %w", err)
	}
	currentRecord, currentExists, err := outcomeRecordFromMetadata(issue.Metadata)
	if err != nil {
		return false, fmt.Errorf("decode current finalization outbox: %w", err)
	}
	if !eventExists || !currentExists {
		return false, fmt.Errorf("finalization canonical outbox is missing")
	}
	oldRecord, oldExists, err := outcomeRecordFromMetadata(event.OldMetadata)
	if err != nil {
		return false, fmt.Errorf("decode pre-close finalization outbox: %w", err)
	}
	oldOutbox := metadataString(event.OldMetadata, metadataOutcomeOutbox)
	eventOutbox := metadataString(event.Metadata, metadataOutcomeOutbox)
	currentOutbox := metadataString(issue.Metadata, metadataOutcomeOutbox)
	if !oldExists || oldRecord.State != OutcomeCoordinatorAcknowledged ||
		oldOutbox == "" || oldOutbox != eventOutbox || oldOutbox != currentOutbox ||
		!outcomeFinalizationMatchesRecord(eventReceipt, oldRecord) {
		return false, fmt.Errorf("finalization does not preserve the pre-close acknowledged outbox")
	}
	if eventRaw != currentRaw ||
		!outcomeFinalizationMatchesRecord(eventReceipt, eventRecord) ||
		!outcomeFinalizationMatchesRecord(currentReceipt, currentRecord) {
		return false, fmt.Errorf("finalization receipt and acknowledged outbox conflict")
	}
	return true, nil
}

func legacyTransitionIsDispositioned(
	issue legacyOutcomeIssue,
	event legacyOutcomeEvent,
) (bool, error) {
	raw := metadataString(issue.Metadata, metadataOutcomeNonOutcome)
	if raw == "" {
		return false, nil
	}
	var disposition legacyNonOutcomeDisposition
	if err := json.Unmarshal([]byte(raw), &disposition); err != nil {
		return false, fmt.Errorf("decode %s: %w", metadataOutcomeNonOutcome, err)
	}
	if disposition.ContractVersion != 1 {
		return false, fmt.Errorf(
			"contract_version %d is unsupported",
			disposition.ContractVersion,
		)
	}
	if disposition.Disposition != "non-outcome" {
		return false, fmt.Errorf(
			"disposition %q is unsupported",
			disposition.Disposition,
		)
	}
	for _, field := range []struct {
		label string
		value string
	}{
		{label: "non-outcome work id", value: disposition.WorkID},
		{label: "non-outcome transition id", value: disposition.TransitionID},
		{label: "non-outcome recorded by", value: disposition.RecordedBy},
	} {
		if err := validateSegment(field.label, field.value); err != nil {
			return false, err
		}
	}
	if disposition.RecordedAt.IsZero() {
		return false, fmt.Errorf("recorded_at is required")
	}
	if disposition.WorkID != issue.ID {
		return false, fmt.Errorf(
			"work_id %q does not match candidate %q",
			disposition.WorkID,
			issue.ID,
		)
	}
	if disposition.TransitionID != event.ID {
		return false, fmt.Errorf(
			"transition_id %q does not match qualifying transition %q",
			disposition.TransitionID,
			event.ID,
		)
	}
	if disposition.RecordedAt.Before(event.CreatedAt) {
		return false, fmt.Errorf(
			"recorded_at %s precedes qualifying transition at %s",
			disposition.RecordedAt.UTC().Format(time.RFC3339Nano),
			event.CreatedAt.UTC().Format(time.RFC3339Nano),
		)
	}
	return true, nil
}

func legacyOutcomeGeneration(metadata map[string]any) (int64, error) {
	for _, key := range []string{
		"gc.outcome.generation",
		"gc.claim_generation",
		"gc.temporal.generation",
	} {
		value := metadataString(metadata, key)
		if value == "" {
			continue
		}
		generation, err := strconv.ParseInt(value, 10, 64)
		if err != nil || generation <= 0 {
			return 0, fmt.Errorf("legacy outcome generation %q is invalid", value)
		}
		return generation, nil
	}
	return 1, nil
}

func firstLegacyMetadata(metadata map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(metadataString(metadata, key)); value != "" {
			return value
		}
	}
	return ""
}

func legacyOutcomeImpact(producer OutcomeProducerKind) string {
	switch producer {
	case OutcomeProducerFormulaStep:
		return "The formula step result is ready for coordinator acknowledgement while its source root may remain open."
	case OutcomeProducerProjectLead:
		return "The project-lead result is ready for coordinator acknowledgement."
	case OutcomeProducerMaintenanceOrder:
		return "The maintenance-order result is ready for coordinator acknowledgement."
	default:
		return "The direct-worker result is ready for coordinator acknowledgement."
	}
}
