package temporalbeads

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	metadataOutcomeEnvelope       = "gc.coordinator_outcome.envelope"
	metadataOutcomeOutbox         = "gc.coordinator_outcome.outbox"
	metadataOutcomeState          = "gc.coordinator_outcome.state"
	metadataOutcomePendingAt      = "gc.coordinator_outcome.pending_at"
	metadataOutcomeDeliveredAt    = "gc.coordinator_outcome.delivered_at"
	metadataOutcomeAcknowledgedAt = "gc.coordinator_outcome.acknowledged_at"
	metadataOutcomeNonOutcome     = "gc.coordinator_outcome.non_outcome"
	metadataOutcomeFinalization   = "gc.coordinator_outcome.finalization"
)

var ErrOutcomeNotFound = errors.New("coordinator outcome not found")

// OutcomeRecord is the canonical outbox and acknowledgement projection.
type OutcomeRecord struct {
	Envelope         OutcomeReady            `json:"envelope"`
	State            OutcomeCoordinatorState `json:"state"`
	PendingAt        time.Time               `json:"pending_at"`
	DeliveredAt      time.Time               `json:"delivered_at,omitempty"`
	AcknowledgedAt   time.Time               `json:"acknowledged_at,omitempty"`
	DeliveryRef      string                  `json:"delivery_ref,omitempty"`
	CoordinatorFence string                  `json:"coordinator_fence,omitempty"`
	AcknowledgedBy   string                  `json:"acknowledged_by,omitempty"`
}

type OutcomeDelivery struct {
	OutcomeID        string `json:"outcome_id"`
	WorkID           string `json:"work_id"`
	DeliveryRef      string `json:"delivery_ref"`
	CoordinatorFence string `json:"coordinator_fence"`
}

type OutcomeAcknowledgement struct {
	StoreRef         string `json:"store_ref"`
	OutcomeID        string `json:"outcome_id"`
	WorkID           string `json:"work_id"`
	CoordinatorFence string `json:"coordinator_fence"`
	AcknowledgedBy   string `json:"acknowledged_by"`
}

// OutcomeFinalizationRequest closes work whose exact result generation has
// already been acknowledged. It is not an acknowledgement or a non-outcome
// disposition: the existing canonical outbox remains the completion proof.
type OutcomeFinalizationRequest struct {
	StoreRef           string `json:"store_ref"`
	OutcomeID          string `json:"outcome_id"`
	WorkID             string `json:"work_id"`
	ProducerGeneration int64  `json:"producer_generation"`
	ProducerToken      string `json:"producer_token"`
	CoordinatorFence   string `json:"coordinator_fence"`
	FinalizedBy        string `json:"finalized_by"`
	CloseReason        string `json:"close_reason"`
}

func (r OutcomeFinalizationRequest) Validate() error {
	return validateOutcomeFinalizationRequest(r)
}

// OutcomeFinalizationReceipt binds one close to the exact acknowledged
// producer generation that justified it.
type OutcomeFinalizationReceipt struct {
	ContractVersion    int       `json:"contract_version"`
	StoreRef           string    `json:"store_ref"`
	OutcomeID          string    `json:"outcome_id"`
	WorkID             string    `json:"work_id"`
	ProducerGeneration int64     `json:"producer_generation"`
	ProducerToken      string    `json:"producer_token"`
	CoordinatorFence   string    `json:"coordinator_fence"`
	AcknowledgedAt     time.Time `json:"acknowledged_at"`
	AcknowledgedBy     string    `json:"acknowledged_by"`
	FinalizedAt        time.Time `json:"finalized_at"`
	FinalizedBy        string    `json:"finalized_by"`
	CloseReason        string    `json:"close_reason"`
}

func (a OutcomeAcknowledgement) Validate() error {
	return validateOutcomeAcknowledgement(a)
}

type SilentOutcome struct {
	StoreRef     string    `json:"store_ref"`
	WorkID       string    `json:"work_id"`
	Kind         string    `json:"kind,omitempty"`
	Reason       string    `json:"reason"`
	Error        string    `json:"error,omitempty"`
	TransitionID string    `json:"transition_id"`
	TransitionAt time.Time `json:"transition_at"`
}

// OutcomeStore is shared by the producer adapters, delivery Activities, and
// independent surfacer.
type OutcomeStore interface {
	EmitOutcome(context.Context, OutcomeReady) (OutcomeRecord, error)
	DiscoverLegacyOutcomes(context.Context) ([]OutcomeReady, error)
	PendingOutcomes(context.Context) ([]OutcomeRecord, error)
	MarkOutcomeDelivered(context.Context, OutcomeDelivery) (OutcomeRecord, error)
	MarkOutcomeAcknowledged(context.Context, OutcomeAcknowledgement) (OutcomeRecord, error)
	InspectOutcome(context.Context, string) (OutcomeRecord, error)
	FindSilentOutcomes(context.Context) ([]SilentOutcome, error)
}

// EmitOutcome stores the envelope and its pending outbox record in the same
// canonical Beads transaction without changing the work or source-root status.
func (s *DoltBeadStore) EmitOutcome(
	ctx context.Context,
	envelope OutcomeReady,
) (OutcomeRecord, error) {
	if err := envelope.Validate(); err != nil {
		return OutcomeRecord{}, err
	}
	var result OutcomeRecord
	err := s.mutateIssue(ctx, envelope.WorkID, "outcome-ready", func(
		tx *sql.Tx,
		issue canonicalIssue,
	) (bool, error) {
		existing, exists, err := outcomeRecordFromMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		if exists {
			if outcomesEqual(existing.Envelope, envelope) {
				result = existing
				return false, nil
			}
			if err := validateOutcomeSupersession(existing, envelope); err != nil {
				return false, err
			}
		}
		delete(issue.Metadata, "gc.coordinator_outcome.history")
		result = OutcomeRecord{
			Envelope:  cloneOutcomeReady(envelope),
			State:     OutcomeCoordinatorPending,
			PendingAt: s.clock.Now().UTC(),
		}
		if err := setOutcomeMetadata(issue.Metadata, result); err != nil {
			return false, err
		}
		encoded, err := encodeMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(
			ctx,
			"UPDATE issues SET metadata = ? WHERE id = ?",
			encoded,
			envelope.WorkID,
		)
		if err == nil && s.outcomeMetadataFault != nil {
			err = s.outcomeMetadataFault()
		}
		return true, err
	})
	return cloneOutcomeRecord(result), err
}

func validateOutcomeSupersession(
	existing OutcomeRecord,
	incoming OutcomeReady,
) error {
	if existing.State != OutcomeCoordinatorAcknowledged {
		return fmt.Errorf(
			"outcome %s conflicts with unacknowledged outcome %s",
			incoming.OutcomeID,
			existing.Envelope.OutcomeID,
		)
	}
	if incoming.Fence.Generation <= existing.Envelope.Fence.Generation {
		return fmt.Errorf(
			"outcome generation %d must be greater than acknowledged generation %d",
			incoming.Fence.Generation,
			existing.Envelope.Fence.Generation,
		)
	}
	if !incoming.OccurredAt.After(existing.Envelope.OccurredAt) {
		return fmt.Errorf(
			"outcome occurred_at %s must be newer than acknowledged outcome time %s",
			incoming.OccurredAt.Format(time.RFC3339Nano),
			existing.Envelope.OccurredAt.Format(time.RFC3339Nano),
		)
	}
	return nil
}

// PendingOutcomes returns every unacknowledged outbox record. This makes a
// delivered record rediscoverable after worker or coordinator restarts while
// the durable Workflow also owns its periodic redelivery.
func (s *DoltBeadStore) PendingOutcomes(
	ctx context.Context,
) ([]OutcomeRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT metadata
		  FROM issues
		 WHERE JSON_EXTRACT(
		           metadata,
		           '$."gc.coordinator_outcome.outbox"'
		       ) IS NOT NULL
		   AND JSON_EXTRACT(
		           metadata,
		           '$."gc.coordinator_outcome.acknowledged_at"'
		       ) IS NULL`)
	if err != nil {
		return nil, fmt.Errorf("query pending coordinator outcomes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var records []OutcomeRecord
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan pending coordinator outcome: %w", err)
		}
		metadata, err := decodeMetadata(raw)
		if err != nil {
			return nil, err
		}
		record, exists, err := outcomeRecordFromMetadata(metadata)
		if err != nil {
			return nil, err
		}
		if exists {
			records = append(records, record)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending coordinator outcomes: %w", err)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].PendingAt.Equal(records[j].PendingAt) {
			return records[i].Envelope.OutcomeID < records[j].Envelope.OutcomeID
		}
		return records[i].PendingAt.Before(records[j].PendingAt)
	})
	return cloneOutcomeRecords(records), nil
}

func (s *DoltBeadStore) MarkOutcomeDelivered(
	ctx context.Context,
	delivery OutcomeDelivery,
) (OutcomeRecord, error) {
	if err := validateOutcomeDelivery(delivery); err != nil {
		return OutcomeRecord{}, err
	}
	var result OutcomeRecord
	err := s.mutateIssue(ctx, delivery.WorkID, "outcome-delivered", func(
		tx *sql.Tx,
		issue canonicalIssue,
	) (bool, error) {
		record, exists, err := outcomeRecordFromMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		if !exists || record.Envelope.OutcomeID != delivery.OutcomeID {
			return false, ErrStaleFence
		}
		if !record.AcknowledgedAt.IsZero() {
			result = record
			return false, nil
		}
		changed := record.CoordinatorFence != delivery.CoordinatorFence ||
			record.DeliveryRef != delivery.DeliveryRef ||
			record.DeliveredAt.IsZero()
		if !changed {
			result = record
			return false, nil
		}
		record.State = OutcomeCoordinatorNeedsAck
		if record.DeliveredAt.IsZero() {
			record.DeliveredAt = s.clock.Now().UTC()
		}
		record.DeliveryRef = delivery.DeliveryRef
		record.CoordinatorFence = delivery.CoordinatorFence
		if err := setOutcomeMetadata(issue.Metadata, record); err != nil {
			return false, err
		}
		encoded, err := encodeMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(
			ctx,
			"UPDATE issues SET metadata = ? WHERE id = ?",
			encoded,
			delivery.WorkID,
		)
		if err == nil {
			result = record
		}
		return true, err
	})
	return cloneOutcomeRecord(result), err
}

func (s *DoltBeadStore) MarkOutcomeAcknowledged(
	ctx context.Context,
	ack OutcomeAcknowledgement,
) (OutcomeRecord, error) {
	if err := validateOutcomeAcknowledgement(ack); err != nil {
		return OutcomeRecord{}, err
	}
	var result OutcomeRecord
	err := s.mutateIssue(ctx, ack.WorkID, "outcome-acknowledged", func(
		tx *sql.Tx,
		issue canonicalIssue,
	) (bool, error) {
		record, exists, err := outcomeRecordFromMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		if !exists ||
			record.Envelope.StoreRef != ack.StoreRef ||
			record.Envelope.OutcomeID != ack.OutcomeID ||
			record.CoordinatorFence != ack.CoordinatorFence {
			return false, ErrStaleFence
		}
		if !record.AcknowledgedAt.IsZero() {
			if record.AcknowledgedBy == ack.AcknowledgedBy {
				result = record
				return false, nil
			}
			return false, fmt.Errorf("outcome acknowledgement conflicts")
		}
		if record.DeliveredAt.IsZero() {
			return false, ErrStaleFence
		}
		record.State = OutcomeCoordinatorAcknowledged
		record.AcknowledgedAt = s.clock.Now().UTC()
		record.AcknowledgedBy = ack.AcknowledgedBy
		if err := setOutcomeMetadata(issue.Metadata, record); err != nil {
			return false, err
		}
		encoded, err := encodeMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(
			ctx,
			"UPDATE issues SET metadata = ? WHERE id = ?",
			encoded,
			ack.WorkID,
		)
		if err == nil {
			result = record
		}
		return true, err
	})
	return cloneOutcomeRecord(result), err
}

// FinalizeAcknowledgedOutcome atomically closes an issue only when its current
// canonical outbox proves the exact acknowledged generation named by request.
// The acknowledged outbox is preserved byte-for-byte as the outcome proof.
func (s *DoltBeadStore) FinalizeAcknowledgedOutcome(
	ctx context.Context,
	request OutcomeFinalizationRequest,
) (OutcomeFinalizationReceipt, error) {
	if err := validateOutcomeFinalizationRequest(request); err != nil {
		return OutcomeFinalizationReceipt{}, err
	}
	var result OutcomeFinalizationReceipt
	err := s.mutateIssue(ctx, request.WorkID, "outcome-finalized", func(
		tx *sql.Tx,
		issue canonicalIssue,
	) (bool, error) {
		record, exists, err := outcomeRecordFromMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, ErrOutcomeNotFound
		}
		stored, finalized, err := outcomeFinalizationFromMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		if finalized {
			if issue.Status != "closed" {
				return false, fmt.Errorf(
					"outcome finalization exists on non-closed work",
				)
			}
			if !outcomeFinalizationMatchesRequest(stored, request) ||
				!outcomeFinalizationMatchesRecord(stored, record) {
				return false, fmt.Errorf("outcome finalization conflicts")
			}
			result = stored
			return false, nil
		}
		if issue.Status == "closed" {
			return false, fmt.Errorf(
				"work is already closed without an acknowledged-generation finalization",
			)
		}
		if record.State != OutcomeCoordinatorAcknowledged ||
			record.AcknowledgedAt.IsZero() ||
			record.AcknowledgedBy == "" ||
			record.DeliveredAt.IsZero() {
			return false, fmt.Errorf(
				"outcome must be acknowledged before finalization",
			)
		}
		if record.Envelope.StoreRef != request.StoreRef ||
			record.Envelope.OutcomeID != request.OutcomeID ||
			record.Envelope.WorkID != request.WorkID ||
			record.Envelope.Fence.Generation != request.ProducerGeneration ||
			record.Envelope.Fence.Token != request.ProducerToken ||
			record.CoordinatorFence != request.CoordinatorFence {
			return false, fmt.Errorf(
				"finalization does not match the exact acknowledged generation",
			)
		}

		result = OutcomeFinalizationReceipt{
			ContractVersion:    1,
			StoreRef:           record.Envelope.StoreRef,
			OutcomeID:          record.Envelope.OutcomeID,
			WorkID:             record.Envelope.WorkID,
			ProducerGeneration: record.Envelope.Fence.Generation,
			ProducerToken:      record.Envelope.Fence.Token,
			CoordinatorFence:   record.CoordinatorFence,
			AcknowledgedAt:     record.AcknowledgedAt.UTC(),
			AcknowledgedBy:     record.AcknowledgedBy,
			FinalizedAt:        s.clock.Now().UTC(),
			FinalizedBy:        request.FinalizedBy,
			CloseReason:        request.CloseReason,
		}
		if err := validateOutcomeFinalizationReceipt(result); err != nil {
			return false, err
		}
		encodedFinalization, err := json.Marshal(result)
		if err != nil {
			return false, fmt.Errorf("encode outcome finalization: %w", err)
		}
		issue.Metadata[metadataOutcomeFinalization] = string(encodedFinalization)
		encodedMetadata, err := encodeMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE issues
			   SET status = 'closed',
			       closed_at = CURRENT_TIMESTAMP,
			       closed_by_session = ?,
			       close_reason = ?,
			       metadata = ?
			 WHERE id = ?`,
			request.FinalizedBy,
			request.CloseReason,
			encodedMetadata,
			request.WorkID,
		)
		return true, err
	})
	return result, err
}

func (s *DoltBeadStore) InspectOutcome(
	ctx context.Context,
	workID string,
) (OutcomeRecord, error) {
	if err := validateSegment("outcome work id", workID); err != nil {
		return OutcomeRecord{}, err
	}
	issue, err := scanCanonicalIssue(s.db.QueryRowContext(
		ctx,
		"SELECT id, status, metadata FROM issues WHERE id = ?",
		workID,
	))
	if err != nil {
		return OutcomeRecord{}, err
	}
	record, exists, err := outcomeRecordFromMetadata(issue.Metadata)
	if err != nil {
		return OutcomeRecord{}, err
	}
	if !exists {
		return OutcomeRecord{}, ErrOutcomeNotFound
	}
	return cloneOutcomeRecord(record), nil
}

// FindSilentOutcomes independently detects terminal or verified work whose
// producer failed to create any canonical outcome envelope.
func (s *DoltBeadStore) FindSilentOutcomes(
	ctx context.Context,
) ([]SilentOutcome, error) {
	envelopes, discoveryErr := s.DiscoverLegacyOutcomes(ctx)
	var candidateErr *legacyOutcomeDiscoveryError
	if discoveryErr != nil && !errors.As(discoveryErr, &candidateErr) {
		return nil, discoveryErr
	}
	failureCount := 0
	if candidateErr != nil {
		failureCount = len(candidateErr.Failures)
	}
	silent := make([]SilentOutcome, 0, len(envelopes)+failureCount)
	for _, envelope := range envelopes {
		reason := "terminal without outcome envelope"
		if envelope.State == OutcomeEvidenceVerified {
			reason = "verified without outcome envelope"
		}
		silent = append(silent, SilentOutcome{
			StoreRef:     envelope.StoreRef,
			WorkID:       envelope.WorkID,
			Reason:       reason,
			TransitionID: envelope.Fence.Token,
			TransitionAt: envelope.OccurredAt,
		})
	}
	if candidateErr != nil {
		for _, failure := range candidateErr.Failures {
			silent = append(silent, SilentOutcome{
				StoreRef:     s.outcomeStoreRef,
				WorkID:       failure.WorkID,
				Kind:         "malformed-outcome-candidate",
				Reason:       "malformed outcome candidate: " + failure.Err.Error(),
				Error:        failure.Err.Error(),
				TransitionID: failure.TransitionID,
				TransitionAt: failure.TransitionAt,
			})
		}
	}
	return silent, nil
}

func isPassingOutcomeValue(value string) bool {
	switch value {
	case "pass", "passed", "branch-ready", "completed", "verified":
		return true
	default:
		return false
	}
}

func setOutcomeMetadata(metadata map[string]any, record OutcomeRecord) error {
	envelope, err := json.Marshal(record.Envelope)
	if err != nil {
		return fmt.Errorf("encode outcome envelope: %w", err)
	}
	outbox, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode outcome outbox: %w", err)
	}
	metadata[metadataOutcomeEnvelope] = string(envelope)
	metadata[metadataOutcomeOutbox] = string(outbox)
	metadata[metadataOutcomeState] = string(record.State)
	metadata[metadataOutcomePendingAt] = record.PendingAt.Format(time.RFC3339Nano)
	setOptionalOutcomeTime(metadata, metadataOutcomeDeliveredAt, record.DeliveredAt)
	setOptionalOutcomeTime(metadata, metadataOutcomeAcknowledgedAt, record.AcknowledgedAt)
	return nil
}

func setOptionalOutcomeTime(metadata map[string]any, key string, value time.Time) {
	if value.IsZero() {
		delete(metadata, key)
		return
	}
	metadata[key] = value.UTC().Format(time.RFC3339Nano)
}

func outcomeRecordFromMetadata(
	metadata map[string]any,
) (OutcomeRecord, bool, error) {
	var record OutcomeRecord
	if metadataString(metadata, metadataOutcomeOutbox) == "" {
		return OutcomeRecord{}, false, nil
	}
	if err := decodeMetadataJSON(metadata, metadataOutcomeOutbox, &record); err != nil {
		return OutcomeRecord{}, false, err
	}
	if err := record.Envelope.Validate(); err != nil {
		return OutcomeRecord{}, false, fmt.Errorf("validate outcome outbox: %w", err)
	}
	switch record.State {
	case OutcomeCoordinatorPending,
		OutcomeCoordinatorNeedsAck,
		OutcomeCoordinatorAcknowledged:
	default:
		return OutcomeRecord{}, false, fmt.Errorf(
			"outcome coordinator state %q is invalid",
			record.State,
		)
	}
	if record.PendingAt.IsZero() {
		return OutcomeRecord{}, false, fmt.Errorf("outcome pending timestamp is required")
	}
	return record, true, nil
}

func validateOutcomeDelivery(delivery OutcomeDelivery) error {
	for _, field := range []struct {
		label string
		value string
	}{
		{label: "outcome id", value: delivery.OutcomeID},
		{label: "outcome work id", value: delivery.WorkID},
		{label: "outcome coordinator fence", value: delivery.CoordinatorFence},
	} {
		if err := validateSegment(field.label, field.value); err != nil {
			return err
		}
	}
	if len(delivery.DeliveryRef) > 512 ||
		!uriPattern.MatchString(delivery.DeliveryRef) {
		return fmt.Errorf("outcome delivery ref must be a compact absolute reference")
	}
	return nil
}

func validateOutcomeAcknowledgement(ack OutcomeAcknowledgement) error {
	if err := validateStoreRef(ack.StoreRef); err != nil {
		return fmt.Errorf("outcome acknowledgement store ref: %w", err)
	}
	if err := validateOutcomeDelivery(OutcomeDelivery{
		OutcomeID:        ack.OutcomeID,
		WorkID:           ack.WorkID,
		DeliveryRef:      "local:ack-validation",
		CoordinatorFence: ack.CoordinatorFence,
	}); err != nil {
		return err
	}
	return validateSegment("outcome acknowledged by", ack.AcknowledgedBy)
}

func validateOutcomeFinalizationRequest(request OutcomeFinalizationRequest) error {
	if err := validateStoreRef(request.StoreRef); err != nil {
		return fmt.Errorf("outcome finalization store ref: %w", err)
	}
	if err := validateOutcomeDelivery(OutcomeDelivery{
		OutcomeID:        request.OutcomeID,
		WorkID:           request.WorkID,
		DeliveryRef:      "local:finalization-validation",
		CoordinatorFence: request.CoordinatorFence,
	}); err != nil {
		return err
	}
	if request.ProducerGeneration <= 0 {
		return fmt.Errorf("outcome finalization producer generation must be positive")
	}
	if err := validateSegment(
		"outcome finalization producer token",
		request.ProducerToken,
	); err != nil {
		return err
	}
	if err := validateSegment("outcome finalized by", request.FinalizedBy); err != nil {
		return err
	}
	return validateOutcomeCloseReason(request.CloseReason)
}

func validateOutcomeFinalizationReceipt(receipt OutcomeFinalizationReceipt) error {
	if receipt.ContractVersion != 1 {
		return fmt.Errorf(
			"outcome finalization contract version %d is unsupported",
			receipt.ContractVersion,
		)
	}
	if err := validateOutcomeFinalizationRequest(OutcomeFinalizationRequest{
		StoreRef:           receipt.StoreRef,
		OutcomeID:          receipt.OutcomeID,
		WorkID:             receipt.WorkID,
		ProducerGeneration: receipt.ProducerGeneration,
		ProducerToken:      receipt.ProducerToken,
		CoordinatorFence:   receipt.CoordinatorFence,
		FinalizedBy:        receipt.FinalizedBy,
		CloseReason:        receipt.CloseReason,
	}); err != nil {
		return err
	}
	if receipt.AcknowledgedAt.IsZero() {
		return fmt.Errorf("outcome finalization acknowledged_at is required")
	}
	if err := validateSegment(
		"outcome finalization acknowledged by",
		receipt.AcknowledgedBy,
	); err != nil {
		return err
	}
	if receipt.FinalizedAt.IsZero() {
		return fmt.Errorf("outcome finalization finalized_at is required")
	}
	return nil
}

func validateOutcomeCloseReason(reason string) error {
	if !utf8.ValidString(reason) {
		return fmt.Errorf("outcome finalization close reason must be valid UTF-8")
	}
	if strings.TrimSpace(reason) == "" || len(reason) > 512 {
		return fmt.Errorf(
			"outcome finalization close reason must contain 1-512 bytes",
		)
	}
	if strings.ContainsRune(reason, '\x00') {
		return fmt.Errorf("outcome finalization close reason contains NUL")
	}
	return nil
}

func outcomeFinalizationFromMetadata(
	metadata map[string]any,
) (OutcomeFinalizationReceipt, bool, error) {
	if metadataString(metadata, metadataOutcomeFinalization) == "" {
		return OutcomeFinalizationReceipt{}, false, nil
	}
	var receipt OutcomeFinalizationReceipt
	if err := decodeMetadataJSON(
		metadata,
		metadataOutcomeFinalization,
		&receipt,
	); err != nil {
		return OutcomeFinalizationReceipt{}, false, err
	}
	if err := validateOutcomeFinalizationReceipt(receipt); err != nil {
		return OutcomeFinalizationReceipt{}, false, err
	}
	return receipt, true, nil
}

func outcomeFinalizationMatchesRequest(
	receipt OutcomeFinalizationReceipt,
	request OutcomeFinalizationRequest,
) bool {
	return receipt.StoreRef == request.StoreRef &&
		receipt.OutcomeID == request.OutcomeID &&
		receipt.WorkID == request.WorkID &&
		receipt.ProducerGeneration == request.ProducerGeneration &&
		receipt.ProducerToken == request.ProducerToken &&
		receipt.CoordinatorFence == request.CoordinatorFence &&
		receipt.FinalizedBy == request.FinalizedBy &&
		receipt.CloseReason == request.CloseReason
}

func outcomeFinalizationMatchesRecord(
	receipt OutcomeFinalizationReceipt,
	record OutcomeRecord,
) bool {
	return record.State == OutcomeCoordinatorAcknowledged &&
		!record.AcknowledgedAt.IsZero() &&
		record.AcknowledgedBy != "" &&
		receipt.StoreRef == record.Envelope.StoreRef &&
		receipt.OutcomeID == record.Envelope.OutcomeID &&
		receipt.WorkID == record.Envelope.WorkID &&
		receipt.ProducerGeneration == record.Envelope.Fence.Generation &&
		receipt.ProducerToken == record.Envelope.Fence.Token &&
		receipt.CoordinatorFence == record.CoordinatorFence &&
		receipt.AcknowledgedAt.Equal(record.AcknowledgedAt) &&
		receipt.AcknowledgedBy == record.AcknowledgedBy
}

func cloneOutcomeReady(outcome OutcomeReady) OutcomeReady {
	cloned := outcome
	cloned.Evidence = cloneArtifacts(outcome.Evidence)
	return cloned
}

func cloneOutcomeRecord(record OutcomeRecord) OutcomeRecord {
	cloned := record
	cloned.Envelope = cloneOutcomeReady(record.Envelope)
	return cloned
}

func cloneOutcomeRecords(records []OutcomeRecord) []OutcomeRecord {
	cloned := make([]OutcomeRecord, len(records))
	for index, record := range records {
		cloned[index] = cloneOutcomeRecord(record)
	}
	return cloned
}

func outcomesEqual(left, right OutcomeReady) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}
