package temporalbeads

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	OutcomeContractVersion       = 1
	MaxOutcomeEvidenceReferences = 64
)

// OutcomeProducerKind identifies the compatibility boundary that observed the
// terminal or verified work fact.
type OutcomeProducerKind string

const (
	OutcomeProducerDirectWorker     OutcomeProducerKind = "direct-worker"
	OutcomeProducerFormulaStep      OutcomeProducerKind = "formula-step"
	OutcomeProducerProjectLead      OutcomeProducerKind = "project-lead"
	OutcomeProducerMaintenanceOrder OutcomeProducerKind = "maintenance-order"
	OutcomeProducerTemporal         OutcomeProducerKind = "temporal"
)

// OutcomeEvidenceState distinguishes a terminal execution from a verified
// result that intentionally leaves its source root open.
type OutcomeEvidenceState string

const (
	OutcomeEvidenceTerminal OutcomeEvidenceState = "terminal"
	OutcomeEvidenceVerified OutcomeEvidenceState = "verified"
)

// OutcomeCoordinatorState is the canonical coordinator-delivery lifecycle.
type OutcomeCoordinatorState string

const (
	OutcomeCoordinatorPending      OutcomeCoordinatorState = "pending"
	OutcomeCoordinatorNeedsAck     OutcomeCoordinatorState = "needs-coordinator-ack"
	OutcomeCoordinatorAcknowledged OutcomeCoordinatorState = "acknowledged"
)

// OutcomeProducerFence identifies the exact producer attempt that observed the
// outcome. Generation changes intentionally produce a new Outcome ID.
type OutcomeProducerFence struct {
	ProducerID string `json:"producer_id"`
	Generation int64  `json:"generation"`
	Token      string `json:"token"`
}

func (f OutcomeProducerFence) Validate() error {
	if err := validateSegment("outcome producer fence", f.ProducerID); err != nil {
		return err
	}
	if f.Generation <= 0 {
		return fmt.Errorf("outcome producer fence generation must be positive")
	}
	if err := validateSegment("outcome producer fence token", f.Token); err != nil {
		return err
	}
	return nil
}

// OutcomeReady is the versioned citywide result envelope. It contains only
// compact identity, meaning, and evidence references; transcripts never enter
// Temporal history or canonical metadata.
type OutcomeReady struct {
	ContractVersion int                  `json:"contract_version"`
	OutcomeID       string               `json:"outcome_id"`
	Producer        OutcomeProducerKind  `json:"producer"`
	StoreRef        string               `json:"store_ref"`
	WorkID          string               `json:"work_id"`
	SourceRootID    string               `json:"source_root_id"`
	WorkflowID      string               `json:"workflow_id,omitempty"`
	WorkflowRunID   string               `json:"workflow_run_id,omitempty"`
	StepKey         string               `json:"step_key"`
	Summary         string               `json:"summary"`
	Impact          string               `json:"impact"`
	State           OutcomeEvidenceState `json:"state"`
	Evidence        []ArtifactRef        `json:"evidence"`
	Fence           OutcomeProducerFence `json:"fence"`
	OccurredAt      time.Time            `json:"occurred_at"`
}

// OutcomeAdapterInput is the shared input consumed by every compatibility
// adapter.
type OutcomeAdapterInput struct {
	StoreRef      string               `json:"store_ref"`
	WorkID        string               `json:"work_id"`
	SourceRootID  string               `json:"source_root_id"`
	WorkflowID    string               `json:"workflow_id,omitempty"`
	WorkflowRunID string               `json:"workflow_run_id,omitempty"`
	StepKey       string               `json:"step_key"`
	Summary       string               `json:"summary"`
	Impact        string               `json:"impact"`
	State         OutcomeEvidenceState `json:"state"`
	Evidence      []ArtifactRef        `json:"evidence"`
	Fence         OutcomeProducerFence `json:"fence"`
	OccurredAt    time.Time            `json:"occurred_at"`
}

func DirectWorkerOutcome(input OutcomeAdapterInput) (OutcomeReady, error) {
	return newOutcomeReady(OutcomeProducerDirectWorker, input)
}

func FormulaStepOutcome(input OutcomeAdapterInput) (OutcomeReady, error) {
	return newOutcomeReady(OutcomeProducerFormulaStep, input)
}

func ProjectLeadOutcome(input OutcomeAdapterInput) (OutcomeReady, error) {
	return newOutcomeReady(OutcomeProducerProjectLead, input)
}

func MaintenanceOrderOutcome(input OutcomeAdapterInput) (OutcomeReady, error) {
	return newOutcomeReady(OutcomeProducerMaintenanceOrder, input)
}

func TemporalOutcome(input OutcomeAdapterInput) (OutcomeReady, error) {
	return newOutcomeReady(OutcomeProducerTemporal, input)
}

func temporalCompletionOutcome(
	record BeadRecord,
	completion Completion,
	storeRef string,
	occurredAt time.Time,
) (OutcomeReady, error) {
	evidence := cloneArtifacts(completion.ArtifactRefs)
	if len(evidence) == 0 {
		receipt, err := terminalReceiptArtifact(record, completion)
		if err != nil {
			return OutcomeReady{}, err
		}
		evidence = []ArtifactRef{receipt}
	}
	return TemporalOutcome(OutcomeAdapterInput{
		StoreRef:      storeRef,
		WorkID:        record.BeadID,
		SourceRootID:  record.Formula.RootID,
		WorkflowID:    completion.SourceWorkflowID,
		WorkflowRunID: completion.SourceWorkflowRunID,
		StepKey:       record.Formula.StepKey,
		Summary: fmt.Sprintf(
			"Temporal completed %s for %s.",
			record.Formula.StepKey,
			record.BeadID,
		),
		Impact:   "The verified terminal result is ready for coordinator acknowledgement.",
		State:    OutcomeEvidenceTerminal,
		Evidence: evidence,
		Fence: OutcomeProducerFence{
			ProducerID: completion.SessionID,
			Generation: completion.Generation,
			Token:      completion.ClaimToken,
		},
		OccurredAt: occurredAt,
	})
}

func terminalReceiptArtifact(
	record BeadRecord,
	completion Completion,
) (ArtifactRef, error) {
	receipt := struct {
		BeadID        string  `json:"bead_id"`
		Generation    int64   `json:"generation"`
		WorkflowID    string  `json:"workflow_id"`
		WorkflowRunID string  `json:"workflow_run_id"`
		SessionID     string  `json:"session_id"`
		Outcome       Outcome `json:"outcome"`
	}{
		BeadID: record.BeadID, Generation: completion.Generation,
		WorkflowID:    completion.SourceWorkflowID,
		WorkflowRunID: completion.SourceWorkflowRunID,
		SessionID:     completion.SessionID, Outcome: completion.Outcome,
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("encode terminal receipt evidence: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return ArtifactRef{
		Kind:   ArtifactKindReviewRecord,
		URI:    "bead:" + record.BeadID,
		SHA256: hex.EncodeToString(sum[:]),
	}, nil
}

func newOutcomeReady(
	producer OutcomeProducerKind,
	input OutcomeAdapterInput,
) (OutcomeReady, error) {
	envelope := OutcomeReady{
		ContractVersion: OutcomeContractVersion,
		Producer:        producer,
		StoreRef:        input.StoreRef,
		WorkID:          input.WorkID,
		SourceRootID:    input.SourceRootID,
		WorkflowID:      input.WorkflowID,
		WorkflowRunID:   input.WorkflowRunID,
		StepKey:         input.StepKey,
		Summary:         input.Summary,
		Impact:          input.Impact,
		State:           input.State,
		Evidence:        cloneArtifacts(input.Evidence),
		Fence:           input.Fence,
		OccurredAt:      input.OccurredAt.UTC(),
	}
	outcomeID, err := outcomeReadyID(envelope)
	if err != nil {
		return OutcomeReady{}, err
	}
	envelope.OutcomeID = outcomeID
	if err := envelope.Validate(); err != nil {
		return OutcomeReady{}, err
	}
	return envelope, nil
}

// Validate rejects envelopes that cannot be deduplicated or understood without
// opening a transcript.
func (o OutcomeReady) Validate() error {
	if o.ContractVersion != OutcomeContractVersion {
		return fmt.Errorf("unsupported outcome contract version %d", o.ContractVersion)
	}
	switch o.Producer {
	case OutcomeProducerDirectWorker,
		OutcomeProducerFormulaStep,
		OutcomeProducerProjectLead,
		OutcomeProducerMaintenanceOrder,
		OutcomeProducerTemporal:
	default:
		return fmt.Errorf("outcome producer %q is invalid", o.Producer)
	}
	if err := validateStoreRef(o.StoreRef); err != nil {
		return err
	}
	for _, field := range []struct {
		label string
		value string
	}{
		{label: "outcome work id", value: o.WorkID},
		{label: "outcome source root id", value: o.SourceRootID},
		{label: "outcome step key", value: o.StepKey},
	} {
		if err := validateSegment(field.label, field.value); err != nil {
			return err
		}
	}
	if o.WorkflowID == "" && o.WorkflowRunID != "" {
		return fmt.Errorf("outcome workflow run requires a workflow id")
	}
	if o.WorkflowID != "" {
		if err := validateWorkflowID("outcome workflow id", o.WorkflowID); err != nil {
			return err
		}
		if err := validateSegment("outcome workflow run id", o.WorkflowRunID); err != nil {
			return err
		}
	}
	if err := validateOutcomeMeaning("summary", o.Summary, 512); err != nil {
		return err
	}
	if err := validateOutcomeMeaning("impact", o.Impact, 1024); err != nil {
		return err
	}
	switch o.State {
	case OutcomeEvidenceTerminal, OutcomeEvidenceVerified:
	default:
		return fmt.Errorf("outcome evidence state %q is invalid", o.State)
	}
	if len(o.Evidence) == 0 {
		return fmt.Errorf("outcome evidence reference is required")
	}
	if len(o.Evidence) > MaxOutcomeEvidenceReferences {
		return fmt.Errorf(
			"outcome evidence must contain at most %d references",
			MaxOutcomeEvidenceReferences,
		)
	}
	for _, evidence := range o.Evidence {
		if err := evidence.Validate(); err != nil {
			return err
		}
	}
	if err := o.Fence.Validate(); err != nil {
		return err
	}
	if o.OccurredAt.IsZero() {
		return fmt.Errorf("outcome occurred_at is required")
	}
	expected, err := outcomeReadyID(o)
	if err != nil {
		return err
	}
	if o.OutcomeID != expected {
		return fmt.Errorf("outcome id does not match its producer fence")
	}
	return nil
}

func outcomeReadyID(outcome OutcomeReady) (string, error) {
	identity := struct {
		ContractVersion int                  `json:"contract_version"`
		Producer        OutcomeProducerKind  `json:"producer"`
		StoreRef        string               `json:"store_ref"`
		WorkID          string               `json:"work_id"`
		State           OutcomeEvidenceState `json:"state"`
		Fence           OutcomeProducerFence `json:"fence"`
	}{
		ContractVersion: outcome.ContractVersion,
		Producer:        outcome.Producer,
		StoreRef:        outcome.StoreRef,
		WorkID:          outcome.WorkID,
		State:           outcome.State,
		Fence:           outcome.Fence,
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode outcome identity: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return "outcome-" + hex.EncodeToString(sum[:16]), nil
}

func validateStoreRef(storeRef string) error {
	parts := strings.Split(storeRef, ":")
	if len(parts) != 2 || (parts[0] != "city" && parts[0] != "rig") {
		return fmt.Errorf("outcome store ref must be city:<id> or rig:<id>")
	}
	return validateSegment("outcome store id", parts[1])
}

// ValidateOutcomeStoreRef verifies the stable canonical-store identity shared
// by outcome producers and consumers.
func ValidateOutcomeStoreRef(storeRef string) error {
	return validateStoreRef(storeRef)
}

func validateOutcomeMeaning(label, value string, limit int) error {
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("outcome %s must be non-empty and trimmed", label)
	}
	if len(value) > limit {
		return fmt.Errorf("outcome %s exceeds %d characters", label, limit)
	}
	if strings.ContainsAny(value, "\x00\r") {
		return fmt.Errorf("outcome %s contains control characters", label)
	}
	return nil
}
