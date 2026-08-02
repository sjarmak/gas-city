package temporalbeads

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

const (
	CurrentContractVersion = 1
	DefaultEventLimit      = 1024
	AgentTaskQueue         = "gascity-agent-work"
	SignalReady            = "bead.ready"
	SignalClose            = "orchestration.close"
	SignalParentChildLink  = "maintenance.bead-child-linked"
	QueryState             = "bead-orchestration-state"
	formulaSegmentLimit    = 96
)

var (
	segmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	sha256Pattern  = regexp.MustCompile(`^[a-f0-9]{64}$`)
	uriPattern     = regexp.MustCompile(`^[a-z][a-z0-9+.-]*:[^\s]+$`)
)

// ArtifactKind identifies a durable artifact without embedding its contents.
type ArtifactKind string

const (
	ArtifactKindCommit       ArtifactKind = "commit"
	ArtifactKindPatch        ArtifactKind = "patch"
	ArtifactKindReviewRecord ArtifactKind = "review-record"
	ArtifactKindTestReport   ArtifactKind = "test-report"
)

// ArtifactRef is the only representation of agent output allowed in history.
type ArtifactRef struct {
	Kind   ArtifactKind `json:"kind"`
	URI    string       `json:"uri"`
	SHA256 string       `json:"sha256"`
}

// FormulaRef is the immutable formula-step identity captured at ready time.
//
// Workflows and Activities consume this snapshot. They must not resolve
// mutable formula files while replaying or executing a ready event.
type FormulaRef struct {
	Name    string `json:"name"`
	Hash    string `json:"hash"`
	Version string `json:"version"`
	RootID  string `json:"root_id"`
	StepKey string `json:"step_key"`
	Rig     string `json:"rig"`
	// ParentWorkflowID/ParentRunID retain the maintenance-cycle topology when
	// this formula step is delivered through external Signal-With-Start.
	ParentWorkflowID string `json:"parent_workflow_id,omitempty"`
	ParentRunID      string `json:"parent_run_id,omitempty"`
}

// Validate rejects incomplete or mutable formula identities.
func (r FormulaRef) Validate() error {
	for _, field := range []struct {
		label string
		value string
	}{
		{label: "formula name", value: r.Name},
		{label: "formula version", value: r.Version},
		{label: "formula root id", value: r.RootID},
		{label: "formula step key", value: r.StepKey},
		{label: "formula rig", value: r.Rig},
	} {
		if err := validateSegment(field.label, field.value); err != nil {
			return err
		}
		if len(field.value) > formulaSegmentLimit {
			return fmt.Errorf("%s exceeds %d characters", field.label, formulaSegmentLimit)
		}
	}
	if !sha256Pattern.MatchString(r.Hash) {
		return fmt.Errorf("formula hash must be 64 lowercase hex characters")
	}
	if r.ParentWorkflowID == "" && r.ParentRunID != "" {
		return fmt.Errorf("formula parent run requires a parent workflow id")
	}
	if r.ParentWorkflowID != "" {
		if err := validateWorkflowID("formula parent workflow id", r.ParentWorkflowID); err != nil {
			return err
		}
	}
	if r.ParentRunID != "" {
		if err := validateSegment("formula parent run id", r.ParentRunID); err != nil {
			return err
		}
	}
	return nil
}

// ChildWorkflowLink is the compact logical parent/child edge retained by the
// MaintenanceCycleWorkflow for externally Signal-With-Started children.
type ChildWorkflowLink struct {
	EventID         string `json:"event_id"`
	BeadID          string `json:"bead_id"`
	FormulaRootID   string `json:"formula_root_id"`
	StepKey         string `json:"step_key"`
	ChildWorkflowID string `json:"child_workflow_id"`
	ChildRunID      string `json:"child_run_id"`
	Status          string `json:"status"`
	ErrorCode       string `json:"error_code,omitempty"`
}

const (
	ChildWorkflowStarted   = "started"
	ChildWorkflowCompleted = "completed"
	ChildWorkflowFailed    = "failed"
)

// NewChildWorkflowLink builds the stable topology signal after Temporal accepts
// the child Workflow start or signal.
func NewChildWorkflowLink(
	event ReadyEvent,
	receipt WorkflowReceipt,
	status string,
	errorCode string,
) (ChildWorkflowLink, error) {
	if err := event.Validate(); err != nil {
		return ChildWorkflowLink{}, err
	}
	link := ChildWorkflowLink{
		EventID:         event.EventID,
		BeadID:          event.BeadID,
		FormulaRootID:   event.Formula.RootID,
		StepKey:         event.Formula.StepKey,
		ChildWorkflowID: receipt.WorkflowID,
		ChildRunID:      receipt.RunID,
		Status:          status,
		ErrorCode:       errorCode,
	}
	return link, link.Validate()
}

// Validate rejects links that cannot be used as deterministic Workflow state.
func (l ChildWorkflowLink) Validate() error {
	for _, field := range []struct {
		label string
		value string
	}{
		{label: "child event id", value: l.EventID},
		{label: "child bead id", value: l.BeadID},
		{label: "child formula root id", value: l.FormulaRootID},
		{label: "child formula step key", value: l.StepKey},
		{label: "child run id", value: l.ChildRunID},
	} {
		if err := validateSegment(field.label, field.value); err != nil {
			return err
		}
	}
	if err := validateWorkflowID("child workflow id", l.ChildWorkflowID); err != nil {
		return err
	}
	switch l.Status {
	case ChildWorkflowStarted, ChildWorkflowCompleted:
		if l.ErrorCode != "" {
			return fmt.Errorf("successful child workflow link cannot have an error code")
		}
	case ChildWorkflowFailed:
		if err := validateSegment("child workflow error code", l.ErrorCode); err != nil {
			return err
		}
	default:
		return fmt.Errorf("child workflow status %q is invalid", l.Status)
	}
	return nil
}

// Terminal reports whether the maintenance parent received a child result.
func (l ChildWorkflowLink) Terminal() bool {
	return l.Status == ChildWorkflowCompleted || l.Status == ChildWorkflowFailed
}

// Validate rejects inline content and unstable artifact identities.
func (r ArtifactRef) Validate() error {
	switch r.Kind {
	case ArtifactKindCommit, ArtifactKindPatch, ArtifactKindReviewRecord, ArtifactKindTestReport:
	default:
		return fmt.Errorf("artifact kind %q is not allowed", r.Kind)
	}
	if len(r.URI) > 512 || !uriPattern.MatchString(r.URI) {
		return fmt.Errorf("artifact uri must be a compact absolute reference")
	}
	scheme, _, _ := strings.Cut(r.URI, ":")
	switch scheme {
	case "bead", "file", "git", "https", "s3":
	default:
		return fmt.Errorf("artifact uri scheme %q is not allowed", scheme)
	}
	if !sha256Pattern.MatchString(r.SHA256) {
		return fmt.Errorf("artifact sha256 must be 64 lowercase hex characters")
	}
	return nil
}

// ReadyEvent is one durable Beads readiness transition.
type ReadyEvent struct {
	ContractVersion int           `json:"contract_version"`
	EventID         string        `json:"event_id"`
	CityID          string        `json:"city_id"`
	RunID           string        `json:"run_id"`
	BeadID          string        `json:"bead_id"`
	Generation      int64         `json:"generation"`
	Formula         FormulaRef    `json:"formula"`
	ReadyAt         time.Time     `json:"ready_at"`
	Artifacts       []ArtifactRef `json:"artifacts,omitempty"`
}

// NewReadyEvent derives the event identity from the authoritative transition.
func NewReadyEvent(
	cityID string,
	runID string,
	beadID string,
	generation int64,
	formula FormulaRef,
	readyAt time.Time,
) (ReadyEvent, error) {
	event := ReadyEvent{
		ContractVersion: CurrentContractVersion,
		CityID:          cityID,
		RunID:           runID,
		BeadID:          beadID,
		Generation:      generation,
		Formula:         formula,
		ReadyAt:         readyAt.UTC(),
	}
	event.EventID = readyEventID(cityID, runID, beadID, generation)
	return event, event.Validate()
}

// Key is the generation identity within a Beads record.
func (e ReadyEvent) Key() string {
	return fmt.Sprintf("%s/%d", e.BeadID, e.Generation)
}

// Validate checks the stable event identity and all boundary references.
func (e ReadyEvent) Validate() error {
	return e.validate(true)
}

func (e ReadyEvent) validate(requireFormula bool) error {
	if e.ContractVersion != CurrentContractVersion {
		return fmt.Errorf("unsupported contract version %d", e.ContractVersion)
	}
	for _, field := range []struct {
		label string
		value string
	}{
		{label: "event id", value: e.EventID},
		{label: "city id", value: e.CityID},
		{label: "run id", value: e.RunID},
		{label: "bead id", value: e.BeadID},
	} {
		if err := validateSegment(field.label, field.value); err != nil {
			return err
		}
	}
	if e.Generation <= 0 {
		return fmt.Errorf("generation must be positive")
	}
	if e.ReadyAt.IsZero() {
		return fmt.Errorf("ready_at must not be zero")
	}
	if requireFormula {
		if err := e.Formula.Validate(); err != nil {
			return err
		}
	}
	expected := readyEventID(e.CityID, e.RunID, e.BeadID, e.Generation)
	if e.EventID != expected {
		return fmt.Errorf("event id %q does not match readiness transition", e.EventID)
	}
	for index, artifact := range e.Artifacts {
		if err := artifact.Validate(); err != nil {
			return fmt.Errorf("artifact %d: %w", index, err)
		}
	}
	return nil
}

func (e ReadyEvent) validatePayload() error {
	return e.Validate()
}

// WorkflowInput identifies a deterministic orchestration run.
type WorkflowInput struct {
	ContractVersion  int           `json:"contract_version"`
	CityID           string        `json:"city_id"`
	RunID            string        `json:"run_id"`
	BeadID           string        `json:"bead_id,omitempty"`
	InitialReady     []ReadyEvent  `json:"initial_ready,omitempty"`
	CloseWhenIdle    bool          `json:"close_when_idle"`
	SearchAttributes bool          `json:"search_attributes,omitempty"`
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"`
	EventLimit       int           `json:"event_limit"`
}

// CloseRequest seals a run only after every authoritative ready event is seen.
type CloseRequest struct {
	ContractVersion  int      `json:"contract_version"`
	CityID           string   `json:"city_id"`
	RunID            string   `json:"run_id"`
	BeadID           string   `json:"bead_id,omitempty"`
	ExpectedEventIDs []string `json:"expected_event_ids"`
	ReasonCode       string   `json:"reason_code"`
}

func (r CloseRequest) validatePayload() error {
	if r.ContractVersion != CurrentContractVersion {
		return fmt.Errorf("unsupported contract version %d", r.ContractVersion)
	}
	if err := validateSegment("city id", r.CityID); err != nil {
		return err
	}
	if err := validateSegment("run id", r.RunID); err != nil {
		return err
	}
	if r.BeadID != "" {
		if err := validateSegment("bead id", r.BeadID); err != nil {
			return err
		}
	}
	if err := validateSegment("reason code", r.ReasonCode); err != nil {
		return err
	}
	if len(r.ExpectedEventIDs) == 0 {
		return fmt.Errorf("expected_event_ids must contain the authoritative run set")
	}
	seen := make(map[string]struct{}, len(r.ExpectedEventIDs))
	for _, eventID := range r.ExpectedEventIDs {
		if err := validateSegment("event id", eventID); err != nil {
			return err
		}
		if _, exists := seen[eventID]; exists {
			return fmt.Errorf("duplicate expected event id %q", eventID)
		}
		seen[eventID] = struct{}{}
	}
	return nil
}

func (i WorkflowInput) validatePayload() error {
	return i.validate(true)
}

func (i WorkflowInput) validate(requireFormula bool) error {
	if i.ContractVersion != CurrentContractVersion {
		return fmt.Errorf("unsupported contract version %d", i.ContractVersion)
	}
	if err := validateSegment("city id", i.CityID); err != nil {
		return err
	}
	if err := validateSegment("run id", i.RunID); err != nil {
		return err
	}
	if requireFormula {
		if err := validateSegment("bead id", i.BeadID); err != nil {
			return err
		}
	} else if i.BeadID != "" {
		if err := validateSegment("bead id", i.BeadID); err != nil {
			return err
		}
	}
	if i.HeartbeatTimeout <= 0 {
		return fmt.Errorf("heartbeat_timeout must be positive")
	}
	if i.EventLimit < 0 || i.EventLimit > DefaultEventLimit {
		return fmt.Errorf("event_limit must be between 0 and %d", DefaultEventLimit)
	}
	for _, event := range i.InitialReady {
		if err := validateEventForRun(
			i.CityID,
			i.RunID,
			i.BeadID,
			event,
			requireFormula,
		); err != nil {
			return err
		}
	}
	return nil
}

// WorkflowState is queryable procedure state, never a copy of Beads facts.
type WorkflowState struct {
	ContractVersion   int      `json:"contract_version"`
	CityID            string   `json:"city_id"`
	RunID             string   `json:"run_id"`
	BeadID            string   `json:"bead_id,omitempty"`
	Phase             string   `json:"phase"`
	ReceivedEventIDs  []string `json:"received_event_ids,omitempty"`
	CompletedEventIDs []string `json:"completed_event_ids,omitempty"`
	FailedEventIDs    []string `json:"failed_event_ids,omitempty"`
	LastErrorCode     string   `json:"last_error_code,omitempty"`
}

func (s WorkflowState) validatePayload() error {
	if s.ContractVersion != CurrentContractVersion {
		return fmt.Errorf("unsupported contract version %d", s.ContractVersion)
	}
	if err := validateSegment("city id", s.CityID); err != nil {
		return err
	}
	if err := validateSegment("run id", s.RunID); err != nil {
		return err
	}
	if s.BeadID != "" {
		if err := validateSegment("bead id", s.BeadID); err != nil {
			return err
		}
	}
	switch s.Phase {
	case workflowPhaseRunning, workflowPhaseCompleted, workflowPhaseFailed:
	default:
		return fmt.Errorf("workflow phase %q is invalid", s.Phase)
	}
	for _, eventID := range append(
		append(append([]string(nil), s.ReceivedEventIDs...), s.CompletedEventIDs...),
		s.FailedEventIDs...,
	) {
		if err := validateSegment("event id", eventID); err != nil {
			return err
		}
	}
	return nil
}

// ClaimRequest asks Beads for one exact generation owned by one Workflow.
type ClaimRequest struct {
	BeadID     string `json:"bead_id"`
	Generation int64  `json:"generation"`
	WorkflowID string `json:"workflow_id"`
}

// ClaimLease fences every mutation made by an Activity.
type ClaimLease struct {
	BeadID     string `json:"bead_id"`
	Generation int64  `json:"generation"`
	Token      string `json:"token"`
	Acquired   bool   `json:"acquired"`
}

// HeartbeatCheckpoint is the compact resumable Activity checkpoint.
type HeartbeatCheckpoint struct {
	BeadID                string        `json:"bead_id"`
	Generation            int64         `json:"generation"`
	ClaimToken            string        `json:"claim_token"`
	SessionID             string        `json:"session_id"`
	Sequence              int64         `json:"sequence"`
	Phase                 string        `json:"phase"`
	ArtifactRefs          []ArtifactRef `json:"artifact_refs,omitempty"`
	ArtifactRefsTruncated bool          `json:"artifact_refs_truncated,omitempty"`
}

func (h HeartbeatCheckpoint) validatePayload() error {
	if h.Generation <= 0 || h.Sequence < 0 {
		return fmt.Errorf("heartbeat generation must be positive and sequence non-negative")
	}
	if h.Sequence == 0 &&
		(h.Phase != CheckpointPhaseAttached || len(h.ArtifactRefs) != 0) {
		return fmt.Errorf("sequence zero is reserved for the attachment checkpoint")
	}
	for _, field := range []struct {
		label string
		value string
	}{
		{label: "bead id", value: h.BeadID},
		{label: "claim token", value: h.ClaimToken},
		{label: "session id", value: h.SessionID},
		{label: "phase", value: h.Phase},
	} {
		if err := validateSegment(field.label, field.value); err != nil {
			return err
		}
	}
	for _, artifact := range h.ArtifactRefs {
		if err := artifact.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// ActivityInput supplies work identity; Activity Workers never discover work.
type ActivityInput struct {
	Event ReadyEvent `json:"event"`
}

func (i ActivityInput) validatePayload() error {
	return i.Event.Validate()
}

// ActivityResult contains only a terminal code and durable references.
type ActivityResult struct {
	EventID               string        `json:"event_id"`
	Outcome               string        `json:"outcome"`
	SessionID             string        `json:"session_id"`
	ArtifactRefs          []ArtifactRef `json:"artifact_refs,omitempty"`
	ArtifactRefsTruncated bool          `json:"artifact_refs_truncated,omitempty"`
}

func (r ActivityResult) validatePayload() error {
	if err := validateSegment("event id", r.EventID); err != nil {
		return err
	}
	if err := validateSegment("session id", r.SessionID); err != nil {
		return err
	}
	if r.Outcome != string(OutcomeCompleted) {
		return fmt.Errorf("activity outcome %q is invalid", r.Outcome)
	}
	for _, artifact := range r.ArtifactRefs {
		if err := artifact.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type workflowPayload interface {
	validatePayload() error
}

// EncodeWorkflowPayload accepts only the closed set of compact boundary types.
func EncodeWorkflowPayload(payload workflowPayload) ([]byte, error) {
	if payload == nil {
		return nil, fmt.Errorf("workflow payload is required")
	}
	if err := payload.validatePayload(); err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}

// DecodeWorkflowPayload rejects unknown fields and trailing JSON documents.
func DecodeWorkflowPayload(data []byte, destination workflowPayload) error {
	if destination == nil {
		return fmt.Errorf("workflow payload destination is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode workflow payload: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	return destination.validatePayload()
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	switch {
	case err == io.EOF:
		return nil
	case err != nil:
		return fmt.Errorf("decode trailing workflow payload: %w", err)
	default:
		return fmt.Errorf("trailing workflow payload document is not allowed")
	}
}

func readyEventID(cityID, runID, beadID string, generation int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf(
		"%d\x00%s\x00%s\x00%s\x00%d",
		CurrentContractVersion,
		cityID,
		runID,
		beadID,
		generation,
	)))
	return "evt-" + hex.EncodeToString(sum[:16])
}

func validateSegment(label, value string) error {
	if strings.TrimSpace(value) != value || !segmentPattern.MatchString(value) {
		return fmt.Errorf("%s %q is not a stable identifier", label, value)
	}
	return nil
}

func validateWorkflowID(label, value string) error {
	if strings.TrimSpace(value) != value ||
		len(value) > 255 ||
		value == "" ||
		strings.ContainsAny(value, "\x00\n\r\t") {
		return fmt.Errorf("%s %q is not a stable workflow identifier", label, value)
	}
	return nil
}

func validateEventForRun(
	cityID string,
	runID string,
	beadID string,
	event ReadyEvent,
	requireFormula bool,
) error {
	if err := event.validate(requireFormula); err != nil {
		return err
	}
	if event.CityID != cityID || event.RunID != runID {
		return fmt.Errorf("event targets %s/%s, workflow owns %s/%s",
			event.CityID, event.RunID, cityID, runID)
	}
	if beadID != "" && event.BeadID != beadID {
		return fmt.Errorf(
			"event targets bead %s, workflow owns bead %s",
			event.BeadID,
			beadID,
		)
	}
	return nil
}
