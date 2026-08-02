package temporalbeads

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

var (
	ErrBeadNotFound = errors.New("bead not found")
	ErrStaleFence   = errors.New("stale bead generation or claim token")
)

// BeadStatus is the authoritative work-fact lifecycle used by this boundary.
type BeadStatus string

const (
	BeadStatusReady     BeadStatus = "ready"
	BeadStatusClaimed   BeadStatus = "claimed"
	BeadStatusCompleted BeadStatus = "completed"
)

// Outcome is a structured terminal result written conditionally to Beads.
type Outcome string

const (
	OutcomeCompleted Outcome = "completed"
)

// Completion conditionally writes the result for one fenced generation.
type Completion struct {
	BeadID              string        `json:"bead_id"`
	Generation          int64         `json:"generation"`
	ClaimToken          string        `json:"claim_token"`
	SessionID           string        `json:"session_id"`
	SourceWorkflowID    string        `json:"source_workflow_id,omitempty"`
	SourceWorkflowRunID string        `json:"source_workflow_run_id,omitempty"`
	Outcome             Outcome       `json:"outcome"`
	ArtifactRefs        []ArtifactRef `json:"artifact_refs,omitempty"`
}

// AttemptFailure records a failed Activity attempt without releasing its lease.
type AttemptFailure struct {
	BeadID     string `json:"bead_id"`
	Generation int64  `json:"generation"`
	ClaimToken string `json:"claim_token"`
	Attempt    int32  `json:"attempt"`
	Code       string `json:"code"`
}

// BeadRecord is the inspectable work-fact projection owned by Beads.
type BeadRecord struct {
	CityID         string           `json:"city_id"`
	RunID          string           `json:"run_id"`
	BeadID         string           `json:"bead_id"`
	Generation     int64            `json:"generation"`
	Formula        FormulaRef       `json:"formula"`
	Status         BeadStatus       `json:"status"`
	WorkflowID     string           `json:"workflow_id,omitempty"`
	ClaimToken     string           `json:"claim_token,omitempty"`
	SessionID      string           `json:"session_id,omitempty"`
	Outcome        Outcome          `json:"outcome,omitempty"`
	ArtifactRefs   []ArtifactRef    `json:"artifact_refs,omitempty"`
	AttemptFailure []AttemptFailure `json:"attempt_failures,omitempty"`
}

// BeadStore is the generation-fenced Beads boundary used by Activities.
type BeadStore interface {
	Claim(context.Context, ClaimRequest) (ClaimLease, error)
	Complete(context.Context, Completion) error
	RecordAttemptFailure(context.Context, AttemptFailure) error
	Inspect(context.Context, string) (BeadRecord, error)
}

// ReadyEventSource exposes durable, unacknowledged Beads outbox records.
type ReadyEventSource interface {
	PendingReadyEvents(context.Context) ([]ReadyEvent, error)
	AcknowledgeReadyEvent(context.Context, string) error
}

type outboxRecord struct {
	Event          ReadyEvent `json:"event"`
	AcknowledgedAt time.Time  `json:"acknowledged_at,omitempty"`
	SupersededAt   time.Time  `json:"superseded_at,omitempty"`
}

type fileState struct {
	Version int                     `json:"version"`
	Beads   map[string]BeadRecord   `json:"beads"`
	Outbox  map[string]outboxRecord `json:"outbox"`
}

// FileBeadStore is a crash-durable reference adapter for the Beads contract.
//
// Production must bind the same operations to the authoritative Beads
// transaction API. This adapter exists so the boundary and crash windows can be
// exercised without a live Dolt server.
type FileBeadStore struct {
	mu    sync.Mutex
	path  string
	clock Clock
	state fileState
}

// OpenFileBeadStore opens or initializes the crash-durable reference store.
func OpenFileBeadStore(path string, clock Clock) (*FileBeadStore, error) {
	if path == "" {
		return nil, fmt.Errorf("bead store path is required")
	}
	if clock == nil {
		return nil, fmt.Errorf("clock is required")
	}
	store := &FileBeadStore{path: path, clock: clock}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

// TransitionReady writes the new generation and outbox record in one fsync.
func (s *FileBeadStore) TransitionReady(
	_ context.Context,
	cityID string,
	runID string,
	beadID string,
	generation int64,
	formula FormulaRef,
) (ReadyEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.lockState()
	if err != nil {
		return ReadyEvent{}, err
	}
	defer release()

	event, err := NewReadyEvent(
		cityID,
		runID,
		beadID,
		generation,
		formula,
		s.clock.Now(),
	)
	if err != nil {
		return ReadyEvent{}, err
	}
	current, exists := s.state.Beads[beadID]
	if exists && generation < current.Generation {
		return ReadyEvent{}, ErrStaleFence
	}
	if exists && generation == current.Generation {
		record, ok := s.state.Outbox[event.EventID]
		if !ok {
			return ReadyEvent{}, fmt.Errorf("ready generation has no outbox event")
		}
		if record.Event.Formula != formula {
			return ReadyEvent{}, fmt.Errorf(
				"ready generation belongs to a different formula snapshot",
			)
		}
		return cloneReadyEvent(record.Event), nil
	}

	s.state.Beads[beadID] = BeadRecord{
		CityID: cityID, RunID: runID, BeadID: beadID,
		Generation: generation, Formula: formula, Status: BeadStatusReady,
	}
	for eventID, record := range s.state.Outbox {
		if record.Event.BeadID == beadID &&
			record.Event.Generation < generation &&
			record.AcknowledgedAt.IsZero() &&
			record.SupersededAt.IsZero() {
			record.SupersededAt = s.clock.Now().UTC()
			s.state.Outbox[eventID] = record
		}
	}
	s.state.Outbox[event.EventID] = outboxRecord{Event: event}
	if err := s.persistMutationLocked(); err != nil {
		return ReadyEvent{}, err
	}
	return cloneReadyEvent(event), nil
}

// Claim conditionally acquires the exact ready generation.
func (s *FileBeadStore) Claim(
	_ context.Context,
	request ClaimRequest,
) (ClaimLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.lockState()
	if err != nil {
		return ClaimLease{}, err
	}
	defer release()

	record, ok := s.state.Beads[request.BeadID]
	if !ok || record.Generation != request.Generation {
		return ClaimLease{}, nil
	}
	if (record.Status == BeadStatusClaimed || record.Status == BeadStatusCompleted) &&
		record.WorkflowID == request.WorkflowID {
		return leaseFromRecord(record, true), nil
	}
	if record.Status != BeadStatusReady {
		return leaseFromRecord(record, false), nil
	}

	record.Status = BeadStatusClaimed
	record.WorkflowID = request.WorkflowID
	record.ClaimToken = claimToken(request)
	s.state.Beads[request.BeadID] = record
	if err := s.persistMutationLocked(); err != nil {
		return ClaimLease{}, err
	}
	return leaseFromRecord(record, true), nil
}

// Complete conditionally writes one terminal outcome.
func (s *FileBeadStore) Complete(_ context.Context, completion Completion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.lockState()
	if err != nil {
		return err
	}
	defer release()

	record, ok := s.state.Beads[completion.BeadID]
	if !ok || !matchesFence(record, completion.Generation, completion.ClaimToken) {
		return ErrStaleFence
	}
	if record.Status == BeadStatusCompleted {
		if record.SessionID == completion.SessionID &&
			record.Outcome == completion.Outcome &&
			artifactsEqual(record.ArtifactRefs, completion.ArtifactRefs) {
			return nil
		}
		return fmt.Errorf("completion conflicts with existing terminal outcome")
	}
	if record.Status != BeadStatusClaimed {
		return ErrStaleFence
	}
	if err := validateSegment("session id", completion.SessionID); err != nil {
		return err
	}
	if completion.Outcome != OutcomeCompleted {
		return fmt.Errorf("completion outcome %q is invalid", completion.Outcome)
	}
	for _, artifact := range completion.ArtifactRefs {
		if err := artifact.Validate(); err != nil {
			return err
		}
	}
	record.Status = BeadStatusCompleted
	record.SessionID = completion.SessionID
	record.Outcome = completion.Outcome
	record.ArtifactRefs = cloneArtifacts(completion.ArtifactRefs)
	s.state.Beads[completion.BeadID] = record
	return s.persistMutationLocked()
}

// RecordAttemptFailure conditionally appends a compact attempt code.
func (s *FileBeadStore) RecordAttemptFailure(
	_ context.Context,
	failure AttemptFailure,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.lockState()
	if err != nil {
		return err
	}
	defer release()

	record, ok := s.state.Beads[failure.BeadID]
	if !ok || record.Status != BeadStatusClaimed ||
		!matchesFence(record, failure.Generation, failure.ClaimToken) {
		return ErrStaleFence
	}
	for _, existing := range record.AttemptFailure {
		if existing.Attempt == failure.Attempt && existing.Code == failure.Code {
			return nil
		}
	}
	record.AttemptFailure = append(record.AttemptFailure, failure)
	s.state.Beads[failure.BeadID] = record
	return s.persistMutationLocked()
}

// Inspect returns an immutable copy of the current Beads fact.
func (s *FileBeadStore) Inspect(_ context.Context, beadID string) (BeadRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.lockState()
	if err != nil {
		return BeadRecord{}, err
	}
	defer release()

	record, ok := s.state.Beads[beadID]
	if !ok {
		return BeadRecord{}, ErrBeadNotFound
	}
	return cloneBeadRecord(record), nil
}

// PendingReadyEvents returns unacknowledged outbox records in stable order.
func (s *FileBeadStore) PendingReadyEvents(_ context.Context) ([]ReadyEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.lockState()
	if err != nil {
		return nil, err
	}
	defer release()

	events := make([]ReadyEvent, 0, len(s.state.Outbox))
	for _, record := range s.state.Outbox {
		if record.AcknowledgedAt.IsZero() && record.SupersededAt.IsZero() {
			events = append(events, cloneReadyEvent(record.Event))
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].ReadyAt.Equal(events[j].ReadyAt) {
			return events[i].EventID < events[j].EventID
		}
		return events[i].ReadyAt.Before(events[j].ReadyAt)
	})
	return events, nil
}

// AcknowledgeReadyEvent records the Temporal receipt after acknowledgement.
func (s *FileBeadStore) AcknowledgeReadyEvent(_ context.Context, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.lockState()
	if err != nil {
		return err
	}
	defer release()

	record, ok := s.state.Outbox[eventID]
	if !ok {
		return fmt.Errorf("outbox event %s: %w", eventID, ErrBeadNotFound)
	}
	if !record.AcknowledgedAt.IsZero() {
		return nil
	}
	record.AcknowledgedAt = s.clock.Now().UTC()
	s.state.Outbox[eventID] = record
	return s.persistMutationLocked()
}

func (s *FileBeadStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.state = fileState{
			Version: CurrentContractVersion,
			Beads:   make(map[string]BeadRecord),
			Outbox:  make(map[string]outboxRecord),
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read bead store: %w", err)
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		return fmt.Errorf("decode bead store: %w", err)
	}
	if s.state.Version != CurrentContractVersion {
		return fmt.Errorf("unsupported bead store version %d", s.state.Version)
	}
	if s.state.Beads == nil || s.state.Outbox == nil {
		return fmt.Errorf("bead store maps are required")
	}
	return nil
}

func (s *FileBeadStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create bead store directory: %w", err)
	}
	data, err := json.Marshal(s.state)
	if err != nil {
		return fmt.Errorf("encode bead store: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(s.path), ".beads-*.tmp")
	if err != nil {
		return fmt.Errorf("create bead store temp file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		return err
	}
	return fsyncDirectory(filepath.Dir(s.path))
}

func (s *FileBeadStore) persistMutationLocked() error {
	if err := s.persistLocked(); err != nil {
		if reloadErr := s.load(); reloadErr != nil {
			return fmt.Errorf(
				"persist bead store: %v; reload after ambiguous write: %w",
				err,
				reloadErr,
			)
		}
		return err
	}
	return nil
}

func (s *FileBeadStore) lockState() (func(), error) {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return nil, fmt.Errorf("create bead store directory: %w", err)
	}
	lockFile, err := os.OpenFile(s.path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open bead store lock: %w", err)
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("lock bead store: %w", err)
	}
	if err := s.load(); err != nil {
		_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
		_ = lockFile.Close()
		return nil, err
	}
	return func() {
		_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
		_ = lockFile.Close()
	}, nil
}

func fsyncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func claimToken(request ClaimRequest) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf(
		"%s\x00%d\x00%s",
		request.BeadID,
		request.Generation,
		request.WorkflowID,
	)))
	return "claim-" + hex.EncodeToString(sum[:16])
}

func leaseFromRecord(record BeadRecord, acquired bool) ClaimLease {
	return ClaimLease{
		BeadID: record.BeadID, Generation: record.Generation,
		Token: record.ClaimToken, Acquired: acquired,
	}
}

func matchesFence(record BeadRecord, generation int64, token string) bool {
	return record.Generation == generation && record.ClaimToken != "" &&
		record.ClaimToken == token
}

func cloneBeadRecord(record BeadRecord) BeadRecord {
	record.ArtifactRefs = cloneArtifacts(record.ArtifactRefs)
	record.AttemptFailure = append([]AttemptFailure(nil), record.AttemptFailure...)
	return record
}

func cloneReadyEvent(event ReadyEvent) ReadyEvent {
	event.Artifacts = cloneArtifacts(event.Artifacts)
	return event
}

func cloneArtifacts(artifacts []ArtifactRef) []ArtifactRef {
	return append([]ArtifactRef(nil), artifacts...)
}

func artifactsEqual(left, right []ArtifactRef) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
