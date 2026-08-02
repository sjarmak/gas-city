package temporalbeads

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	metadataContractVersion = "gc.temporal.contract_version"
	metadataCityID          = "gc.temporal.city_id"
	metadataRunID           = "gc.temporal.run_id"
	metadataGeneration      = "gc.temporal.generation"
	metadataWorkflowID      = "gc.temporal.workflow_id"
	metadataClaimToken      = "gc.temporal.claim_token"
	metadataSessionID       = "gc.temporal.session_id"
	metadataOutcome         = "gc.temporal.outcome"
	metadataArtifactRefs    = "gc.temporal.artifact_refs"
	metadataAttemptFailures = "gc.temporal.attempt_failures"
	metadataReadyEvent      = "gc.temporal.ready_event"
	metadataReadyEventID    = "gc.temporal.ready_event_id"
	metadataReadyAckAt      = "gc.temporal.ready_acknowledged_at"

	doltConnectionIdleLimit = 30 * time.Second
	doltConnectionLifetime  = 5 * time.Minute
	doltMutationAttempts    = 5
	doltDateTimeLayout      = "2006-01-02 15:04:05.999999"
)

// DoltBeadStoreConfig identifies one canonical Beads database on the managed
// Dolt endpoint. Opening the adapter does not migrate issue data or control the
// shared server lifecycle. Rollout scans require a generation-scoped activation
// receipt written by the explicit single-owner timezone preflight.
type DoltBeadStoreConfig struct {
	Host         string
	Port         int
	Database     string
	User         string
	Password     string
	Actor        string
	CommitAuthor string
	Clock        Clock
	MaxOpenConns int
	// OutcomeRolloutEpoch excludes pre-contract history from legacy outcome
	// reconciliation and silent-outcome health queries.
	OutcomeRolloutEpoch time.Time
	// OutcomeStoreRef identifies this canonical store in every outcome envelope.
	OutcomeStoreRef string
	// EventTimeZone is the explicit IANA zone used by the Dolt server for the
	// native events.created_at wall clock. It is required with rollout scans.
	EventTimeZone string
	// DoltServerPID identifies the loopback managed server whose timezone
	// identity is validated before legacy rollout scans.
	DoltServerPID int
	// DoltServerConfigPath is the exact managed configuration path from the
	// service-readable sql-server command line.
	DoltServerConfigPath string
	// DoltServerGeneration is the sql-server.info generation identifier.
	DoltServerGeneration string
}

// DoltBeadStore binds the Temporal boundary directly to canonical Beads issue
// rows. Orchestration facts live under gc.temporal.* metadata keys; the file
// adapter remains only the reference implementation.
type DoltBeadStore struct {
	db                  *sql.DB
	clock               Clock
	actor               string
	commitAuthor        string
	outcomeRolloutEpoch time.Time
	outcomeStoreRef     string
	// Canonical issues.updated_at is UTC wall time, but native Beads events use
	// DATETIME DEFAULT CURRENT_TIMESTAMP in the loopback server's configured
	// zone. Keep that validated IANA location column-specific so it cannot widen
	// the UTC issue query or inherit a divergent worker TZ setting.
	eventTimeLocation    *time.Location
	eventTimeZone        string
	doltServerPID        int
	doltServerConfigPath string
	doltServerGeneration string
	doltServerIdentity   managedDoltServerIdentity
	recordEventFault     func(string) error
	outcomeMetadataFault func() error
}

var (
	_ BeadStore        = (*DoltBeadStore)(nil)
	_ ReadyEventSource = (*DoltBeadStore)(nil)
)

// OpenDoltBeadStore connects to an existing Beads/Dolt database and verifies
// the single-owner endpoint activation receipt before returning.
func OpenDoltBeadStore(
	ctx context.Context,
	config DoltBeadStoreConfig,
) (*DoltBeadStore, error) {
	if config.Host == "" {
		return nil, fmt.Errorf("dolt host is required")
	}
	if err := validateLoopbackHost(config.Host); err != nil {
		return nil, err
	}
	if config.Port <= 0 || config.Port > 65535 {
		return nil, fmt.Errorf("dolt port must be between 1 and 65535")
	}
	if err := validateDatabaseName(config.Database); err != nil {
		return nil, err
	}
	if config.User == "" {
		return nil, fmt.Errorf("dolt user is required")
	}
	if config.CommitAuthor == "" {
		return nil, fmt.Errorf("dolt commit author is required")
	}
	if err := validateSegment("Beads actor", config.Actor); err != nil {
		return nil, err
	}
	if config.Clock == nil {
		return nil, fmt.Errorf("clock is required")
	}
	if err := ValidateOutcomeStoreRef(config.OutcomeStoreRef); err != nil {
		return nil, fmt.Errorf("outcome store ref: %w", err)
	}
	if !config.OutcomeRolloutEpoch.IsZero() {
		if err := ValidateOutcomeStoreIdentity(
			config.OutcomeRolloutEpoch,
			config.OutcomeStoreRef,
		); err != nil {
			return nil, err
		}
		if err := ValidateDoltEventTimeZone(config.EventTimeZone); err != nil {
			return nil, err
		}
		if config.DoltServerPID <= 0 {
			return nil, fmt.Errorf("managed Dolt server PID is required")
		}
		if !filepath.IsAbs(config.DoltServerConfigPath) {
			return nil, fmt.Errorf(
				"managed Dolt server config path must be absolute",
			)
		}
		if config.DoltServerGeneration == "" {
			return nil, fmt.Errorf("managed Dolt server generation is required")
		}
	} else {
		if config.EventTimeZone != "" {
			return nil, fmt.Errorf(
				"outcome rollout epoch is required with an event time zone",
			)
		}
		if config.DoltServerPID != 0 {
			return nil, fmt.Errorf(
				"outcome rollout epoch is required with a managed Dolt server PID",
			)
		}
		if config.DoltServerConfigPath != "" {
			return nil, fmt.Errorf(
				"outcome rollout epoch is required with a managed Dolt server config path",
			)
		}
		if config.DoltServerGeneration != "" {
			return nil, fmt.Errorf(
				"outcome rollout epoch is required with a managed Dolt server generation",
			)
		}
	}
	var eventTimeLocation *time.Location
	if config.EventTimeZone != "" {
		eventTimeLocation, _ = time.LoadLocation(config.EventTimeZone)
	}

	driverConfig := mysql.NewConfig()
	driverConfig.User = config.User
	driverConfig.Passwd = config.Password
	driverConfig.Net = "tcp"
	driverConfig.Addr = net.JoinHostPort(config.Host, strconv.Itoa(config.Port))
	driverConfig.DBName = config.Database
	driverConfig.ParseTime = true
	driverConfig.Timeout = 5 * time.Second
	driverConfig.ReadTimeout = 30 * time.Second
	driverConfig.WriteTimeout = 30 * time.Second
	if config.EventTimeZone != "" {
		driverConfig.Params = map[string]string{
			"time_zone": "'" + config.EventTimeZone + "'",
		}
	}

	db, err := sql.Open("mysql", driverConfig.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open canonical Beads database: %w", err)
	}
	maxOpen := config.MaxOpenConns
	if maxOpen == 0 {
		maxOpen = 8
	}
	if maxOpen < 1 {
		_ = db.Close()
		return nil, fmt.Errorf("max open connections must be positive")
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(max(1, maxOpen/2))
	db.SetConnMaxIdleTime(doltConnectionIdleLimit)
	db.SetConnMaxLifetime(doltConnectionLifetime)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping canonical Beads database: %w", err)
	}
	if err := verifyCanonicalDoltSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	var serverIdentity managedDoltServerIdentity
	if eventTimeLocation != nil {
		serverIdentity, err = verifyDoltEventTimeZone(
			ctx,
			db,
			config.EventTimeZone,
			eventTimeLocation,
			config.DoltServerPID,
			config.DoltServerConfigPath,
			config.DoltServerGeneration,
			serverIdentity,
		)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return &DoltBeadStore{
		db: db, clock: config.Clock, actor: config.Actor,
		commitAuthor:         config.CommitAuthor,
		outcomeRolloutEpoch:  config.OutcomeRolloutEpoch.UTC(),
		outcomeStoreRef:      config.OutcomeStoreRef,
		eventTimeLocation:    eventTimeLocation,
		eventTimeZone:        config.EventTimeZone,
		doltServerPID:        config.DoltServerPID,
		doltServerConfigPath: config.DoltServerConfigPath,
		doltServerGeneration: config.DoltServerGeneration,
		doltServerIdentity:   serverIdentity,
	}, nil
}

type doltTimeZoneExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func configureDoltEventTimeZone(
	ctx context.Context,
	executor doltTimeZoneExecutor,
	name string,
) error {
	if _, err := executor.ExecContext(
		ctx,
		"SET GLOBAL time_zone = ?",
		name,
	); err != nil {
		return fmt.Errorf(
			"configure managed Dolt global time zone %q: %w",
			name,
			err,
		)
	}
	return nil
}

func ValidateOutcomeStoreIdentity(
	rolloutEpoch time.Time,
	storeRef string,
) error {
	if rolloutEpoch.IsZero() {
		return fmt.Errorf("outcome rollout epoch is required")
	}
	if err := validateStoreRef(storeRef); err != nil {
		return fmt.Errorf("outcome store ref: %w", err)
	}
	return nil
}

// ValidateDoltEventTimeZone rejects process-relative aliases so the configured
// value remains stable when the worker and managed Dolt services have different
// TZ environments.
func ValidateDoltEventTimeZone(name string) error {
	if name == "" {
		return fmt.Errorf("Dolt event time zone is required")
	}
	if name == "Local" {
		return fmt.Errorf(
			"Dolt event time zone must be an explicit IANA zone, not Local",
		)
	}
	if _, err := time.LoadLocation(name); err != nil {
		return fmt.Errorf("Dolt event time zone %q is not a valid IANA zone: %w", name, err)
	}
	return nil
}

type managedDoltServerIdentity struct {
	PID       int
	StartTime uint64
}

func managedDoltServerIdentityAt(
	procRoot string,
	pid int,
	configPath string,
	expectedUID int,
) (managedDoltServerIdentity, error) {
	if pid <= 0 {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"managed Dolt server PID must be positive",
		)
	}
	if !filepath.IsAbs(configPath) {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"managed Dolt server config path must be absolute",
		)
	}
	pidRoot := filepath.Join(procRoot, strconv.Itoa(pid))
	stat, err := os.ReadFile(filepath.Join(pidRoot, "stat"))
	if err != nil {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"read managed Dolt server stat: %w",
			err,
		)
	}
	startTime, command, err := parseManagedDoltProcessStat(stat)
	if err != nil {
		return managedDoltServerIdentity{}, err
	}
	if command != "dolt" {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"managed Dolt server PID %d command is %q",
			pid,
			command,
		)
	}
	status, err := os.ReadFile(filepath.Join(pidRoot, "status"))
	if err != nil {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"read managed Dolt server status: %w",
			err,
		)
	}
	if err := validateManagedDoltProcessStatus(status, expectedUID); err != nil {
		return managedDoltServerIdentity{}, err
	}
	commandLine, err := os.ReadFile(filepath.Join(pidRoot, "cmdline"))
	if err != nil {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"read managed Dolt server command line: %w",
			err,
		)
	}
	arguments := splitNullTerminated(commandLine)
	if len(arguments) != 4 ||
		filepath.Base(arguments[0]) != "dolt" ||
		arguments[1] != "sql-server" ||
		arguments[2] != "--config" ||
		filepath.Clean(arguments[3]) != filepath.Clean(configPath) {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"managed Dolt server PID %d does not use exact config %q",
			pid,
			configPath,
		)
	}
	return managedDoltServerIdentity{PID: pid, StartTime: startTime}, nil
}

func parseManagedDoltProcessStat(data []byte) (uint64, string, error) {
	value := string(data)
	open := strings.IndexByte(value, '(')
	close := strings.LastIndexByte(value, ')')
	if open < 0 || close <= open {
		return 0, "", fmt.Errorf("managed Dolt server stat is malformed")
	}
	fields := strings.Fields(value[close+1:])
	if len(fields) < 20 {
		return 0, "", fmt.Errorf(
			"managed Dolt server stat has %d suffix fields",
			len(fields),
		)
	}
	if fields[0] == "Z" {
		return 0, "", fmt.Errorf("managed Dolt server process is a zombie")
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTime == 0 {
		return 0, "", fmt.Errorf(
			"managed Dolt server start time is invalid",
		)
	}
	return startTime, value[open+1 : close], nil
}

func validateManagedDoltProcessStatus(data []byte, expectedUID int) error {
	var name string
	var uids []int
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "Name:":
			if len(fields) == 2 {
				name = fields[1]
			}
		case "Uid:":
			for _, raw := range fields[1:] {
				uid, err := strconv.Atoi(raw)
				if err != nil {
					return fmt.Errorf(
						"parse managed Dolt server UID %q: %w",
						raw,
						err,
					)
				}
				uids = append(uids, uid)
			}
		}
	}
	if name != "dolt" || len(uids) != 4 {
		return fmt.Errorf(
			"managed Dolt server status lacks exact Name/Uid identity",
		)
	}
	for _, uid := range uids {
		if uid != expectedUID {
			return fmt.Errorf(
				"managed Dolt server UID %d differs from expected %d",
				uid,
				expectedUID,
			)
		}
	}
	return nil
}

func splitNullTerminated(data []byte) []string {
	parts := strings.Split(string(data), "\x00")
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func verifyDoltEventTimeZone(
	ctx context.Context,
	db *sql.DB,
	name string,
	location *time.Location,
	serverPID int,
	serverConfigPath string,
	serverGeneration string,
	expectedIdentity managedDoltServerIdentity,
) (managedDoltServerIdentity, error) {
	identity, err := managedDoltServerIdentityAt(
		"/proc",
		serverPID,
		serverConfigPath,
		os.Geteuid(),
	)
	if err != nil {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"verify Dolt event time zone %q: %w",
			name,
			err,
		)
	}
	if expectedIdentity != (managedDoltServerIdentity{}) &&
		identity != expectedIdentity {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"verify Dolt event time zone %q: managed server identity changed from %+v to %+v",
			name,
			expectedIdentity,
			identity,
		)
	}
	if err := verifyDoltTimeZoneActivationReceipt(
		serverConfigPath,
		name,
		identity,
		serverGeneration,
	); err != nil {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"verify Dolt event time zone %q activation receipt: %w",
			name,
			err,
		)
	}
	var observation doltEventTimeObservation
	err = db.QueryRowContext(ctx, `
		SELECT CAST(UTC_TIMESTAMP(6) AS CHAR),
		       CAST(NOW(6) AS CHAR),
		       @@session.time_zone,
		       @@global.time_zone,
		       @@system_time_zone`,
	).Scan(
		&observation.UTCWall,
		&observation.ServerWall,
		&observation.SessionZone,
		&observation.GlobalZone,
		&observation.SystemZone,
	)
	if err != nil {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"verify Dolt event time zone %q: %w",
			name,
			err,
		)
	}
	after, err := managedDoltServerIdentityAt(
		"/proc",
		serverPID,
		serverConfigPath,
		os.Geteuid(),
	)
	if err != nil {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"verify Dolt event time zone %q after SQL observation: %w",
			name,
			err,
		)
	}
	if after != identity {
		return managedDoltServerIdentity{}, fmt.Errorf(
			"verify Dolt event time zone %q: managed server identity changed during SQL observation",
			name,
		)
	}
	if err := validateDoltEventTimeZoneObservation(
		observation,
		name,
		location,
	); err != nil {
		return managedDoltServerIdentity{}, err
	}
	return identity, nil
}

type doltEventTimeObservation struct {
	UTCWall     string
	ServerWall  string
	SessionZone string
	GlobalZone  string
	SystemZone  string
}

func validateDoltEventTimeZoneObservation(
	observation doltEventTimeObservation,
	name string,
	location *time.Location,
) error {
	if observation.SessionZone != observation.GlobalZone {
		return fmt.Errorf(
			"verify Dolt event time zone %q: session zone %q differs from global zone %q",
			name,
			observation.SessionZone,
			observation.GlobalZone,
		)
	}
	utcInstant, err := time.ParseInLocation(
		doltDateTimeLayout,
		observation.UTCWall,
		time.UTC,
	)
	if err != nil {
		return fmt.Errorf(
			"verify Dolt event time zone %q: parse UTC wall time %q: %w",
			name,
			observation.UTCWall,
			err,
		)
	}
	expectedWall := utcInstant.In(location).Format(doltDateTimeLayout)
	if observation.ServerWall != expectedWall {
		return fmt.Errorf(
			"verify Dolt event time zone %q: server wall time does not match configured zone (session=%q global=%q system=%q)",
			name,
			observation.SessionZone,
			observation.GlobalZone,
			observation.SystemZone,
		)
	}
	if observation.SessionZone == "SYSTEM" {
		return fmt.Errorf(
			"verify Dolt event time zone %q: server uses SYSTEM/%s, which does not expose an explicit IANA rule-set identity; configure session and global time_zone explicitly",
			name,
			observation.SystemZone,
		)
	}
	if observation.SessionZone != name {
		return fmt.Errorf(
			"verify Dolt event time zone %q: explicit server zone is %q",
			name,
			observation.SessionZone,
		)
	}
	return nil
}

func (s *DoltBeadStore) verifyEventTimeZone(ctx context.Context) error {
	if s.eventTimeLocation == nil {
		return nil
	}
	_, err := verifyDoltEventTimeZone(
		ctx,
		s.db,
		s.eventTimeZone,
		s.eventTimeLocation,
		s.doltServerPID,
		s.doltServerConfigPath,
		s.doltServerGeneration,
		s.doltServerIdentity,
	)
	return err
}

func verifyCanonicalDoltSchema(ctx context.Context, db *sql.DB) error {
	var branch string
	if err := db.QueryRowContext(ctx, "SELECT active_branch()").Scan(&branch); err != nil {
		return fmt.Errorf("verify Dolt endpoint: %w", err)
	}
	if branch == "" {
		return fmt.Errorf("verify Dolt endpoint: active branch is empty")
	}
	probes := []struct {
		table string
		query string
	}{
		{table: "issues", query: "SELECT id, status, metadata FROM issues LIMIT 0"},
		{table: "events", query: "SELECT issue_id, event_type, actor FROM events LIMIT 0"},
	}
	for _, probe := range probes {
		rows, err := db.QueryContext(ctx, probe.query)
		if err != nil {
			return fmt.Errorf(
				"verify canonical Beads %s schema: %w",
				probe.table,
				err,
			)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf(
				"close canonical Beads %s schema probe: %w",
				probe.table,
				err,
			)
		}
	}
	return nil
}

// Close releases only this adapter's connection pool. It never stops Dolt.
func (s *DoltBeadStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// TransitionReady atomically writes the canonical ready state and its outbox
// event to the same Beads issue row.
func (s *DoltBeadStore) TransitionReady(
	ctx context.Context,
	cityID string,
	runID string,
	beadID string,
	generation int64,
	formula FormulaRef,
) (ReadyEvent, error) {
	event, err := NewReadyEvent(
		cityID, runID, beadID, generation, formula, s.clock.Now(),
	)
	if err != nil {
		return ReadyEvent{}, err
	}
	var result ReadyEvent
	err = s.mutateIssue(ctx, beadID, "ready", func(
		tx *sql.Tx,
		issue canonicalIssue,
	) (bool, error) {
		record, err := issue.record()
		if err != nil && !errors.Is(err, ErrBeadNotFound) {
			return false, err
		}
		switch {
		case record.Generation > generation:
			return false, ErrStaleFence
		case record.Generation == generation:
			stored, decodeErr := readyEventFromMetadata(issue.Metadata)
			if decodeErr != nil {
				return false, fmt.Errorf(
					"ready generation has no valid outbox event: %w",
					decodeErr,
				)
			}
			if stored.EventID != event.EventID {
				return false, fmt.Errorf(
					"ready generation belongs to a different orchestration run",
				)
			}
			if stored.Formula != formula {
				return false, fmt.Errorf(
					"ready generation belongs to a different formula snapshot",
				)
			}
			result = stored
			return false, nil
		}

		eventJSON, err := json.Marshal(event)
		if err != nil {
			return false, fmt.Errorf("encode ready event: %w", err)
		}
		metadata := issue.Metadata
		clearGenerationMetadata(metadata)
		metadata[metadataContractVersion] = strconv.Itoa(CurrentContractVersion)
		metadata[metadataCityID] = cityID
		metadata[metadataRunID] = runID
		metadata[metadataGeneration] = strconv.FormatInt(generation, 10)
		metadata[metadataArtifactRefs] = "[]"
		metadata[metadataAttemptFailures] = "[]"
		metadata[metadataReadyEvent] = string(eventJSON)
		metadata[metadataReadyEventID] = event.EventID
		result = event

		encoded, err := encodeMetadata(metadata)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE issues
			   SET status = 'open',
			       assignee = NULL,
			       started_at = NULL,
			       closed_at = NULL,
			       closed_by_session = '',
			       close_reason = '',
			       metadata = ?
			 WHERE id = ?`,
			encoded,
			beadID,
		)
		return true, err
	})
	if err != nil {
		return ReadyEvent{}, err
	}
	return cloneReadyEvent(result), nil
}

// Claim conditionally acquires one exact generation for one Workflow.
func (s *DoltBeadStore) Claim(
	ctx context.Context,
	request ClaimRequest,
) (ClaimLease, error) {
	if err := validateSegment("bead id", request.BeadID); err != nil {
		return ClaimLease{}, err
	}
	if request.Generation <= 0 {
		return ClaimLease{}, fmt.Errorf("generation must be positive")
	}
	if err := validateWorkflowID("workflow id", request.WorkflowID); err != nil {
		return ClaimLease{}, err
	}

	var lease ClaimLease
	err := s.mutateIssue(ctx, request.BeadID, "claim", func(
		tx *sql.Tx,
		issue canonicalIssue,
	) (bool, error) {
		record, err := issue.record()
		if err != nil {
			return false, err
		}
		if record.Generation != request.Generation {
			return false, nil
		}
		if (record.Status == BeadStatusClaimed ||
			record.Status == BeadStatusCompleted) &&
			record.WorkflowID == request.WorkflowID {
			lease = leaseFromRecord(record, true)
			return false, nil
		}
		if record.Status != BeadStatusReady {
			lease = leaseFromRecord(record, false)
			return false, nil
		}

		record.Status = BeadStatusClaimed
		record.WorkflowID = request.WorkflowID
		record.ClaimToken = claimToken(request)
		issue.Metadata[metadataWorkflowID] = record.WorkflowID
		issue.Metadata[metadataClaimToken] = record.ClaimToken
		encoded, err := encodeMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE issues
			   SET status = 'in_progress',
			       assignee = ?,
			       started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
			       metadata = ?
			 WHERE id = ?`,
			request.WorkflowID,
			encoded,
			request.BeadID,
		)
		if err == nil {
			lease = leaseFromRecord(record, true)
		}
		return true, err
	})
	return lease, err
}

// Complete conditionally writes one terminal receipt to canonical Beads.
func (s *DoltBeadStore) Complete(
	ctx context.Context,
	completion Completion,
) error {
	if err := validateCompletion(completion); err != nil {
		return err
	}
	return s.mutateIssue(ctx, completion.BeadID, "complete", func(
		tx *sql.Tx,
		issue canonicalIssue,
	) (bool, error) {
		record, err := issue.record()
		if err != nil {
			return false, err
		}
		if !matchesFence(record, completion.Generation, completion.ClaimToken) {
			return false, ErrStaleFence
		}
		if record.Status == BeadStatusCompleted {
			if record.SessionID == completion.SessionID &&
				record.Outcome == completion.Outcome &&
				artifactsEqual(record.ArtifactRefs, completion.ArtifactRefs) {
				return false, nil
			}
			return false, fmt.Errorf(
				"completion conflicts with existing terminal outcome",
			)
		}
		if record.Status != BeadStatusClaimed {
			return false, ErrStaleFence
		}
		if completion.SourceWorkflowID != record.WorkflowID {
			return false, fmt.Errorf(
				"completion source workflow %s does not match claimed workflow %s",
				completion.SourceWorkflowID,
				record.WorkflowID,
			)
		}

		artifacts, err := json.Marshal(completion.ArtifactRefs)
		if err != nil {
			return false, fmt.Errorf("encode artifact references: %w", err)
		}
		issue.Metadata[metadataSessionID] = completion.SessionID
		issue.Metadata[metadataOutcome] = string(completion.Outcome)
		issue.Metadata[metadataArtifactRefs] = string(artifacts)
		outcome, err := temporalCompletionOutcome(
			record,
			completion,
			s.outcomeStoreRef,
			s.clock.Now().UTC(),
		)
		if err != nil {
			return false, fmt.Errorf("build coordinator outcome: %w", err)
		}
		existing, exists, err := outcomeRecordFromMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		if exists && !outcomesEqual(existing.Envelope, outcome) {
			if err := validateOutcomeSupersession(existing, outcome); err != nil {
				return false, err
			}
		}
		outcomeRecord := OutcomeRecord{
			Envelope:  outcome,
			State:     OutcomeCoordinatorPending,
			PendingAt: s.clock.Now().UTC(),
		}
		if err := setOutcomeMetadata(issue.Metadata, outcomeRecord); err != nil {
			return false, err
		}
		encoded, err := encodeMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE issues
			   SET status = 'closed',
			       closed_at = CURRENT_TIMESTAMP,
			       closed_by_session = ?,
			       close_reason = 'Temporal fenced completion',
			       metadata = ?
			 WHERE id = ?`,
			completion.SessionID,
			encoded,
			completion.BeadID,
		)
		return true, err
	})
}

// RecordAttemptFailure appends one deduplicated fenced Activity failure.
func (s *DoltBeadStore) RecordAttemptFailure(
	ctx context.Context,
	failure AttemptFailure,
) error {
	if err := validateAttemptFailure(failure); err != nil {
		return err
	}
	return s.mutateIssue(ctx, failure.BeadID, "attempt-failure", func(
		tx *sql.Tx,
		issue canonicalIssue,
	) (bool, error) {
		record, err := issue.record()
		if err != nil {
			return false, err
		}
		if record.Status != BeadStatusClaimed ||
			!matchesFence(record, failure.Generation, failure.ClaimToken) {
			return false, ErrStaleFence
		}
		for _, existing := range record.AttemptFailure {
			if existing.Attempt == failure.Attempt &&
				existing.Code == failure.Code {
				return false, nil
			}
		}
		record.AttemptFailure = append(record.AttemptFailure, failure)
		failures, err := json.Marshal(record.AttemptFailure)
		if err != nil {
			return false, fmt.Errorf("encode attempt failures: %w", err)
		}
		issue.Metadata[metadataAttemptFailures] = string(failures)
		encoded, err := encodeMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(
			ctx,
			"UPDATE issues SET metadata = ? WHERE id = ?",
			encoded,
			failure.BeadID,
		)
		return true, err
	})
}

// Inspect returns the current Temporal-owned projection from a canonical issue.
func (s *DoltBeadStore) Inspect(
	ctx context.Context,
	beadID string,
) (BeadRecord, error) {
	issue, err := scanCanonicalIssue(s.db.QueryRowContext(
		ctx,
		"SELECT id, status, metadata FROM issues WHERE id = ?",
		beadID,
	))
	if err != nil {
		return BeadRecord{}, err
	}
	record, err := issue.record()
	if err != nil {
		return BeadRecord{}, err
	}
	return cloneBeadRecord(record), nil
}

// PendingReadyEvents returns the latest unacknowledged generation for each
// enrolled canonical issue in stable ready-time order.
func (s *DoltBeadStore) PendingReadyEvents(
	ctx context.Context,
) ([]ReadyEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT metadata
		  FROM issues
		 WHERE JSON_EXTRACT(metadata, '$."gc.temporal.ready_event"') IS NOT NULL
		   AND COALESCE(
		         JSON_UNQUOTE(JSON_EXTRACT(
		           metadata,
		           '$."gc.temporal.ready_acknowledged_at"'
		         )),
		         ''
		       ) = ''`)
	if err != nil {
		return nil, fmt.Errorf("query pending ready events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var events []ReadyEvent
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan pending ready event: %w", err)
		}
		metadata, err := decodeMetadata(raw)
		if err != nil {
			return nil, err
		}
		event, err := readyEventFromMetadata(metadata)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending ready events: %w", err)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].ReadyAt.Equal(events[j].ReadyAt) {
			return events[i].EventID < events[j].EventID
		}
		return events[i].ReadyAt.Before(events[j].ReadyAt)
	})
	return events, nil
}

// AcknowledgeReadyEvent checkpoints successful Signal-With-Start delivery.
func (s *DoltBeadStore) AcknowledgeReadyEvent(
	ctx context.Context,
	eventID string,
) error {
	if err := validateSegment("event id", eventID); err != nil {
		return err
	}
	beadID, err := s.findEventBeadID(ctx, eventID)
	if err != nil {
		return err
	}
	return s.mutateIssue(ctx, beadID, "ack-ready", func(
		tx *sql.Tx,
		issue canonicalIssue,
	) (bool, error) {
		if metadataString(issue.Metadata, metadataReadyEventID) != eventID {
			return false, fmt.Errorf("outbox event %s: %w", eventID, ErrBeadNotFound)
		}
		if metadataString(issue.Metadata, metadataReadyAckAt) != "" {
			return false, nil
		}
		issue.Metadata[metadataReadyAckAt] = s.clock.Now().UTC().Format(
			time.RFC3339Nano,
		)
		encoded, err := encodeMetadata(issue.Metadata)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(
			ctx,
			"UPDATE issues SET metadata = ? WHERE id = ?",
			encoded,
			beadID,
		)
		return true, err
	})
}

type canonicalIssue struct {
	ID       string
	Status   string
	Metadata map[string]any
}

type rowScanner interface {
	Scan(...any) error
}

func scanCanonicalIssue(row rowScanner) (canonicalIssue, error) {
	var issue canonicalIssue
	var raw []byte
	if err := row.Scan(&issue.ID, &issue.Status, &raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return canonicalIssue{}, ErrBeadNotFound
		}
		return canonicalIssue{}, fmt.Errorf("read canonical Beads issue: %w", err)
	}
	metadata, err := decodeMetadata(raw)
	if err != nil {
		return canonicalIssue{}, err
	}
	issue.Metadata = metadata
	return issue, nil
}

func (i canonicalIssue) record() (BeadRecord, error) {
	generation, err := metadataInt64(i.Metadata, metadataGeneration)
	if err != nil {
		return BeadRecord{}, err
	}
	if generation == 0 {
		return BeadRecord{}, ErrBeadNotFound
	}
	version, err := metadataInt64(i.Metadata, metadataContractVersion)
	if err != nil {
		return BeadRecord{}, err
	}
	if version != CurrentContractVersion {
		return BeadRecord{}, fmt.Errorf(
			"unsupported canonical Beads contract version %d",
			version,
		)
	}
	status, err := canonicalBeadStatus(i.Status)
	if err != nil {
		return BeadRecord{}, err
	}
	record := BeadRecord{
		CityID:       metadataString(i.Metadata, metadataCityID),
		RunID:        metadataString(i.Metadata, metadataRunID),
		BeadID:       i.ID,
		Generation:   generation,
		Status:       status,
		WorkflowID:   metadataString(i.Metadata, metadataWorkflowID),
		ClaimToken:   metadataString(i.Metadata, metadataClaimToken),
		SessionID:    metadataString(i.Metadata, metadataSessionID),
		Outcome:      Outcome(metadataString(i.Metadata, metadataOutcome)),
		ArtifactRefs: nil,
	}
	if event, eventErr := readyEventFromMetadata(i.Metadata); eventErr == nil {
		record.Formula = event.Formula
	} else {
		return BeadRecord{}, fmt.Errorf("read formula snapshot: %w", eventErr)
	}
	if err := decodeMetadataJSON(
		i.Metadata,
		metadataArtifactRefs,
		&record.ArtifactRefs,
	); err != nil {
		return BeadRecord{}, err
	}
	if err := decodeMetadataJSON(
		i.Metadata,
		metadataAttemptFailures,
		&record.AttemptFailure,
	); err != nil {
		return BeadRecord{}, err
	}
	return record, nil
}

func (s *DoltBeadStore) mutateIssue(
	ctx context.Context,
	beadID string,
	operation string,
	mutate func(*sql.Tx, canonicalIssue) (bool, error),
) error {
	var lastErr error
	for attempt := 0; attempt < doltMutationAttempts; attempt++ {
		err := s.mutateIssueOnce(ctx, beadID, operation, mutate)
		if err == nil || !isSerializationFailure(err) {
			return err
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 5 * time.Millisecond):
		}
	}
	return fmt.Errorf(
		"canonical Beads transaction conflicted after %d attempts: %w",
		doltMutationAttempts,
		lastErr,
	)
}

func (s *DoltBeadStore) mutateIssueOnce(
	ctx context.Context,
	beadID string,
	operation string,
	mutate func(*sql.Tx, canonicalIssue) (bool, error),
) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire canonical Beads connection: %w", err)
	}
	defer func() { _ = conn.Close() }()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin canonical Beads transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	issue, err := scanCanonicalIssue(tx.QueryRowContext(
		ctx,
		"SELECT id, status, metadata FROM issues WHERE id = ? FOR UPDATE",
		beadID,
	))
	if err != nil {
		return err
	}
	beforeMetadata, err := encodeMetadata(issue.Metadata)
	if err != nil {
		return fmt.Errorf("snapshot canonical issue before %s: %w", operation, err)
	}
	before := issue
	before.Metadata, err = decodeMetadata(beforeMetadata)
	if err != nil {
		return fmt.Errorf("restore canonical issue snapshot before %s: %w", operation, err)
	}
	changed, err := mutate(tx, issue)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if err := s.recordBeadsEvent(ctx, tx, before, operation); err != nil {
		return err
	}
	for _, table := range []string{"issues", "events"} {
		if _, err := tx.ExecContext(ctx, "CALL DOLT_ADD(?)", table); err != nil {
			return fmt.Errorf("stage canonical Beads table %s: %w", table, err)
		}
	}
	message := fmt.Sprintf("temporal-beads: %s %s", operation, beadID)
	if _, err := tx.ExecContext(
		ctx,
		"CALL DOLT_COMMIT('-m', ?, '--author', ?)",
		message,
		s.commitAuthor,
	); err != nil && !isNothingToCommit(err) {
		return fmt.Errorf("commit canonical Beads issue: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finish canonical Beads transaction: %w", err)
	}
	return nil
}

func (s *DoltBeadStore) recordBeadsEvent(
	ctx context.Context,
	tx *sql.Tx,
	before canonicalIssue,
	operation string,
) error {
	if s.recordEventFault != nil {
		if err := s.recordEventFault(operation); err != nil {
			return fmt.Errorf("record canonical Beads event: %w", err)
		}
	}
	after, err := scanCanonicalIssue(tx.QueryRowContext(
		ctx,
		"SELECT id, status, metadata FROM issues WHERE id = ?",
		before.ID,
	))
	if err != nil {
		return fmt.Errorf("read canonical issue after %s: %w", operation, err)
	}
	beforeJSON, err := json.Marshal(map[string]any{
		"id": before.ID, "status": before.Status, "metadata": before.Metadata,
	})
	if err != nil {
		return fmt.Errorf("encode Beads event old value: %w", err)
	}
	afterJSON, err := json.Marshal(map[string]any{
		"id": after.ID, "status": after.Status, "metadata": after.Metadata,
	})
	if err != nil {
		return fmt.Errorf("encode Beads event new value: %w", err)
	}
	eventType := "updated"
	switch operation {
	case "ready":
		eventType = "status_changed"
		if before.Status == "closed" {
			eventType = "reopened"
		}
	case "claim":
		eventType = "status_changed"
	case "complete":
		eventType = "closed"
	case "outcome-finalized":
		eventType = "closed"
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO events
			(issue_id, event_type, actor, old_value, new_value)
		VALUES (?, ?, ?, ?, ?)`,
		before.ID,
		eventType,
		s.actor,
		string(beforeJSON),
		string(afterJSON),
	)
	if err != nil {
		return fmt.Errorf("record canonical Beads event: %w", err)
	}
	return nil
}

func (s *DoltBeadStore) findEventBeadID(
	ctx context.Context,
	eventID string,
) (string, error) {
	var beadID string
	err := s.db.QueryRowContext(ctx, `
		SELECT id
		  FROM issues
		 WHERE JSON_UNQUOTE(JSON_EXTRACT(
		         metadata,
		         '$."gc.temporal.ready_event_id"'
		       )) = ?
		 LIMIT 1`,
		eventID,
	).Scan(&beadID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("outbox event %s: %w", eventID, ErrBeadNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("find ready event %s: %w", eventID, err)
	}
	return beadID, nil
}

func validateCompletion(completion Completion) error {
	if err := validateSegment("bead id", completion.BeadID); err != nil {
		return err
	}
	if completion.Generation <= 0 {
		return fmt.Errorf("generation must be positive")
	}
	if err := validateSegment("claim token", completion.ClaimToken); err != nil {
		return err
	}
	if err := validateSegment("session id", completion.SessionID); err != nil {
		return err
	}
	if err := validateWorkflowID(
		"completion source workflow id",
		completion.SourceWorkflowID,
	); err != nil {
		return err
	}
	if err := validateSegment(
		"completion source workflow run id",
		completion.SourceWorkflowRunID,
	); err != nil {
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
	return nil
}

func validateAttemptFailure(failure AttemptFailure) error {
	if err := validateSegment("bead id", failure.BeadID); err != nil {
		return err
	}
	if failure.Generation <= 0 {
		return fmt.Errorf("generation must be positive")
	}
	if err := validateSegment("claim token", failure.ClaimToken); err != nil {
		return err
	}
	if failure.Attempt <= 0 {
		return fmt.Errorf("attempt must be positive")
	}
	return validateSegment("attempt failure code", failure.Code)
}

func validateDatabaseName(database string) error {
	if database == "" {
		return fmt.Errorf("dolt database is required")
	}
	for _, character := range database {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '_' ||
			character == '-' {
			continue
		}
		return fmt.Errorf("dolt database %q contains an unsafe character", database)
	}
	return nil
}

func validateLoopbackHost(host string) error {
	if host == "localhost" {
		return nil
	}
	address := net.ParseIP(host)
	if address == nil || !address.IsLoopback() {
		return fmt.Errorf(
			"dolt host %q must be a loopback address for the no-TLS managed endpoint",
			host,
		)
	}
	return nil
}

func canonicalBeadStatus(status string) (BeadStatus, error) {
	switch status {
	case "open":
		return BeadStatusReady, nil
	case "in_progress":
		return BeadStatusClaimed, nil
	case "closed":
		return BeadStatusCompleted, nil
	default:
		return "", fmt.Errorf("canonical Beads status %q is not orchestratable", status)
	}
}

func clearGenerationMetadata(metadata map[string]any) {
	for _, key := range []string{
		metadataWorkflowID,
		metadataClaimToken,
		metadataSessionID,
		metadataOutcome,
		metadataArtifactRefs,
		metadataAttemptFailures,
		metadataReadyEvent,
		metadataReadyEventID,
		metadataReadyAckAt,
	} {
		delete(metadata, key)
	}
}

func decodeMetadata(raw []byte) (map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return make(map[string]any), nil
	}
	var metadata map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&metadata); err != nil {
		return nil, fmt.Errorf("decode canonical Beads metadata: %w", err)
	}
	if metadata == nil {
		metadata = make(map[string]any)
	}
	return metadata, nil
}

func encodeMetadata(metadata map[string]any) ([]byte, error) {
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode canonical Beads metadata: %w", err)
	}
	return encoded, nil
}

func metadataString(metadata map[string]any, key string) string {
	value, exists := metadata[key]
	if !exists || value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func metadataInt64(metadata map[string]any, key string) (int64, error) {
	value := metadataString(metadata, key)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("metadata %s is not an integer: %w", key, err)
	}
	return parsed, nil
}

func decodeMetadataJSON(metadata map[string]any, key string, target any) error {
	value := metadataString(metadata, key)
	if value == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(value), target); err != nil {
		return fmt.Errorf("decode metadata %s: %w", key, err)
	}
	return nil
}

func readyEventFromMetadata(metadata map[string]any) (ReadyEvent, error) {
	var event ReadyEvent
	if err := decodeMetadataJSON(metadata, metadataReadyEvent, &event); err != nil {
		return ReadyEvent{}, err
	}
	if err := event.Validate(); err != nil {
		return ReadyEvent{}, err
	}
	return event, nil
}

func isNothingToCommit(err error) bool {
	return err != nil && strings.Contains(
		strings.ToLower(err.Error()),
		"nothing to commit",
	)
}

func isSerializationFailure(err error) bool {
	var mysqlError *mysql.MySQLError
	if errors.As(err, &mysqlError) {
		return mysqlError.Number == 1205 || mysqlError.Number == 1213
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "serialization failure") ||
		strings.Contains(text, "deadlock found")
}
