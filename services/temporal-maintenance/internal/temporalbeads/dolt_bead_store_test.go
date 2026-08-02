package temporalbeads

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestDoltBeadStorePersistsReadyTransitionAndOutboxAtomically(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()

	event, err := fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 1, validFormulaRef(),
	)
	require.NoError(t, err)

	reopened, err := OpenDoltBeadStore(ctx, fixture.config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	record, err := reopened.Inspect(ctx, fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusReady, record.Status)
	require.Equal(t, int64(1), record.Generation)

	pending, err := reopened.PendingReadyEvents(ctx)
	require.NoError(t, err)
	require.Equal(t, []ReadyEvent{event}, pending)

	var status string
	var metadataJSON []byte
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT status, metadata FROM issues WHERE id = ?",
		fixture.beadID,
	).Scan(&status, &metadataJSON))
	require.Equal(t, "open", status)
	var metadata map[string]any
	require.NoError(t, json.Unmarshal(metadataJSON, &metadata))
	require.Equal(t, "keep", metadata["unrelated"])
	require.Equal(t, "1", metadata[metadataGeneration])
	require.NotEmpty(t, metadata[metadataReadyEvent])

	var eventCount int
	require.NoError(t, fixture.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = 'status_changed'",
		fixture.beadID,
	).Scan(&eventCount))
	require.Equal(t, 1, eventCount)
}

func TestDoltBeadStoreClaimsWithGeneratedWorkflowID(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	_, err := fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 1, validFormulaRef(),
	)
	require.NoError(t, err)

	workflowID, err := WorkflowID("city", "run", fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, "bead-orchestration/city/run/"+fixture.beadID, workflowID)

	lease, err := fixture.store.Claim(ctx, ClaimRequest{
		BeadID:     fixture.beadID,
		Generation: 1,
		WorkflowID: workflowID,
	})
	require.NoError(t, err)
	require.True(t, lease.Acquired)
	require.NotEmpty(t, lease.Token)

	record, err := fixture.store.Inspect(ctx, fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusClaimed, record.Status)
	require.Equal(t, workflowID, record.WorkflowID)
	require.Equal(t, lease.Token, record.ClaimToken)
}

func TestDoltBeadStoreDuplicateAndSupersededGenerations(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()

	first, err := fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 1, validFormulaRef(),
	)
	require.NoError(t, err)
	duplicate, err := fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 1, validFormulaRef(),
	)
	require.NoError(t, err)
	require.Equal(t, first, duplicate)

	fixture.clock.Advance(time.Minute)
	second, err := fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 2, validFormulaRef(),
	)
	require.NoError(t, err)
	pending, err := fixture.store.PendingReadyEvents(ctx)
	require.NoError(t, err)
	require.Equal(t, []ReadyEvent{second}, pending)

	require.NoError(t, fixture.store.AcknowledgeReadyEvent(ctx, second.EventID))
	require.NoError(t, fixture.store.AcknowledgeReadyEvent(ctx, second.EventID))
	pending, err = fixture.store.PendingReadyEvents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending)

	err = fixture.store.AcknowledgeReadyEvent(ctx, first.EventID)
	require.ErrorIs(t, err, ErrBeadNotFound)
}

func TestDoltBeadStoreConcurrentClaimAndTerminalWritesAreFenced(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	_, err := fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 1, validFormulaRef(),
	)
	require.NoError(t, err)

	const claimers = 8
	var wait sync.WaitGroup
	wait.Add(claimers)
	leases := make(chan ClaimLease, claimers)
	errs := make(chan error, claimers)
	for index := 0; index < claimers; index++ {
		go func(index int) {
			defer wait.Done()
			store, openErr := OpenDoltBeadStore(ctx, fixture.config)
			if openErr != nil {
				errs <- openErr
				return
			}
			defer func() { _ = store.Close() }()
			lease, claimErr := store.Claim(ctx, ClaimRequest{
				BeadID:     fixture.beadID,
				Generation: 1,
				WorkflowID: fmt.Sprintf("workflow-%d", index),
			})
			if claimErr != nil {
				errs <- claimErr
				return
			}
			leases <- lease
		}(index)
	}
	wait.Wait()
	close(leases)
	close(errs)
	for claimErr := range errs {
		require.NoError(t, claimErr)
	}

	var acquired []ClaimLease
	for lease := range leases {
		if lease.Acquired {
			acquired = append(acquired, lease)
		}
	}
	require.Len(t, acquired, 1)
	oldLease := acquired[0]
	require.NotEmpty(t, oldLease.Token)

	record, err := fixture.store.Inspect(ctx, fixture.beadID)
	require.NoError(t, err)
	retry, err := fixture.store.Claim(ctx, ClaimRequest{
		BeadID:     fixture.beadID,
		Generation: 1,
		WorkflowID: record.WorkflowID,
	})
	require.NoError(t, err)
	require.Equal(t, oldLease, retry)

	fixture.clock.Advance(time.Minute)
	_, err = fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 2, validFormulaRef(),
	)
	require.NoError(t, err)
	currentLease, err := fixture.store.Claim(ctx, ClaimRequest{
		BeadID: fixture.beadID, Generation: 2, WorkflowID: "workflow-current",
	})
	require.NoError(t, err)
	require.True(t, currentLease.Acquired)

	err = fixture.store.Complete(ctx, Completion{
		BeadID: fixture.beadID, Generation: 1, ClaimToken: oldLease.Token,
		SessionID: "stale-session", Outcome: OutcomeCompleted,
		SourceWorkflowID: record.WorkflowID, SourceWorkflowRunID: "stale-run",
	})
	require.ErrorIs(t, err, ErrStaleFence)

	failure := AttemptFailure{
		BeadID: fixture.beadID, Generation: 2, ClaimToken: currentLease.Token,
		Attempt: 1, Code: "agent-execution-failed",
	}
	require.NoError(t, fixture.store.RecordAttemptFailure(ctx, failure))
	require.NoError(t, fixture.store.RecordAttemptFailure(ctx, failure))

	completion := Completion{
		BeadID: fixture.beadID, Generation: 2, ClaimToken: currentLease.Token,
		SessionID: "current-session", Outcome: OutcomeCompleted,
		SourceWorkflowID: "workflow-current", SourceWorkflowRunID: "current-run",
		ArtifactRefs: []ArtifactRef{testArtifact()},
	}
	require.NoError(t, fixture.store.Complete(ctx, completion))
	require.NoError(t, fixture.store.Complete(ctx, completion))

	final, err := fixture.store.Inspect(ctx, fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusCompleted, final.Status)
	require.Equal(t, "current-session", final.SessionID)
	require.Equal(t, []AttemptFailure{failure}, final.AttemptFailure)
	require.Equal(t, completion.ArtifactRefs, final.ArtifactRefs)
}

func TestDoltBeadStoreFailureLeavesCanonicalIssueInspectable(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	ctx := context.Background()
	_, err := fixture.store.TransitionReady(
		ctx, "city", "run", fixture.beadID, 1, validFormulaRef(),
	)
	require.NoError(t, err)

	err = fixture.store.Complete(ctx, Completion{
		BeadID: fixture.beadID, Generation: 1, ClaimToken: "wrong-token",
		SessionID: "session", Outcome: OutcomeCompleted,
		SourceWorkflowID: "workflow-missing", SourceWorkflowRunID: "missing-run",
	})
	require.ErrorIs(t, err, ErrStaleFence)

	record, err := fixture.store.Inspect(ctx, fixture.beadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusReady, record.Status)
	require.Empty(t, record.ClaimToken)

	pending, err := fixture.store.PendingReadyEvents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	_, err = fixture.store.TransitionReady(
		ctx, "city", "run", "missing-bead", 1, validFormulaRef(),
	)
	require.ErrorIs(t, err, ErrBeadNotFound)
}

func TestOpenDoltBeadStoreRejectsNonLoopbackEndpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store, err := OpenDoltBeadStore(ctx, DoltBeadStoreConfig{
		Host:         "192.0.2.1",
		Port:         3306,
		Database:     "beads",
		User:         "root",
		Password:     "must-not-appear",
		Actor:        "temporal-orchestrator",
		CommitAuthor: "Temporal Orchestrator <temporal@localhost>",
		Clock:        NewManualClock(time.Unix(1, 0)),
	})
	require.Nil(t, store)
	require.ErrorContains(t, err, "loopback")
	require.NotContains(t, err.Error(), "must-not-appear")
}

func TestOpenDoltBeadStoreRequiresOutcomeStoreRef(t *testing.T) {
	store, err := OpenDoltBeadStore(context.Background(), DoltBeadStoreConfig{
		Host:         "127.0.0.1",
		Port:         3306,
		Database:     "beads",
		User:         "root",
		Actor:        "temporal-orchestrator",
		CommitAuthor: "Temporal Orchestrator <temporal@localhost>",
		Clock:        NewManualClock(time.Unix(1, 0)),
	})
	require.Nil(t, store)
	require.ErrorContains(t, err, "outcome store ref")
}

func TestDoltEventTimeZoneRequiresExplicitIANANameAndRollout(t *testing.T) {
	for _, name := range []string{"", "Local", "Not/A_Zone"} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, ValidateDoltEventTimeZone(name))
		})
	}
	require.NoError(t, ValidateDoltEventTimeZone("America/New_York"))

	store, err := OpenDoltBeadStore(context.Background(), DoltBeadStoreConfig{
		Host:            "127.0.0.1",
		Port:            3306,
		Database:        "beads",
		User:            "root",
		Actor:           "temporal-orchestrator",
		CommitAuthor:    "Temporal Orchestrator <temporal@localhost>",
		Clock:           NewManualClock(time.Unix(1, 0)),
		OutcomeStoreRef: "city:test",
		EventTimeZone:   "America/New_York",
	})
	require.Nil(t, store)
	require.ErrorContains(t, err, "rollout epoch")

	store, err = OpenDoltBeadStore(context.Background(), DoltBeadStoreConfig{
		Host:                 "127.0.0.1",
		Port:                 3306,
		Database:             "beads",
		User:                 "root",
		Actor:                "temporal-orchestrator",
		CommitAuthor:         "Temporal Orchestrator <temporal@localhost>",
		Clock:                NewManualClock(time.Unix(1, 0)),
		OutcomeStoreRef:      "city:test",
		DoltServerPID:        1234,
		DoltServerConfigPath: "/city/dolt-config.yaml",
	})
	require.Nil(t, store)
	require.ErrorContains(t, err, "rollout epoch")

	config := DoltBeadStoreConfig{
		Host:                "127.0.0.1",
		Port:                3306,
		Database:            "beads",
		User:                "root",
		Actor:               "temporal-orchestrator",
		CommitAuthor:        "Temporal Orchestrator <temporal@localhost>",
		Clock:               NewManualClock(time.Unix(1, 0)),
		OutcomeRolloutEpoch: time.Unix(1, 0).UTC(),
		OutcomeStoreRef:     "city:test",
		EventTimeZone:       "America/New_York",
	}
	store, err = OpenDoltBeadStore(context.Background(), config)
	require.Nil(t, store)
	require.ErrorContains(t, err, "server PID")
	config.DoltServerPID = 1234
	store, err = OpenDoltBeadStore(context.Background(), config)
	require.Nil(t, store)
	require.ErrorContains(t, err, "config path")
	config.DoltServerConfigPath = "/city/dolt-config.yaml"
	store, err = OpenDoltBeadStore(context.Background(), config)
	require.Nil(t, store)
	require.ErrorContains(t, err, "generation")
}

func TestValidateDoltEventTimeZoneObservationAcceptsSecondFallbackHour(
	t *testing.T,
) {
	location, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	err = validateDoltEventTimeZoneObservation(
		doltEventTimeObservation{
			UTCWall:     "2026-11-01 06:30:00.123456",
			ServerWall:  "2026-11-01 01:30:00.123456",
			SessionZone: "America/New_York",
			GlobalZone:  "America/New_York",
			SystemZone:  "EST",
		},
		"America/New_York",
		location,
	)
	require.NoError(t, err)
}

func TestValidateDoltEventTimeZoneObservationRejectsCoincidentCurrentRuleSets(
	t *testing.T,
) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	indianapolis, err := time.LoadLocation("America/Indiana/Indianapolis")
	require.NoError(t, err)

	current := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	newYorkName, newYorkOffset := current.In(newYork).Zone()
	indianapolisName, indianapolisOffset := current.In(indianapolis).Zone()
	require.Equal(t, newYorkName, indianapolisName)
	require.Equal(t, newYorkOffset, indianapolisOffset)

	standardSentinel := time.Date(2005, 1, 15, 12, 0, 0, 0, time.UTC)
	_, newYorkStandardOffset := standardSentinel.In(newYork).Zone()
	_, indianapolisStandardOffset := standardSentinel.In(indianapolis).Zone()
	require.Equal(t, newYorkStandardOffset, indianapolisStandardOffset)

	daylightSentinel := time.Date(2005, 7, 15, 12, 0, 0, 0, time.UTC)
	_, newYorkDaylightOffset := daylightSentinel.In(newYork).Zone()
	_, indianapolisDaylightOffset := daylightSentinel.In(indianapolis).Zone()
	require.NotEqual(t, newYorkDaylightOffset, indianapolisDaylightOffset)

	err = validateDoltEventTimeZoneObservation(
		doltEventTimeObservation{
			UTCWall:     "2026-07-31 12:00:00.123456",
			ServerWall:  "2026-07-31 08:00:00.123456",
			SessionZone: "SYSTEM",
			GlobalZone:  "SYSTEM",
			SystemZone:  "EDT",
		},
		"America/New_York",
		newYork,
	)
	require.ErrorContains(t, err, "explicit IANA")
}

func TestValidateDoltEventTimeZoneObservationRejectsInvalidServerState(
	t *testing.T,
) {
	location, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	valid := doltEventTimeObservation{
		UTCWall:     "2026-07-31 07:48:00.123456",
		ServerWall:  "2026-07-31 03:48:00.123456",
		SessionZone: "America/New_York",
		GlobalZone:  "America/New_York",
		SystemZone:  "EDT",
	}

	mismatchedSession := valid
	mismatchedSession.SessionZone = "+00:00"
	require.ErrorContains(
		t,
		validateDoltEventTimeZoneObservation(
			mismatchedSession,
			"America/New_York",
			location,
		),
		"differs",
	)
	invalidUTC := valid
	invalidUTC.UTCWall = "not-a-time"
	require.ErrorContains(
		t,
		validateDoltEventTimeZoneObservation(
			invalidUTC,
			"America/New_York",
			location,
		),
		"parse UTC",
	)
	mismatchedWall := valid
	mismatchedWall.ServerWall = "2026-07-31 07:48:00.123456"
	require.ErrorContains(
		t,
		validateDoltEventTimeZoneObservation(
			mismatchedWall,
			"America/New_York",
			location,
		),
		"does not match",
	)
	mismatchedIdentity := valid
	mismatchedIdentity.SessionZone = "America/Indiana/Indianapolis"
	mismatchedIdentity.GlobalZone = "America/Indiana/Indianapolis"
	require.ErrorContains(
		t,
		validateDoltEventTimeZoneObservation(
			mismatchedIdentity,
			"America/New_York",
			location,
		),
		"explicit server zone",
	)
	explicitEmptyTZ := valid
	explicitEmptyTZ.ServerWall = explicitEmptyTZ.UTCWall
	explicitEmptyTZ.SystemZone = "UTC"
	require.ErrorContains(
		t,
		validateDoltEventTimeZoneObservation(
			explicitEmptyTZ,
			"America/New_York",
			location,
		),
		"does not match",
	)
}

func TestManagedDoltProcessIdentityUsesServiceReadableProcContract(t *testing.T) {
	procRoot := t.TempDir()
	pid := 4242
	pidRoot := filepath.Join(procRoot, strconv.Itoa(pid))
	require.NoError(t, os.Mkdir(pidRoot, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(pidRoot, "stat"),
		[]byte("4242 (dolt) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 424242 20\n"),
		0o600,
	))
	uid := os.Geteuid()
	require.NoError(t, os.WriteFile(
		filepath.Join(pidRoot, "status"),
		[]byte(fmt.Sprintf(
			"Name:\tdolt\nUid:\t%d\t%d\t%d\t%d\n",
			uid,
			uid,
			uid,
			uid,
		)),
		0o600,
	))
	configPath := "/srv/city/.gc/runtime/packs/dolt/dolt-config.yaml"
	require.NoError(t, os.WriteFile(
		filepath.Join(pidRoot, "cmdline"),
		[]byte("dolt\x00sql-server\x00--config\x00"+configPath+"\x00"),
		0o600,
	))
	// The real worker service cannot read a sibling process environment.
	// Make that surface unusable so this regression cannot pass by accident.
	require.NoError(t, os.Mkdir(filepath.Join(pidRoot, "environ"), 0o000))

	identity, err := managedDoltServerIdentityAt(
		procRoot,
		pid,
		configPath,
		uid,
	)
	require.NoError(t, err)
	require.Equal(t, pid, identity.PID)
	require.Equal(t, uint64(424242), identity.StartTime)

	_, err = managedDoltServerIdentityAt(
		procRoot,
		pid,
		"/other/dolt-config.yaml",
		uid,
	)
	require.ErrorContains(t, err, "exact config")
	_, err = managedDoltServerIdentityAt(
		procRoot,
		pid,
		configPath,
		uid+1,
	)
	require.ErrorContains(t, err, "differs from expected")
}

func TestActivateDoltEventTimeZoneDrainsWriterAndGatesStoreAdmission(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStoreWithServerTimezoneMode(
		t,
		"America/New_York",
		false,
		true,
	)
	require.Equal(t, "SYSTEM", fixture.legacyWriterTimeZone)
	require.NotNil(t, fixture.legacyWriter)
	var staleWriterZone string
	err := fixture.legacyWriter.QueryRowContext(
		context.Background(),
		"SELECT @@session.time_zone",
	).Scan(&staleWriterZone)
	require.Error(t, err, "the pre-activation writer must be drained")

	assertExactTimeZone := func() {
		t.Helper()
		var globalZone string
		require.NoError(t, fixture.db.QueryRow(
			"SELECT @@global.time_zone",
		).Scan(&globalZone))
		require.Equal(t, "America/New_York", globalZone)

		var januaryWall, julyWall, writerSessionZone string
		require.NoError(t, fixture.db.QueryRow(`
			SELECT CAST(
				CONVERT_TZ(
					'2026-01-31 06:57:13',
					'+00:00',
					@@global.time_zone
				) AS CHAR
			)`,
		).Scan(&januaryWall))
		require.Equal(t, "2026-01-31 01:57:13", januaryWall)
		require.NoError(t, fixture.db.QueryRow(`
			SELECT CAST(
				CONVERT_TZ(
					'2026-07-31 06:57:13',
					'+00:00',
					@@global.time_zone
				) AS CHAR
			)`,
		).Scan(&julyWall))
		require.Equal(t, "2026-07-31 02:57:13", julyWall)
		require.NoError(t, fixture.db.QueryRow(
			"SELECT @@session.time_zone",
		).Scan(&writerSessionZone))
		require.Equal(t, "America/New_York", writerSessionZone)
	}
	assertExactTimeZone()
	_, err = fixture.store.DiscoverLegacyOutcomes(context.Background())
	require.NoError(t, err, "scan admission requires the drained exact generation")

	_, err = fixture.db.Exec("SET GLOBAL time_zone = 'SYSTEM'")
	require.NoError(t, err)
	_, err = fixture.store.DiscoverLegacyOutcomes(context.Background())
	require.ErrorContains(t, err, "session zone")
	require.ErrorContains(t, err, "global zone")

	require.NoError(t, fixture.store.Close())
	_, err = OpenDoltBeadStore(
		context.Background(),
		fixture.config,
	)
	require.ErrorContains(t, err, "session zone")
	_, err = ActivateDoltEventTimeZone(
		context.Background(),
		activationConfigForStore(fixture.config),
	)
	require.NoError(t, err)
	restarted, err := OpenDoltBeadStore(
		context.Background(),
		fixture.config,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	require.NoError(t, fixture.db.Close())
	reopenedConfig := mysql.Config{
		User:                 fixture.config.User,
		Net:                  "tcp",
		AllowNativePasswords: true,
		Addr: net.JoinHostPort(
			fixture.config.Host,
			strconv.Itoa(fixture.config.Port),
		),
		DBName:    fixture.config.Database,
		ParseTime: true,
	}
	fixture.db, err = sql.Open(
		"mysql",
		reopenedConfig.FormatDSN(),
	)
	require.NoError(t, err)
	assertExactTimeZone()
}

func TestActivateDoltEventTimeZoneDrainsHandshakeVisibleAfterPostSetFence(t *testing.T) {
	type handshakeResult struct {
		connection *sql.Conn
		id         uint64
		zone       string
		err        error
	}
	var raced handshakeResult
	fixture := newIsolatedDoltBeadStoreWithActivationHook(
		t,
		"America/New_York",
		false,
		func(config *DoltTimeZoneActivationConfig) {
			network := fmt.Sprintf("pre-set-handshake-%d", time.Now().UnixNano())
			dialed := make(chan struct{})
			releaseHandshake := make(chan struct{})
			var dialedOnce sync.Once
			mysql.RegisterDialContext(
				network,
				func(ctx context.Context, address string) (net.Conn, error) {
					connection, err := (&net.Dialer{}).DialContext(
						ctx,
						"tcp",
						address,
					)
					if err != nil {
						return nil, err
					}
					dialedOnce.Do(func() { close(dialed) })
					select {
					case <-releaseHandshake:
						return connection, nil
					case <-ctx.Done():
						_ = connection.Close()
						return nil, ctx.Err()
					}
				},
			)
			writerConfig := mysql.NewConfig()
			writerConfig.User = "root"
			writerConfig.Net = network
			writerConfig.Addr = net.JoinHostPort(
				config.Host,
				strconv.Itoa(config.Port),
			)
			writerConfig.DBName = "beads"
			writerConfig.Timeout = 2 * time.Second
			writerDB, err := sql.Open("mysql", writerConfig.FormatDSN())
			require.NoError(t, err)
			t.Cleanup(func() { _ = writerDB.Close() })
			result := make(chan handshakeResult, 1)
			go func() {
				connection, err := writerDB.Conn(context.Background())
				if err != nil {
					result <- handshakeResult{err: err}
					return
				}
				var id uint64
				var zone string
				err = connection.QueryRowContext(
					context.Background(),
					"SELECT CONNECTION_ID(), @@session.time_zone",
				).Scan(&id, &zone)
				result <- handshakeResult{
					connection: connection,
					id:         id,
					zone:       zone,
					err:        err,
				}
			}()
			select {
			case <-dialed:
			case <-time.After(2 * time.Second):
				t.Fatal("pre-SET writer did not reach the handshake pause")
			}
			config.afterPostSetFence = func() {
				close(releaseHandshake)
				select {
				case raced = <-result:
				case <-time.After(2 * time.Second):
					t.Fatal("pre-SET handshake did not become visible after the post-SET fence")
				}
			}
			config.handshakeDrainWindow = 0
		},
	)
	require.NoError(t, raced.err)
	require.NotNil(t, raced.connection)
	t.Cleanup(func() { _ = raced.connection.Close() })
	require.NotZero(t, raced.id)
	require.Equal(t, "America/New_York", raced.zone)
	var survived int
	err := raced.connection.QueryRowContext(
		context.Background(),
		"SELECT 1",
	).Scan(&survived)
	require.Error(
		t,
		err,
		"the repeated drain must kill a pre-SET handshake that appears only after the post-SET fence",
	)
	exactDB := openIsolatedDoltTestDB(t, fixture.config)
	defer exactDB.Close()
	var sessionZone, globalZone string
	require.NoError(t, exactDB.QueryRow(
		"SELECT @@session.time_zone, @@global.time_zone",
	).Scan(&sessionZone, &globalZone))
	require.Equal(t, "America/New_York", sessionZone)
	require.Equal(t, "America/New_York", globalZone)
	_, err = fixture.store.DiscoverLegacyOutcomes(context.Background())
	require.NoError(t, err)
}

func TestActivateDoltEventTimeZonePreservesPostSetClient(t *testing.T) {
	var postSetDB *sql.DB
	var postSetWriter *sql.Conn
	fixture := newIsolatedDoltBeadStoreWithActivationHook(
		t,
		"America/New_York",
		false,
		func(config *DoltTimeZoneActivationConfig) {
			config.afterPostSetFence = func() {
				writerConfig := mysql.NewConfig()
				writerConfig.User = config.User
				writerConfig.Passwd = config.Password
				writerConfig.Net = "tcp"
				writerConfig.Addr = net.JoinHostPort(
					config.Host,
					strconv.Itoa(config.Port),
				)
				writerConfig.DBName = "beads"
				writerConfig.ParseTime = true
				var err error
				postSetDB, err = sql.Open("mysql", writerConfig.FormatDSN())
				require.NoError(t, err)
				postSetWriter, err = postSetDB.Conn(context.Background())
				require.NoError(t, err)
				var sessionZone string
				require.NoError(t, postSetWriter.QueryRowContext(
					context.Background(),
					"SELECT @@session.time_zone",
				).Scan(&sessionZone))
				require.Equal(t, "America/New_York", sessionZone)
			}
			config.handshakeDrainWindow = 150 * time.Millisecond
		},
	)
	t.Cleanup(func() {
		if postSetWriter != nil {
			_ = postSetWriter.Close()
		}
		if postSetDB != nil {
			_ = postSetDB.Close()
		}
	})
	require.NotNil(t, postSetWriter)
	var connectionID uint64
	var sessionZone, globalZone string
	require.NoError(t, postSetWriter.QueryRowContext(
		context.Background(),
		"SELECT CONNECTION_ID(), @@session.time_zone, @@global.time_zone",
	).Scan(&connectionID, &sessionZone, &globalZone))
	require.NotZero(t, connectionID)
	require.Equal(t, "America/New_York", sessionZone)
	require.Equal(t, "America/New_York", globalZone)
	_, err := fixture.store.DiscoverLegacyOutcomes(context.Background())
	require.NoError(t, err)
}

func TestActivateDoltEventTimeZoneRejectsInsufficientBudgetBeforeMutation(
	t *testing.T,
) {
	fixture := newIsolatedDoltBeadStore(t)
	_, err := fixture.db.Exec("SET GLOBAL time_zone = 'SYSTEM'")
	require.NoError(t, err)

	activation := activationConfigForStore(fixture.config)
	activation.handshakeDrainWindow = 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	_, err = ActivateDoltEventTimeZone(ctx, activation)
	require.ErrorContains(t, err, "insufficient remaining deadline before mutation")
	require.Less(t, time.Since(started), 500*time.Millisecond)

	var globalZone string
	require.NoError(t, fixture.db.QueryRow(
		"SELECT @@global.time_zone",
	).Scan(&globalZone))
	require.Equal(t, "SYSTEM", globalZone)
}

func TestDoltTimeZoneActivationConfigFailsClosed(t *testing.T) {
	valid := DoltTimeZoneActivationConfig{
		Host:               "127.0.0.1",
		Port:               3307,
		User:               "root",
		EventTimeZone:      "America/New_York",
		ServerPID:          4321,
		ServerConfigPath:   "/city/dolt-config.yaml",
		EndpointGeneration: "generation-1",
		Clock:              NewSystemClock(),
	}
	require.NoError(t, validateDoltTimeZoneActivationConfig(valid))

	for _, test := range []struct {
		name   string
		mutate func(*DoltTimeZoneActivationConfig)
		want   string
	}{
		{
			name: "non-loopback-host",
			mutate: func(config *DoltTimeZoneActivationConfig) {
				config.Host = "db.example.test"
			},
			want: "loopback",
		},
		{
			name: "port",
			mutate: func(config *DoltTimeZoneActivationConfig) {
				config.Port = 0
			},
			want: "between 1 and 65535",
		},
		{
			name: "user",
			mutate: func(config *DoltTimeZoneActivationConfig) {
				config.User = ""
			},
			want: "user is required",
		},
		{
			name: "time-zone",
			mutate: func(config *DoltTimeZoneActivationConfig) {
				config.EventTimeZone = "Local"
			},
			want: "IANA",
		},
		{
			name: "server-pid",
			mutate: func(config *DoltTimeZoneActivationConfig) {
				config.ServerPID = 0
			},
			want: "server PID is required",
		},
		{
			name: "relative-config",
			mutate: func(config *DoltTimeZoneActivationConfig) {
				config.ServerConfigPath = "dolt-config.yaml"
			},
			want: "must be absolute",
		},
		{
			name: "generation",
			mutate: func(config *DoltTimeZoneActivationConfig) {
				config.EndpointGeneration = ""
			},
			want: "generation is required",
		},
		{
			name: "clock",
			mutate: func(config *DoltTimeZoneActivationConfig) {
				config.Clock = nil
			},
			want: "clock is required",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			require.ErrorContains(
				t,
				validateDoltTimeZoneActivationConfig(config),
				test.want,
			)
		})
	}
}

func TestDoltConnectionIDOrderingAcrossUint32Wrap(t *testing.T) {
	for _, test := range []struct {
		name     string
		id       uint64
		fence    uint64
		precedes bool
	}{
		{name: "normal-before", id: 41, fence: 42, precedes: true},
		{name: "normal-equal", id: 42, fence: 42, precedes: false},
		{name: "normal-after", id: 43, fence: 42, precedes: false},
		{
			name: "pre-wrap-before-low-fence",
			id:   0xfffffffe, fence: 1, precedes: true,
		},
		{
			name: "max-before-low-fence",
			id:   0xffffffff, fence: 1, precedes: true,
		},
		{
			name: "max-before-zero-fence",
			id:   0xffffffff, fence: 0, precedes: true,
		},
		{name: "post-wrap-after", id: 2, fence: 1, precedes: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			precedes, err := doltConnectionIDPrecedesFence(test.id, test.fence)
			require.NoError(t, err)
			require.Equal(t, test.precedes, precedes)
		})
	}

	_, err := doltConnectionIDPrecedesFence(0x100000000, 1)
	require.ErrorContains(t, err, "outside uint32 range")
	_, err = doltConnectionIDPrecedesFence(1, 0x100000000)
	require.ErrorContains(t, err, "outside uint32 range")
}

func TestDoltTimeZoneActivationLockIsSingleOwnerAndBounded(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "dolt-config.yaml")
	first, err := lockDoltTimeZoneActivation(
		context.Background(),
		configPath,
	)
	require.NoError(t, err)
	defer func() { unlockDoltTimeZoneActivation(first) }()

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	second, err := lockDoltTimeZoneActivation(ctx, configPath)
	require.Nil(t, second)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	unlockDoltTimeZoneActivation(first)
	first = nil
	reacquired, err := lockDoltTimeZoneActivation(
		context.Background(),
		configPath,
	)
	require.NoError(t, err)
	unlockDoltTimeZoneActivation(reacquired)
	unlockDoltTimeZoneActivation(nil)
}

func TestOpenDoltBeadStoreOnlyVerifiesCurrentActivationReceipt(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	receiptPath := DoltTimeZoneActivationReceiptPath(
		fixture.config.DoltServerConfigPath,
	)
	require.NoError(t, fixture.store.Close())
	require.NoError(t, os.Remove(receiptPath))

	store, err := OpenDoltBeadStore(context.Background(), fixture.config)
	require.Nil(t, store)
	require.ErrorContains(t, err, "activation receipt")
	_, statErr := os.Stat(receiptPath)
	require.ErrorIs(
		t,
		statErr,
		os.ErrNotExist,
		"per-store open must not recreate or mutate the activation receipt",
	)

	_, err = ActivateDoltEventTimeZone(
		context.Background(),
		activationConfigForStore(fixture.config),
	)
	require.NoError(t, err)
	staleGeneration := fixture.config
	staleGeneration.DoltServerGeneration = "isolated-generation-2"
	store, err = OpenDoltBeadStore(
		context.Background(),
		staleGeneration,
	)
	require.Nil(t, store)
	require.ErrorContains(t, err, "current managed generation")

	require.NoError(t, os.WriteFile(
		fixture.config.DoltServerConfigPath,
		[]byte("log_level: info\n"),
		0o600,
	))
	store, err = OpenDoltBeadStore(context.Background(), fixture.config)
	require.Nil(t, store)
	require.ErrorContains(t, err, "current managed generation")
}

func TestMatchingDoltTimeZonePreflightIsReadOnly(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	writerDB := openIsolatedDoltTestDB(t, fixture.config)
	defer writerDB.Close()
	writer, err := writerDB.Conn(context.Background())
	require.NoError(t, err)
	defer writer.Close()
	var connectionID uint64
	var zone string
	require.NoError(t, writer.QueryRowContext(
		context.Background(),
		"SELECT CONNECTION_ID(), @@session.time_zone",
	).Scan(&connectionID, &zone))
	require.Equal(t, "America/New_York", zone)

	first, err := readDoltTimeZoneActivationReceipt(
		DoltTimeZoneActivationReceiptPath(
			fixture.config.DoltServerConfigPath,
		),
	)
	require.NoError(t, err)
	second, err := ActivateDoltEventTimeZone(
		context.Background(),
		activationConfigForStore(fixture.config),
	)
	require.NoError(t, err)
	require.Equal(t, first, second)
	var afterID uint64
	require.NoError(t, writer.QueryRowContext(
		context.Background(),
		"SELECT CONNECTION_ID()",
	).Scan(&afterID))
	require.Equal(
		t,
		connectionID,
		afterID,
		"a matching receipt must not drain already-exact writers",
	)
}

func TestDoltTimeZoneActivationReappliesAfterRealServerRestart(t *testing.T) {
	fixture := newIsolatedDoltBeadStore(t)
	require.NoError(t, fixture.store.Close())
	require.NoError(t, fixture.db.Close())
	require.NoError(t, fixture.server.Process.Signal(syscall.SIGTERM))
	select {
	case <-fixture.serverExited:
	case <-time.After(5 * time.Second):
		t.Fatal("first Dolt generation did not exit")
	}

	configPath := fixture.config.DoltServerConfigPath
	require.NoError(t, os.WriteFile(
		configPath,
		[]byte(fmt.Sprintf(
			"log_level: warning\nlistener:\n  host: 127.0.0.1\n  port: %d\ndata_dir: %q\nsystem_variables:\n  connect_timeout: \"2\"\n",
			fixture.config.Port,
			fixture.serverRoot,
		)),
		0o600,
	))
	restartLog, err := os.Create(fixture.serverLogPath + ".restart")
	require.NoError(t, err)
	restartedServer := exec.Command(
		"dolt",
		"sql-server",
		"--config",
		configPath,
	)
	restartedServer.Env = append(
		os.Environ(),
		"TZ=America/New_York",
	)
	restartedServer.Dir = fixture.serverRoot
	restartedServer.Stdout = restartLog
	restartedServer.Stderr = restartLog
	require.NoError(t, restartedServer.Start())
	restartedDone := make(chan error, 1)
	go func() { restartedDone <- restartedServer.Wait() }()
	t.Cleanup(func() {
		_ = restartedServer.Process.Signal(syscall.SIGTERM)
		select {
		case <-restartedDone:
		case <-time.After(10 * time.Second):
			t.Errorf(
				"restarted isolated Dolt server did not stop; pid=%d",
				restartedServer.Process.Pid,
			)
		}
		_ = restartLog.Close()
	})

	rotated := fixture.config
	rotated.DoltServerPID = restartedServer.Process.Pid
	rotated.DoltServerGeneration = "isolated-generation-2"
	writerDB := openIsolatedDoltTestDBAfterReady(
		t,
		rotated,
		restartLog.Name(),
	)
	defer writerDB.Close()
	writer, err := writerDB.Conn(context.Background())
	require.NoError(t, err)
	defer writer.Close()
	var beforeZone string
	require.NoError(t, writer.QueryRowContext(
		context.Background(),
		"SELECT @@session.time_zone",
	).Scan(&beforeZone))
	require.Equal(t, "SYSTEM", beforeZone)

	activation := activationConfigForStore(rotated)
	activation.handshakeDrainWindow = 0
	receipt, err := ActivateDoltEventTimeZone(
		context.Background(),
		activation,
	)
	require.NoError(t, err)
	require.Equal(t, "isolated-generation-2", receipt.EndpointGeneration)
	var staleZone string
	require.Error(t, writer.QueryRowContext(
		context.Background(),
		"SELECT @@session.time_zone",
	).Scan(&staleZone))

	store, err := OpenDoltBeadStore(context.Background(), rotated)
	require.NoError(t, err)
	defer store.Close()
	_, err = store.DiscoverLegacyOutcomes(context.Background())
	require.NoError(t, err)
	exactDB := openIsolatedDoltTestDB(t, rotated)
	defer exactDB.Close()
	var sessionZone, globalZone, januaryWall, julyWall string
	require.NoError(t, exactDB.QueryRow(`
		SELECT @@session.time_zone,
		       @@global.time_zone,
		       CAST(CONVERT_TZ('2026-01-31 06:57:13', '+00:00', @@global.time_zone) AS CHAR),
		       CAST(CONVERT_TZ('2026-07-31 06:57:13', '+00:00', @@global.time_zone) AS CHAR)`,
	).Scan(&sessionZone, &globalZone, &januaryWall, &julyWall))
	require.Equal(t, "America/New_York", sessionZone)
	require.Equal(t, "America/New_York", globalZone)
	require.Equal(t, "2026-01-31 01:57:13", januaryWall)
	require.Equal(t, "2026-07-31 02:57:13", julyWall)
}

func openIsolatedDoltTestDB(
	t *testing.T,
	config DoltBeadStoreConfig,
) *sql.DB {
	t.Helper()
	mysqlConfig := mysql.Config{
		User:                 config.User,
		Net:                  "tcp",
		Addr:                 net.JoinHostPort(config.Host, strconv.Itoa(config.Port)),
		DBName:               config.Database,
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	db, err := sql.Open("mysql", mysqlConfig.FormatDSN())
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func openIsolatedDoltTestDBAfterReady(
	t *testing.T,
	config DoltBeadStoreConfig,
	logPath string,
) *sql.DB {
	t.Helper()
	mysqlConfig := mysql.Config{
		User:                 config.User,
		Net:                  "tcp",
		Addr:                 net.JoinHostPort(config.Host, strconv.Itoa(config.Port)),
		DBName:               config.Database,
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	db, err := sql.Open("mysql", mysqlConfig.FormatDSN())
	require.NoError(t, err)
	waitForDolt(t, db, logPath)
	return db
}

func TestConfigureDoltEventTimeZonePropagatesFailure(t *testing.T) {
	injected := errors.New("injected SET GLOBAL failure")
	err := configureDoltEventTimeZone(
		context.Background(),
		failingDoltTimeZoneExecutor{err: injected},
		"America/New_York",
	)
	require.ErrorIs(t, err, injected)
	require.ErrorContains(t, err, "configure managed Dolt global time zone")
}

type failingDoltTimeZoneExecutor struct {
	err error
}

func (f failingDoltTimeZoneExecutor) ExecContext(
	context.Context,
	string,
	...any,
) (sql.Result, error) {
	return nil, f.err
}

type isolatedDoltBeadStore struct {
	store                *DoltBeadStore
	db                   *sql.DB
	config               DoltBeadStoreConfig
	clock                *ManualClock
	beadID               string
	legacyWriter         *sql.Conn
	legacyWriterTimeZone string
	server               *exec.Cmd
	serverRoot           string
	serverLogPath        string
	serverExited         <-chan struct{}
}

func newIsolatedDoltBeadStore(t *testing.T) *isolatedDoltBeadStore {
	return newIsolatedDoltBeadStoreWithServerTimezone(t, "")
}

func newIsolatedDoltBeadStoreWithServerTimezone(
	t *testing.T,
	serverTimezone string,
) *isolatedDoltBeadStore {
	return newIsolatedDoltBeadStoreWithServerTimezoneMode(
		t,
		serverTimezone,
		true,
		false,
	)
}

func newIsolatedDoltBeadStoreWithServerTimezoneMode(
	t *testing.T,
	serverTimezone string,
	explicitServerTimeZone bool,
	configureServerTimeZone bool,
) *isolatedDoltBeadStore {
	return newIsolatedDoltBeadStoreWithActivationHook(
		t,
		serverTimezone,
		explicitServerTimeZone,
		nil,
		configureServerTimeZone,
	)
}

func newIsolatedDoltBeadStoreWithActivationHook(
	t *testing.T,
	serverTimezone string,
	explicitServerTimeZone bool,
	hook func(*DoltTimeZoneActivationConfig),
	configureServerTimeZone ...bool,
) *isolatedDoltBeadStore {
	t.Helper()
	if _, err := exec.LookPath("dolt"); err != nil {
		t.Skip("dolt binary is required for the canonical adapter integration test")
	}

	if serverTimezone == "" {
		serverTimezone = "America/New_York"
	}
	root := t.TempDir()
	databaseDir := filepath.Join(root, "beads")
	require.NoError(t, os.Mkdir(databaseDir, 0o700))
	runCommand(t, databaseDir, "dolt", "init",
		"--name", "Temporal Beads Test",
		"--email", "temporal-beads-test@localhost",
	)

	port := reservePort(t)
	logFile, err := os.Create(filepath.Join(root, "dolt-server.log"))
	require.NoError(t, err)
	server := exec.Command(
		"dolt", "sql-server",
		"--config", filepath.Join(root, "dolt-config.yaml"),
	)
	configPath := filepath.Join(root, "dolt-config.yaml")
	serverConfig := fmt.Sprintf(
		"log_level: warning\nlistener:\n  host: 127.0.0.1\n  port: %d\ndata_dir: %q\nsystem_variables:\n  connect_timeout: \"2\"\n",
		port,
		root,
	)
	if explicitServerTimeZone {
		serverConfig += fmt.Sprintf(
			"  time_zone: %q\n",
			serverTimezone,
		)
	}
	require.NoError(t, os.WriteFile(
		configPath,
		[]byte(serverConfig),
		0o600,
	))
	server.Env = append(os.Environ(), "TZ="+serverTimezone)
	server.Dir = root
	server.Stdout = logFile
	server.Stderr = logFile
	require.NoError(t, server.Start())
	serverDone := make(chan error, 1)
	serverExited := make(chan struct{})
	go func() {
		serverDone <- server.Wait()
		close(serverExited)
	}()
	t.Cleanup(func() {
		if server.Process != nil {
			_ = server.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-serverExited:
		case <-time.After(10 * time.Second):
			t.Errorf("isolated Dolt server did not stop after SIGTERM; pid=%d", server.Process.Pid)
		}
		_ = logFile.Close()
	})

	config := DoltBeadStoreConfig{
		Host:         "127.0.0.1",
		Port:         port,
		Database:     "beads",
		User:         "root",
		Actor:        "temporal-orchestrator",
		CommitAuthor: "Temporal Beads Test <temporal-beads-test@localhost>",
		Clock: NewManualClock(
			time.Date(2026, 7, 30, 20, 0, 0, 0, time.UTC),
		),
		OutcomeRolloutEpoch:  time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		OutcomeStoreRef:      "city:test",
		EventTimeZone:        serverTimezone,
		DoltServerPID:        server.Process.Pid,
		DoltServerConfigPath: configPath,
		DoltServerGeneration: "isolated-generation-1",
	}
	mysqlConfig := mysql.Config{
		User:                 config.User,
		Net:                  "tcp",
		Addr:                 net.JoinHostPort(config.Host, strconv.Itoa(config.Port)),
		DBName:               config.Database,
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	dsn := mysqlConfig.FormatDSN()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	waitForDolt(t, db, logFile.Name())
	createCanonicalIssuesSchema(t, db)
	seedCanonicalIssue(t, db, "dr-test")
	require.NoError(t, db.Close())

	var legacyWriter *sql.Conn
	var legacyWriterTimeZone string
	captureLegacyWriter := len(configureServerTimeZone) > 0 &&
		configureServerTimeZone[0]
	if captureLegacyWriter {
		legacyDB := openIsolatedDoltTestDB(t, config)
		legacyWriter, err = legacyDB.Conn(context.Background())
		require.NoError(t, err)
		require.NoError(t, legacyWriter.QueryRowContext(
			context.Background(),
			"SELECT @@session.time_zone",
		).Scan(&legacyWriterTimeZone))
		t.Cleanup(func() {
			_ = legacyWriter.Close()
			_ = legacyDB.Close()
		})
	}

	activationConfig := activationConfigForStore(config)
	if hook != nil {
		hook(&activationConfig)
	}
	_, err = ActivateDoltEventTimeZone(
		context.Background(),
		activationConfig,
	)
	require.NoError(t, err)
	db = openIsolatedDoltTestDB(t, config)
	store, err := OpenDoltBeadStore(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
		require.NoError(t, db.Close())
	})
	return &isolatedDoltBeadStore{
		store: store, db: db, config: config,
		clock: config.Clock.(*ManualClock), beadID: "dr-test",
		legacyWriter: legacyWriter, legacyWriterTimeZone: legacyWriterTimeZone,
		server: server, serverRoot: root, serverLogPath: logFile.Name(),
		serverExited: serverExited,
	}
}

func activationConfigForStore(
	config DoltBeadStoreConfig,
) DoltTimeZoneActivationConfig {
	return DoltTimeZoneActivationConfig{
		Host: config.Host, Port: config.Port, User: config.User,
		Password: config.Password, EventTimeZone: config.EventTimeZone,
		ServerPID:            config.DoltServerPID,
		ServerConfigPath:     config.DoltServerConfigPath,
		EndpointGeneration:   config.DoltServerGeneration,
		Clock:                config.Clock,
		handshakeDrainWindow: 100 * time.Millisecond,
	}
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, listener.Close()) }()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForDolt(t *testing.T, db *sql.DB, logPath string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		err := db.PingContext(ctx)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	logData, _ := os.ReadFile(logPath)
	t.Fatalf("isolated Dolt server did not become ready:\n%s", logData)
}

func createCanonicalIssuesSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	const schema = `
CREATE TABLE issues (
	id varchar(255) NOT NULL,
	title varchar(500) NOT NULL,
	description longtext NOT NULL,
	design longtext NOT NULL,
	acceptance_criteria longtext NOT NULL,
	notes longtext NOT NULL,
	status varchar(32) NOT NULL DEFAULT 'open',
	priority int NOT NULL DEFAULT 2,
	issue_type varchar(32) NOT NULL DEFAULT 'task',
	assignee varchar(255),
	created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	closed_at datetime,
	closed_by_session varchar(255) DEFAULT '',
	close_reason longtext DEFAULT '',
	metadata json DEFAULT (json_object()),
	started_at datetime,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin`
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err = conn.ExecContext(context.Background(), schema)
	require.NoError(t, err)
	const eventsSchema = `
CREATE TABLE events (
	id char(36) NOT NULL DEFAULT (uuid()),
	issue_id varchar(255) NOT NULL,
	event_type varchar(32) NOT NULL,
	actor varchar(255) NOT NULL,
	old_value longtext,
	new_value longtext,
	comment text,
	created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (id),
	CONSTRAINT fk_events_issue
		FOREIGN KEY (issue_id) REFERENCES issues (id)
		ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin`
	_, err = conn.ExecContext(context.Background(), eventsSchema)
	require.NoError(t, err)
	const dependenciesSchema = `
CREATE TABLE dependencies (
	id char(36) NOT NULL,
	issue_id varchar(255) NOT NULL,
	type varchar(32) NOT NULL DEFAULT 'blocks',
	created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
	created_by varchar(255) NOT NULL,
	metadata json DEFAULT (json_object()),
	thread_id varchar(255) DEFAULT '',
	depends_on_issue_id varchar(255),
	depends_on_wisp_id varchar(255),
	depends_on_external varchar(255),
	PRIMARY KEY (id),
	CONSTRAINT fk_dep_issue
		FOREIGN KEY (issue_id) REFERENCES issues (id)
		ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT fk_dep_issue_target
		FOREIGN KEY (depends_on_issue_id) REFERENCES issues (id)
		ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin`
	_, err = conn.ExecContext(context.Background(), dependenciesSchema)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "CALL DOLT_ADD('issues')")
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "CALL DOLT_ADD('events')")
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "CALL DOLT_ADD('dependencies')")
	require.NoError(t, err)
	_, err = conn.ExecContext(
		context.Background(),
		"CALL DOLT_COMMIT('-m', ?, '--author', ?)",
		"test: initialize canonical Beads issues schema",
		"Temporal Beads Test <temporal-beads-test@localhost>",
	)
	require.NoError(t, err)
}

func seedCanonicalIssue(t *testing.T, db *sql.DB, beadID string) {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err = conn.ExecContext(
		context.Background(),
		`INSERT INTO issues
			(id, title, description, design, acceptance_criteria, notes, metadata)
		 VALUES (?, 'adapter test', '', '', '', '', JSON_OBJECT('unrelated', 'keep'))`,
		beadID,
	)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "CALL DOLT_ADD('issues')")
	require.NoError(t, err)
	_, err = conn.ExecContext(
		context.Background(),
		"CALL DOLT_COMMIT('-m', ?, '--author', ?)",
		"test: seed canonical Beads issue",
		"Temporal Beads Test <temporal-beads-test@localhost>",
	)
	require.NoError(t, err)
}

func runCommand(t *testing.T, dir, name string, args ...string) {
	t.Helper()
	command := exec.Command(name, args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s %v:\n%s", name, args, output)
}
