package temporalbeads

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/sys/unix"
)

const (
	doltTimeZoneActivationVersion           = 1
	doltTimeZoneMaxConfigBytes              = 1 << 20
	doltTimeZoneMaxDrainSessions            = 4096
	doltTimeZoneActivationTimeout           = 15 * time.Second
	doltTimeZoneActivationCompletionReserve = 2 * time.Second
	doltConnectionIDLimit                   = uint64(1) << 32
	doltConnectionIDHalfRange               = uint32(1) << 31
)

// DoltTimeZoneActivationConfig identifies one managed endpoint generation.
// Activation is an explicit single-owner preflight, never a per-store action.
type DoltTimeZoneActivationConfig struct {
	Host               string
	Port               int
	User               string
	Password           string
	EventTimeZone      string
	ServerPID          int
	ServerConfigPath   string
	EndpointGeneration string
	Clock              Clock
	// Test hooks run inside the real handshake drain window. They are
	// intentionally unexported outside this package.
	afterPostSetFence    func()
	handshakeDrainWindow time.Duration
}

// DoltTimeZoneActivationReceipt proves that every client session which existed
// before the named global zone was installed has left this exact generation.
type DoltTimeZoneActivationReceipt struct {
	Version            int       `json:"version"`
	TimeZone           string    `json:"time_zone"`
	ServerPID          int       `json:"server_pid"`
	StartTime          uint64    `json:"start_time"`
	EndpointGeneration string    `json:"endpoint_generation"`
	ConfigSHA256       string    `json:"config_sha256"`
	ActivatedAt        time.Time `json:"activated_at"`
}

// ActivateDoltEventTimeZone performs the bounded endpoint-wide activation
// transaction. A matching receipt makes repeated preflights read-only.
func ActivateDoltEventTimeZone(
	ctx context.Context,
	config DoltTimeZoneActivationConfig,
) (DoltTimeZoneActivationReceipt, error) {
	if err := validateDoltTimeZoneActivationConfig(config); err != nil {
		return DoltTimeZoneActivationReceipt{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, doltTimeZoneActivationTimeout)
	defer cancel()
	lock, err := lockDoltTimeZoneActivation(ctx, config.ServerConfigPath)
	if err != nil {
		return DoltTimeZoneActivationReceipt{}, err
	}
	defer unlockDoltTimeZoneActivation(lock)

	identity, err := managedDoltServerIdentityAt(
		"/proc",
		config.ServerPID,
		config.ServerConfigPath,
		os.Geteuid(),
	)
	if err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q: %w",
			config.EventTimeZone,
			err,
		)
	}
	configDigest, err := managedDoltConfigDigest(config.ServerConfigPath)
	if err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q config identity: %w",
			config.EventTimeZone,
			err,
		)
	}
	receiptPath := DoltTimeZoneActivationReceiptPath(config.ServerConfigPath)

	db, err := openDoltTimeZoneControl(config)
	if err != nil {
		return DoltTimeZoneActivationReceipt{}, err
	}
	defer db.Close()
	control, err := db.Conn(ctx)
	if err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q control connection: %w",
			config.EventTimeZone,
			err,
		)
	}
	defer control.Close()

	var controlID uint64
	var globalZone string
	if err := control.QueryRowContext(
		ctx,
		"SELECT CONNECTION_ID(), @@global.time_zone",
	).Scan(&controlID, &globalZone); err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q inspect endpoint: %w",
			config.EventTimeZone,
			err,
		)
	}
	if globalZone == config.EventTimeZone {
		receipt, err := readDoltTimeZoneActivationReceipt(receiptPath)
		if err == nil && validateDoltTimeZoneActivationReceipt(
			receipt,
			config.EventTimeZone,
			identity,
			config.EndpointGeneration,
			configDigest,
		) == nil {
			return receipt, nil
		}
	}

	drainWindow := config.handshakeDrainWindow
	if drainWindow <= 0 {
		var connectTimeoutSeconds int
		if err := control.QueryRowContext(
			ctx,
			"SELECT @@global.connect_timeout",
		).Scan(&connectTimeoutSeconds); err != nil {
			return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
				"activate Dolt event time zone %q read handshake timeout: %w",
				config.EventTimeZone,
				err,
			)
		}
		if connectTimeoutSeconds < 1 || connectTimeoutSeconds > 30 {
			return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
				"activate Dolt event time zone %q connect_timeout=%d is outside bounded range 1..30 seconds",
				config.EventTimeZone,
				connectTimeoutSeconds,
			)
		}
		drainWindow = time.Duration(connectTimeoutSeconds)*time.Second +
			100*time.Millisecond
	}
	preSetIDs, err := doltConnectionIDsExcept(ctx, control, controlID)
	if err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q snapshot pre-mutation sessions: %w",
			config.EventTimeZone,
			err,
		)
	}
	if err := requireDoltTimeZoneActivationBudget(ctx, drainWindow); err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q: %w",
			config.EventTimeZone,
			err,
		)
	}
	if err := configureDoltEventTimeZone(
		ctx,
		control,
		config.EventTimeZone,
	); err != nil {
		return DoltTimeZoneActivationReceipt{}, err
	}
	fenceID, err := openDoltPostSetConnectionFence(
		ctx,
		db,
		config.EventTimeZone,
	)
	if err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q establish post-SET connection fence: %w",
			config.EventTimeZone,
			err,
		)
	}
	if config.afterPostSetFence != nil {
		config.afterPostSetFence()
	}
	if err := drainDoltConnectionsForHandshakeWindow(
		ctx,
		control,
		controlID,
		fenceID,
		preSetIDs,
		drainWindow,
	); err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q handshake drain: %w",
			config.EventTimeZone,
			err,
		)
	}
	var sessionZone string
	if err := control.QueryRowContext(
		ctx,
		"SELECT @@session.time_zone, @@global.time_zone",
	).Scan(&sessionZone, &globalZone); err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q verify endpoint: %w",
			config.EventTimeZone,
			err,
		)
	}
	if sessionZone != config.EventTimeZone ||
		globalZone != config.EventTimeZone {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q left session=%q global=%q",
			config.EventTimeZone,
			sessionZone,
			globalZone,
		)
	}
	afterIdentity, err := managedDoltServerIdentityAt(
		"/proc",
		config.ServerPID,
		config.ServerConfigPath,
		os.Geteuid(),
	)
	if err != nil {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q after drain: %w",
			config.EventTimeZone,
			err,
		)
	}
	if afterIdentity != identity {
		return DoltTimeZoneActivationReceipt{}, fmt.Errorf(
			"activate Dolt event time zone %q: managed server identity changed during activation",
			config.EventTimeZone,
		)
	}
	receipt := DoltTimeZoneActivationReceipt{
		Version:  doltTimeZoneActivationVersion,
		TimeZone: config.EventTimeZone, ServerPID: identity.PID,
		StartTime:          identity.StartTime,
		EndpointGeneration: config.EndpointGeneration,
		ConfigSHA256:       configDigest,
		ActivatedAt:        config.Clock.Now().UTC(),
	}
	if err := writeDoltTimeZoneActivationReceipt(receiptPath, receipt); err != nil {
		return DoltTimeZoneActivationReceipt{}, err
	}
	return receipt, nil
}

func requireDoltTimeZoneActivationBudget(
	ctx context.Context,
	drainWindow time.Duration,
) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("bounded activation deadline is required")
	}
	required := drainWindow + doltTimeZoneActivationCompletionReserve
	remaining := time.Until(deadline)
	if remaining < required {
		return fmt.Errorf(
			"insufficient remaining deadline before mutation: have %s, require at least %s",
			remaining.Round(time.Millisecond),
			required,
		)
	}
	return nil
}

func validateDoltTimeZoneActivationConfig(
	config DoltTimeZoneActivationConfig,
) error {
	if err := validateLoopbackHost(config.Host); err != nil {
		return err
	}
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("Dolt activation port must be between 1 and 65535")
	}
	if config.User == "" {
		return fmt.Errorf("Dolt activation user is required")
	}
	if err := ValidateDoltEventTimeZone(config.EventTimeZone); err != nil {
		return err
	}
	if config.ServerPID <= 0 {
		return fmt.Errorf("managed Dolt server PID is required")
	}
	if !filepath.IsAbs(config.ServerConfigPath) {
		return fmt.Errorf("managed Dolt server config path must be absolute")
	}
	if config.EndpointGeneration == "" {
		return fmt.Errorf("managed Dolt endpoint generation is required")
	}
	if config.Clock == nil {
		return fmt.Errorf("Dolt activation clock is required")
	}
	return nil
}

func openDoltTimeZoneControl(
	config DoltTimeZoneActivationConfig,
) (*sql.DB, error) {
	driverConfig := mysql.NewConfig()
	driverConfig.User = config.User
	driverConfig.Passwd = config.Password
	driverConfig.Net = "tcp"
	driverConfig.Addr = net.JoinHostPort(
		config.Host,
		strconv.Itoa(config.Port),
	)
	driverConfig.ParseTime = true
	driverConfig.Timeout = 5 * time.Second
	driverConfig.ReadTimeout = 30 * time.Second
	driverConfig.WriteTimeout = 30 * time.Second
	driverConfig.Params = map[string]string{
		"time_zone": "'" + config.EventTimeZone + "'",
	}
	db, err := sql.Open("mysql", driverConfig.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open Dolt time-zone activation control: %w", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(0)
	return db, nil
}

func openDoltPostSetConnectionFence(
	ctx context.Context,
	db *sql.DB,
	eventTimeZone string,
) (uint64, error) {
	fence, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer fence.Close()
	var fenceID uint64
	var sessionZone, globalZone string
	if err := fence.QueryRowContext(
		ctx,
		"SELECT CONNECTION_ID(), @@session.time_zone, @@global.time_zone",
	).Scan(&fenceID, &sessionZone, &globalZone); err != nil {
		return 0, err
	}
	if sessionZone != eventTimeZone || globalZone != eventTimeZone {
		return 0, fmt.Errorf(
			"post-SET fence observed session=%q global=%q",
			sessionZone,
			globalZone,
		)
	}
	return fenceID, nil
}

func doltConnectionIDsExcept(
	ctx context.Context,
	control *sql.Conn,
	controlID uint64,
) ([]uint64, error) {
	rows, err := control.QueryContext(
		ctx,
		"SELECT ID FROM information_schema.processlist WHERE ID <> ?",
		controlID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// doltConnectionIDPrecedesFence compares Dolt/Vitess uint32 connection IDs
// as serial numbers. The subtraction remains correct when the sequence wraps
// from 0xffffffff to zero; equality is the fence itself, never pre-fence.
func doltConnectionIDPrecedesFence(id uint64, fence uint64) (bool, error) {
	if id >= doltConnectionIDLimit || fence >= doltConnectionIDLimit {
		return false, fmt.Errorf(
			"Dolt connection ID or fence is outside uint32 range: id=%d fence=%d",
			id,
			fence,
		)
	}
	if id == fence {
		return false, nil
	}
	distance := uint32(fence) - uint32(id)
	return distance <= doltConnectionIDHalfRange, nil
}

func drainDoltConnectionsForHandshakeWindow(
	ctx context.Context,
	control *sql.Conn,
	controlID uint64,
	fenceID uint64,
	preSetIDs []uint64,
	window time.Duration,
) error {
	if window <= 0 {
		return fmt.Errorf("handshake drain window must be positive")
	}
	if _, err := doltConnectionIDPrecedesFence(controlID, fenceID); err != nil {
		return err
	}
	knownPreSet := make(map[uint64]struct{}, len(preSetIDs))
	for _, id := range preSetIDs {
		knownPreSet[id] = struct{}{}
	}
	preSetCandidates := func() ([]uint64, error) {
		sessionIDs, err := doltConnectionIDsExcept(ctx, control, controlID)
		if err != nil {
			return nil, err
		}
		candidates := sessionIDs[:0]
		for _, id := range sessionIDs {
			_, known := knownPreSet[id]
			precedes, err := doltConnectionIDPrecedesFence(id, fenceID)
			if err != nil {
				return nil, err
			}
			if known || precedes {
				candidates = append(candidates, id)
			}
		}
		return candidates, nil
	}
	deadline := time.Now().Add(window)
	for {
		sessionIDs, err := preSetCandidates()
		if err != nil {
			return err
		}
		if len(sessionIDs) > doltTimeZoneMaxDrainSessions {
			return fmt.Errorf(
				"refuses to drain %d sessions (limit %d)",
				len(sessionIDs),
				doltTimeZoneMaxDrainSessions,
			)
		}
		for _, id := range sessionIDs {
			_, _ = control.ExecContext(
				ctx,
				fmt.Sprintf("KILL CONNECTION %d", id),
			)
		}
		if !time.Now().Before(deadline) {
			finalIDs, err := preSetCandidates()
			if err != nil {
				return err
			}
			if len(finalIDs) != 0 {
				return fmt.Errorf(
					"residual sessions after handshake window: %v",
					finalIDs,
				)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func lockDoltTimeZoneActivation(
	ctx context.Context,
	configPath string,
) (*os.File, error) {
	lockPath := configPath + ".outcome-ready-time-zone.lock"
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open activation lock: %w", err)
	}
	for {
		err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			_ = lock.Close()
			return nil, fmt.Errorf("acquire activation lock: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = lock.Close()
			return nil, fmt.Errorf("acquire activation lock: %w", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func unlockDoltTimeZoneActivation(lock *os.File) {
	if lock == nil {
		return
	}
	_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	_ = lock.Close()
}

// DoltTimeZoneActivationReceiptPath is the deterministic local receipt path
// shared by the single owner and all read-only store adapters.
func DoltTimeZoneActivationReceiptPath(configPath string) string {
	return configPath + ".outcome-ready-time-zone.json"
}

func managedDoltConfigDigest(configPath string) (string, error) {
	info, err := os.Lstat(configPath)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("managed Dolt config is not a regular file")
	}
	if info.Size() > doltTimeZoneMaxConfigBytes {
		return "", fmt.Errorf(
			"managed Dolt config exceeds %d bytes",
			doltTimeZoneMaxConfigBytes,
		)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest[:]), nil
}

func readDoltTimeZoneActivationReceipt(
	path string,
) (DoltTimeZoneActivationReceipt, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return DoltTimeZoneActivationReceipt{}, err
	}
	var receipt DoltTimeZoneActivationReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		return DoltTimeZoneActivationReceipt{}, err
	}
	return receipt, nil
}

func writeDoltTimeZoneActivationReceipt(
	path string,
	receipt DoltTimeZoneActivationReceipt,
) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("marshal Dolt time-zone activation receipt: %w", err)
	}
	data = append(data, '\n')
	if err := writeDoltTimeZoneFileAtomic(path, data, 0o600); err != nil {
		return fmt.Errorf("write Dolt time-zone activation receipt: %w", err)
	}
	return nil
}

func writeDoltTimeZoneFileAtomic(
	path string,
	data []byte,
	mode os.FileMode,
) (returnErr error) {
	directory := filepath.Dir(path)
	temp, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		if returnErr != nil {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(mode); err != nil {
		return err
	}
	if _, err := temp.Write(data); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func validateDoltTimeZoneActivationReceipt(
	receipt DoltTimeZoneActivationReceipt,
	name string,
	identity managedDoltServerIdentity,
	endpointGeneration string,
	configDigest string,
) error {
	if receipt.TimeZone != name {
		return fmt.Errorf(
			"Dolt time-zone activation receipt zone %q does not match requested %q",
			receipt.TimeZone,
			name,
		)
	}
	if receipt.Version != doltTimeZoneActivationVersion ||
		receipt.ServerPID != identity.PID ||
		receipt.StartTime != identity.StartTime ||
		receipt.EndpointGeneration != endpointGeneration ||
		receipt.ConfigSHA256 != configDigest ||
		receipt.ActivatedAt.IsZero() {
		return fmt.Errorf(
			"Dolt time-zone activation receipt does not match current managed generation",
		)
	}
	return nil
}

func verifyDoltTimeZoneActivationReceipt(
	configPath string,
	name string,
	identity managedDoltServerIdentity,
	endpointGeneration string,
) error {
	digest, err := managedDoltConfigDigest(configPath)
	if err != nil {
		return err
	}
	receipt, err := readDoltTimeZoneActivationReceipt(
		DoltTimeZoneActivationReceiptPath(configPath),
	)
	if err != nil {
		return err
	}
	return validateDoltTimeZoneActivationReceipt(
		receipt,
		name,
		identity,
		endpointGeneration,
		digest,
	)
}
