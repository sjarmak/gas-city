// Command fakeagent is a stand-in coding agent that speaks the real
// start-or-attach adapter protocol: newline-delimited JSON on stdin and stdout,
// with the operation (resolve | execute | cancel) as argv[1].
//
// It is invoked by the reviewed CommandAgentExecutor without modification, so
// the process-spawn path, the protocol parsing, and the Activity's session
// handling are all the production code. Only the work is fake: instead of
// editing a git worktree with a model, it appends lines to a fixture file.
//
// Two properties are load-bearing for the demo and are implemented on purpose:
//
//  1. Session identity is resolved from a durable registry keyed by
//     (work item, generation). A second resolve for the same key returns the
//     session that already exists rather than minting a new one. This is what
//     prevents a duplicate launch, and it works whether or not a heartbeat
//     checkpoint survived.
//
//  2. SIGPIPE is disarmed. When the Worker that spawned this process is killed,
//     the stdout pipe breaks; this process notices, records that it is now
//     orphaned, and keeps doing its work. That is the whole point: the agent
//     outlives the Worker that started it.
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	phaseAttached  = "agent-attached"
	outcomeDone    = "completed"
	artifactKind   = "commit"
	minLiveWindow  = 2 * time.Second
	sessionIDBytes = 4
)

// ---------------------------------------------------------------- protocol in

type readyEvent struct {
	BeadID     string `json:"bead_id"`
	Generation int64  `json:"generation"`
	CityID     string `json:"city_id"`
	RunID      string `json:"run_id"`
}

type checkpoint struct {
	SessionID string `json:"session_id"`
	Sequence  int64  `json:"sequence"`
	Phase     string `json:"phase"`
}

type executionRequest struct {
	Event      readyEvent  `json:"event"`
	ClaimToken string      `json:"claim_token"`
	SessionID  string      `json:"session_id"`
	ResumeFrom *checkpoint `json:"resume_from"`
}

type cancellation struct {
	BeadID     string `json:"bead_id"`
	Generation int64  `json:"generation"`
	ClaimToken string `json:"claim_token"`
	SessionID  string `json:"session_id"`
}

type protocolRequest struct {
	Operation    string            `json:"operation"`
	Execution    *executionRequest `json:"execution"`
	Cancellation *cancellation     `json:"cancellation"`
}

// --------------------------------------------------------------- protocol out

type artifactRef struct {
	Kind   string `json:"kind"`
	URI    string `json:"uri"`
	SHA256 string `json:"sha256"`
}

type progressBody struct {
	SessionID    string        `json:"session_id"`
	Sequence     int64         `json:"sequence"`
	Phase        string        `json:"phase"`
	ArtifactRefs []artifactRef `json:"artifact_refs,omitempty"`
}

type resultBody struct {
	SessionID    string        `json:"session_id"`
	Outcome      string        `json:"outcome"`
	ArtifactRefs []artifactRef `json:"artifact_refs,omitempty"`
}

type message struct {
	Type      string        `json:"type"`
	SessionID string        `json:"session_id,omitempty"`
	Progress  *progressBody `json:"progress,omitempty"`
	Result    *resultBody   `json:"result,omitempty"`
}

// ------------------------------------------------------------- durable record

type sessionRecord struct {
	SessionID  string `json:"session_id"`
	WorkItemID string `json:"work_item_id"`
	Generation int64  `json:"generation"`
	CreatedAt  string `json:"created_at"`
}

type liveRecord struct {
	PID       int    `json:"pid"`
	UpdatedAt string `json:"updated_at"`
}

type terminalRecord struct {
	SessionID  string      `json:"session_id"`
	Outcome    string      `json:"outcome"`
	Artifact   artifactRef `json:"artifact"`
	Ticks      int         `json:"ticks"`
	FinishedAt string      `json:"finished_at"`
}

type eventRecord struct {
	TS             string `json:"ts"`
	PID            int    `json:"pid"`
	Operation      string `json:"operation"`
	Kind           string `json:"kind"`
	Key            string `json:"key"`
	SessionID      string `json:"session_id,omitempty"`
	ResumeSequence int64  `json:"resume_sequence,omitempty"`
	Detail         string `json:"detail,omitempty"`
}

type workEntry struct {
	TS             string `json:"ts"`
	PID            int    `json:"pid"`
	SessionID      string `json:"session_id"`
	Sequence       int64  `json:"sequence"`
	AfterPipeBreak bool   `json:"after_pipe_break"`
}

// ------------------------------------------------------------------- settings

type settings struct {
	home          string
	resolveDelay  time.Duration
	tick          time.Duration
	ticks         int
	attachTimeout time.Duration
}

func loadSettings() (settings, error) {
	home := os.Getenv("AGENT_HOME")
	if home == "" {
		return settings{}, errors.New("AGENT_HOME is required")
	}
	if !filepath.IsAbs(home) {
		return settings{}, errors.New("AGENT_HOME must be an absolute path")
	}
	s := settings{
		home:          home,
		resolveDelay:  envDuration("AGENT_RESOLVE_DELAY_MS", 0),
		tick:          envDuration("AGENT_TICK_MS", 500*time.Millisecond),
		ticks:         envInt("AGENT_EXECUTE_TICKS", 8),
		attachTimeout: envDuration("AGENT_ATTACH_TIMEOUT_MS", 120*time.Second),
	}
	if s.tick <= 0 {
		return settings{}, errors.New("AGENT_TICK_MS must be positive")
	}
	if s.ticks <= 0 {
		return settings{}, errors.New("AGENT_EXECUTE_TICKS must be positive")
	}
	return s, nil
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	milliseconds, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

// ----------------------------------------------------------------------- main

// orphaned records that stdout has broken, which means the Worker that spawned
// this process is gone. It is never reset: once orphaned, always orphaned.
var orphaned bool

func main() {
	// Disarm SIGPIPE so a broken stdout returns EPIPE to the writer instead of
	// killing this process. Without this the agent dies with its Worker and the
	// demo's central claim silently stops being true.
	pipeSignals := make(chan os.Signal, 8)
	signal.Notify(pipeSignals, syscall.SIGPIPE)
	go func() {
		for range pipeSignals {
		}
	}()

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fakeagent: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: fakeagent resolve|execute|cancel")
	}
	config, err := loadSettings()
	if err != nil {
		return err
	}
	payload, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	var request protocolRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	switch os.Args[1] {
	case "resolve":
		return resolve(config, request)
	case "execute":
		return execute(config, request)
	case "cancel":
		return cancel(config, request)
	default:
		return fmt.Errorf("unknown operation %q", os.Args[1])
	}
}

// -------------------------------------------------------------------- resolve

func resolve(config settings, request protocolRequest) error {
	if request.Execution == nil {
		return errors.New("resolve requires an execution request")
	}
	key := sessionKey(request.Execution.Event)
	release, err := lockHome(config)
	if err != nil {
		return err
	}
	record, created, err := loadOrCreateSession(config, request.Execution.Event)
	if err != nil {
		release()
		return err
	}
	kind := "session-attached"
	if created {
		kind = "session-created"
	}
	appendErr := appendEvent(config, eventRecord{
		TS: stamp(), PID: os.Getpid(), Operation: "resolve", Kind: kind,
		Key: key, SessionID: record.SessionID,
	})
	release()
	if appendErr != nil {
		return appendErr
	}

	// The delay sits AFTER the durable registration on purpose. A Worker killed
	// during resolve therefore leaves a registered session behind, which is the
	// state the second arm of the demo needs.
	if config.resolveDelay > 0 {
		time.Sleep(config.resolveDelay)
	}
	return emit(message{Type: "resolved", SessionID: record.SessionID})
}

// -------------------------------------------------------------------- execute

func execute(config settings, request protocolRequest) error {
	if request.Execution == nil {
		return errors.New("execute requires an execution request")
	}
	event := request.Execution.Event
	key := sessionKey(event)
	directory := sessionDirectory(config, event)

	release, err := lockHome(config)
	if err != nil {
		return err
	}
	record, err := readSession(directory)
	if err != nil {
		release()
		return err
	}
	if request.Execution.SessionID != "" &&
		request.Execution.SessionID != record.SessionID {
		release()
		return fmt.Errorf(
			"execute targets a session that is not bound to %s", key)
	}
	resumeSequence := int64(0)
	if request.Execution.ResumeFrom != nil {
		resumeSequence = request.Execution.ResumeFrom.Sequence
	}
	mode := decideMode(directory, config)
	if mode == "execute-own" {
		if err := writeJSON(filepath.Join(directory, "live.json"), liveRecord{
			PID: os.Getpid(), UpdatedAt: stamp(),
		}); err != nil {
			release()
			return err
		}
	}
	appendErr := appendEvent(config, eventRecord{
		TS: stamp(), PID: os.Getpid(), Operation: "execute", Kind: mode,
		Key: key, SessionID: record.SessionID, ResumeSequence: resumeSequence,
	})
	release()
	if appendErr != nil {
		return appendErr
	}

	sequence := resumeSequence
	switch mode {
	case "execute-replay":
		sequence++
		_ = emit(progressMessage(record.SessionID, sequence))
		return emitTerminal(directory, record.SessionID)
	case "execute-attach":
		return attachAndWait(config, directory, record, key, sequence)
	default:
		return ownAndWork(config, directory, record, key, sequence)
	}
}

// decideMode chooses between replaying a finished session, attaching to one
// that another process is still driving, and owning the work.
func decideMode(directory string, config settings) string {
	if fileExists(filepath.Join(directory, "terminal.json")) {
		return "execute-replay"
	}
	var live liveRecord
	if err := readJSON(filepath.Join(directory, "live.json"), &live); err != nil {
		return "execute-own"
	}
	updated, err := time.Parse(time.RFC3339Nano, live.UpdatedAt)
	if err != nil {
		return "execute-own"
	}
	window := 3 * config.tick
	if window < minLiveWindow {
		window = minLiveWindow
	}
	if time.Since(updated) > window {
		return "execute-own"
	}
	if live.PID == os.Getpid() || !processAlive(live.PID) {
		return "execute-own"
	}
	return "execute-attach"
}

func ownAndWork(
	config settings,
	directory string,
	record sessionRecord,
	key string,
	sequence int64,
) error {
	worktree := filepath.Join(config.home, "worktree", record.WorkItemID+".txt")
	for index := 1; index <= config.ticks; index++ {
		time.Sleep(config.tick)
		release, err := lockHome(config)
		if err != nil {
			return err
		}
		err = errors.Join(
			appendLine(worktree, fmt.Sprintf(
				"%s edit %d of %d by session %s\n",
				stamp(), index, config.ticks, record.SessionID)),
			appendJSONL(filepath.Join(directory, "worklog.jsonl"), workEntry{
				TS: stamp(), PID: os.Getpid(), SessionID: record.SessionID,
				Sequence: int64(index), AfterPipeBreak: orphaned,
			}),
			writeJSON(filepath.Join(directory, "live.json"), liveRecord{
				PID: os.Getpid(), UpdatedAt: stamp(),
			}),
		)
		release()
		if err != nil {
			return err
		}
		sequence++
		// A failed emit means the Worker is gone. Keep working anyway.
		_ = emit(progressMessage(record.SessionID, sequence))
	}

	release, err := lockHome(config)
	if err != nil {
		return err
	}
	digest, err := fileDigest(worktree)
	if err != nil {
		release()
		return err
	}
	terminal := terminalRecord{
		SessionID: record.SessionID,
		Outcome:   outcomeDone,
		Artifact: artifactRef{
			Kind:   artifactKind,
			URI:    "file:worktree/" + record.WorkItemID + ".txt",
			SHA256: digest,
		},
		Ticks:      config.ticks,
		FinishedAt: stamp(),
	}
	err = errors.Join(
		writeJSON(filepath.Join(directory, "terminal.json"), terminal),
		appendEvent(config, eventRecord{
			TS: stamp(), PID: os.Getpid(), Operation: "execute",
			Kind: "execute-result", Key: key, SessionID: record.SessionID,
			Detail: "owned",
		}),
	)
	release()
	if err != nil {
		return err
	}
	return emit(message{Type: "result", Result: &resultBody{
		SessionID:    terminal.SessionID,
		Outcome:      terminal.Outcome,
		ArtifactRefs: []artifactRef{terminal.Artifact},
	}})
}

func attachAndWait(
	config settings,
	directory string,
	record sessionRecord,
	key string,
	sequence int64,
) error {
	deadline := time.Now().Add(config.attachTimeout)
	terminalPath := filepath.Join(directory, "terminal.json")
	for time.Now().Before(deadline) {
		if fileExists(terminalPath) {
			release, err := lockHome(config)
			if err != nil {
				return err
			}
			appendErr := appendEvent(config, eventRecord{
				TS: stamp(), PID: os.Getpid(), Operation: "execute",
				Kind: "execute-result", Key: key, SessionID: record.SessionID,
				Detail: "attached",
			})
			release()
			if appendErr != nil {
				return appendErr
			}
			return emitTerminal(directory, record.SessionID)
		}
		time.Sleep(config.tick)
		sequence++
		_ = emit(progressMessage(record.SessionID, sequence))
	}
	return fmt.Errorf(
		"attached session %s did not reach a terminal result in time",
		record.SessionID)
}

func emitTerminal(directory, sessionID string) error {
	var terminal terminalRecord
	if err := readJSON(
		filepath.Join(directory, "terminal.json"),
		&terminal,
	); err != nil {
		return err
	}
	if terminal.SessionID != sessionID {
		return fmt.Errorf(
			"terminal record belongs to a different session than %s", sessionID)
	}
	return emit(message{Type: "result", Result: &resultBody{
		SessionID:    terminal.SessionID,
		Outcome:      terminal.Outcome,
		ArtifactRefs: []artifactRef{terminal.Artifact},
	}})
}

// --------------------------------------------------------------------- cancel

func cancel(config settings, request protocolRequest) error {
	if request.Cancellation == nil {
		return errors.New("cancel requires a cancellation request")
	}
	release, err := lockHome(config)
	if err != nil {
		return err
	}
	appendErr := appendEvent(config, eventRecord{
		TS: stamp(), PID: os.Getpid(), Operation: "cancel", Kind: "cancel",
		Key:       request.Cancellation.BeadID + "-g" +
			strconv.FormatInt(request.Cancellation.Generation, 10),
		SessionID: request.Cancellation.SessionID,
	})
	release()
	if appendErr != nil {
		return appendErr
	}
	return emit(message{Type: "canceled"})
}

// -------------------------------------------------------------------- storage

func sessionKey(event readyEvent) string {
	return event.BeadID + "-g" + strconv.FormatInt(event.Generation, 10)
}

func sessionDirectory(config settings, event readyEvent) string {
	return filepath.Join(config.home, "sessions", sessionKey(event))
}

func loadOrCreateSession(
	config settings,
	event readyEvent,
) (sessionRecord, bool, error) {
	directory := sessionDirectory(config, event)
	existing, err := readSession(directory)
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return sessionRecord{}, false, err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return sessionRecord{}, false, err
	}
	suffix := make([]byte, sessionIDBytes)
	if _, err := rand.Read(suffix); err != nil {
		return sessionRecord{}, false, err
	}
	record := sessionRecord{
		SessionID:  "agent-session-" + hex.EncodeToString(suffix),
		WorkItemID: event.BeadID,
		Generation: event.Generation,
		CreatedAt:  stamp(),
	}
	if err := writeJSON(
		filepath.Join(directory, "session.json"),
		record,
	); err != nil {
		return sessionRecord{}, false, err
	}
	return record, true, nil
}

func readSession(directory string) (sessionRecord, error) {
	var record sessionRecord
	if err := readJSON(
		filepath.Join(directory, "session.json"),
		&record,
	); err != nil {
		return sessionRecord{}, err
	}
	if record.SessionID == "" {
		return sessionRecord{}, errors.New("session record has no identity")
	}
	return record, nil
}

func appendEvent(config settings, record eventRecord) error {
	return appendJSONL(filepath.Join(config.home, "agent-events.jsonl"), record)
}

func lockHome(config settings) (func(), error) {
	if err := os.MkdirAll(config.home, 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(
		filepath.Join(config.home, "worktree"),
		0o700,
	); err != nil {
		return nil, err
	}
	handle, err := os.OpenFile(
		filepath.Join(config.home, "agent.lock"),
		os.O_CREATE|os.O_RDWR,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open agent lock: %w", err)
	}
	if err := unix.Flock(int(handle.Fd()), unix.LOCK_EX); err != nil {
		handle.Close()
		return nil, fmt.Errorf("lock agent home: %w", err)
	}
	return func() {
		_ = unix.Flock(int(handle.Fd()), unix.LOCK_UN)
		_ = handle.Close()
	}, nil
}

func writeJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func readJSON(path string, destination any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, destination)
}

func appendJSONL(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return appendLine(path, string(data)+"\n")
}

func appendLine(path, line string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := handle.WriteString(line); err != nil {
		handle.Close()
		return err
	}
	return handle.Close()
}

func fileDigest(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

// ----------------------------------------------------------------------- wire

func progressMessage(sessionID string, sequence int64) message {
	return message{Type: "progress", Progress: &progressBody{
		SessionID: sessionID, Sequence: sequence, Phase: phaseAttached,
	}}
}

func emit(value message) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := os.Stdout.Write(append(data, '\n')); err != nil {
		orphaned = true
		return err
	}
	return nil
}

func stamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
