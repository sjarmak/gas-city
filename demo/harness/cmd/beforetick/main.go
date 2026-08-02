// Command beforetick is the pre-Temporal coordinator, reconstructed for the
// demo. One reconcile loop claims ready work, launches the agent, waits for it
// inline, and records the completion. The claim is durable; the procedure
// around it lives in this process's memory, which is the before-world's
// defining property and the thing a kill -9 erases.
//
// Three before-world behaviours are implemented on purpose, because they are
// what the conversion removed:
//
//  1. No fence. A completion is recorded last-writer-wins; nothing checks that
//     it belongs to the current claim generation.
//  2. Staleness is inferred, not owned. A restarted loop decides a claim is
//     dead when it is older than BEFORE_STALE_AFTER_SECONDS, because the
//     in-memory procedure that knew better no longer exists. A slow but alive
//     agent is indistinguishable from a dead one.
//  3. The recovery scan records whatever finished. Any terminal session it
//     finds with no recorded completion is written to the store, current
//     generation or not.
//
// The agent it launches is the same fakeagent binary the Temporal arms use,
// driven over the same resolve/execute protocol. Only the coordinator differs.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ------------------------------------------------------------------ store

type claimRecord struct {
	Generation int64  `json:"generation"`
	ClaimedAt  string `json:"claimed_at"`
}

type completionRecord struct {
	Generation int64  `json:"generation"`
	SessionID  string `json:"session_id"`
	Outcome    string `json:"outcome"`
	RecordedAt string `json:"recorded_at"`
	RecordedBy string `json:"recorded_by"`
}

type storeEvent struct {
	TS     string `json:"ts"`
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

type beforeStore struct {
	Version    int               `json:"version"`
	WorkItem   string            `json:"work_item"`
	Status     string            `json:"status"`
	Claim      *claimRecord      `json:"claim,omitempty"`
	Completion *completionRecord `json:"completion,omitempty"`
	History    []storeEvent      `json:"history"`
}

func loadStore(path, workItem string) (*beforeStore, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &beforeStore{Version: 1, WorkItem: workItem, Status: "ready"}, nil
	}
	if err != nil {
		return nil, err
	}
	var s beforeStore
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("decode store: %w", err)
	}
	return &s, nil
}

func (s *beforeStore) save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *beforeStore) note(kind, detail string) {
	s.History = append(s.History, storeEvent{TS: stamp(), Kind: kind, Detail: detail})
}

// ------------------------------------------------------------- agent driving

type readyEvent struct {
	BeadID     string `json:"bead_id"`
	Generation int64  `json:"generation"`
	CityID     string `json:"city_id"`
	RunID      string `json:"run_id"`
}

type executionRequest struct {
	Event      readyEvent `json:"event"`
	ClaimToken string     `json:"claim_token"`
	SessionID  string     `json:"session_id"`
}

type protocolRequest struct {
	Operation string            `json:"operation"`
	Execution *executionRequest `json:"execution"`
}

type resultBody struct {
	SessionID string `json:"session_id"`
	Outcome   string `json:"outcome"`
}

type message struct {
	Type      string      `json:"type"`
	SessionID string      `json:"session_id,omitempty"`
	Result    *resultBody `json:"result,omitempty"`
}

type config struct {
	storePath  string
	agentBin   string
	agentHome  string
	workItem   string
	cityID     string
	runID      string
	tick       time.Duration
	staleAfter time.Duration
	g1Ticks    string
	gnTicks    string
}

func load() (config, error) {
	c := config{
		storePath:  os.Getenv("DEMO_STORE_PATH"),
		agentBin:   os.Getenv("DEMO_AGENT_BIN"),
		agentHome:  os.Getenv("AGENT_HOME"),
		workItem:   envOr("DEMO_WORK_ITEM", "work-item-1"),
		cityID:     envOr("DEMO_CITY_ID", "demo-city"),
		runID:      envOr("DEMO_RUN_ID", "before-temporal-demo"),
		tick:       envSeconds("BEFORE_TICK_SECONDS", 4),
		staleAfter: envSeconds("BEFORE_STALE_AFTER_SECONDS", 6),
		g1Ticks:    envOr("BEFORE_G1_TICKS", "40"),
		gnTicks:    envOr("BEFORE_GN_TICKS", "8"),
	}
	switch {
	case c.storePath == "":
		return c, errors.New("DEMO_STORE_PATH is required")
	case c.agentBin == "":
		return c, errors.New("DEMO_AGENT_BIN is required")
	case c.agentHome == "":
		return c, errors.New("AGENT_HOME is required")
	}
	return c, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envSeconds(key string, fallback int) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return time.Duration(fallback) * time.Second
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return time.Duration(fallback) * time.Second
	}
	return time.Duration(n) * time.Second
}

func stamp() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func say(format string, args ...any) {
	fmt.Printf("[before] "+format+"\n", args...)
}

// runAgent drives one fakeagent operation and returns the decoded messages.
// The child is its own process: if this coordinator dies, the child stays.
func runAgent(c config, generation int64, op string, ticks string) ([]message, error) {
	request := protocolRequest{
		Operation: op,
		Execution: &executionRequest{
			Event: readyEvent{
				BeadID:     c.workItem,
				Generation: generation,
				CityID:     c.cityID,
				RunID:      c.runID,
			},
		},
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(c.agentBin, op)
	cmd.Env = append(os.Environ(),
		"AGENT_HOME="+c.agentHome,
		"AGENT_EXECUTE_TICKS="+ticks,
		"AGENT_RESOLVE_DELAY_MS=0",
	)
	cmd.Stdin = strings.NewReader(string(payload))
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	var seen []message
	decoder := json.NewDecoder(out)
	for {
		var m message
		if err := decoder.Decode(&m); err != nil {
			break
		}
		seen = append(seen, m)
	}
	if err := cmd.Wait(); err != nil {
		return seen, fmt.Errorf("agent %s: %w", op, err)
	}
	return seen, nil
}

// --------------------------------------------------------------------- ticks

// recoveryScan records any terminal session that has no recorded completion.
// This is behaviour 3: it does not know or care which generation is current.
func recoveryScan(c config, s *beforeStore) (bool, error) {
	sessionsDir := filepath.Join(c.agentHome, "sessions")
	entries, err := os.ReadDir(sessionsDir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	changed := false
	for _, name := range names {
		prefix := c.workItem + "-g"
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		generation, err := strconv.ParseInt(strings.TrimPrefix(name, prefix), 10, 64)
		if err != nil {
			continue
		}
		terminalPath := filepath.Join(sessionsDir, name, "terminal.json")
		raw, err := os.ReadFile(terminalPath)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return changed, err
		}
		var terminal struct {
			SessionID string `json:"session_id"`
			Outcome   string `json:"outcome"`
		}
		if err := json.Unmarshal(raw, &terminal); err != nil {
			return changed, fmt.Errorf("decode terminal for %s: %w", name, err)
		}
		// The session-close path recorded each finished session once; the
		// marker is that memory. What it never checked is whether the session
		// being recorded belongs to the current generation.
		marker := filepath.Join(sessionsDir, name, "store-recorded.json")
		if _, err := os.Stat(marker); err == nil {
			continue
		}
		previous := "none"
		if s.Completion != nil {
			previous = fmt.Sprintf("generation %d by %s",
				s.Completion.Generation, s.Completion.SessionID)
		}
		s.Completion = &completionRecord{
			Generation: generation,
			SessionID:  terminal.SessionID,
			Outcome:    terminal.Outcome,
			RecordedAt: stamp(),
			RecordedBy: "recovery-scan",
		}
		s.Status = "completed"
		s.note("completion-recorded", fmt.Sprintf(
			"recovery scan recorded generation %d session %s over previous %s",
			generation, terminal.SessionID, previous))
		say("recovery scan found a finished session for generation %d and recorded it (previous receipt: %s)",
			generation, previous)
		if err := writeMarker(marker, generation); err != nil {
			return changed, err
		}
		changed = true
	}
	return changed, nil
}

// claimAndRun is the inline procedure: claim, launch, wait, record. Everything
// between the claim write and the completion write exists only in this
// process. A kill -9 anywhere in that window loses the procedure, never the
// claim.
func claimAndRun(c config, s *beforeStore, generation int64) error {
	s.Claim = &claimRecord{Generation: generation, ClaimedAt: stamp()}
	s.Status = "claimed"
	s.note("claimed", fmt.Sprintf("generation %d claimed; procedure held in process memory", generation))
	if err := s.save(c.storePath); err != nil {
		return err
	}
	say("claimed generation %d; launching the agent and waiting inline", generation)

	ticks := c.gnTicks
	if generation == 1 {
		ticks = c.g1Ticks
	}
	resolved, err := runAgent(c, generation, "resolve", ticks)
	if err != nil {
		return err
	}
	sessionID := ""
	for _, m := range resolved {
		if m.Type == "resolved" {
			sessionID = m.SessionID
		}
	}
	if sessionID == "" {
		return errors.New("agent resolve returned no session")
	}
	say("generation %d bound to session %s; agent working", generation, sessionID)

	messages, err := runAgent(c, generation, "execute", ticks)
	if err != nil {
		return err
	}
	for _, m := range messages {
		if m.Type != "result" || m.Result == nil {
			continue
		}
		s.Completion = &completionRecord{
			Generation: generation,
			SessionID:  m.Result.SessionID,
			Outcome:    m.Result.Outcome,
			RecordedAt: stamp(),
			RecordedBy: "inline-wait",
		}
		s.Status = "completed"
		s.note("completion-recorded", fmt.Sprintf(
			"inline wait recorded generation %d session %s",
			generation, m.Result.SessionID))
		say("generation %d finished; completion recorded inline", generation)
		marker := filepath.Join(c.agentHome, "sessions",
			fmt.Sprintf("%s-g%d", c.workItem, generation), "store-recorded.json")
		if err := writeMarker(marker, generation); err != nil {
			return err
		}
		return s.save(c.storePath)
	}
	return errors.New("agent execute ended without a result")
}

func writeMarker(path string, generation int64) error {
	raw, err := json.Marshal(map[string]any{
		"generation":  generation,
		"recorded_at": stamp(),
	})
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func tick(c config) error {
	s, err := loadStore(c.storePath, c.workItem)
	if err != nil {
		return err
	}

	scanChanged, err := recoveryScan(c, s)
	if err != nil {
		return err
	}
	if scanChanged {
		if err := s.save(c.storePath); err != nil {
			return err
		}
	}

	switch {
	case s.Claim == nil:
		say("tick: work item is ready and unclaimed")
		return claimAndRun(c, s, 1)
	case s.Status == "completed":
		say("tick: store says completed (receipt: generation %d); nothing to do",
			s.Completion.Generation)
		return nil
	default:
		claimedAt, err := time.Parse(time.RFC3339Nano, s.Claim.ClaimedAt)
		if err != nil {
			return fmt.Errorf("bad claim timestamp: %w", err)
		}
		age := time.Since(claimedAt)
		if age < c.staleAfter {
			say("tick: claim for generation %d is %ds old; assuming its procedure is still running",
				s.Claim.Generation, int(age.Seconds()))
			return nil
		}
		// Behaviour 2: the restarted loop cannot ask the dead procedure, so it
		// infers. The agent may be alive; the inference cannot see it.
		say("tick: claim for generation %d is %ds old with no procedure in this process; declaring it stale",
			s.Claim.Generation, int(age.Seconds()))
		s.note("claim-declared-stale", fmt.Sprintf(
			"generation %d declared stale after %ds; the running agent, if any, is invisible to this inference",
			s.Claim.Generation, int(age.Seconds())))
		return claimAndRun(c, s, s.Claim.Generation+1)
	}
}

func main() {
	c, err := load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "beforetick: %v\n", err)
		os.Exit(1)
	}
	say("reconcile loop starting: tick every %ds, claims stale after %ds",
		int(c.tick.Seconds()), int(c.staleAfter.Seconds()))
	for {
		if err := tick(c); err != nil {
			fmt.Fprintf(os.Stderr, "beforetick: tick failed: %v\n", err)
			os.Exit(1)
		}
		time.Sleep(c.tick)
	}
}
