// Command inspect collects the evidence for one arm of the demo and writes it
// as two JSON artifacts: receipt.json (what the work store ended up holding)
// and invariants.json (the facts verify.py asserts on).
//
// It gathers, it does not judge. The pass/fail decision lives in verify.py so
// that the gate is readable without a Go toolchain.
//
// One check does mutate state, deliberately: the stale-claim probe. It runs
// against a COPY of the store file so the archived evidence is never touched.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	tb "github.com/sjarmak/gas-city/services/temporal-maintenance/internal/temporalbeads"
)

type agentEvent struct {
	TS        string `json:"ts"`
	PID       int    `json:"pid"`
	Operation string `json:"operation"`
	Kind      string `json:"kind"`
	Key       string `json:"key"`
	SessionID string `json:"session_id"`
}

type workEntry struct {
	TS             string `json:"ts"`
	PID            int    `json:"pid"`
	SessionID      string `json:"session_id"`
	Sequence       int64  `json:"sequence"`
	AfterPipeBreak bool   `json:"after_pipe_break"`
}

type killMarker struct {
	KilledAt string `json:"killed_at"`
}

type receipt struct {
	WorkItemID   string            `json:"work_item_id"`
	Generation   int64             `json:"generation"`
	Status       string            `json:"status"`
	Outcome      string            `json:"outcome"`
	SessionID    string            `json:"session_id"`
	WorkflowID   string            `json:"workflow_id"`
	ArtifactRefs []tb.ArtifactRef  `json:"artifact_refs"`
	Attempts     []attemptFailure  `json:"recorded_attempt_failures"`
}

type attemptFailure struct {
	Attempt int32  `json:"attempt"`
	Code    string `json:"code"`
}

type orphanEvidence struct {
	KillRecorded             bool `json:"kill_recorded"`
	WorkEntriesTotal         int  `json:"work_entries_total"`
	WorkEntriesAfterKill     int  `json:"work_entries_after_kill"`
	WorkEntriesAfterPipeBrk  int  `json:"work_entries_after_pipe_break"`
	DistinctWorkingProcesses int  `json:"distinct_working_processes"`
	FirstWorkerKeptWorking   bool `json:"first_agent_process_kept_working_after_kill"`
}

type invariants struct {
	Arm                    string         `json:"arm"`
	Description            string         `json:"description"`
	WorkItemID             string         `json:"work_item_id"`
	Generation             int64          `json:"generation"`
	WorkflowID             string         `json:"workflow_id"`
	WorkflowStatus         string         `json:"workflow_status"`
	ActivityIDs            []string       `json:"activity_ids_in_history"`
	ActivityAttempts       int32          `json:"activity_attempts"`
	ResolveCalls           int            `json:"resolve_calls"`
	SessionsCreated        int            `json:"sessions_created"`
	SessionsBound          []string       `json:"sessions_bound"`
	SessionsOnDisk         int            `json:"session_records_on_disk"`
	AgentTerminalRecords   int            `json:"agent_terminal_records"`
	StoreStatus            string         `json:"store_status"`
	StoreSessionID         string         `json:"store_session_id"`
	StoreOutcome           string         `json:"store_outcome"`
	StoreTerminalReceipts  int            `json:"store_terminal_receipts"`
	StaleTokenRejected     bool           `json:"stale_token_rejected"`
	StaleTokenError        string         `json:"stale_token_error"`
	Orphan                 orphanEvidence `json:"orphan_evidence"`
	BuiltAgainstRevision   string         `json:"built_against_revision"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("inspect: %v", err)
	}
}

func run() error {
	arm := requireEnv("DEMO_ARM")
	description := os.Getenv("DEMO_ARM_DESCRIPTION")
	storePath := requireEnv("DEMO_STORE_PATH")
	agentHome := requireEnv("AGENT_HOME")
	outputDir := requireEnv("DEMO_ARTIFACT_DIR")
	workItemID := envOr("DEMO_WORK_ITEM", "work-item-1")
	address := envOr("TEMPORAL_ADDRESS", "127.0.0.1:7233")
	namespace := envOr("TEMPORAL_NAMESPACE", "default")
	revision := os.Getenv("DEMO_SERVICE_REVISION")

	var driven struct {
		WorkflowID string `json:"workflow_id"`
		RunID      string `json:"run_id"`
		Generation int64  `json:"generation"`
	}
	if err := readJSON(
		filepath.Join(outputDir, "episode.json"),
		&driven,
	); err != nil {
		return fmt.Errorf("read episode identity: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store, err := tb.OpenFileBeadStore(storePath, tb.NewSystemClock())
	if err != nil {
		return fmt.Errorf("open work store: %w", err)
	}
	record, err := store.Inspect(ctx, workItemID)
	if err != nil {
		return fmt.Errorf("inspect work item: %w", err)
	}

	events, err := readAgentEvents(filepath.Join(agentHome, "agent-events.jsonl"))
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s-g%d", workItemID, record.Generation)
	resolveCalls, sessionsCreated, bound := summarizeEvents(events, key)

	sessionsOnDisk, terminalRecords, err := summarizeSessions(
		filepath.Join(agentHome, "sessions"), key)
	if err != nil {
		return err
	}
	orphan, err := summarizeOrphan(
		filepath.Join(agentHome, "sessions", key, "worklog.jsonl"),
		filepath.Join(outputDir, "kill.json"),
	)
	if err != nil {
		return err
	}

	activityIDs, attempts, status, err := readTemporalEvidence(
		ctx, address, namespace, driven.WorkflowID, driven.RunID)
	if err != nil {
		return err
	}

	staleRejected, staleError, err := probeStaleClaim(
		ctx, storePath, filepath.Join(outputDir, "stale-probe"), record)
	if err != nil {
		return fmt.Errorf("stale claim probe: %w", err)
	}

	terminalReceipts := 0
	if record.Status == tb.BeadStatusCompleted &&
		record.Outcome != "" &&
		record.SessionID != "" {
		terminalReceipts = 1
	}

	out := invariants{
		Arm:                   arm,
		Description:           description,
		WorkItemID:            workItemID,
		Generation:            record.Generation,
		WorkflowID:            driven.WorkflowID,
		WorkflowStatus:        status,
		ActivityIDs:           activityIDs,
		ActivityAttempts:      attempts,
		ResolveCalls:          resolveCalls,
		SessionsCreated:       sessionsCreated,
		SessionsBound:         bound,
		SessionsOnDisk:        sessionsOnDisk,
		AgentTerminalRecords:  terminalRecords,
		StoreStatus:           string(record.Status),
		StoreSessionID:        record.SessionID,
		StoreOutcome:          string(record.Outcome),
		StoreTerminalReceipts: terminalReceipts,
		StaleTokenRejected:    staleRejected,
		StaleTokenError:       staleError,
		Orphan:                orphan,
		BuiltAgainstRevision:  revision,
	}
	if err := writeJSON(filepath.Join(outputDir, "invariants.json"), out); err != nil {
		return err
	}

	failures := make([]attemptFailure, 0, len(record.AttemptFailure))
	for _, failure := range record.AttemptFailure {
		failures = append(failures, attemptFailure{
			Attempt: failure.Attempt,
			Code:    failure.Code,
		})
	}
	return writeJSON(filepath.Join(outputDir, "receipt.json"), receipt{
		WorkItemID:   record.BeadID,
		Generation:   record.Generation,
		Status:       string(record.Status),
		Outcome:      string(record.Outcome),
		SessionID:    record.SessionID,
		WorkflowID:   record.WorkflowID,
		ArtifactRefs: record.ArtifactRefs,
		Attempts:     failures,
	})
}

// ------------------------------------------------------------- agent evidence

func readAgentEvents(path string) ([]agentEvent, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("agent event log %s was never written", path)
	}
	if err != nil {
		return nil, err
	}
	var events []agentEvent
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event agentEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, fmt.Errorf("decode agent event: %w", err)
		}
		events = append(events, event)
	}
	return events, nil
}

func summarizeEvents(
	events []agentEvent,
	key string,
) (resolveCalls int, created int, bound []string) {
	seen := make(map[string]struct{})
	for _, event := range events {
		if event.Key != key {
			continue
		}
		if event.Operation == "resolve" {
			resolveCalls++
		}
		if event.Kind == "session-created" {
			created++
		}
		if event.SessionID != "" {
			seen[event.SessionID] = struct{}{}
		}
	}
	for sessionID := range seen {
		bound = append(bound, sessionID)
	}
	sort.Strings(bound)
	return resolveCalls, created, bound
}

func summarizeSessions(root, key string) (int, int, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, fmt.Errorf("agent session registry %s was never written", root)
	}
	if err != nil {
		return 0, 0, err
	}
	sessions, terminals := 0, 0
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() != key {
			continue
		}
		sessions++
		if _, err := os.Stat(
			filepath.Join(root, entry.Name(), "terminal.json"),
		); err == nil {
			terminals++
		}
	}
	return sessions, terminals, nil
}

func summarizeOrphan(worklogPath, killPath string) (orphanEvidence, error) {
	evidence := orphanEvidence{}
	data, err := os.ReadFile(worklogPath)
	if errors.Is(err, os.ErrNotExist) {
		return evidence, nil
	}
	if err != nil {
		return evidence, err
	}
	var entries []workEntry
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry workEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return evidence, fmt.Errorf("decode agent work entry: %w", err)
		}
		entries = append(entries, entry)
	}
	evidence.WorkEntriesTotal = len(entries)
	processes := make(map[int]struct{})
	for _, entry := range entries {
		processes[entry.PID] = struct{}{}
		if entry.AfterPipeBreak {
			evidence.WorkEntriesAfterPipeBrk++
		}
	}
	evidence.DistinctWorkingProcesses = len(processes)

	var marker killMarker
	if err := readJSON(killPath, &marker); err != nil {
		return evidence, nil
	}
	killedAt, err := time.Parse(time.RFC3339Nano, marker.KilledAt)
	if err != nil {
		return evidence, nil
	}
	evidence.KillRecorded = true
	if len(entries) == 0 {
		return evidence, nil
	}
	firstProcess := entries[0].PID
	for _, entry := range entries {
		stamp, err := time.Parse(time.RFC3339Nano, entry.TS)
		if err != nil {
			continue
		}
		if stamp.After(killedAt) {
			evidence.WorkEntriesAfterKill++
			if entry.PID == firstProcess {
				evidence.FirstWorkerKeptWorking = true
			}
		}
	}
	return evidence, nil
}

// ---------------------------------------------------------- temporal evidence

func readTemporalEvidence(
	ctx context.Context,
	address string,
	namespace string,
	workflowID string,
	runID string,
) ([]string, int32, string, error) {
	temporalClient, err := client.Dial(client.Options{
		HostPort:  address,
		Namespace: namespace,
	})
	if err != nil {
		return nil, 0, "", fmt.Errorf("dial temporal at %s: %w", address, err)
	}
	defer temporalClient.Close()

	described, err := temporalClient.DescribeWorkflowExecution(
		ctx, workflowID, runID)
	if err != nil {
		return nil, 0, "", fmt.Errorf("describe workflow: %w", err)
	}
	status := described.GetWorkflowExecutionInfo().GetStatus().String()

	activityIDs := make([]string, 0, 1)
	seen := make(map[string]struct{})
	var attempts int32
	iterator := temporalClient.GetWorkflowHistory(
		ctx, workflowID, runID, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, 0, "", fmt.Errorf("read workflow history: %w", err)
		}
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			id := event.GetActivityTaskScheduledEventAttributes().GetActivityId()
			if _, exists := seen[id]; !exists {
				seen[id] = struct{}{}
				activityIDs = append(activityIDs, id)
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			if attempt := event.
				GetActivityTaskStartedEventAttributes().
				GetAttempt(); attempt > attempts {
				attempts = attempt
			}
		}
	}
	for _, pending := range described.GetPendingActivities() {
		if pending.GetAttempt() > attempts {
			attempts = pending.GetAttempt()
		}
	}
	sort.Strings(activityIDs)
	return activityIDs, attempts, status, nil
}

// -------------------------------------------------------------- fence evidence

// probeStaleClaim advances the generation on a COPY of the store and then
// replays the completion the finished attempt already used. A correct fence
// rejects it. This is the "operator advances the generation, a late completion
// from the dead attempt arrives" case, run for real against the real store code.
func probeStaleClaim(
	ctx context.Context,
	storePath string,
	probeDir string,
	record tb.BeadRecord,
) (bool, string, error) {
	if record.ClaimToken == "" {
		return false, "work item never carried a claim token", nil
	}
	if err := os.MkdirAll(probeDir, 0o700); err != nil {
		return false, "", err
	}
	data, err := os.ReadFile(storePath)
	if err != nil {
		return false, "", err
	}
	probePath := filepath.Join(probeDir, "store-copy.json")
	if err := os.WriteFile(probePath, data, 0o600); err != nil {
		return false, "", err
	}
	probe, err := tb.OpenFileBeadStore(probePath, tb.NewSystemClock())
	if err != nil {
		return false, "", err
	}
	if _, err := probe.TransitionReady(
		ctx,
		record.CityID,
		record.RunID,
		record.BeadID,
		record.Generation+1,
		record.Formula,
	); err != nil {
		return false, "", fmt.Errorf("advance generation on the probe copy: %w", err)
	}
	err = probe.Complete(ctx, tb.Completion{
		BeadID:     record.BeadID,
		Generation: record.Generation,
		ClaimToken: record.ClaimToken,
		SessionID:  record.SessionID,
		Outcome:    tb.OutcomeCompleted,
	})
	if errors.Is(err, tb.ErrStaleFence) {
		return true, err.Error(), nil
	}
	if err == nil {
		return false, "stale completion was accepted", nil
	}
	return false, err.Error(), nil
}

// ---------------------------------------------------------------------- utils

func readJSON(path string, destination any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, destination)
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("inspect: %s is required", key)
	}
	return value
}
