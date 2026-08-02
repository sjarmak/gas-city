// Command drive starts one execution episode and then exits.
//
// Three calls, all into the reviewed package: transition one fixture work item
// to ready (which writes the outbox record), deliver that record through the
// real Signal-With-Start bridge, and seal the run so the Workflow closes once
// its one authoritative event reaches a terminal state.
//
// The heartbeat timeout is the one knob that matters here. The Activity's
// start-to-close timeout is 24 hours and is not adjustable from outside the
// package, so heartbeat timeout is what makes Temporal notice a dead Worker in
// seconds instead of a day. It is injected through the bridge's timing config.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"go.temporal.io/sdk/client"

	tb "github.com/sjarmak/gas-city/services/temporal-maintenance/internal/temporalbeads"
)

type driveOutput struct {
	WorkflowID       string `json:"workflow_id"`
	RunID            string `json:"run_id"`
	EventID          string `json:"event_id"`
	WorkItemID       string `json:"work_item_id"`
	Generation       int64  `json:"generation"`
	ActivityID       string `json:"activity_id"`
	HeartbeatTimeout string `json:"heartbeat_timeout"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("drive: %v", err)
	}
}

func run() error {
	address := envOr("TEMPORAL_ADDRESS", "127.0.0.1:7233")
	namespace := envOr("TEMPORAL_NAMESPACE", "default")
	cityID := envOr("DEMO_CITY_ID", "demo-city")
	runID := envOr("DEMO_RUN_ID", "worker-kill-demo")
	workItemID := envOr("DEMO_WORK_ITEM", "work-item-1")
	generation, err := strconv.ParseInt(envOr("DEMO_GENERATION", "1"), 10, 64)
	if err != nil {
		return fmt.Errorf("DEMO_GENERATION must be an integer: %w", err)
	}
	heartbeatSeconds, err := strconv.Atoi(envOr("DEMO_HEARTBEAT_SECONDS", "8"))
	if err != nil {
		return fmt.Errorf("DEMO_HEARTBEAT_SECONDS must be an integer: %w", err)
	}
	storePath := os.Getenv("DEMO_STORE_PATH")
	if storePath == "" {
		return fmt.Errorf("DEMO_STORE_PATH is required")
	}
	outputPath := os.Getenv("DEMO_DRIVE_OUT")
	if outputPath == "" {
		return fmt.Errorf("DEMO_DRIVE_OUT is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store, err := tb.OpenFileBeadStore(storePath, tb.NewSystemClock())
	if err != nil {
		return fmt.Errorf("open work store: %w", err)
	}
	formula := fixtureFormula()
	event, err := store.TransitionReady(
		ctx, cityID, runID, workItemID, generation, formula)
	if err != nil {
		return fmt.Errorf("transition fixture work item to ready: %w", err)
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort:  address,
		Namespace: namespace,
	})
	if err != nil {
		return fmt.Errorf("dial temporal at %s: %w", address, err)
	}
	defer temporalClient.Close()

	bridge := tb.ReadyEventBridge{
		Temporal: tb.TemporalClientGateway{Client: temporalClient},
		Acker:    store,
		Timing: tb.TimingConfig{
			HeartbeatTimeout:  time.Duration(heartbeatSeconds) * time.Second,
			ReconcileInterval: time.Minute,
			Clock:             tb.NewSystemClock(),
		},
	}
	receipt, err := bridge.Deliver(ctx, event)
	if err != nil {
		return fmt.Errorf("deliver ready event: %w", err)
	}
	if err := bridge.SealRun(ctx, tb.CloseRequest{
		ContractVersion:  tb.CurrentContractVersion,
		CityID:           cityID,
		RunID:            runID,
		BeadID:           workItemID,
		ExpectedEventIDs: []string{event.EventID},
		ReasonCode:       "demo-single-episode",
	}); err != nil {
		return fmt.Errorf("seal orchestration run: %w", err)
	}

	activityID, err := tb.FormulaActivityID(formula, generation)
	if err != nil {
		return fmt.Errorf("derive stable activity id: %w", err)
	}
	output := driveOutput{
		WorkflowID:       receipt.WorkflowID,
		RunID:            receipt.RunID,
		EventID:          receipt.EventID,
		WorkItemID:       workItemID,
		Generation:       generation,
		ActivityID:       activityID,
		HeartbeatTimeout: (time.Duration(heartbeatSeconds) * time.Second).String(),
	}
	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(outputPath, append(data, '\n'), 0o600); err != nil {
		return err
	}
	log.Printf("episode started: workflow=%s activity=%s heartbeat=%s",
		output.WorkflowID, output.ActivityID, output.HeartbeatTimeout)
	return nil
}

// fixtureFormula is a fully valid step identity. The Workflow requires one for
// any new run, and its root and step key become the Activity ID that has to
// stay stable across the kill.
func fixtureFormula() tb.FormulaRef {
	hash := sha256.Sum256([]byte("worker-kill-demo/edit-file/v1"))
	return tb.FormulaRef{
		Name:    "worker-kill-demo",
		Hash:    hex.EncodeToString(hash[:]),
		Version: "v1",
		RootID:  "demo-root",
		StepKey: "edit-file.author",
		Rig:     "demo",
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
