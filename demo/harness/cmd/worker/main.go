// Command worker starts the reviewed WorkerSet and blocks.
//
// It registers nothing of its own. BeadOrchestrationWorkflow on the
// orchestration Task Queue and ExecuteBeadActivity on the agent Task Queue both
// come from the reviewed package; this process only supplies the two injectable
// dependencies (the file-backed store adapter and the command agent executor)
// and then gets out of the way. It is meant to be killed with kill -9 and
// replaced by a second copy of itself.
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/client"

	tb "github.com/sjarmak/gas-city/services/temporal-maintenance/internal/temporalbeads"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("worker: %v", err)
	}
}

func run() error {
	address := envOr("TEMPORAL_ADDRESS", "127.0.0.1:7233")
	namespace := envOr("TEMPORAL_NAMESPACE", "default")
	label := envOr("DEMO_WORKER_LABEL", "worker")
	storePath, err := requireEnv("DEMO_STORE_PATH")
	if err != nil {
		return err
	}
	agentBinary, err := requireEnv("DEMO_AGENT_BIN")
	if err != nil {
		return err
	}
	agentHome, err := requireEnv("AGENT_HOME")
	if err != nil {
		return err
	}

	store, err := tb.OpenFileBeadStore(storePath, tb.NewSystemClock())
	if err != nil {
		return fmt.Errorf("open work store: %w", err)
	}
	agent, err := tb.NewCommandAgentExecutor(tb.CommandAgentExecutorConfig{
		Executable:       agentBinary,
		WorkingDirectory: agentHome,
	})
	if err != nil {
		return fmt.Errorf("bind agent executor: %w", err)
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort:  address,
		Namespace: namespace,
	})
	if err != nil {
		return fmt.Errorf("dial temporal at %s: %w", address, err)
	}
	defer temporalClient.Close()

	workers, err := tb.NewWorkerSet(temporalClient, store, agent)
	if err != nil {
		return fmt.Errorf("build worker set: %w", err)
	}
	if err := workers.Start(); err != nil {
		return fmt.Errorf("start worker set: %w", err)
	}
	defer workers.Stop()

	log.Printf(
		"worker up: label=%s pid=%d address=%s queues=%s,%s",
		label, os.Getpid(), address,
		tb.OrchestrationTaskQueue, tb.AgentTaskQueue,
	)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	<-interrupt
	log.Printf("worker %s stopping on signal", label)
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func requireEnv(key string) (string, error) {
	value := os.Getenv(key)
	if value == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return value, nil
}
