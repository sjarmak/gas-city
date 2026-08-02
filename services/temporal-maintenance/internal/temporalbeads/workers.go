package temporalbeads

import (
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// WorkerSet registers deterministic orchestration and nondeterministic agent
// execution on separate Task Queues.
type WorkerSet struct {
	mu            sync.Mutex
	orchestration worker.Worker
	agent         worker.Worker
	started       bool
	stopped       bool
}

// NewWorkerSet constructs both workers without starting external processes.
func NewWorkerSet(
	temporalClient client.Client,
	beads BeadStore,
	agent AgentExecutor,
) (*WorkerSet, error) {
	if temporalClient == nil {
		return nil, fmt.Errorf("temporal client is required")
	}
	if beads == nil {
		return nil, fmt.Errorf("Beads store is required")
	}
	if agent == nil {
		return nil, fmt.Errorf("agent executor is required")
	}
	activities := &ActivityWorker{Beads: beads, Agent: agent}
	return newWorkerSet(temporalClient, activities.ExecuteBead)
}

// NewShadowWorkerSet polls the production Task Queues while rejecting every
// agent Activity before it can touch Beads or dispatch an agent.
func NewShadowWorkerSet(temporalClient client.Client) (*WorkerSet, error) {
	if temporalClient == nil {
		return nil, fmt.Errorf("temporal client is required")
	}
	activities := &ShadowActivityWorker{}
	return newWorkerSet(temporalClient, activities.ExecuteBead)
}

func newWorkerSet(
	temporalClient client.Client,
	executeBead interface{},
) (*WorkerSet, error) {
	orchestrationWorker := worker.New(
		temporalClient,
		OrchestrationTaskQueue,
		worker.Options{},
	)
	orchestrationWorker.RegisterWorkflowWithOptions(
		BeadOrchestrationWorkflow,
		workflow.RegisterOptions{Name: BeadOrchestrationWorkflowName},
	)
	agentWorker := worker.New(temporalClient, AgentTaskQueue, worker.Options{})
	agentWorker.RegisterActivityWithOptions(
		executeBead,
		activity.RegisterOptions{Name: ExecuteBeadActivityName},
	)
	return &WorkerSet{
		orchestration: orchestrationWorker,
		agent:         agentWorker,
	}, nil
}

// Start begins both pollers and rolls back if the second start fails.
func (s *WorkerSet) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	if s.stopped {
		return fmt.Errorf("worker set has been stopped and cannot be restarted")
	}
	if err := s.orchestration.Start(); err != nil {
		return fmt.Errorf("start orchestration worker: %w", err)
	}
	if err := s.agent.Start(); err != nil {
		s.orchestration.Stop()
		s.stopped = true
		return fmt.Errorf("start agent worker: %w", err)
	}
	s.started = true
	return nil
}

// Stop stops both Task Queue pollers.
func (s *WorkerSet) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	if !s.started {
		return
	}
	s.agent.Stop()
	s.orchestration.Stop()
	s.started = false
}
