package temporalbeads

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

func TestParseDeploymentModeIsExplicitAndFailClosed(t *testing.T) {
	mode, err := ParseDeploymentMode("shadow")
	require.NoError(t, err)
	require.Equal(t, DeploymentModeShadow, mode)

	mode, err = ParseDeploymentMode("canary")
	require.NoError(t, err)
	require.Equal(t, DeploymentModeCanary, mode)

	_, err = ParseDeploymentMode("")
	require.ErrorContains(t, err, "required")
	_, err = ParseDeploymentMode("live")
	require.ErrorContains(t, err, "shadow or canary")
}

func TestShadowActivityRejectsBeforeBeadsOrAgentExecution(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	shadow := &ShadowActivityWorker{}
	env.RegisterActivityWithOptions(
		shadow.ExecuteBead,
		activity.RegisterOptions{Name: ExecuteBeadActivityName},
	)

	_, err := env.ExecuteActivity(
		shadow.ExecuteBead,
		ActivityInput{Event: validReadyEvent(t)},
	)
	require.ErrorContains(t, err, "shadow mode")
}

func TestManagedRuntimeConfigRequiresProductionDependenciesOnlyForCanary(t *testing.T) {
	require.Error(t, ValidateManagedRuntimeConfig(ManagedRuntimeConfig{
		Mode: DeploymentMode("live"),
	}))
	require.NoError(t, ValidateManagedRuntimeConfig(ManagedRuntimeConfig{
		Mode: DeploymentModeShadow,
	}))

	err := ValidateManagedRuntimeConfig(ManagedRuntimeConfig{
		Mode: DeploymentModeCanary,
	})
	require.ErrorContains(t, err, "canonical Beads store")

	err = ValidateManagedRuntimeConfig(ManagedRuntimeConfig{
		Mode:  DeploymentModeCanary,
		Store: &trapCanonicalStore{},
	})
	require.ErrorContains(t, err, "agent executor")

	err = ValidateManagedRuntimeConfig(ManagedRuntimeConfig{
		Mode:  DeploymentModeCanary,
		Store: &trapCanonicalStore{},
		Agent: &trapAgentExecutor{},
	})
	require.ErrorContains(t, err, "search attributes")

	require.NoError(t, ValidateManagedRuntimeConfig(ManagedRuntimeConfig{
		Mode:                       DeploymentModeCanary,
		Store:                      &trapCanonicalStore{},
		Agent:                      &trapAgentExecutor{},
		SearchAttributesRegistered: true,
	}))

	err = ValidateManagedRuntimeConfig(ManagedRuntimeConfig{
		Mode:                       DeploymentModeCanary,
		Store:                      &trapCanonicalStore{},
		Agent:                      &trapAgentExecutor{},
		SearchAttributesRegistered: true,
		Timing: TimingConfig{
			HeartbeatTimeout: time.Second,
		},
	})
	require.ErrorContains(t, err, "canary timing")
}

func TestManagedRuntimeShadowModeAndFailClosedAcker(t *testing.T) {
	source := shadowReadyEventSource{}
	require.ErrorContains(
		t,
		source.AcknowledgeReadyEvent(context.Background(), "evt-1"),
		"cannot acknowledge",
	)

	runtime := &ManagedRuntime{mode: DeploymentModeShadow}
	require.Equal(t, DeploymentModeShadow, runtime.Mode())
}

func TestManagedRuntimeConstructionIsModeGated(t *testing.T) {
	_, err := NewManagedRuntime(nil, ManagedRuntimeConfig{
		Mode: DeploymentModeShadow,
	})
	require.ErrorContains(t, err, "temporal client")
}

func TestIntegration_ManagedShadowPollsBothQueuesWithoutStoreOrAgentCalls(t *testing.T) {
	binary, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("temporal CLI is required for the managed shadow integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: binary,
		LogLevel:     "error",
	})
	require.NoError(t, err)
	defer func() { _ = server.Stop() }()
	temporalClient := server.Client()
	defer temporalClient.Close()

	runtime, err := NewManagedRuntime(temporalClient, ManagedRuntimeConfig{
		Mode:  DeploymentModeShadow,
		Store: &trapCanonicalStore{},
		Agent: &trapAgentExecutor{},
	})
	require.NoError(t, err)
	require.NoError(t, runtime.Start(ctx))
	defer runtime.Stop()
	canary, err := NewManagedRuntime(temporalClient, ManagedRuntimeConfig{
		Mode:                       DeploymentModeCanary,
		Store:                      &trapCanonicalStore{},
		Agent:                      &trapAgentExecutor{},
		SearchAttributesRegistered: true,
	})
	require.NoError(t, err)
	require.Equal(t, DeploymentModeCanary, canary.Mode())
	canary.Stop()

	event := validReadyEvent(t)
	run, err := temporalClient.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			ID:        "managed-shadow-queues",
			TaskQueue: OrchestrationTaskQueue,
		},
		BeadOrchestrationWorkflowName,
		WorkflowInput{
			ContractVersion:  CurrentContractVersion,
			CityID:           event.CityID,
			RunID:            event.RunID,
			BeadID:           event.BeadID,
			InitialReady:     []ReadyEvent{event},
			CloseWhenIdle:    true,
			HeartbeatTimeout: time.Second,
			EventLimit:       DefaultEventLimit,
		},
	)
	require.NoError(t, err)
	err = run.Get(ctx, nil)
	require.ErrorContains(t, err, "activity-failed")
}

type trapCanonicalStore struct{}

func (*trapCanonicalStore) Claim(context.Context, ClaimRequest) (ClaimLease, error) {
	panic("shadow mode reached Claim")
}

func (*trapCanonicalStore) Complete(context.Context, Completion) error {
	panic("shadow mode reached Complete")
}

func (*trapCanonicalStore) RecordAttemptFailure(context.Context, AttemptFailure) error {
	panic("shadow mode reached RecordAttemptFailure")
}

func (*trapCanonicalStore) Inspect(context.Context, string) (BeadRecord, error) {
	panic("shadow mode reached Inspect")
}

func (*trapCanonicalStore) PendingReadyEvents(context.Context) ([]ReadyEvent, error) {
	panic("shadow mode reached PendingReadyEvents")
}

func (*trapCanonicalStore) AcknowledgeReadyEvent(context.Context, string) error {
	panic("shadow mode reached AcknowledgeReadyEvent")
}

type trapAgentExecutor struct{}

func (*trapAgentExecutor) ResolveSession(
	context.Context,
	AgentExecutionRequest,
) (string, error) {
	panic("shadow mode reached ResolveSession")
}

func (*trapAgentExecutor) Execute(
	context.Context,
	AgentExecutionRequest,
	func(AgentProgress) error,
) (AgentExecutionResult, error) {
	panic("shadow mode reached Execute")
}

func (*trapAgentExecutor) Cancel(context.Context, AgentCancellation) error {
	panic("shadow mode reached Cancel")
}
