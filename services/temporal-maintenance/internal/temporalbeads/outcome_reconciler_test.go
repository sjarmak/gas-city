package temporalbeads

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func TestOutcomeReconcilerSignalWithStartsEachPendingEnvelope(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	source := &recordingOutcomeStore{
		pending: []OutcomeRecord{{
			Envelope: envelope,
			State:    OutcomeCoordinatorPending,
		}},
	}
	gateway := &recordingOutcomeGateway{}
	reconciler := CoordinatorOutcomeReconciler{
		Source: source,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}

	result, err := reconciler.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, OutcomeReconcileResult{Scanned: 1, Signalled: 1}, result)
	require.Equal(t, []OutcomeReady{envelope}, gateway.envelopes)
}

func TestOutcomeReconcilerTransportFailureLeavesOutboxPending(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	source := &recordingOutcomeStore{
		pending: []OutcomeRecord{{Envelope: envelope, State: OutcomeCoordinatorPending}},
	}
	gateway := &recordingOutcomeGateway{err: errors.New("temporal unavailable")}
	reconciler := CoordinatorOutcomeReconciler{
		Source: source,
		Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
	}

	_, err := reconciler.Reconcile(context.Background())
	require.ErrorContains(t, err, "temporal unavailable")
	require.Len(t, source.pending, 1)
}

func TestOutcomeReconcilerAvoidsDuplicateSignalsAndRecoversMissingWorkflow(
	t *testing.T,
) {
	envelope := validOutcomeEnvelope(t)
	for _, test := range []struct {
		name          string
		workflowAlive bool
		wantSignals   int
	}{
		{name: "running workflow", workflowAlive: true, wantSignals: 0},
		{name: "missing workflow", workflowAlive: false, wantSignals: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := &recordingOutcomeStore{
				pending: []OutcomeRecord{{
					Envelope: envelope,
					State:    OutcomeCoordinatorNeedsAck,
				}},
			}
			gateway := &recordingOutcomeGateway{
				workflowAlive: test.workflowAlive,
			}
			reconciler := CoordinatorOutcomeReconciler{
				Source: source,
				Bridge: CoordinatorOutcomeBridge{Temporal: gateway},
			}

			result, err := reconciler.Reconcile(context.Background())
			require.NoError(t, err)
			require.Equal(t, test.wantSignals, result.Signalled)
			require.Len(t, gateway.envelopes, test.wantSignals)
		})
	}
}

func TestOutcomeReconcilerIsolatesPerCandidateFailures(t *testing.T) {
	firstInput := validOutcomeAdapterInput()
	firstInput.WorkID = "dr-candidate-a"
	firstInput.Fence.Token = "claim-token-a"
	first, err := FormulaStepOutcome(firstInput)
	require.NoError(t, err)
	secondInput := validOutcomeAdapterInput()
	secondInput.WorkID = "dr-candidate-b"
	secondInput.Fence.Token = "claim-token-b"
	second, err := FormulaStepOutcome(secondInput)
	require.NoError(t, err)

	tests := []struct {
		name       string
		source     *recordingOutcomeStore
		gateway    *recordingOutcomeGateway
		wantMade   []string
		wantSent   []string
		wantResult OutcomeReconcileResult
	}{
		{
			name: "emit failure",
			source: &recordingOutcomeStore{
				discovered: []OutcomeReady{first, second},
				emitErrors: map[string]error{
					first.WorkID: errors.New("injected canonical write failure"),
				},
			},
			gateway:  &recordingOutcomeGateway{},
			wantMade: []string{second.WorkID},
			wantSent: []string{second.WorkID},
			wantResult: OutcomeReconcileResult{
				Produced: 1, Scanned: 1, Signalled: 1,
			},
		},
		{
			name: "signal-with-start failure",
			source: &recordingOutcomeStore{
				discovered: []OutcomeReady{first, second},
			},
			gateway: &recordingOutcomeGateway{
				errorsByWork: map[string]error{
					first.WorkID: errors.New("injected Signal-With-Start failure"),
				},
			},
			wantMade: []string{first.WorkID, second.WorkID},
			wantSent: []string{first.WorkID, second.WorkID},
			wantResult: OutcomeReconcileResult{
				Produced: 2, Scanned: 2, Signalled: 1,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reconciler := CoordinatorOutcomeReconciler{
				Source: test.source,
				Bridge: CoordinatorOutcomeBridge{Temporal: test.gateway},
			}
			result, err := reconciler.Reconcile(context.Background())
			require.Error(t, err)
			require.ErrorContains(t, err, first.WorkID)
			require.Equal(t, test.wantResult, result)
			require.Equal(t, test.wantMade, test.source.emitted)
			sent := make([]string, 0, len(test.gateway.envelopes))
			for _, envelope := range test.gateway.envelopes {
				sent = append(sent, envelope.WorkID)
			}
			require.Equal(t, test.wantSent, sent)
		})
	}
}

func TestCoordinatorOutcomeGatewayStartsWithExactCorrelationMemo(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	envelope.WorkflowID = "bead-orchestration/city/run/dr-result"
	envelope.WorkflowRunID = "source-run"
	transport := &recordingTemporalClient{}
	queue, err := CoordinatorOutcomeTaskQueueForStore(envelope.StoreRef)
	require.NoError(t, err)
	gateway := CoordinatorOutcomeTemporalGateway{
		Client: transport, TaskQueue: queue, RedeliveryInterval: time.Minute,
	}

	_, err = gateway.SignalWithStartOutcome(context.Background(), envelope)
	require.NoError(t, err)
	require.Equal(t, queue, transport.startOptions.TaskQueue)
	require.Equal(t, map[string]interface{}{
		"GasCityOutcomeStoreRef":     envelope.StoreRef,
		"GasCityOutcomeWorkID":       envelope.WorkID,
		"GasCityOutcomeSourceRootID": envelope.SourceRootID,
		"GasCityOutcomeProducer":     string(envelope.Producer),
		"GasCityOutcomeStepKey":      envelope.StepKey,
		"GasCityOutcomeID":           envelope.OutcomeID,
		"GasCitySourceWorkflowID":    envelope.WorkflowID,
		"GasCitySourceWorkflowRunID": envelope.WorkflowRunID,
	}, transport.startOptions.Memo)
}

func TestCoordinatorOutcomeGatewayRecoversOnlyMissingOrFailedWorkflow(t *testing.T) {
	envelope := validOutcomeEnvelope(t)
	for _, test := range []struct {
		name        string
		status      enumspb.WorkflowExecutionStatus
		describeErr error
		wantStarted bool
	}{
		{
			name:   "running",
			status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		{
			name:        "failed",
			status:      enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			wantStarted: true,
		},
		{
			name:        "missing",
			describeErr: serviceerror.NewNotFound("missing workflow"),
			wantStarted: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTemporalClient{describeErr: test.describeErr}
			if test.describeErr == nil {
				transport.describeResponse =
					&workflowservice.DescribeWorkflowExecutionResponse{
						WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
							Status: test.status,
						},
					}
			}
			bridge := CoordinatorOutcomeBridge{
				Temporal: CoordinatorOutcomeTemporalGateway{
					Client: transport,
				},
			}
			_, started, err := bridge.Ensure(context.Background(), envelope)
			require.NoError(t, err)
			require.Equal(t, test.wantStarted, started)
			require.Equal(t, 1, transport.describeCalls)
			if test.wantStarted {
				require.Equal(t, 1, transport.signalStartCalls)
			} else {
				require.Zero(t, transport.signalStartCalls)
			}
		})
	}
}

type recordingOutcomeGateway struct {
	envelopes     []OutcomeReady
	err           error
	errorsByWork  map[string]error
	workflowAlive bool
}

func (g *recordingOutcomeGateway) SignalWithStartOutcome(
	_ context.Context,
	envelope OutcomeReady,
) (WorkflowReceipt, error) {
	g.envelopes = append(g.envelopes, envelope)
	if err := g.errorsByWork[envelope.WorkID]; err != nil {
		return WorkflowReceipt{}, err
	}
	if g.err != nil {
		return WorkflowReceipt{}, g.err
	}
	workflowID, err := CoordinatorOutcomeWorkflowID(envelope)
	if err != nil {
		return WorkflowReceipt{}, err
	}
	g.workflowAlive = true
	return WorkflowReceipt{WorkflowID: workflowID, RunID: "outcome-run"}, nil
}

func (g *recordingOutcomeGateway) OutcomeWorkflowRunning(
	context.Context,
	OutcomeReady,
) (bool, error) {
	return g.workflowAlive, nil
}
