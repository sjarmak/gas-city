package temporalbeads

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	CoordinatorOutcomeTaskQueue    = "gascity-coordinator-outcomes"
	CoordinatorOutcomeWorkflowName = "CoordinatorOutcomeWorkflow"

	SignalOutcomeReady               = "coordinator.outcome-ready"
	SignalCoordinatorAcknowledged    = "coordinator.acknowledged"
	QueryCoordinatorOutcomeState     = "coordinator-outcome-state"
	defaultOutcomeRedeliveryPeriod   = time.Minute
	defaultOutcomeContinueAsNewAfter = 100
)

type CoordinatorOutcomeInput struct {
	Envelope           OutcomeReady             `json:"envelope"`
	RedeliveryInterval time.Duration            `json:"redelivery_interval"`
	ContinueAsNewAfter int                      `json:"continue_as_new_after,omitempty"`
	ResumeState        *CoordinatorOutcomeState `json:"resume_state,omitempty"`
}

// CoordinatorOutcomeState remains queryable at needs-coordinator-ack until the
// exact current delivery fence is explicitly acknowledged.
type CoordinatorOutcomeState struct {
	Envelope              OutcomeReady            `json:"envelope"`
	Phase                 OutcomeCoordinatorState `json:"phase"`
	DeliveryAttempts      int                     `json:"delivery_attempts"`
	DuplicateSignals      int                     `json:"duplicate_signals"`
	StaleAcknowledgements int                     `json:"stale_acknowledgements"`
	DeliveryRef           string                  `json:"delivery_ref,omitempty"`
	CoordinatorFence      string                  `json:"coordinator_fence,omitempty"`
	DeliveredAt           time.Time               `json:"delivered_at,omitempty"`
	AcknowledgedAt        time.Time               `json:"acknowledged_at,omitempty"`
	LastDeliveryError     string                  `json:"last_delivery_error,omitempty"`
	ContinueAsNewCount    int                     `json:"continue_as_new_count"`
}

func CoordinatorOutcomeWorkflowID(envelope OutcomeReady) (string, error) {
	if err := envelope.Validate(); err != nil {
		return "", err
	}
	return CoordinatorOutcomeWorkflowIDForOutcome(envelope.OutcomeID)
}

func CoordinatorOutcomeWorkflowIDForOutcome(outcomeID string) (string, error) {
	if err := validateSegment("coordinator outcome id", outcomeID); err != nil {
		return "", err
	}
	workflowID := "coordinator-outcome/" + outcomeID
	if err := validateWorkflowID("coordinator outcome workflow id", workflowID); err != nil {
		return "", err
	}
	return workflowID, nil
}

// CoordinatorOutcomeWorkflow owns retryable local delivery and the explicit
// acknowledgement gate. A coordinator restart changes the delivery fence on a
// later redelivery; stale acknowledgements cannot complete the Workflow.
func CoordinatorOutcomeWorkflow(
	ctx workflow.Context,
	input CoordinatorOutcomeInput,
) (CoordinatorOutcomeState, error) {
	if err := input.Envelope.Validate(); err != nil {
		return CoordinatorOutcomeState{}, temporal.NewNonRetryableApplicationError(
			"invalid coordinator outcome input",
			"InvalidOutcomeReady",
			err,
		)
	}
	if input.RedeliveryInterval <= 0 {
		input.RedeliveryInterval = defaultOutcomeRedeliveryPeriod
	}
	if input.ContinueAsNewAfter <= 0 {
		input.ContinueAsNewAfter = defaultOutcomeContinueAsNewAfter
	}
	state := CoordinatorOutcomeState{
		Envelope: cloneOutcomeReady(input.Envelope),
		Phase:    OutcomeCoordinatorPending,
	}
	if input.ResumeState != nil {
		if !outcomesEqual(input.Envelope, input.ResumeState.Envelope) {
			return CoordinatorOutcomeState{}, temporal.NewNonRetryableApplicationError(
				"continued coordinator outcome changed identity",
				"InvalidOutcomeContinuation",
				nil,
			)
		}
		state = *input.ResumeState
		state.Envelope = cloneOutcomeReady(input.ResumeState.Envelope)
	}
	if err := workflow.SetQueryHandler(
		ctx,
		QueryCoordinatorOutcomeState,
		func() (CoordinatorOutcomeState, error) { return state, nil },
	); err != nil {
		return state, err
	}
	readyCh := workflow.GetSignalChannel(ctx, SignalOutcomeReady)
	ackCh := workflow.GetSignalChannel(ctx, SignalCoordinatorAcknowledged)
	runDeliveryAttempts := 0
	if input.ResumeState != nil {
		for {
			var duplicate OutcomeReady
			if !readyCh.ReceiveAsync(&duplicate) {
				break
			}
			if outcomesEqual(state.Envelope, duplicate) {
				state.DuplicateSignals++
			} else {
				state.LastDeliveryError = "conflicting-outcome-ready-signal"
			}
		}
		for {
			var boundaryAck OutcomeAcknowledgement
			if !ackCh.ReceiveAsync(&boundaryAck) {
				break
			}
			if !outcomeAcknowledgementMatches(state, boundaryAck) {
				state.StaleAcknowledgements++
				continue
			}
			record, err := executeOutcomeAcknowledgement(
				ctx,
				boundaryAck,
				state.DeliveryAttempts,
			)
			if err != nil {
				state.LastDeliveryError = "canonical-acknowledgement-failed"
				break
			}
			state.Phase = OutcomeCoordinatorAcknowledged
			state.AcknowledgedAt = record.AcknowledgedAt
			if state.AcknowledgedAt.IsZero() {
				state.AcknowledgedAt = workflow.Now(ctx)
			}
			return state, nil
		}
	}

	for {
		state.DeliveryAttempts++
		runDeliveryAttempts++
		delivery, err := executeOutcomeDelivery(
			ctx,
			state.Envelope,
			state.DeliveryAttempts,
		)
		if err != nil {
			state.LastDeliveryError = "local-delivery-failed"
			if err := workflow.Sleep(ctx, input.RedeliveryInterval); err != nil {
				return state, err
			}
			if runDeliveryAttempts >= input.ContinueAsNewAfter {
				return continueCoordinatorOutcomeAsNew(ctx, input, state)
			}
			continue
		}
		state.Phase = OutcomeCoordinatorNeedsAck
		state.DeliveryRef = delivery.DeliveryRef
		state.CoordinatorFence = delivery.CoordinatorFence
		if state.DeliveredAt.IsZero() {
			state.DeliveredAt = workflow.Now(ctx)
		}
		state.LastDeliveryError = ""

		redeliver, acknowledgement, err := awaitOutcomeAcknowledgement(
			ctx,
			readyCh,
			ackCh,
			input.RedeliveryInterval,
			&state,
		)
		if err != nil {
			return state, err
		}
		if redeliver {
			if runDeliveryAttempts >= input.ContinueAsNewAfter {
				return continueCoordinatorOutcomeAsNew(ctx, input, state)
			}
			continue
		}
		record, err := executeOutcomeAcknowledgement(
			ctx,
			acknowledgement,
			state.DeliveryAttempts,
		)
		if err != nil {
			state.LastDeliveryError = "canonical-acknowledgement-failed"
			if err := workflow.Sleep(ctx, input.RedeliveryInterval); err != nil {
				return state, err
			}
			if runDeliveryAttempts >= input.ContinueAsNewAfter {
				return continueCoordinatorOutcomeAsNew(ctx, input, state)
			}
			continue
		}
		state.Phase = OutcomeCoordinatorAcknowledged
		state.AcknowledgedAt = record.AcknowledgedAt
		if state.AcknowledgedAt.IsZero() {
			state.AcknowledgedAt = workflow.Now(ctx)
		}
		return state, nil
	}
}

func continueCoordinatorOutcomeAsNew(
	ctx workflow.Context,
	input CoordinatorOutcomeInput,
	state CoordinatorOutcomeState,
) (CoordinatorOutcomeState, error) {
	state.ContinueAsNewCount++
	resume := state
	resume.Envelope = cloneOutcomeReady(state.Envelope)
	input.ResumeState = &resume
	return state, workflow.NewContinueAsNewError(
		ctx,
		CoordinatorOutcomeWorkflowName,
		input,
	)
}

func executeOutcomeDelivery(
	ctx workflow.Context,
	envelope OutcomeReady,
	attempt int,
) (OutcomeDelivery, error) {
	deliveryCycle := fmt.Sprintf("cycle-%06d", attempt)
	options := workflow.ActivityOptions{
		ActivityID:             fmt.Sprintf("deliver/%s/%s", envelope.OutcomeID, deliveryCycle),
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    5,
		},
	}
	var delivery OutcomeDelivery
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, options),
		DeliverCoordinatorOutcomeActivityName,
		OutcomeDeliveryRequest{
			Envelope: envelope, DeliveryCycle: deliveryCycle,
		},
	).Get(ctx, &delivery)
	if err != nil {
		return OutcomeDelivery{}, err
	}
	if err := validateOutcomeDelivery(delivery); err != nil {
		return OutcomeDelivery{}, temporal.NewNonRetryableApplicationError(
			"invalid coordinator delivery receipt",
			"InvalidOutcomeDelivery",
			err,
		)
	}
	if delivery.OutcomeID != envelope.OutcomeID ||
		delivery.WorkID != envelope.WorkID {
		return OutcomeDelivery{}, temporal.NewNonRetryableApplicationError(
			"coordinator delivery receipt changed outcome identity",
			"InvalidOutcomeDelivery",
			nil,
		)
	}
	return delivery, nil
}

func awaitOutcomeAcknowledgement(
	ctx workflow.Context,
	readyCh workflow.ReceiveChannel,
	ackCh workflow.ReceiveChannel,
	redeliveryInterval time.Duration,
	state *CoordinatorOutcomeState,
) (bool, OutcomeAcknowledgement, error) {
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	defer cancelTimer()
	timer := workflow.NewTimer(timerCtx, redeliveryInterval)
	for {
		var acknowledgement OutcomeAcknowledgement
		var ackReceived, redeliver bool
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(readyCh, func(channel workflow.ReceiveChannel, _ bool) {
			var duplicate OutcomeReady
			channel.Receive(ctx, &duplicate)
			if outcomesEqual(state.Envelope, duplicate) {
				state.DuplicateSignals++
				return
			}
			state.LastDeliveryError = "conflicting-outcome-ready-signal"
		})
		selector.AddReceive(ackCh, func(channel workflow.ReceiveChannel, _ bool) {
			channel.Receive(ctx, &acknowledgement)
			if !outcomeAcknowledgementMatches(*state, acknowledgement) {
				state.StaleAcknowledgements++
				return
			}
			ackReceived = true
		})
		selector.AddFuture(timer, func(workflow.Future) { redeliver = true })
		selector.Select(ctx)
		if ackReceived {
			cancelTimer()
			return false, acknowledgement, nil
		}
		if redeliver {
			for {
				var boundaryAck OutcomeAcknowledgement
				if !ackCh.ReceiveAsync(&boundaryAck) {
					break
				}
				if !outcomeAcknowledgementMatches(*state, boundaryAck) {
					state.StaleAcknowledgements++
					continue
				}
				cancelTimer()
				return false, boundaryAck, nil
			}
			for {
				var duplicate OutcomeReady
				if !readyCh.ReceiveAsync(&duplicate) {
					break
				}
				if outcomesEqual(state.Envelope, duplicate) {
					state.DuplicateSignals++
				} else {
					state.LastDeliveryError = "conflicting-outcome-ready-signal"
				}
			}
			return true, OutcomeAcknowledgement{}, nil
		}
	}
}

func outcomeAcknowledgementMatches(
	state CoordinatorOutcomeState,
	acknowledgement OutcomeAcknowledgement,
) bool {
	return validateOutcomeAcknowledgement(acknowledgement) == nil &&
		acknowledgement.StoreRef == state.Envelope.StoreRef &&
		acknowledgement.OutcomeID == state.Envelope.OutcomeID &&
		acknowledgement.WorkID == state.Envelope.WorkID &&
		acknowledgement.CoordinatorFence == state.CoordinatorFence
}

func executeOutcomeAcknowledgement(
	ctx workflow.Context,
	ack OutcomeAcknowledgement,
	deliveryCycle int,
) (OutcomeRecord, error) {
	ackCycle := fmt.Sprintf("cycle-%06d", deliveryCycle)
	options := workflow.ActivityOptions{
		ActivityID:             "ack/" + ack.OutcomeID + "/" + ackCycle,
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    5,
		},
	}
	var record OutcomeRecord
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, options),
		AcknowledgeCoordinatorOutcomeActivityName,
		ack,
	).Get(ctx, &record)
	return record, err
}
