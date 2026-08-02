package temporalbeads

import (
	"context"
	"fmt"
)

const (
	DeliverCoordinatorOutcomeActivityName     = "DeliverCoordinatorOutcomeActivity"
	AcknowledgeCoordinatorOutcomeActivityName = "AcknowledgeCoordinatorOutcomeActivity"
)

// OutcomeNotificationReceipt is returned by a local-only delivery adapter.
// DeliveryRef is an idempotent local reference; CoordinatorFence is the exact
// mayor/coordinator session that received it.
type OutcomeNotificationReceipt struct {
	DeliveryRef      string `json:"delivery_ref"`
	CoordinatorFence string `json:"coordinator_fence"`
}

// OutcomeDeliveryRequest distinguishes a retry of one Temporal Activity from a
// later Workflow redelivery cycle. The notifier reuses a durable delivery for
// the same outcome/coordinator fence; a changed fence receives a fresh wake.
type OutcomeDeliveryRequest struct {
	Envelope      OutcomeReady `json:"envelope"`
	DeliveryCycle string       `json:"delivery_cycle"`
}

func (r OutcomeDeliveryRequest) Validate() error {
	if err := r.Envelope.Validate(); err != nil {
		return err
	}
	return validateSegment("outcome delivery cycle", r.DeliveryCycle)
}

type OutcomeNotifier interface {
	Deliver(context.Context, OutcomeDeliveryRequest) (OutcomeNotificationReceipt, error)
}

type CoordinatorOutcomeActivities struct {
	Store    OutcomeStore
	Notifier OutcomeNotifier
	StoreRef string
}

func (a *CoordinatorOutcomeActivities) Deliver(
	ctx context.Context,
	request OutcomeDeliveryRequest,
) (OutcomeDelivery, error) {
	if a.Store == nil {
		return OutcomeDelivery{}, fmt.Errorf("coordinator outcome store is required")
	}
	if a.Notifier == nil {
		return OutcomeDelivery{}, fmt.Errorf("coordinator outcome notifier is required")
	}
	if err := validateStoreRef(a.StoreRef); err != nil {
		return OutcomeDelivery{}, fmt.Errorf("configured outcome store ref: %w", err)
	}
	if err := request.Validate(); err != nil {
		return OutcomeDelivery{}, err
	}
	envelope := request.Envelope
	if envelope.StoreRef != a.StoreRef {
		return OutcomeDelivery{}, fmt.Errorf(
			"outcome store ref %q does not match configured store ref %q",
			envelope.StoreRef,
			a.StoreRef,
		)
	}
	receipt, err := a.Notifier.Deliver(ctx, request)
	if err != nil {
		return OutcomeDelivery{}, fmt.Errorf("deliver coordinator outcome locally: %w", err)
	}
	delivery := OutcomeDelivery{
		OutcomeID:        envelope.OutcomeID,
		WorkID:           envelope.WorkID,
		DeliveryRef:      receipt.DeliveryRef,
		CoordinatorFence: receipt.CoordinatorFence,
	}
	if err := validateOutcomeDelivery(delivery); err != nil {
		return OutcomeDelivery{}, err
	}
	record, err := a.Store.MarkOutcomeDelivered(ctx, delivery)
	if err != nil {
		return OutcomeDelivery{}, fmt.Errorf("record coordinator outcome delivery: %w", err)
	}
	return OutcomeDelivery{
		OutcomeID:        record.Envelope.OutcomeID,
		WorkID:           record.Envelope.WorkID,
		DeliveryRef:      record.DeliveryRef,
		CoordinatorFence: record.CoordinatorFence,
	}, nil
}

func (a *CoordinatorOutcomeActivities) Acknowledge(
	ctx context.Context,
	ack OutcomeAcknowledgement,
) (OutcomeRecord, error) {
	if a.Store == nil {
		return OutcomeRecord{}, fmt.Errorf("coordinator outcome store is required")
	}
	if err := validateStoreRef(a.StoreRef); err != nil {
		return OutcomeRecord{}, fmt.Errorf("configured outcome store ref: %w", err)
	}
	if err := ack.Validate(); err != nil {
		return OutcomeRecord{}, err
	}
	if ack.StoreRef != a.StoreRef {
		return OutcomeRecord{}, fmt.Errorf(
			"acknowledgement store ref %q does not match configured store ref %q",
			ack.StoreRef,
			a.StoreRef,
		)
	}
	record, err := a.Store.MarkOutcomeAcknowledged(ctx, ack)
	if err != nil {
		return OutcomeRecord{}, fmt.Errorf("record coordinator acknowledgement: %w", err)
	}
	return record, nil
}
