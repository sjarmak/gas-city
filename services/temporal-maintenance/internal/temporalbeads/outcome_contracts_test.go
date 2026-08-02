package temporalbeads

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOutcomeAdaptersShareVersionedEnvelopeContract(t *testing.T) {
	occurredAt := time.Date(2026, 7, 31, 2, 45, 0, 0, time.UTC)
	base := OutcomeAdapterInput{
		StoreRef:     "city:ds-research",
		WorkID:       "dr-step",
		SourceRootID: "dr-root",
		StepKey:      "mol-do-work.do-work",
		Summary:      "The implementation passed independent review.",
		Impact:       "The coordinator can decide whether to promote it.",
		State:        OutcomeEvidenceVerified,
		Evidence:     []ArtifactRef{testArtifact()},
		Fence: OutcomeProducerFence{
			ProducerID: "city-infra-pl",
			Generation: 7,
			Token:      "claim-token",
		},
		OccurredAt: occurredAt,
	}

	tests := []struct {
		name    string
		kind    OutcomeProducerKind
		adapter func(OutcomeAdapterInput) (OutcomeReady, error)
	}{
		{name: "direct worker", kind: OutcomeProducerDirectWorker, adapter: DirectWorkerOutcome},
		{name: "formula step", kind: OutcomeProducerFormulaStep, adapter: FormulaStepOutcome},
		{name: "project lead", kind: OutcomeProducerProjectLead, adapter: ProjectLeadOutcome},
		{name: "maintenance order", kind: OutcomeProducerMaintenanceOrder, adapter: MaintenanceOrderOutcome},
		{name: "temporal result", kind: OutcomeProducerTemporal, adapter: TemporalOutcome},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envelope, err := test.adapter(base)
			require.NoError(t, err)
			require.Equal(t, OutcomeContractVersion, envelope.ContractVersion)
			require.Equal(t, test.kind, envelope.Producer)
			require.Equal(t, base.StoreRef, envelope.StoreRef)
			require.Equal(t, base.WorkID, envelope.WorkID)
			require.Equal(t, base.SourceRootID, envelope.SourceRootID)
			require.Equal(t, base.Fence, envelope.Fence)
			require.Equal(t, occurredAt, envelope.OccurredAt)
			require.NotEmpty(t, envelope.OutcomeID)
			require.NoError(t, envelope.Validate())
		})
	}
}

func TestFormulaOutcomeDoesNotRequireSourceRootClosure(t *testing.T) {
	envelope, err := FormulaStepOutcome(validOutcomeAdapterInput())
	require.NoError(t, err)
	require.Equal(t, "dr-root", envelope.SourceRootID)
	require.Equal(t, "dr-step", envelope.WorkID)
	require.Equal(t, OutcomeEvidenceVerified, envelope.State)
}

func TestOutcomeIDIsDeterministicAcrossDuplicateEmission(t *testing.T) {
	input := validOutcomeAdapterInput()
	first, err := DirectWorkerOutcome(input)
	require.NoError(t, err)
	second, err := DirectWorkerOutcome(input)
	require.NoError(t, err)
	require.Equal(t, first.OutcomeID, second.OutcomeID)

	input.Fence.Generation++
	next, err := DirectWorkerOutcome(input)
	require.NoError(t, err)
	require.NotEqual(t, first.OutcomeID, next.OutcomeID)
}

func TestOutcomeEnvelopeRejectsMissingHumanMeaningAndFence(t *testing.T) {
	input := validOutcomeAdapterInput()
	input.Summary = ""
	_, err := ProjectLeadOutcome(input)
	require.ErrorContains(t, err, "summary")

	input = validOutcomeAdapterInput()
	input.Impact = ""
	_, err = MaintenanceOrderOutcome(input)
	require.ErrorContains(t, err, "impact")

	input = validOutcomeAdapterInput()
	input.Fence.ProducerID = ""
	_, err = TemporalOutcome(input)
	require.ErrorContains(t, err, "producer fence")

	input = validOutcomeAdapterInput()
	input.Fence.Generation = 0
	_, err = DirectWorkerOutcome(input)
	require.ErrorContains(t, err, "generation")

	input = validOutcomeAdapterInput()
	input.Fence.Token = ""
	_, err = FormulaStepOutcome(input)
	require.ErrorContains(t, err, "token")

	input = validOutcomeAdapterInput()
	input.Fence.Token = "not an exact token"
	_, err = MaintenanceOrderOutcome(input)
	require.ErrorContains(t, err, "token")
}

func TestOutcomeEnvelopeRejectsMoreThanSixtyFourEvidenceReferences(t *testing.T) {
	input := validOutcomeAdapterInput()
	input.Evidence = make([]ArtifactRef, MaxOutcomeEvidenceReferences+1)
	for index := range input.Evidence {
		input.Evidence[index] = testArtifact()
	}

	_, err := DirectWorkerOutcome(input)
	require.ErrorContains(t, err, "at most 64")
}

func TestOutcomeReadyValidationRejectsInvalidBoundaryFields(t *testing.T) {
	valid, err := DirectWorkerOutcome(validOutcomeAdapterInput())
	require.NoError(t, err)
	tests := []struct {
		name   string
		mutate func(*OutcomeReady)
		want   string
	}{
		{"version", func(o *OutcomeReady) { o.ContractVersion++ }, "version"},
		{"producer", func(o *OutcomeReady) { o.Producer = "invalid" }, "producer"},
		{"store", func(o *OutcomeReady) { o.StoreRef = "invalid" }, "store ref"},
		{"work", func(o *OutcomeReady) { o.WorkID = "" }, "work id"},
		{"root", func(o *OutcomeReady) { o.SourceRootID = "" }, "source root"},
		{"step", func(o *OutcomeReady) { o.StepKey = "" }, "step key"},
		{
			"run-without-workflow",
			func(o *OutcomeReady) {
				o.WorkflowID = ""
				o.WorkflowRunID = "run"
			},
			"requires a workflow id",
		},
		{"summary", func(o *OutcomeReady) { o.Summary = " bad" }, "summary"},
		{"impact", func(o *OutcomeReady) { o.Impact = "bad\r" }, "impact"},
		{"state", func(o *OutcomeReady) { o.State = "invalid" }, "state"},
		{"evidence-empty", func(o *OutcomeReady) { o.Evidence = nil }, "required"},
		{
			"evidence-invalid",
			func(o *OutcomeReady) { o.Evidence[0].SHA256 = "bad" },
			"sha256",
		},
		{
			"fence",
			func(o *OutcomeReady) { o.Fence.Generation = 0 },
			"generation",
		},
		{"occurred-at", func(o *OutcomeReady) { o.OccurredAt = time.Time{} }, "occurred_at"},
		{"outcome-id", func(o *OutcomeReady) { o.OutcomeID = "outcome-wrong" }, "outcome id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneOutcomeReady(valid)
			test.mutate(&candidate)
			require.ErrorContains(t, candidate.Validate(), test.want)
		})
	}
}

func TestTemporalCompletionUsesReceiptEvidenceWhenAgentSuppliesNone(t *testing.T) {
	record := BeadRecord{
		CityID:     "city",
		BeadID:     "dr-result",
		WorkflowID: "bead-orchestration/city/logical-run/dr-result",
		RunID:      "logical-run",
		Formula:    validFormulaRef(),
	}
	const sourceRunID = "6ef5aec5-a532-4ae3-8504-b18db66467e9"
	envelope, err := temporalCompletionOutcome(
		record,
		Completion{
			BeadID:              record.BeadID,
			SessionID:           "session",
			Generation:          1,
			ClaimToken:          "claim",
			Outcome:             OutcomeCompleted,
			SourceWorkflowID:    record.WorkflowID,
			SourceWorkflowRunID: sourceRunID,
		},
		"city:ds-research",
		time.Date(2026, 7, 31, 3, 15, 0, 0, time.UTC),
	)
	require.NoError(t, err)
	require.Len(t, envelope.Evidence, 1)
	require.Equal(t, ArtifactKindReviewRecord, envelope.Evidence[0].Kind)
	require.Equal(t, "bead:dr-result", envelope.Evidence[0].URI)
	require.Equal(t, record.WorkflowID, envelope.WorkflowID)
	require.Equal(t, sourceRunID, envelope.WorkflowRunID)
	require.NotEqual(t, record.RunID, envelope.WorkflowRunID)
}

func validOutcomeAdapterInput() OutcomeAdapterInput {
	return OutcomeAdapterInput{
		StoreRef:     "city:ds-research",
		WorkID:       "dr-step",
		SourceRootID: "dr-root",
		StepKey:      "mol-do-work.do-work",
		Summary:      "The work passed its verification gate.",
		Impact:       "The coordinator can act without inspecting a transcript.",
		State:        OutcomeEvidenceVerified,
		Evidence:     []ArtifactRef{testArtifact()},
		Fence: OutcomeProducerFence{
			ProducerID: "worker-session",
			Generation: 1,
			Token:      "claim-token",
		},
		OccurredAt: time.Date(2026, 7, 31, 2, 45, 0, 0, time.UTC),
	}
}
