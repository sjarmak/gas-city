package temporalbeads

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadyEventRoundTripContainsOnlyBoundaryData(t *testing.T) {
	event := validReadyEvent(t)

	data, err := EncodeWorkflowPayload(event)
	require.NoError(t, err)
	for _, forbidden := range []string{"prompt", "transcript", "diff", "log"} {
		require.NotContains(t, string(data), forbidden)
	}

	var decoded ReadyEvent
	require.NoError(t, DecodeWorkflowPayload(data, &decoded))
	require.Equal(t, event, decoded)
	require.Equal(t, validFormulaRef(), decoded.Formula)
}

func TestFormulaRefIsCompleteAndStable(t *testing.T) {
	ref := validFormulaRef()
	require.NoError(t, ref.Validate())

	for name, mutate := range map[string]func(*FormulaRef){
		"name":    func(value *FormulaRef) { value.Name = "" },
		"hash":    func(value *FormulaRef) { value.Hash = "not-a-hash" },
		"version": func(value *FormulaRef) { value.Version = "" },
		"root":    func(value *FormulaRef) { value.RootID = "" },
		"step":    func(value *FormulaRef) { value.StepKey = "" },
		"rig":     func(value *FormulaRef) { value.Rig = "" },
	} {
		t.Run(name, func(t *testing.T) {
			broken := ref
			mutate(&broken)
			require.Error(t, broken.Validate())
		})
	}

	first, err := FormulaActivityID(ref, 7)
	require.NoError(t, err)
	second, err := FormulaActivityID(ref, 7)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, "formula/dr-gst6/mol-do-work.do-work/g7", first)
}

func TestFormulaMemoExposesCanaryTopology(t *testing.T) {
	event := validReadyEvent(t)
	require.Equal(t, map[string]interface{}{
		"GasCityFormulaName":    event.Formula.Name,
		"GasCityFormulaHash":    event.Formula.Hash,
		"GasCityFormulaVersion": event.Formula.Version,
		"GasCityFormulaRoot":    event.Formula.RootID,
		"GasCityFormulaStep":    event.Formula.StepKey,
		"GasCityRig":            event.Formula.Rig,
		"GasCityBead":           event.BeadID,
		"GasCityGeneration":     event.Generation,
	}, FormulaMemo(event))
}

func TestChildWorkflowLinkLifecycleIsValidated(t *testing.T) {
	event := validReadyEvent(t)
	receipt := WorkflowReceipt{
		WorkflowID: "bead-orchestration/ds-research/run/gc-1",
		RunID:      "child-run-1",
		EventID:    event.EventID,
	}
	started, err := NewChildWorkflowLink(
		event,
		receipt,
		ChildWorkflowStarted,
		"",
	)
	require.NoError(t, err)
	require.False(t, started.Terminal())

	completed := started
	completed.Status = ChildWorkflowCompleted
	require.NoError(t, completed.Validate())
	require.True(t, completed.Terminal())

	failed := started
	failed.Status = ChildWorkflowFailed
	failed.ErrorCode = "activity-failed"
	require.NoError(t, failed.Validate())
	require.True(t, failed.Terminal())

	completed.ErrorCode = "unexpected"
	require.ErrorContains(t, completed.Validate(), "cannot have an error")
	failed.ErrorCode = ""
	require.Error(t, failed.Validate())
	started.Status = "unknown"
	require.ErrorContains(t, started.Validate(), "status")
}

func TestWorkflowPayloadRejectsUnrecognizedContent(t *testing.T) {
	for _, field := range []string{"prompt", "transcript", "diff", "log"} {
		t.Run(field, func(t *testing.T) {
			payload := []byte(`{
				"contract_version":1,
				"event_id":"evt",
				"city_id":"city",
				"run_id":"run",
				"bead_id":"gc-1",
				"generation":1,
				"ready_at":"2026-07-30T12:00:00Z",
				"` + field + `":"must-not-enter-history"
			}`)
			var decoded ReadyEvent
			err := DecodeWorkflowPayload(payload, &decoded)
			require.ErrorContains(t, err, "unknown field")
		})
	}
}

func TestArtifactReferenceRequiresHashAndSafeKind(t *testing.T) {
	ref := ArtifactRef{
		Kind:   ArtifactKindCommit,
		URI:    "git:commit:0123456789abcdef",
		SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	require.NoError(t, ref.Validate())

	ref.Kind = ArtifactKind("prompt")
	require.ErrorContains(t, ref.Validate(), "artifact kind")

	ref.Kind = ArtifactKindCommit
	ref.URI = "data:text/plain,inline-content"
	require.ErrorContains(t, ref.Validate(), "scheme")
}

func TestReadyEventIdentityIsDeterministic(t *testing.T) {
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	first, err := NewReadyEvent(
		"city", "run", "gc-1", 7, validFormulaRef(), now,
	)
	require.NoError(t, err)
	second, err := NewReadyEvent(
		"city", "run", "gc-1", 7, validFormulaRef(), now.Add(time.Hour),
	)
	require.NoError(t, err)

	require.Equal(t, first.EventID, second.EventID)
	require.Equal(t, CurrentContractVersion, first.ContractVersion)
	require.Equal(t, "gc-1/7", first.Key())
}

func TestDecodeWorkflowPayloadRejectsTrailingDocument(t *testing.T) {
	event := validReadyEvent(t)
	first, err := json.Marshal(event)
	require.NoError(t, err)
	data := append(first, []byte(`{"extra":true}`)...)

	var decoded ReadyEvent
	err = DecodeWorkflowPayload(data, &decoded)
	require.ErrorContains(t, err, "trailing")
}

func TestTimingConfigUsesDeterministicClock(t *testing.T) {
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	clock := NewManualClock(now)
	config := TimingConfig{
		HeartbeatTimeout:  30 * time.Second,
		ReconcileInterval: 2 * time.Minute,
		Clock:             clock,
	}
	require.NoError(t, config.Validate())
	require.Equal(t, now, config.Now())

	clock.Advance(config.ReconcileInterval)
	require.Equal(t, now.Add(2*time.Minute), config.Now())
}

func TestCloseRequestRequiresAuthoritativeEventSet(t *testing.T) {
	event := validReadyEvent(t)
	request := CloseRequest{
		ContractVersion:  CurrentContractVersion,
		CityID:           event.CityID,
		RunID:            event.RunID,
		BeadID:           event.BeadID,
		ExpectedEventIDs: []string{event.EventID},
		ReasonCode:       "run-sealed",
	}
	data, err := EncodeWorkflowPayload(request)
	require.NoError(t, err)
	var decoded CloseRequest
	require.NoError(t, DecodeWorkflowPayload(data, &decoded))
	require.Equal(t, request, decoded)

	request.ExpectedEventIDs = nil
	_, err = EncodeWorkflowPayload(request)
	require.ErrorContains(t, err, "authoritative run set")
}

func TestAttachmentHeartbeatReservesSequenceZero(t *testing.T) {
	checkpoint := HeartbeatCheckpoint{
		BeadID:     "gc-1",
		Generation: 1,
		ClaimToken: "claim-token",
		SessionID:  "session-1",
		Sequence:   0,
		Phase:      CheckpointPhaseAttached,
	}
	require.NoError(t, checkpoint.validatePayload())

	checkpoint.Phase = "running"
	require.ErrorContains(t, checkpoint.validatePayload(), "sequence zero")
	checkpoint.Phase = CheckpointPhaseAttached
	checkpoint.ArtifactRefs = []ArtifactRef{testArtifact()}
	require.ErrorContains(t, checkpoint.validatePayload(), "sequence zero")
}

func TestEveryWorkflowPayloadTypeRoundTripsWithoutInlineAgentContent(t *testing.T) {
	event := validReadyEvent(t)
	payloads := []workflowPayload{
		event,
		WorkflowInput{
			ContractVersion:  CurrentContractVersion,
			CityID:           event.CityID,
			RunID:            event.RunID,
			BeadID:           event.BeadID,
			InitialReady:     []ReadyEvent{event},
			HeartbeatTimeout: time.Minute,
		},
		WorkflowState{
			ContractVersion:  CurrentContractVersion,
			CityID:           event.CityID,
			RunID:            event.RunID,
			BeadID:           event.BeadID,
			Phase:            workflowPhaseRunning,
			ReceivedEventIDs: []string{event.EventID},
		},
		CloseRequest{
			ContractVersion:  CurrentContractVersion,
			CityID:           event.CityID,
			RunID:            event.RunID,
			BeadID:           event.BeadID,
			ExpectedEventIDs: []string{event.EventID},
			ReasonCode:       "run-sealed",
		},
		ActivityInput{Event: event},
		ActivityResult{
			EventID: event.EventID, Outcome: string(OutcomeCompleted),
			SessionID: "session-1", ArtifactRefs: []ArtifactRef{event.Artifacts[0]},
		},
		HeartbeatCheckpoint{
			BeadID: event.BeadID, Generation: event.Generation,
			ClaimToken: "claim-token", SessionID: "session-1",
			Sequence: 1, Phase: "tests",
			ArtifactRefs: []ArtifactRef{event.Artifacts[0]},
		},
	}
	for _, payload := range payloads {
		data, err := EncodeWorkflowPayload(payload)
		require.NoError(t, err)
		for _, forbidden := range []string{
			`"prompt"`, `"transcript"`, `"diff"`, `"log"`,
		} {
			require.NotContains(t, string(data), forbidden)
		}
	}
}

func TestLegacyHeartbeatCheckpointDefaultsArtifactTruncationToFalse(t *testing.T) {
	var checkpoint HeartbeatCheckpoint
	require.NoError(
		t,
		json.Unmarshal(
			[]byte(`{
				"bead_id":"dr-legacy",
				"generation":1,
				"claim_token":"claim-legacy",
				"session_id":"session-legacy",
				"sequence":1,
				"phase":"agent-complete"
			}`),
			&checkpoint,
		),
	)

	require.False(t, checkpoint.ArtifactRefsTruncated)
	require.NoError(t, checkpoint.validatePayload())
}

func validReadyEvent(t *testing.T) ReadyEvent {
	t.Helper()
	event, err := NewReadyEvent(
		"ds-research",
		"goal-gc-636326",
		"gc-636335",
		1,
		validFormulaRef(),
		time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)
	event.Artifacts = []ArtifactRef{{
		Kind:   ArtifactKindCommit,
		URI:    "git:commit:0123456789abcdef",
		SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}}
	return event
}

func validFormulaRef() FormulaRef {
	return FormulaRef{
		Name:    "mol-do-work",
		Hash:    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Version: "graph.v2",
		RootID:  "dr-gst6",
		StepKey: "mol-do-work.do-work",
		Rig:     "gascity",
	}
}
