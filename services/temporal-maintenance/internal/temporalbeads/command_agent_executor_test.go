package temporalbeads

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommandAgentExecutorRunsStartAttachProtocolWithoutShellArguments(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "requests.jsonl")
	executable := writeAgentProtocolFixture(t, logPath, false)
	t.Setenv("TEMPORAL_BEADS_DOLT_PASSWORD", "must-not-reach-agent")
	executor, err := NewCommandAgentExecutor(CommandAgentExecutorConfig{
		Executable:       executable,
		WorkingDirectory: t.TempDir(),
	})
	require.NoError(t, err)
	event := validReadyEvent(t)
	request := AgentExecutionRequest{
		Event:      event,
		ClaimToken: "claim-secret-fence",
	}

	sessionID, err := executor.ResolveSession(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "agent-session-1", sessionID)

	request.SessionID = sessionID
	var progress []AgentProgress
	result, err := executor.Execute(
		context.Background(),
		request,
		func(checkpoint AgentProgress) error {
			progress = append(progress, checkpoint)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, AgentExecutionResult{
		SessionID: "agent-session-1",
		Outcome:   OutcomeCompleted,
		ArtifactRefs: []ArtifactRef{{
			Kind:   ArtifactKindCommit,
			URI:    "git:0123456789abcdef0123456789abcdef01234567",
			SHA256: strings.Repeat("a", 64),
		}},
	}, result)
	require.Equal(t, []AgentProgress{{
		SessionID: "agent-session-1",
		Sequence:  1,
		Phase:     CheckpointPhaseAttached,
	}}, progress)

	require.NoError(t, executor.Cancel(context.Background(), AgentCancellation{
		BeadID:     event.BeadID,
		Generation: event.Generation,
		ClaimToken: request.ClaimToken,
		SessionID:  sessionID,
	}))

	logged, err := os.ReadFile(logPath)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(logged)), "\n")
	require.Len(t, lines, 3)
	require.Contains(t, lines[0], `"operation":"resolve"`)
	require.Contains(t, lines[1], `"operation":"execute"`)
	require.Contains(t, lines[2], `"operation":"cancel"`)
	require.Contains(t, string(logged), request.ClaimToken)
	require.NotContains(t, string(logged), "must-not-reach-agent")
	require.NotContains(t, string(logged), `"argv":"resolve claim-secret-fence"`)
	require.NotContains(t, string(logged), `"argv":"execute claim-secret-fence"`)
}

func TestCommandAgentExecutorRejectsUnsafeConfigurationAndProtocol(t *testing.T) {
	_, err := NewCommandAgentExecutor(CommandAgentExecutorConfig{
		Executable: "relative-agent-helper",
	})
	require.ErrorContains(t, err, "absolute")

	executable := writeAgentProtocolFixture(
		t,
		filepath.Join(t.TempDir(), "requests.jsonl"),
		true,
	)
	executor, err := NewCommandAgentExecutor(CommandAgentExecutorConfig{
		Executable:       executable,
		WorkingDirectory: t.TempDir(),
	})
	require.NoError(t, err)
	event := validReadyEvent(t)
	_, err = executor.ResolveSession(context.Background(), AgentExecutionRequest{
		Event: event, ClaimToken: "claim",
	})
	require.ErrorContains(t, err, "multiple terminal responses")
}

func writeAgentProtocolFixture(t *testing.T, logPath string, invalid bool) string {
	t.Helper()
	executable := filepath.Join(t.TempDir(), "agent-protocol")
	resolveResponse := `printf '%s\n' '{"type":"resolved","session_id":"agent-session-1"}'`
	if invalid {
		resolveResponse += "\n" + resolveResponse
	}
	script := `#!/usr/bin/env bash
set -euo pipefail
payload="$(cat)"
printf '{"operation":"%s","argv":"%s","secret":"%s","payload":%s}\n' "$1" "$*" "${TEMPORAL_BEADS_DOLT_PASSWORD-}" "$payload" >> "$AGENT_PROTOCOL_LOG"
case "$1" in
  resolve)
    ` + resolveResponse + `
    ;;
  execute)
    printf '%s\n' '{"type":"progress","progress":{"session_id":"agent-session-1","sequence":1,"phase":"agent-attached"}}'
    printf '%s\n' '{"type":"result","result":{"session_id":"agent-session-1","outcome":"completed","artifact_refs":[{"kind":"commit","uri":"git:0123456789abcdef0123456789abcdef01234567","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}}'
    ;;
  cancel)
    printf '%s\n' '{"type":"canceled"}'
    ;;
  *)
    exit 64
    ;;
esac
`
	require.NoError(t, os.WriteFile(executable, []byte(script), 0o700))
	t.Setenv("AGENT_PROTOCOL_LOG", logPath)
	return executable
}
