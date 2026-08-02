package temporalbeads

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const maxAgentProtocolLine = 1024 * 1024
const maxAgentProtocolMessages = 10_000

// CommandAgentExecutorConfig binds the Activity to a trusted executable that
// implements the stable start-or-attach agent protocol.
type CommandAgentExecutorConfig struct {
	Executable       string
	WorkingDirectory string
	Environment      []string
}

// CommandAgentExecutor invokes a fixed executable directly, never through a
// shell. Claim tokens and work payloads travel over stdin rather than argv.
type CommandAgentExecutor struct {
	executable       string
	workingDirectory string
	environment      []string
}

type agentProtocolRequest struct {
	Operation    string                 `json:"operation"`
	Execution    *AgentExecutionRequest `json:"execution,omitempty"`
	Cancellation *AgentCancellation     `json:"cancellation,omitempty"`
}

type agentProtocolMessage struct {
	Type      string                `json:"type"`
	SessionID string                `json:"session_id,omitempty"`
	Progress  *AgentProgress        `json:"progress,omitempty"`
	Result    *AgentExecutionResult `json:"result,omitempty"`
}

// NewCommandAgentExecutor validates the fixed executable and working directory.
func NewCommandAgentExecutor(
	config CommandAgentExecutorConfig,
) (*CommandAgentExecutor, error) {
	if !filepath.IsAbs(config.Executable) {
		return nil, fmt.Errorf("agent executor path must be absolute")
	}
	info, err := os.Stat(config.Executable)
	if err != nil {
		return nil, fmt.Errorf("inspect agent executor: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, fmt.Errorf("agent executor must be an executable regular file")
	}
	if !filepath.IsAbs(config.WorkingDirectory) {
		return nil, fmt.Errorf("agent working directory must be absolute")
	}
	workInfo, err := os.Stat(config.WorkingDirectory)
	if err != nil {
		return nil, fmt.Errorf("inspect agent working directory: %w", err)
	}
	if !workInfo.IsDir() {
		return nil, fmt.Errorf("agent working directory must be a directory")
	}
	environment := append([]string(nil), config.Environment...)
	if environment == nil {
		environment = filteredAgentEnvironment(os.Environ())
	}
	return &CommandAgentExecutor{
		executable:       config.Executable,
		workingDirectory: config.WorkingDirectory,
		environment:      environment,
	}, nil
}

// ResolveSession asks the trusted adapter to create or reattach the session
// keyed by the claim token.
func (e *CommandAgentExecutor) ResolveSession(
	ctx context.Context,
	request AgentExecutionRequest,
) (string, error) {
	if err := validateExecutionRequest(request, false); err != nil {
		return "", err
	}
	messages, err := e.run(ctx, "resolve", agentProtocolRequest{
		Operation: "resolve",
		Execution: &request,
	}, nil)
	if err != nil {
		return "", err
	}
	if len(messages) != 1 || messages[0].Type != "resolved" {
		if len(messages) > 1 {
			return "", fmt.Errorf("agent resolve returned multiple terminal responses")
		}
		return "", fmt.Errorf("agent resolve returned an invalid response")
	}
	sessionID := messages[0].SessionID
	if err := validateSegment("session id", sessionID); err != nil {
		return "", fmt.Errorf("agent resolve response: %w", err)
	}
	return sessionID, nil
}

// Execute attaches to the resolved session, streams compact checkpoints, and
// returns one terminal result.
func (e *CommandAgentExecutor) Execute(
	ctx context.Context,
	request AgentExecutionRequest,
	record func(AgentProgress) error,
) (AgentExecutionResult, error) {
	if err := validateExecutionRequest(request, true); err != nil {
		return AgentExecutionResult{}, err
	}
	if record == nil {
		return AgentExecutionResult{}, fmt.Errorf("agent progress recorder is required")
	}
	var terminal *AgentExecutionResult
	_, err := e.run(ctx, "execute", agentProtocolRequest{
		Operation: "execute",
		Execution: &request,
	}, func(message agentProtocolMessage) error {
		switch message.Type {
		case "progress":
			if message.Progress == nil {
				return fmt.Errorf("agent progress response is empty")
			}
			return record(*message.Progress)
		case "result":
			if message.Result == nil {
				return fmt.Errorf("agent terminal response is empty")
			}
			if terminal != nil {
				return fmt.Errorf("agent execute returned multiple terminal responses")
			}
			result := *message.Result
			terminal = &result
			return nil
		default:
			return fmt.Errorf("agent execute returned unknown response type %q", message.Type)
		}
	})
	if err != nil {
		return AgentExecutionResult{}, err
	}
	if terminal == nil {
		return AgentExecutionResult{}, fmt.Errorf("agent execute returned no terminal response")
	}
	return *terminal, nil
}

// Cancel targets the exact attached session and claim fence.
func (e *CommandAgentExecutor) Cancel(
	ctx context.Context,
	cancellation AgentCancellation,
) error {
	if cancellation.BeadID == "" || cancellation.Generation <= 0 ||
		cancellation.ClaimToken == "" || cancellation.SessionID == "" {
		return fmt.Errorf("agent cancellation requires bead, generation, claim, and session")
	}
	messages, err := e.run(ctx, "cancel", agentProtocolRequest{
		Operation:    "cancel",
		Cancellation: &cancellation,
	}, nil)
	if err != nil {
		return err
	}
	if len(messages) != 1 || messages[0].Type != "canceled" {
		return fmt.Errorf("agent cancel returned an invalid response")
	}
	return nil
}

func (e *CommandAgentExecutor) run(
	ctx context.Context,
	operation string,
	request agentProtocolRequest,
	consume func(agentProtocolMessage) error,
) ([]agentProtocolMessage, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode agent %s request: %w", operation, err)
	}
	commandContext, cancel := context.WithCancel(ctx)
	defer cancel()
	command := exec.CommandContext(commandContext, e.executable, operation)
	command.Dir = e.workingDirectory
	command.Env = append([]string(nil), e.environment...)
	command.Stdin = bytes.NewReader(append(payload, '\n'))
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("open agent %s output: %w", operation, err)
	}
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start agent %s adapter: %w", operation, err)
	}

	var messages []agentProtocolMessage
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), maxAgentProtocolLine)
	for scanner.Scan() {
		var message agentProtocolMessage
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&message); err != nil {
			cancel()
			_ = command.Wait()
			return nil, fmt.Errorf("decode agent %s response: %w", operation, err)
		}
		if err := requireJSONEOF(decoder); err != nil {
			cancel()
			_ = command.Wait()
			return nil, fmt.Errorf("decode agent %s response: %w", operation, err)
		}
		if consume != nil {
			if err := consume(message); err != nil {
				cancel()
				_ = command.Wait()
				return nil, err
			}
		} else {
			messages = append(messages, message)
			if len(messages) > maxAgentProtocolMessages {
				cancel()
				_ = command.Wait()
				return nil, fmt.Errorf(
					"agent %s adapter exceeded message limit",
					operation,
				)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		cancel()
		_ = command.Wait()
		return nil, fmt.Errorf("read agent %s response: %w", operation, err)
	}
	if err := command.Wait(); err != nil {
		return nil, fmt.Errorf("agent %s adapter failed: %w", operation, err)
	}
	return messages, nil
}

func validateExecutionRequest(request AgentExecutionRequest, requireSession bool) error {
	if err := request.Event.Validate(); err != nil {
		return fmt.Errorf("agent execution event: %w", err)
	}
	if request.ClaimToken == "" {
		return fmt.Errorf("agent execution claim token is required")
	}
	if requireSession {
		if err := validateSegment("session id", request.SessionID); err != nil {
			return err
		}
	}
	return nil
}

func filteredAgentEnvironment(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, value := range environment {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "TEMPORAL_BEADS_DOLT_PASSWORD":
			continue
		default:
			filtered = append(filtered, value)
		}
	}
	return filtered
}
