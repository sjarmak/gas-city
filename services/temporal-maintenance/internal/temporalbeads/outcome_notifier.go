package temporalbeads

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

const maxOutcomeNotifierFailureDetailBytes = 4096

type CommandOutcomeNotifierConfig struct {
	Executable       string
	WorkingDirectory string
	Environment      []string
}

// CommandOutcomeNotifier invokes one trusted local executable directly. The
// OutcomeReady envelope travels over stdin; Dolt credentials are stripped from
// the child environment.
type CommandOutcomeNotifier struct {
	executable       string
	workingDirectory string
	environment      []string
}

func NewCommandOutcomeNotifier(
	config CommandOutcomeNotifierConfig,
) (*CommandOutcomeNotifier, error) {
	if !filepath.IsAbs(config.Executable) {
		return nil, fmt.Errorf("outcome notifier path must be absolute")
	}
	info, err := os.Stat(config.Executable)
	if err != nil {
		return nil, fmt.Errorf("inspect outcome notifier: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, fmt.Errorf("outcome notifier must be an executable regular file")
	}
	if !filepath.IsAbs(config.WorkingDirectory) {
		return nil, fmt.Errorf("outcome notifier working directory must be absolute")
	}
	workInfo, err := os.Stat(config.WorkingDirectory)
	if err != nil {
		return nil, fmt.Errorf("inspect outcome notifier working directory: %w", err)
	}
	if !workInfo.IsDir() {
		return nil, fmt.Errorf("outcome notifier working directory must be a directory")
	}
	environment := append([]string(nil), config.Environment...)
	if environment == nil {
		environment = filteredOutcomeNotifierEnvironment(os.Environ())
	}
	return &CommandOutcomeNotifier{
		executable: config.Executable, workingDirectory: config.WorkingDirectory,
		environment: environment,
	}, nil
}

func (n *CommandOutcomeNotifier) Deliver(
	ctx context.Context,
	request OutcomeDeliveryRequest,
) (OutcomeNotificationReceipt, error) {
	if err := request.Validate(); err != nil {
		return OutcomeNotificationReceipt{}, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return OutcomeNotificationReceipt{}, fmt.Errorf(
			"encode coordinator outcome notification: %w",
			err,
		)
	}
	command := exec.CommandContext(ctx, n.executable, "deliver")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if err == syscall.ESRCH {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = 2 * time.Second
	command.Dir = n.workingDirectory
	command.Env = append([]string(nil), n.environment...)
	command.Stdin = bytes.NewReader(append(payload, '\n'))
	var stdout bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stdout
	if err := command.Run(); err != nil {
		detail := boundedOutcomeNotifierFailureDetail(stdout.String())
		if detail != "" {
			return OutcomeNotificationReceipt{}, fmt.Errorf(
				"local coordinator outcome notifier failed: %w: %s",
				err,
				detail,
			)
		}
		return OutcomeNotificationReceipt{}, fmt.Errorf(
			"local coordinator outcome notifier failed: %w",
			err,
		)
	}
	scanner := bufio.NewScanner(bytes.NewReader(stdout.Bytes()))
	scanner.Buffer(make([]byte, 4096), maxAgentProtocolLine)
	if !scanner.Scan() {
		return OutcomeNotificationReceipt{}, fmt.Errorf(
			"local coordinator outcome notifier returned no receipt",
		)
	}
	var receipt OutcomeNotificationReceipt
	decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return OutcomeNotificationReceipt{}, fmt.Errorf(
			"decode coordinator outcome receipt: %w",
			err,
		)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return OutcomeNotificationReceipt{}, fmt.Errorf(
			"decode coordinator outcome receipt: %w",
			err,
		)
	}
	if scanner.Scan() {
		return OutcomeNotificationReceipt{}, fmt.Errorf(
			"local coordinator outcome notifier returned multiple receipts",
		)
	}
	if err := scanner.Err(); err != nil {
		return OutcomeNotificationReceipt{}, fmt.Errorf(
			"read coordinator outcome receipt: %w",
			err,
		)
	}
	if err := validateOutcomeDelivery(OutcomeDelivery{
		OutcomeID: request.Envelope.OutcomeID, WorkID: request.Envelope.WorkID,
		DeliveryRef: receipt.DeliveryRef, CoordinatorFence: receipt.CoordinatorFence,
	}); err != nil {
		return OutcomeNotificationReceipt{}, err
	}
	return receipt, nil
}

func boundedOutcomeNotifierFailureDetail(raw string) string {
	detail := strings.TrimSpace(strings.ToValidUTF8(raw, "\uFFFD"))
	if len(detail) <= maxOutcomeNotifierFailureDetailBytes {
		return detail
	}
	const suffix = "..."
	cut := maxOutcomeNotifierFailureDetailBytes - len(suffix)
	for cut > 0 && !utf8.ValidString(detail[:cut]) {
		cut--
	}
	return detail[:cut] + suffix
}

func filteredOutcomeNotifierEnvironment(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, value := range environment {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "TEMPORAL_BEADS_DOLT_PASSWORD", "TEMPORAL_OUTCOME_DOLT_PASSWORD":
			continue
		default:
			filtered = append(filtered, value)
		}
	}
	return filtered
}
