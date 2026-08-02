package temporalbeads

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestCommandOutcomeNotifierCancellationKillsDescendantSideEffects(t *testing.T) {
	dir := t.TempDir()
	lateSideEffect := filepath.Join(dir, "late-side-effect")
	executable := writeOutcomeNotifierFixture(t, dir, `#!/usr/bin/env bash
set -euo pipefail
cat >/dev/null
(sleep 0.3; printf survived >"$OUTCOME_LATE_SIDE_EFFECT") &
wait
`)
	notifier, err := NewCommandOutcomeNotifier(CommandOutcomeNotifierConfig{
		Executable: executable, WorkingDirectory: dir,
		Environment: []string{"OUTCOME_LATE_SIDE_EFFECT=" + lateSideEffect},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = notifier.Deliver(ctx, OutcomeDeliveryRequest{
		Envelope: validOutcomeEnvelope(t), DeliveryCycle: "cycle-000001",
	})
	require.Error(t, err)
	time.Sleep(400 * time.Millisecond)
	require.NoFileExists(t, lateSideEffect)
}

func TestCommandOutcomeNotifierUsesFixedLocalProtocolWithoutSecrets(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "delivery.json")
	executable := writeOutcomeNotifierFixture(t, dir, `#!/usr/bin/env bash
set -euo pipefail
payload="$(cat)"
printf '{"secret":"%s","payload":%s}\n' "${TEMPORAL_OUTCOME_DOLT_PASSWORD-}" "$payload" >"$OUTCOME_TEST_LOG"
printf '%s\n' '{"delivery_ref":"mail:message-1","coordinator_fence":"mayor-session-1"}'
`)
	t.Setenv("OUTCOME_TEST_LOG", logPath)
	t.Setenv("TEMPORAL_OUTCOME_DOLT_PASSWORD", "must-not-reach-notifier")
	notifier, err := NewCommandOutcomeNotifier(CommandOutcomeNotifierConfig{
		Executable:       executable,
		WorkingDirectory: dir,
	})
	require.NoError(t, err)
	envelope := validOutcomeEnvelope(t)
	request := OutcomeDeliveryRequest{
		Envelope: envelope, DeliveryCycle: "cycle-000001",
	}

	receipt, err := notifier.Deliver(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, OutcomeNotificationReceipt{
		DeliveryRef:      "mail:message-1",
		CoordinatorFence: "mayor-session-1",
	}, receipt)
	payload, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "must-not-reach-notifier")
	require.Contains(t, string(payload), envelope.OutcomeID)
}

func TestCommandOutcomeNotifierRejectsMalformedReceipt(t *testing.T) {
	dir := t.TempDir()
	executable := writeOutcomeNotifierFixture(t, dir, `#!/usr/bin/env bash
set -euo pipefail
cat >/dev/null
printf '%s\n' '{"delivery_ref":"not-absolute","coordinator_fence":"mayor-session-1"}'
`)
	notifier, err := NewCommandOutcomeNotifier(CommandOutcomeNotifierConfig{
		Executable: executable, WorkingDirectory: dir,
	})
	require.NoError(t, err)

	_, err = notifier.Deliver(context.Background(), OutcomeDeliveryRequest{
		Envelope: validOutcomeEnvelope(t), DeliveryCycle: "cycle-000001",
	})
	require.ErrorContains(t, err, "delivery ref")
}

func TestCommandOutcomeNotifierConfigurationFailsClosed(t *testing.T) {
	dir := t.TempDir()
	executable := writeOutcomeNotifierFixture(t, dir, "#!/bin/sh\n")
	plainFile := filepath.Join(dir, "plain")
	require.NoError(t, os.WriteFile(plainFile, []byte("plain"), 0o600))

	for _, test := range []struct {
		name   string
		config CommandOutcomeNotifierConfig
		want   string
	}{
		{
			name: "relative executable",
			config: CommandOutcomeNotifierConfig{
				Executable: "relative", WorkingDirectory: dir,
			},
			want: "path must be absolute",
		},
		{
			name: "missing executable",
			config: CommandOutcomeNotifierConfig{
				Executable: filepath.Join(dir, "missing"), WorkingDirectory: dir,
			},
			want: "inspect outcome notifier",
		},
		{
			name: "directory executable",
			config: CommandOutcomeNotifierConfig{
				Executable: dir, WorkingDirectory: dir,
			},
			want: "executable regular file",
		},
		{
			name: "non executable file",
			config: CommandOutcomeNotifierConfig{
				Executable: plainFile, WorkingDirectory: dir,
			},
			want: "executable regular file",
		},
		{
			name: "relative working directory",
			config: CommandOutcomeNotifierConfig{
				Executable: executable, WorkingDirectory: "relative",
			},
			want: "working directory must be absolute",
		},
		{
			name: "missing working directory",
			config: CommandOutcomeNotifierConfig{
				Executable:       executable,
				WorkingDirectory: filepath.Join(dir, "missing"),
			},
			want: "inspect outcome notifier working directory",
		},
		{
			name: "file working directory",
			config: CommandOutcomeNotifierConfig{
				Executable: executable, WorkingDirectory: plainFile,
			},
			want: "must be a directory",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewCommandOutcomeNotifier(test.config)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestCommandOutcomeNotifierDeliveryProtocolFailsClosed(t *testing.T) {
	dir := t.TempDir()
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "command failure",
			body: "#!/bin/sh\ncat >/dev/null\nexit 7\n",
			want: "notifier failed",
		},
		{
			name: "empty receipt",
			body: "#!/bin/sh\ncat >/dev/null\n",
			want: "no receipt",
		},
		{
			name: "invalid json",
			body: "#!/bin/sh\ncat >/dev/null\nprintf 'not-json\\n'\n",
			want: "decode coordinator outcome receipt",
		},
		{
			name: "trailing json",
			body: "#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' '{\"delivery_ref\":\"local:one\",\"coordinator_fence\":\"mayor\"} {}'\n",
			want: "decode coordinator outcome receipt",
		},
		{
			name: "multiple receipts",
			body: "#!/bin/sh\ncat >/dev/null\nprintf '%s\\n%s\\n' '{\"delivery_ref\":\"local:one\",\"coordinator_fence\":\"mayor\"}' '{\"delivery_ref\":\"local:two\",\"coordinator_fence\":\"mayor\"}'\n",
			want: "multiple receipts",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			notifier, err := NewCommandOutcomeNotifier(
				CommandOutcomeNotifierConfig{
					Executable:       writeOutcomeNotifierFixture(t, dir, test.body),
					WorkingDirectory: dir,
					Environment:      []string{},
				},
			)
			require.NoError(t, err)
			_, err = notifier.Deliver(context.Background(), OutcomeDeliveryRequest{
				Envelope: validOutcomeEnvelope(t), DeliveryCycle: "cycle-000001",
			})
			require.ErrorContains(t, err, test.want)
		})
	}

	notifier, err := NewCommandOutcomeNotifier(CommandOutcomeNotifierConfig{
		Executable:       writeOutcomeNotifierFixture(t, dir, "#!/bin/sh\n"),
		WorkingDirectory: dir,
	})
	require.NoError(t, err)
	_, err = notifier.Deliver(context.Background(), OutcomeDeliveryRequest{})
	require.Error(t, err)
}

func TestCommandOutcomeNotifierPreservesBoundedFailureDetail(t *testing.T) {
	unicodeBoundary := strings.Repeat("x", 4095) + "€unbounded-tail"
	bounded := boundedOutcomeNotifierFailureDetail(unicodeBoundary)
	require.True(t, utf8.ValidString(bounded))
	require.LessOrEqual(t, len(bounded), maxOutcomeNotifierFailureDetailBytes)
	require.Equal(t, strings.Repeat("x", 4093)+"...", bounded)

	dir := t.TempDir()
	executable := writeOutcomeNotifierFixture(t, dir, `#!/bin/sh
cat >/dev/null
printf '%s\n' 'coordinator-outcome-notify: gc mail send timed out after 15s' >&2
exit 1
`)
	notifier, err := NewCommandOutcomeNotifier(CommandOutcomeNotifierConfig{
		Executable: executable, WorkingDirectory: dir, Environment: []string{},
	})
	require.NoError(t, err)

	_, err = notifier.Deliver(context.Background(), OutcomeDeliveryRequest{
		Envelope: validOutcomeEnvelope(t), DeliveryCycle: "cycle-000001",
	})
	require.ErrorContains(
		t, err,
		"coordinator-outcome-notify: gc mail send timed out after 15s",
	)
	require.Less(t, len(err.Error()), maxOutcomeNotifierFailureDetailBytes+256)

	executable = writeOutcomeNotifierFixture(t, dir, `#!/bin/sh
cat >/dev/null
head -c 5000 /dev/zero | tr '\0' x >&2
printf 'unbounded-tail' >&2
exit 1
`)
	notifier, err = NewCommandOutcomeNotifier(CommandOutcomeNotifierConfig{
		Executable: executable, WorkingDirectory: dir, Environment: []string{},
	})
	require.NoError(t, err)
	_, err = notifier.Deliver(context.Background(), OutcomeDeliveryRequest{
		Envelope: validOutcomeEnvelope(t), DeliveryCycle: "cycle-000001",
	})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "unbounded-tail")
	require.Less(t, len(err.Error()), maxOutcomeNotifierFailureDetailBytes+256)

	executable = writeOutcomeNotifierFixture(t, dir, `#!/usr/bin/env python3
import sys
sys.stdin.read()
sys.stderr.write(("x" * 4095) + "€unbounded-tail")
raise SystemExit(1)
`)
	notifier, err = NewCommandOutcomeNotifier(CommandOutcomeNotifierConfig{
		Executable: executable, WorkingDirectory: dir, Environment: []string{},
	})
	require.NoError(t, err)
	_, err = notifier.Deliver(context.Background(), OutcomeDeliveryRequest{
		Envelope: validOutcomeEnvelope(t), DeliveryCycle: "cycle-000001",
	})
	require.Error(t, err)
	require.True(t, utf8.ValidString(err.Error()))
	require.NotContains(t, err.Error(), "unbounded-tail")
	require.Less(t, len(err.Error()), maxOutcomeNotifierFailureDetailBytes+256)
}

func writeOutcomeNotifierFixture(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "outcome-notifier")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o700))
	return path
}
