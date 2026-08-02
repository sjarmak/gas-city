#!/usr/bin/env bash
#
# publish-artifacts.sh <target-dir>
#
# Curates the evidence from the latest run.sh and run-before.sh runs into a
# directory fit for publishing on the website: the invariant reports, the
# per-arm evidence files, and the agent-side records that prove the agent
# worked across the kill.
#
# Two rules applied while copying:
#   1. Nothing with an absolute path ships (server.log stays behind).
#   2. Process ids are scrubbed from the agent-side JSONL records; a published
#      artifact proves the sequence, not one machine's process table.
set -euo pipefail

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${1:?usage: publish-artifacts.sh <target-dir>}"
AFTER="$DEMO/out/run-artifacts"
BEFORE="$DEMO/out/before-artifacts"

[ -f "$AFTER/verify-report.txt" ] || { echo "no run.sh artifacts; run ./run.sh first" >&2; exit 1; }
[ -f "$BEFORE/verify-report.txt" ] || { echo "no run-before.sh artifacts; run ./run-before.sh first" >&2; exit 1; }

scrub_pid_jsonl() {
  jq -c 'del(.pid)' "$1" > "$2"
}

rm -rf "$TARGET"
mkdir -p "$TARGET/worker-kill/mid-execute" "$TARGET/worker-kill/pre-checkpoint" \
  "$TARGET/before-temporal"

cp "$AFTER/provenance.json" "$TARGET/provenance.json"
cp "$AFTER/verify-report.txt" "$TARGET/worker-kill/verify-report.txt"
for arm in mid-execute pre-checkpoint; do
  src="$AFTER/$arm"
  dst="$TARGET/worker-kill/$arm"
  cp "$src/episode.json" "$src/workflow-status.txt" "$src/agent-survivors.txt" \
    "$src/retry-gap.jsonl" "$src/invariants.json" "$src/receipt.json" "$dst/"
  scrub_pid_jsonl "$src/agent/agent-events.jsonl" "$dst/agent-events.jsonl"
  scrub_pid_jsonl "$src/agent/sessions/work-item-1-g1/worklog.jsonl" "$dst/worklog-g1.jsonl"
done

cp "$BEFORE/verify-report.txt" "$TARGET/before-temporal/verify-report.txt"
cp "$BEFORE/store/work.json" "$TARGET/before-temporal/work.json"
cp "$BEFORE/agents-live.jsonl" "$TARGET/before-temporal/agents-live.jsonl"
cp "$BEFORE/agent-survivors.txt" "$TARGET/before-temporal/agent-survivors.txt"
cp "$BEFORE/agent/worktree/work-item-1.txt" "$TARGET/before-temporal/worktree-work-item-1.txt"
scrub_pid_jsonl "$BEFORE/agent/agent-events.jsonl" "$TARGET/before-temporal/agent-events.jsonl"

if grep -rlE "/(home|Users)/[a-z]" "$TARGET" >/dev/null 2>&1; then
  echo "FATAL: an absolute path reached the published set:" >&2
  grep -rlE "/(home|Users)/[a-z]" "$TARGET" >&2
  exit 1
fi
if grep -rl '"pid"' "$TARGET" >/dev/null 2>&1; then
  echo "FATAL: a process id reached the published set:" >&2
  grep -rl '"pid"' "$TARGET" >&2
  exit 1
fi
# Name the repository this demo ships in and its head revision, so a reader
# can resolve the published code even when service_revision_full names a
# commit from a private lineage. Derived from the checkout at publish time;
# absent when the demo does not live in one. The revision is the 12-char
# short form: it resolves on GitHub, and the site's identifier guard only
# allowlists one full-length hash (service_revision_full).
published_repo="$(git -C "$DEMO" remote get-url origin 2>/dev/null \
  | sed -E 's#^git@github\.com:#https://github.com/#; s#\.git$##' || true)"
published_revision="$(git -C "$DEMO" rev-parse --short=12 HEAD 2>/dev/null || true)"
if [ -n "$published_repo" ] && [ -n "$published_revision" ]; then
  jq --arg repo "$published_repo" --arg rev "$published_revision" \
    '. + {published_repo: $repo, published_revision: $rev}' \
    "$TARGET/provenance.json" > "$TARGET/provenance.json.tmp" \
    && mv "$TARGET/provenance.json.tmp" "$TARGET/provenance.json"
fi

# Hash every published file into the provenance record. A reader who downloads
# the set can then confirm these are the same bytes this run wrote, and a later
# accidental edit to one artifact stops being invisible.
hashes="$(cd "$TARGET" && find . -type f ! -name provenance.json | sort \
  | while read -r f; do
      printf '%s %s\n' "${f#./}" "$(sha256sum "$f" | cut -d' ' -f1)"
    done \
  | jq -Rn '[inputs | split(" ") | {key: .[0], value: .[1]}] | from_entries')"
jq --argjson files "$hashes" '. + {published_files_sha256: $files}' \
  "$TARGET/provenance.json" > "$TARGET/provenance.json.tmp" \
  && mv "$TARGET/provenance.json.tmp" "$TARGET/provenance.json"

echo "published evidence set -> $TARGET"
find "$TARGET" -type f | sort | sed "s|^$TARGET/|  |"
