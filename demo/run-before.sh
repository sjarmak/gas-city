#!/usr/bin/env bash
#
# run-before.sh -- the same kill, before Temporal. One command, no server.
#
# The coordinator is a reconcile loop (beforetick) that claims ready work,
# launches the same fakeagent the Temporal arms use, waits for it inline, and
# records the completion. The claim is durable; the procedure is process
# memory. The demo kills the loop mid-wait and restarts it, and the restart
# does exactly what the pre-Temporal system did:
#
#   1. it cannot tell that the first agent is still alive, so it declares the
#      claim stale and launches a second agent for the same work item;
#   2. when the first agent eventually finishes, the recovery scan records its
#      completion over the current one, because nothing fences generations.
#
# The gate at the end asserts the failure actually reproduced. A run where the
# duplicate or the overwrite did not happen exits non-zero, exactly like run.sh
# refuses to print a result it did not earn.
#
# Env knobs:
#   G1_TICKS  first agent's work length in 500ms ticks (default 40 = 20s)
#   GN_TICKS  relaunched agent's work length (default 8 = 4s)
set -uo pipefail

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS="$DEMO/harness"
OUT="$DEMO/out"
ART="$OUT/before-artifacts"
BIN="$OUT/bin"

G1_TICKS="${G1_TICKS:-40}"
GN_TICKS="${GN_TICKS:-8}"
WORK_ITEM="work-item-1"

export PATH="$PATH:$HOME/go/bin:$HOME/.local/bin"

log()  { printf '[demo] %s\n' "$*"; }
die()  { printf '[demo] FATAL: %s\n' "$*" >&2; cleanup; exit 1; }
rule() { printf '\n=========================================================================\n'; }
here() { printf '%s' "${1#"$DEMO"/}"; }

LIVE_PIDS=()
cleanup() {
  local pid
  for pid in "${LIVE_PIDS[@]:-}"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
  done
}
trap cleanup EXIT INT TERM

# ------------------------------------------------------------- 1. preflight
for tool in go jq python3; do
  command -v "$tool" >/dev/null 2>&1 || die "'$tool' not found on PATH"
done
pkill -9 -f "^$BIN/beforetick" 2>/dev/null
pkill -9 -f "^$BIN/fakeagent" 2>/dev/null

# ------------------------------------------------------------- 2. build
rm -rf "$ART"
mkdir -p "$ART/store" "$ART/agent" "$BIN"
for command_name in fakeagent beforetick; do
  ( cd "$HARNESS" && go build -o "$BIN/$command_name" "./cmd/$command_name" ) \
    || die "build $command_name"
done
log "built fakeagent and the before-world coordinator into $(here "$BIN")"

STORE="$ART/store/work.json"
AGENT_HOME="$ART/agent"

start_loop() {
  local label="$1"
  env DEMO_STORE_PATH="$STORE" DEMO_AGENT_BIN="$BIN/fakeagent" \
    AGENT_HOME="$AGENT_HOME" DEMO_WORK_ITEM="$WORK_ITEM" \
    BEFORE_G1_TICKS="$G1_TICKS" BEFORE_GN_TICKS="$GN_TICKS" \
    "$BIN/beforetick" >> "$ART/$label.log" 2>&1 &
  local pid=$!
  disown "$pid" 2>/dev/null
  printf '%s' "$pid"
}

# Samples how many agent processes exist, so "two agents for one work item"
# is measured rather than asserted.
: > "$ART/agents-live.jsonl"
(
  while true; do
    count="$(pgrep -c -f "^$BIN/fakeagent" 2>/dev/null || true)"
    jq -cn --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      --argjson n "${count:-0}" '{observed_at: $ts, agent_processes: $n}' \
      >> "$ART/agents-live.jsonl"
    sleep 0.25
  done
) &
SAMPLER=$!
disown "$SAMPLER" 2>/dev/null
LIVE_PIDS+=("$SAMPLER")

# ------------------------------------------------------------- 3. the run
rule
log "BEFORE TEMPORAL -- the coordinator holds the procedure in process memory"
LOOP_A="$(start_loop coordinator-first)"
LIVE_PIDS+=("$LOOP_A")
log "coordinator loop up; it claims the work and waits for the agent inline"

waited=0
until [ -f "$AGENT_HOME/sessions/$WORK_ITEM-g1/worklog.jsonl" ] &&
      [ "$(wc -l < "$AGENT_HOME/sessions/$WORK_ITEM-g1/worklog.jsonl")" -ge 3 ]; do
  (( waited++ > 240 )) && die "agent never started working"
  sleep 0.25
done
log "generation 1 agent is mid-work"

kill -9 "$LOOP_A" 2>/dev/null
sleep 1
ps -p "$LOOP_A" >/dev/null 2>&1 && die "coordinator survived kill -9"
survivors="$(pgrep -c -f "^$BIN/fakeagent" 2>/dev/null || true)"
echo "${survivors:-0}" > "$ART/agent-survivors.txt"
log "coordinator killed (kill -9). Agent processes still alive: ${survivors:-0}"
log "the claim survived in the store; everything around it just vanished"

sleep 3
rule
log "RESTART -- a new coordinator loop starts with an empty memory"
LOOP_B="$(start_loop coordinator-restarted)"
LIVE_PIDS+=("$LOOP_B")

# The duplicate: the restarted loop declares the claim stale and launches a
# second agent while the first is still working.
waited=0
until [ -f "$AGENT_HOME/sessions/$WORK_ITEM-g2/worklog.jsonl" ]; do
  (( waited++ > 240 )) && die "the restart never launched a second agent"
  sleep 0.25
done
live_now="$(pgrep -c -f "^$BIN/fakeagent" 2>/dev/null || true)"
log "the restart declared the claim stale and launched generation 2"
log "agent processes now working the same item: ${live_now:-0}"

# The overwrite: generation 2 records its completion, then generation 1
# finishes late and the recovery scan records it over the current receipt.
waited=0
until jq -e '.completion.generation == 1 and .completion.recorded_by == "recovery-scan"' \
      "$STORE" >/dev/null 2>&1; do
  (( waited++ > 480 )) && die "the stale overwrite never happened"
  sleep 0.25
done
log "generation 1 finished late; the recovery scan just recorded it over generation 2"

kill -9 "$LOOP_B" "$SAMPLER" 2>/dev/null
{ wait "$SAMPLER"; } 2>/dev/null

# ------------------------------------------------------------- 4. the gate
rule
log "final store state:"
jq '{status, claim, completion}' "$STORE"
python3 "$DEMO/verify-before.py" "$ART" | tee "$ART/verify-report.txt"
gate=${PIPESTATUS[0]}
rule
if [ "$gate" != 0 ]; then
  log "the failure did not reproduce. Nothing here is shippable."
  exit "$gate"
fi
log "artifacts: $(here "$ART")"
