#!/usr/bin/env bash
#
# run.sh -- the whole worker-kill demo, end to end, in one command.
#
# Two arms, both against a local Temporal dev server with a file-backed
# database so the Web UI still has the history afterwards. Neither arm needs
# Dolt, a real coding agent, GitHub, or any production credential.
#
#   arm 1  mid-execute      the Worker is killed while the agent is working.
#                           The retry resumes from the heartbeat checkpoint.
#   arm 2  pre-checkpoint   the Worker is killed while the agent session is
#                           still being resolved, so no checkpoint exists at
#                           all. The retry has to ask the resolver again, and
#                           the resolver returns the session that already
#                           exists. This is the arm that shows the heartbeat is
#                           not what prevents a duplicate agent.
#
# Both arms end at the same place: one bound session, one terminal receipt, and
# a stale claim token that fails closed.
#
# This script runs and verifies. It does not record video and it does not push
# anything anywhere.
#
# Env knobs:
#   SERVICE      service checkout to build against (read-only)
#   PORT/UIPORT  dev server ports
#   HEARTBEAT    Activity heartbeat timeout in seconds (kill detection speed)
#   WAIT_BUDGET  seconds to wait for a terminal workflow status per arm
#   KEEP_SERVER  1 leaves the dev server running for a Web UI walkthrough
#
# Everything this script prints is meant to be safe to put on a screen, so paths
# are shown relative to this directory rather than as one machine's layout.
set -uo pipefail

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS="$DEMO/harness"
OUT="$DEMO/out"
ART="$OUT/run-artifacts"
BIN="$OUT/bin"

# SERVICE is the temporal-maintenance checkout to build against. It is read
# only. Set it once; after that the .service-checkout symlink remembers it.
SERVICE="${SERVICE:-}"
if [ -z "$SERVICE" ] && [ -L "$DEMO/.service-checkout" ]; then
  SERVICE="$(readlink -f "$DEMO/.service-checkout")"
fi
PORT="${PORT:-7244}"
UIPORT="${UIPORT:-8244}"
ADDR="127.0.0.1:$PORT"
HEARTBEAT="${HEARTBEAT:-8}"
WAIT_BUDGET="${WAIT_BUDGET:-180}"
KEEP_SERVER="${KEEP_SERVER:-0}"

WORK_ITEM="work-item-1"
CITY_ID="demo-city"
AGENT_QUEUE="gascity-agent-work"

export PATH="$PATH:$HOME/go/bin:$HOME/.local/bin"

log()  { printf '[demo] %s\n' "$*"; }
die()  { printf '[demo] FATAL: %s\n' "$*" >&2; cleanup; exit 1; }
rule() { printf '\n=========================================================================\n'; }

# here strips the demo directory off a path so nothing printed to the terminal
# carries this machine's directory layout into a screen recording.
here() { printf '%s' "${1#"$DEMO"/}"; }

# server_diagnosis distinguishes "the harness is wrong" from "this host cannot
# currently run a Temporal server", which look identical from a timeout.
server_diagnosis() {
  printf '[demo] --- why it failed ---\n' >&2
  printf '[demo] host load average: %s\n' \
    "$(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)" >&2
  printf '[demo] memory available: %s\n' \
    "$(free -m 2>/dev/null | awk '/^Mem:/{print $7" MiB"}')" >&2
  if grep -qE 'context deadline exceeded|disk I/O error' "$ART/server.log" 2>/dev/null; then
    printf '[demo] the dev server logged persistence or matching timeouts.\n' >&2
    printf '[demo] on a saturated host that is the host, not the harness.\n' >&2
  fi
  tail -5 "$ART/server.log" 2>/dev/null >&2
}

SERVER_PID=""
LIVE_PIDS=()

cleanup() {
  local pid
  for pid in "${LIVE_PIDS[@]:-}"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
  done
  if [ "$KEEP_SERVER" != "1" ] && [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null
  fi
}
trap cleanup EXIT INT TERM

# ------------------------------------------------------------- 1. preflight
for tool in go temporal jq python3; do
  command -v "$tool" >/dev/null 2>&1 || die "'$tool' not found on PATH"
done
[ -n "$SERVICE" ] || die "set SERVICE to a temporal-maintenance checkout (see README.md)"
[ -d "$SERVICE" ] || die "SERVICE does not point at a directory"
[ -f "$SERVICE/internal/temporalbeads/workflow.go" ] \
  || die "SERVICE is not a temporal-maintenance checkout"
[ -f "$DEMO/verify.py" ] || die "missing $(here "$DEMO/verify.py")"

# A worker or agent left behind by an interrupted earlier run holds the same
# store and the same Task Queues, which would quietly corrupt this run's
# evidence. Only this demo's own binaries are matched.
pkill -9 -f "^$BIN/worker" 2>/dev/null
pkill -9 -f "^$BIN/fakeagent" 2>/dev/null

# Provenance must be right or absent, never confidently wrong.
#
# A live checkout answers with its HEAD. A pinned tree (see .service-pins) has
# no git metadata on purpose and carries a marker file instead. The marker is
# checked FIRST, and the git answer is only trusted once we confirm the found
# repository actually tracks this directory: `git -C` walks UP the filesystem,
# so a pinned tree that happens to sit inside some unrelated repository will
# otherwise report that repository's HEAD. That is not a missing revision, it
# is a wrong one attributed to the wrong project.
if [[ -f "$SERVICE/.pinned-revision" ]]; then
  REVISION_FULL="$(tr -d '[:space:]' < "$SERVICE/.pinned-revision")"
  REVISION="$(printf '%.7s' "$REVISION_FULL")"
elif git -C "$SERVICE" ls-files --error-unmatch go.mod >/dev/null 2>&1; then
  REVISION_FULL="$(git -C "$SERVICE" rev-parse HEAD)"
  REVISION="$(git -C "$SERVICE" rev-parse --short HEAD)"
else
  REVISION_FULL=unknown
  REVISION=unknown
fi
log "building against the service checkout (read-only; revision recorded in the artifacts)"

# ------------------------------------------------------------- 2. build
rm -rf "$ART"
mkdir -p "$ART" "$BIN"
# The replace directive resolves through this symlink, so the module file never
# has to name one machine's directory layout.
ln -sfn "$SERVICE" "$DEMO/.service-checkout" || die "could not point .service-checkout at SERVICE"
for command_name in fakeagent worker drive inspect; do
  ( cd "$HARNESS" && go build -o "$BIN/$command_name" "./cmd/$command_name" ) \
    || die "build $command_name"
done
log "built fakeagent, worker, drive, inspect into $(here "$BIN")"
# Provenance is what lets a reader tie this evidence to the code that produced
# it. A short revision alone does not: it does not say which repository, which
# SDK, or which machine, so a reviewer has to reconstruct all of it.
jq -n \
  --arg rev "$REVISION" \
  --arg rev_full "$REVISION_FULL" \
  --arg module "$(awk '/^module /{print $2; exit}' "$SERVICE/go.mod" 2>/dev/null)" \
  --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg go_toolchain "$(go version 2>/dev/null)" \
  --arg sdk "$(awk '/go\.temporal\.io\/sdk /{print $2; exit}' "$SERVICE/go.mod" 2>/dev/null)" \
  --arg api "$(awk '/go\.temporal\.io\/api /{print $2; exit}' "$SERVICE/go.mod" 2>/dev/null)" \
  --arg cli "$(temporal --version 2>/dev/null | head -1)" \
  --arg host "$(uname -sr) $(uname -m)" \
  --arg heartbeat "$HEARTBEAT" \
  --arg run_command "HEARTBEAT=$HEARTBEAT PORT=$PORT ./run.sh" \
  '{service_revision: $rev, service_revision_full: $rev_full, service_module: $module,
    built_at: $at, go_toolchain: $go_toolchain, temporal_sdk: $sdk, temporal_api: $api,
    temporal_cli: $cli, host: $host, heartbeat_timeout_seconds: ($heartbeat | tonumber),
    run_command: $run_command}' > "$ART/provenance.json"

# ------------------------------------------------------------- 3. dev server
#
# A file-backed database, not the default in-memory one, so the Event History
# survives the run and the Web UI still has something to show afterwards.
start_dev_server() {
  # Refuse to touch ports that belong to something else. This box runs a
  # production Temporal on 7233 with the city's maintenance worker attached to
  # it, and the pkill below matches on the port, so `PORT=7233 ./run.sh` would
  # kill a live production server. The demo owns 7244/8244 and nothing else.
  for reserved in 7233 8233; do
    if [[ "$PORT" == "$reserved" || "$UIPORT" == "$reserved" ]]; then
      die "refusing to use port $reserved: it belongs to the production Temporal server, not this demo"
    fi
  done

  # Scope the kill to a server this demo started. Matching only on the port
  # would match any start-dev on that port, including one a human is using.
  pkill -f "start-dev --port $PORT --ui-port $UIPORT --db-filename $ART/temporal-dev.db" 2>/dev/null
  local waited=0
  while ss -ltn 2>/dev/null | grep -q ":$PORT "; do
    (( waited++ > 20 )) && return 1
    sleep 1
  done
  # The sidecars matter: a stale write-ahead log against a fresh database file
  # fails schema setup with an I/O error that reads like a disk fault.
  rm -f "$ART/temporal-dev.db" "$ART/temporal-dev.db-shm" "$ART/temporal-dev.db-wal"
  nohup temporal server start-dev \
    --port "$PORT" --ui-port "$UIPORT" \
    --db-filename "$ART/temporal-dev.db" \
    --log-level error >> "$ART/server.log" 2>&1 &
  SERVER_PID=$!
  local started=$SECONDS
  until temporal operator cluster health --address "$ADDR" >/dev/null 2>&1; do
    ps -p "$SERVER_PID" >/dev/null 2>&1 || return 1
    (( SECONDS - started > 60 )) && return 1
    sleep 1
  done
  # Health goes SERVING before matching can actually serve a Task Queue, and on
  # a loaded host the gap is wide enough to swallow the first delivery. Probe
  # the path the demo is about to use rather than trusting the health endpoint.
  started=$SECONDS
  until temporal task-queue describe --task-queue "$AGENT_QUEUE" \
        --address "$ADDR" >/dev/null 2>&1; do
    ps -p "$SERVER_PID" >/dev/null 2>&1 || return 1
    (( SECONDS - started > 60 )) && return 1
    sleep 1
  done
  return 0
}

if ! start_dev_server; then
  log "dev server did not come up; retrying once"
  if ! start_dev_server; then
    printf '%s\n' "--- server log ---" >&2
    tail -20 "$ART/server.log" >&2
    die "dev server never became healthy (see $(here "$ART/server.log"))"
  fi
fi
log "dev server healthy on $ADDR (Web UI http://127.0.0.1:$UIPORT)"

# ------------------------------------------------------------- 4. one arm
#
# run_arm <arm> <run-id> <description> <trigger> <worker1 agent env> <worker2 agent env>
#
# trigger names the condition run.sh waits for before it pulls the trigger on
# worker one, which is what puts the kill in a known place instead of a lucky one.
run_arm() {
  local arm="$1" run_id="$2" description="$3" trigger="$4"
  local worker1_env="$5" worker2_env="$6"

  local dir="$ART/$arm"
  local store="$dir/store/work.json"
  local agent_home="$dir/agent"
  mkdir -p "$dir/store" "$agent_home"

  rule
  log "ARM: $arm -- $description"

  # --- worker one
  local w1
  # shellcheck disable=SC2086
  env $worker1_env \
    TEMPORAL_ADDRESS="$ADDR" DEMO_WORKER_LABEL="worker-one" \
    DEMO_STORE_PATH="$store" DEMO_AGENT_BIN="$BIN/fakeagent" \
    AGENT_HOME="$agent_home" \
    "$BIN/worker" > "$dir/worker-one.log" 2>&1 &
  w1=$!
  # Drop it from the job table. A tracked job that dies makes the shell announce
  # the death later, printing the whole command line and this machine's paths
  # into whatever is recording the screen. Liveness is checked with ps instead.
  disown "$w1" 2>/dev/null
  LIVE_PIDS+=("$w1")
  sleep 2
  ps -p "$w1" >/dev/null 2>&1 || die "$arm: worker one died on startup (see $(here "$dir")/worker-one.log)"

  # --- start the episode
  TEMPORAL_ADDRESS="$ADDR" DEMO_STORE_PATH="$store" \
    DEMO_CITY_ID="$CITY_ID" DEMO_RUN_ID="$run_id" DEMO_WORK_ITEM="$WORK_ITEM" \
    DEMO_HEARTBEAT_SECONDS="$HEARTBEAT" DEMO_DRIVE_OUT="$dir/episode.json" \
    "$BIN/drive" > "$dir/drive.log" 2>&1 \
    || { server_diagnosis; die "$arm: drive failed (see $(here "$dir")/drive.log)"; }

  local workflow_id activity_id
  workflow_id="$(jq -r '.workflow_id' "$dir/episode.json")"
  activity_id="$(jq -r '.activity_id' "$dir/episode.json")"
  log "$arm: workflow $workflow_id"
  log "$arm: activity $activity_id (stable across attempts by construction)"

  # --- wait for the kill point
  local waited=0 ready=0
  while (( waited < 60 )); do
    case "$trigger" in
      agent-working)
        if [ -f "$agent_home/sessions/$WORK_ITEM-g1/worklog.jsonl" ] &&
           [ "$(wc -l < "$agent_home/sessions/$WORK_ITEM-g1/worklog.jsonl")" -ge 3 ]; then
          ready=1
        fi
        ;;
      session-registered)
        if [ -f "$agent_home/agent-events.jsonl" ] &&
           grep -q '"kind":"session-created"' "$agent_home/agent-events.jsonl"; then
          ready=1
        fi
        ;;
      *) die "unknown trigger $trigger" ;;
    esac
    [ "$ready" = 1 ] && break
    sleep 0.25
    waited=$(( waited + 1 ))
  done
  [ "$ready" = 1 ] || die "$arm: kill point '$trigger' never reached"

  # --- the kill
  local agent_pids_before
  agent_pids_before="$(pgrep -P "$w1" -f fakeagent | tr '\n' ' ')"
  kill -9 "$w1" 2>/dev/null
  jq -n --arg t "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" '{killed_at: $t}' > "$dir/kill.json"
  sleep 1
  ps -p "$w1" >/dev/null 2>&1 && die "$arm: worker one survived kill -9"
  log "$arm: worker one is gone (kill -9, no shutdown hook, no cleanup)"

  # Does the agent it started still exist? This is the property the whole
  # boundary is designed around, so it is measured rather than asserted.
  local survivors=0 pid
  for pid in $agent_pids_before; do
    ps -p "$pid" >/dev/null 2>&1 && survivors=$(( survivors + 1 ))
  done
  echo "$survivors" > "$dir/agent-survivors.txt"
  log "$arm: agent processes still alive after the kill: $survivors"

  # --- watch the retry gap
  #
  # Best-effort. It samples the pending Activity while Temporal is between
  # attempts, which is where the attempt counter climbs and where the surviving
  # heartbeat checkpoint (or its absence) is visible. Nothing is gated on this
  # file: a run that samples too slowly leaves it short rather than wrong.
  : > "$dir/retry-gap.jsonl"
  (
    while true; do
      temporal workflow describe -w "$workflow_id" --address "$ADDR" -o json 2>/dev/null \
        | jq -c --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            '.pendingActivities // [] | .[] | {
               observed_at: $ts,
               activity_id: .activityId,
               attempt: .attempt,
               state: .state,
               heartbeat_checkpoint_present: (.heartbeatDetails != null),
               last_failure: (.lastFailure.message // null)
             }' >> "$dir/retry-gap.jsonl" 2>/dev/null
      sleep 0.25
    done
  ) &
  local watcher=$!
  LIVE_PIDS+=("$watcher")

  # --- worker two
  local w2
  # shellcheck disable=SC2086
  env $worker2_env \
    TEMPORAL_ADDRESS="$ADDR" DEMO_WORKER_LABEL="worker-two" \
    DEMO_STORE_PATH="$store" DEMO_AGENT_BIN="$BIN/fakeagent" \
    AGENT_HOME="$agent_home" \
    "$BIN/worker" > "$dir/worker-two.log" 2>&1 &
  w2=$!
  LIVE_PIDS+=("$w2")
  log "$arm: replacement worker up; waiting for a terminal workflow status (up to ${WAIT_BUDGET}s)"

  # --- wait for terminal
  local status="" lowered started_wait=$SECONDS
  while true; do
    status="$(temporal workflow describe -w "$workflow_id" --address "$ADDR" -o json 2>/dev/null \
      | jq -r '.workflowExecutionInfo.status // empty')"
    lowered="$(printf '%s' "$status" | tr 'A-Z' 'a-z')"
    case "$lowered" in
      *completed*|*failed*|*terminated*|*timed*|*canceled*) break ;;
    esac
    (( SECONDS - started_wait > WAIT_BUDGET )) && die "$arm: no terminal status after ${WAIT_BUDGET}s (last=${status:-none})"
    sleep 2
  done
  kill -9 "$watcher" 2>/dev/null
  { wait "$watcher"; } 2>/dev/null
  echo "$status" > "$dir/workflow-status.txt"
  log "$arm: workflow reached $status"

  local attempts_seen
  attempts_seen="$(jq -rs '[.[].attempt] | unique | join(", ")' "$dir/retry-gap.jsonl" 2>/dev/null)"
  if [ -n "$attempts_seen" ] && [ "$attempts_seen" != "" ]; then
    log "$arm: Activity attempts observed while the retry gap was open: $attempts_seen"
  else
    log "$arm: the retry gap closed faster than the sampler; see Event History instead"
  fi

  kill -TERM "$w2" 2>/dev/null
  sleep 1
  kill -9 "$w2" 2>/dev/null
  { wait "$w2"; } 2>/dev/null

  # --- collect the evidence
  DEMO_ARM="$arm" DEMO_ARM_DESCRIPTION="$description" \
    DEMO_STORE_PATH="$store" AGENT_HOME="$agent_home" \
    DEMO_ARTIFACT_DIR="$dir" DEMO_WORK_ITEM="$WORK_ITEM" \
    TEMPORAL_ADDRESS="$ADDR" DEMO_SERVICE_REVISION="$REVISION" \
    "$BIN/inspect" > "$dir/inspect.log" 2>&1 \
    || die "$arm: inspect failed (see $(here "$dir")/inspect.log)"
  log "$arm: evidence written to out/run-artifacts/$arm/"
}

# ------------------------------------------------------------- 5. the arms
run_arm mid-execute "worker-kill-demo" \
  "Worker killed while the agent is working; the retry resumes from the checkpoint" \
  agent-working \
  "AGENT_RESOLVE_DELAY_MS=0 AGENT_EXECUTE_TICKS=40 AGENT_TICK_MS=500 AGENT_ATTACH_TIMEOUT_MS=120000" \
  "AGENT_RESOLVE_DELAY_MS=0 AGENT_EXECUTE_TICKS=40 AGENT_TICK_MS=500 AGENT_ATTACH_TIMEOUT_MS=120000"

run_arm pre-checkpoint "worker-kill-demo-resolve" \
  "Worker killed before any checkpoint exists; the retry has to resolve again" \
  session-registered \
  "AGENT_RESOLVE_DELAY_MS=25000 AGENT_EXECUTE_TICKS=4 AGENT_TICK_MS=500 AGENT_ATTACH_TIMEOUT_MS=60000" \
  "AGENT_RESOLVE_DELAY_MS=0 AGENT_EXECUTE_TICKS=4 AGENT_TICK_MS=500 AGENT_ATTACH_TIMEOUT_MS=60000"

# ------------------------------------------------------------- 6. the gate
rule
python3 "$DEMO/verify.py" "$ART" | tee "$ART/verify-report.txt"
gate=${PIPESTATUS[0]}
rule
if [ "$gate" != 0 ]; then
  log "the run did not earn its claim. Nothing here is shippable."
  exit "$gate"
fi
log "artifacts: $(here "$ART")"
if [ "$KEEP_SERVER" = "1" ]; then
  log "dev server left running: http://127.0.0.1:$UIPORT (KEEP_SERVER=1)"
fi
