#!/usr/bin/env bash
#
# watch-processes.sh <label> <coordinator-binary-relpath> <worktree-dir-relpath>
#
# Repaints a compact liveness view: is the coordinating process alive, is the
# agent process alive, and what did the agent last write. This pane is the
# demo's witness that the agent outlives the process that launched it, so it
# measures with ps/pgrep rather than trusting anyone's log line.
#
# Run from the demo directory. Prints no absolute paths. Every line stays
# under 39 columns so nothing wraps in the recording's side pane.
set -u

LABEL="${1:?label}"
BINARY="${2:?coordinator binary}"
WORKTREES="${3:?worktree dir}"
# "before" brands a second agent process as the failure it is there; "after"
# explains it, because the retry's re-bind to the same session is the fix
# working, not a duplicate.
MODE="${4:-before}"
ROOT="$(pwd)"

while true; do
  coordinator="$(pgrep -c -f "^$ROOT/$BINARY" 2>/dev/null || true)"
  agents="$(pgrep -c -f "^$ROOT/out/bin/fakeagent" 2>/dev/null || true)"
  coordinator="${coordinator:-0}"
  agents="${agents:-0}"

  edit="(no edits yet)"
  by=""
  # WORKTREES may itself carry a glob (the Temporal arms switch directories),
  # so it expands unquoted on purpose.
  # shellcheck disable=SC2086
  newest="$(ls -t $WORKTREES/*.txt 2>/dev/null | head -1)"
  if [ -n "$newest" ]; then
    line="$(tail -1 "$newest" 2>/dev/null | cut -d' ' -f2-)"
    if [ -n "$line" ]; then
      edit="${line%% by session *}"
      by="${line##* by session }"
    fi
  fi

  note=""
  if [ "$agents" -ge 1 ] && [ "$coordinator" -eq 0 ]; then
    note="agent outlived its launcher"
  fi
  if [ "$agents" -ge 2 ]; then
    if [ "$MODE" = "before" ]; then
      note="TWO agents for one task"
    else
      note="retry re-binds the same session"
    fi
  fi

  # One write per frame, composed first, so the pane is never shown empty.
  body="$(
    printf 'measured with pgrep, once a second\n\n'
    printf '  %-12s %s\n' "$LABEL" "$coordinator"
    printf '  %-12s %s\n' "agent" "$agents"
    if [ -n "$note" ]; then
      printf '\n  >> %.34s\n' "$note"
    else
      printf '\n\n'
    fi
    printf '\n  last agent edit:\n'
    printf '  %.37s\n' "$edit"
    [ -n "$by" ] && printf '  by %.34s\n' "$by"
  )"
  printf '\033[H\033[2J%s\n' "$body"
  sleep 1
done
