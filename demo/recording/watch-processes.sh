#!/usr/bin/env bash
#
# watch-processes.sh <label> <coordinator-binary-relpath> <worktree-dir-relpath>
#
# Repaints a compact liveness view: is the coordinating process alive, how
# many agent processes are alive, how many distinct sessions have written the
# current work item, and what the agent last wrote. This pane is the demo's
# witness that the agent outlives the process that launched it, so it
# measures with ps/pgrep and the worktree file itself rather than trusting
# anyone's log line. The process count and the session count are the point:
# a retry re-binding reads "procs 2 / sessions 1", a real duplicate reads
# "procs 2 / sessions 2".
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
  sessions=0
  # WORKTREES may itself carry a glob (the Temporal arms switch directories),
  # so it expands unquoted on purpose. Session identities are counted from
  # the newest file only: that is the current work item, and the glob spans
  # earlier arms' worktrees.
  # shellcheck disable=SC2086
  newest="$(ls -t $WORKTREES/*.txt 2>/dev/null | head -1)"
  if [ -n "$newest" ]; then
    line="$(tail -1 "$newest" 2>/dev/null | cut -d' ' -f2-)"
    if [ -n "$line" ]; then
      edit="${line%% by session *}"
      by="${line##* by session }"
    fi
    sessions="$(grep -o 'by session [^ ]*' "$newest" 2>/dev/null | sort -u | wc -l)"
  fi

  note=""
  if [ "$agents" -ge 1 ] && [ "$coordinator" -eq 0 ]; then
    note="agent outlived its launcher"
  fi
  # The notes follow the measurements. Two sessions in one work item is the
  # duplicate-launch failure wherever it appears; two processes over one
  # session is the retry re-binding, which is the fix working.
  if [ "$sessions" -ge 2 ]; then
    note="TWO sessions for one task"
  elif [ "$agents" -ge 2 ]; then
    if [ "$MODE" = "before" ]; then
      note="second process launched"
    else
      note="2 procs, ONE session: re-bound"
    fi
  fi

  # One write per frame, composed first, so the pane is never shown empty.
  body="$(
    printf 'pgrep + the worktree, once a second\n\n'
    printf '  %-12s %s\n' "$LABEL" "$coordinator"
    printf '  %-12s %s\n' "agent procs" "$agents"
    printf '  %-12s %s\n' "sessions" "$sessions"
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
