#!/usr/bin/env bash
#
# record-before-session.sh -- the tmux session the before-Temporal cast records.
#
# Three panes: the run on the left, and on the right the two independent
# witnesses, the process table and the durable store. The watchers measure for
# themselves; nothing on the right trusts a log line on the left.
#
# Invoked by asciinema:
#   asciinema rec --cols 120 --rows 38 \
#     -c ./recording/record-before-session.sh before-temporal.cast
#
# Panes are addressed by their tmux ids, never by index: a user config with
# base-index set silently breaks every ":0" target.
set -u

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
S=tco-before-rec

tmux kill-session -t "$S" 2>/dev/null
tmux new-session -d -s "$S" -x 120 -y 38 -c "$DEMO" \
  "sleep 2; ./run-before.sh; sleep 8; tmux kill-session -t $S"
MAIN="$(tmux display-message -p -t "$S" '#{pane_id}')"
tmux set-option -t "$S" status off
tmux set-option -t "$S" -w pane-border-status top
tmux set-option -t "$S" -w pane-border-format " #{pane_title} "
tmux set-option -t "$S" -w pane-border-style "fg=colour240"
tmux set-option -t "$S" -w pane-active-border-style "fg=colour240"
PROC="$(tmux split-window -h -P -F '#{pane_id}' -t "$MAIN" -l 41 -c "$DEMO" \
  "./recording/watch-processes.sh coordinator out/bin/beforetick out/before-artifacts/agent/worktree")"
STOREPANE="$(tmux split-window -v -P -F '#{pane_id}' -t "$PROC" -l 18 -c "$DEMO" \
  "./recording/watch-store.sh out/before-artifacts/store/work.json")"
tmux select-pane -t "$MAIN" -T "the run: kill the coordinator, restart it"
tmux select-pane -t "$PROC" -T "processes"
tmux select-pane -t "$STOREPANE" -T "the store"
tmux select-pane -t "$MAIN"
exec tmux attach -t "$S"
