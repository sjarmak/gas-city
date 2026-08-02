#!/usr/bin/env bash
#
# record-after-session.sh -- the tmux session the worker-kill cast records.
#
# Three panes: run.sh on the left, and on the right two independent witnesses.
# The process pane proves the agent outlives the Worker with pgrep, and the
# Temporal pane shows the server's own status and attempt counter while it
# notices the dead Worker and retries. Neither watcher reads run.sh's output.
#
# Invoked by asciinema:
#   KEEP_SERVER=1 asciinema rec --cols 120 --rows 38 \
#     -c ./recording/record-after-session.sh worker-kill.cast
#
# KEEP_SERVER=1 leaves the dev server (and its Event History) up afterwards for
# the Web UI screenshots.
#
# Panes are addressed by their tmux ids, never by index: a user config with
# base-index set silently breaks every ":0" target.
set -u

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
S=tco-after-rec

tmux kill-session -t "$S" 2>/dev/null
tmux new-session -d -s "$S" -x 120 -y 38 -c "$DEMO" \
  "sleep 2; KEEP_SERVER=${KEEP_SERVER:-0} ./run.sh; sleep 8; tmux kill-session -t $S"
MAIN="$(tmux display-message -p -t "$S" '#{pane_id}')"
tmux set-option -t "$S" status off
tmux set-option -t "$S" -w pane-border-status top
tmux set-option -t "$S" -w pane-border-format " #{pane_title} "
tmux set-option -t "$S" -w pane-border-style "fg=colour240"
tmux set-option -t "$S" -w pane-active-border-style "fg=colour240"
PROC="$(tmux split-window -h -P -F '#{pane_id}' -t "$MAIN" -l 41 -c "$DEMO" \
  "./recording/watch-processes.sh Worker out/bin/worker 'out/run-artifacts/*/agent/worktree' after")"
TEMPORALPANE="$(tmux split-window -v -P -F '#{pane_id}' -t "$PROC" -l 18 -c "$DEMO" \
  "./recording/watch-temporal.sh 127.0.0.1:${PORT:-7244}")"
tmux select-pane -t "$MAIN" -T "the run: kill the Worker, twice"
tmux select-pane -t "$PROC" -T "processes"
tmux select-pane -t "$TEMPORALPANE" -T "temporal"
tmux select-pane -t "$MAIN"
exec tmux attach -t "$S"
