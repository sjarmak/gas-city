#!/usr/bin/env bash
#
# watch-temporal.sh <address>
#
# Repaints Temporal's own view of the newest workflow: execution status, the
# pending Activity's attempt counter, and whether a heartbeat checkpoint
# survives. During the two kill windows this pane is where Temporal can be
# seen noticing the dead Worker and scheduling the retry.
#
# Run from the demo directory. Prints no absolute paths.
set -u

ADDR="${1:?temporal address}"

while true; do
  # Compose the frame before touching the screen: clearing first and then
  # blocking on the temporal CLI left this pane blank for tens of
  # milliseconds every tick, which is the flicker in the recording.
  body="$(
    printf "temporal's own view, once a second\n\n"
    listing="$(temporal workflow list --address "$ADDR" -o json 2>/dev/null)"
    wf_id="$(printf '%s' "$listing" | jq -r '.[0].execution.workflowId // empty' 2>/dev/null)"
    if [ -z "$wf_id" ]; then
      printf '  (no workflow yet; dev server may still be starting)\n'
    else
      arm="$(printf '%s' "$wf_id" | cut -d/ -f3)"
      describe="$(temporal workflow describe -w "$wf_id" --address "$ADDR" -o json 2>/dev/null)"
      status="$(printf '%s' "$describe" | jq -r '.workflowExecutionInfo.status // "?"' \
        | sed 's/WORKFLOW_EXECUTION_STATUS_//')"
      attempt="$(printf '%s' "$describe" | jq -r '.pendingActivities[0].attempt // empty')"
      checkpoint="$(printf '%s' "$describe" | jq -r 'if .pendingActivities[0].heartbeatDetails then "survives" else "none" end')"
      failure="$(printf '%s' "$describe" | jq -r '.pendingActivities[0].lastFailure.message // empty')"
      printf '  %-10s %.26s\n' "workflow" "$arm"
      printf '  %-10s %.26s\n' "status" "$status"
      if [ -n "$attempt" ]; then
        printf '  %-10s %s\n' "attempt" "$attempt"
        printf '  %-10s %s\n' "checkpoint" "$checkpoint"
        [ -n "$failure" ] && printf '  %-10s %.25s\n' "last error" "$failure"
      else
        printf '  %-10s (none pending)\n' "activity"
      fi
    fi
  )"
  printf '\033[H\033[2J%s\n' "$body"
  sleep 1
done
