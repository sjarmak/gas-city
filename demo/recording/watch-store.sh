#!/usr/bin/env bash
#
# watch-store.sh <store-relpath>
#
# Repaints the durable work store's view of one item: the claim generation,
# and whose completion is currently on the receipt. In the before-world run
# this pane is where the stale overwrite becomes visible.
#
# Run from the demo directory. Prints no absolute paths. Every line stays
# under 39 columns so nothing wraps in the recording's side pane.
set -u

STORE="${1:?store path}"

while true; do
  # One write per frame, composed first, so the pane is never shown empty.
  body="$(
    printf 'the store, as a restart reads it\n\n'
    if [ ! -f "$STORE" ]; then
      printf '  (no store yet)\n'
    else
      status="$(jq -r '.status // "?"' "$STORE" 2>/dev/null)"
      claim="$(jq -r '.claim.generation // "none"' "$STORE" 2>/dev/null)"
      receipt_gen="$(jq -r '.completion.generation // empty' "$STORE" 2>/dev/null)"
      receipt_by="$(jq -r '.completion.recorded_by // empty' "$STORE" 2>/dev/null)"
      printf '  %-9s %s\n' "status" "$status"
      printf '  %-9s generation %s\n' "claim" "$claim"
      if [ -z "$receipt_gen" ]; then
        printf '  %-9s (none)\n' "receipt"
      else
        printf '  %-9s generation %s\n' "receipt" "$receipt_gen"
        printf '  %-9s via %s\n' "" "$receipt_by"
        if [ "$claim" != "none" ] && [ "$receipt_gen" -lt "$claim" ] 2>/dev/null; then
          printf '\n  >> STALE RECEIPT: generation %s\n' "$receipt_gen"
          printf '  >> erased generation %s\n' "$claim"
        fi
      fi
    fi
  )"
  printf '\033[H\033[2J%s\n' "$body"
  sleep 1
done
