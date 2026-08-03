# The recordings

Two asciinema recordings, both real runs, both exit 0, both re-recorded
2026-08-03 as tmux split screens against service revision `2b5df98`. Nothing
in either is staged: every `kill -9` is a real signal to a real process, and
the side panes measure with `pgrep`, `jq`, the `temporal` CLI, and the
worktree file itself rather than echoing the run's own log lines.

The process pane carries two counters, and their pairing is the story:
`agent procs` (pgrep) against `sessions` (distinct session identities that
have written the current work item). A retry re-binding reads procs 2 /
sessions 1; the before-world duplicate reads procs 2 / sessions 2.

- **Terminal:** 120x38, three panes: the run on the left, witnesses on the right.
- **`worker-kill.cast`** (50.1s): `run.sh`, both arms, with a process-liveness
  pane and a pane polling Temporal's own view (status, attempt, checkpoint).
- **`before-temporal.cast`** (31.8s): `run-before.sh`, the same kill against
  the pre-Temporal reconcile-loop coordinator, with the process pane and a
  pane reading the durable store. This is the arm where it goes wrong on
  purpose: two sessions for one task, then a stale receipt over the current one.

## The files, and which one to use

| File | Length | Timing | Use |
|---|---:|---|---|
| `worker-kill.cast` | 50.1s | real | source of truth; embed on a web page |
| `before-temporal.cast` | 31.8s | real | source of truth; embed on a web page |
| `*-faithful.mp4` / `.gif` | +3s | real | slots that cannot play a cast |

There is no talk cut anymore. The split screen's watcher panes repaint once
a second, so the cast has no idle gaps for `--idle-time-limit` to cap; a
shorter cut would need time-scaling, which changes what the recording claims.
If a slot needs under 30 seconds, play arm 2 only (27.5s to 42.1s) and say the
detection pause is real.

## Re-record

```bash
asciinema rec --cols 120 --rows 38 --overwrite \
  -c ./recording/record-before-session.sh recording/before-temporal.cast
KEEP_SERVER=1 asciinema rec --cols 120 --rows 38 --overwrite \
  -c ./recording/record-after-session.sh recording/worker-kill.cast
```

`KEEP_SERVER=1` leaves the dev server up so the Web UI screenshot can be
captured afterwards; if the server exits with the tmux session anyway,
restart it on the surviving database:

```bash
temporal server start-dev --port 7244 --ui-port 8244 \
  --db-filename out/run-artifacts/temporal-dev.db --log-level error
```

Each watcher composes its whole frame into a variable and paints it with one
`printf '\033[H\033[2J%s'`. Clearing first and then blocking on the temporal
CLI or jq leaves the pane empty until the body arrives, which showed up as a
blink once per tick in every earlier recording. Keep the compose-then-paint
shape when editing them.

The record scripts address tmux panes by id, never by index, because a user
config with `base-index` set silently breaks every `:0` target. The watcher
scripts print nothing wider than 39 columns and no absolute paths.

Renders: `agg --font-size 26 --idle-time-limit 60` then ffmpeg to H.264
yuv420p. The `--idle-time-limit` flag is inert on these casts (see above) but
stays explicit so a future single-pane re-record does not silently compress.

## worker-kill.cast beats

| At | Gap | What is happening |
|---:|---:|---|
| 0.0 | | Build against the read-only service checkout, revision recorded |
| 3.4 | | Dev server healthy, arm 1 starts |
| 8.3 | | `kill -9`. Worker one is gone. Processes pane: Worker 0, procs 1 |
| 15.8 | | Retry re-binds. Processes pane: procs 2, sessions 1 |
| 26.1 | **17.8s** | Temporal noticed, retried, workflow COMPLETED |
| 27.5 | | Arm 2 starts |
| 30.9 | | `kill -9` again, before any checkpoint exists |
| 38.8 | | Retry re-resolves; same session returned. Procs 2, sessions 1 |
| 40.7 | **9.8s** | Workflow COMPLETED |
| 42.1 | | Full invariant report, both arms |

The two gaps are the demo: the heartbeat timeout elapsing and Temporal
scheduling the next attempt. During both, the temporal pane shows the attempt
counter climbing and the processes pane shows the agent still alive. If the
slot only allows one arm, use arm 2; see "Arm 1 cannot be shown alone" in the
demo README.

## before-temporal.cast beats

| At | What is happening |
|---:|---|
| 2.1 | The coordinator claims generation 1; the agent starts |
| 4.6 | `kill -9`. Coordinator gone; processes pane: coordinator 0, procs 1 |
| 7.6 | Restart with an empty memory |
| 12.6 | The claim looks stale; a second session launches. Procs 2, sessions 2 |
| 24.2 | Generation 1 finishes late; the store pane flags the stale receipt |
| 26+ | The failure gate: every check must PASS for the run to count |

## What the runs showed

```
with Temporal:
arm 1  mid-execute      resolver calls = 1    session creations = 1   final attempt = 2
arm 2  pre-checkpoint   resolver calls = 2    session creations = 1   final attempt = 2
both                    one session, one receipt, stale claim token rejected

before Temporal:
one kill                sessions launched for one task = 2
                        final receipt = generation 1, recorded over generation 2
```
