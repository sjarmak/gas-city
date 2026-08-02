# Worker-kill demo

One command. A local Temporal dev server, a Worker, one fixture work item. The
Worker is killed with `kill -9` twice, at two different points in the Activity,
and each time a replacement Worker picks the retry up, reattaches to the same
agent session, and writes exactly one generation-fenced terminal receipt. The
script prints the invariant checks and exits non-zero if any of them fails.

The two kills are one result. Arm 1 on its own passes even against a resolver
that mints a duplicate session on every call, so an arm 1 transcript does not
show what this demo exists to show. Read
[Arm 1 cannot be shown alone](#arm-1-cannot-be-shown-alone) before putting any
of this on a slide.

```bash
SERVICE=/path/to/services/temporal-maintenance ./run.sh
```

Set `SERVICE` once. After that a `.service-checkout` symlink remembers it and
plain `./run.sh` works. The checkout is read only; nothing here writes into it.

By default `SERVICE` now points at a **pinned tree**, not a live checkout:
`../services/temporal-maintenance`. That is a
`git archive` of the service at exactly the revision the recording was made
against. It has no `.git`, registers no worktree, and nothing can advance it.

The pin exists because the live checkout moves under you. On 2026-08-01 it went
`65c2edd` to `2b5df98` to `2e0740d` inside a few hours while the city worked in
it. Runs passed against each, which is worth something, but a demo whose
provenance changes between takes cannot back a recording.

Point `SERVICE` at the live checkout when you want to test against current
`HEAD`. Keep it on the pin when you are recording or presenting.

Every run records what it built against in `out/run-artifacts/provenance.json`.
A pinned tree carries its revision in a `.pinned-revision` marker, which
`run.sh` reads in preference to git. That ordering is deliberate: `git -C` walks
up the filesystem, so a pinned tree sitting inside any unrelated repository will
otherwise report *that* repository's HEAD. Provenance is checked to be right or
recorded as `unknown`; it is never allowed to be confidently wrong.

A recording of a full passing run is committed at
[`recording/worker-kill.cast`](recording/README.md): 41.7 seconds, both arms,
real `kill -9`, no staging.

## What is real and what is simulated

This matters more than the result, so it comes before the result.

**Real, running unmodified from the reviewed service:**

| | |
|---|---|
| `BeadOrchestrationWorkflow` | the procedure, on Task Queue `gascity-bead-orchestration` |
| `ExecuteBeadActivity` | fenced claim, start-or-attach, heartbeat, generation-fenced completion, on `gascity-agent-work` |
| `FileBeadStore` | the file-backed work store adapter, with its flock, atomic rename, and fence checks |
| `CommandAgentExecutor` | the production agent adapter, spawning a real child process and speaking the real newline-delimited JSON protocol |
| `ReadyEventBridge` | the real Signal-With-Start delivery path with outbox acknowledgement |
| Temporal | a real server, real Event History, real heartbeat-timeout detection, real retry |

The harness registers no Workflow and no Activity of its own. It supplies the
two injectable dependencies, starts the reviewed `WorkerSet`, and gets out of
the way. The `kill -9` is a real signal to a real process; the retry is
Temporal's, not the script's.

**Simulated:**

| | |
|---|---|
| The work store | a JSON file, not Dolt. `FileBeadStore` is real production code and enforces the same generation fence, but the canonical Gas City store is Dolt and this is not it. |
| The agent | `cmd/fakeagent`, which appends lines to a fixture file instead of editing a git worktree with a model. It speaks the real adapter protocol and holds a real durable session registry, so the code around it is exercised for real. The work it does is not. |
| Scale | one work item, one generation, one host, two Worker processes. |

No Dolt, no coding agent, no GitHub, no Slack, no production credential, no
network beyond localhost.

## Status, stated once

The unit this demo reproduces, work item to agent, is proved by a bounded canary
and runs in shadow mode. The part running continuously in production is result
delivery and acknowledgement, which is a second application of the same
boundary.

So: this is a fixture reproduction of an invariant on one host. It is not
evidence that Temporal-driven agent mutation is rolled out, that Activities
execute exactly once, or that recovery survives losing the whole host. Those
claims are not made here and are not supported by anything in `out/`.

## The two arms

`run.sh` runs the kill twice, at two different points, because the two points
prove different things.

**Arm 1, mid-execute.** The Worker dies while the agent is working and a
heartbeat checkpoint already exists. The retry resumes from that checkpoint and
never asks the resolver again. Meanwhile the agent process the dead Worker
started is still alive, its stdout pipe broken, still writing to the fixture
worktree. That orphan is the whole reason the boundary is shaped this way: the
Activity spawns something that can outlive the Worker that spawned it.

**Arm 2, pre-checkpoint.** The Worker dies during session resolution, before any
checkpoint exists. The retry has nothing to resume from, so it calls the
resolver again, and the resolver returns the session that is already registered
for that (work item, generation) pair rather than minting a second one.

Arm 2 exists to head off one specific misstatement. A heartbeat is not what
prevents a duplicate agent launch. A Worker can die before the first heartbeat ever
lands. What prevents the duplicate is the resolver finding the existing session
by stable identity. The heartbeat resumes progress on a retry; it does not stop
a competitor.

That distinction is visible in the sampled evidence rather than only asserted.
Compare `heartbeat_checkpoint_present` in each arm's `retry-gap.jsonl`: in arm 1
the pending Activity carries a checkpoint from the first attempt, and in arm 2
attempt one carries none. Both arms still finish with one session and one
receipt.

### Arm 1 cannot be shown alone

Arm 1 does not test duplicate-launch prevention. Its own passing check says why:
*the retry resumed from the checkpoint and never re-resolved*. A retry that
never calls the resolver cannot catch a resolver that is broken.

This was measured, not reasoned about. With the resolver patched to mint a fresh
session on every call, which is precisely the duplicate-launch defect this
boundary exists to prevent, arm 1 still printed 13 of 13 PASS and a clean
one-session, one-receipt transcript. Arm 2 failed immediately, because arm 2 is
the only arm that gives the resolver a second chance to misbehave.

So the headline claim is earned by both arms together and by neither alone.
Arm 1 is worth showing, but for what it actually demonstrates: an agent process
outliving the Worker that spawned it, and a retry resuming from a checkpoint. It
is not evidence that a second agent was prevented. Only arm 2 is.

`run.sh` runs both arms unconditionally and `verify.py` gates on both, so the
artifact is honest as it stands. The way to misrepresent it is to put half of it
on a slide.

## The invariants

`verify.py` reads the evidence each arm's `inspect` run wrote and asserts, per
arm:

1. The bound session identity did not change across attempts, and the work store
   recorded that same session.
2. Exactly one session was created for the (work item, generation) pair, and
   exactly one session record exists on disk for it.
3. One terminal receipt was written, the agent produced one terminal record, and
   a completion presenting the stale claim token after a generation advance
   failed closed with a stale-fence error.

Plus: one Activity identity in the Event History across both attempts, a final
attempt of two or later so the retry demonstrably happened, and a workflow that
closed on its own terms. Arm 1 additionally asserts that the orphaned agent kept
working after the kill and that exactly one process did the work. Arm 2
additionally asserts that the resolver was called twice and still created one
session.

The stale-token check advances the generation on a *copy* of the store file and
then replays the completion the finished attempt already used, so the archived
evidence is never mutated by the probe.

`retry-gap.jsonl` is sampled, not gated. It records the pending Activity while
Temporal sits between attempts, which is where the attempt counter climbs and
where a surviving heartbeat checkpoint is visible. A run whose gap closes faster
than the sampler leaves that file short, and says so, rather than failing.

Any failure prints `INVARIANT VIOLATION` and exits non-zero. `run.sh` propagates
that, so a run that did not earn its claim fails loudly rather than leaving a
plausible-looking transcript behind.

A per-arm PASS is not the claim, and a green arm 1 in isolation is specifically
not the claim. The gate is both arms passing, for the reason set out in
[Arm 1 cannot be shown alone](#arm-1-cannot-be-shown-alone).

## Timing, and why the heartbeat timeout is the knob

The Activity's start-to-close timeout is 24 hours and is fixed inside the
reviewed package. Waiting for it is not a demo. Heartbeat timeout is injectable
through the bridge's timing config, so that is what makes Temporal notice a dead
Worker in seconds. `HEARTBEAT` defaults to 8 seconds. The retry policy is the
real one: 1 second initial interval, coefficient 2, 1 minute cap, 5 maximum
attempts.

## What it writes

Everything lands under `out/`, which is disposable and gitignored.

```
out/bin/                     fakeagent, worker, drive, inspect
out/run-artifacts/
  provenance.json            which service revision this was built against
  temporal-dev.db            file-backed dev server database (history survives)
  <arm>/episode.json         workflow, run, and activity identity
  <arm>/kill.json            when worker one was killed
  <arm>/retry-gap.jsonl      pending Activity sampled while the retry gap is open
  <arm>/workflow-status.txt  the terminal status the workflow reached
  <arm>/agent-survivors.txt  agent processes still alive after the kill
  <arm>/receipt.json         what the work store ended up holding
  <arm>/invariants.json      the facts verify.py asserts on
  <arm>/worker-one.log, worker-two.log, drive.log, inspect.log
  verify-report.txt          the printed invariant report
```

`KEEP_SERVER=1` leaves the dev server up afterwards so the Web UI at
`http://127.0.0.1:8244` can be walked through. The eight formula Search
Attributes are not registered on a fresh dev server, so formula identity shows
up in Memo rather than as searchable attributes unless you register them first.

## Requirements

`go`, `temporal` (the CLI, which brings the dev server), `jq`, `python3`, and a
Unix host. A full run takes a couple of minutes, most of it waiting for
heartbeat timeouts and retry backoff.

Give it a quiet machine. The dev server keeps its state in SQLite, and on a host
that is already saturated it will report healthy and then time out its own
matching calls, which fails the run for reasons that have nothing to do with the
boundary being demonstrated. `run.sh` probes the Task Queue rather than trusting
the health endpoint, retries the server once, and prints load average and memory
alongside the server log when delivery fails, so that case is legible instead of
mysterious. Do not film on a loaded box.

## The before-world arm

`run-before.sh` reproduces the failure the conversion removed, with the same
kill. Its coordinator, `harness/cmd/beforetick`, is the pre-Temporal shape: a
reconcile loop that claims ready work, launches the same fakeagent over the
same protocol, waits for it inline, and records the completion, holding the
procedure in process memory. Three before-world behaviours are implemented on
purpose: no fence on completions (last writer wins), staleness inferred from
claim age (a live slow agent is indistinguishable from a dead one), and a
recovery scan that records whatever finished regardless of generation.

The run kills the loop mid-wait, restarts it, and `verify-before.py` asserts
the failure actually reproduced: a second agent launched for the same work
item while the first was alive, and the current receipt overwritten by the
stale generation. A run where the failure does not reproduce exits non-zero.
Artifacts land under `out/before-artifacts/`. No Temporal server is involved.

## Publishing evidence

`publish-artifacts.sh <target>` curates both runs' evidence into a directory
fit for the website: the gate reports, per-arm evidence files, and the
agent-side records, with process ids scrubbed from the JSONL and a hard check
that no absolute path ships.

## Layout

```
run.sh                   the whole sequence, one command
run-before.sh            the same kill against the pre-Temporal coordinator
verify.py                the invariant gate, readable without a Go toolchain
verify-before.py         the failure gate: the before-run must reproduce the bug
publish-artifacts.sh     curate both runs' evidence for the website
harness/cmd/worker       thin main() around the reviewed WorkerSet
harness/cmd/drive        transition one work item to ready, deliver it, seal the run
harness/cmd/fakeagent    stand-in agent speaking the real adapter protocol
harness/cmd/beforetick   the before-world reconcile loop, reconstructed
harness/cmd/inspect      gathers evidence, judges nothing
```

The harness is a separate Go module with a `replace` directive pointing at the
service checkout. Its module path is rooted under the service path because the
code it exercises lives in an `internal` package, and Go admits only importers
sharing that prefix. Nothing in the service is edited to make this run.
