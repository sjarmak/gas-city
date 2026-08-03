# When durable execution earns its weight

We spent several weeks evaluating Temporal across a running multi-agent
orchestration installation: dozens of coding agents, a work ledger, a
recurring-job scheduler with roughly 150 scheduled jobs, a workflow-graph
engine, dispatchers, and a mesh of reliability scans. The method was to prove
where durability was actually exercised rather than adopt on faith, and the
epic was framed so that "adopt nowhere" was a legitimate outcome.

Most candidates failed. One survived. The negatives took more effort to prove
than the positive and are the more useful output, so this document records the
decision framework first and the verdicts second.

## Failure shapes decide the tool

Every observed stall in the installation was classified into one of three
shapes, because the shape determines the right mechanism:

- **Query-shaped**: level-triggered, answerable by a periodic scan against
  durable state ("any ready work item unclaimed longer than 30 minutes?"). A
  scan needs no timer, cannot drift, and re-derives itself every pass. No case
  for a workflow engine; a scan is strictly simpler and self-healing.
- **Event-shaped**: edge-triggered, needs a signal, and signals get lost at
  integration boundaries, so it also needs a reconciler. A workflow engine
  helps only if the signal source is itself durable; otherwise the reconciler
  is query-shaped and does the real work.
- **Timer-shaped**: genuinely needs a durable wakeup that survives process
  death ("no completion in N hours, escalate"). The only shape with a prima
  facie case for durable execution.

Mapping the whole reliability surface showed the large majority of leaks were
query-shaped, and most already had scan coverage. The recurring meta-defect was
not missing timers. It was scans that checked **liveness** (is the process
alive?) instead of **outcome** (did the artifact move?), and scans whose own
substrate could die unnoticed.

## The three-question adoption test

Before putting a workload behind a workflow engine, ask:

1. Does it run long enough to be crash-exposed mid-flight?
2. Does it wait on an external event?
3. Is the side effect irreversible?

If all three answers are no, the answer is cron. We measured this the
hard way: our first cutover moved a maintenance job doing 44 seconds of work
per 120-minute window onto a Temporal Schedule. Overlap protection fired zero
times and at that duty cycle essentially never will; there was no long-lived
state and no event wait. On that workload Temporal reduces to cron plus a
lockfile, and Schedules make the reduction easy to reach without noticing.

## The two-layer thesis

An orchestration system has exactly two reliability postures, and confusing
them is the catastrophic mistake we made once and documented:

- **Work layer** (the mutation itself): at-most-once, fail-closed. A retryable
  preflight that stamps nothing, a guarded single write, permanent defects
  recorded terminally, and a mandatory escalation hook. The escalation hook is
  not optional: our first fail-closed implementation, killed mid-side-effect,
  produced a poisoned pending claim, an orphaned work item, a FAILED workflow,
  and no escalation. It went silent. Fail-closed without escalation
  self-erases.
- **Watchdog layer** (everything watching the work): at-least-once,
  idempotent, re-derived from the source of record every tick. A watchdog
  built on fail-closed durability stops reporting at the crash, which is the
  one moment a watchdog must not.

A duplicate pull request is worse than a skipped cycle, so external mutations
get the work-layer posture. Everything supervisory gets the watchdog posture.
The two postures must never share an implementation.

## Signals advance, queries repair

Events are for latency; correctness always comes from a level-triggered
reconciler. Our reference pattern delivers escalations by event for speed and
runs a 15-minute scan for the lost-signal case. The counterexample also comes
from our own record: a signal-metadata contract rotted in place because the
fields the signal handlers depended on were never populated by the live path,
both handlers became permanent no-ops, and nothing noticed, because nothing
level-triggered reconciled the contract. A workflow parked on a signal that
has never once been delivered looks healthy from the outside.

## Watch outcome, not liveness

"Is the process alive" is the weaker question; "did the artifact move" is the
real one. A liveness-green worker can produce nothing: agents park on
interactive prompts while reading as active, and work items get closed while
the code never lands on the main branch. Counting our scan mesh made the bias
measurable: liveness checks outnumbered outcome checks. The highest-value scan
we run reads Git, not work-item status; it asks whether each closed item's
branch is an ancestor of main.

## Substrate independence

Every reliability scan in the installation runs on the recurring-job
scheduler, and the scheduler is what silently dies. One job sat dormant for
ten days, unnoticed, because the thing that would have checked it was itself a
scheduled job. A cover that shares its failure domain with the thing it guards
has moved the single point of failure, not removed it.

This was the one place the substrate argument for Temporal was real, and
Temporal still lost the pricing: a systemd user timer running the same scan
logic has the same failure-domain independence for this job, with zero new
state, zero new operational surface, and no memory footprint on a host that
had none to give.

## Verdicts

| Candidate | Verdict | Why |
| --- | --- | --- |
| Recurring-job scheduler (Schedules replacing cron-style jobs) | don't | 44s of work per 120-minute window; overlap protection never fired; cron plus a lockfile |
| Workflow-graph engine (supervisor workflow over multi-step jobs) | don't | Strongest on paper, but every stuck state traced to source bugs and missing scans, not lost durable state; a supervisor workflow over a buggy engine durably supervises the bug |
| Dispatchers (heartbeat-supervised) | don't | Kill-and-respawn already covers every observed failure; dispatch state re-derives from the store, so there is no in-flight state worth preserving |
| Watchdog substrate (scan-of-scans) | don't; systemd timer | Same failure-domain independence, zero new state, zero memory rent |
| External-mutation boundary (gated GitHub merge/push writes) | adopt | The one lane where crash-survivable exactly-once mutation and failure-domain independence compound; proven by a kill-mid-side-effect chaos test asserting exactly one recorded mutation after worker restart |
| Signal bridges | defer | Moot unless observe-mode data shows events the scan mesh misses and a latency need a real consumer has |
| Long-lived review-iterate workflow | defer, then adopt-shaped | The first workload passing all three questions: long-lived, event-waiting, human-gated; see the [walkthrough](durable-execution-walkthrough.md) |

Each "don't" carries a recorded condition that would change it. The scheduler
verdict flips if a job appears whose duty cycle actually exercises durability:
hours long, event-waiting, crash-exposed. The dispatcher verdict flips if a
failure mode appears that kill-and-respawn cannot repair. The watchdog verdict
flips if the scan-of-scans ever needs durable per-episode state a stateless
tick cannot carry, which the re-derive rule says it should not.

## The one adopted boundary, and what came after

The surviving lane, durable procedure around external mutations with the work
ledger kept authoritative for work facts, was later formalized as a full
orchestration boundary: Temporal owns procedure history, ordering, retries,
durable waits, and human gates; the ledger owns work identity, dependencies,
claims, generations, and artifacts; Activities own everything nondeterministic
and write back through generation-fenced conditional writes. That decision and
its verification gates are recorded in the
[ADR](adr-temporal-beads-boundary.md), and the reference implementation with
its chaos harness is the code in this repository.

The general rule that falls out: a workflow engine owns durable **procedure**
where the procedure itself must survive exactly: long-lived, event-waiting,
crash-exposed, irreversible. Everything else in an orchestration system is
either a fact (a ledger), a scan (level-triggered), or a disposable process.
Durability never launders a bad decision into a good one.

## Related documents

- [ADR: Temporal owns orchestration; Beads owns work facts](adr-temporal-beads-boundary.md)
- [Walkthrough: a hand-rolled poller against durable-execution primitives](durable-execution-walkthrough.md)
- [Feedback for Temporal from this evaluation](temporal-product-feedback.md)
