# ADR: Temporal Owns Orchestration; Beads Owns Work Facts

**Date**: 2026-07-30 (amended 2026-08-02: persistence gate)
**Status**: accepted
**Supersedes**: an earlier draft constraint, "Temporal watches, never owns,"
which conflated the work ledger with ownership of the execution procedure.

This is the decision record behind the code in
[`services/temporal-maintenance/internal/temporalbeads`](../services/temporal-maintenance/internal/temporalbeads).
Internal tracker identifiers have been removed; dates and evidence stand.

## Context

Our first Temporal pilot correctly separated deterministic Workflow code from
nondeterministic Activities, but it stopped at the wrong boundary. Its Activity
created a bead (a work item in the Beads ledger) and dispatched an agent, then
returned. The installation's pull-based scheduler, claim hooks, nudges, and
reapers continued to own the agent execution lifecycle. Temporal therefore
recorded coordination around an execution controlled elsewhere.

The draft constraint generalized that pilot boundary into "Temporal watches,
never owns" and "never block an Activity on an agent session." Those statements
conflate two independent ownership questions:

- which system records work identity, dependencies, claims, and artifacts; and
- which system records and advances the durable execution procedure.

A prior decision had already moved workers off self-discovery: a scheduler
binds one ready workload to one execution slot before launch, and the worker
receives the bead ID, generation, and fencing token. This ADR selects Temporal
as the durable procedure engine and execution controller for that model. It
does not move the work facts out of Beads.

## Decision

Beads and Temporal have separate authoritative domains:

| Domain | Authority |
| --- | --- |
| Work identity, readiness, dependencies, priority, binding generation, fencing token, claim state, and artifact/review facts | Beads |
| Procedure history, ordering, retries, durable waits, cancellation, fan-out/join, and human gates | Temporal Workflow |
| Filesystem, model calls, agent sessions, tests, Git operations, and conditional writes back to Beads | Temporal Activity Worker |

A worker process polls a Temporal Task Queue. Activity Workers do not poll
Beads, choose their own bead, race another worker to claim it, or depend on a
nudge to discover work.

### Ready-to-execution path

```text
Bead transition to ready
  -> durable outbox event: (version, event ID, city, run, bead, generation, ready_at)
  -> Signal-With-Start to the stable orchestration Workflow ID
  -> Workflow deduplicates the deterministic event ID
  -> Workflow schedules ExecuteBead on the agent Task Queue
  -> Activity Worker conditionally acquires the exact generation
  -> Activity Worker starts or reattaches the agent execution
  -> Activity heartbeats the session and artifact checkpoint
  -> Activity conditionally records the result with the same fencing token
  -> Workflow advances review, repair, human-gate, or terminal transitions
```

Signal-With-Start uses a stable Workflow ID derived from installation identity
and orchestration-run identity. Duplicate delivery of the same ready event is a
no-op in Workflow state. A closed Workflow ID is not silently reused for the
same run.

The producer seals a finite orchestration run with a typed close request
containing the authoritative set of ready-event IDs for that run. The Workflow
does not close until it has received that complete set and every scheduled
Activity is terminal. This makes close-before-ready delivery safe: closure
waits for the missing durable event instead of discarding it. Emitting new work
after the authoritative run is sealed is a producer contract violation and
requires a new run identity. A deterministic event limit fails closed below
Temporal's history ceiling if a producer violates the finite-run contract;
unbounded orchestration runs are not supported.

### Fenced Activity contract

Every agent Activity input contains the bead identity and ready generation.
Before any agent process starts, the Activity obtains a lease containing:

- the same bead identity;
- the exact generation;
- an opaque fencing token; and
- the owning Workflow identity.

Retries by the same Workflow reacquire the same lease. A different generation
or token is a non-retryable stale-claim error. Completion, attempt-failure, and
artifact writes all include the generation and token; the Beads write rejects a
zombie executor after rebinding.

Agent execution is idempotent by claim token. The Activity resolves the stable
session identity before execution begins, so cancellation can target the
attached session even before its first checkpoint. Retrying an Activity
attaches to that session or resumes from its heartbeat checkpoint instead of
launching another agent. Heartbeats contain the bead identity, generation,
claim token, session identity, monotonic checkpoint sequence, phase, and
artifact references. Prompts, transcripts, diffs, and logs remain outside
Workflow history.

### Reconciliation

The event bridge is the normal path. A narrow reconciler compares durable ready
outbox records with exact Workflow receipts for `(bead, generation)` and
redelivers only a missing event through the same Signal-With-Start operation.
It does not choose work, bind capacity, launch agents, or maintain a second
scheduler. A newer ready generation transactionally supersedes any
unacknowledged older generation for the same bead, so outage recovery never
schedules a known-stale Activity.

### Failure-domain boundary

Temporal is not a prerequisite for:

- inspecting or repairing Beads;
- starting the terminal multiplexer that hosts agent sessions;
- recovering the process supervisor or the managed database;
- running host-pressure guards and reapers; or
- reading the binding record that fences a stale executor.

During a Temporal-unavailable interval, ready events remain durable and no new
execution is bound. Recovery replays the outbox through the same idempotent
bridge.

## Alternatives Considered

### Temporal only watches execution

Small change to the existing pilot, but it keeps two orchestration systems,
split lifecycle ownership, nudge dependence, pull-based claim races, and
reapers that infer missing procedure state. It preserves the exact
control-plane boundary the scheduler-binding decision replaced.

### Temporal replaces Beads

One persistence technology for procedure state, but Workflow history is a poor
home for mutable work metadata, dependency queries, large artifacts, review
facts, and operator repair. Work facts and execution history have different
query, retention, and mutation semantics.

### Workers poll Beads from inside an Activity

Minimal event-bridge work, but it recreates a competing scheduler inside
Activity code and loses the stable ready-event identity needed for replay and
reconciliation. Temporal Workers poll Task Queues; ready transitions schedule
specific Activity Tasks.

## Consequences

Positive: one durable owner advances the execution procedure; Beads remains the
inspectable and repairable work/artifact ledger; Activity retries survive
process loss without allowing stale completion; Task Queues provide
capacity-aware worker polling without warm-worker nudges; lost boundary events
are repaired without periodic semantic rescheduling.

Negative: Beads needs an exact generation-fenced conditional-write API; the
ready transition needs a transactional outbox or equivalent durable event;
agent runtimes need a start-or-attach protocol keyed by fencing token; Workflow
changes require replay-safe versioning.

Risks and mitigations: each field has one authority, so cross-system values are
links and receipts rather than mirrored lifecycle state (dual-writer drift);
generation-fenced writes and start-or-attach execution keyed by claim token
bound duplicate side effects under at-least-once Activity delivery; Workflow
history stores only typed checkpoints and artifact references, and each
Workflow closes at its explicit event-set-fenced run boundary (history growth);
the failure-domain boundary above is a hard deployment gate, so an outage
cannot become self-sealing (control-plane coupling).

## Verification Gates

The implementation is not complete until all of these are independently
verified on the exact head:

1. duplicate ready-event delivery schedules one Activity for one generation;
2. concurrent claims produce one lease and one fencing token;
3. a rebound generation rejects an old Activity's completion;
4. an Activity retry reattaches or resumes instead of launching a second agent;
5. heartbeat checkpoints survive worker loss and preserve artifact references;
6. a dropped ready event is repaired once by receipt reconciliation;
7. Workflow replay passes across the registered production history;
8. Beads inspection and core recovery still work while Temporal is unavailable.

Production enablement additionally requires two standing gates. The memory
gate: any expansion adds its worker footprint to a host already at its memory
ceiling, and the arithmetic must close first. The persistence gate: the
originating deployment is a Temporal dev-server with SQLite persistence, a
configuration Temporal documents as development-only; before the bead-execution
path is armed, either move to a production-grade persistence tier or record an
explicit acceptance of SQLite for the bounded single-host profile together with
its recovery limits. Neither gate is satisfied by code review.

The reference implementation and deterministic fixtures live under
[`services/temporal-maintenance/internal/temporalbeads`](../services/temporal-maintenance/internal/temporalbeads).
The file-backed store is a crash-test adapter for the contract, not a second
production work ledger; production enablement remains gated on binding these
operations to the authoritative Beads transaction API and the installation's
start-or-attach session adapter.
