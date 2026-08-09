# Distributed-systems review playbook for software factories

Use this playbook to review an agent software factory as one distributed work
system. The scope includes the work ledger, durable workflows, sessions,
agents, repositories, maintenance processes, and external effects.

The goal is incremental simplification backed by measurements. A review should
not begin with a plan to move every state transition into one orchestrator.

## Start with the ownership boundary

Record which layer owns each kind of truth:

| Layer | Responsibility |
|---|---|
| Work ledger | Canonical work facts, dependencies, claims, generations, artifacts, outcomes, and cancellation |
| Durable procedure engine | Ordering, waits, retries, signals, recovery, and acknowledgement progress |
| Workers and activities | Processes, filesystems, Git, tests, APIs, and external effects |
| Control plane | Priority, decomposition, admission, capacity allocation, and intervention policy |

Then state the review scope, snapshot time, repositories, canonical stores, and
exact workflow identities. Separate observed evidence from inference and
proposed behavior. Define each invariant and its pass/fail oracle before
proposing a change.

## Phase 1: map state and authority

Inventory every mutable resource, including resources outside the work ledger:

- work item;
- claim or lease;
- agent session and process;
- worktree and branch;
- repository ref;
- artifact and result;
- delivery and acknowledgement;
- background maintenance operation;
- external API object.

Complete an ownership table. An empty cell is a finding.

| Resource | Canonical state | Owner | Generation | Claim token | Expiry | Mutation boundary | Stale-owner behavior | Recovery | Health evidence |
|---|---|---|---|---|---|---|---|---|---|
| `<resource>` | `<record>` | `<actor>` | `<source>` | `<source>` | `<lease/revocation/none>` | `<destination>` | `<rejected/ignored/unsafe>` | `<procedure>` | `<query/metric/test>` |

For every mutating path, answer:

1. Does the destination check authority, or does the caller assert it?
2. Are owner, generation, and claim token checked together?
3. Is validation atomic with the authoritative mutation?
4. Can an old generation appear current again after a newer one?
5. Can a stalled process wake and commit, publish, close, deliver, or
   acknowledge?
6. Does a duplicate request converge at the destination?
7. Does a stale attempt fail closed with a typed, observable result?

Search for code equivalent to this time-of-check/time-of-use race:

```text
if claim.valid():
    mutate()
```

Long-running work commonly exceeds a lease interval. Validate authority at
each mutation boundary, not in every computation loop.

### Inventory side effects

| Effect | Destination | Fence accepted there? | Idempotency key | Duplicate behavior | Stale behavior | Acceptance record |
|---|---|---|---|---|---|---|
| Ledger mutation | Transactional store |  |  |  |  |  |
| Artifact publication | Artifact store plus pointer |  |  |  |  |  |
| Worktree mutation | Filesystem owner |  |  |  |  |  |
| Git ref update | Local or remote Git |  |  |  |  |  |
| Process termination | Supervisor |  |  |  |  |  |
| External API call | Remote endpoint |  |  |  |  |  |

## Phase 2: map waits, queues, joins, and reductions

Find every loop shaped like `query -> nothing -> sleep -> query` and classify
it.

| Class | Meaning | Default challenge |
|---|---|---|
| A | Periodic maintenance | Keep when time itself is the trigger |
| B | Waiting for a transition | Prefer an event plus a durable wait |
| C | Failure detection | Prefer heartbeats, deadlines, and absence detection; retain an independent watchdog |
| D | Reconciliation | Keep one bounded safety net and remove duplicate recovery owners |
| E | Garbage collection | Keep periodic when suitable, with fences and bounded deletion |

The target is unnecessary polling. A small periodic query can be safer and
cheaper than a workflow. An event-driven path still needs a lost-event and
replay strategy.

Model operations as queues, fan-out, joins, and reductions:

```text
                    +-> worker A --+
work -> fan-out ----+-> worker B --+-> JOIN -> next policy decision
                    +-> worker C --+
```

Identify implicit joins such as implementation plus review, code plus tests,
parallel work items plus parent completion, and multi-repository changes.

| Operation | Input queue | Fan-out | Required facts | Barrier | Wait owner | Policy owner | Recovery |
|---|---|---|---|---|---|---|---|
| `<operation>` | `<queue>` | `<children>` | `<facts>` | `<canonical representation>` | `<engine>` | `<control plane>` | `<behavior>` |

Challenge global barriers when partial results can be combined. Associative and
commutative reductions can lower latency and context size. Keep a full barrier
when correctness requires a complete set or stable snapshot.

## Phase 3: bound failure cost

### Assign one retry owner per failure class

Nested retries multiply. Record every retry layer, budget, backoff, jitter,
idempotency key, and terminal disposition.

| Operation and failure | Layer | May retry? | Budget | Backoff and jitter | Idempotency key | Terminal state |
|---|---|---|---|---|---|---|
| `<operation>` | `<workflow/worker/store/API>` |  |  |  |  |  |

One layer should normally own recovery policy for one failure class. Every
retry policy needs a finite attempt, elapsed-time, or resource budget and a
visible intervention state.

### Separate admission from readiness

Ready work is eligible to run. Admission decides whether the system can accept
it now.

Measure pressure before choosing thresholds:

- ready backlog and oldest-ready age;
- running workers and claim latency;
- schedule-to-start latency and retry rate;
- store query and transaction latency;
- CPU, memory, disk, and network pressure;
- worktree availability;
- external API throttling.

Define `normal`, `throttled`, and `paused` modes with hysteresis. A pressure
response must reduce offered work instead of creating another fleet-wide retry
loop.

### Bound obsolete and poison work

Before expensive work, at bounded checkpoints, and before irreversible effects,
ask whether the generation is current, prerequisites still hold, another result
is already canonical, and the output is still wanted.

Permanently failing work needs an explicit progression such as:

```text
ready -> leased -> running -> retrying -> quarantined / intervention
```

Bound attempts, elapsed time, compute, and share of capacity. Quarantine must be
canonical and visible.

### Give destructive operations stronger semantics

Require the applicable controls:

- typed target and explicit scope;
- destination-side generation fence;
- dry run with the exact candidate set;
- maximum batch size and rate limit;
- idempotency and an audit record;
- circuit breaker on surprising cardinality or lookup uncertainty;
- approval above the configured authority threshold;
- rollback or forensic capture when deletion is irreversible.

No cleanup may translate `LOOKUP_FAILED` or `UNKNOWN` into `ABSENT`.

## Phase 4: define health from promises

Measure latency by stage:

```text
total latency
  = ready latency
  + claim and dispatch latency
  + worker start latency
  + execution latency
  + publication and delivery latency
  + acknowledgement latency
```

Use bounded metric dimensions such as operation, state, failure class, worker
pool, and workflow type. Work IDs, run IDs, claim tokens, and outcome IDs belong
in traces and structured logs.

The investigation path should be:

```text
metric shows a breached promise
  -> trace identifies the operation
  -> logs explain local behavior
  -> work ledger shows canonical state
  -> workflow history shows durable procedure
```

Monitor silence as well as errors:

| Condition | Expected activity | Anomalous silence |
|---|---|---|
| Ready work exists | Claims begin | Claim rate is zero |
| Workers run | Progress appears | Heartbeat rate is zero |
| Work completes | Results publish | Publication rate is zero |
| Results await disposition | Acknowledgements progress | Acknowledgement rate is zero |
| Workflows run | Canonical transitions occur | Ledger mutation rate is zero |

Model maintenance, paused admission, and dependency outages explicitly so
absence alerts do not train operators to ignore them.

## Phase 5: test invariants under failure and load

Cover at least these fault classes:

| Fault class | Injections |
|---|---|
| Control and process | Control plane dies; workflow worker dies; agent dies; process pauses past lease expiry |
| State and procedure | Ledger unavailable; workflow service unavailable; slow transaction; lost signal |
| Messaging and order | Duplicate event; reorder; late completion; stale generation; expired lease |
| External and resource | Rate limit; server error; network delay; disk full |
| Lifecycle | Cancellation during mutation; parent superseded; branch changed; result no longer wanted |

Assert system invariants:

- no duplicate logical worker or accepted result;
- no stale-generation mutation;
- no lost canonical result;
- no permanent claim;
- no silent ready work;
- bounded retry and recovery load;
- eventual progress or an explicit intervention state.

Each experiment defines its scope, oracle, safety boundary, abort condition,
and recovery owner. Establish a baseline, inject one fault, inspect canonical
state and effects separately, capture evidence before cleanup, and restore
through the documented owner.

After single-fault behavior is sound, test this sequence under increasing load:

```text
load -> failure -> backlog -> recovery
```

Determine whether recovery creates a second overload. Steady-state throughput
does not prove safe recovery.

## Phase 6: turn findings into changes

Rank findings by the loss they can cause:

| Priority | Condition |
|---|---|
| P0 | Stale or unauthorized mutation, lost canonical result, destructive ambiguity, or lookup failure treated as absence |
| P1 | Unbounded retry or capacity use, permanent wedge, silent loss of progress, or unfenced cancellation |
| P2 | Missing admission control, implicit polling barrier, excessive queue latency, or required dependency without health semantics |
| P3 | Reduction, telemetry, latency, and simplification after correctness is proven |

Every finding should name its evidence, violated invariant, blast radius,
owning layer, smallest complete change, fence or idempotency behavior, failure
proof, rollback, and old mechanism to remove.

Fix one cross-layer obligation end to end before collecting a broad list of
local cleanups. Start with destination fences, retry amplification, stage
latency, backpressure, and absence detection.
