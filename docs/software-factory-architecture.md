# Systems architecture for an agent software factory

A software factory turns requirements and operational evidence into reviewed,
tested, landed changes. Coding agents are nondeterministic, so the controls
around them must be deterministic, inspectable, and repairable.

The central design rule is to separate facts, procedure, effects, and policy.
Putting all four in one orchestrator creates a system that is difficult to
inspect and even harder to recover.

## The four authorities

| Layer | Owns | Must not own merely by convenience |
|---|---|---|
| Work ledger | Work identity, dependencies, claims, generations, artifacts, outcomes, cancellation, and terminal reasons | Live process truth or long-running procedure |
| Durable procedure engine | Ordering, waits, retries, cancellation propagation, signals, and acknowledgement progress | Canonical work facts or the effects themselves |
| Workers and activities | Processes, worktrees, Git, tests, APIs, and other effects | Durable authority because a process is still alive |
| Control plane | Prioritization, decomposition, admission, allocation, and human intervention | Repeated inspection as a substitute for a canonical barrier |

When the layers disagree, inspect them separately:

1. Read the canonical work fact and its generation.
2. Inspect the exact durable execution for procedural history.
3. Inspect the live process, worktree, repository, or external system for effect
   evidence.
4. Classify a failed observation as `UNKNOWN` or `LOOKUP_FAILED`, never
   `ABSENT`.
5. Repair the layer that owns the violated responsibility.

Do not make one layer silently impersonate another. A running worker does not
prove that work is current. A completed workflow does not prove that an artifact
became authoritative. A closed work item does not prove that its result landed.

## The requirements that shape the system

### Agents are nondeterministic; recovery is not

Recovery re-derives state from durable facts. It does not trust an agent's
memory, transcript, or self-report. Terminal checks should observe the promised
effect, such as an accepted artifact or an authoritative ref update, instead of
trusting a status field that says `done`.

### External mutations carry the highest loss

Internal work can often retry. A duplicate merge, push, payment, deployment, or
message can be worse than a skipped attempt. External effects therefore need an
idempotency key or a fenced publication boundary.

### Reliability covers can fail too

A watchdog that shares a scheduler, process, or store with the component it
watches shares its failure domain. The last watchdog for a subsystem should run
on an independent substrate and derive health from expected outcomes, including
expected activity that has gone silent.

### Evidence binds to artifacts

Reviews, approvals, and test results bind to an exact revision or artifact
digest. Notifications and health banners are attention signals. They are not
proof.

## Operating principles

### Classify the failure shape before choosing the tool

Most reliability work falls into three shapes:

| Shape | Need | Default mechanism |
|---|---|---|
| Query-shaped | Re-derive whether a condition holds | Level-triggered scan or reconciler |
| Event-shaped | React quickly to a transition | Event for latency plus reconciliation for completeness |
| Timer-shaped | Wake after process death or a long delay | Durable timer or workflow |

A workflow engine is valuable when a procedure must survive. It adds little to
a short periodic query that can reconstruct all state on every pass.

### Signals advance; queries repair

Events lower latency. A level-triggered reconciler provides correctness after a
lost, duplicated, delayed, or reordered event. Every event contract should name
its replay or reconciliation path.

### Watch the outcome, not only liveness

`process is running` is weaker evidence than `the expected artifact moved`.
Measure queue progress, accepted results, publication, delivery, and
acknowledgement. A live worker can remain green while producing nothing.

### Use different semantics for work and watchdogs

The mutation path and the monitoring path need opposite failure postures:

- The work layer is at-most-once and fail-closed. It preflights without side
  effects, performs one guarded mutation, records permanent failure, and
  escalates when it cannot proceed.
- The watchdog layer is at-least-once, idempotent, and re-derived. A watchdog
  must continue after the work path fails.

Fail-closed work without escalation disappears. A fail-closed watchdog goes
silent at the moment it is needed.

## The mutation boundary

Possessing an old claim is never sufficient authority to perform a current
mutation. The destination must validate authority at the point where the
mutation becomes authoritative.

```text
request
  resource
  owner
  generation
  claim_token
  operation
  idempotency_key

destination
  atomically validate generation and token
  accept or refuse the mutation
  record the accepted operation and result
```

Checking a lease in the caller immediately before a side effect leaves a
time-of-check/time-of-use race. If the destination cannot participate in the
same transaction, create an immutable or isolated effect first, then publish a
small authoritative pointer through a fence.

Keep these identities distinct and correlate them through trace context:

```text
work_id
workflow_id
workflow_run_id
generation
claim_token
session_id
activity_attempt
process_id
outcome_id
acknowledgement_id
```

They answer different questions. Collapsing them into one identifier makes
diagnosis shorter only until the first retry, reassignment, or continue-as-new.

## Where durable execution fits

Use durable execution when at least one of these properties matters enough to
pay its operational cost:

- the procedure runs long enough to be exposed to a crash mid-flight;
- it waits on an external event;
- it coordinates an irreversible effect;
- cancellation and retry state must survive worker replacement.

The durable engine owns the procedure. The work ledger retains canonical work
facts, and workers retain the effects. Temporal is the implementation used by
the executable case study in this repository, but the boundary does not depend
on Temporal.

## Where humans fit

Human approval belongs as late as possible, after the system has assembled the
smallest decision with the strongest evidence. The review, tests, artifact
digest, target, rollback, and proposed effect should already be known. Approval
binds to that exact package, not to a broad intention to ship something later.

## Related patterns

- [Distributed-systems review playbook](distributed-systems-review-playbook.md)
- [The instrument contract](instrument-contract.md)
- [Idempotent convergence and fenced publication](idempotent-convergence-and-fenced-publication.md)
- [Scheduling, admission control, and backpressure](scheduling-admission-and-backpressure.md)
- [External effects and resource reclamation](external-effects-and-resource-reclamation.md)
- [Temporal and work-ledger boundary](adr-temporal-beads-boundary.md)
