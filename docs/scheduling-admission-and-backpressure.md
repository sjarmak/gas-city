# Scheduling, admission control, and backpressure

Queueing, scheduling, admission control, and backpressure answer different
questions. Treating them as one mechanism hides starvation and turns overload
into a retry storm.

| Layer | Question |
|---|---|
| Queue | Where does pending work wait? |
| Scheduler | Which eligible work runs next? |
| Admission control | Should work enter now, later, in a cheaper form, or at all? |
| Backpressure | How does downstream saturation cause upstream producers to slow down? |

The execution engine sits below these policy decisions. A task queue routes
work to compatible workers. It should not encode the product's priority or
fairness policy in deployment topology.

## Keep workload class separate from priority

Priority answers how urgent an item is. Workload class identifies the capacity
pool it draws from.

A useful starting set is:

| Class | Purpose |
|---|---|
| `interactive` | A human is waiting |
| `normal` | Ordinary background work |
| `recovery` | Backlog drained after an outage |

A high-priority recovery item remains recovery-class. It should advance before
other recovery work without consuming the floor reserved for interactive work.
Conflating class and priority lets a recovery storm disguise itself as urgency.

## Use a hierarchical scheduler

At software-factory scale, scheduling is a stack of constraints:

```text
global resource limits
   -> tenant fairness
      -> workload class
         -> task priority
            -> individual work
```

Each layer allocates only what the layer above handed it. A single-tenant system
can make the tenant layer a pass-through while keeping the boundary explicit.

The application owns the policy. An execution framework can supply queues,
priorities, rate controls, and durable retries, but it cannot decide what the
system owes each tenant or which workload should degrade first.

## Use weighted fair share with guaranteed floors

Strict priority starves lower-priority work under a steady urgent stream. Fixed
partitions avoid starvation but waste idle capacity.

Weighted fair share combines a reserved floor with borrowable spare capacity:

1. Each class receives a minimum share.
2. Unused tokens return to a shared spare pool.
3. Any class may borrow spare capacity.
4. Released capacity is fungible.
5. Selection rotates so stable input order cannot starve the same class.

Priority affects progress within a class. Guaranteed floors ensure every class
continues to progress.

Weights belong in configuration and should be tuned from queue-age and
throughput measurements. A new class requires evidence that the existing axis
cannot express the needed fairness.

## Route by execution need

Use separate task queues when workers need different images, credentials, host
affinity, or hardware. Keep class and priority as work attributes resolved by
the scheduler before routing.

Encoding policy as queue topology causes three problems:

- policy changes require deployment changes;
- fairness across queues becomes hard to express;
- operators must infer policy from the deployment diagram.

## Give admission three outcomes

Admission may:

- delay work;
- reject work;
- degrade work.

Degradation can select a smaller model, lower effort tier, reduced scope, or
cached result. It is often available only before the work enters execution.

Resource-protection gates and workload-policy gates can coexist. A store
connection limiter answers whether the store can absorb another mutation. A
workload admission policy answers whether this class of work should enter the
factory. Combining them makes one question impossible to answer.

## Propagate pressure to producers

An error returned to a caller describes saturation. Backpressure reaches the
components that create work.

A saturated factory should be able to make a periodic producer skip a tick,
reduce fan-out, defer maintenance, or submit a degraded job. Producers that
continue at full rate turn saturation into a self-amplifying backlog.

Useful pressure inputs include:

- oldest-ready age and ready backlog;
- schedule-to-start latency;
- active workers and claim latency;
- store transaction latency and connection use;
- model or API throttling;
- CPU, memory, disk, and network pressure;
- retry and conflict rate.

Use hysteresis around admission modes so the system does not oscillate between
open and paused.

## Measure before partitioning

When throughput flattens as concurrency rises, identify the serialized resource
before adding workers or repositories.

Candidates include:

- repository-wide locks;
- one hot table or index;
- worktree creation;
- transaction contention;
- a shared coordinator;
- disk I/O;
- a publication step that all work eventually reaches.

Sharding the work does not help if every shard still serializes through one
authoritative mutation. Measure throughput against concurrency, queue latency,
execution latency, lock wait, transaction latency, conflicts, worktree
acquisition, and dependency-blocked versus runnable work.

Dependency starvation and capacity starvation can produce the same empty ready
set and require opposite fixes.

## Shrink the critical section before changing topology

Suppose publication holds a lock across this whole sequence:

```text
fetch repository state
run validation
update authoritative ref
upload metadata
```

Only the ref update may carry the invariant. Fetch, validation, and upload can
run concurrently, followed by a small compare-and-set on the ref.

Use this order:

1. Identify the invariant.
2. Move non-authoritative work out of the critical section.
3. Narrow the invariant to a smaller resource when the domain permits it.
4. Prefer optimistic compare-and-set publication over a lock held across
   computation.
5. Split repositories only after measurement shows repository-wide contention
   remains.

Distributing the lock service does not distribute an indivisible protected
resource. Granularity creates parallelism only when the domain invariant can be
split by module, path, branch, repository, or another conflict boundary.

## Durable execution coordinates the protocol

A durable workflow can coordinate:

```text
prepare -> wait -> validate -> publish -> rebase or retry on staleness
```

The workflow preserves the procedure across worker death. The application must
still state which publications may happen concurrently. A durable retry will
faithfully repeat an unsafe domain decision.

## Measurements and invariants

Track:

- progress and queue age per class;
- floor allocation, borrowing, and unused capacity;
- admission outcomes and reasons;
- producer rate before and after pressure;
- throughput versus concurrency;
- schedule-to-start versus execution latency;
- retry amplification during recovery.

Assert:

- every class progresses under sustained load;
- idle capacity is borrowable;
- interactive work survives a recovery backlog;
- recovery drains under an interactive stream;
- pressure reduces production;
- a stale publication loses through compare-and-set;
- recovery does not create a second overload.
