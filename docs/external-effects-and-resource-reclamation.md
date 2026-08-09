# External effects and resource reclamation

Leases and workflow cancellation solve different parts of stale work. Safe
systems assign each property to the layer capable of enforcing it.

| Property | Meaning | Enforcement layer |
|---|---|---|
| Correctness | Stale work cannot affect authoritative state | Destination-side fencing token |
| Isolation | Stale work cannot interfere with current work | Cancellation plus generation-stamped resources |
| Efficiency | Stale work stops consuming resources promptly | Heartbeats, timeouts, and cancellation |
| Cleanup | Abandoned resources are eventually reclaimed | Durable liveness record plus structurally safe deletion |

Cancellation is cooperative. It tells a worker to stop; it does not remove the
worker's ability to push a branch or call an API before it observes the signal.
Correctness therefore depends on an enforcement point at the destination.

## Classify every external effect

Every externally meaningful effect needs one of two designs:

1. idempotent by a stable logical key;
2. performed behind a fenced publication boundary.

`neither` is a design error.

| Effect | Preferred class | Example mechanism |
|---|---|---|
| Shared Git ref update | Fenced boundary | Compare-and-set expected old ref |
| CI trigger derived from a push | Derived | No independent trigger |
| Pull request creation | Idempotent | One request per head branch and generation |
| Comment or notification | Idempotent | Marker or outbox key containing work, generation, and content digest |
| Deployment or release | Fenced boundary | Broker validates generation and approved artifact digest |
| Mutable remote record | Fenced or idempotent | Conditional request, version token, or deterministic operation ID |

Idempotent means the second request is a no-op. `Duplicates are harmless` is
not an idempotency strategy.

## Move capability behind the fence

Caller-side policy depends on the stale caller cooperating. Stronger designs
move the capability:

1. A credential broker holds the external credential and validates the current
   generation before every call.
2. A hook or gateway validates the generation at the egress boundary.
3. A convention asks workers to check before acting.

The broker is strongest because a stale or confused worker cannot authenticate
around it. Hooks can close common paths quickly but require a bypass policy and
an independent inventory of uncovered egress.

New egress paths should default to refused until their effect class, fence,
idempotency key, and acceptance record are declared.

## Bound wasted work through heartbeats

Fencing prevents stale effects. It does not stop stale computation.

Use activity or worker heartbeats to deliver cancellation, and use heartbeat and
execution timeouts to bound a wedged attempt. The heartbeat interval becomes a
cost bound: a stale attempt may consume roughly one interval of CPU, model
tokens, quota, and held capacity before it stops.

Set that interval from the acceptable wasted cost and external rate limit, not
from an arbitrary liveness preference.

Once effects are fenced, process reapers can become approximate cost controls.
A missed kill wastes resources; it cannot corrupt authoritative state. This
lets reapers favor low false-positive rates without carrying the full
correctness burden.

On fence-out, release resources in an idempotent order:

1. expensive model or account allocation;
2. worker or process slot;
3. worktree lock;
4. local ephemeral files.

The stale owner does not delete the lease or generation record it no longer
owns.

## Stamp resources at creation

Every reclaimable resource should carry the generation that created it.

| Resource | Stamp location |
|---|---|
| Worktree | Directory name and provenance record |
| Branch | Branch name or protected metadata |
| Temporary files | Per-generation directory |
| Process | Environment or supervisor metadata |
| Lease | Generation is part of the lease record |
| Artifact | Immutable key includes generation and digest |

Cleanup can then use a structural rule:

```text
delete only when
  resource.generation < current.generation
  and current lease state is terminal
  and ownership lookup succeeded
```

The resource path makes the cheap candidate test possible. An authoritative
provenance record confirms it. An unstamped or unreadable resource is
`UNKNOWN` and is never deleted automatically.

Age may bound when cleanup runs. It should not establish ownership. A young
resource can be stale, and an old resource can still be current.

## Instrument mechanism population

A generation field in a schema is not protection when no producer writes it.
For every stamp or fence, measure the population it covers:

```text
eligible_resources
stamped_resources
validated_resources
unknown_resources
accepted_stale_mutations
```

A low-population mechanism must report `UNKNOWN`, not `no stale resources`.
Mechanism present and writer absent is a common failure mode in control planes.

## Discover egress from execution structure

String searches miss calls constructed as argument arrays, wrappers, SDK
methods, generated commands, or indirect actions. Build an egress inventory
from execution structure where possible:

- subprocess argument construction;
- SDK client calls;
- workflow activity registration;
- hook and gateway entry points;
- declared credentials and network destinations.

Forms the inventory cannot parse remain `UNKNOWN`. A zero-hit scan over one
syntax does not prove that the system has no writers.

## Failure tests

Exercise:

- cancellation arriving during an external call;
- stale generation trying every declared egress path;
- duplicate notification and request delivery;
- credential broker unavailable;
- hook bypass attempt;
- worker wedged beyond heartbeat timeout;
- cleanup sees a newer generation;
- cleanup sees an unstamped resource;
- cleanup loses access to the ownership store;
- a stamp field exists but no producer populates it.

Assert:

- no stale effect is accepted;
- duplicates converge on one logical effect;
- stale work stops within the declared cost bound;
- current resources survive cleanup;
- unknown resources remain intact and visible;
- every egress path appears in the declared inventory.
