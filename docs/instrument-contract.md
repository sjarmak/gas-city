# The instrument contract

An instrument is any check, reaper, health script, dashboard number, or alert
whose output changes what a human or agent does next.

Instruments often fail silently toward `fine`. That is worse than having no
instrument, because operators learn to trust a conclusion that was never
measured. This contract makes uncertainty visible.

## C1. Report on every run

A clean run produces a record. Silence cannot encode `nothing found`, because
silence also means the check never ran, crashed, or wrote to the wrong place.

Good output includes the zero:

```text
checked=<count> actionable=0 expected=<count> lookup_failed=0
```

## C2. Separate absence from observation failure

Use at least these three outcomes:

- `ABSENT`: the correct destination was checked and the item was not there;
- `LOOKUP_FAILED`: the destination could not be checked;
- `UNKNOWN`: the destination was checked but the evidence is inconclusive.

The third outcome must be reachable and emitted. Do not write the health
artifact only after a successful check; that makes its absence ambiguous.

## C3. Make the instrument fail on purpose

A check observed only in its passing state is unverified. The test suite must
include a case that fails when the instrument is miswired, and that failure must
be observed by running the test against the broken behavior.

Mutation checks are useful here:

- point the instrument at the wrong destination;
- make its lookup command fail;
- provide an empty corpus;
- remove the state transition it is meant to observe;
- invert the predicate;
- return stale cached data.

The suite should reject each mutation for the reason a real operator would care
about.

## C4. Report the measured effect

`restart requested` measures intent. `new process is serving requests` measures
the effect. Every instrument should say which of those it observed.

After an action, read the system again. Do not promote an exit code or queued
message into a postcondition.

## C5. Name the measured quantity

An alert named `fan-out` should not fire on memory use while reporting no
process count. If a threshold combines distinct quantities, split the alert or
name both quantities in every firing.

## C6. Attach age and source to every reading

A number without a timestamp and provenance cannot be distinguished from a
cache, default, or stale value.

```json
{
  "value": "<number>",
  "unit": "queued_work_items",
  "observed_at": "<RFC3339 timestamp>",
  "source": "ledger/ready-query",
  "source_revision": "sha256:..."
}
```

## C7. Prove the corpus is non-empty

`found nothing` and `looked at nothing` render identically. Before reporting a
clean sweep, count the corpus and print the count. A zero corpus is
`LOOKUP_FAILED` or `UNKNOWN` unless zero is itself the expected, independently
verified state.

## C8. Prove both destination and exclusion

When a check asserts that a write went to a sandbox instead of production, it
must verify both sides:

- the expected artifact exists in the sandbox;
- the artifact does not exist in production.

The first check alone passes when a write went to both. The second passes when
the write failed entirely.

## C9. Separate expected states from actionable states

Report expected conditions and actionable conditions separately, and read the
expectation from durable state instead of hardcoding it.

```text
actionable=2 expected_under_pause=305
```

Alert on the actionable set. A large expected set should remain visible without
burying the small set that needs intervention.

## C10. Cover transitions as well as states

A state sampler cannot see a failure that occurred and recovered between
samples. If the promise concerns an edge, record the edge or an event counter.

Examples include claim acquired, generation advanced, publication accepted,
cancellation observed, and acknowledgement finalized.

## C11. Watch expected activity for silence

Presence of errors is half of observability. Expected activity that stops is the
other half.

| Condition | Expected activity | Anomalous silence |
|---|---|---|
| Ready work exists | Claims begin | Claim rate is zero |
| Workers run | Heartbeats appear | Heartbeat rate is zero |
| Work completes | Results publish | Publication rate is zero |
| Results await acknowledgement | Dispositions advance | Acknowledgement rate is zero |

An independent instrument must watch the primary instrument's reporting rate.
An in-process watchdog cannot detect a wedged event loop that also prevents the
watchdog from running.

Model maintenance, paused admission, and unavailable dependencies explicitly.
Otherwise a correct absence alarm becomes a permanent false positive.

## C12. Translate imported vocabulary before scanning

A source search for another system's words can classify careful code as
unprotected because the same behavior has different local names. Before an
audit:

1. write down the mapping from the imported concepts to local concepts;
2. test the mapping against one component already known to implement the
   behavior;
3. classify from behavior and control flow, not keyword presence;
4. report an unparseable form as `UNKNOWN`.

A zero-hit keyword scan is not evidence that a mechanism is absent.

## Gate for a new instrument

Before trusting a new instrument, verify:

- it reports on clean runs;
- it emits an unable-to-check outcome;
- it prints the corpus it examined;
- its readings carry time and source;
- it reports the measured effect;
- expected and actionable states are separated;
- at least one failing behavior was observed;
- something independent detects when it stops running.

Mechanical scans can establish the presence of a test file or a logging call.
They cannot prove that the test failed for the right reason or that the log
describes the promised effect. A scan miss should ask for review instead of
declaring the instrument broken.

## Review template

```text
Instrument:
Decision it influences:
Promise measured:
Corpus and destination:
PASS outcome:
FAIL outcome:
LOOKUP_FAILED outcome:
UNKNOWN outcome:
Reading timestamp and source:
Expected activity rate:
Independent silence detector:
Failure mutation observed:
Post-action read-back:
```

The instrument is ready when every outcome is observable and a failure in the
observation path cannot masquerade as a clean system.
