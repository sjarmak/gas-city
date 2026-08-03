# Feedback for Temporal from this evaluation

Recorded 2026-07-22 during the evaluation described in
[when-durable-execution-earns-its-weight.md](when-durable-execution-earns-its-weight.md),
because the negatives are the most valuable output and the sharpest version of
"adopt only at the external-mutation boundary" is a set of product gaps that
made the boundary hard to find. Our vantage is narrow and worth stating: one
self-hosted worker under a systemd user service on a single host at its memory
ceiling, evaluated by proving where durability was actually exercised rather
than adopting on faith. The recommendations are weighted toward that profile
and ranked by how much each would have helped us.

## The most useful thing to ship is a reason not to adopt

Our largest finding was a negative that took real effort to prove: on a
maintenance workload doing 44 seconds of work per 120-minute window, with no
long-lived state, no event wait, and overlap protection that never fired,
Temporal reduces to cron plus a lockfile, and Schedules make that reduction
easy to reach without noticing. An adoption heuristic stated loudly in
onboarding and in the Schedules documentation would have saved us weeks: does
the workload run long enough to be crash-exposed mid-flight, does it wait on
an external event, is the side effect irreversible; if all three are no, the
answer is cron. The same question should be answerable after adoption
from telemetry the product already has. A per-workflow flag for "never
replayed, never waited on a signal, completed synchronously every run" is a
cron-in-disguise detector, and we had to build our own observe-mode metrics to
answer it by hand.

## The guarded external mutation needs a paved road, because the naive version self-erases

The one lane where Temporal won for us was crash-survivable exactly-once
external mutation, and the hard part of that lane, exactly-once at a
side-effect boundary, is exactly what the product leaves to the user. Our
first implementation (at-most-once, fail-closed) did what we wrote it to do
and, on a SIGKILL mid-side-effect, produced a poisoned pending claim, an
orphaned work item, a FAILED workflow, and no escalation; it went silent. A
competent engineer falls into that on the first try, because the durability
story is "the workflow survives" and says little about "the side effect is now
in an unknown state and nobody was told." A supported pattern for the guarded
mutation, at-most-once on the write, a retryable preflight that stamps
nothing, a dead-letter path for permanently failed mutations, and a mandatory
escalation hook so the boundary cannot go quiet, would make the differentiated
use case a paved road instead of a trap we designed our way out of by hand.

## A watchdog built on fail-closed durability goes silent when it matters most

Temporal is pitched as the substrate for reliability layers and supervisory
scan-of-scans work, and for that shape we found the opposite: a watchdog built
on fail-closed durable execution stops reporting at the one moment a watchdog
must not, the crash. The reliability layer most teams actually want is
at-least-once, idempotent, and re-derived from the source of record every tick
(our rule became "signals advance, queries repair"), and for that a stateless
timer over a SQL scan beat a durable workflow on every axis we measured: no
new state, no new operational surface, the same failure-domain independence,
no memory footprint on a host that had none to give. Either ship first-class
guidance for at-least-once idempotent reconciliation as a distinct mode, or
cede that lane in the documentation. The positioning today implies both, and
the fail-closed default quietly makes Temporal the wrong tool for half of what
it is pitched to cover.

## The self-hosted single-node story is thin, and it gated us three times

Everything above assumes Cloud or a Kubernetes fleet; ours was one worker on a
host near its memory limit. Worker memory was a real line item that stalled
expansion behind a standing memory gate, and an optimized minimal single-node
profile with real per-worker numbers would have let us make the call faster.
We bound localhost-only with no authorizer, safe only while nothing else
touches the loopback interface, because the secure-small recipe is absent. The
sharpest gap is deploy and replay safety: a self-hosted team cannot roll a
fleet to dodge a breaking change, so the experimental Worker Versioning path
being removed in favor of the GA API landed churn on the single most
safety-critical primitive we have, and a shippable "will this definition
change break replay against live histories" gate, beyond the
replay-against-captured-history test we wrote ourselves, would lower that
barrier a lot.

## Signals that never arrive are invisible

Our signal metadata contract rotted in place: the fields the bridges depended
on were never populated by the live path, both handlers became permanent
no-ops, and nothing flagged it, because nothing level-triggered reconciled the
contract. A workflow parked forever on a signal type that has never once been
delivered to its namespace looks healthy from the outside, so dangling-signal
detection would have caught in a query what took us a manual audit to find.

## The chaos-test-in-CI story is the differentiator; protect it

Keep investing in the one capability that let us validate the real win
empirically instead of on faith. `testsuite.StartDevServer`, replay against a
captured production history, and a forced-worker-restart chaos test
expressible directly in `go test` (kill the worker mid-human-gate, then assert
exactly one mutation recorded on resume) turned "trust the durability claim"
into a single assertion: the recorded-mutation count equals one. That is the
reason our record concludes "adopt at the external-mutation boundary" rather
than "we could not tell," and it is worth expanding before anything else on
this list.
