# Walkthrough: a hand-rolled orchestration loop against durable-execution primitives

A line-by-line comparison of one production bash poller against the
durable-execution primitives it reimplements. Written 2026-07-16 from a live
trace of the running poller, not from a reading of its source. The subject is
real and was healthy at the time of the trace; nothing here is a bug report
about it being broken. It works. The question is what it costs to make it
work.

## Thesis

> Hand-rolled orchestration reliably guarantees at-most-once, and silently
> abandons completion.

Two independent loops in the same installation were found to have this exact
shape on the same day:

| Loop | Guarantees | Silently drops |
| --- | --- | --- |
| Maintenance cycle (Temporal, dispatch-only) | no duplicate work item, no duplicate dispatch | a worker crash mid-dispatch orphans the work item, poisons the claim, and the next cycle looks healthy |
| PR-state poller (bash, 15-minute poll) | no duplicate iterate job per review | a wedged downstream job leaves the review marked handled forever, and the review comments sit unaddressed |

The second case is the sharper one, because the poller was written to fix
exactly that leak: review comments sitting unnoticed. It closes the "review
never noticed" half and leaves the "job dispatched then died" half wide open.

## The subject

A 243-line bash script on a 15-minute cron-style schedule. Every tick it lists
our open pull requests, and for each one asks GitHub whether an automated
reviewer left a review with inline comments. If so, it dispatches a
review-iterate job to a pool of coding agents.

## The trace

Real data from one PR:

```text
T+0        reviewer submits review R (COMMENTED), 4 inline comments
T+60m22s   poller logs iterate_dispatched for R
           (60 minutes from review to dispatch, on a 15-minute poll)
T+10d      still polling this PR; cache and GitHub agree exactly; nothing to
           do, and it has re-derived that same answer every ~16 minutes for
           ten days
```

Steady-state cost: 9 open PRs, 2 API calls each, ~4 ticks per hour, roughly 72
GitHub API calls per hour to learn nothing.

## Anatomy: every mechanism, and the primitive it reimplements

| Poller mechanism | Reimplements |
| --- | --- |
| Per-PR JSON cache with `handled_review_ids` | workflow state / idempotency |
| Open-job scan that string-matches job titles | workflow ID uniqueness |
| 15-minute poll | durable timer + Signals |
| Append-only event log | event history |
| Cron-style schedule entry | Schedule |

### Two idempotency mechanisms, because neither is sufficient

`handled_review_ids` is written **after** the dispatch. Crash in between and
the next tick re-dispatches. So a second guard was added, whose own source
comment states the reason: "This guard covers the narrow window where a
dispatch succeeded but the script died before the cache was written." It
detects the duplicate by scanning every open job and comparing titles to a
constructed marker string. That is a hand-rolled compensation for a lost
update, implemented as title-string matching, costing one ledger query per
open job.

A workflow ID makes both mechanisms disappear.
`pr-iterate/{repo}/{pr}/{review_id}` either exists or it does not; a duplicate
start is a no-op decided server-side. There is no window to compensate for.

### The cache is unbounded and never collected

174 cache files for 8 open PRs. Once a PR closes it drops out of the open-PR
listing, so its file is never touched again: 166 tombstones, the oldest frozen
for two months. Nothing collects them. This is memory, not a query, and
nothing reconciles it.

## Defects found by this trace

1. **A PR-wide count gated a per-review decision** (fixed same day, with a
   regression test). The poller fetched all inline comments on the PR, then
   used that count to decide whether a specific review had actionable
   feedback. On a PR where an earlier review left 4 comments, a later
   summary-only review saw count=4 and dispatched a spurious iterate job.
   Proven by fault injection with a two-review fixture, not by inspection; on
   the live single-review PR the per-review and PR-wide counts coincide, which
   is precisely why the bug needed a fixture to surface.
2. **Latent: a review seen before its comments land is dropped forever.** If
   the API exposes the review object before its inline comments, the poller
   takes the zero-comment branch, which marks the review handled permanently.
   No sighting in the record; the race is real, the sighting is not. Only a
   re-check closes it.
3. **Fire-and-forget past dispatch.** Once the dispatch returns success, the
   handled set advances and nothing revisits. No timeout, no completion check,
   no state past dispatch. If the downstream job wedges, the review stays
   handled and the comments sit unaddressed, which is the original leak
   returning by another door.

Defect 3 is the one bash cannot fix without becoming a workflow engine.

## The after

One workflow per review, ID `pr-iterate/{repo}/{pr}/{review_id}`:

```text
PRIterateWorkflow(repo, pr, review_id):
    comments = GetReviewComments(repo, pr, review_id)   # per-review; fixes defect 1
    if comments == 0: return skipped
    job = DispatchIterate(...)          # returns job id; never blocks on the agent
    await Signal(iterate.done) | timer(N hours)
    on timeout: reconcile job state -> escalate         # closes defect 3
    record evidence
```

That ID alone deletes the handled-set, the 174-file cache, the title-matching
duplicate scan, and the crash window.

**Keep the poll; demote it.** It stops being the dispatcher and becomes the
reconciler: any review with no workflow gets one started. Events give latency;
reconciliation gives completeness. Events get lost at integration boundaries,
so the scan must survive. What dies is the memory, not the scan.

### Three constraints

1. **Never block an Activity on the agent session.** The downstream job is an
   interactive agent session, not a function call. Dispatch, return the job
   ID, then wait on a Signal or reconcile.
2. **Do not route this through the at-most-once fail-closed adapter** built
   for the guarded maintenance mutation. That posture refuses a poisoned
   pending claim forever, which is correct for an irreversible external write
   and wrong here, where re-dispatch is idempotent by workflow ID and silence
   is the failure being replaced. The two-layer thesis in
   [when-durable-execution-earns-its-weight.md](when-durable-execution-earns-its-weight.md)
   is this distinction.
3. **No webhook intake exists.** Start with a REST-poll shim that converts
   verdicts to Signals, plus the narrow reconciler.

### What this buys

| Win | Available when |
| --- | --- |
| Delete 2 idempotency mechanisms, 174 cache files, and the crash window | immediately (workflow ID) |
| Escalate a wedged job instead of dropping it (defect 3) | immediately (durable timer) |
| Event history replaces the hand-rolled log | immediately |
| 60-minute dispatch latency drops to seconds | only with webhook intake, which does not exist |

With the REST shim and no webhooks, latency stays at roughly the poll
interval. The durability wins are real now; the latency win is not. Claiming
otherwise would be the same overstatement that made the maintenance cycle look
like a Temporal win when it was a 44-second job that needed cron and a
lockfile.
