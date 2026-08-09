# gas-city: systems patterns for agent software factories

Gas City is a working study of how to turn nondeterministic coding agents into
a deterministic software delivery system. This repository publishes the
reusable architecture patterns and one executable Temporal case study curated
from a running installation.

The core system separates four authorities: a work ledger owns canonical facts,
a durable engine owns procedure, workers own effects, and a control plane owns
policy. The documents explain how that boundary extends into fencing,
idempotency, observability, scheduling, overload, and cleanup.

## Start with the software-factory model

| Document | Question it answers |
|---|---|
| [Systems architecture](docs/software-factory-architecture.md) | Which layer owns facts, procedure, effects, and policy? |
| [Distributed-systems review playbook](docs/distributed-systems-review-playbook.md) | How do you audit authority, retries, barriers, capacity, and failure recovery? |
| [The instrument contract](docs/instrument-contract.md) | How do checks avoid turning observation failure into a reassuring green? |
| [Idempotent convergence and fenced publication](docs/idempotent-convergence-and-fenced-publication.md) | How do interrupted multi-store operations converge without distributed transactions? |
| [Scheduling, admission control, and backpressure](docs/scheduling-admission-and-backpressure.md) | How do queues, fairness, overload policy, and execution routing stay separate? |
| [External effects and resource reclamation](docs/external-effects-and-resource-reclamation.md) | How do stale workers lose authority and abandoned resources get reclaimed safely? |

These patterns do not require Temporal. The implementation below uses Temporal
where the procedure itself must survive worker death.

## Executable Temporal case study

A coding agent is editing a worktree when the process that launched it dies.
Can the retry find the agent that is still running, or does it start a second
one against the same work?

The executable case study answers that question with a Temporal Workflow that
owns the procedure, an Activity that owns the claim and agent session, and a
test driver that kills the Worker and checks what survived.

| Path | What it is |
|---|---|
| `services/temporal-maintenance/internal/temporalbeads` | The Workflow, the Activity, the Beads store adapter, the session resolver, and their tests |
| `demo/harness` | A separate Go module that drives the real Workflow and Activity against a local Temporal server |
| `demo/run.sh`, `demo/run-before.sh` | The two arms, end to end, with their verification gates |
| `demo/recording` | The tmux split-screen scripts that record a run as an asciicast |

`BeadOrchestrationWorkflow` owns the procedure. `ExecuteBeadActivity` writes the
generation-fenced claim, starts the agent or attaches to the one already
bound to that claim, heartbeats, and reports completion. The demo kills the
Worker at two points: after a heartbeat checkpoint exists, and before one does.
The second is the case that matters, because only there must the retry ask the
resolver again.

## Reproducing the runs

Requires Go, a `temporal` CLI, `jq`, `tmux`, and `asciinema` for the recording.
Neither arm needs Dolt, a real coding agent, GitHub, or any credential.

```bash
cd demo
SERVICE=../services/temporal-maintenance ./run.sh          # the Temporal arm
SERVICE=../services/temporal-maintenance ./run-before.sh   # the pre-Temporal arm
```

Each writes its evidence under `demo/out/` and fails loudly rather than
reporting a pass it did not earn. `demo/publish-artifacts.sh` curates that
evidence into a publishable set and records a SHA-256 for every file.

The package tests run on their own:

```bash
cd services/temporal-maintenance && go test ./internal/temporalbeads/...
```

Two of them are the point rather than the coverage:
`TestReplayPersistedWorkflowHistory` replays a captured history against current
code, and `TestReplayRejectsPlantedNondeterministicWorkflow` requires the replay
gate to reject a deliberately nondeterministic Workflow. A gate that never
fails is a gate that is not wired up.

## Temporal decision records

| Doc | What it is |
|---|---|
| [when-durable-execution-earns-its-weight](docs/when-durable-execution-earns-its-weight.md) | The decision framework and verdicts from evaluating Temporal across a running multi-agent installation; most candidates lost |
| [adr-temporal-beads-boundary](docs/adr-temporal-beads-boundary.md) | The ADR this code implements: Temporal owns the procedure, Beads owns the work facts |
| [durable-execution-walkthrough](docs/durable-execution-walkthrough.md) | A production bash poller traced line by line against the primitives it reimplements |
| [temporal-product-feedback](docs/temporal-product-feedback.md) | What the evaluation surfaced as product gaps, ranked |

## License

MIT. Upstream [gastownhall/gascity](https://github.com/gastownhall/gascity) is MIT
too; its LICENSE file reads (c) 2025 Steve Yegge. This module is separate from
any Gas City build.

## Publication boundary

The working installation carries deployment units, live configuration,
operational runbooks, internal trackers, and local evidence. Those are excluded.
The public documents keep the reusable mechanisms, decision tests, and failure
oracles.

Local absolute paths and worker identities have been replaced with neutral
values in the test fixtures and scripts. The behaviour is unchanged and the
suite passes, including both replay cases, but that makes this snapshot
textually different from the internal revision `2b5df98802e00625a80163df8405d05a13188d62`
that this was taken from.
