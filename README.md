# gas-city: the Temporal bead-orchestration boundary

A coding agent is editing a worktree when the process that launched it dies.
Can the retry find the agent that is still running, or does it start a second
one against the same work?

This repository holds the code that answers that, and nothing else: a Temporal
Workflow that owns the procedure, an Activity that owns the claim and the agent
session, and a harness that kills the Worker on camera and checks what survived.
It is a snapshot, curated from a working installation rather than exported from
it.

## What is here

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

## License

MIT. Upstream [gastownhall/gascity](https://github.com/gastownhall/gascity) is MIT
too; its LICENSE file reads (c) 2025 Steve Yegge. This module is separate from
any Gas City build.

## What this snapshot is not

The service this was taken from carries more than the boundary published here:
deployment units, operational runbooks, observation commands, and the rest of a
running installation. None of that is needed to run the demo, so none of it
is here.

Local absolute paths and worker identities have been replaced with neutral
values in the test fixtures and scripts. The behaviour is unchanged and the
suite passes, including both replay cases, but that makes this snapshot
textually different from the internal revision `2b5df98802e00625a80163df8405d05a13188d62`
that this was taken from.
