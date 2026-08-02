#!/usr/bin/env python3
"""Invariant gate for the before-Temporal demonstration.

The before-arm's claim is that the pre-Temporal coordinator, given the same
kill the Temporal arms survive, produces the two failures the conversion
removed: a duplicate agent for one work item, and a stale completion recorded
over the current one. This gate asserts the failure actually reproduced.

Exits non-zero on any violation so run-before.sh fails loudly instead of
printing a demonstration it did not earn.
"""
import json
import sys
from pathlib import Path


def load(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"missing evidence file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        raise SystemExit(f"missing evidence file: {path}")
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: verify-before.py <before-artifacts-dir>")
    art = Path(sys.argv[1])

    store = load(art / "store" / "work.json")
    events = load_jsonl(art / "agent" / "agent-events.jsonl")
    samples = load_jsonl(art / "agents-live.jsonl")
    survivors = int((art / "agent-survivors.txt").read_text().strip() or "0")

    created = [e for e in events if e.get("kind") == "session-created"]
    created_keys = sorted(e.get("key", "") for e in created)
    max_live = max((s.get("agent_processes", 0) for s in samples), default=0)
    completion = store.get("completion") or {}
    claim = store.get("claim") or {}
    overwrites = [
        e
        for e in store.get("history", [])
        if e.get("kind") == "completion-recorded"
        and "over previous generation" in e.get("detail", "")
    ]

    worktree = art / "agent" / "worktree" / "work-item-1.txt"
    sessions_in_worktree: set[str] = set()
    if worktree.exists():
        for line in worktree.read_text(encoding="utf-8").splitlines():
            if " by session " in line:
                sessions_in_worktree.add(line.rsplit(" by session ", 1)[1])

    checks = [
        (
            "the agent outlived the coordinator that launched it",
            survivors == 1,
            f"agent processes alive after the kill = {survivors}",
        ),
        (
            "failure 1: the restart launched a second agent for the same work item",
            len(created) == 2 and created_keys == ["work-item-1-g1", "work-item-1-g2"],
            f"sessions created = {created_keys}",
        ),
        (
            "failure 1: both agents were alive at the same time",
            max_live >= 2,
            f"most agent processes observed at once = {max_live}",
        ),
        (
            "failure 1: both sessions edited the same worktree file",
            len(sessions_in_worktree) == 2,
            f"distinct sessions in the worktree = {len(sessions_in_worktree)}",
        ),
        (
            "failure 2: the final receipt is a stale generation's",
            completion.get("generation") == 1 and claim.get("generation") == 2,
            f"receipt generation = {completion.get('generation')}, "
            f"current claim generation = {claim.get('generation')}",
        ),
        (
            "failure 2: the recovery scan wrote it, not the current procedure",
            completion.get("recorded_by") == "recovery-scan",
            f"recorded by = {completion.get('recorded_by')!r}",
        ),
        (
            "failure 2: the store history shows the overwrite explicitly",
            len(overwrites) == 1,
            f"overwrite events = {len(overwrites)}",
        ),
    ]

    print("before-Temporal gate: the kill must reproduce the failure")
    print()
    failed = 0
    for name, ok, detail in checks:
        marker = "PASS" if ok else "FAIL"
        if not ok:
            failed += 1
        print(f"  {marker}  {name}")
        print(f"        {detail}")
    print()
    if failed:
        print(f"{failed} check(s) failed: the before-world failure did not reproduce.")
        return 1
    print("Reproduced: one kill, two agents for one task, and the current")
    print("receipt overwritten by a stale generation. This is the before-world.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
