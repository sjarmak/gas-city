#!/usr/bin/env python3
"""Invariant gate for the worker-kill demo.

Reads the evidence each arm's inspect run wrote and asserts the three proofs the
claim rests on:

  1. The bound session identity did not change across attempts.
  2. Exactly one session is bound for the (work item, generation) pair.
  3. One terminal receipt exists, and a stale claim token fails closed.

Plus the arm-specific fact that distinguishes them. In the mid-execute arm the
retry resumed from a heartbeat checkpoint, so it never asked the resolver again.
In the pre-checkpoint arm no checkpoint existed, the retry had to ask the
resolver, and the resolver returned the session that was already bound. The
second arm is the one that shows a heartbeat is not what prevents a second
agent.

Exits non-zero on any violation so run.sh fails loudly instead of printing a
result it did not earn.
"""
import argparse
import json
import sys
from pathlib import Path

ARMS = [
    (
        "mid-execute",
        "Worker killed mid-work; retry resumed from the checkpoint",
    ),
    (
        "pre-checkpoint",
        "Worker killed before any checkpoint; retry resolved again",
    ),
]


def load(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"missing evidence file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def checks_for(arm: str, ev: dict) -> list[tuple[str, bool, str]]:
    bound = ev.get("sessions_bound") or []
    store_session = ev.get("store_session_id") or ""
    activity_ids = ev.get("activity_ids_in_history") or []
    attempts = ev.get("activity_attempts", 0)
    resolve_calls = ev.get("resolve_calls", 0)
    orphan = ev.get("orphan_evidence") or {}

    checks = [
        (
            "proof 1: the bound session identity did not change across attempts",
            len(bound) == 1,
            f"distinct session identities seen by the agent adapter = {len(bound)}",
        ),
        (
            "proof 1: the work store recorded that same session",
            bool(store_session) and bound == [store_session],
            f"store session matches the one the agent bound = "
            f"{bool(store_session) and bound == [store_session]}",
        ),
        (
            "proof 2: exactly one session was created for this work item and generation",
            ev.get("sessions_created") == 1,
            f"session creations = {ev.get('sessions_created')}",
        ),
        (
            "proof 2: exactly one session record exists on disk for that pair",
            ev.get("session_records_on_disk") == 1,
            f"session records = {ev.get('session_records_on_disk')}",
        ),
        (
            "proof 3: exactly one terminal receipt was written to the work store",
            ev.get("store_terminal_receipts") == 1
            and ev.get("store_status") == "completed",
            f"receipts = {ev.get('store_terminal_receipts')}, "
            f"status = {ev.get('store_status')}",
        ),
        (
            "proof 3: the agent session produced one terminal record, not two",
            ev.get("agent_terminal_records") == 1,
            f"agent terminal records = {ev.get('agent_terminal_records')}",
        ),
        (
            "proof 3: a completion carrying the stale claim token failed closed",
            ev.get("stale_token_rejected") is True,
            f"rejection = {ev.get('stale_token_error')!r}",
        ),
        (
            "the retry ran under the same Activity identity",
            len(activity_ids) == 1,
            f"activity identities in history = {activity_ids}",
        ),
        (
            "Temporal really did retry (attempt two or later finished the work)",
            attempts >= 2,
            f"final attempt = {attempts}",
        ),
        (
            "the workflow closed on its own terms",
            "COMPLETED" in (ev.get("workflow_status") or "").upper(),
            f"status = {ev.get('workflow_status')}",
        ),
    ]

    if arm == "mid-execute":
        checks += [
            (
                "arm fact: the retry resumed from the checkpoint and never re-resolved",
                resolve_calls == 1,
                f"resolver calls = {resolve_calls}",
            ),
            (
                "arm fact: the agent kept working after its Worker was killed",
                orphan.get("first_agent_process_kept_working_after_kill") is True
                and orphan.get("work_entries_after_kill", 0) >= 1,
                f"work entries written after the kill = "
                f"{orphan.get('work_entries_after_kill')}, "
                f"of which after the pipe broke = "
                f"{orphan.get('work_entries_after_pipe_break')}",
            ),
            (
                "arm fact: exactly one agent process did the work",
                orphan.get("distinct_working_processes") == 1,
                f"processes that wrote work = "
                f"{orphan.get('distinct_working_processes')}",
            ),
        ]
    elif arm == "pre-checkpoint":
        checks += [
            (
                "arm fact: no checkpoint survived, so the retry had to ask the resolver again",
                resolve_calls >= 2,
                f"resolver calls = {resolve_calls}",
            ),
            (
                "arm fact: the second resolve returned the existing session, it did not mint one",
                ev.get("sessions_created") == 1 and resolve_calls >= 2,
                f"resolver calls = {resolve_calls}, "
                f"session creations = {ev.get('sessions_created')}",
            ),
        ]
    return checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifacts", help="out/run-artifacts directory")
    args = parser.parse_args()
    root = Path(args.artifacts)

    print("=== worker-kill demo: invariant report ===")
    print()
    print("  What this proves: a local fixture reproduction of the boundary.")
    print("  What it does not prove: production rollout. The unit converted here")
    print("  is proved by a bounded canary and runs in shadow; the part running")
    print("  continuously in production is result delivery and acknowledgement.")
    print()
    print("  Both arms are one result. Arm 1 passes even against a resolver that")
    print("  mints a duplicate session on every call, because its retry resumes")
    print("  from a checkpoint and never re-resolves. Only arm 2 shows that a")
    print("  duplicate launch was prevented. Do not present arm 1 on its own.")
    print()

    ok = True
    for arm, headline in ARMS:
        evidence = load(root / arm / "invariants.json")
        print(f"--- arm: {arm} ---")
        print(f"    {headline}")
        print(f"    workflow : {evidence.get('workflow_id')}")
        print(f"    activity : {', '.join(evidence.get('activity_ids_in_history') or [])}")
        print(f"    session  : {', '.join(evidence.get('sessions_bound') or [])}")
        print(f"    receipt  : status={evidence.get('store_status')} "
              f"outcome={evidence.get('store_outcome')}")
        print()
        for name, passed, detail in checks_for(arm, evidence):
            if not passed:
                ok = False
            print(f"    [{'PASS' if passed else 'FAIL'}] {name}")
            print(f"           {detail}")
        print()

    if ok:
        print("ALL INVARIANTS HOLD, BOTH ARMS.")
        return 0
    print("INVARIANT VIOLATION. Do not present this run as evidence.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
