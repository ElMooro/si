#!/usr/bin/env python3
"""Ops work queue (deploy lane v2, 2026-09-11).

Problem this fixes: run-ops.yml serialises on the ``run-ops-serial`` concurrency
group, and GitHub keeps at most ONE pending run per group. When run A is in
progress and run B is queued, a third push cancels B, and B's scripts were only
ever discoverable through B's own push boundaries -- so they silently never ran.

The fix is the standard work-queue shape: the queue is the source of truth, not
the event. Every ``aws/ops/pending/*.py`` is identified by its content hash and
recorded in a ledger. A run executes:

  * every pending script inside the push range (exactly as before), plus
  * every pending script whose content hash the ledger has never seen and which
    is eligible for recovery (recently touched, not held, not pushed with
    ``[skip-ops]``, not the plan-only rollout script).

Scripts that already ran (success OR failure) are recorded by hash and never
re-executed unless their content changes. The 467 legacy scripts that sat in
pending/ before this ledger existed are frozen at baseline and never auto-run.

Usage (run-ops.yml):
  python3 scripts/ops_queue.py select --base <sha> --head <sha>   # prints script paths
  python3 scripts/ops_queue.py select --explicit a.py b.py        # dispatch mode
  python3 scripts/ops_queue.py record <path> <status> --run-id N --commit <sha>
  python3 scripts/ops_queue.py baseline                           # one-time freeze
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PENDING = Path("aws/ops/pending")
LEDGER = Path("aws/ops/reports/_ops_ledger.json")
# Recovery only reaches back this far; anything older is a human decision.
RECOVERY_WINDOW_DAYS = 7
# Scripts that must never be auto-recovered (plan/apply flows with human gates).
HOLD = {
    "aws/ops/pending/justhodl_ai_live_rollout.py",
}


def git(*args: str, cwd: Path = ROOT, check: bool = True) -> str:
    result = subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True)
    if check and result.returncode:
        raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_ledger(root: Path = ROOT) -> dict:
    path = root / LEDGER
    if not path.exists():
        return {"schema": "ops-ledger.v1", "baseline_at": None, "entries": {}}
    return json.loads(path.read_text())


def save_ledger(ledger: dict, root: Path = ROOT) -> None:
    path = root / LEDGER
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ledger, indent=1, sort_keys=True) + "\n")


def pending_scripts(root: Path = ROOT) -> list[Path]:
    folder = root / PENDING
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.glob("*.py") if p.is_file())


def range_scripts(base: str, head: str, root: Path = ROOT) -> list[str]:
    """Scripts added/modified between the exact push boundaries (legacy behaviour)."""
    files = git("diff", "--name-only", base, head, cwd=root).splitlines()
    return [f for f in files if f.startswith(str(PENDING) + "/") and f.endswith(".py")]


def last_touch(path: str, root: Path = ROOT) -> tuple[int, str]:
    """(unix commit time, commit message) of the newest commit touching path."""
    out = git("log", "-1", "--format=%ct%x00%B", "--", path, cwd=root, check=False).strip()
    if not out:
        return 0, ""
    ts, _, msg = out.partition("\x00")
    return int(ts or 0), msg


def recoverable(path: str, ledger: dict, now: float, root: Path = ROOT) -> tuple[bool, str]:
    rel = path
    full = root / rel
    if rel in HOLD:
        return False, "held"
    digest = sha256_of(full)
    entry = ledger["entries"].get(rel)
    if entry and entry.get("sha256") == digest:
        return False, f"ledger:{entry.get('status')}"
    ts, msg = last_touch(rel, root)
    if ts and now - ts > RECOVERY_WINDOW_DAYS * 86400:
        return False, "older than recovery window"
    if "[skip-ops]" in msg:
        return False, "pushed with [skip-ops]"
    return True, "never recorded"


def select(base: str | None, head: str | None, explicit: list[str], root: Path = ROOT,
           now: float | None = None) -> tuple[list[str], list[tuple[str, str]]]:
    """Return (scripts_to_run, recovery_notes)."""
    now = time.time() if now is None else now
    ledger = load_ledger(root)
    chosen: list[str] = []
    notes: list[tuple[str, str]] = []
    if explicit:
        chosen.extend(explicit)
    elif base and head:
        chosen.extend(range_scripts(base, head, root))
    # Queue recovery: only when the ledger has a baseline (else we would run 467 legacy files).
    if ledger.get("baseline_at"):
        for p in pending_scripts(root):
            rel = str(p.relative_to(root))
            if rel in chosen:
                continue
            ok, why = recoverable(rel, ledger, now, root)
            if ok:
                chosen.append(rel)
                notes.append((rel, "recovered from queue"))
            elif why == "never recorded" or why.startswith("older"):
                notes.append((rel, why))
    # Only scripts that still exist run (a push may move a script to ran/).
    chosen = [c for c in dict.fromkeys(chosen) if (root / c).is_file()]
    return chosen, notes


def record(path: str, status: str, run_id: str, commit: str, root: Path = ROOT) -> None:
    ledger = load_ledger(root)
    full = root / path
    entry = {
        "sha256": sha256_of(full) if full.is_file() else None,
        "status": status,
        "run_id": run_id,
        "commit": commit,
        "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    ledger["entries"][path] = entry
    save_ledger(ledger, root)


def baseline(root: Path = ROOT) -> int:
    """Freeze every script currently in pending/ so it is never auto-recovered."""
    ledger = load_ledger(root)
    n = 0
    for p in pending_scripts(root):
        rel = str(p.relative_to(root))
        if rel not in ledger["entries"]:
            ledger["entries"][rel] = {
                "sha256": sha256_of(p), "status": "legacy-frozen",
                "run_id": None, "commit": None,
                "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            n += 1
    ledger["baseline_at"] = ledger.get("baseline_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save_ledger(ledger, root)
    return n


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("select")
    s.add_argument("--base")
    s.add_argument("--head")
    s.add_argument("--explicit", nargs="*", default=[])
    r = sub.add_parser("record")
    r.add_argument("path")
    r.add_argument("status", choices=["success", "failure", "cancelled", "skipped"])
    r.add_argument("--run-id", required=True)
    r.add_argument("--commit", required=True)
    sub.add_parser("baseline")
    args = parser.parse_args(argv)

    if args.cmd == "select":
        chosen, notes = select(args.base, args.head, args.explicit)
        for rel, why in notes:
            print(f"::notice::ops_queue {why}: {rel}", file=sys.stderr)
        print(" ".join(chosen))
        return 0
    if args.cmd == "record":
        record(args.path, args.status, args.run_id, args.commit)
        print(f"ops_queue recorded {args.path} {args.status}")
        return 0
    if args.cmd == "baseline":
        n = baseline()
        print(f"ops_queue baseline froze {n} legacy pending scripts")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
