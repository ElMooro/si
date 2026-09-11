"""Deploy lane v2 — the ops queue recovers cancelled runs and never re-runs history."""
from __future__ import annotations

import json
import runpy
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = runpy.run_path(str(ROOT / "scripts/ops_queue.py"))


def git(folder, *args):
    return subprocess.check_output(["git", *args], cwd=folder, text=True, stderr=subprocess.DEVNULL).strip()


def repo(folder: Path) -> Path:
    git(folder, "init", "-q", "--initial-branch=main")
    git(folder, "config", "user.email", "fixture@example.invalid")
    git(folder, "config", "user.name", "Fixture")
    (folder / "aws/ops/pending").mkdir(parents=True)
    (folder / "aws/ops/reports").mkdir(parents=True)
    (folder / "aws/ops/pending/legacy_1.py").write_text("print('legacy')\n")
    git(folder, "add", "."); git(folder, "commit", "-qm", "baseline")
    return folder


def write_commit(folder, name, body, message):
    (folder / "aws/ops/pending" / name).write_text(body)
    git(folder, "add", "."); git(folder, "commit", "-qm", message)
    return git(folder, "rev-parse", "HEAD")


def test_legacy_pending_scripts_are_frozen_and_never_recovered():
    with tempfile.TemporaryDirectory() as tmp:
        root = repo(Path(tmp))
        assert MODULE["baseline"](root) == 1
        ledger = json.loads((root / "aws/ops/reports/_ops_ledger.json").read_text())
        assert ledger["entries"]["aws/ops/pending/legacy_1.py"]["status"] == "legacy-frozen"
        chosen, notes = MODULE["select"](None, None, [], root)
        assert chosen == [] and notes == []


def test_cancelled_queued_script_is_recovered_on_the_next_run():
    with tempfile.TemporaryDirectory() as tmp:
        root = repo(Path(tmp))
        MODULE["baseline"](root)
        base = git(root, "rev-parse", "HEAD")
        a = write_commit(root, "ops_9001_a.py", "print('a')\n", "ops 9001")
        b = write_commit(root, "ops_9002_b.py", "print('b')\n", "ops 9002")
        # Run for push b..b only (a's queued run was cancelled): both must run.
        chosen, notes = MODULE["select"](a, b, [], root)
        assert chosen == ["aws/ops/pending/ops_9002_b.py", "aws/ops/pending/ops_9001_a.py"]
        assert ("aws/ops/pending/ops_9001_a.py", "recovered from queue") in notes
        # Record both; a re-run selects nothing.
        for p in chosen:
            MODULE["record"](p, "success", "1", b, root)
        assert MODULE["select"](b, b, [], root)[0] == []
        assert base != b


def test_failed_script_is_not_retried_until_its_content_changes():
    with tempfile.TemporaryDirectory() as tmp:
        root = repo(Path(tmp))
        MODULE["baseline"](root)
        a = write_commit(root, "ops_9003.py", "raise SystemExit(1)\n", "ops 9003")
        MODULE["record"]("aws/ops/pending/ops_9003.py", "failure", "2", a, root)
        assert MODULE["select"](None, None, [], root)[0] == []
        b = write_commit(root, "ops_9003.py", "print('fixed')\n", "ops 9003 fix")
        assert MODULE["select"](None, None, [], root)[0] == ["aws/ops/pending/ops_9003.py"]
        assert a != b


def test_skip_ops_pushes_holds_and_stale_scripts_are_not_recovered():
    with tempfile.TemporaryDirectory() as tmp:
        root = repo(Path(tmp))
        MODULE["baseline"](root)
        write_commit(root, "ops_9004_held.py", "print('later')\n", "park for manual dispatch [skip-ops]")
        (root / "aws/ops/pending/justhodl_ai_live_rollout.py").write_text("print('plan')\n")
        git(root, "add", "."); git(root, "commit", "-qm", "rollout plan")
        write_commit(root, "ops_9005_old.py", "print('old')\n", "ops 9005")
        far_future = time.time() + 30 * 86400
        chosen, notes = MODULE["select"](None, None, [], root, now=far_future)
        assert chosen == []
        reasons = dict(notes)
        assert reasons["aws/ops/pending/ops_9005_old.py"] == "older than recovery window"
        assert "aws/ops/pending/ops_9004_held.py" not in chosen


def test_explicit_dispatch_runs_only_the_named_script_plus_recovery():
    with tempfile.TemporaryDirectory() as tmp:
        root = repo(Path(tmp))
        MODULE["baseline"](root)
        write_commit(root, "ops_9006.py", "print('x')\n", "ops 9006")
        chosen, _ = MODULE["select"](None, None, ["aws/ops/pending/ops_9006.py"], root)
        assert chosen == ["aws/ops/pending/ops_9006.py"]
        # A script deleted/moved in the same push is dropped, never executed as a missing file;
        # queue recovery still applies on a dispatch (the unrecorded 9006 is picked up).
        chosen, _ = MODULE["select"](None, None, ["aws/ops/pending/does_not_exist.py"], root)
        assert chosen == ["aws/ops/pending/ops_9006.py"]


def test_run_ops_workflow_uses_the_queue_and_records_every_execution():
    text = (ROOT / ".github/workflows/run-ops.yml").read_text()
    assert "scripts/ops_queue.py select" in text
    assert "scripts/ops_queue.py record" in text
    assert "group: run-ops-serial" in text
    assert "cancel-in-progress: false" in text
