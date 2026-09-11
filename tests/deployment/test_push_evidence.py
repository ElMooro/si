"""Deploy lane v2 — receipts go to the ops-evidence branch and never touch main."""
from __future__ import annotations

import runpy
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = runpy.run_path(str(ROOT / "scripts/push_evidence.py"))


def git(folder, *args):
    return subprocess.check_output(["git", *args], cwd=folder, text=True, stderr=subprocess.DEVNULL).strip()


def setup(folder: Path):
    origin = folder / "origin.git"; work = folder / "work"
    git(folder, "init", "-q", "--bare", "--initial-branch=main", str(origin))
    git(folder, "clone", "-q", str(origin), str(work))
    git(work, "config", "user.email", "fixture@example.invalid"); git(work, "config", "user.name", "Fixture")
    (work / "product.py").write_text("product\n")
    git(work, "add", "."); git(work, "commit", "-qm", "baseline"); git(work, "push", "-q", "origin", "main")
    return origin, work


def test_first_publish_creates_orphan_branch_and_main_is_untouched():
    with tempfile.TemporaryDirectory() as tmp:
        origin, work = setup(Path(tmp))
        main_before = git(work, "rev-parse", "HEAD")
        (work / "aws/ops/reports").mkdir(parents=True)
        (work / "aws/ops/reports/r1.json").write_text('{"a":1}\n')
        sha = MODULE["publish"](["aws/ops/reports/r1.json", "aws/ops/reports/missing.json"], "audit: r1", work)
        assert sha
        assert git(work, "rev-parse", "HEAD") == main_before
        assert git(origin, "rev-parse", "refs/heads/main") == main_before
        listing = git(origin, "ls-tree", "-r", "--name-only", "refs/heads/ops-evidence")
        assert "aws/ops/reports/r1.json" in listing and "product.py" not in listing
        # orphan: no ancestry back to main
        assert git(origin, "rev-list", "--count", "refs/heads/ops-evidence") == "1"


def test_second_publish_appends_history_and_unchanged_is_a_noop():
    with tempfile.TemporaryDirectory() as tmp:
        origin, work = setup(Path(tmp))
        (work / "aws/ops/reports").mkdir(parents=True)
        (work / "aws/ops/reports/r1.json").write_text('{"a":1}\n')
        MODULE["publish"](["aws/ops/reports/r1.json"], "audit: r1", work)
        (work / "aws/ops/reports/r1.json").write_text('{"a":2}\n')
        MODULE["publish"](["aws/ops/reports/r1.json"], "audit: r1 again", work)
        assert git(origin, "rev-list", "--count", "refs/heads/ops-evidence") == "2"
        MODULE["publish"](["aws/ops/reports/r1.json"], "audit: same", work)
        assert git(origin, "rev-list", "--count", "refs/heads/ops-evidence") == "2"
        assert git(work, "status", "--porcelain") == "?? aws/"     # working tree of main untouched
        assert git(work, "worktree", "list").count("\n") == 0      # temp worktree cleaned up


def test_audit_workflows_no_longer_commit_receipts_to_main():
    for name in ("audit-release-progress.yml", "audit-release-observation.yml"):
        text = (ROOT / ".github/workflows" / name).read_text()
        assert "scripts/push_evidence.py" in text, name
        assert "git push origin HEAD:main" not in text, name
        assert "commit_ops_evidence.py" not in text, name
