#!/usr/bin/env python3
"""Publish ops/audit receipts to the ``ops-evidence`` branch (deploy lane v2).

Why: the release-observation workflows committed 2-3 receipts to ``main`` for
every completed workflow (117 bot commits on 2026-09-11 alone). Every one of
those pushes raced every lane's ``git pull --rebase && git push`` and buried
real commits. Receipts are evidence, not product: they now live on an orphan
branch that triggers nothing and collides with nobody, with the same history.

Usage:
  python3 scripts/push_evidence.py --run-id N --message "audit: ..." path [path ...]

Each path is copied verbatim (same relative path) into the evidence branch.
Missing paths are skipped with a notice. Pushes with fetch+rebase retry.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BRANCH = "ops-evidence"


def git(*args: str, cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True,
                            env={**os.environ, "GIT_EDITOR": "true"})
    if check and result.returncode:
        raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result


def branch_exists(root: Path, remote: str = "origin") -> bool:
    return git("ls-remote", "--exit-code", "--heads", remote, BRANCH, cwd=root, check=False).returncode == 0


def publish(paths: list[str], message: str, root: Path = ROOT, remote: str = "origin",
            attempts: int = 3) -> str:
    """Copy paths into the evidence branch, commit, push. Returns the new commit sha."""
    existing = [p for p in paths if (root / p).is_file()]
    for p in paths:
        if p not in existing:
            print(f"::notice::push_evidence: {p} not found, skipped", file=sys.stderr)
    if not existing:
        print("push_evidence: nothing to publish")
        return ""
    work = Path(tempfile.mkdtemp(prefix="evidence-"))
    try:
        if branch_exists(root, remote):
            git("fetch", "--quiet", "--depth=1", remote, BRANCH, cwd=root)
            git("worktree", "add", "--quiet", "--detach", str(work), "FETCH_HEAD", cwd=root)
        else:
            git("worktree", "add", "--quiet", "--detach", str(work), cwd=root)
            git("checkout", "--quiet", "--orphan", BRANCH, cwd=work)
            git("rm", "-rfq", "--cached", ".", cwd=work, check=False)
            for child in work.iterdir():
                if child.name == ".git":
                    continue
                shutil.rmtree(child) if child.is_dir() else child.unlink()
            (work / "README.md").write_text("# ops-evidence\n\nRelease/audit receipts published by workflows. "
                                             "Never merge into main.\n")
            git("add", "README.md", cwd=work)
        git("config", "user.name", os.environ.get("GIT_AUTHOR_NAME", "github-actions[bot]"), cwd=work)
        git("config", "user.email", os.environ.get("GIT_AUTHOR_EMAIL",
                                                   "41898282+github-actions[bot]@users.noreply.github.com"), cwd=work)
        for p in existing:
            dst = work / p
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / p, dst)
            git("add", "--", p, cwd=work)
        if not git("diff", "--cached", "--quiet", cwd=work, check=False).returncode and branch_exists(root, remote):
            print("push_evidence: receipts unchanged; nothing to commit")
            return git("rev-parse", "HEAD", cwd=work).stdout.strip()
        git("commit", "--quiet", "-m", message, cwd=work)
        for attempt in range(1, attempts + 1):
            if not git("push", remote, f"HEAD:refs/heads/{BRANCH}", cwd=work, check=False).returncode:
                sha = git("rev-parse", "HEAD", cwd=work).stdout.strip()
                print(f"push_evidence: published {len(existing)} file(s) to {BRANCH} @ {sha[:10]}")
                return sha
            git("fetch", "--quiet", "--depth=1", remote, BRANCH, cwd=work, check=False)
            git("rebase", "--quiet", "FETCH_HEAD", cwd=work, check=False)
        raise RuntimeError(f"push_evidence: push to {BRANCH} failed after {attempts} attempts")
    finally:
        git("worktree", "remove", "--force", str(work), cwd=root, check=False)
        shutil.rmtree(work, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--message", required=True)
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args(argv)
    publish(args.paths, f"{args.message} (run {args.run_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
