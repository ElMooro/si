"""Deploy lane v2 — the stub guard catches truncated writes of any size."""
from __future__ import annotations

import runpy
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = runpy.run_path(str(ROOT / "scripts/guard_stub_lambdas.py"))


def git(folder, *args):
    return subprocess.check_output(["git", *args], cwd=folder, text=True, stderr=subprocess.DEVNULL).strip()


def repo(folder: Path, body: str) -> Path:
    git(folder, "init", "-q", "--initial-branch=main")
    git(folder, "config", "user.email", "fixture@example.invalid")
    git(folder, "config", "user.name", "Fixture")
    src = folder / "aws/lambdas/justhodl-big/source"
    src.mkdir(parents=True)
    (src / "lambda_function.py").write_text(body)
    git(folder, "add", "."); git(folder, "commit", "-qm", "real engine")
    return folder


def rewrite(folder, body, message="truncated write"):
    (folder / "aws/lambdas/justhodl-big/source/lambda_function.py").write_text(body)
    git(folder, "add", "."); git(folder, "commit", "-qm", message)


def test_absolute_floor_rejects_placeholder_bodies():
    with tempfile.TemporaryDirectory() as tmp:
        root = repo(Path(tmp), "def lambda_handler(e, c):\n    return {}\n" * 60)
        rewrite(root, "# PLACEHOLDER\n")
        problems = MODULE["check"](root)
        assert len(problems) == 1 and "stub body" in problems[0]


def test_shrink_guard_rejects_a_large_engine_that_came_back_half_size():
    with tempfile.TemporaryDirectory() as tmp:
        big = "x = 1  # engine line\n" * 2000          # ~40 KB
        root = repo(Path(tmp), big)
        rewrite(root, big[: len(big) // 2 - 100])        # ~18 KB, well above the 500-byte floor
        problems = MODULE["check"](root)
        assert len(problems) == 1 and "looks truncated" in problems[0]


def test_shrink_ok_marker_permits_a_deliberate_rewrite():
    with tempfile.TemporaryDirectory() as tmp:
        big = "x = 1  # engine line\n" * 2000
        root = repo(Path(tmp), big)
        rewrite(root, "def lambda_handler(e, c):\n    return {'ok': True}\n" * 20, "lean rewrite [shrink-ok]")
        assert MODULE["check"](root) == []


def test_small_real_engines_and_growth_pass():
    with tempfile.TemporaryDirectory() as tmp:
        small = "import json\n\ndef lambda_handler(event, context):\n    return json.dumps({'ok': True})\n" * 12
        root = repo(Path(tmp), small)
        rewrite(root, small + "\n# new feature\n" * 50, "grow")
        assert MODULE["check"](root) == []


def test_base_sha_env_compares_against_the_push_base_not_the_last_commit():
    with tempfile.TemporaryDirectory() as tmp:
        big = "x = 1  # engine line\n" * 2000
        root = repo(Path(tmp), big)
        base = git(root, "rev-parse", "HEAD")
        rewrite(root, big[: len(big) // 3], "step 1 (truncate)")
        rewrite(root, big[: len(big) // 3] + "\n# noise\n", "step 2 (tiny growth)")
        assert MODULE["check"](root) == []                # HEAD~1 -> HEAD grew: invisible
        assert MODULE["check"](root, base)                # base -> HEAD lost 66%: caught
