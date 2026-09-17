"""A red deploy leaves a readable, redacted report on main (scripts/deploy_failure_report.py)."""
from __future__ import annotations

import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = runpy.run_path(str(ROOT / "scripts/deploy_failure_report.py"))
# Planted secret SHAPES are assembled at runtime so no credential-shaped literal ever sits in the repo
# (GitHub push protection rightly blocks the literals).
FAKE_SECRET = "".join(chr(97 + i) for i in range(26)) + "0123456789" + "ABCD"          # 40 chars, AWS secret shape
FAKE_ACCESS = "AK" + "IA" + "ABCDEFGHIJKLMNOP"                                          # AWS access-id shape
FAKE_GH = "gh" + "p_" + "a" * 36
FAKE_JWT = "eyJ" + "hbGciOiJIUzI1NiJ9" + ".payload.sig"
LOG = ("Deployment static tests passed: 317\n── justhodl-x: tests/run_tests.py\nTraceback (most recent call last):\n"
       "  File \"tests/run_tests.py\", line 9, in <module>\n    test_scores_are_bounded()\nAssertionError: score 1.7 > 1\n"
       f"AWS_SECRET_ACCESS_KEY={FAKE_SECRET} Authorization: Bearer {FAKE_JWT}\n"
       f"token {FAKE_GH} id {FAKE_ACCESS}\n::error::justhodl-x: tests failed\n")


def test_report_keeps_the_traceback_and_redacts_every_planted_secret():
    rep = MODULE["build"](LOG, "preflight", "justhodl-x", "0123456789abcdef0123456789abcdef01234567", "https://example/run/1", 150)
    assert "AssertionError: score 1.7 > 1" in rep and "failed step: **preflight**" in rep
    for bad in (FAKE_GH, FAKE_ACCESS, FAKE_SECRET, FAKE_JWT):
        assert bad not in rep, bad
    assert "[REDACTED]" in rep and not MODULE["still_leaks"](rep)


def test_report_tail_is_bounded_and_error_lines_are_surfaced_first():
    long_log = "\n".join(f"line {i}" for i in range(1000)) + "\n::error::the real cause\n"
    rep = MODULE["build"](long_log, "deploy", "justhodl-y", "f" * 40, "", 40)
    assert "line 0\n" not in rep and "line 999" in rep and "Last 40 lines" in rep
    assert rep.index("::error::the real cause") < rep.index("Last 40 lines")
