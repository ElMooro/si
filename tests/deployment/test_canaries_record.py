"""Deploy gate for the canaries' record (2026-11-27): justhodl-warroom-weights 2.0, justhodl-canary-warroom 2.0,
justhodl-canary-grid's history completion, and canaries.html's "What the record says".

Khalid: "improve the engine : canaries ... test it against historical data to calibrate it".
"""
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WEIGHTS = ROOT / "aws/lambdas/justhodl-warroom-weights"
WARROOM = ROOT / "aws/lambdas/justhodl-canary-warroom"
GRID = ROOT / "aws/lambdas/justhodl-canary-grid"


def _run(path):
    out = subprocess.run([sys.executable, str(path)], capture_output=True, text=True, timeout=900, cwd=str(ROOT))
    assert out.returncode == 0, (out.stdout[-1500:], out.stderr[-800:])
    return out.stdout


def test_the_three_engine_suites_pass():
    assert "19 tests, 0 failed" in _run(WEIGHTS / "tests/run_tests.py")
    assert "8 tests, 0 failed" in _run(WARROOM / "tests/run_tests.py")
    assert "3 tests, 0 failed" in _run(GRID / "tests/run_tests.py")


def test_the_record_tests_the_rules_the_engines_run_today():
    out = subprocess.run([sys.executable, str(ROOT / "scripts/build_canary_defs.py"), "--check"], capture_output=True, text=True, cwd=str(ROOT))
    assert out.returncode == 0, out.stdout + out.stderr
    defs = json.loads((WEIGHTS / "source/canary_defs.json").read_text(encoding="utf-8"))
    assert len(defs["grid"]) == 64 and len(defs["plumbing"]) == 40 and sum(1 for p in defs["plumbing"] if p["deduped"]) == 7
    assert (WEIGHTS / "source/canary_record.py").read_bytes() == (WARROOM / "source/canary_record.py").read_bytes()


def test_the_record_reads_the_warehouse_and_no_provider():
    for name in ("canary_record.py", "record_io.py"):
        src = (WEIGHTS / "source" / name).read_text(encoding="utf-8")
        for word in ("stlouisfed", "financialmodelingprep", "polygon.io", "api_key", "apikey", "managed_secret"):
            assert word not in src, (name, word)
    pure = (WEIGHTS / "source/canary_record.py").read_text(encoding="utf-8")
    for word in ("import boto3", "urllib", "import datetime", "from datetime", "import time", "import os", "open("):
        assert word not in pure, word                                   # arithmetic only: no network, no clock, no storage
    io = (WEIGHTS / "source/record_io.py").read_text(encoding="utf-8")
    assert io.count("put_object(") == 1 and 'OUT_KEY = "data/canary-record.json"' in io and "if why:" in io and "elif not dry_run:" in io


def test_schedules_and_settings_keep_each_engine_to_one_run_a_day_or_less():
    wr = json.loads((WARROOM / "config.json").read_text(encoding="utf-8"))
    assert wr["schedule"]["cron"] == "cron(50 20 * * ? *)" and wr["timeout"] == 120 and len(wr["description"]) <= 256
    wt = json.loads((WEIGHTS / "config.json").read_text(encoding="utf-8"))
    assert wt["timeout"] == 900 and wt["memory"] == 1024 and "schedule" not in wt and len(wt["description"]) <= 256   # its monthly schedule lives in the scheduler manifest
    manifest = (ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8")
    i = manifest.index('"name": "justhodl-warroom-weights-monthly"')
    assert '"expr": "cron(10 7 1 * ? *)"' in manifest[i - 200:i] and '"state": "ENABLED"' in manifest[i:i + 120]


def test_the_war_room_keeps_everything_it_published_and_adds_beside_it():
    src = (WARROOM / "source/lambda_function.py").read_text(encoding="utf-8")
    for key in ('"top_picks": picks', '"barometer": barometer', '"master":', '"mechanisms": cards', '"firing": firing[:40]', '"all_canaries": all_cans',
                '"divergences": divs', '"brain_playbook": brain_playbook()', '"conviction_leads": conviction', '"velocity": velocity'):
        assert key in src, key
    assert '"calibrated": calibrated' in src and 'VERSION = "2.0.0"' in src and 'RECORD_KEY = "data/canary-record.json"' in src
    assert src.count("S3.put_object(") == 3 and "if not dry:" in src and 'return {"ok": True, "dry_run": True' in src
    assert "import canary_record as CR" in src and "CR.live_reading(record, live, today=today, base=base)" in src


def test_the_grid_completes_a_short_answer_from_the_warehouse_only():
    src = (GRID / "source/lambda_function.py").read_text(encoding="utf-8")
    assert "def merge_older(rows, older, limit):" in src and "if out and len(out) < limit * 0.9:" in src
    body = src[src.index("def warehouse_rows(series_id):"):src.index("# ── signal definitions") if "# ── signal definitions" in src[src.index("def warehouse_rows(series_id):"):] else src.index("def fred(series_id, limit):")]
    assert "FRED_KEY" not in body and "api_key" not in body


def test_the_page_shows_the_record_and_the_kit_lists_the_document():
    page = (ROOT / "canaries.html").read_text(encoding="utf-8")
    js = (ROOT / "jh-canary-record.js").read_text(encoding="utf-8")
    assert '<section id="s-record"></section>' in page and "/jh-canary-record.js?v=" in page and "JHCanaryRecord.mount(window,d)" in page
    kit = (ROOT / "jh-engine-data.js").read_text(encoding="utf-8")
    for key in ("data/canary-warroom.json", "data/warroom-history.json", "data/warroom-weights.json", "data/warroom-replay.json", "data/canary-record.json"):
        assert "key: '%s'" % key in kit, key                             # every field of every document is listed on the page
    for word in ("cal.level", "cal.turn", "cal.cell", "cal.record", "cal.what_is_counted", "rec.canaries", "barometer || {}).months", "level_against_turn",
                 "barometer || {}).mechanisms", "barometer || {}).turns", "market || {}).falls", "rec.method", "rec.live_check", "rec.inputs", "rec.verdicts"):
        assert word in js, word
    import gzip
    fx = json.loads(gzip.decompress((ROOT / "tests/fixtures/canary-record-page.json.gz").read_bytes()))
    row = next(c for c in fx["record"]["canaries"] if c.get("months"))
    cols = re.findall(r'\["([a-z_0-9]+)", "[^"]+", "[a-z0-9]+"\]', js[js.index("var COLS = ["):js.index("var TIPS")])
    assert len(cols) == 16 and all(c in row for c in cols), [c for c in cols if c not in row]
    shown = set(cols) | {"id", "falls_covered", "why_not", "flag", "source", "votes_live"}
    hidden = set(row) - shown - {"key", "group", "frequency", "lag_days", "fire_at", "lead_claimed", "to", "fall_after_firing", "fall_after_quiet", "fall_any", "early_months",
                                 "early_fall_any", "auc_se", "warned_by_line", "false_alarm_share", "last_day", "last_stress", "gap_to_live"}
    assert hidden == set(), hidden                                       # a new field of the record must be given a place, here or in the kit's "Every field"
