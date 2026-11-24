"""justhodl-regime-map (the Risk Map, 2.0.0): the things a deploy of ANY engine must not be able to break.

  * it reads the warehouse and nothing else - no provider URL, no key, no invoke;
  * its tape rules are the leaders engine's file, byte for byte (a split rule changed there changes here in the same commit);
  * it reads the keys the engines that write them actually write;
  * it still publishes what the first engine published, because the paper book, the trust engines, the strategist and
    the cycle clock read it - and it runs once a day, before the paper book;
  * its own suite passes (the real warehouse cut of ops 6936 rebuilt figure for figure).
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENGINE = ROOT / "aws" / "lambdas" / "justhodl-regime-map"
SRC = ENGINE / "source"
TEXT = (SRC / "lambda_function.py").read_text(encoding="utf-8")
ALL = "".join((SRC / n).read_text(encoding="utf-8") for n in ("lambda_function.py", "risk_math.py", "risk_universe.py"))
CONFIG = json.loads((ENGINE / "config.json").read_text(encoding="utf-8"))


def test_the_risk_map_reads_the_warehouse_only():
    for word in ("urllib", "requests", "http://", "https://", "managed_secret", "apiKey", "api.polygon.io", "financialmodelingprep", ".invoke(", "system_events"):
        assert word not in ALL, "the Risk Map engine reads S3 only: %r" % word
    assert "put_object" not in ALL.split("def lambda_handler(")[0], "only the handler writes"
    assert TEXT.count("put_object(") == 2 and 'OUT_KEY, PREV_KEY = "data/regime-map.json", "data/regime-map-prev.json"' in TEXT


def test_the_risk_map_tape_rules_are_the_leaders_engines():
    mine = (SRC / "leaders_tape.py").read_bytes()
    theirs = (ROOT / "aws/lambdas/justhodl-leaders/source/leaders_tape.py").read_bytes()
    assert hashlib.sha256(mine).hexdigest() == hashlib.sha256(theirs).hexdigest(), "aws/lambdas/justhodl-regime-map/source/leaders_tape.py must stay byte-identical to the leaders engine's"


def test_the_risk_map_reads_the_keys_their_engines_write():
    today = (ROOT / "aws/lambdas/justhodl-polygon-today/source/lambda_function.py").read_text(encoding="utf-8")
    assert 'TODAY_KEY = "data/warm/polygon-full/today.json.gz"' in TEXT and '"data/warm/polygon-full/today.json.gz"' in today
    bankfirst = (ROOT / "aws/shared/polygon_bankfirst.py").read_text(encoding="utf-8")
    assert 'EOD_ROOT = "data/warm/polygon-full/eod/"' in TEXT and 'ROOT = "data/warm/polygon-full/eod/"' in bankfirst
    katlin = (ROOT / "aws/lambdas/justhodl-katlin/source/lambda_function.py").read_text(encoding="utf-8")
    assert 'COIN_ROOT = "data/warm/katlin/crypto-bars/"' in TEXT and 'CRYPTO_ROOT = "data/warm/katlin/crypto-bars/"' in katlin
    fmp_bank = (ROOT / "aws/shared/fmp_bank.py").read_text(encoding="utf-8")
    assert 'FMP_ROOT = "data/warm/fmp/historical-price-eod-full/"' in TEXT and 'WALKER_ROOT = "data/warm/fmp/"' in fmp_bank and '"historical-price-eod/full": "historical-price-eod-full"' in fmp_bank


def test_the_risk_map_still_publishes_the_first_engines_feed_once_a_day():
    contract = json.loads((ROOT / "config" / "engine-contracts.json").read_text(encoding="utf-8"))["contracts"]["data/regime-map.json"]
    for key in contract["required_keys"]:
        assert '"%s"' % key in TEXT, "the feed's contract requires %r" % key
    for consumer_reads in ('"regime": regime', '"sleeves": sleeves', '"ranked": ranked', '"booming":', '"destroyed":', '"regime_changed":', '"transitions": transitions[:14]'):
        assert consumer_reads in TEXT, consumer_reads
    for label in ("BIFURCATED", "BROAD RISK-ON", "NARROW / CONCENTRATED", "BROAD RISK-OFF", "MIXED / ROTATIONAL"):
        assert 'label = "%s"' % label in TEXT, "the regime labels other engines bucket by: %s" % label
    assert CONFIG["schedule"]["cron"] == "cron(0 21 * * ? *)" and CONFIG["schedule"]["rule_name"] == "justhodl-regime-map-daily", "once a day at 21:00 UTC, before the paper book's 21:30"
    assert len(CONFIG["description"]) <= 256 and CONFIG["environment"] == {}
    paper = json.loads((ROOT / "aws/lambdas/justhodl-paper-book/config.json").read_text(encoding="utf-8"))
    assert paper["schedule"]["cron"] == "cron(30 21 * * ? *)"


def test_the_risk_map_suite_passes():
    out = subprocess.run([sys.executable, str(ENGINE / "tests" / "run_tests.py")], capture_output=True, text=True, timeout=300)
    assert out.returncode == 0, (out.stdout[-3000:] + out.stderr[-2000:])
