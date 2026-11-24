"""justhodl-credit-before-equity (2.0.0): the things a deploy of ANY engine must not be able to break.

  * it reads the warehouse and nothing else - no provider URL, no key, no invoke;
  * its split rules are the leaders engine's file, byte for byte;
  * it reads the keys the engines that write them actually write;
  * it still publishes every key the first engine published, because the stock screener, the invest engine and the
    equity donor inputs read it - and it runs once a day, after the corporate bonds desk;
  * its own suite passes (the real warehouse cut of ops 6942 rebuilt figure for figure);
  * the site build cannot re-colour its page: a widening spread stays the trouble colour.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENGINE = ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity"
SRC = ENGINE / "source"
TEXT = (SRC / "lambda_function.py").read_text(encoding="utf-8")
ALL = "".join((SRC / n).read_text(encoding="utf-8") for n in ("lambda_function.py", "cbe_core.py", "cbe_names.py"))
CONFIG = json.loads((ENGINE / "config.json").read_text(encoding="utf-8"))


def test_credit_before_equity_reads_the_warehouse_only():
    for word in ("urllib", "requests", "http://", "https://", "managed_secret", "apiKey", "api.polygon.io", "financialmodelingprep", ".invoke(", "system_events"):
        assert word not in ALL, "the engine reads S3 only: %r" % word
    build = TEXT[TEXT.index("def build("):TEXT.index("def lambda_handler(")]
    assert "put_json(" not in build and "put_object(" not in build and "delete_object" not in ALL, "only the handler writes, and nothing is ever deleted"
    handler = TEXT[TEXT.index("def lambda_handler("):]
    assert handler.count("put_json(s3, ") == 3 and TEXT.count("put_object(") == 1, "the document, the ledger days and the model ledger - nothing else is written"
    assert handler.index('event.get("dry_run")') < handler.index("put_json(s3, "), "a dry run must return before anything is written"


def test_credit_before_equity_tape_rules_are_the_leaders_engines():
    mine = (SRC / "leaders_tape.py").read_bytes()
    theirs = (ROOT / "aws/lambdas/justhodl-leaders/source/leaders_tape.py").read_bytes()
    assert hashlib.sha256(mine).hexdigest() == hashlib.sha256(theirs).hexdigest(), "aws/lambdas/justhodl-credit-before-equity/source/leaders_tape.py must stay byte-identical to the leaders engine's"


def test_credit_before_equity_reads_the_keys_their_engines_write():
    bonds = "".join(p.read_text(encoding="utf-8") for p in (ROOT / "aws/lambdas/justhodl-corp-bonds/source").glob("*.py"))
    assert 'BONDS_KEY = "data/corp-bonds.json"' in TEXT and "data/corp-bonds.json" in bonds
    for field in ('"k": key', '"rate": rate', '"chg": chg', '"chg_y": chg_y', '"dur": dur', '"demand": demand', '"book": book', '"move_as_of"', '"ytd_as_of"'):
        assert field in bonds, "the corporate bonds desk no longer writes %s" % field
    cds = (ROOT / "aws/shared/cds_desk.py").read_text(encoding="utf-8")
    assert 'CDS_KEY = "data/cds-desk.json"' in TEXT
    for field in ("spread_bp", "chg_1m_bp", "chg_1w_bp", "chg_3m_bp", "trades_30d", "liquid", "us_corp", "global_corp"):
        assert field in cds, "the CDS desk no longer writes %s" % field
    bankfirst = (ROOT / "aws/shared/polygon_bankfirst.py").read_text(encoding="utf-8")
    assert 'EOD_ROOT = "data/warm/polygon-full/eod/"' in TEXT and 'ROOT = "data/warm/polygon-full/eod/"' in bankfirst
    assert 'PAR_KEY = "data/warm/treasury-par/curve.json.gz"' in TEXT and 'MODEL_KEY = "data/cds-monitor.json"' in TEXT


def test_credit_before_equity_still_publishes_what_its_readers_read_once_a_day():
    contract = json.loads((ROOT / "config" / "engine-contracts.json").read_text(encoding="utf-8"))["contracts"]["data/credit-before-equity.json"]
    for key in contract["required_keys"]:
        assert '"%s"' % key in TEXT, "the document's contract requires %r" % key
    for field in ('"ticker": tk', '"d_distance_to_default": d_dd', '"synthetic_cds_bp": cds', '"distance_to_default": dd', '"hist_n": len(dates)', '"credit_direction":', '"signal": signal'):
        assert field in TEXT, "a reader of the document reads %s" % field
    invest = (ROOT / "aws/lambdas/justhodl-invest/source/lambda_function.py").read_text(encoding="utf-8")
    assert '"d_distance_to_default"' in invest and 'CREDIT_BEFORE_EQUITY_KEY = "data/credit-before-equity.json"' in invest
    donors = (ROOT / "aws/shared/equity_donor_inputs.py").read_text(encoding="utf-8")
    assert "rows_by(credit,'names')" in donors
    assert CONFIG["schedule"]["cron"] == "cron(35 22 * * ? *)" and CONFIG["schedule"]["rule_name"] == "justhodl-credit-before-equity-daily", "once a day at 22:35 UTC"
    bonds_cfg = json.loads((ROOT / "aws/lambdas/justhodl-corp-bonds/config.json").read_text(encoding="utf-8"))
    assert "15 20" in json.dumps(bonds_cfg), "the corporate bonds desk runs at 20:15 UTC, before this engine"
    assert len(CONFIG["description"]) <= 256 and CONFIG["environment"] == {} and CONFIG["memory"] >= 1024


def test_credit_before_equity_suite_passes():
    out = subprocess.run([sys.executable, str(ENGINE / "tests" / "run_tests.py")], capture_output=True, text=True, timeout=300)
    assert out.returncode == 0, (out.stdout[-3000:] + out.stderr[-2000:])


def test_the_site_builds_reskin_cannot_change_what_a_colour_on_the_credit_board_means():
    sys.path.insert(0, str(ROOT / "scripts"))
    import reskin_site
    script = (ROOT / "jh-credit-board.js").read_text(encoding="utf-8")
    page = (ROOT / "credit-before-equity.html").read_text(encoding="utf-8")
    assert "jh-reskin-skip" in script, "the build would re-colour the tiles: a tightening spread would no longer be the safety colour"
    assert reskin_site.reskin_text("a{color:#089981}") != "a{color:#089981}", "the re-skin no longer touches this: the opt-out can go"
    assert "jh-reskin-skip" not in page
    style = page[page.index("<style>"):page.index("</style>")]
    assert reskin_site.reskin_text(style) == style, "the page's frame would look different on the site than in this file"
