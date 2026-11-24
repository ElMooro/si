"""justhodl-credit-before-equity (2.1.0): the things a deploy of ANY engine must not be able to break.

  * it reads the warehouse and nothing else - no provider URL, no key, no invoke;
  * its split rules are the leaders engine's file, byte for byte;
  * it reads the keys the engines that write them actually write;
  * it still publishes every key the first engine published, because the stock screener, the invest engine and the
    equity donor inputs read it - and it runs once a day, after the corporate bonds desk;
  * its own suite passes (the real warehouse cut of ops 6942 rebuilt figure for figure);
  * the site build cannot re-colour its page: a widening spread stays the trouble colour;
  * (2.1) its AI stack theme is the AI infrastructure stack engine's own layers and tickers - a name added there is on the
    credit board the same day - with the three layers the credit board adds, and the 2.1 columns come after the 2.0 ones.
"""
import ast
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENGINE = ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity"
SRC = ENGINE / "source"
TEXT = (SRC / "lambda_function.py").read_text(encoding="utf-8")
ALL = "".join((SRC / n).read_text(encoding="utf-8") for n in ("lambda_function.py", "cbe_core.py", "cbe_names.py", "cbe_themes.py"))
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


def _literal(path, name):
    """The value a module assigns to `name` at its top level, read without importing the module."""
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError("%s assigns no %s" % (path.name, name))


def test_the_credit_boards_ai_stack_is_the_stack_engines_own_layers():
    theirs = _literal(ROOT / "aws/lambdas/justhodl-ai-infra-stack/source/lambda_function.py", "LAYERS")
    mine = _literal(SRC / "cbe_themes.py", "AI_STACK")
    assert [(k, label, list(seeds)) for k, label, _, seeds, _ in theirs] == [(k, label, list(seeds)) for k, label, seeds in mine], \
        "cbe_themes.AI_STACK must be the AI infrastructure stack engine's LAYERS (key, label, seeds), in its order: a ticker added to one is added to the other"
    stack = {t for _, _, seeds in mine for t in seeds}
    added = _literal(SRC / "cbe_themes.py", "HYPERSCALERS")[2] + _literal(SRC / "cbe_themes.py", "SERVERS")[2]
    assert not stack & set(added), "a layer the credit board adds may not take a ticker the stack engine already places: %s" % sorted(stack & set(added))
    assert set(_literal(SRC / "cbe_themes.py", "HYPERSCALERS")[2]) >= {"MSFT", "GOOGL", "AMZN", "META", "ORCL"}


def test_the_2_1_columns_come_after_the_2_0_columns_and_nothing_was_taken_away():
    cols = _literal(SRC / "lambda_function.py", "ISSUER_COLS")
    assert cols[:25] == ["id", "ticker", "name", "industry", "book", "country", "n_bonds", "value", "yield_pct", "dur", "spread_bp", "d_w1", "d_m1", "x_m1", "pct_m1", "n_m1", "d_m3", "d_ytd",
                         "cds_bp", "cds_d1m", "px_m1", "direction", "signal", "distressed", "tie"], "the 2.0 columns keep their names and their order"
    assert cols[25:] == ["pct_ytd", "stress", "rk_level", "rk_year", "rk_month", "theme", "layer", "cds_d3m", "cds_rank", "px_ytd", "pct_w1", "pct_m3"] and len(set(cols)) == len(cols)
    core = (SRC / "cbe_core.py").read_text(encoding="utf-8")
    assert 'VERSION = "2.1.0"' in core and 'CONTRACT = "credit-before-equity.v2"' in core, "2.1 adds to the v2 contract; it does not break it"
    build = TEXT[TEXT.index("def build("):TEXT.index("def lambda_handler(")]
    for key in ('"themes":', '"stress":', '"industries":', '"issuers":', '"leads":', '"names":', '"cds":', '"windows":', '"rules":'):
        assert key in build, "the document must carry %s" % key
    assert 'signal = "NONE"' in build and "conflict" in build, "no lead is called when the bonds and a liquid CDS disagree"
