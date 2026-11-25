"""justhodl-credit-before-equity (3.0.0): the things a deploy of ANY engine must not be able to break.

  * it reads the warehouse and nothing else - no provider URL, no key, no invoke;
  * its split rules are the leaders engine's file, byte for byte;
  * it reads the keys the engines that write them actually write;
  * it still publishes every key the first engine published, because the stock screener, the invest engine and the
    equity donor inputs read it - and it runs once a day, after the corporate bonds desk;
  * its own suite passes (the real warehouse cut of ops 6942 rebuilt figure for figure);
  * the site build cannot re-colour its page: a widening spread stays the trouble colour;
  * (2.1) its AI stack theme is the AI infrastructure stack engine's own layers and tickers - a name added there is on the
    credit board the same day - with the three layers the credit board adds, and the 2.1 columns come after the 2.0 ones;
  * (3.0) the 3.0 columns come after the 2.1 ones; the government curves, the CDS history and the holdings columns it reads
    are what their engines write; the study never writes; and everything the engine publishes has a place on the page -
    a column or a key added to the engine without one fails here (Khalid 2026-11-25: "make sure all the data from the
    engines is viewable in the iux").
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
ALL = "".join((SRC / n).read_text(encoding="utf-8") for n in ("lambda_function.py", "cbe_core.py", "cbe_names.py", "cbe_themes.py", "cbe_study.py"))
CONFIG = json.loads((ENGINE / "config.json").read_text(encoding="utf-8"))


def test_credit_before_equity_reads_the_warehouse_only():
    for word in ("urllib", "requests", "http://", "https://", "managed_secret", "apiKey", "api.polygon.io", "financialmodelingprep", ".invoke(", "system_events"):
        assert word not in ALL, "the engine reads S3 only: %r" % word
    build = TEXT[TEXT.index("def build("):TEXT.index("def lambda_handler(")]
    assert "put_json(" not in build and "put_object(" not in build and "delete_object" not in ALL, "only the handler writes, and nothing is ever deleted"
    handler = TEXT[TEXT.index("def lambda_handler("):]
    assert handler.count("put_json(s3, ") == 9 and TEXT.count("put_object(") == 1, \
        "the document, the ledger days and the model ledger; since 3.0 the bond document, the day's holdings, the day's calls, the track, and (when a month has finished) the evidence and the history - nothing else is written"
    assert handler.index('event.get("dry_run")') < handler.index("if dry:") < handler.index("put_json(s3, "), "a dry run must return before anything is written"
    assert "return {" in handler[handler.index("if dry:"):handler.index("put_json(s3, ")], "a dry run must return before anything is written"
    for name in ("study", "build_panel", "build_cds_panel", "score_calls", "other_curves"):
        body = TEXT[TEXT.index("def %s(" % name):]
        body = body[:body.index("\ndef ", 1)]
        assert "put_json(" not in body and "put_object(" not in body, "%s reads; only the handler writes" % name
    study = (SRC / "cbe_study.py").read_text(encoding="utf-8")
    assert "boto3" not in study and "s3" not in study.lower().replace("r2(", "") and "open(" not in study, "cbe_study is arithmetic: no bucket, no file"


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
    # 3.0: the government curves are the bond war room's banked series, the CDS history is the CDS desk's, the kept holdings are the bond desk's own fields
    warroom = ROOT / "aws/lambdas/justhodl-bond-warroom/source/lambda_function.py"
    banked = {v[0] for v in _literal(warroom, "OY_SLUGS").values()}
    assert 'OY_PREFIX = "data/warm/official-yields/"' in warroom.read_text(encoding="utf-8") and 'YIELDS_ROOT = "data/warm/official-yields/"' in TEXT
    for ccy, files in _literal(SRC / "lambda_function.py", "CURVE_FILES").items():
        assert set(files.values()) <= banked, "the bond war room no longer banks %s for %s" % (sorted(set(files.values()) - banked), ccy)
    desk = (ROOT / "aws/lambdas/justhodl-cds-desk/source/lambda_function.py").read_text(encoding="utf-8")
    assert 'CDS_HIST_KEY = "data/cds-desk-history.json"' in TEXT and '"data/cds-desk-history.json"' in desk and '"points": pts' in (ROOT / "aws/shared/cds_long_context.py").read_text(encoding="utf-8")
    for field in _literal(SRC / "lambda_function.py", "HOLD_COLS"):
        assert '"%s":' % field in bonds, "the corporate bonds desk no longer writes %s, which the kept holdings carry" % field


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
    needs = _literal(SRC / "lambda_function.py", "STUDY_NEEDS_MS")
    assert CONFIG["timeout"] * 1000 >= needs + 180000, "the run needs the study's %d seconds and the daily build's on top: timeout %d" % (needs // 1000, CONFIG["timeout"])


def test_credit_before_equity_suite_passes():
    out = subprocess.run([sys.executable, str(ENGINE / "tests" / "run_tests.py")], capture_output=True, text=True, timeout=600)
    assert out.returncode == 0, (out.stdout[-3000:] + out.stderr[-2000:])


def test_the_site_builds_reskin_cannot_change_what_a_colour_on_the_credit_board_means():
    sys.path.insert(0, str(ROOT / "scripts"))
    import reskin_site
    script = (ROOT / "jh-credit-board.js").read_text(encoding="utf-8")
    page = (ROOT / "credit-before-equity.html").read_text(encoding="utf-8")
    sheet = (ROOT / "jh-credit-sheet.js").read_text(encoding="utf-8")
    assert "jh-reskin-skip" in script, "the build would re-colour the tiles: a tightening spread would no longer be the safety colour"
    assert "jh-reskin-skip" in sheet and reskin_site.reskin_text("a{stroke:#4C8DF6}") != "a{stroke:#4C8DF6}", "the build would re-colour the history chart's two lines into one colour"
    assert "#4C8DF6" in sheet and "#4C8DF6" not in page and "#B8801A" not in page, "the chart's series colours live in the script the build leaves alone, not in the page's style"
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
    assert cols[25:37] == ["pct_ytd", "stress", "rk_level", "rk_year", "rk_month", "theme", "layer", "cds_d3m", "cds_rank", "px_ytd", "pct_w1", "pct_m3"] and len(set(cols)) == len(cols)
    assert cols[37:] == ["cell", "pd_1y", "pd_5y", "wam", "due2y", "coupon", "step_bp", "px", "px_min", "s_short", "s_long", "curve_bp", "n_spread", "n_issuers", "fund_sector", "from_m1", "bar_m1",
                         "x_w1", "x_m3", "x_ytd", "px_w1", "px_m3", "cds_d1w", "cds_last", "vol", "band", "risk_1m", "risk_3m", "lo_3m", "hi_3m", "ccy", "n_other"], "the 3.0 columns come after the 2.1 ones"
    core = (SRC / "cbe_core.py").read_text(encoding="utf-8")
    assert 'VERSION = "3.0.0"' in core and 'CONTRACT = "credit-before-equity.v2"' in core, "3.0 adds to the v2 contract; it does not break it"
    assert _literal(SRC / "cbe_core.py", "BOOKS") == {"US IG": "ig", "US HY": "hy", "EM corporate": "em", "EM HY": "em"}, "the dollar books are read as they always were"
    assert "k" not in _literal(SRC / "lambda_function.py", "BOND_COLS"), "no identifier of a bond is republished: not the ISIN, not the CUSIP, not the bond desk's own key"
    build = TEXT[TEXT.index("def build("):TEXT.index("def lambda_handler(")]
    for key in ('"themes":', '"stress":', '"industries":', '"issuers":', '"leads":', '"names":', '"cds":', '"windows":', '"rules":'):
        assert key in build, "the document must carry %s" % key
    assert 'signal = "NONE"' in build and "conflict" in build, "no lead is called when the bonds and a liquid CDS disagree"


def test_everything_the_credit_board_engine_publishes_has_a_place_on_its_page():
    """Khalid 2026-11-25: "make sure all the data from the engines is viewable in the iux". A column or a key the engine adds
    without a label, a description and a place on the page fails here, before it can be deployed."""
    import gzip
    import re
    sheet = (ROOT / "jh-credit-sheet.js").read_text(encoding="utf-8")
    fields = set(re.findall(r'^    \["(\w+)", "[^"]+", "\w+", \d+, "', sheet, re.M))
    cols = _literal(SRC / "lambda_function.py", "ISSUER_COLS")
    assert not set(cols) - fields, "published for every company with no label and description on the page: %s" % sorted(set(cols) - fields)
    bond_fields = set(re.findall(r'\["(\w+)", "[^"]+", "\w+"\]', sheet[sheet.index("var BOND_FIELDS"):sheet.index("function bondsOf")]))
    assert not set(_literal(SRC / "lambda_function.py", "BOND_COLS")) - bond_fields - {"co", "ccy"}, "a column of the bond document the company sheet does not show"
    block = sheet[sheet.index("var DOC_KEYS = {"):sheet.index("function sizeOf")]
    described = set(re.findall(r'(\w+): \["', block))
    fix = json.loads(gzip.decompress((ROOT / "tests/fixtures/credit-before-equity-warehouse.json.gz").read_bytes()))
    published = set(fix["expected"]) | {"track"}                    # the handler adds the track to what build() returns
    assert not published - described, "top-level keys of the document with no description on the page: %s" % sorted(published - described)
    handler = TEXT[TEXT.index("def lambda_handler("):]
    assert 'payload["track"] = ' in handler
    page = (ROOT / "credit-before-equity.html").read_text(encoding="utf-8")
    for needed in ('id="s-next"', 'id="next"', 'id="s-data"', 'id="data"', 'id="sheet"', 'role="dialog"', "data-jh-chrome", "/jh-credit-sheet.js", "/jh-credit-board.js"):
        assert needed in page, "the page lost %s" % needed
    assert page.index("/jh-credit-board.js") < page.index("/jh-credit-sheet.js") < page.index("JHCreditBoard.start"), "the sheet script loads after the board and before it starts"
    for control in ("data-distress", 'data-csv="companies"', 'data-csv="bonds"', "data-sheet=", 'seg("cols"', "data-bond-sort", "data-data-open"):
        assert control in sheet or control in (ROOT / "jh-credit-board.js").read_text(encoding="utf-8"), "the page lost the control %s" % control
