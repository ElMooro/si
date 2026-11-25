"""justhodl-cds-desk (2.0.0): the things a deploy of ANY engine or page must not be able to break.

Khalid 2026-11-25: "audit and improve the CDS engine ... include more companies and all asset classes and even sovereign
CDS and public companies worldwide ... beautiful visuals ... make sure all the engine data is visible on the page".

  * the audit's root cause stays fixed: a coupon written "1" is one percent, never one basis point (AT&T at 7 bp);
  * every write of the engine goes through one door, and a dry run shuts it before anything is read;
  * the engine fetches the public DTCC files and the public records it always read - no key, no invoke, no event;
  * the reference obligation's identifier (ISIN / RED code) is never read into what the engine keeps;
  * the engine can finish: the memory and the time the whole tape needs;
  * the page loads the views before the desk script, the site build cannot re-colour them, and every key the engine
    publishes has words on the page - a key added to the engine without a place on the page fails here;
  * the credit board, which reads this desk's document, reads the 2.0 document;
  * the engine's own suite passes.
"""
import ast
import gzip
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENGINE = ROOT / "aws" / "lambdas" / "justhodl-cds-desk"
LAMBDA = (ENGINE / "source" / "lambda_function.py").read_text(encoding="utf-8")
CORE = (ROOT / "aws" / "shared" / "cds_desk.py").read_text(encoding="utf-8")
CONFIG = json.loads((ENGINE / "config.json").read_text(encoding="utf-8"))
VIEWS = (ROOT / "jh-cds-views.js").read_text(encoding="utf-8")
DESK = (ROOT / "jh-cds-desk.js").read_text(encoding="utf-8")
PAGE = (ROOT / "cds.html").read_text(encoding="utf-8")
FIXTURE = json.loads(gzip.decompress((ROOT / "tests" / "fixtures" / "cds-desk-v200.json.gz").read_bytes()))


def _core():
    sys.path.insert(0, str(ROOT / "aws" / "shared"))
    try:
        import cds_desk
        return cds_desk
    finally:
        sys.path.pop(0)


def _body(text, name):
    body = text[text.index("def %s(" % name):]
    return body[:body.index("\ndef ", 1)] if "\ndef " in body[1:] else body


def test_cds_desk_is_two_and_reads_a_percent_coupon_as_percent():
    C = _core()
    assert C.VERSION == "2.0.0"
    assert C.coupon_bp_of("1") == (100.0, "percent") and C.coupon_bp_of("5") == (500.0, "percent"), "the audit's root cause: AT&T at 7 bp"
    assert C.coupon_bp_of("0.01") == (100.0, "fraction") and C.coupon_bp_of("100") == (100.0, "bp")
    assert C.GROUPS == ("sovereign", "us_corp", "global_corp", "sub"), "the three 1.6 groups keep their names and their order; subordinated comes after"
    assert set(C.INDEX_FAMILIES_1_6) <= set(C.INDEX_FAMILIES) and len(C.INDEX_FAMILIES) >= 12, "no 1.6 index family is dropped"
    assert C.SPREAD_CEILING_BP == 10000.0 and C.MAX_PRINT_NOTIONAL <= 1e11


def test_cds_desk_writes_through_one_door_and_a_dry_run_shuts_it():
    assert LAMBDA.count("s3.put_object(") == 1 and "s3.put_object(" in _body(LAMBDA, "_put_object"), "every write goes through _put_object"
    door = _body(LAMBDA, "_put_object")
    assert door.index("if DRY:") < door.index("s3.put_object("), "a dry run returns before the write"
    assert "delete_object" not in LAMBDA and "delete_objects" not in LAMBDA, "the warehouse is never erased"
    handler = LAMBDA[LAMBDA.index("def lambda_handler("):]
    assert handler.index('DRY = bool(event.get("dry_run"))') < handler.index("par_curve()"), "the door is shut before anything is read"
    assert LAMBDA.count("_put_object(") >= 6, "document, history, tape, bank, the day's prints and the long records"


def test_cds_desk_fetches_public_records_only_and_fires_nothing():
    hosts = set(re.findall(r"https?://([A-Za-z0-9.\-]+)", LAMBDA))
    assert hosts <= {"kgc0418-tdw-data-0.s3.amazonaws.com", "fred.stlouisfed.org", "www.financialresearch.gov", "www.federalreserve.gov", "data-api.ecb.europa.eu", "www.imf.org",
                     "justhodl.ai", "www.esma.europa.eu", "registers.esma.europa.eu", "www.eba.europa.eu", "data.financialresearch.gov"}, hosts
    for word in ("managed_secret", "apikey=", "apiKey", "Authorization", ".invoke(", "put_events", "system_events", "financialmodelingprep.com", "api.polygon.io"):
        assert word not in LAMBDA and word not in CORE, "the desk needs no key and fires no event: %r" % word
    # 2.0 reads three more things, all from the warehouse: the credit board, the sovereign desk's reference quotes, the worldwide company list
    for key in ('CBE_KEY', 'SOV_REF_KEY = "data/global-sovereign.json"', 'PROFILE_PREFIX = "data/warm/fmp-bank/profile-bulk/"'):
        assert key in LAMBDA, key


def test_cds_desk_never_keeps_the_reference_obligations_identifier():
    cols = set(ast.literal_eval(re.search(r"TRADE_COLS = (\(.*?\))\n", LAMBDA, re.S).group(1))) | set(ast.literal_eval(re.search(r"RECORD_COLS = (\(.*?\))\n", LAMBDA, re.S).group(1)))
    assert not {c for c in cols if re.search(r"isin|cusip|underlier|^red", c, re.I)}, cols
    for text in (LAMBDA, CORE):
        assert "Underlier ID" not in text and not re.search(r"\b(ISIN|CUSIP)\b", text), "the Underlier ID column of the file is never read"
    tape = json.loads(gzip.decompress((ROOT / "tests" / "fixtures" / "cds-desk-tape-v200.json.gz").read_bytes()))
    assert not [c for c in tape["cols"] if re.search(r"isin|cusip|underlier|upi|\bid\b", c, re.I)], tape["cols"]
    assert not re.search(r"\b[A-Z]{2}[A-Z0-9]{9}[0-9]\b", json.dumps(tape["names"])), "nothing shaped like an ISIN in the published prints"


def test_cds_desk_can_finish_and_keeps_its_one_run_a_day():
    assert CONFIG["memory"] >= 2048 and CONFIG["timeout"] == 900 and len(CONFIG["description"]) <= 256
    sch = CONFIG["eventbridge_scheduler"]
    assert sch["cron"] == "cron(0 4 ? * TUE-SAT *)" and sch["input"] == {"action": "daily"}, "once a day, after the DTCC files post"
    assert "TIME_BUDGET_S" in LAMBDA and "budget_exhausted" in LAMBDA, "a long rebuild stops cleanly and says where it stopped"


def test_cds_page_mounts_the_views_and_the_build_leaves_their_colours_alone():
    assert 0 < PAGE.index("/jh-cds-views.js") < PAGE.index("/jh-cds-desk.js"), "the views load before the desk script that calls them"
    assert "jh-reskin-skip" in VIEWS, "the series colours were validated on the page's dark surface: the site build must not re-skin them"
    for colour in ("#f23645", "#089981", "#363a45", "#4C8DF6", "#B8801A", "#1F9E89", "#A56EE8"):
        assert colour in VIEWS, colour
    for slot in ("cds2-glance", "cds2-sub-panel", "cds2-products", "cds2-options", "cds2-structured", "cds2-quiet", "cds2-quality", "cds2-data"):
        assert 'id="%s"' % slot in DESK, "the page has no place for %s" % slot
    # nothing Khalid had on the 1.6 page is renamed, moved out or removed: its panels are still built, in the desk script
    for panel in ("cdsdesk-map", "cdsdesk-sov", "cdsdesk-us", "cdsdesk-gl", "bank-sovereign", "us-credit", "cdsdesk-interp", "Index strip", "Data sources"):
        assert panel in DESK, "a 1.6 panel is gone: %s" % panel


def test_everything_the_cds_desk_publishes_has_words_on_the_page():
    words = set(re.findall(r"(\w+): '", VIEWS[VIEWS.index("const DOC_WORDS = {"):VIEWS.index("const typeOf")]))
    published = set(re.findall(r'packet\["(\w+)"\] = ', CORE + LAMBDA)) | set(FIXTURE)
    assert published - words == set(), "published by the engine with no words on the page: %s" % sorted(published - words)
    fields = set(re.findall(r"(\w+): \[", VIEWS[VIEWS.index("const FIELDS = {"):VIEWS.index("function findRow")]))
    rows = [r for blk in FIXTURE["groups"].values() for kind in ("rows", "unpriced", "dormant", "quiet") for r in blk.get(kind) or []] + FIXTURE["indices"]
    shown_another_way = {"spark", "curve_last", "sign_sources_20d", "candidates", "unconfirmed", "listing"}     # drawn or listed, not a labelled figure
    seen = {k for r in rows for k in r} - shown_another_way
    assert seen - fields == set(), "a figure of a name with no label on its sheet: %s" % sorted(seen - fields)
    # the two documents written beside the main one are both read by the page
    assert "data/cds-desk-tape.json" in DESK + VIEWS and "data/cds-desk-history.json" in DESK
    assert '"data/cds-desk-tape.json"' in LAMBDA and "TAPE_KEY" in _body(LAMBDA, "publish")


def test_the_credit_board_reads_the_two_point_zero_document():
    src = ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source"
    sys.path.insert(0, str(src))
    try:
        import cbe_core
    finally:
        sys.path.pop(0)

    class Nobody:
        def match(self, key, *a, **kw):
            return None, None
    priced, bare, by_ticker, by_key = cbe_core.cds_rows(FIXTURE, Nobody())
    names = {r["name"]: r for r in priced}
    assert 60 < names["AT&T"]["spread_bp"] < 200, "the board carried AT&T's CDS at 7 bp from the 1.6 document"
    assert sum(1 for r in priced if r["name"] == "AT&T") == 1, "the desk no longer carries AT&T twice"
    assert all(isinstance(r["spread_bp"], (int, float)) and r["spread_bp"] <= 10000 for r in priced)
    assert len(priced) >= 40 and len(bare) >= 5


def test_cds_desk_own_suite_passes():
    out = subprocess.run([sys.executable, str(ENGINE / "tests" / "run_tests.py")], capture_output=True, text=True, timeout=600)
    assert out.returncode == 0, out.stdout[-1500:] + out.stderr[-1500:]
    assert re.search(r"(\d+)/\1 passed", out.stdout), out.stdout[-300:]
