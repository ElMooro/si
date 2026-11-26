"""Source-level facts for the engine fixes that came out of the chart-tab audit of 2026-11-26 (ops 6971 / 6972)."""
import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def src(engine, name="lambda_function.py"):
    return (ROOT / "aws" / "lambdas" / engine / "source" / name).read_text(encoding="utf-8")



def assert_in(a, b, msg=None):
    assert a in b, msg or "%r not found" % (a,)


def assert_not_in(a, b, msg=None):
    assert a not in b, msg or "%r found" % (a,)


def assert_equal(a, b, msg=None):
    assert a == b, msg or "%r != %r" % (a, b)


def assert_none(a, msg=None):
    assert a is None, msg or "%r is not None" % (a,)


def assert_false(a, msg=None):
    assert not a, msg or "%r is true" % (a,)


def test_plumbing_counts_every_indicator_it_scores():
    s = src("justhodl-plumbing-aggregator")
    assert_in('"n_indicators": len(enriched),', s)
    assert_in('"n_base_indicators": len(INDICATORS),', s)
    assert_equal(s.count('"n_indicators": len(INDICATORS)'), 0, "the base list alone printed 55/40 on the page")


def test_calendar_carries_the_feds_own_2027_schedule():
    s = src("justhodl-catalyst-calendar")
    tree = ast.parse(s)
    got = {n.targets[0].id: ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "").startswith("FOMC_DATES_")}
    # Federal Reserve press release of 2026-11-12 (revised tentative schedule for 2027)
    assert_equal([(a, b) for a, b, *_ in got["FOMC_DATES_2027"]], [
        ("2027-01-26", "2027-01-27"), ("2027-03-16", "2027-03-17"), ("2027-04-27", "2027-04-28"), ("2027-06-08", "2027-06-09"),
        ("2027-07-27", "2027-07-28"), ("2027-09-14", "2027-09-15"), ("2027-10-26", "2027-10-27"), ("2027-11-16", "2027-11-17")])
    assert_equal([row[3] for row in got["FOMC_DATES_2027"]].count(False), 1)
    assert_false(next(row for row in got["FOMC_DATES_2027"] if row[0] == "2027-10-26")[3], "October 26-27 is a strategy meeting with no decision")
    assert_equal(got["FOMC_DATES_2028"], [("2028-01-25", "2028-01-26", None, True)])
    assert_not_in("placeholder", s.lower())
    assert_not_in("typical pattern", s.lower())


def test_calendar_reads_only_kept_documents_for_its_new_sources():
    mod = src("justhodl-catalyst-calendar", "catalyst_sources.py")
    assert_none(re.search(r"urllib|requests|http[s]?://|boto3", mod), "the new sources fetch nothing")
    s = src("justhodl-catalyst-calendar")
    for key in ("data/econ-calendar.json", "data/finviz-earnings-calendar.json", "data/ipo-pipeline.json", "data/lockup-expiration.json", "data/rebalance-radar.json", "data/russell-recon-frontrun.json"):
        assert_in('"%s"' % key, s)
    assert_in('if isinstance(event, dict) and bool(event.get("dry_run"))', s.replace("dry = isinstance", "if isinstance"))
    assert_in('"sources": report,', s)


def test_contract_awards_are_given_a_date():
    s = src("justhodl-catalyst-calendar")
    assert_in('r.get("Action Date") or r.get("Last Modified Date") or r.get("Base Obligation Date") or r.get("Start Date")', s)
    assert_in('"date_basis"', s)


def test_onchain_join_takes_any_kept_document_with_metrics():
    s = src("justhodl-onchain-ratios")
    assert_not_in('if cq.get("status") == "LIVE":', s)
    assert_in('if isinstance(cm, dict) and cm:', s)
    assert_in('v.get("category") == "eth"', s)


def test_options_universe_is_listed_once():
    tree = ast.parse(src("justhodl-options-analytics"))
    u = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "UNIVERSE")
    assert_equal(len(u), 155)
    assert_equal(len(set(u)), 155)


if __name__ == "__main__":
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            _f()
    print("OK")
