"""justhodl-insider-cluster-scanner hygiene tests (v2.2.0; run by run_tests.py in its own process): the hygiene rules and the handler end to end on REAL rows
of the banked insider feed (fixtures/insider_feed.json: 14 tickers as ops 6831 read them on 2026-11-18, each filing
line repeated the way the day files repeat it) with the factor book's rows for them.  The bank, the book, S3 and the
clock are in-memory fakes.  No network, no AWS.

Run: python3 aws/lambdas/justhodl-insider-cluster-scanner/tests/test_hygiene.py
"""
from datetime import datetime, timezone
from types import SimpleNamespace
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
sys.path[:0] = [SRC]
os.environ["MIN_BUY_VALUE_USD"] = "5000"          # the live function's setting (ops 6831 read the feed with it)
os.environ["TELEGRAM_TOKEN"] = ""
FX = json.load(open(os.path.join(HERE, "fixtures", "insider_feed.json"), encoding="utf-8"))
ROWS, BOOK = FX["rows"], FX["book"]
STATE = {"book": True}


class FakeS3:
    def __init__(self):
        self.store = {}

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey %s" % Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **_kw):
        self.store[Key] = Body if isinstance(Body, bytes) else Body.encode("utf-8")


S3 = FakeS3()
sys.modules["boto3"] = SimpleNamespace(client=lambda *a, **k: S3)
sys.modules["managed_secret"] = SimpleNamespace(managed_secret=lambda *a, **k: "")
sys.modules["fmp_bankfirst"] = SimpleNamespace(install=lambda: None)
sys.modules["fmp_bank"] = SimpleNamespace(walker_days=lambda dataset, days=30, end=None, where=None: [("2026-11-13", [r for r in ROWS if where is None or where(r)])])
sys.modules["bank_enrich"] = SimpleNamespace(available=lambda: STATE["book"], as_of=lambda: FX["book_as_of"], factor=lambda t: None,
                                             lookup=lambda t, fields=None: ({k: BOOK[t].get(k) for k in (fields or BOOK[t])} if STATE["book"] and t in BOOK else None))

import insider_hygiene as hygiene  # noqa: E402
import lambda_function as lf  # noqa: E402


class FixedClock(datetime):          # the engine's 30-day window is counted from "now": pin it to the day the feed was read
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 11, 17, 13, 50, tzinfo=tz or timezone.utc)


lf.datetime = FixedClock


def engine_rows():
    txns, _days, _n = lf.bank_feed_transactions()
    return txns


def run():
    S3.store.clear()
    lf.lambda_handler({}, None)
    return json.loads(S3.store[lf.S3_KEY])


def near(a, b, tol=0.011):
    assert abs(a - b) <= tol, (a, b)


def test_the_feed_repeats_a_filing_line_and_unique_keeps_it_once():
    txns = engine_rows()
    lines = hygiene.unique(txns)
    assert len(txns) == 175 and len(lines) == 60, (len(txns), len(lines))
    lona = [t for t in lines if t["ticker"] == "LONA"]
    assert len([t for t in txns if t["ticker"] == "LONA"]) == 3 and len(lona) == 1 and lona[0]["value"] == 9919161280.9
    assert lona[0]["security"] == "Common Stock" and lona[0]["role"] == "10% Owner"
    assert hygiene.unique(lines) == lines


def test_one_filing_under_three_tickers_is_kept_once_under_the_ticker_the_tape_knows():
    lines = hygiene.unique(engine_rows())
    merged, groups = hygiene.merge_same_filing(lines, lambda s: s in BOOK)
    assert groups == [{"cik": "0001567892", "kept": "MNK", "also_filed_as": ["MNKKQ", "MNKPF"], "n_transactions": 4}]
    assert {t["ticker"] for t in merged if t["cik"] == "0001567892"} == {"MNK"} and len(merged) == len(lines) - 8
    mnk = [t for t in merged if t["ticker"] == "MNK"]
    assert all(t["also_filed_as"] == ["MNKKQ", "MNKPF"] for t in mnk)
    near(sum(t["value"] for t in mnk), 40049485.0)
    # the tape decides when it knows one of them; a line that exists under ONE ticker only is another class and stays
    _, g2 = hygiene.merge_same_filing(lines, lambda s: s == "MNKPF")
    assert g2[0]["kept"] == "MNKPF" and g2[0]["also_filed_as"] == ["MNK", "MNKKQ"]
    a = {"ticker": "GOOG", "cik": "1", "accession": "x", "insider_name": "A", "txn_date": "2026-11-01", "shares": 10, "price": 100.0}
    b = dict(a, ticker="GOOGL", shares=20)
    out, g3 = hygiene.merge_same_filing([a, b])
    assert out == [a, b] and g3 == []
    assert hygiene.merge_same_filing([dict(a, cik=""), dict(a, cik="", ticker="GOOGL")])[1] == [], "no issuer CIK, no merge"


def test_a_price_that_cannot_be_is_set_aside_and_a_possible_one_is_counted_and_labelled():
    lines, _ = hygiene.merge_same_filing(hygiene.unique(engine_rows()), lambda s: s in BOOK)
    counted, aside, summary = hygiene.screen(lines, BOOK)
    assert sorted((a["ticker"], a["value"]) for a in aside) == [("AASP", 200000000000.0), ("LONA", 9919161280.9)]
    lona = next(a for a in aside if a["ticker"] == "LONA")
    assert lona["reason"] == "priced at 907.6 times the tape and worth more than the whole company (72,478% of its market value)", lona["reason"]
    assert lona["tape_price"] == 3.47 and lona["accession"] == "0002126186-26-000002" and lona["insider_name"] == "Portman David J."
    aasp = next(a for a in aside if a["ticker"] == "AASP")
    assert BOOK["AASP"]["high_52w"] is None and aasp["tape_ratio"] > 100000, "no 52-week range on the book: the last price is the tape"
    assert summary["n_set_aside"] == 2 and summary["usd_set_aside"] == 209919161280.9 and summary["n_checked"] == len(lines)
    assert summary["n_in_range"] + summary["n_off_tape"] + summary["n_no_tape"] == len(counted) == len(lines) - 2
    status = lambda t: {x["price_check"] for x in counted if x["ticker"] == t}  # noqa: E731
    assert status("BH") == {"off_tape", "in_range"}, "class A at $1,768 against BH at $362 is another share class and a real purchase; the class B line is on the tape"
    assert status("KNOP") == {"off_tape"} and status("BFH") == {"off_tape"} and status("CRESY") == {"off_tape"}
    assert status("MNK") == {"no_tape"} and status("UPBD") == {"in_range"} and status("MTDR") == {"in_range"}
    nfrx = [x for x in counted if x["ticker"] == "NFRX"]
    assert {x["price_check"] for x in nfrx} == {"in_range"} and max(x["value"] for x in nfrx) > BOOK["NFRX"]["mcap"], "a price on the tape is never set aside, whatever the book's market value says"
    assert any(x["ticker"] == "AASP" and x["price"] == 7.0 and x["security"] == "Common Stock Purchase Warrants" for x in counted), "the $7 warrant line of the same filing stays"


def test_the_rule_at_its_edges():
    ref = {"price": 10.0, "high_52w": 12.0, "low_52w": 8.0, "mcap": 1_000_000.0}
    chk = lambda price, shares, r=ref: hygiene.tape_check({"price": price, "value": price * shares}, r)["status"]  # noqa: E731
    assert chk(4.0, 10) == "in_range" and chk(18.0, 10) == "in_range" and chk(3.99, 10) == "off_tape" and chk(18.01, 10) == "off_tape"
    assert chk(60.0, 10) == "off_tape", "far off the tape but small against the company: another class, counted"
    assert chk(60.0, 100_000) == "impossible", "far off the tape AND worth more than the company"
    assert chk(20.0, 100_000) == "off_tape", "worth more than the company but not far off the tape (a recapitalisation): counted"
    assert chk(1.9, 1_000_000) == "impossible" and chk(2.1, 1_000_000) == "off_tape"
    bare = {"price": 10.0}
    assert chk(29.0, 10, bare) == "in_range" and chk(31.0, 10, bare) == "off_tape" and chk(999.0, 10, bare) == "off_tape" and chk(1001.0, 10, bare) == "impossible" and chk(0.09, 10, bare) == "impossible"
    assert chk(5.0, 10, None) == "no_tape" and chk(5.0, 10, {"mcap": 5.0}) == "no_tape" and chk(0, 10) == "no_tape"
    assert hygiene.screen([{"ticker": "X", "price": 5.0, "value": 50.0}], {})[2] == {"n_checked": 1, "n_in_range": 0, "n_off_tape": 0, "n_no_tape": 1, "n_set_aside": 0, "usd_set_aside": 0.0}


def test_the_engine_publishes_the_cleaned_list_and_says_what_it_set_aside():
    out = run()
    h, s = out["hygiene"], out["stats"]
    assert out["version"] == "2.2.0" and h["applied"] is True and h["n_feed_purchase_rows"] == 175 and h["n_filing_lines"] == 52
    assert [a["ticker"] for a in sorted(h["set_aside"], key=lambda a: a["ticker"])] == ["AASP", "LONA"] and h["n_set_aside"] == 2
    assert h["merged_filings"] == [{"cik": "0001567892", "kept": "MNK", "also_filed_as": ["MNKKQ", "MNKPF"], "n_transactions": 4}]
    assert s["n_feed_purchase_rows"] == 175 and s["n_buy_transactions"] == 50, "filing lines counted once (it counted feed rows)"
    buys = {b["ticker"]: b for b in out["all_ticker_buys"]}
    assert not ({"LONA", "MNKKQ", "MNKPF"} & set(buys)), sorted(buys)
    assert "AASP" not in buys, "its remaining line is older than the 30-day window"
    assert buys["MNK"]["also_filed_as"] == ["MNKKQ", "MNKPF"] and buys["MNK"]["cik"] == "0001567892" and buys["MNK"]["n_transactions"] == 4
    near(buys["MNK"]["total_value"], 40049485.0)
    assert buys["BH"]["n_off_tape"] == 2 and buys["BH"]["n_transactions"] == 3 and buys["BH"]["company"] == "Biglari Holdings Inc." and buys["UPBD"]["company"] == "Upbound Group, Inc." and buys["UPBD"]["n_off_tape"] == 0
    assert max(b["total_value"] for b in buys.values()) < 2e8, "nothing in the list is larger than the largest real purchase"
    clusters = {c["ticker"]: c for c in out["clusters"]}
    assert not ({"LONA", "AASP", "MNKKQ", "MNKPF"} & set(clusters))
    up = clusters["UPBD"]                                    # the live feed's top cluster that day, number for number
    assert (up["n_insiders"], up["n_transactions"], up["total_value"], up["avg_price"], up["score"], up["signal_type"]) == (8, 11, 1840818.17, 22.98, 91.8, "smart_money_dual")
    assert up["company"] == "Upbound Group, Inc." and up["also_filed_as"] == [] and up["n_off_tape"] == 0
    assert clusters["MNK"]["also_filed_as"] == ["MNKKQ", "MNKPF"] and clusters["MNK"]["n_transactions"] == 4
    bh = clusters["BH"]
    assert bh["n_off_tape"] == 2 and sorted((t["price_check"], t["security"]) for t in bh["transactions"]) == [("in_range", "Class B common stock"), ("off_tape", "Class A common stock"), ("off_tape", "Class A common stock")]
    assert all("price_check" in t for c in out["clusters"] for t in c["transactions"])
    assert out["schema_version"] == "2.1" and out["method"] == "insider_cluster_scanner_v2" and "tape" in h and "rule" in h


def test_hygiene_never_blocks_the_engine_and_needs_no_book():
    real = hygiene.screen
    hygiene.screen = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    try:
        out = run()
    finally:
        hygiene.screen = real
    assert out["hygiene"] == {"applied": False, "n_feed_purchase_rows": 175} and any(b["ticker"] == "LONA" for b in out["all_ticker_buys"]), "raw rows, as before"
    STATE["book"] = False
    try:
        out = run()
    finally:
        STATE["book"] = True
    h = out["hygiene"]
    assert h["applied"] and h["n_set_aside"] == 0 and h["tickers_on_the_tape"] == 0 and h["n_no_tape"] == h["n_filing_lines"], "no book: nothing can be judged, nothing is set aside"
    assert h["merged_filings"][0]["kept"] == "MNK" and not ({"MNKKQ", "MNKPF"} & {b["ticker"] for b in out["all_ticker_buys"]}), "the multi-ticker merge needs no tape"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print("ok  %s" % t.__name__)
    print("%d tests passed" % len(tests))
