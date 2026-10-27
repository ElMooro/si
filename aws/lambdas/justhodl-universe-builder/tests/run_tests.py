"""universe-builder v4.1.0 - offline: a provider outage can no longer publish an empty universe.

No network, no AWS: the screener is replaced per test, S3 is an in-memory fake holding the FMP bank's profile-bulk
parts (gzip CSV, the real column names), the factor book's universe and the previous publication."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import csv
import gzip
import io
import json
import os
import sys
import types
import unittest
import urllib.error

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-universe-builder" / "source"))
sys.path.insert(1, str(ROOT / "aws" / "shared"))
os.environ.setdefault("FMP_KEY", "test-placeholder")
sys.modules.setdefault("fmp_bankfirst", types.SimpleNamespace(install=lambda: None))   # no urllib shim in tests
import lambda_function as ub  # noqa: E402

NOW = datetime(2026, 10, 27, 18, 0, tzinfo=timezone.utc)
HEADER = ["symbol", "price", "marketCap", "beta", "companyName", "currency", "cik", "exchangeFullName", "exchange", "industry",
          "description", "sector", "country", "isEtf", "isActivelyTrading", "isAdr", "isFund"]
V4_ROW_KEYS = {"symbol", "name", "sector", "industry", "exchange", "country", "is_adr", "market_cap", "price", "cap_bucket", "source"}


def prow(sym, mc, exchange="NASDAQ", industry="Software - Application", name=None, active="true", etf="false", fund="false", country="US"):
    return {"symbol": sym, "price": "12.5", "marketCap": str(mc), "beta": "1.1", "companyName": name or (sym + " Inc."), "currency": "USD",
            "cik": "0000000001", "exchangeFullName": exchange, "exchange": exchange, "industry": industry,
            "description": "a long, quoted, multi-sentence description, with commas", "sector": "Technology", "country": country,
            "isEtf": etf, "isActivelyTrading": active, "isAdr": "false", "isFund": fund}


def gz_csv(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=HEADER)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return gzip.compress(buf.getvalue().encode())


# one real common stock per span, plus everything the screener request / builder / book must drop
COMMONS = [("NANO%d" % i, 10e6 + i) for i in range(4)] + [("MICR%d" % i, 100e6 + i) for i in range(4)] + \
          [("SMAL%d" % i, 800e6 + i) for i in range(4)] + [("MIDC%d" % i, 5e9 + i) for i in range(4)] + \
          [("LARG%d" % i, 50e9 + i) for i in range(4)] + [("MEGA%d" % i, 900e9 + i) for i in range(2)]
JUNK = [prow("WARRW", 800e6),                                   # the book types it as a warrant
        prow("ETFX", 5e9, etf="true"),                          # an ETF (isEtf)
        prow("FNDX", 5e9, fund="true"),                         # a fund (isFund)
        prow("OTCX", 5e9, exchange="OTC"),                      # not NYSE/NASDAQ/AMEX
        prow("GONE", 5e9, active="false"),                      # not actively trading
        prow("PREF-PA", 5e9),                                   # preferred suffix - the builder's own filter
        prow("AMGT", 5e9, industry="Asset Management"),         # fund industry - the builder's own filter
        prow("TINY", 500e3),                                    # below the $1M floor
        prow("LSEX", 5e9, exchange="LSE")]                      # foreign exchange


class FakeS3:
    def __init__(self, objects):
        self.objects = dict(objects)            # key -> (bytes, last_modified)
        self.gets, self.lists, self.puts = [], [], {}

    def list_objects_v2(self, Bucket, Prefix, **kw):
        self.lists.append(Prefix)
        return {"Contents": [{"Key": k, "LastModified": lm, "Size": len(b)} for k, (b, lm) in sorted(self.objects.items()) if k.startswith(Prefix)],
                "IsTruncated": False}

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.objects:
            raise KeyError("NoSuchKey " + Key)
        return {"Body": io.BytesIO(self.objects[Key][0])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = json.loads(Body)


def bank_objects(age_days=1.0, rows=None):
    rows = rows if rows is not None else [prow(s, mc) for s, mc in COMMONS] + JUNK
    lm = NOW - timedelta(days=age_days)
    half = len(rows) // 2
    return {ub.BANK_PREFIX + "part-0/latest.csv.gz": (gz_csv(rows[:half]), lm),
            ub.BANK_PREFIX + "part-1/latest.csv.gz": (gz_csv(rows[half:]), lm),
            ub.BANK_PREFIX + "daily/2026-10-25.csv.gz": (b"not read", lm)}


def book_objects(extra=None):
    syms = {"WARRW": {"is_common": False, "security_type": "WARRANT", "name": "Warr Holdings Warrants"},
            "NANO0": {"is_common": True, "security_type": "CS", "name": "NANO0 Inc."}}
    syms.update(extra or {})
    doc = {"generated_at": "2026-10-27T02:55:00+00:00", "symbols": syms}
    return {ub.BOOK_UNIVERSE_KEY: (gzip.compress(json.dumps(doc).encode()), NOW)}


def previous_objects(total, by_bucket, stocks=None):
    doc = {"generated_at": "2026-10-26T18:06:00+00:00", "stats": {"total_stocks": total, "by_cap_bucket": by_bucket},
           "stocks": stocks or []}
    return {ub.S3_KEY: (json.dumps(doc).encode(), NOW)}


def screener(rows_for=None, fail=lambda lo, hi: False):
    """A stand-in for _screener_call: FMP-shaped rows inside [lo, hi) from `rows_for`, or HTTP 429 when fail(lo, hi)."""
    def call(lo, hi):
        if fail(lo, hi):
            raise urllib.error.HTTPError("https://financialmodelingprep.com/stable/company-screener", 429, "Too Many Requests", {}, None)
        out = []
        for sym, mc in (rows_for or []):
            if lo <= mc < hi:
                out.append({"symbol": sym, "companyName": sym + " Corp", "marketCap": mc * 1.01, "price": 20.0, "exchange": "NYSE",
                            "exchangeShortName": "NYSE", "industry": "Banks - Regional", "sector": "Financial Services", "country": "US"})
        return out
    return call


class Base(unittest.TestCase):
    def setUp(self):
        self.saved = (ub.S3, ub._screener_call, ub.fetch_url, ub.MIN_PUBLISH, ub.datetime)
        ub.fetch_url = self._no_network                    # curated seeds: no live quote/profile in tests

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return NOW
        ub.datetime = Clock

    def tearDown(self):
        ub.S3, ub._screener_call, ub.fetch_url, ub.MIN_PUBLISH, ub.datetime = self.saved

    @staticmethod
    def _no_network(url, timeout=20):
        raise urllib.error.URLError("offline test")

    def run_with(self, objects, call, event=None, min_publish=5):
        ub.S3 = FakeS3(objects)
        ub._screener_call = call
        ub.MIN_PUBLISH = min_publish
        return ub.lambda_handler(event or {}, None)


class TestUniverse(Base):
    def test_every_range_429_fills_from_the_bank_with_labels_and_publishes(self):
        res = self.run_with({**bank_objects(), **book_objects()}, screener(fail=lambda lo, hi: True))
        doc = ub.S3.puts[ub.S3_KEY]
        syms = {s["symbol"] for s in doc["stocks"]}
        self.assertEqual(syms, {s for s, _ in COMMONS})                      # every junk row dropped, every common kept
        self.assertTrue(all(s["source"] == "bank_profile_bulk" and s["as_of"].startswith("2026-10-26") for s in doc["stocks"]))
        self.assertEqual(doc["stats"]["by_cap_bucket"], {"nano": 4, "micro": 4, "small": 4, "mid": 4, "large": 4, "mega": 2, "unknown": 0})
        self.assertEqual(doc["sources"]["screener"]["ranges_failed"], 6)
        fill = doc["sources"]["bank_profile_bulk"]
        # the listing rule drops WARRW (Polygon WARRANT) and PREF-PA (untyped, preferred ticker) inside the fill; AMGT reaches
        # the combined set and the builder's own filter drops it (fund industry)
        self.assertEqual((fill["status"], fill["membership"], fill["added_before_filter"], fill["dropped_not_equity"]),
                         ("read", "listing_rule_only", 23, 2))
        self.assertEqual(doc["sources"]["listing_rule"]["status"], "read")
        self.assertNotIn(ub.BANK_PREFIX + "daily/2026-10-25.csv.gz", ub.S3.gets)  # only the latest parts are read
        self.assertEqual([s["market_cap"] for s in doc["stocks"]], sorted((s["market_cap"] for s in doc["stocks"]), reverse=True))
        self.assertEqual(json.loads(res["body"])["bank_fill"], "read")

    def test_a_failed_span_is_filled_only_there_and_the_screener_wins_on_overlap(self):
        # SMAL0: the bank has it at 800M (the failed small span), the screener answers it at 2.05B in the mid span -> live wins
        live = [("SMAL0", 2.03e9), ("LIVE1", 5e9), ("LIVE2", 60e9)]
        small = next((lo, hi) for n, lo, hi in ub.CAP_BUCKETS if n == "small")
        res = self.run_with({**bank_objects(), **book_objects()}, screener(live, fail=lambda lo, hi: (lo, hi) == small))
        doc = ub.S3.puts[ub.S3_KEY]
        by = {s["symbol"]: s for s in doc["stocks"]}
        self.assertEqual(doc["sources"]["screener"]["failed_ranges"], [{"bucket": "small", "low": small[0], "high": small[1],
                                                                         "error": "HTTP Error 429: Too Many Requests"}])
        self.assertEqual({s for s, r in by.items() if r["source"] == "bank_profile_bulk"}, {"SMAL1", "SMAL2", "SMAL3"})
        self.assertEqual((by["SMAL0"]["source"], by["SMAL0"]["cap_bucket"]), ("screener", "mid"))
        self.assertNotIn("as_of", by["SMAL0"])
        self.assertNotIn("MIDC0", by)                                         # the mid span answered live: no bank rows there
        self.assertEqual(doc["stats"]["by_source"], {"screener": 3, "bank_profile_bulk": 3})
        self.assertEqual(json.loads(res["body"])["ranges_failed"], 1)

    def test_a_failed_sub_range_after_bisection_is_the_only_span_filled(self):
        mid = next((lo, hi) for n, lo, hi in ub.CAP_BUCKETS if n == "mid")
        split = int((mid[0] * mid[1]) ** 0.5)                                # the builder's geometric midpoint
        saturated = [("SAT%04d" % i, mid[0] + 1 + i) for i in range(1000)]    # 1000 rows -> bisect
        bank = [prow("MIDHI%d" % i, split + 10 + i) for i in range(3)] + [prow("MIDLO", mid[0] + 5)]

        def call(lo, hi):
            if (lo, hi) == (split, mid[1]):
                raise urllib.error.HTTPError("u", 429, "Too Many Requests", {}, None)
            if (lo, hi) == mid:
                return [{"symbol": s, "marketCap": mc, "companyName": s, "exchange": "NYSE", "industry": "Banks", "sector": "F"} for s, mc in saturated]
            return screener(saturated)(lo, hi)
        self.run_with({**bank_objects(rows=bank), **book_objects()}, call)
        doc = ub.S3.puts[ub.S3_KEY]
        self.assertEqual([(f["low"], f["high"]) for f in doc["sources"]["screener"]["failed_ranges"]], [(split, mid[1])])
        banked = {s["symbol"] for s in doc["stocks"] if s["source"] == "bank_profile_bulk"}
        self.assertEqual(banked, {"MIDHI0", "MIDHI1", "MIDHI2"})              # MIDLO's span answered live

    def test_a_non_list_answer_is_a_failed_range_not_an_empty_one(self):
        ub.fetch_url = lambda url, timeout=20: {"Error Message": "Limit Reach . Please upgrade your plan"}
        failed = []
        self.assertEqual(ub.fetch_screener_bucket("mega", 200e9, 100e12, 0, failed), [])
        self.assertEqual(len(failed), 1)
        self.assertIn("Error Message", failed[0]["error"])

    def test_stale_bank_and_failed_screener_keep_the_previous_publication_and_fail_loudly(self):
        prev = previous_objects(5230, {"nano": 1015, "micro": 907, "small": 1296, "mid": 1090, "large": 850, "mega": 72})
        with self.assertRaises(RuntimeError) as cm:
            self.run_with({**bank_objects(age_days=15), **book_objects(), **prev}, screener(fail=lambda lo, hi: True))
        self.assertEqual(ub.S3.puts, {})
        self.assertIn("previous publication (generated_at 2026-10-26T18:06:00+00:00) stays live", str(cm.exception))
        self.assertTrue(all(not k.startswith(ub.BANK_PREFIX) for k in ub.S3.gets))   # a stale bank is never decoded

    def test_an_empty_universe_is_never_published_even_with_no_previous(self):
        with self.assertRaises(RuntimeError) as cm:
            self.run_with({}, screener(fail=lambda lo, hi: True), min_publish=1000)
        self.assertEqual(ub.S3.puts, {})
        self.assertIn("only 0 stocks", str(cm.exception))

    def test_a_silently_collapsed_bucket_is_refused(self):
        prev = previous_objects(30, {"small": 60, "mid": 4})
        live = [("SMALLONE", 800e6)] + [("M%d" % i, 5e9 + i) for i in range(30)]   # every range answers; small has 1 row
        with self.assertRaises(RuntimeError) as cm:
            self.run_with({**prev}, screener(live))
        self.assertIn("bucket small 1 < 50% of the previous 60", str(cm.exception))
        self.assertEqual(ub.S3.puts, {})

    def test_a_healthy_run_reads_no_bank_and_keeps_the_v4_row_shape(self):
        live = [("H%d" % i, mc) for i, mc in enumerate([10e6, 100e6, 800e6, 5e9, 50e9, 900e9])]
        self.run_with({**bank_objects(), **book_objects()}, screener(live))
        doc = ub.S3.puts[ub.S3_KEY]
        self.assertEqual(len(doc["stocks"]), 6)
        self.assertTrue(all(set(s) == V4_ROW_KEYS for s in doc["stocks"]))
        self.assertEqual(doc["sources"]["bank_profile_bulk"], {"status": "not_needed", "added_before_filter": 0})
        self.assertTrue(doc["sources"]["listing_rule"]["applied"])
        self.assertFalse([k for k in ub.S3.gets if k.startswith(ub.BANK_PREFIX)])
        self.assertNotIn(ub.BANK_PREFIX, ub.S3.lists)
        for k in ("schema_version", "method", "generated_at", "duration_s", "stats", "cap_buckets", "stocks"):
            self.assertIn(k, doc)
        self.assertEqual((doc["schema_version"], doc["method"], doc["engine_version"]), (4, "universe_builder_v4_exhaustive_bisection", "4.1.0"))

    def test_dry_run_with_force_bank_writes_nothing_and_compares_with_the_live_publication(self):
        live_stocks = [{"symbol": s, "cap_bucket": ub.bucket_of(mc)} for s, mc in COMMONS[:20]] + [{"symbol": "DELISTED", "cap_bucket": "mid"}]
        prev = previous_objects(21, {"nano": 4}, live_stocks)
        calls = []
        res = self.run_with({**bank_objects(), **book_objects(), **prev}, lambda lo, hi: calls.append((lo, hi)) or [],
                            event={"dry_run": True, "force_bank": True})
        body = json.loads(res["body"])
        self.assertEqual(ub.S3.puts, {})
        self.assertEqual(calls, [])                                           # force_bank: the screener is not called
        self.assertTrue(body["dry_run"] and body["would_publish"])
        # membership = the previous publication's: MEGA0/MEGA1 are in the bank but were never listed -> not invented
        self.assertEqual((body["compare"]["both"], body["compare"]["only_previous"], body["compare"]["only_new"]), (20, 1, 0))
        self.assertEqual(body["compare"]["only_previous_sample"], ["DELISTED"])
        self.assertEqual(body["compare"]["same_bucket"], 20)
        self.assertEqual(body["by_source"], {"bank_profile_bulk": 20})
        fill = body["sources"]["bank_profile_bulk"]
        self.assertEqual((fill["membership"], fill["dropped_not_member"]), ("previous_publication", 5))   # MEGA0-1, WARRW, PREF-PA, AMGT


def ident_fixture():
    common = {"SO": "The Southern Company", "PRU": "Prudential Financial, Inc.", "CMCSA": "Comcast Corporation", "BC": "Brunswick Corporation",
              "VSEC": "VSE Corporation", "ASTL": "Algoma Steel Group Inc.", "T": "AT&T Inc.", "GOOG": "Alphabet Inc."}
    out = {k: {"security_type": "CS", "name": v} for k, v in common.items()}
    out.update({
        "TBB": {"security_type": "SP", "name": "AT&T Inc. 5.35% GLB NTS 66"},
        "SOMN": {"security_type": "SP", "name": "The Southern Company"},
        "PRH": {"security_type": "SP", "name": "Prudential Financial, Inc. 5.95"},
        "CCZ": {"security_type": "SP", "name": "Comcast Holdings Corp."},
        "TPC": {"security_type": "PFD", "name": "Tutor Perini Corporation", "is_common": False},
        "BCPC": {"security_type": "SP", "name": "Balchem Corp", "is_common": False},
        "GWW": {"security_type": "CS", "name": "W.W. Grainger, Inc.", "is_warrant_unit": True, "is_common": False},
        "ARM": {"security_type": "ADRC", "name": "Arm Holdings plc American Depositary Shares", "is_preferred": True, "is_common": False},
        "TMUSL": {"security_type": "CS", "name": "T-Mobile US, Inc. 6.25 % Notes -01.09.69"},
        "GOOGM": {"security_type": "CS", "name": "Alphabet Inc. Depository Shs Repr 1/20th Conv Pfd Registered Shs"},
        "ACMEW": {"security_type": "WARRANT", "name": "Acme Corp"},
        "MNR": {"security_type": "UNIT", "name": "Mach Natural Resources LP"},
        "CCXIU": {"security_type": "UNIT", "name": "Churchill Capital Corp XI Units"},
        "VSECU": {"security_type": "UNIT", "name": "VSE Corporation"},
        "ASTLW": {"name": "Algoma Steel Group Inc."},
        "BZH": {"name": "Beazer Homes USA, Inc."},
        "PFBC": {"security_type": "CS", "name": "Preferred Bank"},
    })
    return out


class TestListingRule(Base):
    def test_notes_preferreds_warrants_out_misflagged_and_mistyped_commons_in(self):
        rule = ub.ListingRule(ident_fixture())
        expect = {"TBB": "note_by_name", "SOMN": "type_sp_issuer_sibling", "PRH": "type_sp_name", "CCZ": "type_sp_issuer_sibling",
                  "TMUSL": "note_by_name", "GOOGM": "preferred_by_name", "ACMEW": "type_warrant", "CCXIU": "type_unit_name",
                  "VSECU": "type_unit_secondary_ticker", "ASTLW": "untyped_by_name_or_ticker",
                  "TPC": None, "BCPC": None, "GWW": None, "ARM": None, "MNR": None, "BZH": None, "PFBC": None, "SO": None}
        names = {k: v["name"] for k, v in ident_fixture().items()}
        got = {sym: rule.verdict(sym, names[sym])[1] for sym in expect}
        self.assertEqual(got, expect)

    def test_the_screener_path_drops_non_equity_listings_and_says_so(self):
        live = [("SO", 100e9), ("SOMN", 105e9), ("TBB", 140e9), ("GWW", 60e9), ("TPC", 4e9), ("ACMEW", 50e6)]
        extra = {k: v for k, v in ident_fixture().items() if k in {"SO", "SOMN", "TBB", "GWW", "TPC", "ACMEW"}}

        def call(lo, hi):
            names = {k: v["name"] for k, v in extra.items()}
            return [dict(r, companyName=names[r["symbol"]]) for r in screener(live)(lo, hi)]
        self.run_with({**book_objects(extra)}, call, min_publish=1)
        doc = ub.S3.puts[ub.S3_KEY]
        self.assertEqual(sorted(s["symbol"] for s in doc["stocks"]), ["GWW", "SO", "TPC"])
        lr = doc["sources"]["listing_rule"]
        self.assertEqual((lr["applied"], lr["dropped"], lr["by_reason"]),
                         (True, 3, {"note_by_name": 1, "type_sp_issuer_sibling": 1, "type_warrant": 1}))
        self.assertEqual(doc["stats"]["by_cap_bucket"]["mega"], 0)          # TBB / SOMN no longer pose as mega caps

    def test_a_fill_never_invents_members_the_previous_publication_did_not_list(self):
        prev_stocks = [{"symbol": s, "cap_bucket": ub.bucket_of(mc)} for s, mc in COMMONS if not s.startswith("SMAL")] + \
                      [{"symbol": "SMAL0", "cap_bucket": "small"}]
        prev = previous_objects(len(prev_stocks), {"small": 1}, prev_stocks)
        small = next((lo, hi) for n, lo, hi in ub.CAP_BUCKETS if n == "small")
        live = [(s, mc) for s, mc in COMMONS if not s.startswith("SMAL")]
        self.run_with({**bank_objects(), **book_objects(), **prev}, screener(live, fail=lambda lo, hi: (lo, hi) == small))
        doc = ub.S3.puts[ub.S3_KEY]
        banked = sorted(s["symbol"] for s in doc["stocks"] if s["source"] == "bank_profile_bulk")
        self.assertEqual(banked, ["SMAL0"])                                   # SMAL1-3 are in the bank but were never members
        fill = doc["sources"]["bank_profile_bulk"]
        self.assertEqual((fill["membership"], fill["dropped_not_member"]), ("previous_publication", 4))   # SMAL1-3 + WARRW

    def test_without_the_book_the_v4_filter_stands_and_the_document_says_so(self):
        live = [("SO", 100e9), ("TBB", 140e9), ("GWW", 60e9)]
        self.run_with({}, screener(live), min_publish=1)
        doc = ub.S3.puts[ub.S3_KEY]
        self.assertEqual(sorted(s["symbol"] for s in doc["stocks"]), ["GWW", "SO", "TBB"])
        self.assertEqual((doc["sources"]["listing_rule"]["status"], doc["sources"]["listing_rule"]["applied"]), ("unavailable", False))


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
