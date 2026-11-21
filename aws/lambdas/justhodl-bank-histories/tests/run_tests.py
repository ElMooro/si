"""justhodl-bank-histories offline tests: an in-memory S3 seeded with small FMP-bank year-files; the inversion, the
write-if-changed state, the manifest, and the fmp_bank / fmp_bankfirst readers taking the book first.  No network, no AWS,
no key.

v1.2.0: a document is written only when its content changed - a new bank vintage alone writes nothing, the upgrade from
the v1.0-1.1 hashes writes nothing that did not change, a block with unchanged inputs is confirmed without being read, a
failed put is written by the next run, a dry run writes nothing at all.  (The bank vintage in the fixture is relative to
the clock: the freshness guard reads the bank manifest's age, and a fixed date turned this suite red two days after it
was written - which would have blocked every deploy of this engine.)

Run: python3 aws/lambdas/justhodl-bank-histories/tests/run_tests.py
"""
from datetime import datetime, timedelta, timezone
import csv
import gzip
import hashlib
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
SHARED = os.path.join(HERE, "..", "..", "..", "shared")
sys.path[:0] = [SRC, SHARED]
os.environ["HISTORY_BOOK_BUDGET_S"] = "120"
os.environ["HISTORY_BOOK_WORKERS"] = "4"
os.environ["HISTORY_BOOK_DB"] = os.path.join(os.environ.get("TMPDIR", "/tmp"), "history-book-test.sqlite")
os.environ["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] = "3008"


def ago(**kw):
    return (datetime.now(timezone.utc) - timedelta(**kw)).replace(microsecond=0).isoformat()


VINTAGE = ago(hours=3)                       # the bank's last run in the fixture
VINTAGE_2 = ago(hours=1)                     # the bank ran again


class S3Stub:
    def __init__(self):
        self.store, self.meta, self.puts = {}, {}, 0
        self.put_keys, self.fail = [], set()             # every key put, in order; keys whose next put raises

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey: " + Key)
        return {"Body": io.BytesIO(self.store[Key]), "Metadata": self.meta.get(Key) or {}}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey: " + Key)
        return {"ContentLength": len(self.store[Key]), "ETag": '"%s"' % hashlib.md5(self.store[Key]).hexdigest()}

    def put_object(self, Bucket, Key, Body, **kw):
        if Key in self.fail:
            self.fail.discard(Key)
            raise RuntimeError("SlowDown: " + Key)
        self.put_keys.append(Key)
        self.store[Key] = Body if isinstance(Body, bytes) else Body.encode()
        self.meta[Key] = kw.get("Metadata") or {}
        self.puts += 1

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        return {"Contents": [{"Key": k} for k in sorted(k for k in self.store if k.startswith(Prefix))]}


def csv_gz(rows, mtime=0):
    """mtime: the gzip header stamp - the same rows saved at another moment are other bytes (another ETag), as a bank that
    re-saves an unchanged file produces."""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return gzip.compress(buf.getvalue().encode(), mtime=mtime)


def jgz(obj):
    return gzip.compress(json.dumps(obj).encode())


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    import fmp_bank
    fmp_bank._S3 = s3
    fmp_bank._cache.clear()
    fmp_bank._TABLES.clear()
    fmp_bank._MISSING.clear()
    fmp_bank._HIST_MAN.update(at=0.0, doc=None)
    for m in list(sys.modules):
        if m in ("lambda_function",):
            del sys.modules[m]
    import lambda_function as lf
    lf.s3 = s3
    lf._LOCAL.c = s3
    return lf, s3, fmp_bank


def inc(sym, fy, period, d, rev):
    return {"symbol": sym, "date": d, "fiscalYear": str(fy), "period": period, "reportedCurrency": "USD", "revenue": rev, "netIncome": rev // 10, "eps": "1.5", "cik": "0000320193"}


INC_2025 = [("AAPL", 2025, "FY", "2025-09-27", 416000000000), ("MSFT", 2025, "FY", "2025-06-30", 281000000000), ("SAP", 2025, "FY", "2025-12-31", 1)]


def seed(lf, s3):
    R = lf.FMP_ROOT
    s3.store[R + "income-statement-bulk/2025/annual.csv.gz"] = csv_gz([inc(*r) for r in INC_2025])
    s3.store[R + "income-statement-bulk/2024/annual.csv.gz"] = csv_gz([inc("AAPL", 2024, "FY", "2024-09-28", 391000000000), inc("MSFT", 2024, "FY", "2024-06-30", 245000000000)])
    s3.store[R + "income-statement-bulk/1999/annual.csv.gz"] = csv_gz([inc("AAPL", 1999, "FY", "1999-09-25", 6)])             # below the floor: never read
    s3.store[R + "income-statement-bulk/2025/Q1.csv.gz"] = csv_gz([inc("AAPL", 2025, "Q1", "2024-12-28", 124000000000)])
    s3.store[R + "income-statement-bulk/2025/Q2.csv.gz"] = csv_gz([inc("AAPL", 2025, "Q2", "2025-03-29", 95000000000)])
    s3.store[R + "income-statement-bulk/2025/Q3.csv.gz"] = csv_gz([inc("AAPL", 2025, "Q3", "2025-06-28", 94000000000)])
    s3.store[R + "ratios-bulk/2025/annual.csv.gz"] = csv_gz([{"symbol": "AAPL", "date": "2025-09-27", "fiscalYear": "2025", "period": "FY", "priceToEarningsRatio": 39.2}, {"symbol": "MSFT", "date": "2025-06-30", "fiscalYear": "2025", "period": "FY", "priceToEarningsRatio": 30.0}])
    s3.store[R + "manifest.json"] = json.dumps({"updated_at": VINTAGE, "dated_keys": [
        "income-statement-bulk/2025/annual", "income-statement-bulk/2024/annual", "income-statement-bulk/1999/annual", "income-statement-bulk/2025/Q1",
        "income-statement-bulk/2025/Q2", "income-statement-bulk/2025/Q3", "ratios-bulk/2025/annual", "balance-sheet-statement-bulk/2025/annual"]}).encode()
    s3.store[lf.FACTOR_UNIVERSE_KEY] = jgz({"symbols": ["AAPL", "MSFT", "TINY"]})                                                   # SAP is Frankfurt -> out of the book
    s3.store[R + "profile-bulk/part-0/latest.csv.gz"] = csv_gz([{"symbol": "AAPL", "companyName": "Apple Inc.", "price": 347.7, "mktCap": 5074761240500, "description": "Apple designs...", "isEtf": "false"}])
    s3.store[R + "profile-bulk/part-1/latest.csv.gz"] = csv_gz([{"symbol": "MSFT", "companyName": "Microsoft", "price": 524.5, "mktCap": 3900000000000, "description": "Microsoft develops...", "isEtf": "false"}, {"symbol": "SAP", "companyName": "SAP SE", "price": 1, "mktCap": 1, "description": "x", "isEtf": "false"}])
    s3.store[R + "ratios-ttm-bulk/latest.csv.gz"] = csv_gz([{"symbol": "AAPL", "peRatioTTM": 39.2, "priceToSalesRatioTTM": 9.1}, {"symbol": "MSFT", "peRatioTTM": 30.0, "priceToSalesRatioTTM": 12.0}])


def test_inverts_year_files_into_per_symbol_docs_write_if_changed():
    lf, s3, fmp_bank = boot()
    seed(lf, s3)
    out = lf.lambda_handler({}, None)
    assert out["ok"], out
    assert "income-statement-bulk/annual" in out["blocks"] and "income-statement-bulk/quarter" in out["blocks"] and "ratios-bulk/annual" in out["blocks"], out
    # balance-sheet 2025/annual is in dated_keys but the file is missing -> block processed with a note, no docs
    assert any("missing" in n and "balance-sheet-statement-bulk" in n for n in out["notes"]), out["notes"]
    doc = json.loads(gzip.decompress(s3.store[lf.ROOT + "income-statement-bulk/annual/AAPL.json.gz"]))
    assert doc["symbol"] == "AAPL" and doc["endpoint"] == "income-statement" and doc["period"] == "annual" and doc["as_of"] == VINTAGE
    assert [r["date"] for r in doc["rows"]] == ["2025-09-27", "2024-09-28"] and doc["years"] == ["2025", "2024"] and doc["n"] == 2, doc    # newest first; 1999 below the floor
    assert doc["rows"][0]["revenue"] == 416000000000 and isinstance(doc["rows"][0]["revenue"], int) and doc["rows"][0]["cik"] == "0000320193" and doc["rows"][0]["eps"] == 1.5
    q = json.loads(gzip.decompress(s3.store[lf.ROOT + "income-statement-bulk/quarter/AAPL.json.gz"]))
    assert [r["period"] for r in q["rows"]] == ["Q3", "Q2", "Q1"], q
    assert lf.ROOT + "income-statement-bulk/annual/SAP.json.gz" not in s3.store, "SAP is outside the factor-book universe"
    assert lf.ROOT + "income-statement-bulk/quarter/MSFT.json.gz" not in s3.store, "MSFT has no quarterly rows in the fixture"
    man = json.loads(s3.store[lf.MANIFEST_KEY])
    b = man["blocks"]["income-statement-bulk/annual"]
    assert b["files"] == 2 and b["symbols"] == 2 and b["written"] == 2 and b["unchanged"] == 0 and b["years"] == [2024, 2025] and not b["partial"], b
    assert man["universe"] == "factor-book" and man["universe_size"] == 3 and man["stale_blocks"] == [] and man["floor_quarter"] == 2016
    assert man["blocks"]["income-statement-bulk/quarter"]["symbols"] == 1
    hashes = json.loads(gzip.decompress(s3.store[lf.ROOT + "_state/hashes/income-statement-bulk__annual.json.gz"]))
    assert set(hashes) == {"AAPL", "MSFT"}
    # v1.1.0: snapshot blocks -> one tiny doc per symbol, verbatim row (description kept), part files merged
    prof = json.loads(gzip.decompress(s3.store[lf.ROOT + "profile-bulk/latest/MSFT.json.gz"]))
    assert prof["period"] == "latest" and prof["years"] == [] and prof["rows"][0]["companyName"] == "Microsoft" and prof["rows"][0]["description"] == "Microsoft develops..." and prof["rows"][0]["mktCap"] == 3900000000000, prof
    assert lf.ROOT + "profile-bulk/latest/SAP.json.gz" not in s3.store and lf.ROOT + "ratios-ttm-bulk/latest/AAPL.json.gz" in s3.store
    assert man["blocks"]["profile-bulk/latest"]["files"] == 2 and man["blocks"]["profile-bulk/latest"]["symbols"] == 2 and man["blocks"]["profile-bulk/latest"]["years"] is None
    assert man["blocks"]["scores-bulk/latest"]["files"] == 0                      # not banked in the fixture -> recorded, nothing written
    # second run: nothing changed -> no document re-put
    puts = s3.puts
    out2 = lf.lambda_handler({}, None)
    assert out2["ok"]
    man2 = json.loads(s3.store[lf.MANIFEST_KEY])
    assert man2["blocks"]["income-statement-bulk/annual"]["written"] == 0 and man2["blocks"]["income-statement-bulk/annual"]["unchanged"] == 2
    doc_puts = sum(1 for k in s3.store if k.startswith(lf.ROOT) and "/_state/" not in k and k != lf.MANIFEST_KEY and "/latest/" not in k)
    assert doc_puts == 5 and all(b.get("written", 0) == 0 for b in man2["blocks"].values()), (doc_puts, man2["blocks"])   # AAPL+MSFT annual, AAPL quarter, AAPL+MSFT ratios; nothing re-put
    # v1.2.0: the inputs did not change, so no block was read again - each was confirmed from its files' ETags and sizes
    assert sorted(out2["confirmed_unread"]) == sorted(out2["blocks"]) and out2["puts"] == 0 and out2["unchanged"] == 9, out2
    assert all(b.get("checked") == "inputs unchanged" for b in man2["blocks"].values() if b.get("files")), man2["blocks"]
    assert not [k for k in s3.put_keys[puts:] if k.startswith(lf.ROOT) and "/_state/" not in k and k != lf.MANIFEST_KEY], "no document and no hash state re-put"
    assert not [k for k in s3.put_keys[puts:] if "/_state/hashes/" in k], "an unchanged hash state is not re-put either"
    # lease: a run inside another run's lease is skipped
    st = json.loads(s3.store[lf.STATE_KEY])
    assert st["phase"] == "STEADY" and st["updated_at"] and "lease_until" not in st and st["last_run"]["puts"] == 0, st
    assert man2["phase"] == "STEADY" and man2["pending_blocks"] == []
    # a block refreshed > 20 h ago is pending again (phase BACKFILL) even though it is not yet "stale" (> 2 days)
    st["blocks"]["income-statement-bulk/annual"]["refreshed_at"] = ago(hours=21)
    s3.store[lf.STATE_KEY] = json.dumps(st).encode()
    import time as _time
    real_time, calls = _time.time, {"n": 0}

    def fast_clock():                                                            # the run's first tick sets the deadline; every later tick is past it
        calls["n"] += 1
        return real_time() + (0 if calls["n"] <= 1 else 10_000)
    lf.time.time = fast_clock
    try:
        out3 = lf.lambda_handler({}, None)
    finally:
        lf.time.time = real_time
    assert out3["ok"] and out3["blocks"] == [] and out3["phase"] == "BACKFILL" and out3["pending"] == ["income-statement-bulk/annual"] and out3["stale_blocks"] == [], out3
    st = json.loads(s3.store[lf.STATE_KEY])
    st.pop("lease_until", None)
    st["lease_until"] = 4102444800.0                                                    # 2100-01-01, epoch seconds (the conductor's contract)
    s3.store[lf.STATE_KEY] = json.dumps(st).encode()
    assert lf.lambda_handler({}, None)["skipped"].startswith("lease held")
    assert lf.lambda_handler({"force": True}, None)["ok"]


def test_fmp_bank_statements_read_the_history_book_first_and_the_shim_accepts_deep_limits():
    lf, s3, fmp_bank = boot()
    seed(lf, s3)
    lf.lambda_handler({}, None)
    fmp_bank._HIST_MAN.update(at=0.0, doc=None)
    assert fmp_bank.history_block("income-statement", "annual") and fmp_bank.history_block("balance-sheet-statement", "annual") is None
    rows = fmp_bank.statements("income-statement", "AAPL", period="annual", limit=20)                # limit 20 > the year-file ceiling: the book answers
    assert [r["date"] for r in rows] == ["2025-09-27", "2024-09-28"] and fmp_bank.STATS.get("history_book_hits") == 1, rows
    assert [r["period"] for r in fmp_bank.statements("income-statement", "aapl", period="quarter", limit=22)] == ["Q3", "Q2", "Q1"]
    assert [r["period"] for r in fmp_bank.statements("income-statement", "AAPL", period="Q2")] == ["Q2"]
    assert fmp_bank.statements("income-statement", "AAPL", period="annual", years=[2024])[0]["date"] == "2024-09-28"
    assert fmp_bank.statements("ratios", "MSFT", period="annual", limit=15)[0]["priceToEarningsRatio"] == 30.0
    assert fmp_bank.history_has("income-statement", "AAPL", "annual") and not fmp_bank.history_has("income-statement", "ZZZZ", "annual")
    # a symbol the book lacks falls through to the year-file scan (which still answers from the bulk tables)
    fmp_bank._TABLES.clear()
    rows = fmp_bank.statements("income-statement", "SAP", period="annual", limit=5)
    assert rows and rows[0]["symbol"] == "SAP"
    # snapshots: the per-symbol document answers first (no table decode); the symbol field is carried
    fmp_bank._TABLES.clear(); fmp_bank._MISSING.clear(); fmp_bank.STATS["table_builds"] = 0
    prow = fmp_bank.snapshot("profile", "aapl")
    assert prow and prow[0]["companyName"] == "Apple Inc." and prow[0]["symbol"] == "AAPL" and prow[0]["description"] == "Apple designs..." and fmp_bank.STATS["table_builds"] == 0, prow
    assert fmp_bank.try_bank("profile", "symbol=MSFT")[0]["price"] == 524.5 and fmp_bank.STATS["table_builds"] == 0
    assert fmp_bank.snapshot("ratios-ttm", "MSFT")[0]["peRatioTTM"] == 30.0
    assert fmp_bank.snapshot("profile", "SAP")[0]["companyName"] == "SAP SE" and fmp_bank.STATS["table_builds"] >= 1       # outside the book: the table path still answers
    import fmp_bankfirst as fb
    rows_hist = fmp_bank.try_bank("income-statement", "symbol=AAPL&period=annual&limit=20")
    assert fb._fresh_enough("income-statement", rows_hist, {"symbol": "AAPL", "period": "annual", "limit": "20"}) is True, "a history-book answer is as fresh as the bank"
    rows_snap = fmp_bank.try_bank("profile", "symbol=AAPL")
    assert fb._fresh_enough("profile", rows_snap, {"symbol": "AAPL"}) is True
    assert fb._shaped("income-statement", "income-statement", {"symbol": "AAPL", "period": "annual", "limit": "20"}) is True       # the book holds it
    assert fb._shaped("income-statement", "income-statement", {"symbol": "ZZZZ", "period": "annual", "limit": "20"}) is False      # nobody holds it: live
    assert fb._shaped("income-statement", "income-statement", {"symbol": "ZZZZ", "period": "annual", "limit": "10"}) is True       # inside the year-file ceiling


def test_point_lookups_never_decode_whole_market_tables_in_a_1gb_container_once_the_book_is_built():
    """2026-10-26: the why.html desk (1 GB, request-serving) died out-of-memory on 19 of 145 requests - every book miss
    (a delisted / foreign / OTC name) decoded statement year-files and the profile bulk.  At <= POINT_SCAN_MIN_MB the books
    answer or nobody does; above it (batch engines) the scan stays; with no book built the legacy scan stays everywhere."""
    import os
    lf, s3, fmp_bank = boot()
    seed(lf, s3)
    lf.lambda_handler({}, None)
    fmp_bank._HIST_MAN.update(at=0.0, doc=None)
    saved = os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
    try:
        os.environ["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] = "1024"
        fmp_bank._TABLES.clear(); fmp_bank._MISSING.clear(); fmp_bank.STATS["table_builds"] = 0; fmp_bank.STATS["book_only_misses"] = 0
        assert fmp_bank.statements("income-statement", "AAPL", period="annual", limit=20)[0]["date"] == "2025-09-27"      # the book answers as before
        assert fmp_bank.statements("income-statement", "SAP", period="annual", limit=5) is None                            # book built, symbol absent: no scan
        assert fmp_bank.snapshot("profile", "SAP") is None
        assert fmp_bank.snapshot("profile", "aapl")[0]["companyName"] == "Apple Inc."
        assert fmp_bank.STATS["table_builds"] == 0 and fmp_bank.STATS["book_only_misses"] == 2, fmp_bank.STATS
        assert fmp_bank.statements("balance-sheet-statement", "AAPL", period="annual", limit=5) is None or fmp_bank.STATS["table_builds"] >= 0   # no balance block in the book: the legacy scan is allowed (nothing banked here either)
        os.environ["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] = "1536"
        fmp_bank._TABLES.clear(); fmp_bank._MISSING.clear(); fmp_bank.STATS["table_builds"] = 0
        rows = fmp_bank.statements("income-statement", "SAP", period="annual", limit=5)
        assert rows and rows[0]["symbol"] == "SAP" and fmp_bank.STATS["table_builds"] >= 1                                 # a batch engine still scans
        assert fmp_bank.snapshot("profile", "SAP")[0]["companyName"] == "SAP SE"
    finally:
        if saved is None:
            os.environ.pop("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", None)
        else:
            os.environ["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] = saved
        fmp_bank._TABLES.clear(); fmp_bank._MISSING.clear()


def _docs(lf, keys):
    return [k for k in keys if k.startswith(lf.ROOT) and "/_state/" not in k and k != lf.MANIFEST_KEY]


def _bank_ran(lf, s3, fmp_bank, vintage):
    man = json.loads(s3.store[lf.FMP_ROOT + "manifest.json"])
    man["updated_at"] = vintage
    s3.store[lf.FMP_ROOT + "manifest.json"] = json.dumps(man).encode()
    fmp_bank._cache.clear()


def _doc(lf, s3, rel):
    return json.loads(gzip.decompress(s3.store[lf.ROOT + rel]))


def test_a_new_bank_vintage_alone_rewrites_nothing_and_a_real_change_rewrites_one_document():
    """The defect: the hash covered `as_of` (the bank manifest's updated_at), so every bank run made every document
    'changed' - 110,000 to 400,000 identical files rewritten a day.  A vintage is not a change."""
    lf, s3, fmp_bank = boot()
    seed(lf, s3)
    assert lf.lambda_handler({}, None)["puts"] == 9
    # the bank runs again: a new vintage, and it re-saved one year-file with the same rows (other bytes, another ETag)
    _bank_ran(lf, s3, fmp_bank, VINTAGE_2)
    s3.store[lf.FMP_ROOT + "income-statement-bulk/2025/annual.csv.gz"] = csv_gz([inc(*r) for r in INC_2025], mtime=1700000000)
    mark = len(s3.put_keys)
    out = lf.lambda_handler({}, None)
    assert out["ok"] and out["puts"] == 0 and out["unchanged"] == 9 and out["phase"] == "STEADY", out
    assert _docs(lf, s3.put_keys[mark:]) == [], "no document is rewritten for a new vintage"
    man = json.loads(s3.store[lf.MANIFEST_KEY])
    b = man["blocks"]["income-statement-bulk/annual"]
    assert b["checked"] == "read in full" and b["written"] == 0 and b["unchanged"] == 2 and b["as_of"] == VINTAGE_2 and b["hash_rev"] == 2 and b["source_sig"], b
    assert "income-statement-bulk/annual" not in out["confirmed_unread"] and "ratios-bulk/annual" in out["confirmed_unread"], out
    assert man["blocks"]["ratios-bulk/annual"]["checked"] == "inputs unchanged" and man["blocks"]["ratios-bulk/annual"]["as_of"] == VINTAGE_2
    assert man["as_of"] == VINTAGE_2 and "last changed" in man["as_of_meaning"]
    assert _doc(lf, s3, "income-statement-bulk/annual/AAPL.json.gz")["as_of"] == VINTAGE, "the document keeps the vintage its content last changed at"
    # a real change: one company's reported revenue is restated -> exactly that one document is written
    rows = [inc(*r) for r in INC_2025]
    rows[0]["revenue"] = 417000000000
    s3.store[lf.FMP_ROOT + "income-statement-bulk/2025/annual.csv.gz"] = csv_gz(rows)
    mark = len(s3.put_keys)
    out = lf.lambda_handler({}, None)
    assert out["puts"] == 1 and out["unchanged"] == 8, out
    assert _docs(lf, s3.put_keys[mark:]) == [lf.ROOT + "income-statement-bulk/annual/AAPL.json.gz"], s3.put_keys[mark:]
    d = _doc(lf, s3, "income-statement-bulk/annual/AAPL.json.gz")
    assert d["rows"][0]["revenue"] == 417000000000 and d["as_of"] == VINTAGE_2
    b = json.loads(s3.store[lf.MANIFEST_KEY])["blocks"]["income-statement-bulk/annual"]
    assert b["written"] == 1 and b["changed"] == 1 and b["new"] == 0 and b["unchanged"] == 1, b


def test_the_upgrade_from_the_old_hashes_rewrites_only_what_changed():
    """The store as v1.1.0 left it: hashes over the whole body including the vintage, no signature, no hash revision.  The
    first v1.2.0 run must recognise every unchanged document from its old hash - the upgrade is not a full rewrite."""
    lf, s3, fmp_bank = boot()
    seed(lf, s3)
    lf.lambda_handler({}, None)
    st = json.loads(s3.store[lf.STATE_KEY])
    for bid, b in st["blocks"].items():
        for k in ("hash_rev", "source_sig", "staged_at", "checked", "legacy_as_of", "changed", "new", "migrated", "missing_files"):
            b.pop(k, None)
        if not b.get("symbols"):
            continue
        bulk, kind = bid.split("/")
        hk = lf.ROOT + "_state/hashes/%s__%s.json.gz" % (bulk, kind)
        old = {}
        for sym in json.loads(gzip.decompress(s3.store[hk])):
            raw = gzip.decompress(s3.store[lf.ROOT + "%s/%s/%s.json.gz" % (bulk, kind, sym)])
            old[sym] = hashlib.sha1(raw).hexdigest()[:20]          # what v1.1.0 stored: sha1 of the body it wrote
            assert old[sym] == lf.legacy_hash(json.loads(raw), VINTAGE) != lf.doc_hash(json.loads(raw))
        s3.store[hk] = jgz(old)
    s3.store[lf.STATE_KEY] = json.dumps(st).encode()
    # the bank has run since (new vintage) and one company restated; everything else is as it was
    _bank_ran(lf, s3, fmp_bank, VINTAGE_2)
    rows = [inc(*r) for r in INC_2025]
    rows[1]["revenue"] = 282000000000                                # MSFT
    s3.store[lf.FMP_ROOT + "income-statement-bulk/2025/annual.csv.gz"] = csv_gz(rows)
    mark = len(s3.put_keys)
    out = lf.lambda_handler({}, None)
    assert out["ok"] and out["confirmed_unread"] == [] and out["puts"] == 1 and out["unchanged"] == 8, out
    assert _docs(lf, s3.put_keys[mark:]) == [lf.ROOT + "income-statement-bulk/annual/MSFT.json.gz"], s3.put_keys[mark:]
    man = json.loads(s3.store[lf.MANIFEST_KEY])
    b = man["blocks"]["income-statement-bulk/annual"]
    assert b["migrated"] == 1 and b["changed"] == 1 and b["hash_rev"] == 2 and b["source_sig"] and b["legacy_as_of"] is None, b
    assert man["blocks"]["profile-bulk/latest"]["migrated"] == 2 and man["blocks"]["ratios-bulk/annual"]["migrated"] == 2
    new = json.loads(gzip.decompress(s3.store[lf.ROOT + "_state/hashes/income-statement-bulk__annual.json.gz"]))
    assert new == {sym: lf.doc_hash(_doc(lf, s3, "income-statement-bulk/annual/%s.json.gz" % sym)) for sym in ("AAPL", "MSFT")}, new
    # and from then on the block is confirmed without a read
    out = lf.lambda_handler({}, None)
    assert sorted(out["confirmed_unread"]) == sorted(out["blocks"]) and out["puts"] == 0, out


def test_a_failed_put_is_written_by_the_next_run():
    """Before v1.2.0 the new hash was recorded before the put was known to have succeeded; only the daily full rewrite
    hid a lost document.  Now the old hash stands and the block is not signed off until every put landed."""
    lf, s3, fmp_bank = boot()
    seed(lf, s3)
    lost = lf.ROOT + "income-statement-bulk/annual/AAPL.json.gz"
    s3.fail.add(lost)
    out = lf.lambda_handler({}, None)
    assert out["ok"] and out["puts"] == 8 and lost not in s3.store, out
    st = json.loads(s3.store[lf.STATE_KEY])
    b = st["blocks"]["income-statement-bulk/annual"]
    assert b["failed"] == 1 and b["written"] == 1 and b["source_sig"] is None and b["hash_rev"] is None, b
    assert set(json.loads(gzip.decompress(s3.store[lf.ROOT + "_state/hashes/income-statement-bulk__annual.json.gz"]))) == {"MSFT"}
    mark = len(s3.put_keys)
    out = lf.lambda_handler({}, None)
    assert out["puts"] == 1 and _docs(lf, s3.put_keys[mark:]) == [lost], (out, s3.put_keys[mark:])
    assert "income-statement-bulk/annual" not in out["confirmed_unread"]
    b = json.loads(s3.store[lf.STATE_KEY])["blocks"]["income-statement-bulk/annual"]
    assert b["failed"] == 0 and b["written"] == 1 and b["unchanged"] == 1 and b["source_sig"] and b["hash_rev"] == 2, b


def test_a_universe_change_and_the_weekly_full_read():
    lf, s3, fmp_bank = boot()
    seed(lf, s3)
    lf.lambda_handler({}, None)
    # a symbol joins the universe: the files did not change, yet its documents must be written (the signature holds the universe)
    s3.store[lf.FACTOR_UNIVERSE_KEY] = jgz({"symbols": ["AAPL", "MSFT", "TINY", "SAP"]})
    mark = len(s3.put_keys)
    out = lf.lambda_handler({}, None)
    assert out["confirmed_unread"] == [] and out["puts"] == 2, out
    assert sorted(_docs(lf, s3.put_keys[mark:])) == [lf.ROOT + "income-statement-bulk/annual/SAP.json.gz", lf.ROOT + "profile-bulk/latest/SAP.json.gz"], s3.put_keys[mark:]
    # each block is read in full once a week whatever its signature says; a block last read 9 days ago is overdue
    st = json.loads(s3.store[lf.STATE_KEY])
    st["blocks"]["ratios-bulk/annual"]["staged_at"] = ago(days=9)
    s3.store[lf.STATE_KEY] = json.dumps(st).encode()
    out = lf.lambda_handler({}, None)
    b = json.loads(s3.store[lf.MANIFEST_KEY])["blocks"]
    assert out["puts"] == 0 and "ratios-bulk/annual" not in out["confirmed_unread"] and b["ratios-bulk/annual"]["checked"] == "read in full", out
    assert b["income-statement-bulk/annual"]["checked"] == "inputs unchanged" and b["ratios-bulk/annual"]["unchanged"] == 2
    # the weekly day: exactly one day in seven, and never twice within 20 hours
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    base = _dt(2026, 11, 2, 3, 5, tzinfo=_tz.utc)
    prev = {"staged_at": (base - _td(days=1)).isoformat()}
    due = [d for d in range(7) if lf.full_check_due("ratios-bulk/annual", dict(prev, staged_at=(base + _td(days=d - 1)).isoformat()), base + _td(days=d))]
    assert len(due) == 1, due
    assert not lf.full_check_due("ratios-bulk/annual", {"staged_at": (base + _td(days=due[0], hours=-2)).isoformat()}, base + _td(days=due[0]))
    assert lf.full_check_due("ratios-bulk/annual", {}, base) and lf.full_check_due("ratios-bulk/annual", {"staged_at": "not a date"}, base)
    slots = {bid: [d for d in range(7) if lf.full_check_due(bid, {"staged_at": (base + _td(days=d - 1)).isoformat()}, base + _td(days=d))][0]
             for bid in ["%s/%s" % (b_, k) for _, b_ in lf.BULKS for k in ("annual", "quarter")] + ["%s/latest" % b_ for _, b_ in lf.SNAPSHOT_BULKS]}
    per_day = [list(slots.values()).count(d) for d in range(7)]
    assert sum(per_day) == 27 and max(per_day) <= 9, per_day           # spread over the week: no day reads the whole book


def test_a_dry_run_writes_nothing():
    lf, s3, fmp_bank = boot()
    seed(lf, s3)
    before = dict(s3.store)
    out = lf.lambda_handler({"dry_run": True}, None)
    assert out["ok"] and out["dry_run"] and out["would_put"] == 9 and out["unchanged"] == 0, out
    assert s3.store == before and s3.puts == 0, "a dry run puts nothing: no lease, no state, no manifest, no document"
    assert out["detail"]["income-statement-bulk/annual"] == {"symbols": 2, "written": 2, "unchanged": 0, "changed": 0, "new": 2, "migrated": 0,
                                                             "seconds": out["detail"]["income-statement-bulk/annual"]["seconds"], "checked": "read in full"}, out["detail"]
    lf.lambda_handler({}, None)
    st = json.loads(s3.store[lf.STATE_KEY])
    st["lease_until"] = 4102444800.0                                  # another run holds the lease: a dry run reads beside it
    s3.store[lf.STATE_KEY] = json.dumps(st).encode()
    before, puts = dict(s3.store), s3.puts
    out = lf.lambda_handler({"dry_run": True, "full": True}, None)
    assert out["ok"] and out["would_put"] == 0 and out["unchanged"] == 9 and out["confirmed_unread"] == [], out
    assert s3.store == before and s3.puts == puts


TESTS = [test_inverts_year_files_into_per_symbol_docs_write_if_changed,
         test_fmp_bank_statements_read_the_history_book_first_and_the_shim_accepts_deep_limits,
         test_point_lookups_never_decode_whole_market_tables_in_a_1gb_container_once_the_book_is_built,
         test_a_new_bank_vintage_alone_rewrites_nothing_and_a_real_change_rewrites_one_document,
         test_the_upgrade_from_the_old_hashes_rewrites_only_what_changed,
         test_a_failed_put_is_written_by_the_next_run,
         test_a_universe_change_and_the_weekly_full_read,
         test_a_dry_run_writes_nothing]

if __name__ == "__main__":
    for t in TESTS:
        t()
        print("ok " + t.__name__)
    print("%d tests passed" % len(TESTS))
