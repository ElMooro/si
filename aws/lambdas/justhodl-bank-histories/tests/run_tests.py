"""justhodl-bank-histories offline tests: an in-memory S3 seeded with small FMP-bank year-files; the inversion, the
write-if-changed state, the manifest, and the fmp_bank / fmp_bankfirst readers taking the book first.  No network, no AWS,
no key.

Run: python3 aws/lambdas/justhodl-bank-histories/tests/run_tests.py
"""
import csv
import gzip
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


class S3Stub:
    def __init__(self):
        self.store, self.meta, self.puts = {}, {}, 0

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey: " + Key)
        return {"Body": io.BytesIO(self.store[Key]), "Metadata": self.meta.get(Key) or {}}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey: " + Key)
        return {"ContentLength": len(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body if isinstance(Body, bytes) else Body.encode()
        self.meta[Key] = kw.get("Metadata") or {}
        self.puts += 1

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        return {"Contents": [{"Key": k} for k in sorted(k for k in self.store if k.startswith(Prefix))]}


def csv_gz(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return gzip.compress(buf.getvalue().encode())


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


def seed(lf, s3):
    R = lf.FMP_ROOT
    inc = lambda sym, fy, period, d, rev: {"symbol": sym, "date": d, "fiscalYear": str(fy), "period": period, "reportedCurrency": "USD", "revenue": rev, "netIncome": rev // 10, "eps": "1.5", "cik": "0000320193"}
    s3.store[R + "income-statement-bulk/2025/annual.csv.gz"] = csv_gz([inc("AAPL", 2025, "FY", "2025-09-27", 416000000000), inc("MSFT", 2025, "FY", "2025-06-30", 281000000000), inc("SAP", 2025, "FY", "2025-12-31", 1)])
    s3.store[R + "income-statement-bulk/2024/annual.csv.gz"] = csv_gz([inc("AAPL", 2024, "FY", "2024-09-28", 391000000000), inc("MSFT", 2024, "FY", "2024-06-30", 245000000000)])
    s3.store[R + "income-statement-bulk/1999/annual.csv.gz"] = csv_gz([inc("AAPL", 1999, "FY", "1999-09-25", 6)])             # below the floor: never read
    s3.store[R + "income-statement-bulk/2025/Q1.csv.gz"] = csv_gz([inc("AAPL", 2025, "Q1", "2024-12-28", 124000000000)])
    s3.store[R + "income-statement-bulk/2025/Q2.csv.gz"] = csv_gz([inc("AAPL", 2025, "Q2", "2025-03-29", 95000000000)])
    s3.store[R + "income-statement-bulk/2025/Q3.csv.gz"] = csv_gz([inc("AAPL", 2025, "Q3", "2025-06-28", 94000000000)])
    s3.store[R + "ratios-bulk/2025/annual.csv.gz"] = csv_gz([{"symbol": "AAPL", "date": "2025-09-27", "fiscalYear": "2025", "period": "FY", "priceToEarningsRatio": 39.2}, {"symbol": "MSFT", "date": "2025-06-30", "fiscalYear": "2025", "period": "FY", "priceToEarningsRatio": 30.0}])
    s3.store[R + "manifest.json"] = json.dumps({"updated_at": "2026-10-25T23:25:00+00:00", "dated_keys": [
        "income-statement-bulk/2025/annual", "income-statement-bulk/2024/annual", "income-statement-bulk/1999/annual", "income-statement-bulk/2025/Q1",
        "income-statement-bulk/2025/Q2", "income-statement-bulk/2025/Q3", "ratios-bulk/2025/annual", "balance-sheet-statement-bulk/2025/annual"]}).encode()
    s3.store[lf.FACTOR_UNIVERSE_KEY] = jgz({"symbols": ["AAPL", "MSFT", "TINY"]})                                                   # SAP is Frankfurt -> out of the book


def test_inverts_year_files_into_per_symbol_docs_write_if_changed():
    lf, s3, fmp_bank = boot()
    seed(lf, s3)
    out = lf.lambda_handler({}, None)
    assert out["ok"], out
    assert "income-statement-bulk/annual" in out["blocks"] and "income-statement-bulk/quarter" in out["blocks"] and "ratios-bulk/annual" in out["blocks"], out
    # balance-sheet 2025/annual is in dated_keys but the file is missing -> block processed with a note, no docs
    assert any("missing" in n and "balance-sheet-statement-bulk" in n for n in out["notes"]), out["notes"]
    doc = json.loads(gzip.decompress(s3.store[lf.ROOT + "income-statement-bulk/annual/AAPL.json.gz"]))
    assert doc["symbol"] == "AAPL" and doc["endpoint"] == "income-statement" and doc["period"] == "annual" and doc["as_of"] == "2026-10-25T23:25:00+00:00"
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
    # second run: nothing changed -> no document re-put
    puts = s3.puts
    out2 = lf.lambda_handler({}, None)
    assert out2["ok"]
    man2 = json.loads(s3.store[lf.MANIFEST_KEY])
    assert man2["blocks"]["income-statement-bulk/annual"]["written"] == 0 and man2["blocks"]["income-statement-bulk/annual"]["unchanged"] == 2
    doc_puts = sum(1 for k in s3.store if k.startswith(lf.ROOT) and "/_state/" not in k and k != lf.MANIFEST_KEY)
    assert doc_puts == 5 and all(b.get("written", 0) == 0 for b in man2["blocks"].values()), (doc_puts, man2["blocks"])   # AAPL+MSFT annual, AAPL quarter, AAPL+MSFT ratios; nothing re-put
    # lease: a run inside another run's lease is skipped
    st = json.loads(s3.store[lf.STATE_KEY])
    assert st["phase"] == "STEADY" and st["updated_at"] and "lease_until" not in st and st["last_run"]["puts"] == 0, st
    assert man2["phase"] == "STEADY" and man2["pending_blocks"] == []
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
    import fmp_bankfirst as fb
    assert fb._shaped("income-statement", "income-statement", {"symbol": "AAPL", "period": "annual", "limit": "20"}) is True       # the book holds it
    assert fb._shaped("income-statement", "income-statement", {"symbol": "ZZZZ", "period": "annual", "limit": "20"}) is False      # nobody holds it: live
    assert fb._shaped("income-statement", "income-statement", {"symbol": "ZZZZ", "period": "annual", "limit": "10"}) is True       # inside the year-file ceiling


if __name__ == "__main__":
    test_inverts_year_files_into_per_symbol_docs_write_if_changed()
    print("ok test_inverts_year_files_into_per_symbol_docs_write_if_changed")
    test_fmp_bank_statements_read_the_history_book_first_and_the_shim_accepts_deep_limits()
    print("ok test_fmp_bank_statements_read_the_history_book_first_and_the_shim_accepts_deep_limits")
    print("2 tests passed")
