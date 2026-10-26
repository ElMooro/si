"""fmp_bank reader + fmp_book chain (2026-10-23): the FMP Ultimate bank answers statements/snapshots from S3 with full
history and honest periods; the TTM harvest no longer answers quarter/deep-history requests with one TTM row."""
import os
import sys, io, gzip, json, csv
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'aws', 'shared'))
import boto3
store={}
class S:
    def get_object(self,Bucket,Key):
        if Key not in store: raise KeyError(Key)
        return {"Body":io.BytesIO(store[Key])}
boto3.client=lambda *a,**k:S()
import fmp_bank, fmp_book
def gz(s): return gzip.compress(s.encode())
R=fmp_bank.ROOT
store[R+"manifest.json"]=json.dumps({"updated_at":"2026-10-23T23:00:00+00:00","endpoints_served":["ratios-ttm-bulk","income-statement-bulk"],"dated_keys":["income-statement-bulk/2025/annual","income-statement-bulk/2024/annual","income-statement-bulk/2025/Q1","income-statement-bulk/2025/Q2"]}).encode()
store[R+"ratios-ttm-bulk/latest.csv.gz"]=gz("symbol,peRatioTTM\nAAPL,39.2\nMSFT,30\n")
store[R+"income-statement-bulk/2025/annual.csv.gz"]=gz("symbol,date,period,revenue\nAAPL,2025-09-27,FY,416000000000\nMSFT,2025-06-30,FY,2\n")
store[R+"income-statement-bulk/2024/annual.csv.gz"]=gz("symbol,date,period,revenue\nAAPL,2024-09-28,FY,391000000000\n")
store[R+"income-statement-bulk/2025/Q1.csv.gz"]=gz("symbol,date,period,revenue\nAAPL,2024-12-28,Q1,124000000000\n")
store[R+"income-statement-bulk/2025/Q2.csv.gz"]=gz("symbol,date,period,revenue\nAAPL,2025-03-29,Q2,95000000000\n")
r=fmp_bank.snapshot("ratios-ttm","AAPL"); assert r==[{"symbol":"AAPL","peRatioTTM":39.2}], r
a=fmp_bank.statements("income-statement","AAPL"); assert [x["date"] for x in a]==["2025-09-27","2024-09-28"], a
q=fmp_bank.statements("income-statement","AAPL",period="quarter"); assert [x["date"] for x in q]==["2025-03-29","2024-12-28"], q
assert a[0]["revenue"]==416000000000 and isinstance(a[0]["revenue"],int)
assert fmp_bank.statements("income-statement","ZZZZ") is None
assert fmp_bank.snapshot("ratios-ttm","ZZZZ") is None
# book chain: bank first
h=fmp_book.try_harvest("income-statement","symbol=AAPL&period=quarter&limit=8"); assert len(h)==2 and h[0]["period"]=="Q2", h
h=fmp_book.try_harvest("ratios-ttm","symbol=AAPL"); assert h[0]["peRatioTTM"]==39.2
assert fmp_book.try_harvest("balance-sheet-statement","symbol=AAPL&period=quarter") is None   # not banked, history asked -> live
assert fmp_book.try_harvest("key-metrics","symbol=AAPL&limit=25") is None
h=fmp_book.try_harvest("income-statement/AAPL",""); assert h and h[0]["date"]=="2025-09-27"
# types follow FMP's JSON, not the CSV: identifiers/dates/fiscal years stay strings (zero-padded ciks included), booleans become booleans, numbers numbers
store[R+"profile-bulk/latest.csv.gz"]=gz("symbol,cik,fiscalYear,isEtf,isActivelyTrading,marketCap,beta,zip,ipoDate\n600519,0000320193,2025,false,True,3900000000000,1.25,95014,1980-12-12\n")
fmp_bank._TABLES.clear(); fmp_bank._MISSING.clear()
pr=fmp_bank.snapshot("profile","600519")[0]
assert pr["symbol"]=="600519" and pr["cik"]=="0000320193" and pr["fiscalYear"]=="2025" and pr["isEtf"] is False and pr["isActivelyTrading"] is True, pr
assert pr["marketCap"]==3900000000000 and isinstance(pr["marketCap"],int) and pr["beta"]==1.25 and pr["zip"]=="95014" and pr["ipoDate"]=="1980-12-12", pr

# 2026-10-26 (bank fusion first light): eight engine threads each decoded the same 60 MB bulk at the same instant ->
# Runtime.OutOfMemory.  One loader per key: concurrent callers wait for the first decode and reuse the table.
import threading, time as _time
_fetch_calls = {"n": 0}
_orig_fetch = fmp_bank._fetch
def _slow_fetch(key):
    _fetch_calls["n"] += 1
    _time.sleep(0.2)
    return _orig_fetch(key)
fmp_bank._fetch = _slow_fetch
fmp_bank._TABLES.clear(); fmp_bank._MISSING.clear(); fmp_bank._LOADING.clear()
_key = R + "ratios-ttm-bulk/latest.csv.gz"
_got = []
_threads = [threading.Thread(target=lambda: _got.append(fmp_bank._table(_key))) for _ in range(8)]
[t.start() for t in _threads]; [t.join() for t in _threads]
assert _fetch_calls["n"] == 1, "eight concurrent first touches of one table must fetch + decode it once, fetched %d times" % _fetch_calls["n"]
assert len(_got) == 8 and all(t is _got[0] for t in _got), "every waiter must get the one built table"
fmp_bank._fetch = _orig_fetch
assert fmp_bank._load_slots() in (1, 2)


def test_fmp_bank_reader_and_book_chain():
    pass  # assertions above ran at import (module-level, like the sibling byte tests)


if __name__ == "__main__":
    print("fmp_bank + fmp_book chain OK")
