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


def test_fmp_bank_reader_and_book_chain():
    pass  # assertions above ran at import (module-level, like the sibling byte tests)


if __name__ == "__main__":
    print("fmp_bank + fmp_book chain OK")
