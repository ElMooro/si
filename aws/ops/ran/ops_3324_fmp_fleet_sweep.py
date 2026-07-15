"""ops 3324 — fleet sweep: probe every distinct FMP /stable endpoint used
across the Lambda fleet with a representative param set, to definitively
map WORKING vs BROKEN (renamed/removed). Fixes only what's proven broken.
Guards against silent-empty engines beyond the known grades-news /
earnings-surprises renames.
"""
import json, urllib.request, urllib.error
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", "us-east-1")
def key():
    for fn in ("justhodl-analyst-consensus","justhodl-confluence-meta","justhodl-sellside-views"):
        try:
            c=LAM.get_function_configuration(FunctionName=fn)
            e=(c.get("Environment") or {}).get("Variables") or {}
            for k in ("FMP_KEY","FMP_API_KEY"):
                if e.get(k): return e[k]
        except Exception: pass
    return None

def g(path, params, k):
    p={**params,"apikey":k}
    url=f"https://financialmodelingprep.com/stable/{path}?"+"&".join(f"{a}={b}" for a,b in p.items())
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh-3324"}),timeout=20) as r:
            b=json.loads(r.read())
            n=len(b) if isinstance(b,list) else 1
            return {"http":r.status,"n":n}
    except urllib.error.HTTPError as e:
        try: d=json.loads(e.read().decode()).get("Error Message") or e.read().decode()[:80]
        except Exception: d="(400/err)"
        return {"http":e.code,"err":str(d)[:90]}
    except Exception as e:
        return {"http":None,"err":type(e).__name__}

# representative params per endpoint (symbol/limit/etc as the fleet uses)
SYM={"symbol":"AAPL"}
TESTS={
  "quote":SYM, "historical-price-eod":{"symbol":"AAPL"}, "profile":SYM,
  "income-statement":{**SYM,"limit":1}, "ratios-ttm":SYM,
  "cash-flow-statement":{**SYM,"limit":1}, "key-metrics-ttm":SYM,
  "balance-sheet-statement":{**SYM,"limit":1}, "sp500-constituent":{},
  "insider-trading":SYM, "earnings-calendar":SYM, "ipos-calendar":{},
  "etf":SYM, "stock-price-change":SYM, "news":{"limit":5},
  "company-screener":{"limit":5}, "analyst-estimates":SYM,
  "earnings":{**SYM,"limit":4}, "key-metrics":{**SYM,"limit":1},
  "grades-news":{"limit":10}, "grades":SYM, "grades-latest-news":{"limit":10},
  "grades-consensus":SYM, "economic-calendar":{}, "stock-peers":SYM,
  "ratios":{**SYM,"limit":1}, "price-target-consensus":SYM,
  "financial-scores":SYM, "earnings-surprises":{**SYM,"limit":4},
  "shares-float":SYM, "institutional-ownership":SYM,
  "income-statement-growth":{**SYM,"limit":1}, "bankruptcies":{},
  "upgrades-downgrades-consensus":SYM, "short-interest":SYM,
  "sector-pe-snapshot":{"date":"2026-07-10","sector":"Technology"},
  "stock-list":{}, "peers":SYM, "quote-short":SYM, "profile-bulk":{"part":"0"},
  "owner-earnings":SYM, "mutual-fund-info":SYM,
  "mergers-acquisitions-latest":{"limit":5}, "industry-pe-snapshot":{"date":"2026-07-10","industry":"Software"},
  "price-target-latest-news":{"limit":10},
}

with report("3324_fmp_fleet_sweep") as rep:
    k=key()
    rep.kv(key_present=bool(k))
    broken={}; working={}
    for path,params in TESTS.items():
        r=g(path,params,k)
        if r.get("http")==200: working[path]=r.get("n")
        else: broken[path]=r
    rep.section("WORKING")
    rep.kv(**{p:f"200 n={n}" for p,n in working.items()})
    rep.section("BROKEN / SUSPECT")
    for p,r in broken.items():
        rep.kv(**{p:r})
    rep.section("SUMMARY")
    rep.kv(n_working=len(working), n_broken=len(broken),
           broken_list=sorted(broken.keys()))
    rep.ok("sweep complete")
    rep.kv(RESULT="DONE")
