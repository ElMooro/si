"""ops 3325 — re-probe the ambiguous endpoints from 3324 with the EXACT
params the fleet uses, to separate real renames from probe-param errors.
Only genuinely-broken endpoints get fixed."""
import json, urllib.request, urllib.error
from pathlib import Path
import boto3
from ops_report import report
LAM=boto3.client("lambda","us-east-1")
def key():
    c=LAM.get_function_configuration(FunctionName="justhodl-analyst-consensus")
    return ((c.get("Environment") or {}).get("Variables") or {}).get("FMP_KEY")
def g(path,params,k):
    p={**params,"apikey":k}
    url=f"https://financialmodelingprep.com/stable/{path}?"+"&".join(f"{a}={b}" for a,b in p.items())
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh-3325"}),timeout=20) as r:
            b=json.loads(r.read()); return {"http":r.status,"n":len(b) if isinstance(b,list) else 1}
    except urllib.error.HTTPError as e:
        try: d=json.loads(e.read().decode()).get("Error Message","")[:90]
        except Exception: d="err"
        return {"http":e.code,"err":str(d)[:90]}
    except Exception as e:
        return {"http":None,"err":type(e).__name__}
# correct params per fleet usage + candidate replacements for suspected renames
TESTS={
  "analyst-estimates":{"symbol":"AAPL","period":"annual","limit":2},
  "insider-trading":{"symbol":"AAPL","page":0,"limit":10},
  "insider-trading/search":{"symbol":"AAPL","page":0,"limit":10},
  "institutional-ownership":{"symbol":"AAPL"},
  "institutional-ownership/symbol-ownership":{"symbol":"AAPL"},
  "short-interest":{"symbol":"AAPL"},
  "peers":{"symbol":"AAPL"},
  "stock-peers":{"symbol":"AAPL"},
  "etf":{"symbol":"SPY"},
  "etf/holdings":{"symbol":"SPY"},
  "historical-price-eod":{"symbol":"AAPL","from":"2026-06-01","to":"2026-07-10"},
  "historical-price-eod/full":{"symbol":"AAPL","from":"2026-06-01","to":"2026-07-10"},
  "news":{"limit":5},
  "news/general-latest":{"limit":5},
  "bankruptcies":{"page":0,"limit":5},
  "profile-bulk":{"part":"0"},
  "mutual-fund-info":{"symbol":"VTSAX"},
  "upgrades-downgrades-consensus":{"symbol":"AAPL"},
}
with report("3325_fmp_reprobe") as rep:
    k=key(); broken={}
    for path,params in TESTS.items():
        r=g(path,params,k)
        rep.kv(**{path:r})
        if r.get("http")!=200: broken[path]=r
    rep.section("STILL BROKEN")
    rep.kv(still_broken=sorted(broken.keys()))
    rep.kv(RESULT="DONE")
