"""ops 3326 — confirm exact working forms for the last broken live calls
before patching: insider-trading variants + mutual-fund-info variants +
re-confirm the 4 known renames resolve."""
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
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh-3326"}),timeout=20) as r:
            b=json.loads(r.read()); return {"http":r.status,"n":len(b) if isinstance(b,list) else 1,"fields":list(b[0].keys())[:6] if isinstance(b,list) and b else None}
    except urllib.error.HTTPError as e:
        return {"http":e.code}
    except Exception as e:
        return {"err":type(e).__name__}
with report("3326_final_confirm") as rep:
    k=key()
    rep.section("INSIDER-TRADING")
    for path in ("insider-trading","insider-trading/search","insider-trading/latest"):
        rep.kv(**{path:g(path,{"symbol":"AAPL","page":0,"limit":50},k)})
    rep.section("MUTUAL-FUND-INFO")
    for path in ("mutual-fund-info","funds/info","mutual-fund/info"):
        rep.kv(**{path:g(path,{"symbol":"VTSAX"},k)})
    rep.section("RENAME RECONFIRM")
    rep.kv(grades_latest_news=g("grades-latest-news",{"limit":10},k),
           earnings=g("earnings",{"symbol":"AAPL","limit":4},k))
    rep.kv(RESULT="DONE")
