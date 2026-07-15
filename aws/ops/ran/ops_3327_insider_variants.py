"""ops 3327 — confirm the right insider path for market-wide S-Sale scan
(insider-sell-cluster uses no symbol + transactionType=S-Sale)."""
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
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh-3327"}),timeout=20) as r:
            b=json.loads(r.read()); return {"http":r.status,"n":len(b) if isinstance(b,list) else 1,"fields":list(b[0].keys())[:8] if isinstance(b,list) and b else None}
    except urllib.error.HTTPError as e:
        return {"http":e.code}
    except Exception as e:
        return {"err":type(e).__name__}
with report("3327_insider_variants") as rep:
    k=key()
    for path,params in [
        ("insider-trading/latest",{"page":0,"limit":50}),
        ("insider-trading/latest",{"transactionType":"S-Sale","page":0,"limit":50}),
        ("insider-trading/search",{"transactionType":"S-Sale","page":0,"limit":50}),
    ]:
        rep.kv(**{f"{path} {params.get('transactionType','')}":g(path,params,k)})
    rep.kv(RESULT="DONE")
