"""ops 3321 — find the correct current FMP /stable earnings-surprises
endpoint + fields for analyst-consensus (old names 400/404)."""
import json, urllib.request, urllib.error
from pathlib import Path
import boto3
from ops_report import report
LAM = boto3.client("lambda","us-east-1")
def key():
    c=LAM.get_function_configuration(FunctionName="justhodl-analyst-consensus")
    return ((c.get("Environment") or {}).get("Variables") or {}).get("FMP_KEY")
def g(path, params, k):
    p={**params,"apikey":k}
    url=f"https://financialmodelingprep.com/stable/{path}?"+"&".join(f"{a}={b}" for a,b in p.items())
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh-3321"}),timeout=20) as r:
            b=json.loads(r.read())
            if isinstance(b,list): return {"http":r.status,"n":len(b),"fields":list(b[0].keys()) if b else [],"sample":b[0] if b else None}
            return {"http":r.status,"keys":list(b.keys())[:8]}
    except urllib.error.HTTPError as e:
        try: d=e.read().decode()[:120]
        except Exception: d=""
        return {"http":e.code,"err":d}
    except Exception as e:
        return {"http":None,"err":f"{type(e).__name__}"}
with report("3321_earnings_surprise_probe") as rep:
    k=key()
    for path,params in [
        ("earnings-surprises",{"symbol":"AAPL","limit":8}),
        ("earnings",{"symbol":"AAPL","limit":8}),
        ("earnings-surprises-bulk",{"symbol":"AAPL","year":"2025"}),
        ("historical-earnings",{"symbol":"AAPL"}),
        ("earnings-calendar",{"symbol":"AAPL"}),
    ]:
        rep.kv(**{path: g(path,params,k)})
    rep.kv(RESULT="DONE")
