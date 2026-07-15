"""ops 3320 — audit the remaining FMP endpoints justhodl-analyst-consensus
calls, to fix ALL broken ones in one pass (not just grades-news). Tests:
quote, earnings-surprises-bulk, earnings-surprises, grades-news (known
broken), grades-consensus + price-target-consensus (known good, re-confirm).
"""
import json, sys, urllib.request, urllib.error
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", "us-east-1")
def key():
    cfg = LAM.get_function_configuration(FunctionName="justhodl-analyst-consensus")
    return ((cfg.get("Environment") or {}).get("Variables") or {}).get("FMP_KEY")

def g(path, params, k):
    p = {**params, "apikey": k}
    url = f"https://financialmodelingprep.com/stable/{path}?" + "&".join(f"{a}={b}" for a,b in p.items())
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent":"jh-3320"}), timeout=20) as r:
            b = json.loads(r.read())
            if isinstance(b, list):
                return {"http": r.status, "n": len(b), "fields": list(b[0].keys())[:8] if b else []}
            return {"http": r.status, "keys": list(b.keys())[:8]}
    except urllib.error.HTTPError as e:
        try: d = e.read().decode()[:120]
        except Exception: d = ""
        return {"http": e.code, "err": d}
    except Exception as e:
        return {"http": None, "err": f"{type(e).__name__}"}

with report("3320_consensus_endpoint_audit") as rep:
    k = key()
    rep.kv(key_suffix=k[-4:] if k else None)
    tests = {
        "quote": {"symbol":"AAPL"},
        "earnings-surprises-bulk": {"symbol":"AAPL"},
        "earnings-surprises": {"symbol":"AAPL"},
        "grades-news": {"limit":500},
        "grades-latest-news": {"limit":500},
        "grades-consensus": {"symbol":"AAPL"},
        "price-target-consensus": {"symbol":"AAPL"},
    }
    for path, params in tests.items():
        rep.kv(**{path: g(path, params, k)})
    rep.ok("audit complete — see rows for which paths 400")
    rep.kv(RESULT="AUDIT_DONE")
