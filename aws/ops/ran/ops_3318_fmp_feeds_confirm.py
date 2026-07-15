"""ops 3318 — confirm the PROVEN grades-news call shape (limit only, no page)
returns live data, and find the working PT-revision feed. Mirrors the exact
call justhodl-analyst-consensus.fetch_grade_changes_universe uses."""
import json, sys, urllib.request, urllib.error
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", "us-east-1")
def fmp_key():
    cfg = LAM.get_function_configuration(FunctionName="justhodl-analyst-consensus")
    return ((cfg.get("Environment") or {}).get("Variables") or {}).get("FMP_KEY")

def g(path, params, key):
    p = {**params, "apikey": key}
    url = f"https://financialmodelingprep.com/stable/{path}?" + "&".join(f"{a}={b}" for a,b in p.items())
    req = urllib.request.Request(url, headers={"User-Agent":"jh-3318"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            b = json.loads(r.read())
            if isinstance(b, list):
                return {"http": r.status, "n": len(b),
                        "fields": list(b[0].keys()) if b else [],
                        "sample": b[0] if b else None}
            return {"http": r.status, "type":"dict", "keys": list(b.keys())[:8]}
    except urllib.error.HTTPError as e:
        try: d = e.read().decode()[:150]
        except Exception: d = ""
        return {"http": e.code, "err": d}
    except Exception as e:
        return {"http": None, "err": f"{type(e).__name__}: {e}"}

with report("3318_fmp_feeds_confirm") as rep:
    key = fmp_key()
    rep.kv(key_suffix=key[-4:] if key else None)
    # proven grades-news call
    rep.section("GRADES-NEWS (limit only)")
    gn = g("grades-news", {"limit": 500}, key)
    rep.kv(grades_news=gn)
    # try PT feed variants
    rep.section("PT-NEWS VARIANTS")
    for path in ("price-target-news", "price-target-latest-news", "grades-latest-news"):
        rep.kv(**{path: g(path, {"limit": 100}, key)})
    rep.section("VERDICT")
    if gn.get("http")==200 and gn.get("n",0)>0:
        rep.ok("grades-news live with proven shape"); rep.kv(RESULT="OK", n=gn.get("n"))
    else:
        rep.fail("grades-news still empty"); rep.kv(RESULT="FAIL"); sys.exit(1)
