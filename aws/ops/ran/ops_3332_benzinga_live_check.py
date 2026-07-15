"""ops 3332 — final live check: hit the exact Function URL benzinga.html
fetches, confirm the page will render (non-empty sections). Read-only."""
import json, urllib.request
from pathlib import Path
from ops_report import report
URL="https://qgmut34alss5bvacffyklqqs3a0ckday.lambda-url.us-east-1.on.aws/"
with report("3332_benzinga_live_check") as rep:
    try:
        req=urllib.request.Request(URL,headers={"User-Agent":"Mozilla/5.0 (jh-live-check)"})
        with urllib.request.urlopen(req,timeout=30) as r:
            d=json.loads(r.read())
        c=d.get("counts",{})
        rep.kv(http=r.status,source=d.get("source"),ts=d.get("ts"),counts=c,
               first_headline=(d.get("market_news") or [{}])[0].get("title"))
        if sum(c.values() if c else [0])>0 and r.status==200:
            rep.ok("Function URL live + populated — benzinga.html renders end-to-end")
            rep.kv(RESULT="PAGE_LIVE")
        else:
            rep.fail("URL up but empty"); rep.kv(RESULT="EMPTY")
    except Exception as e:
        rep.fail(f"URL fetch failed: {e}"); rep.kv(RESULT="URL_FAIL")
