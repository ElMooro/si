"""ops 3273b — TV watchlists on the charts page, live-verified:
(1) served chart-pro carries the ops-3273 section + the real API URL;
(2) the API answers cross-origin-ready (CORS header) for a derived
symbol class too; (3) 3272 close: homepage now serves the VERSIONED
drawer tag (normalizer rebake)."""
import json
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3273b)"}
API = ("https://nu4umjskc25osscrbmqh3o2gte0utlkx"
       ".lambda-url.us-east-1.on.aws")


def get(u):
    r = urllib.request.urlopen(
        urllib.request.Request(u, headers=UA), timeout=20)
    return r.headers, r.read().decode("utf-8", "replace")


with report("3273b_charts_live") as rep:
    fails = []
    ok_p = ok_h = False
    for i in range(24):
        try:
            _, h = get("https://justhodl.ai/chart-pro.html?t="
                       f"{int(time.time())}")
            ok_p = "ops 3273" in h and "lambda-url" in h
        except Exception:
            pass
        if ok_p:
            rep.ok(f"chart-pro live with TV WATCHLISTS "
                   f"(~{(i + 1) * 15}s)")
            break
        time.sleep(15)
    try:
        _, hp = get(f"https://justhodl.ai/?t={int(time.time())}")
        ok_h = "jh-nav-drawer.js?v=" in hp
        if ok_h:
            rep.ok("homepage serves VERSIONED drawer tag "
                   "(3272 closed)")
    except Exception:
        pass
    try:
        hd, body = get(API + "/?sym=TVC:DXY")
        cors = hd.get("Access-Control-Allow-Origin")
        j = json.loads(body)
        rep.kv(api_n=j.get("n"), cors=cors)
        if cors != "*":
            fails.append("CORS header missing")
    except Exception as e:
        fails.append(f"api: {str(e)[:60]}")
    if not ok_p:
        fails.append("chart-pro section not live")
    if not ok_h:
        fails.append("homepage still unversioned")
    rep.kv(n_fails=len(fails),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
