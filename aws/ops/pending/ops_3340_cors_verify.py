"""ops 3340 — verify both CORS fixes: (1) benzinga Function URL returns a
SINGLE Access-Control-Allow-Origin (not doubled); (2) data-proxy
/userdata/self OPTIONS preflight now allows Authorization header."""
import urllib.request, urllib.error
from pathlib import Path
from ops_report import report

def preflight(url, req_headers):
    req=urllib.request.Request(url,method="OPTIONS",headers={
        "Origin":"https://justhodl.ai",
        "Access-Control-Request-Method":"GET",
        "Access-Control-Request-Headers":req_headers,
        "User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req,timeout=20) as r:
            h={k.lower():v for k,v in r.getheaders()}
            return r.status, h
    except urllib.error.HTTPError as e:
        return e.code, {k.lower():v for k,v in (e.headers.items() if e.headers else [])}
    except Exception as e:
        return None, {"err":str(e)}

def simple_get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"}),timeout=25) as r:
            h={k.lower():v for k,v in r.getheaders()}
            return r.status, h.get("access-control-allow-origin")
    except Exception as e:
        return None, str(e)[:100]

with report("3340_cors_verify") as rep:
    # 1. benzinga function url — ACAO must be single value
    rep.section("BENZINGA FUNCTION URL")
    st,acao=simple_get("https://qgmut34alss5bvacffyklqqs3a0ckday.lambda-url.us-east-1.on.aws/")
    doubled = acao and ("," in acao)
    rep.kv(get_status=st, acao=acao, doubled=doubled)
    if doubled: rep.fail("ACAO still doubled")
    else: rep.ok("ACAO single value")
    # 2. data-proxy /userdata/self preflight with Authorization
    rep.section("DATA-PROXY /userdata/self PREFLIGHT")
    st2,h2=preflight("https://justhodl-data-proxy.raafouis.workers.dev/userdata/self","authorization,content-type")
    allow_hdrs=h2.get("access-control-allow-headers","")
    auth_ok="authorization" in allow_hdrs.lower()
    rep.kv(preflight_status=st2, allow_headers=allow_hdrs, authorization_allowed=auth_ok)
    if auth_ok: rep.ok("Authorization header now allowed")
    else: rep.warn("Authorization not yet in allow-headers (worker deploy still rolling?)")
    rep.section("VERDICT")
    if not doubled and auth_ok:
        rep.ok("BOTH CORS fixes live — benzinga.html loads clean"); rep.kv(RESULT="BOTH_FIXED")
    else:
        rep.kv(RESULT="PARTIAL", benzinga_ok=not doubled, userdata_ok=auth_ok)
