"""ops 3333 — benzinga.html now says 'Failed to fetch' (browser-side network
error, NOT empty data — agent returns 200 w/ data per 3332). Diagnose the
client failure: (a) Function URL AUTH mode (AWS_IAM would block anon browser
fetch -> 'Failed to fetch'); (b) CORS config on the URL; (c) actual response
headers incl Access-Control-Allow-Origin on a plain GET; (d) whether our
handler CORS headers survive (Function URL CORS can override/duplicate)."""
import json, urllib.request
from pathlib import Path
import boto3
from ops_report import report
LAM=boto3.client("lambda","us-east-1")
FN="benzinga-news-agent"
URL="https://qgmut34alss5bvacffyklqqs3a0ckday.lambda-url.us-east-1.on.aws/"
with report("3333_benzinga_cors_diag") as rep:
    # 1. Function URL config (auth mode + CORS)
    rep.section("FUNCTION URL CONFIG")
    try:
        cfg=LAM.get_function_url_config(FunctionName=FN)
        rep.kv(auth_type=cfg.get("AuthType"), url=cfg.get("FunctionUrl"),
               cors=cfg.get("Cors"))
    except Exception as e:
        rep.kv(url_config_err=str(e))
        # maybe URL is on an alias/version or not attached
    # 2. resource policy (is public invoke allowed?)
    rep.section("RESOURCE POLICY")
    try:
        pol=LAM.get_policy(FunctionName=FN)
        rep.kv(policy=json.loads(pol["Policy"]))
    except Exception as e:
        rep.kv(policy_err=type(e).__name__, note="no policy = anon invoke likely DENIED")
    # 3. real HTTP response headers on plain GET
    rep.section("LIVE GET HEADERS")
    try:
        req=urllib.request.Request(URL,headers={"User-Agent":"Mozilla/5.0","Origin":"https://justhodl.ai"})
        with urllib.request.urlopen(req,timeout=30) as r:
            hdrs={k:v for k,v in r.getheaders()}
            rep.kv(status=r.status,
                   acao=hdrs.get("Access-Control-Allow-Origin"),
                   content_type=hdrs.get("Content-Type"),
                   all_headers=hdrs)
    except Exception as e:
        rep.kv(get_err=str(e)[:150])
    rep.kv(RESULT="DIAG_DONE")
