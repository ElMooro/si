"""ops 5020 — browser auto-upgrade unblock (Lambda URL CORS).

Root cause of the stuck TSLA layers: the version-gate regen is
triggered FROM THE BROWSER (layers call LIVE?refresh=1 when the CDN
doc is stale). Ops always invoked the function server-side, so a
missing CORS allowance on the function URL never showed up in a
report. This op audits the function-URL CORS config, adds
https://justhodl.ai (GET) if absent — a deliberate, logged
configuration change — and proves it with an Origin-header request
from the runner. Idempotent.
"""
import json
import time
import urllib.request

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-equity-research"
ORIGIN = "https://justhodl.ai"
LIVE = ("https://6nkrwmk2ntjx54okqvtzokosb40whvfb"
        ".lambda-url.us-east-1.on.aws/")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(connect_timeout=10, read_timeout=60,
                                 retries={"max_attempts": 1}))

with report("ops_5020_cors_unblock") as rep:
    rep.heading("ops 5020 -- browser auto-upgrade unblock (CORS)")

    rep.section("G0 current function-URL config")
    cfg = lam.get_function_url_config(FunctionName=FN)
    cors = cfg.get("Cors") or {}
    rep.log("before: " + json.dumps(cors, sort_keys=True))
    allowed = cors.get("AllowOrigins") or []
    ok_already = ("*" in allowed) or (ORIGIN in allowed)

    rep.section("G1 config change (only if needed)")
    if ok_already:
        rep.ok("origin already allowed — no change made")
    else:
        new = {"AllowOrigins": sorted(set(allowed) | {ORIGIN}),
               "AllowMethods": sorted(set(cors.get("AllowMethods") or
                                          []) | {"GET"}),
               "AllowHeaders": sorted(set(cors.get("AllowHeaders") or
                                          []) | {"content-type"}),
               "MaxAge": max(int(cors.get("MaxAge") or 0), 3600)}
        lam.update_function_url_config(FunctionName=FN, Cors=new)
        rep.warn("function URL Cors UPDATED (deliberate config "
                 "change): " + json.dumps(new, sort_keys=True))
        time.sleep(3)

    rep.section("P1 prove it with an Origin-header request")
    req = urllib.request.Request(
        LIVE + "?ticker=KO", headers={"Origin": ORIGIN})
    with urllib.request.urlopen(req, timeout=55) as r:
        acao = r.headers.get("Access-Control-Allow-Origin")
        body = r.read(400)
    rep.kv(status=r.status, acao=acao, body_head=body[:60].decode(
        "utf-8", "replace"))
    if not acao or (acao != "*" and ORIGIN not in acao):
        raise SystemExit("ACAO header still missing for %s" % ORIGIN)
    rep.ok("browser-origin requests now receive "
           "Access-Control-Allow-Origin — stale docs self-upgrade "
           "from the page for every ticker and every future schema "
           "bump")
