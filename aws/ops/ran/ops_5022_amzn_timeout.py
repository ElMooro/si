"""ops 5022 — AMZN first-build vs function-URL timeout.

Symptom: AMZN sits on the building-note even with LIVE-direct
rendering deployed. Hypothesis: a heavy first build (full doc + an
expired 6-day peer-price bundle rebuild) exceeds the Lambda timeout,
so the browser's LIVE call gets a 5xx and the silent catch leaves the
note. This op measures an AMZN cold build, reads the function
timeout, raises it (stated config change) if the ceiling is the
problem, re-verifies, and also fetches why.html WITHOUT a
cache-buster to see what the edge really serves users.
"""
import json
import time
import urllib.request

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-equity-research"
LIVE = ("https://6nkrwmk2ntjx54okqvtzokosb40whvfb"
        ".lambda-url.us-east-1.on.aws/")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126 Safari/537.36")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(connect_timeout=10, read_timeout=600,
                                 retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name="us-east-1")

with report("ops_5022_amzn_timeout") as rep:
    rep.heading("ops 5022 -- AMZN first-build vs timeout")

    rep.section("G0 function config")
    cfg = lam.get_function_configuration(FunctionName=FN)
    t0cfg, mem = cfg["Timeout"], cfg["MemorySize"]
    rep.kv(timeout_s=t0cfg, memory_mb=mem)

    rep.section("P1 AMZN cold build (direct invoke, refresh=1)")
    t0 = time.time()
    r = lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"queryStringParameters": {"ticker": "AMZN",
                                   "refresh": "1"}}).encode())
    body = json.loads(r["Payload"].read() or b"{}")
    dt = round(time.time() - t0, 1)
    doc = body
    if isinstance(body, dict) and isinstance(body.get("body"), str):
        doc = json.loads(body["body"])
    err = body.get("errorMessage") if isinstance(body, dict) else None
    rep.kv(gen_s=dt, schema=(doc.get("schema_version")
                             if isinstance(doc, dict) else None),
           gfx=bool(((doc.get("gf_extras") or {})
                     if isinstance(doc, dict) else {}).get("available")),
           err=(str(err)[:80] if err else None))
    if err:
        raise SystemExit("AMZN invoke errored: %s" % err)

    rep.section("G1 timeout headroom (config change only if needed)")
    need = int(max(t0cfg, min(300, max(120, dt * 6))))
    if dt * 2.5 > t0cfg:
        lam.update_function_configuration(FunctionName=FN,
                                          Timeout=need)
        lam.get_waiter("function_updated_v2").wait(FunctionName=FN)
        rep.warn("Timeout RAISED %ss -> %ss (deliberate config "
                 "change): cold builds with peer-bundle rebuilds need "
                 "headroom on the URL path" % (t0cfg, need))
    else:
        rep.ok("timeout %ss has %.1fx headroom over this cold build "
               "-- unchanged" % (t0cfg, t0cfg / max(dt, 0.1)))

    rep.section("P2 URL-path proof (as the browser calls it)")
    req = urllib.request.Request(
        LIVE + "?ticker=AMZN&refresh=1",
        headers={"User-Agent": UA, "Origin": "https://justhodl.ai"})
    t1 = time.time()
    with urllib.request.urlopen(req, timeout=590) as resp:
        d2 = json.loads(resp.read())
    rep.kv(url_status=resp.status, url_s=round(time.time() - t1, 1),
           url_schema=d2.get("schema_version"),
           url_gfx=bool((d2.get("gf_extras") or {}).get("available")))
    if d2.get("schema_version") != "2.9.3":
        raise SystemExit("URL path did not return a 2.9.3 doc")

    rep.section("P3 edge serves the new page WITHOUT a buster")
    req = urllib.request.Request("https://justhodl.ai/why.html",
                                 headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        page = resp.read().decode("utf-8", "replace")
        cache_hdrs = {k: resp.headers.get(k) for k in
                      ("cf-cache-status", "age", "cache-control")}
    n60 = page.count("if(_pt>60)")
    nff = page.count("First fetch failed")
    rep.kv(edge_pt60=n60, edge_firstfail=nff, **{
        k.replace("-", "_"): v for k, v in cache_hdrs.items() if v})
    if n60 != 6 or nff != 6:
        rep.fail("edge still serves the OLD page to plain requests "
                 "-- users need a cache purge / shorter html TTL")
        raise SystemExit("edge cache stale for users")
    rep.ok("AMZN builds within limits on the exact browser path and "
           "the edge serves the fixed page to plain requests")
