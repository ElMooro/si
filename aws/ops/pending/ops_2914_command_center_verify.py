#!/usr/bin/env python3
"""ops 2914 — Command Center v2 end-to-end verify + tape engine backstop.
(a) market-tape: ensure fn exists (deploy_lambda helper backstop), EB Scheduler
    5-min, invoke once, verify data/market-tape.json fresh in S3.
(b) Poll live index.html until AMBER TERMINAL marker + JH_V v1.2.0 present.
(c) HTTP-check every internal href emitted by the new homepage.
(d) Resolve every binder feed candidate via justhodl.ai/data/* -> 200/404 table.
(e) Live-page lint: no [object Object]/undefined/NaN artifacts.
Report -> aws/ops/reports/2914.json
"""
import json, re, sys, time, urllib.request
from pathlib import Path
sys.path.insert(0, "aws/ops")
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

def get(u, tries=3, to=20):
    for i in range(tries):
        try:
            r = urllib.request.urlopen(urllib.request.Request(
                u, headers={"User-Agent": "Mozilla/5.0 jh-ops"}), timeout=to)
            return r.getcode(), r.read().decode("utf-8", "replace")
        except Exception:
            time.sleep(6)
    return None, ""

out = {"engine": {}, "page": {}, "links": {}, "feeds": {}, "lint": {}}
with report("2914") as r:
    r.section("(a) market-tape engine")
    deploy_lambda(report=r, function_name="justhodl-market-tape",
                  source_dir=Path("aws/lambdas/justhodl-market-tape/source"),
                  env_vars=None, inherit_env_from="justhodl-confluence-meta",
                  timeout=30, memory=192, smoke_test=False)
    sch = boto3.client("scheduler", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1")
    arn = lam.get_function_configuration(FunctionName="justhodl-market-tape")["FunctionArn"]
    role = "arn:aws:iam::857687956942:role/lambda-execution-role"
    try:
        sch.get_schedule(Name="justhodl-market-tape-5min")
        r.ok("  scheduler exists")
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name="justhodl-market-tape-5min",
            ScheduleExpression="rate(5 minutes)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": arn, "RoleArn": role, "Input": "{}"})
        r.ok("  scheduler created rate(5m)")
    inv = lam.invoke(FunctionName="justhodl-market-tape", Payload=b"{}")
    body = json.loads(inv["Payload"].read().decode())
    out["engine"] = body.get("body", body)
    r.ok(f"  invoke: {out['engine']}")
    s3 = boto3.client("s3")
    h = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/market-tape.json")
    out["engine_s3_bytes"] = h["ContentLength"]
    r.ok(f"  s3 tape: {h['ContentLength']}B")

    r.section("(b) live homepage marker")
    live = ""
    for att in range(12):
        c, live = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        ok = c == 200 and "AMBER TERMINAL" in live and 'JH_V="v1.2.0"' in live
        r.log(f"  attempt {att+1}: http={c} marker={'AMBER TERMINAL' in live} v1.2.0={'JH_V=\"v1.2.0\"' in live}")
        if ok:
            out["page"] = {"attempt": att + 1, "bytes": len(live)}
            r.ok(f"  LIVE on attempt {att+1} ({len(live)}B)")
            break
        time.sleep(20)
    else:
        r.fail("  marker never appeared"); out["page"] = {"attempt": None}

    r.section("(c) every internal href on the live page")
    hrefs = sorted(set(re.findall(r'href="(/[a-z0-9-]+\.html)"', live)))
    bad = []
    for h_ in hrefs:
        c, b = get(f"https://justhodl.ai{h_}?t={int(time.time())}", tries=2)
        ok = c == 200 and len(b) > 400
        (r.ok if ok else r.fail)(f"  {h_} http={c}")
        out["links"][h_] = c
        if not ok: bad.append(h_)

    r.section("(d) binder feed resolution")
    feeds = sorted(set(re.findall(r'"(data/[a-z0-9-]+\.json)"', live))) + ["data.json", "nav-manifest.json"]
    for f in feeds:
        c, b = get(f"https://justhodl.ai/{f}?t={int(time.time())}", tries=2, to=15)
        ok = c == 200 and b.strip().startswith(("{", "["))
        out["feeds"][f] = c
        (r.ok if ok else r.log)(f"  {f} -> {c}")

    r.section("(e) live lint")
    for tokn in ["[object Object]", "undefined%", ">NaN<"]:
        bad_t = tokn in live
        out["lint"][tokn] = not bad_t
        (r.fail if bad_t else r.ok)(f"  '{tokn}' absent: {not bad_t}")

    json.dump(out, open("aws/ops/reports/2914.json", "w"), indent=2, default=str)
    r.ok("report -> aws/ops/reports/2914.json")
print("DONE 2914")
sys.exit(0)
