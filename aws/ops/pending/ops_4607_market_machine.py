"""ops 4607 — MARKET MACHINE: Khalid's four-pillar doctrine goes live.

New engine justhodl-market-machine joins 12 existing fleet surfaces
(estimate-revisions, analyst-actions, readthrough, plumbing v2.1,
accum-composite, etf-true-flows, DIX, rotation-dashboard,
vol-target-unwind, capital-flow-radar, spx-ma, risk-gate) plus a small
direct FRED rates block into one composite: profits / rates / flows /
forced-positioning, each 0-100 support, with a plain-English machine
verdict. Page: market-machine.html.

This op: deploy-settle (new function), config floor, hourly EventBridge
schedule via the shared scheduler role, invoke, per-pillar coverage
contracts (>=2 live contributors each — discovery-based joins report
absences honestly), purge (token restored in 4606), edge asserts.
"""
import io
import json
import time
import urllib.request
import zipfile
import sys
import os

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-market-machine"
B = "justhodl-dashboard-live"
ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
FARN = "arn:aws:lambda:us-east-1:857687956942:function:" + FN
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4607"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def cf(path, method="GET", data=None):
    tok = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not tok:
        return None, "no token"
    req = urllib.request.Request(
        "https://api.cloudflare.com/client/v4" + path,
        data=json.dumps(data).encode() if data else None, method=method,
        headers={"Authorization": "Bearer " + tok,
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as h:
            return json.loads(h.read()), None
    except Exception as e:
        return None, str(e)[:100]


def main():
    misses = 0
    with report("4607_market_machine") as r:
        r.heading("ops 4607 — MARKET MACHINE (four-pillar doctrine)")

        r.section("deploy-settle (new function)")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "justhodl-market-machine v1.0.0" in src:
                    settled = True
                    r.log("function live with v1.0.0 (attempt %d)"
                          % (att + 1))
                    break
                r.log("attempt %d: pre-v1 zip" % (att + 1))
            except lam.exceptions.ResourceNotFoundException:
                r.log("attempt %d: function not created yet" % (att + 1))
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:90]))
            time.sleep(30)
        misses += contract(r, "deploy", settled,
                           "justhodl-market-machine exists with v1.0.0")
        if not settled:
            sys.exit(1)

        r.section("config floor + env")
        cfg = lam.get_function_configuration(FunctionName=FN)
        t0, m0 = cfg["Timeout"], cfg["MemorySize"]
        envv = (cfg.get("Environment") or {}).get("Variables") or {}
        want = dict(envv)
        want.setdefault("S3_BUCKET", B)
        want.setdefault("S3_KEY_OUT", "data/market-machine.json")
        need_cfg = (t0 < 120 or m0 < 512 or want != envv)
        if need_cfg:
            lam.update_function_configuration(
                FunctionName=FN, Timeout=max(t0, 120),
                MemorySize=max(m0, 512),
                Environment={"Variables": want})
            for _ in range(20):
                st = lam.get_function_configuration(FunctionName=FN)
                if st.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(5)
        cfg = lam.get_function_configuration(FunctionName=FN)
        misses += contract(r, "config",
                           cfg["Timeout"] >= 120
                           and cfg["MemorySize"] >= 512,
                           "timeout=%ss memory=%sMB env wired"
                           % (cfg["Timeout"], cfg["MemorySize"]))

        r.section("hourly schedule (shared scheduler role)")
        sched_ok = False
        try:
            sch.get_schedule(Name=FN)
            sched_ok = True
            r.log("schedule already exists")
        except Exception:
            try:
                sch.create_schedule(
                    Name=FN, ScheduleExpression="rate(1 hour)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": FARN, "RoleArn": ROLE})
                sched_ok = True
                r.log("created rate(1 hour) schedule")
            except Exception as e:
                r.warn("create_schedule: %s" % str(e)[:120])
        misses += contract(r, "schedule", sched_ok,
                           "hourly EventBridge schedule in place")

        r.section("invoke + four-pillar contracts")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        raw = inv["Payload"].read().decode("utf-8", "replace")
        ok = False
        try:
            body = json.loads(json.loads(raw).get("body") or "{}")
            ok = bool(body.get("ok"))
            r.kv(invoke_composite=body.get("composite"),
                 invoke_label=body.get("label"),
                 invoke_n=body.get("n_contributors"))
        except Exception as e:
            r.warn("parse: %s · %s" % (e, raw[:140]))
        misses += contract(r, "invoke",
                           inv.get("StatusCode") == 200 and ok,
                           "engine ok:true")

        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/market-machine.json")["Body"].read())
        misses += contract(r, "schema",
                           pl.get("schema_version") == "1.0",
                           "schema 1.0")
        found_map = {}
        for pid in ("profits", "rates", "flow", "forced"):
            p = (pl.get("pillars") or {}).get(pid) or {}
            n = p.get("n_contributors") or 0
            found_map[pid] = {"score": p.get("score"), "n": n,
                              "found": [x["name"] for x in
                                        p.get("contributors") or []]}
            misses += contract(r, "pillar-" + pid,
                               p.get("score") is not None and n >= 2,
                               "%s scoring with >=2 live contributors "
                               "(score=%s n=%d)"
                               % (pid, p.get("score"), n))
        comp = pl.get("composite_score")
        misses += contract(r, "composite",
                           comp is not None and 0 <= comp <= 100,
                           "composite %s (%s)"
                           % (comp, pl.get("composite_label")))
        misses += contract(r, "verdict",
                           bool(pl.get("machine_verdict")),
                           "verdict: %s"
                           % str(pl.get("machine_verdict"))[:140])
        r.kv(pillar_detail=json.dumps(found_map)[:900])

        r.section("purge + edge")
        zj, _ = cf("/zones?name=justhodl.ai")
        zid = (((zj or {}).get("result") or [{}])[0] or {}).get("id")
        if zid:
            pj, perr = cf("/zones/%s/purge_cache" % zid, "POST",
                          {"files": [
                              "https://justhodl.ai/market-machine.html",
                              "https://justhodl.ai/data/"
                              "market-machine.json"]})
            r.log("purge ok=%s err=%s"
                  % (bool((pj or {}).get("success")), perr))
        page_ok = payload_ok = False
        for att in range(10):
            try:
                pg = http_get("https://justhodl.ai/market-machine.html"
                              "?cb=%d" % time.time()
                              ).decode("utf-8", "replace")
                page_ok = "MARKET MACHINE" in pg and "doctrine" in pg
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/market-machine.json"
                    "?cb=%d" % time.time()))
                payload_ok = jd.get("schema_version") == "1.0"
                if page_ok and payload_ok:
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:80]))
            time.sleep(25)
        misses += contract(r, "edge-page", page_ok,
                           "market-machine.html serving")
        misses += contract(r, "edge-payload", payload_ok,
                           "market-machine.json serving schema 1.0")

        r.section("verdict")
        if misses:
            r.fail("market machine: %d red" % misses)
            sys.exit(1)
        r.ok("MARKET MACHINE LIVE — composite=%s (%s) · %s · hourly "
             "schedule set · https://justhodl.ai/market-machine.html"
             % (comp, pl.get("composite_label"),
                str(pl.get("machine_verdict"))[:160]))


if __name__ == "__main__":
    main()
