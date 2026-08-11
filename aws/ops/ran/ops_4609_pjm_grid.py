"""ops 4609 — PJM GRID: Khalid's Data Miner 2 key goes live.

New engine justhodl-pjm-grid pulls PJM RTO instantaneous load (8d →
demand momentum, the AI-datacenter thesis read), 7-day forecast peak,
generation fuel mix, and RTO-aggregate RT LMP with the house
day-over-day shock doctrine. Key lives ONLY as env PJM_API_KEY (GitHub
secret, sealed-box — never in the repo; the FRED-key lesson applied).

This op: settle, inject PJM_API_KEY into the Lambda env from the
runner secret, config floor, hourly schedule, invoke, real-data
contracts, purge, edge asserts.
"""
import io
import json
import os
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-pjm-grid"
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4609"})
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
    with report("4609_pjm_grid") as r:
        r.heading("ops 4609 — PJM GRID engine (Data Miner 2)")

        r.section("runner secret present")
        pk = os.environ.get("PJM_API_KEY", "")
        misses += contract(r, "secret", bool(pk),
                           "PJM_API_KEY present in runner env "
                           "(len=%d)" % len(pk))
        if not pk:
            r.fail("secret missing — run-ops.yml env patch not live?")
            sys.exit(1)

        r.section("deploy-settle (new function)")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "justhodl-pjm-grid v1.0.0" in src:
                    settled = True
                    r.log("v1.0.0 live (attempt %d)" % (att + 1))
                    break
            except lam.exceptions.ResourceNotFoundException:
                r.log("attempt %d: not created yet" % (att + 1))
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:90]))
            time.sleep(30)
        misses += contract(r, "deploy", settled, "function live v1.0.0")
        if not settled:
            sys.exit(1)

        r.section("env inject + config floor")
        cfg = lam.get_function_configuration(FunctionName=FN)
        envv = (cfg.get("Environment") or {}).get("Variables") or {}
        envv["PJM_API_KEY"] = pk
        envv.setdefault("S3_BUCKET", B)
        envv.setdefault("S3_KEY_OUT", "data/pjm-grid.json")
        lam.update_function_configuration(
            FunctionName=FN, Timeout=max(cfg["Timeout"], 180),
            MemorySize=max(cfg["MemorySize"], 1024),
            Environment={"Variables": envv})
        for _ in range(20):
            st = lam.get_function_configuration(FunctionName=FN)
            if st.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(5)
        cfg = lam.get_function_configuration(FunctionName=FN)
        has_key = "PJM_API_KEY" in ((cfg.get("Environment") or {})
                                    .get("Variables") or {})
        misses += contract(r, "config",
                           has_key and cfg["Timeout"] >= 180
                           and cfg["MemorySize"] >= 1024,
                           "key injected, timeout=%ss memory=%sMB"
                           % (cfg["Timeout"], cfg["MemorySize"]))

        r.section("hourly schedule")
        sched_ok = False
        try:
            sch.get_schedule(Name=FN)
            sched_ok = True
            r.log("schedule exists")
        except Exception:
            try:
                sch.create_schedule(
                    Name=FN, ScheduleExpression="rate(1 hour)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": FARN, "RoleArn": ROLE})
                sched_ok = True
                r.log("created rate(1 hour)")
            except Exception as e:
                r.warn("create_schedule: %s" % str(e)[:110])
        misses += contract(r, "schedule", sched_ok, "hourly schedule set")

        r.section("invoke + real-data contracts")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        raw = inv["Payload"].read().decode("utf-8", "replace")
        body = {}
        try:
            body = json.loads(json.loads(raw).get("body") or "{}")
        except Exception:
            r.warn("parse: %s" % raw[:150])
        r.kv(invoke=json.dumps(body)[:280])
        misses += contract(r, "invoke",
                           inv.get("StatusCode") == 200
                           and bool(body.get("ok")),
                           "engine ok:true")

        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/pjm-grid.json")["Body"].read())
        ld, lm = pl.get("load") or {}, pl.get("lmp") or {}
        misses += contract(r, "load",
                           isinstance(ld.get("current_gw"),
                                      (int, float))
                           and (ld.get("n_obs") or 0) >= 200,
                           "RTO load live: %s GW from %s obs"
                           % (ld.get("current_gw"), ld.get("n_obs")))
        misses += contract(r, "momentum",
                           ld.get("momentum_8d_pct") is not None,
                           "8-day demand momentum computed: %s%%"
                           % ld.get("momentum_8d_pct"))
        misses += contract(r, "lmp",
                           isinstance(lm.get("daily_avg"),
                                      (int, float))
                           and lm.get("shock_state") in
                           ("CALM", "AMBER", "RED"),
                           "RT LMP live: $%s/MWh daily avg, DoD %s%%, "
                           "shock=%s" % (lm.get("daily_avg"),
                                         lm.get("daily_avg_dod_pct"),
                                         lm.get("shock_state")))
        fm = (pl.get("fuel_mix") or {}).get("shares_pct") or {}
        misses += contract(r, "fuel", len(fm) >= 3,
                           "fuel mix live with %d fuels (top: %s)"
                           % (len(fm),
                              json.dumps(dict(list(fm.items())[:3]))))
        r.kv(canaries=json.dumps(pl.get("canaries") or {})[:250],
             ai_read=str(pl.get("ai_demand_read"))[:160])

        r.section("purge + edge")
        zj, _ = cf("/zones?name=justhodl.ai")
        zid = (((zj or {}).get("result") or [{}])[0] or {}).get("id")
        if zid:
            pj, perr = cf("/zones/%s/purge_cache" % zid, "POST",
                          {"files": [
                              "https://justhodl.ai/pjm-grid.html",
                              "https://justhodl.ai/data/"
                              "pjm-grid.json"]})
            r.log("purge ok=%s err=%s"
                  % (bool((pj or {}).get("success")), perr))
        page_ok = payload_ok = False
        for att in range(10):
            try:
                pg = http_get("https://justhodl.ai/pjm-grid.html?cb=%d"
                              % time.time()).decode("utf-8", "replace")
                page_ok = "PJM GRID" in pg
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/pjm-grid.json?cb=%d"
                    % time.time()))
                payload_ok = jd.get("schema_version") == "1.0"
                if page_ok and payload_ok:
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(25)
        misses += contract(r, "edge-page", page_ok, "pjm-grid.html live")
        misses += contract(r, "edge-payload", payload_ok,
                           "pjm-grid.json serving schema 1.0")

        r.section("verdict")
        if misses:
            r.fail("pjm grid: %d red" % misses)
            sys.exit(1)
        r.ok("PJM GRID LIVE — load %s GW, momentum %s%%, LMP $%s "
             "(shock %s), %d fuels · hourly · "
             "https://justhodl.ai/pjm-grid.html"
             % (ld.get("current_gw"), ld.get("momentum_8d_pct"),
                lm.get("daily_avg"), lm.get("shock_state"), len(fm)))


if __name__ == "__main__":
    main()
