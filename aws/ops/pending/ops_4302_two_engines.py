"""
ops_4302 -- Khalid's two new engines, born verified.

  justhodl-treasury-rehypo   collateral re-use stress: FR2004 keyids
                             DISCOVERED from the NY Fed catalog (fails
                             + Singh-style velocity), OFR GCF-Tri
                             specialness, SOFR-IORB, RRP drain --
                             composite over present legs only.
  justhodl-trend-reversal    early-reversal ensemble on real closes
                             for the ladder + best-setups names, every
                             fired signal carrying its value.

Chain: ensure-create both (donor role) -> first runs -> REAL numbers
printed (rehypo legs w/ z + which keyids the catalog yielded; reversal
top scores w/ named signals) -> Schedulers -> desk v2.3.3 re-run:
rehypo on the risk panel, reversal chips on ladder/map, RRG exhaustive
fallback given one more chance, 32 sources.
"""
import io
import json
import os
import sys
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from ops_report import report

REGION, B = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

def ensure(fn, timeout_s, env=None):
    for _ in range(20):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < 20 * 60:
                    return "deployed"
                break
        except lam.exceptions.ResourceNotFoundException:
            break
        except Exception:
            pass
        time.sleep(8)
    try:
        buf = io.BytesIO()
        sdir = "aws/lambdas/%s/source" % fn
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _, files in os.walk(sdir):
                for f in files:
                    fp = os.path.join(root, f)
                    z.write(fp, os.path.relpath(fp, sdir))
        donor = lam.get_function_configuration(
            FunctionName="justhodl-quantum-desk")
        kd = lam.get_function_configuration(
            FunctionName="justhodl-commodity-curves")
        keyenv = {k: v for k, v in ((kd.get("Environment") or {})
                                    .get("Variables") or {}).items()
                  if k.startswith(("FMP", "FRED"))}
        env = {**keyenv, **(env or {})}
        try:
            lam.create_function(
                FunctionName=fn, Runtime="python3.12",
                Role=donor["Role"],
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": buf.getvalue()},
                Timeout=timeout_s, MemorySize=512,
                Environment={"Variables": env or {}},
                Architectures=["x86_64"])
        except Exception as ce:
            if "exists" in str(ce):
                lam.update_function_code(FunctionName=fn,
                                         ZipFile=buf.getvalue())
            else:
                raise
        for _ in range(30):
            if lam.get_function_configuration(
                    FunctionName=fn).get("State") == "Active":
                return "self-created"
            time.sleep(5)
    except Exception as e:
        return "FAIL:%s" % str(e)[:110]
    return "self-created"

def schedule(name, cron, fn):
    try:
        sch.get_schedule(Name=name, GroupName="default")
        return "present"
    except Exception:
        donor = None
        for pg in sch.get_paginator("list_schedules").paginate(
                GroupName="default"):
            for it in pg.get("Schedules", []):
                d = sch.get_schedule(Name=it["Name"],
                                     GroupName="default")
                if (d.get("Target") or {}).get("RoleArn"):
                    donor = d["Target"]["RoleArn"]
                    break
            if donor:
                break
        sch.create_schedule(
            Name=name, GroupName="default",
            ScheduleExpression=cron,
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": "arn:aws:lambda:us-east-1:857687956942:"
                           "function:%s" % fn,
                    "RoleArn": donor, "Input": "{}"})
        return "created"

fails = []
with report("4302_two_engines") as r:
    r.heading("ops 4302 -- rehypothecation + trend-reversal, live")

    r.section("1. treasury-rehypo")
    st = ensure("justhodl-treasury-rehypo", 180)
    r.log("function: %s" % st)
    if st.startswith("FAIL"):
        fails.append(st)
    else:
        p = lam.invoke(FunctionName="justhodl-treasury-rehypo",
                       InvocationType="RequestResponse", Payload=b"{}")
        pay = (p["Payload"].read() or b"")[:260].decode("utf-8",
                                                        "ignore")
        r.log("run: %s" % pay)
        doc = json.loads(s3.get_object(
            Bucket=B, Key="data/treasury-rehypo.json")["Body"].read())
        r.log("COMPOSITE %s (%s) · legs: %s · missing: %s"
              % (doc.get("composite"), doc.get("band"),
                 list((doc.get("legs") or {})),
                 doc.get("legs_missing")))
        for k, v in (doc.get("legs") or {}).items():
            r.kv(leg=k, latest=str(v.get("latest",
                 v.get("latest_bps")))[:14], z=v.get("z"),
                 n=v.get("n"), src=str(v.get("source"))[:42])
        pk = doc.get("picked_keyids") or {}
        r.log("catalog picks: %s"
              % {k: len(v or []) for k, v in pk.items()})
        if len(doc.get("legs") or {}) < 3:
            fails.append("rehypo legs %d < 3 (missing=%s)"
                         % (len(doc.get("legs") or {}),
                            doc.get("legs_missing")))
        r.log("schedule: %s" % schedule("treasury-rehypo-daily",
              "cron(40 21 ? * MON-FRI *)",
              "justhodl-treasury-rehypo"))

    r.section("2. trend-reversal")
    st = ensure("justhodl-trend-reversal", 240,
                {"MAX_NAMES": "14"})
    r.log("function: %s" % st)
    if st.startswith("FAIL"):
        fails.append(st)
    else:
        p = lam.invoke(FunctionName="justhodl-trend-reversal",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("run: %s"
              % (p["Payload"].read() or b"")[:200].decode("utf-8",
                                                          "ignore"))
        doc = json.loads(s3.get_object(
            Bucket=B, Key="data/trend-reversal.json")["Body"].read())
        r.log("universe %s · hot(>=30) %s · errors %s"
              % (doc.get("universe_n"), doc.get("hot_n"),
                 doc.get("errors")))
        for row in (doc.get("rows") or [])[:6]:
            sigs = ", ".join(s0["signal"]
                             for s0 in (row.get("signals") or [])[:4])
            r.kv(ticker=row.get("ticker"),
                 score=row.get("reversal_score"),
                 dir=row.get("direction"),
                 trend=row.get("prevailing_trend"), signals=sigs[:60])
        if (doc.get("universe_n") or 0) < 10:
            fails.append("reversal universe %s < 10"
                         % doc.get("universe_n"))
        r.log("schedule: %s" % schedule("trend-reversal-daily",
              "cron(50 20 ? * MON-FRI *)",
              "justhodl-trend-reversal"))

    r.section("3. desk v2.3.3 -- both wired + RRG retry")
    if not fails:
        ok = False
        for _ in range(50):
            try:
                c = lam.get_function_configuration(
                    FunctionName="justhodl-quantum-desk")
                if c.get("LastUpdateStatus") in (None, "Successful") \
                        and c.get("State") == "Active":
                    lm = datetime.strptime(
                        c["LastModified"].split(".")[0],
                        "%Y-%m-%dT%H:%M:%S").replace(
                        tzinfo=timezone.utc)
                    if lm >= RUN_START - __import__(
                            "datetime").timedelta(minutes=2):
                        ok = True
                        break
            except Exception:
                pass
            time.sleep(8)
        if not ok:
            fails.append("desk deploy window")
        else:
            lam.invoke(FunctionName="justhodl-quantum-desk",
                       InvocationType="RequestResponse", Payload=b"{}")
            d = json.loads(s3.get_object(
                Bucket=B, Key="data/quantum-desk.json")["Body"].read())
            dh = d.get("data_health") or {}
            r.log("version %s · sources %s/%s"
                  % (d.get("version"), dh.get("sources_ok"),
                     dh.get("sources_total")))
            rp = d.get("risk_panel") or {}
            r.log("rehypo panel: %s" % rp.get("rehypo"))
            lad = d.get("asset_ladder") or []
            r.log("reversal chips (ladder): %s"
                  % [(x["class"], x["reversal"]) for x in lad
                     if x.get("reversal")][:5])
            r.log("RRG: %s" % [(x["class"], x["rrg"]) for x in lad
                               if x.get("rrg")][:5])
            r.log("reversal chips (map): %s"
                  % [(m["ticker"], m["reversal"]) for m in
                     d.get("money_map") or []
                     if m.get("reversal")][:5])
            if d.get("version") != "2.3.3":
                fails.append("desk %s" % d.get("version"))
            if not rp.get("rehypo"):
                fails.append("rehypo absent from risk panel")
            if (dh.get("sources_ok") or 0) < 28:
                fails.append("sources_ok %s" % dh.get("sources_ok"))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4302 PASS -- both engines live, wired, scheduled")
if fails:
    sys.exit(1)
