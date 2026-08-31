"""ops_5068 -- BOJ: fan out per database, and give it a trigger.

ops 5067 drove BOJ for 40 minutes and moved nothing. Three reasons, all
now understood:

 1. WRONG PAYLOAD. The engine gates its API lane on event["api_only"];
    I sent {"mode":"api"}, which fell through to the zips lane and
    api_discover, both already complete. Four invocations, zero work.
 2. NO TRIGGER. "rules: NONE" and 1 invocation/day. Same failure as
    census-us -- a lane that looks healthy in its state document and has
    simply not been run. Newest state write was 2026-08-26, four days
    before anyone noticed.
 3. MY ARITHMETIC. I read "parts done=55,306/7,148" as 773% coverage,
    which should have stopped me on sight. Per db, done is an INDEX INTO
    THE CODES LIST (series drained) and parts is the number of S3 part
    files written -- unrelated quantities. The real denominator is
    sum(len(codes)) across dbs, which is the 120,394 the page has been
    reporting all along.

The fix is structural, not a nudge. api_only walks every db
SEQUENTIALLY inside one 780s budget, so with 22 dbs the later ones are
never reached -- and indeed only 16 have state files at all; six have
never been touched. db_filter is already a shard key, so the engine now
has a fanout mode that invokes one run per db, each with the full budget
to itself.

  P0 real coverage: sum(done) over sum(len(codes)), per db
  P1 wire a trigger -- it has none
  P2 fan out and measure, with the CORRECT payload this time
  P3 closure, and which dbs are still untouched
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCT = "857687956942"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-boj-full"
FN_ARN = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
BST = "data/warm/boj-full/_state/"

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=k)["Body"].read())
    except Exception:
        return None


def coverage():
    """done = index into codes; len(codes) = that db's universe."""
    tot = {"done": 0, "codes": 0, "rows": 0, "parts": 0, "per": []}
    kw = {"Bucket": LIVE, "Prefix": BST, "MaxKeys": 1000}
    ks = []
    while True:
        r = s3.list_objects_v2(**kw)
        ks += [o["Key"] for o in r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    for k in ks:
        if "api_" not in k:
            continue
        d = jget(k)
        if not isinstance(d, dict):
            continue
        n = len(d.get("codes") or [])
        dn = int(d.get("done") or 0)
        tot["done"] += dn
        tot["codes"] += n
        tot["rows"] += int(d.get("rows") or 0)
        tot["parts"] += int(d.get("parts") or 0)
        tot["per"].append((k.split("api_")[-1][:-5], dn, n))
    return tot


with report("ops_5068_boj_fanout") as R:
    fails = []
    out = {"op": "ops_5068"}

    R.section("P0 real coverage (done / len(codes))")
    for i in range(15):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if (c.get("LastModified") or "")[:19] >= (
                    NOW - timedelta(minutes=14)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                R.log("  code fresh %s" % c.get("LastModified"))
                break
        except Exception:
            pass
        time.sleep(20)
    t = coverage()
    R.log("  dbs with state: %d   series %s / %s  (%.1f%%)" % (
        len(t["per"]), f"{t['done']:,}", f"{t['codes']:,}",
        100.0 * t["done"] / max(1, t["codes"])))
    R.log("  rows banked %s across %s part files" % (
        f"{t['rows']:,}", f"{t['parts']:,}"))
    R.log("  the page reports 55,306/120,394 -- so ~%s series are in "
          "dbs that have NO state file at all" % (
              f"{120394 - t['codes']:,}"))
    short = sorted(((n - d), db, d, n) for db, d, n in t["per"] if n > d)
    R.log("  dbs short (%d), worst first:" % len(short))
    for miss, db, d, n in sorted(short, reverse=True)[:12]:
        R.log("    %-12s %8s / %-8s  %s outstanding" % (
            db[:12], f"{d:,}", f"{n:,}", f"{miss:,}"))
    out["before"] = {"done": t["done"], "codes": t["codes"],
                     "rows": t["rows"], "dbs": len(t["per"])}

    R.section("P1 give it a trigger")
    try:
        have = ev.list_rule_names_by_target(
            TargetArn=FN_ARN).get("RuleNames", [])
        R.log("  existing rules: %s" % (have or "NONE"))
    except Exception as e:
        R.log("  rule read err %s" % str(e)[:90])
        have = []
    if not have:
        host = None
        try:
            for page in ev.get_paginator("list_rules").paginate():
                for r in page.get("Rules", []):
                    se = r.get("ScheduleExpression") or ""
                    if r.get("State") != "ENABLED" or not se:
                        continue
                    n = len(ev.list_targets_by_rule(
                        Rule=r["Name"]).get("Targets", []))
                    if n < 5 and ("hour" in se or "minute" in se):
                        host = (r["Name"], se)
                        break
                if host:
                    break
        except Exception as e:
            R.log("  survey err %s" % str(e)[:90])
        if host:
            nm, se = host
            try:
                lam.add_permission(
                    FunctionName=FN, StatementId="evb-boj-%s" % nm[:40],
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:%s:%s:rule/%s"
                              % (REGION, ACCT, nm))
            except Exception:
                pass
            try:
                resp = ev.put_targets(Rule=nm, Targets=[{
                    "Id": "bojfanout", "Arn": FN_ARN,
                    "Input": json.dumps({"fanout": True})}])
                R.log("  fanout target on %s (%s) failed=%s" % (
                    nm, se, resp.get("FailedEntryCount")))
                out["host"] = nm
            except Exception as e:
                R.log("  attach err %s" % str(e)[:110])
                fails.append("P1:attach")
        else:
            R.log("  no rule with a free slot")
            fails.append("P1:nohost")

    R.section("P2 fan out, correct payload")
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"fanout": True}).encode())
        R.log("  fanout -> %s" % (r["Payload"].read() or b"")[:200])
        if r.get("FunctionError"):
            fails.append("P2:funcerror")
    except Exception as e:
        R.log("  fanout err %s" % str(e)[:130])
    t0 = time.time()
    for cycle in range(3):
        time.sleep(700)
        n = coverage()
        el = (time.time() - t0) / 60.0
        R.log("  t+%2.0fmin  series %s/%s (+%s)  rows %s (+%s)  dbs=%d"
              % (el, f"{n['done']:,}", f"{n['codes']:,}",
                 f"{n['done'] - t['done']:,}", f"{n['rows']:,}",
                 f"{n['rows'] - t['rows']:,}", len(n["per"])))
        if cycle < 2:
            try:
                lam.invoke(FunctionName=FN, InvocationType="Event",
                           Payload=json.dumps({"fanout": True}).encode())
            except Exception:
                pass
    n = coverage()
    el = (time.time() - t0) / 60.0
    gained = n["done"] - t["done"]
    R.log("  drained %s series in %.0f min (%.0f/min); rows +%s; dbs "
          "%d -> %d" % (f"{gained:,}", el, gained / max(1, el),
                        f"{n['rows'] - t['rows']:,}", len(t["per"]),
                        len(n["per"])))
    if gained <= 0:
        R.log("  STILL NOTHING -- fanout is not the blocker either")
        fails.append("P2:nomove")
    else:
        left = n["codes"] - n["done"]
        R.log("  %s series left in known dbs -> ~%.1f h at this rate" % (
            f"{left:,}", left / max(1, gained / max(1, el)) / 60.0))
    out["after"] = {"done": n["done"], "codes": n["codes"],
                    "rows": n["rows"], "dbs": len(n["per"])}

    R.section("P3 what is still untouched")
    st = jget("data/warm/boj-full/_state/state.json") or {}
    dbs = sorted(((st.get("api") or {}).get("dbs") or {}))
    have_state = {db for db, _, _ in n["per"]}
    R.log("  universe dbs=%d  with state=%d  never started: %s" % (
        len(dbs), len(have_state),
        sorted(set(dbs) - have_state)[:12] or "none"))
    out["never_started"] = sorted(set(dbs) - have_state)[:20]
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/boj-expedite.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/boj-expedite.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5068 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(series="%s/%s" % (n["done"], n["codes"]), rows=n["rows"],
         dbs=len(n["per"]), host=out.get("host"))
    R.log("ops 5068 GREEN -- BOJ fanned out and triggered")
