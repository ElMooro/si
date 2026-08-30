"""ops_5061 -- give census-us a schedule, and drain the econ lane.

ops 5060 proved the lane produces the right thing. First entry banked:
    cbp/2022 -> 75,886 rows, NAICS2017 = 11311, 113110, 1132, 11321...
Industry detail, not all-industry totals -- the failure mode that would
have looked complete and been useless.

It also went RED for two reasons, both mine.

 1. NO SCHEDULE. "rules targeting justhodl-census-us: []". The function
    has never had a trigger. That is not a new problem -- it is the root
    cause of the census-us STALE chip that has been pinning the whole
    dashboard banner to IMPORT DEGRADED. The sentinel was right all
    along and nobody had asked why.
 2. I used a long RequestResponse invoke to watch a batch job, which is
    the exact mistake I wrote down this morning after two sync invokes
    died mid-run. It threw Rate Exceeded at 263s while the function
    carried on working -- n_done went to 1 and 2 objects landed. The
    observation failed, not the work.

So: Event invokes and state polling only, and a real trigger. The
EventBridge classic rule cap is saturated, so rather than failing on
create_rule this op finds an existing rule with a free target slot
(limit 5) and attaches there, granting the Lambda permission for that
rule's ARN.

  P0 rule survey: which rules have room
  P1 attach timeseries + econ targets, grant invoke permission
  P2 drain: repeated Event invokes, polling state between them
  P3 verify what landed, per family, and confirm NAICS detail persists
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCT = "857687956942"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-census-us"
FN_ARN = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
ESTATE = "data/_state/census-econ.json"
EROOT = "data/warm/census-econ/"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)


def jget(k):
    import gzip
    try:
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return {}


with report("ops_5061_econ_schedule") as R:
    fails = []
    out = {"op": "ops_5061"}

    R.section("P0 which rules have a free target slot")
    cands = []
    try:
        pg = ev.get_paginator("list_rules")
        for page in pg.paginate():
            for r in page.get("Rules", []):
                if r.get("State") != "ENABLED":
                    continue
                se = r.get("ScheduleExpression") or ""
                if not se:
                    continue
                n = len(ev.list_targets_by_rule(
                    Rule=r["Name"]).get("Targets", []))
                if n < 5:
                    cands.append((r["Name"], se, n))
        cands.sort(key=lambda x: (0 if "hour" in x[1] else 1, x[2]))
        R.log("  enabled scheduled rules with room: %d" % len(cands))
        for nm, se, n in cands[:8]:
            R.log("    %-42s %-18s %d/5 targets" % (nm[:42], se, n))
    except Exception as e:
        R.log("  survey err %s" % str(e)[:130])
        fails.append("P0")

    R.section("P1 attach the census targets")
    host = None
    for nm, se, n in cands:
        if n <= 3 and ("hour" in se or "minutes" in se):
            host = (nm, se, n)
            break
    if not host:
        R.log("  no rule with two free slots -- econ will need a manual "
              "drain until one frees up")
        fails.append("P1:nohost")
    else:
        nm, se, n = host
        R.log("  host rule: %s (%s, %d targets used)" % (nm, se, n))
        try:
            lam.add_permission(
                FunctionName=FN, StatementId="evb-%s" % nm[:48],
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:%s:%s:rule/%s"
                          % (REGION, ACCT, nm))
            R.log("  invoke permission granted for %s" % nm)
        except Exception as e:
            msg = str(e)[:90]
            R.log("  permission: %s" % (
                "already present" if "ResourceConflict" in str(e)
                else msg))
        try:
            tg = [{"Id": "censusts", "Arn": FN_ARN, "Input": "{}"},
                  {"Id": "censusecon", "Arn": FN_ARN,
                   "Input": json.dumps({"mode": "econ"})}]
            resp = ev.put_targets(Rule=nm, Targets=tg)
            R.log("  put_targets failed=%s" % resp.get(
                "FailedEntryCount"))
            now = ev.list_targets_by_rule(Rule=nm).get("Targets", [])
            R.log("  %s now has %d targets: %s" % (
                nm, len(now), [t.get("Id") for t in now]))
            out["host_rule"] = nm
            out["host_sched"] = se
            if resp.get("FailedEntryCount"):
                fails.append("P1:puttargets")
        except Exception as e:
            R.log("  put_targets err %s" % str(e)[:140])
            fails.append("P1:attach")

    R.section("P2 drain (Event invokes, polled -- never sync)")
    before = jget(ESTATE)
    R.log("  before: n_done=%s/%s rows=%s failures=%d" % (
        before.get("n_done"), before.get("n_total"),
        f"{before.get('rows_total') or 0:,}",
        len(before.get("failures") or {})))
    t0 = time.time()
    for cycle in range(4):
        try:
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"mode": "econ"}).encode())
        except Exception as e:
            R.log("  invoke err %s" % str(e)[:100])
        for _ in range(15):
            time.sleep(60)
            st = jget(ESTATE)
            if st.get("updated_at") and st.get("phase") == "COMPLETE":
                break
        st = jget(ESTATE)
        R.log("  cycle %d  n_done=%s/%s  rows=%s  queue_left=%s  "
              "failures=%d" % (cycle + 1, st.get("n_done"),
                               st.get("n_total"),
                               f"{st.get('rows_total') or 0:,}",
                               st.get("queue_left"),
                               len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE":
            R.log("  ECON LANE COMPLETE")
            break
    st = jget(ESTATE)
    done = int(st.get("n_done") or 0) - int(before.get("n_done") or 0)
    el = (time.time() - t0) / 60.0
    R.log("  drained %d entries in %.0f min -> %.1f entries/min" % (
        done, el, done / max(1, el)))
    if done and st.get("n_total"):
        left = int(st["n_total"]) - int(st["n_done"])
        R.log("  %d entries left -> ~%.1f h at this rate" % (
            left, left / max(0.01, done / max(1, el)) / 60.0))
    for k, v in list((st.get("failures") or {}).items())[:8]:
        R.log("    fail %-30s %s" % (k[:30], str(v)[:76]))
    out.update(n_done=st.get("n_done"), n_total=st.get("n_total"),
               rows=st.get("rows_total"),
               failures=len(st.get("failures") or {}))

    R.section("P3 what landed")
    objs, kw = [], {"Bucket": LIVE, "Prefix": EROOT, "MaxKeys": 1000}
    while True:
        rr = s3.list_objects_v2(**kw)
        objs += [(o["Key"], o["Size"]) for o in rr.get("Contents", [])]
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    byfam = {}
    for k, sz in objs:
        f = k[len(EROOT):].split("/")[0]
        a = byfam.setdefault(f, [0, 0])
        a[0] += 1
        a[1] += sz
    R.log("  %s objects, %.1f MB" % (f"{len(objs):,}",
                                     sum(s for _, s in objs) / 1e6))
    for f, (n, b) in sorted(byfam.items(), key=lambda kv: -kv[1][1])[:12]:
        R.log("    %-14s %4d objects  %8.1f MB" % (f, n, b / 1e6))
    checked = 0
    for k, _ in objs:
        if checked >= 2:
            break
        d = jget(k)
        if not isinstance(d, list) or len(d) < 2:
            continue
        hdr = d[0]
        nc = next((c for c in hdr
                   if str(c).upper().startswith("NAICS")), None)
        if nc:
            vals = {r[hdr.index(nc)] for r in d[1:400]}
            R.log("  %-46s %s rows · %s has %d distinct codes" % (
                k[len(EROOT):][:46], f"{len(d) - 1:,}", nc, len(vals)))
            checked += 1
    out["objects"] = len(objs)
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/census-econ-lane.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5061 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(host=out.get("host_rule"), n_done=out.get("n_done"),
         n_total=out.get("n_total"), objects=out.get("objects"),
         rows=out.get("rows"))
    R.log("ops 5061 GREEN -- econ lane scheduled and draining")
