"""ops 4583 — wo4580 red-clearance re-verify.

4582 landed 46 green / 5 miss. Root causes fixed in four rev commits:
  rev-1/1b share-flows : missing timedelta import crashed the handler before
                         any S3 write (plus real timeout margin: 700s fetch
                         budget, 900s timeout)
  rev-2    grid-queue  : lowercase s3 vs module client S3 — NameError
                         swallowed; n_snapshots default 1 masqueraded as a
                         written archive (default now 0)
  rev-3    radar       : impact_map added (structural companies, measured
                         industry rollup)

This op: force the share-flows Timeout=900 config (deploy-lambdas ships
code; config drift is asserted here), settle the three, invoke async,
poll, re-assert exactly the missed contracts, and print the congress /
activist feed row shapes (the accum legs read zero rows — evidence for the
next extractor iteration, not a gate).
"""
import json
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

TARGETS = {
    "justhodl-share-flows": "data/share-flows.json",
    "justhodl-grid-queue": "data/grid-queue.json",
    "justhodl-accumulation-radar": "data/accumulation-radar.json",
}


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def key_exists_prefix(prefix):
    try:
        return s3.list_objects_v2(Bucket=B, Prefix=prefix,
                                  MaxKeys=3).get("KeyCount", 0) > 0
    except Exception:
        return False


def settle(r, fn, deadline_s=300):
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("LastUpdateStatus") == "Successful" \
                    and c.get("State") == "Active":
                return c
        except Exception:
            pass
        time.sleep(6)
    r.warn("  %s did not settle in %ss" % (fn, deadline_s))
    return {}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def valid_impact(j):
    m = (j or {}).get("impact_map")
    if not (isinstance(m, dict) and m.get("schema") == "impact-map/1.0"
            and isinstance(m.get("benefiting"), list)
            and isinstance(m.get("suffering"), list)):
        return False, "impact_map absent or malformed"
    for side in ("benefiting", "suffering"):
        for row in m[side]:
            if row.get("pp_kind") == "estimated" and (
                    row.get("ci") is None or row.get("n_obs") is None):
                return False, "NAKED estimated pp in %s" % side
    return True, "impact-map/1.0 valid (%d ben / %d suf / %d insufficient)" % (
        len(m["benefiting"]), len(m["suffering"]),
        len(m.get("insufficient") or []))


def main():
    with report("4583_wo4580_reds") as r:
        r.heading("ops 4583 — wo4580 red clearance")
        misses = 0

        r.section("0. Feed-shape recon: the dead accum legs (evidence only)")
        cg = get_json("data/congress-direct.json") or {}
        for side in ("house", "senate"):
            node = cg.get(side)
            rows = node if isinstance(node, list) else \
                ((node or {}).get("trades") or (node or {}).get("transactions")
                 or (node or {}).get("rows") or (node or {}).get("items") or [])
            if isinstance(node, dict) and not rows:
                r.log("  congress.%s dict keys: %s" % (side,
                                                       sorted(node)[:12]))
            samp = rows[0] if isinstance(rows, list) and rows else None
            r.log("  congress.%s rows=%s sample_keys=%s"
                  % (side, len(rows) if isinstance(rows, list) else "?",
                     sorted(samp) if isinstance(samp, dict) else None))
        av = get_json("data/activist-13d.json") or {}
        setups = av.get("all_setups") or av.get("top_setups") or []
        if isinstance(setups, dict):
            setups = list(setups.values())
        r.log("  activist all_setups=%d sample_keys=%s"
              % (len(setups),
                 sorted(setups[0]) if setups and isinstance(setups[0], dict)
                 else None))

        r.section("1. Config truth: share-flows Timeout must be 900")
        c = settle(r, "justhodl-share-flows")
        if c.get("Timeout") != 900:
            r.log("  live Timeout=%s — applying 900" % c.get("Timeout"))
            lam.update_function_configuration(
                FunctionName="justhodl-share-flows", Timeout=900)
            settle(r, "justhodl-share-flows")
        c = lam.get_function(FunctionName="justhodl-share-flows")[
            "Configuration"]
        misses += contract(r, "share-flows", c.get("Timeout") == 900,
                           "live Timeout=%s" % c.get("Timeout"))

        r.section("2. Settle the other two")
        for fn in ("justhodl-grid-queue", "justhodl-accumulation-radar"):
            settle(r, fn)

        r.section("3. Fire + poll")
        before = {}
        for fn, key in TARGETS.items():
            j = get_json(key) or {}
            before[fn] = j.get("generated_at") or j.get("as_of") or ""
            try:
                lam.invoke(FunctionName=fn, InvocationType="Event")
                r.log("  fired %s" % fn)
            except Exception as e:
                misses += contract(r, fn, False,
                                   "invoke: %s" % str(e)[:100])
        outs, pending, t0 = {}, dict(TARGETS), time.time()
        while pending and time.time() - t0 < 940:
            time.sleep(12)
            for fn in list(pending):
                cur = get_json(pending[fn])
                ts = (cur or {}).get("generated_at") or \
                     (cur or {}).get("as_of") or ""
                if cur is not None and ts and ts != before[fn]:
                    outs[fn] = cur
                    r.log("  %s refreshed (%ss)" % (fn,
                                                    int(time.time() - t0)))
                    del pending[fn]
        for fn in pending:
            r.warn("  %s did not refresh in 940s" % fn)
            outs[fn] = get_json(pending[fn]) or {}
            misses += 1

        r.section("4. Re-assert the five 4582 misses")
        j = outs.get("justhodl-share-flows") or {}
        misses += contract(r, "share-flows", j.get("version") == "2.0.0",
                           "v2.0.0 live (was v1.4 after the crash)")
        bd = j.get("boards") or {}
        misses += contract(r, "share-flows",
                           all(k in bd for k in ("buyback_bluff",
                                                 "atm_shelves_active",
                                                 "buyback_blackout_weeks")),
                           "bluff/ATM/blackout boards (bluff=%d atm=%d "
                           "weeks=%d)"
                           % (len(bd.get("buyback_bluff") or []),
                              len(bd.get("atm_shelves_active") or []),
                              len(bd.get("buyback_blackout_weeks") or [])))
        ok, why = valid_impact(j)
        misses += contract(r, "share-flows", ok, why)
        r.log("  share-flows warns tail: %s" % (j.get("warns") or [])[-4:])

        j = outs.get("justhodl-grid-queue") or {}
        qv = j.get("queue_velocity") or {}
        arch = key_exists_prefix("data/archive/grid-queue/")
        misses += contract(r, "grid-queue",
                           arch and (qv.get("n_snapshots") or 0) >= 1,
                           "archive object exists AND payload agrees "
                           "(n_snapshots=%s)" % qv.get("n_snapshots"))

        j = outs.get("justhodl-accumulation-radar") or {}
        ok, why = valid_impact(j)
        misses += contract(r, "radar", ok, why)

        r.section("verdict")
        if misses:
            r.fail("red clearance: %d still red" % misses)
            sys.exit(1)
        r.ok("wo4580 verification: ALL GREEN — 4582's 46 greens + these 5 "
             "cleared = full impact-layer contract set holds")


if __name__ == "__main__":
    main()
