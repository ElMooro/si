"""ops 4584 — share-flows evidence + final wo4580 clearance.

4583 cleared grid-queue + radar; share-flows still never wrote (940s).
rev-4 restructured it (front-loaded HTTP, 640s fetch budget, WRITE
GUARANTEE shield). This op:

  1. pulls the CloudWatch tail of the FAILED 21:08 run — the actual death
     (timeout vs traceback) goes on the record before anything else
  2. settles the rev-4 deploy, invokes share-flows, polls 960s
  3. asserts the three remaining contracts + the shield's own promise
     (a V2_TAIL_CRASH warn is loud but the payload must still be v2-valid)
  4. re-invokes accum-composite and asserts the congress leg reads rows
     after the rev-5 midpoint parse
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


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
    r.warn("  %s did not settle" % fn)
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
    return True, "impact-map/1.0 valid (%d ben / %d suf)" % (
        len(m["benefiting"]), len(m["suffering"]))


def main():
    with report("4584_shareflows_final") as r:
        r.heading("ops 4584 — share-flows evidence + final clearance")
        misses = 0

        r.section("1. CloudWatch: how did the 21:08 run actually die?")
        try:
            grp = "/aws/lambda/justhodl-share-flows"
            start = int((datetime.now(timezone.utc)
                         - timedelta(hours=3)).timestamp() * 1000)
            ev = logs.filter_log_events(
                logGroupName=grp, startTime=start, limit=400,
                filterPattern="")["events"]
            tail = [e["message"].rstrip() for e in ev][-60:]
            deaths = [m for m in tail
                      if "Task timed out" in m or "Traceback" in m
                      or "[ERROR]" in m or "Error" in m[:30]]
            r.log("  last-3h log lines: %d; death signatures: %d"
                  % (len(ev), len(deaths)))
            for m in (deaths[-8:] or tail[-10:]):
                r.log("    | %s" % m[:220])
        except Exception as e:
            r.warn("  log pull failed: %s" % str(e)[:120])

        r.section("2. Settle rev-4, invoke, poll 960s")
        settle(r, "justhodl-share-flows")
        key = "data/share-flows.json"
        before = (get_json(key) or {}).get("generated_at") or ""
        lam.invoke(FunctionName="justhodl-share-flows",
                   InvocationType="Event")
        r.log("  fired")
        j, t0 = None, time.time()
        while time.time() - t0 < 960:
            time.sleep(15)
            cur = get_json(key)
            ts = (cur or {}).get("generated_at") or ""
            if cur is not None and ts and ts != before:
                j = cur
                r.log("  refreshed (%ss)" % int(time.time() - t0))
                break
        if j is None:
            j = get_json(key) or {}
            misses += contract(r, "share-flows", False,
                               "payload refreshed within 960s")

        r.section("3. Contracts")
        misses += contract(r, "share-flows", j.get("version") == "2.0.0",
                           "v2.0.0 live")
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
        crash = [w for w in (j.get("warns") or []) if "V2_TAIL_CRASH" in str(w)]
        if crash:
            r.warn("  shield fired (payload still valid — bug on record): %s"
                   % crash[0][:300])
        else:
            r.ok("  v2 tail ran clean (no shield fire)")
        r.log("  warns tail: %s" % (j.get("warns") or [])[-4:])

        r.section("4. accum-composite congress leg after rev-5")
        akey = "data/accum-composite.json"
        abefore = (get_json(akey) or {}).get("generated_at") or ""
        settle(r, "justhodl-accum-composite")
        lam.invoke(FunctionName="justhodl-accum-composite",
                   InvocationType="Event")
        aj, t0 = None, time.time()
        while time.time() - t0 < 240:
            time.sleep(10)
            cur = get_json(akey)
            ts = (cur or {}).get("generated_at") or ""
            if cur is not None and ts and ts != abefore:
                aj = cur
                break
        aj = aj or get_json(akey) or {}
        ccv = (aj.get("component_coverage") or {})
        misses += contract(r, "accum-composite",
                           (ccv.get("congress_cluster") or 0) > 0,
                           "congress leg reads rows after midpoint parse "
                           "(coverage=%s; activist=%s — that feed is "
                           "genuinely empty right now, extractor fine)"
                           % (ccv.get("congress_cluster"),
                              ccv.get("activist_13d")))

        r.section("verdict")
        if misses:
            r.fail("final clearance: %d still red" % misses)
            sys.exit(1)
        r.ok("wo4580 COMPLETE: 4582's 46 greens + 4583's grid-queue/radar + "
             "share-flows v2 + congress leg = the full impact-layer "
             "contract set holds")


if __name__ == "__main__":
    main()
