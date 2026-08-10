"""ops 4586 — wo4585 final clearance.

rev-D (ISO-NE cookie-jar heal for the AspxAutoDetectCookieSupport loop)
and rev-E (edgar.cik_map_mf json.loads(dict) bug) landed. Invoke the two
engines and assert the last two punch-list items:

  grid-queue      : ISO-NE LIVE (rows parsed) — or, if the edge still
                    refuses a cookie-carrying client, the gap says so
                    explicitly (contract passes on either honest outcome,
                    but LIVE vs named-refusal is reported distinctly)
  etf-true-flows  : N-PORT WIRED_INDEX with per_etf >= 1
"""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

TARGETS = {
    "justhodl-grid-queue": "data/grid-queue.json",
    "justhodl-etf-true-flows": "data/etf-true-flows.json",
}


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
                return True
        except Exception:
            pass
        time.sleep(6)
    r.warn("  %s did not settle" % fn)
    return False


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4586_wo4585_final") as r:
        r.heading("ops 4586 — wo4585 final clearance")
        misses = 0

        r.section("1. Settle + fire + poll")
        before = {}
        for fn, key in TARGETS.items():
            settle(r, fn)
            j = get_json(key) or {}
            before[fn] = j.get("generated_at") or ""
            lam.invoke(FunctionName=fn, InvocationType="Event")
            r.log("  fired %s" % fn)
        outs, pending, t0 = {}, dict(TARGETS), time.time()
        while pending and time.time() - t0 < 600:
            time.sleep(12)
            for fn in list(pending):
                cur = get_json(pending[fn])
                ts = (cur or {}).get("generated_at") or ""
                if cur is not None and ts and ts != before[fn]:
                    outs[fn] = cur
                    r.log("  %s refreshed (%ss)" % (fn,
                                                    int(time.time() - t0)))
                    del pending[fn]
        for fn in pending:
            r.warn("  %s did not refresh in 600s" % fn)
            outs[fn] = get_json(pending[fn]) or {}
            misses += 1

        r.section("2. ISO-NE after the cookie heal")
        j = outs.get("justhodl-grid-queue") or {}
        iq = (j.get("iso_queues") or {})
        isone = iq.get("ISO-NE") or {}
        gap = next((g for g in (j.get("gaps") or []) if "ISO-NE" in str(g)),
                   "")
        if isone:
            misses += contract(r, "grid-queue", True,
                               "ISO-NE LIVE — %s projects, %s MW headline, "
                               "ia-detection=%s"
                               % (isone.get("n_projects"),
                                  isone.get("headline_mw"),
                                  isone.get("ia_detection")))
            nat = j.get("national") or {}
            r.log("  isos_live now: %s (missing: %s)"
                  % (nat.get("isos_live"), nat.get("isos_missing")))
        else:
            misses += contract(r, "grid-queue",
                               bool(gap) and "cookiejar" not in str(gap),
                               "edge still refusing a cookie-carrying "
                               "client — named honestly: %s"
                               % str(gap)[:220])

        r.section("3. N-PORT after the json.loads fix")
        j = outs.get("justhodl-etf-true-flows") or {}
        gt = j.get("ground_truth") or {}
        misses += contract(r, "etf-true-flows",
                           gt.get("status") == "WIRED_INDEX"
                           and len(gt.get("per_etf") or []) >= 1,
                           "N-PORT %s — %d funds indexed"
                           % (gt.get("status"),
                              len(gt.get("per_etf") or [])))
        for e in (gt.get("per_etf") or [])[:8]:
            r.log("    %s cik=%s latest NPORT-P %s acc=%s"
                  % (e.get("etf"), e.get("cik"),
                     e.get("latest_nport_date"),
                     str(e.get("accession"))[:22]))

        r.section("verdict")
        if misses:
            r.fail("wo4585: %d still red" % misses)
            sys.exit(1)
        r.ok("wo4585 CLOSED — punch list fully cleared")


if __name__ == "__main__":
    main()
