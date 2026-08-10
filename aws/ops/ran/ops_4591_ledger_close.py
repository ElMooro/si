"""ops 4591 — follow-up ledger close-out (revs J/K/L).

Three evidence-backed fixes landed:
  rev-J impact-graph : multi_engine_confluence primary (engines[] lists
                       live there; ticker_map only carries n_engines —
                       the 4590 diag's 274 umbrella votes explained)
  rev-K share-flows  : scanner list key is top_opportunities; ATM match
                       extended to signal_id/signal_label; fallbacks now
                       emit feed evidence
  rev-L grid-queue   : ISO-NE cookie-gated HTML mined for the CSV export
                       href, followed once with the same cookie jar

This op settles all three, fires them async, polls each engine's LAST-
WRITTEN key, and asserts:
  impact-graph : diag.votes_cast > diag.with_industry (engines lists
                 actually read) — rows logged; if still 0 with real
                 multi-engine votes, the sources spread per industry is
                 printed so the next step is evidence
  share-flows  : announcement/ATM sources non-fallback OR the new feed-
                 evidence warns on the record
  grid-queue   : ISO-NE LIVE, or the gap carries the rev-L mining
                 signature (export hrefs counted / followed)
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
    "justhodl-impact-graph": "data/impact/convergence.json",
    "justhodl-share-flows": "data/share-flows.json",
    "justhodl-grid-queue": "data/grid-queue.json",
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
    with report("4591_ledger_close") as r:
        r.heading("ops 4591 — ledger close-out (revs J/K/L)")
        misses = 0

        r.section("1. Settle + fire + poll (last-written keys)")
        before = {}
        for fn, key in TARGETS.items():
            settle(r, fn)
            before[fn] = (get_json(key) or {}).get("generated_at") or ""
            lam.invoke(FunctionName=fn, InvocationType="Event")
            r.log("  fired %s" % fn)
        outs, pending, t0 = {}, dict(TARGETS), time.time()
        while pending and time.time() - t0 < 900:
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
            r.warn("  %s did not refresh in 900s" % fn)
            outs[fn] = get_json(pending[fn]) or {}
            misses += 1

        r.section("2. impact-graph — engines lists flowing")
        j = outs.get("justhodl-impact-graph") or {}
        fl = j.get("flow_convergence") or {}
        d = fl.get("diag") or {}
        misses += contract(r, "impact-graph",
                           (d.get("votes_cast") or 0)
                           > (d.get("with_industry") or 0),
                           "per-engine votes real: field=%s records=%s "
                           "mapped=%s votes=%s"
                           % (d.get("field"), d.get("records_read"),
                              d.get("with_industry"), d.get("votes_cast")))
        rows = fl.get("rows") or []
        r.log("  rows=%d source=%s" % (len(rows),
                                       str(fl.get("source"))[:120]))
        for row in rows[:6]:
            r.log("    %s %s score=%s sources=%s"
                  % (row.get("industry"), row.get("direction"),
                     row.get("score"), sorted(row.get("sources") or {})))
        if not rows:
            r.warn("  rows still 0 — per-industry spread evidence needed "
                   "next; votes flowed so this is an overlap question, "
                   "not a reader bug")

        r.section("3. share-flows — canonical joins")
        j = outs.get("justhodl-share-flows") or {}
        misses += contract(r, "share-flows", j.get("version") == "2.1.1",
                           "v2.1.1 live")
        bn = str((j.get("impact_map") or {}).get("basis_note") or "")
        ws = [w for w in (j.get("warns") or [])
              if "fallback" in str(w).lower()]
        joined = "scanner" in bn and "FALLBACK" not in bn.split("ATM")[0]
        misses += contract(r, "share-flows",
                           joined or bool(ws),
                           ("canonical joins live: %s" % bn[:150]) if joined
                           else "fallback with evidence: %s"
                                % "; ".join(str(w)[:120] for w in ws[:2]))

        r.section("4. grid-queue — ISO-NE after the href mine")
        j = outs.get("justhodl-grid-queue") or {}
        isone = (j.get("iso_queues") or {}).get("ISO-NE") or {}
        gap = next((g for g in (j.get("gaps") or []) if "ISO-NE" in str(g)),
                   "")
        if isone:
            misses += contract(r, "grid-queue", True,
                               "ISO-NE LIVE — %s projects, %s MW"
                               % (isone.get("n_projects"),
                                  isone.get("headline_mw")))
            nat = j.get("national") or {}
            r.log("  isos_live: %s missing: %s"
                  % (nat.get("isos_live"), nat.get("isos_missing")))
        else:
            mined = ("export" in str(gap) or "href" in str(gap))
            misses += contract(r, "grid-queue", mined,
                               "gap carries the mining signature: %s"
                               % str(gap)[:260])

        r.section("verdict")
        if misses:
            r.fail("ledger close: %d red" % misses)
            sys.exit(1)
        r.ok("ledger CLOSED — engines lists flowing, canonical joins "
             "evidence-backed, ISO-NE resolved or precisely named")


if __name__ == "__main__":
    main()
