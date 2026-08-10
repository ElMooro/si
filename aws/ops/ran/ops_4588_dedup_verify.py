"""ops 4588 — duplication-audit reconciliation verification (wo4585).

Khalid's directive: check what I build against what the fleet already
owns. The audit found three re-derivations in share-flows v2 and one in
impact-graph; rev-G/rev-H reconciled them to canonical sources. Verdicts
on the rest of the audit (no code needed):
  chokepoint            — equity criticality, DISTINCT from maritime
  concentration-liquidity — portfolio risk, DISTINCT from passive-ownership
  convergence-radar     — engine-count alerting, DISTINCT from trade impulse
  ATM tracking          — sec-filings-intel owns it (rev-G consumes)
  buyback bluff READ    — novel (built ON scanner's announcement data now)
  venue fingerprint, seasonal port baseline, wrapper netting,
  bps-of-ADV, distribution mirror — no prior art found; stand as built.

This op invokes share-flows + impact-graph and asserts the reconciled
contracts.
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

TARGETS = {"justhodl-share-flows": "data/share-flows.json",
           "justhodl-impact-graph": "data/impact/exposure-graph.json"}


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
    with report("4588_dedup_verify") as r:
        r.heading("ops 4588 — duplication-audit reconciliation")
        misses = 0

        r.section("1. Settle + fire + poll")
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

        r.section("2. share-flows v2.1 — canonical sources")
        j = outs.get("justhodl-share-flows") or {}
        misses += contract(r, "share-flows", j.get("version") == "2.1.0",
                           "v2.1.0 live")
        bd = j.get("boards") or {}
        blk = bd.get("buyback_blackout") or {}
        misses += contract(r, "share-flows",
                           blk.get("source") == "justhodl-earnings-blackout",
                           "blackout CARRIED from canonical (state=%s, "
                           "now=%s%%)"
                           % ((blk.get("now") or {}).get("state")
                              or blk.get("status"),
                              (blk.get("now") or {}).get("blackout_mktcap_pct")))
        misses += contract(r, "share-flows",
                           "buyback_blackout_weeks" not in bd,
                           "local weekly re-derivation retired")
        crash = [w for w in (j.get("warns") or [])
                 if "V2_TAIL_CRASH" in str(w)]
        misses += contract(r, "share-flows", not crash,
                           "v2 tail clean" if not crash
                           else "shield fired: %s" % crash[0][:220])
        im = j.get("impact_map") or {}
        bn = str(im.get("basis_note") or "")
        misses += contract(r, "share-flows",
                           "scanner" in bn or "FALLBACK" in bn,
                           "announcement source named: %s" % bn[:160])
        r.log("  boards: bluff=%d backed=%d atm=%d"
              % (len(bd.get("buyback_bluff") or []),
                 len(bd.get("buyback_backed") or []),
                 len(bd.get("atm_shelves_active") or [])))

        r.section("3. impact-graph v1.1 — flow board as industry lens")
        conv = get_json("data/impact/convergence.json") or {}
        fl = conv.get("flow_convergence") or {}
        misses += contract(r, "impact-graph",
                           conv.get("version") == "1.1",
                           "v1.1 live")
        misses += contract(r, "impact-graph",
                           bool(fl.get("source")),
                           "flow board names its source: %s"
                           % str(fl.get("source"))[:100])
        r.log("  flow rows=%d; trade_impulse=%s"
              % (len(fl.get("rows") or []),
                 (conv.get("trade_impulse") or {}).get("state")))

        r.section("verdict")
        if misses:
            r.fail("dedup reconciliation: %d red" % misses)
            sys.exit(1)
        r.ok("duplication audit reconciled — one computation per concept; "
             "novel work (bluff read, venue fingerprint, seasonal port "
             "baseline, wrapper netting, bps-of-ADV, distribution mirror, "
             "impact contract) stands")


if __name__ == "__main__":
    main()
