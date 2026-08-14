"""ops 4661 — depth ledger live: state -> catalog -> card.

Closes the 5MB confusion at its display root: nyfed engine now keeps a
depth ledger (backfilled once from stored docs), provider-catalog lifts
it through, provider.html renders MEAN DEPTH. This op kicks both
engines and contracts the whole chain on live payloads.
"""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
SLUG = "nyfed"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=90,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return {}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def settle(fn, cap=300):
    t0 = time.time()
    while time.time() - t0 < cap:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if (c.get("State") == "Active"
                    and c.get("LastUpdateStatus") == "Successful"):
                return True
        except Exception:
            pass
        time.sleep(8)
    return False


def main():
    with report("4661_depth_ledger") as r:
        r.heading("ops 4661 — depth ledger: state -> catalog -> card")
        misses = 0
        fst = gj("data/_state/fred-scoped-import.json")
        r.log("fred guard (untouched): imported=%s status=%s"
              % (fst.get("series_imported"), fst.get("status")))

        r.section("1. Settle deploys, kick nyfed")
        r.log("  nyfed settled=%s catalog settled=%s"
              % (settle("justhodl-nyfed-markets-full"),
                 settle("justhodl-provider-catalog")))
        st0 = gj("data/warm/nyfed-markets/pd-state.json")
        a0 = st0.get("as_of")
        r.log("  before: done=%d depth=%s"
              % (len(st0.get("done") or []),
                 (st0.get("depth") or {}).get("keys")))
        try:
            lam.invoke(FunctionName="justhodl-nyfed-markets-full",
                       InvocationType="Event")
        except Exception as e:
            r.warn("  kick failed: %s" % str(e)[:80])
        st = st0
        t1 = time.time()
        while time.time() - t1 < 660:
            time.sleep(30)
            st = gj("data/warm/nyfed-markets/pd-state.json")
            if st.get("as_of") != a0 and st.get("depth"):
                break
        dep = st.get("depth") or {}
        done_n = len(set(st.get("done") or []))
        mean = (dep.get("n_obs_sum", 0)
                / max(1, dep.get("keys", 0)))
        r.log("  after %.0fs: done=%d depth=%s"
              % (time.time() - t1, done_n, dep))
        misses += contract(r, "ledger", bool(dep.get("keys")),
                           "depth ledger present (keys=%s)"
                           % dep.get("keys"))
        misses += contract(r, "ledger", dep.get("keys") == done_n,
                           "ledger keys %s == done %d (backfill + "
                           "incremental consistent)"
                           % (dep.get("keys"), done_n))
        misses += contract(r, "ledger", mean >= 150,
                           "mean n_obs %.0f (shallow era ~110)" % mean)
        misses += contract(r, "ledger",
                           dep.get("multi", 0)
                           >= 0.95 * max(1, dep.get("keys", 0)),
                           "multi-break %s/%s" % (dep.get("multi"),
                                                  dep.get("keys")))
        misses += contract(r, "ledger",
                           str(dep.get("first_min") or "9999")
                           <= "2016",
                           "earliest first %s (full lineage reached)"
                           % dep.get("first_min"))

        r.section("2. Kick provider-catalog, contract the card payload")
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog",
                       InvocationType="Event")
        except Exception as e:
            r.warn("  kick failed: %s" % str(e)[:80])
        pay, t2 = {}, time.time()
        while time.time() - t2 < 300:
            time.sleep(20)
            pay = gj("data/providers/%s.json" % SLUG)
            sd = ((pay.get("series") or {}).get("depth") or {})
            if sd.get("keys") == dep.get("keys"):
                break
        sd = ((pay.get("series") or {}).get("depth") or {})
        r.log("  card payload depth: %s" % sd)
        misses += contract(r, "card", bool(sd.get("keys")),
                           "depth reached the provider payload "
                           "(keys=%s)" % sd.get("keys"))
        if sd.get("keys"):
            r.log("  chip renders: '%d obs · MEAN DEPTH · since %s'"
                  % (round(sd["n_obs_sum"] / sd["keys"]),
                     str(sd.get("first_min") or "—")[:4]))
        rem = max(0, 1539 - done_n)
        r.log("  convergence: %d done, %d remaining ≈ %.0f h"
              % (done_n, rem, rem / 140.0))

        r.section("verdict")
        if misses:
            r.fail("depth ledger: %d red" % misses)
            sys.exit(1)
        r.ok("depth is now a card metric — bytes demoted to what they "
             "are; ledger self-maintains through convergence and "
             "beyond")


if __name__ == "__main__":
    main()
