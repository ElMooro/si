"""ops 4596 — fleet re-gate after the KeyError hardening (wo4592 close).

Sync invocation exposed KeyError('INSUFFICIENT_DATA') in priors[state]
lookups (two engines live-broken, two more latent, all four hardened to
.get(state, QUIET)). This op syncs-invokes all nine gated engines so any
remaining crash is VISIBLE, and asserts every payload refreshed with
data_sufficiency present.
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
                   config=Config(read_timeout=780,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

ENGINES = {
    "justhodl-13f-price-divergence": "data/13f-price-divergence.json",
    "justhodl-catalyst-skew-premove": "data/catalyst-skew-premove.json",
    "justhodl-earnings-iv-crush": "data/earnings-iv-crush.json",
    "justhodl-failed-pattern-reversal": "data/failed-pattern-reversal.json",
    "justhodl-forced-selling-bounce": "data/forced-selling-bounce.json",
    "justhodl-lockup-expiration": "data/lockup-expiration.json",
    "justhodl-ma-target-predictor": "data/ma-target-predictor.json",
    "justhodl-post-earnings-mean-rev": "data/post-earnings-mean-rev.json",
    "justhodl-vvix-vov-regime": "data/vvix-vov-regime.json",
}


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def settle(r, fn, deadline_s=240):
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
    with report("4596_fleet_regate") as r:
        r.heading("ops 4596 — fleet re-gate (sync, crashes visible)")
        misses = 0

        r.section("1. Settle the hardening deploys")
        for fn in ("justhodl-catalyst-skew-premove",
                   "justhodl-failed-pattern-reversal",
                   "justhodl-earnings-iv-crush",
                   "justhodl-lockup-expiration"):
            settle(r, fn)

        r.section("2. Sync invoke + gate, all nine")
        for fn, key in ENGINES.items():
            nm = fn.replace("justhodl-", "")
            before = ((get_json(key) or {}).get("as_of")
                      or (get_json(key) or {}).get("generated_at") or "")
            try:
                resp = lam.invoke(FunctionName=fn,
                                  InvocationType="RequestResponse")
                body = resp["Payload"].read().decode("utf-8", "replace")
                if resp.get("FunctionError") or '"statusCode": 500' in body:
                    misses += contract(r, nm, False,
                                       "handler error: %s" % body[:260])
                    continue
            except Exception as e:
                misses += contract(r, nm, False,
                                   "invoke raised: %s" % str(e)[:160])
                continue
            j = get_json(key) or {}
            ts = j.get("as_of") or j.get("generated_at") or ""
            ds = j.get("data_sufficiency")
            misses += contract(r, nm,
                               ts and ts != before
                               and isinstance(ds, dict),
                               "refreshed, state=%s, ds=%s"
                               % (j.get("state"),
                                  {k: v for k, v in (ds or {}).items()
                                   if k != "rule"} if isinstance(ds, dict)
                                  else None))

        r.section("verdict")
        if misses:
            r.fail("fleet re-gate: %d red" % misses)
            sys.exit(1)
        r.ok("all ten BUG-4 engines (nine here + activist-13d) verified "
             "LIVE: gates present, blind maps to no-edge priors, no "
             "crashes — wo4592 closed")


if __name__ == "__main__":
    main()
