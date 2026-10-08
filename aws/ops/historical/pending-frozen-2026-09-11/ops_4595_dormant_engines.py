"""ops 4595 — dormant-engine revival + FRED key-path evidence.

4594 proved: (a) rate ceiling was already 100 but AIMD sits at 42/min
under 261 429s — the leaked FRED key's stranger traffic is eating the
budget; the real lever is key rotation (Khalid). (b) catalyst-skew +
failed-pattern invokes vanish with ZERO logs and zero errors — the
reserved-concurrency-0 signature. This op:

  1. prints each engine's reserved concurrency + payload age (dormancy
     evidence), clears any zero reservation, and syncs-invokes so a
     crash would be VISIBLE, then asserts the BUG-4 gate
  2. prints where fred-catalog reads its key, so the rotation
     instruction to Khalid is exact
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
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

REDS = {
    "justhodl-catalyst-skew-premove": "data/catalyst-skew-premove.json",
    "justhodl-failed-pattern-reversal": "data/failed-pattern-reversal.json",
}


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4595_dormant_engines") as r:
        r.heading("ops 4595 — dormant-engine revival")
        misses = 0
        now = datetime.now(timezone.utc)

        r.section("1. FRED key path (for the rotation instruction)")
        cfg = lam.get_function(
            FunctionName="justhodl-fred-catalog")["Configuration"]
        envkeys = sorted((cfg.get("Environment") or {})
                         .get("Variables") or {})
        r.log("  fred-catalog env vars: %s" % envkeys)
        r.log("  → rotation: new key goes into this function's FRED_KEY "
              "env var (I flip it the moment Khalid provides it)")

        r.section("2. Concurrency evidence + revival")
        for fn, key in REDS.items():
            nm = fn.replace("justhodl-", "")
            try:
                cc = lam.get_function_concurrency(FunctionName=fn)
                rc = cc.get("ReservedConcurrentExecutions")
            except Exception as e:
                rc = "err:%s" % str(e)[:60]
            j = get_json(key) or {}
            asof = j.get("as_of") or j.get("generated_at") or "?"
            r.log("  %s reserved_concurrency=%s payload_as_of=%s"
                  % (nm, rc, asof))
            if rc == 0:
                lam.delete_function_concurrency(FunctionName=fn)
                r.ok("  [%s] zero reservation CLEARED — the silent-invoke "
                     "mystery (Events aged out unlogged)" % nm)
            elif isinstance(rc, int):
                r.log("  %s reservation %s left as-is" % (nm, rc))

        r.section("3. Sync invoke + gate (crashes would be visible now)")
        for fn, key in REDS.items():
            nm = fn.replace("justhodl-", "")
            try:
                resp = lam.invoke(FunctionName=fn,
                                  InvocationType="RequestResponse")
                body = resp["Payload"].read().decode("utf-8", "replace")
                if resp.get("FunctionError"):
                    misses += contract(r, nm, False,
                                       "sync invoke FunctionError: %s"
                                       % body[:300])
                    continue
                r.log("  %s sync ok: %s" % (nm, body[:140]))
            except Exception as e:
                misses += contract(r, nm, False,
                                   "sync invoke raised: %s" % str(e)[:200])
                continue
            j = get_json(key) or {}
            ds = j.get("data_sufficiency")
            misses += contract(r, nm, isinstance(ds, dict),
                               "data_sufficiency published (state=%s, "
                               "as_of=%s, ds=%s)"
                               % (j.get("state"),
                                  j.get("as_of") or j.get("generated_at"),
                                  {k: v for k, v in (ds or {}).items()
                                   if k != "rule"} if isinstance(ds, dict)
                                  else None))

        r.section("verdict")
        if misses:
            r.fail("revival: %d red" % misses)
            sys.exit(1)
        r.ok("both engines revived and gated — all ten BUG-4 gates now "
             "verified live; FRED speed now waits only on the key "
             "rotation")


if __name__ == "__main__":
    main()
