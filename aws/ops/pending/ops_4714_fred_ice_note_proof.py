"""ops 4714 — prove the FRED card shows the ICE annotation WITHOUT
the FRED count changing (Khalid: extra datasets under FRED, never
overwriting). Captures the FRED provider's series count + size BEFORE
kicking provider-catalog, kicks it, captures AFTER, and hard-fails if
they differ by even one series.
"""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key, dflt=None):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:
        return dflt if dflt is not None else {}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def find_fred(hub):
    for p in hub.get("providers") or []:
        if p.get("slug") == "fred":
            return p
    return {}


def main():
    with report("4714_fred_ice_note_proof") as r:
        r.heading("ops 4714 — additive ICE note on FRED card, "
                  "FRED count PROVEN unchanged")
        misses = 0

        r.section("1. BEFORE snapshot")
        hub0 = gj("data/provider-catalog.json", {})
        fred0 = find_fred(hub0)
        r.log("  FRED before: n_keys=%s total_mb=%s note=%r"
             % (fred0.get("n_keys"), fred0.get("total_mb"),
                fred0.get("coverage_note")))
        misses += contract(r, "before", bool(fred0),
                           "FRED entry found in current hub")

        r.section("2. Kick provider-catalog")
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog",
                     InvocationType="Event")
        except Exception as e:
            r.warn("  kick: %s" % str(e)[:90])

        r.section("3. AFTER snapshot — wait for the new note to "
                  "appear")
        hub1, note1, t0 = {}, None, time.time()
        while time.time() - t0 < 240:
            time.sleep(15)
            hub1 = gj("data/provider-catalog.json", {})
            fred1 = find_fred(hub1)
            note1 = fred1.get("coverage_note")
            if note1 and "ICE BofA" in str(note1):
                break
        r.log("  FRED after: n_keys=%s total_mb=%s note=%r"
             % (fred1.get("n_keys"), fred1.get("total_mb"), note1))

        r.section("4. THE PROOF — count identical, ICE note present")
        misses += contract(
            r, "unchanged", fred0.get("n_keys") == fred1.get("n_keys"),
            "FRED n_keys IDENTICAL before/after (%s == %s)"
            % (fred0.get("n_keys"), fred1.get("n_keys")))
        misses += contract(
            r, "unchanged",
            fred0.get("total_mb") == fred1.get("total_mb"),
            "FRED total_mb IDENTICAL before/after (%s == %s)"
            % (fred0.get("total_mb"), fred1.get("total_mb")))
        misses += contract(
            r, "additive", note1 and "ICE BofA" in str(note1)
            and "cross-validated" in str(note1),
            "FRED note now contains the additive ICE annotation: %r"
            % note1)
        misses += contract(
            r, "additive", note1 and "scoped import" in str(note1),
            "the ORIGINAL fred note content is still present (this "
            "was an append, not a replace)")

        r.section("verdict")
        if misses:
            r.fail("fred-ice-note: %d red" % misses)
            sys.exit(1)
        r.ok("PROVEN: FRED card's own numbers are byte-identical "
            "before/after, and the ICE-via-TE annotation is live as "
            "a pure addition — %r" % note1)


if __name__ == "__main__":
    main()
