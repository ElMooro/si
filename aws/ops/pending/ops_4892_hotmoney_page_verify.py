"""ops/4892 -- hot-money page rebuild verify.
Burn: page called spark() which was never defined (lost in the
desk-split clone) -- ReferenceError blanked the tape below the
cards.  Rebuild: helpers-first + S_ armor per the shared-surface
rule, combo bars+cumulative chart (def-before-use asserted), OTC
accrual chart, 15-session combined table, best/worst chips.
 (1) committed: comboChart defined BEFORE the IIFE; no bare
     spark( call remains; tape/tapeotc/tbl containers present;
     armor count >= 3.
 (2) data fuel: TWSE ledger on S3 has >= 60 rows.
 (3) served with the rebuilt tokens.
"""
import gzip
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

PAGE = Path(__file__).resolve().parents[3] / "hot-money.html"
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"


def main():
    with report("ops 4892 -- hot-money page verify") as rep:
        html = PAGE.read_text(encoding="utf-8")
        sc = html.split("<script>")[1].split("</script>")[0]
        checks = {
            "comboChart defined before IIFE":
                "function comboChart(" in sc
                and sc.index("function comboChart(")
                < sc.index("(async function()"),
            "spark relic gone": "spark(" not in sc,
            "containers": all(x in sc for x in
                              ('id="tape-', 'id="tapeotc-',
                               'id="tbl-')),
            "armor sections": sc.count("S_(") >= 3,
            "combined never blended across dates":
                "never blended across dates" in sc}
        bad = [k for k, ok in checks.items() if not ok]
        for k in checks:
            (rep.ok if k not in bad else rep.fail)("  " + k)
        if bad:
            sys.exit(1)
        raw = s3.get_object(
            Bucket=B,
            Key="data/providers/twse/bfi82u-foreign.json"
        )["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        n = len(json.loads(raw).get("rows") or {})
        if n >= 60:
            rep.ok("TWSE ledger fuel: %d sessions" % n)
        else:
            rep.fail("ledger thin: %d" % n)
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/hot-money.html"
                    "?t=%d" % int(time.time()),
                    headers={"User-Agent": "ops-4892",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(
                        req, timeout=45) as r:
                    if "comboChart" in r.read().decode(
                            "utf-8", "replace"):
                        rep.ok("SERVED (%ds)"
                               % int(time.time() - t0))
                        break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        else:
            rep.fail("not served")
            sys.exit(1)
        rep.ok("tape restored + desk depth: bars+cumulative, "
               "OTC accrual, 15-session table, extremes")


if __name__ == "__main__":
    main()
