"""ops 6760 - READ-ONLY probe of the iShares 2026-11-05 splits in the Polygon EOD bank and in the strength desk's 1.0.9 artifact
(MTUM / QUAL 4:1, ITA / SOXX / EFG 3:1, EFV 2:1; the 1.0.9 rerun of ops 6757 still showed QUAL -75.87% on the day while
MTUM / SOXX / ITA were rebased). Prints, for each fund: the bank's last 8 rows (date, close, shares traded), the median
shares of the 20 sessions before the split, the close ratio, the split-day shares vs that median, and what the live
artifact's row carries (h.d, strength, splits block). Nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import math
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402
import polygon_bankfirst as PB  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)
FUNDS = ["MTUM", "QUAL", "ITA", "SOXX", "EFG", "EFV", "SPY"]
COMMON = (2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0, 25.0, 50.0, 1.5, 0.5, 1 / 3, 0.25, 0.2, 0.125, 0.1, 2 / 3, 0.05, 0.04, 0.02)


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6760_fresh_split_probe") as r:
        r.heading("ops 6760 - fresh-split probe (iShares 2026-11-05) %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        art = json.loads(s3.get_object(Bucket=LIVE, Key="data/strength.json")["Body"].read())
        by = {x["ticker"]: x for x in art.get("rows") or []}
        r.log("artifact version %s as_of %s | splits block %s" % (art.get("version"), art.get("as_of"), json.dumps(art.get("splits"))[:300]))
        for t in FUNDS:
            try:
                raw = s3.get_object(Bucket=LIVE, Key=PB.ROOT + t + ".json.gz")["Body"].read()
            except Exception as e:  # noqa: BLE001
                r.warn("%s: no bank file (%s)" % (t, type(e).__name__))
                continue
            rows = [x for x in json.loads(gzip.decompress(raw)) if isinstance(x, list) and x and x[0] is not None]
            tail = rows[-8:]
            r.section("%s - bank last %d rows" % (t, len(tail)))
            for x in tail:
                r.log("  %s close %10.4f shares %14.0f" % (PB._row_date(x[0]).isoformat(), float(x[4] or 0), float(x[5] or 0)))
            if len(rows) >= 22:
                pc, c = float(rows[-2][4] or 0), float(rows[-1][4] or 0)
                ratio = c / pc if pc else None
                med = sorted(float(x[5] or 0) for x in rows[-21:-1])[10]
                v_day = float(rows[-1][5] or 0)
                snap = min(COMMON, key=lambda k: abs(math.log(k) - math.log(ratio))) if ratio else None
                off = abs(math.log(snap) - math.log(ratio)) if ratio else None
                vr = (med / v_day) if v_day else None
                r.log("  close ratio %s -> nearest common %s (%.1f%% off) | split-day shares / 20-session median = %.2fx (vr %.3f) | bounded |log vr - log r| = %.2f (limit ln 2.5 = 0.92) | direction %s" % (
                    "%.4f" % ratio if ratio else "-", "%.4f" % snap if snap else "-", 100 * (math.exp(off) - 1) if off is not None else -1, (v_day / med) if med else -1, vr or -1,
                    abs(math.log(vr) - math.log(ratio)) if (vr and ratio) else -1, "ok" if (vr and ratio and ((vr < 1) if ratio < 1 else (vr > 1))) else "FAILS"))
            x = by.get(t) or {}
            r.log("  artifact row: strength %s h.d %s asset_class %s kind %s splits %s" % (x.get("strength"), (x.get("h") or {}).get("d"), x.get("asset_class"), x.get("kind"), json.dumps(x.get("splits"))[:300]))
        r.ok("ops 6760 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
