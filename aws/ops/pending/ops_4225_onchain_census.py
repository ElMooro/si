"""ops_4225 — the on-chain bare census: print all 212 real names."""
import json
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
PREF = ("GLASSNODE", "INTOTHEBLOCK", "COINMETRICS", "CRYPTOQUANT")


def main():
    with report("4225_onchain_census") as rep:
        rep.heading("ops 4225 — on-chain bare census")
        wl = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/tv-watchlists.json")["Body"].read())
        by = {}
        for l in (wl.get("lists") or []):
            for sy in l.get("symbols") or []:
                sy = str(sy)
                p = sy.split(":")[0]
                if p in PREF:
                    by.setdefault(p, set()).add(sy.split(":", 1)[1])
        for p, bares in sorted(by.items()):
            rep.kv(**{p.lower() + "_n": len(bares)})
            bl = sorted(bares)
            for i in range(0, len(bl), 8):
                rep.log("  " + " | ".join(bl[i:i + 8]))
        rep.ok("CENSUS — "
               + json.dumps({p: len(b) for p, b in by.items()}))


if __name__ == "__main__":
    main()
