"""ops_4066 — purge junk server-side + fresh real-only delta."""
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
JUNK_RX = re.compile(r"^[a-z0-9_]{3,}$")


def main():
    with report("4066_real_check") as rep:
        rep.heading("ops 4065 — junk purge + real snapshot")
        sr = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-sources.json")["Body"].read())
        m = sr.get("sources") or {}
        real = {k: v for k, v in m.items()
                if not JUNK_RX.match(str(v.get("source") or ""))}
        rep.kv(before=len(m), real=len(real), purged=len(m) - len(real))
        diag = sr.get("last_harvest_diag") or {}
        rep.kv(diag=json.dumps(diag)[:280])
        by = Counter(str(v.get("source")) for v in real.values())
        rep.section("REAL sources so far")
        for src, n in by.most_common(25):
            rep.log(f"  {n:4d}  {src[:70]}")
        rep.ok(f"{len(real)} REAL / {len(m)-len(real)} junk-shaped in store")


if __name__ == "__main__":
    main()
