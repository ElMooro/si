"""ops_4065 — purge junk server-side + fresh real-only delta."""
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
    with report("4065_server_purge") as rep:
        rep.heading("ops 4065 — junk purge + real snapshot")
        sr = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-sources.json")["Body"].read())
        m = sr.get("sources") or {}
        real = {k: v for k, v in m.items()
                if not JUNK_RX.match(str(v.get("source") or ""))}
        rep.kv(before=len(m), real=len(real), purged=len(m) - len(real))
        sr["sources"] = real
        sr["n_symbols"] = len(real)
        sr["generated_at"] = datetime.now(timezone.utc).isoformat()
        s3.put_object(Bucket=BUCKET, Key="data/tv-sources.json",
                      Body=json.dumps(sr), ContentType="application/json",
                      CacheControl="max-age=120")
        by = Counter(str(v.get("source")) for v in real.values())
        rep.section("REAL sources so far")
        for src, n in by.most_common(25):
            rep.log(f"  {n:4d}  {src[:70]}")
        rep.ok(f"PURGED {len(m)-len(real)} junk; {len(real)} real remain — "
               f"re-harvest with v1.7.6 refills both channels clean")


if __name__ == "__main__":
    main()
