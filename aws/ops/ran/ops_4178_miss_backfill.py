"""ops_4178 — miss-bare backfill: historical full-key misses copied to
bare keys so the honest-label wave sees them; vault refired."""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("4178_miss_backfill") as rep:
        rep.heading("ops 4178 — bare-miss backfill")
        d = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/symbol-feed.json")["Body"].read())
        p = d.get("prices") or {}
        added = 0
        for k in list(p):
            v = p[k]
            if ":" in k and isinstance(v, dict) and v.get("miss"):
                bare = k.split(":", 1)[1]
                if bare not in p:
                    p[bare] = {"miss": True}
                    added += 1
        d["prices"] = p
        s3.put_object(Bucket=BUCKET, Key="data/symbol-feed.json",
                      Body=json.dumps(d).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=600")
        rep.kv(bare_misses_added=added, total_keys=len(p))
        if added < 200:
            rep.fail(f"only {added} backfilled")
            sys.exit(1)
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok(f"BACKFILLED {added} — vault refired")


if __name__ == "__main__":
    main()
