"""ops_4157 — read the browser's confession: last_harvest_diag verbatim."""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("4157_harvest_confession") as rep:
        rep.heading("ops 4157 — last_harvest_diag verbatim")
        d = json.loads(s3.get_object(
            Bucket="justhodl-dashboard-live",
            Key="data/tv-sources.json")["Body"].read())
        rep.kv(top_keys=json.dumps(list(d)[:10])[:200])
        diag = d.get("last_harvest_diag")
        if diag is None:
            rep.fail("no last_harvest_diag stored")
            sys.exit(1)
        t = json.dumps(diag, ensure_ascii=False)
        rep.kv(diag_bytes=len(t))
        for i in range(0, min(len(t), 1600), 200):
            rep.log("  " + t[i:i + 200])
        rep.ok("CONFESSION READ")


if __name__ == "__main__":
    main()
