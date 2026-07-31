"""ops_4150 — CFTC store anatomy, correct key: cftc-all-cache.json."""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("4150_cftc_anatomy") as rep:
        rep.heading("ops 4150 — cftc-all-cache anatomy")
        c = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cftc-all-cache.json")["Body"].read())
        rep.kv(top_keys=json.dumps(list(c)[:10])[:240])
        inner = None
        for k in list(c):
            if isinstance(c[k], dict) and len(c[k]) > 20:
                inner = c[k]
                rep.kv(container=k, n=len(inner))
                break
        if inner is None and isinstance(c, dict):
            inner = c
        ks = list(inner)[:12]
        rep.log("  code sample: " + json.dumps(ks)[:300])
        v0 = inner[ks[0]]
        rep.log("  record keys: " + json.dumps(
            list(v0)[:14] if isinstance(v0, dict) else str(v0)[:120])[:280])
        if isinstance(v0, dict):
            rep.log("  record sample: " + json.dumps(v0)[:300])
        wl = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tv-watchlists.json")["Body"].read())
        bares = set()
        for l in (wl.get("lists") or []):
            for sy in l.get("symbols") or []:
                sy = str(sy)
                if sy.split(":")[0] in ("COT", "COT3"):
                    bares.add(sy.split(":", 1)[1])
        up = {b.upper() for b in bares}
        hits = [k for k in inner
                if str(k) in bares or str(k).upper() in up]
        rep.kv(cot_bares=len(bares), direct_overlap=len(hits))
        rep.log("  bare sample: " + json.dumps(sorted(bares)[:10])[:250])
        rep.log("  hit sample: " + json.dumps(hits[:8])[:200])
        rep.ok(f"ANATOMY — {len(inner)} codes, overlap {len(hits)}")


if __name__ == "__main__":
    main()
