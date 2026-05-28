"""ops 1101 — diagnose justhodl.ai slow load.

index.html fetches 12 JSON files directly from S3 origin (no CDN) with
cache:'no-cache' on every page load. Measure each file's size + latency
to find what ballooned.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
s3 = boto3.client("s3", region_name=REGION)

FILES = {
    "intel": "intelligence-report.json",
    "edge": "edge-data.json",
    "liq": "liquidity-data.json",
    "flow": "flow-data.json",
    "crypto": "crypto-intel.json",
    "regime": "regime/current.json",
    "div": "divergence/current.json",
    "cot": "cot/extremes/current.json",
    "risk": "risk/recommendations.json",
    "setups": "opportunities/asymmetric-equity.json",
    "pnl": "portfolio/pnl-daily.json",
    "signalBoard": "signal-board.json",
}

S3_PUBLIC = "https://justhodl-dashboard-live.s3.amazonaws.com"


def main():
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "files": {}}
    total_bytes = 0
    slow_files = []
    big_files = []

    for key, path in FILES.items():
        entry = {"key": path}
        # 1. S3 metadata (size + last modified)
        try:
            o = s3.head_object(Bucket=BUCKET, Key=path)
            size = o["ContentLength"]
            entry["size_kb"] = round(size / 1024, 1)
            entry["size_mb"] = round(size / 1024 / 1024, 2)
            entry["last_modified"] = o["LastModified"].isoformat()
            entry["content_type"] = o.get("ContentType")
            entry["cache_control"] = o.get("CacheControl", "NONE")
            total_bytes += size
            if size > 500_000:
                big_files.append((path, entry["size_mb"]))
        except Exception as e:
            entry["head_err"] = str(e)[:150]
            entry["MISSING"] = True
            slow_files.append((path, "MISSING/404"))

        # 2. Time a public GET (simulates browser fetch from S3 origin)
        try:
            url = f"{S3_PUBLIC}/{path}"
            t0 = time.time()
            req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=30) as r:
                body = r.read()
            elapsed = time.time() - t0
            entry["fetch_seconds"] = round(elapsed, 3)
            entry["fetch_ok"] = True
            if elapsed > 1.0:
                slow_files.append((path, f"{round(elapsed,2)}s"))
        except Exception as e:
            entry["fetch_err"] = str(e)[:150]
            entry["fetch_ok"] = False

        report["files"][key] = entry

    report["total_size_mb"] = round(total_bytes / 1024 / 1024, 2)
    report["total_size_kb"] = round(total_bytes / 1024, 1)
    report["big_files_over_500kb"] = sorted(big_files, key=lambda x: -x[1])
    report["slow_or_missing"] = slow_files

    # Diagnosis
    diag = []
    if report["total_size_mb"] > 3:
        diag.append(f"Total payload {report['total_size_mb']}MB is heavy for a homepage — every visit re-downloads this (cache:'no-cache').")
    if big_files:
        diag.append(f"{len(big_files)} file(s) over 500KB: {[f[0] for f in big_files]}")
    if slow_files:
        diag.append(f"{len(slow_files)} file(s) slow/missing: {slow_files}")
    if not diag:
        diag.append("All 12 files reasonable size + fast. Slowness may be GitHub Pages, DNS, fonts, or a non-index page.")
    report["diagnosis"] = diag

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1101.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("=" * 60)
    print("JUSTHODL.AI LOAD DIAGNOSIS")
    print("=" * 60)
    print(f"Total homepage data payload: {report['total_size_mb']} MB ({report['total_size_kb']} KB)")
    print(f"\nPer-file:")
    for key, e in sorted(report["files"].items(), key=lambda x: -(x[1].get("size_kb") or 0)):
        flag = " ⚠" if (e.get("size_kb") or 0) > 500 or (e.get("fetch_seconds") or 0) > 1 else ""
        print(f"  {e['key']:42s} {e.get('size_kb','?'):>8} KB  {e.get('fetch_seconds','?')}s{flag}")
    print(f"\nDIAGNOSIS:")
    for d in report["diagnosis"]:
        print(f"  • {d}")


if __name__ == "__main__":
    main()
