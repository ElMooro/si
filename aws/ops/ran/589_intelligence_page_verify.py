#!/usr/bin/env python3
"""589 — Verify Intelligence dashboard page deploys + all 6 sidecars accessible
from S3 (CORS for browser fetch)."""
import io, json, os, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/589_intelligence_page_verify.json"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. Page renders from GitHub Pages
    page_url = "https://justhodl.ai/intelligence/"
    try:
        req = urllib.request.Request(page_url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", "replace")
            out["page_status"] = r.status
            out["page_size_kb"] = round(len(html)/1024, 1)
            markers = [
                "🧠 INTELLIGENCE",
                "Adaptive Khalid",
                "Stress Scenarios",
                "Political Trades",
                "Reversal Radar",
                "Auction Grades",
                "Repo &amp; Lending",
                "data/khalid-adaptive.json",
                "data/stress-scenarios.json",
                "data/political-trades.json",
                "data/reversal-radar.json",
                "data/auction-grades.json",
                "data/repo-lending.json",
            ]
            out["markers_found"] = {m: (m in html) for m in markers}
            out["all_markers_pass"] = all(out["markers_found"].values())
    except urllib.error.HTTPError as e:
        out["page_err"] = f"HTTP {e.code} {e.reason}"
    except Exception as e:
        out["page_err"] = str(e)[:200]

    # 2. Homepage nav link present
    try:
        req = urllib.request.Request("https://justhodl.ai/", headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            home = r.read().decode("utf-8", "replace")
            out["home_has_intelligence_link"] = "/intelligence/" in home and "🧠 INTELLIGENCE" in home
    except Exception as e:
        out["home_err"] = str(e)[:200]

    # 3. All 6 sidecars accessible from S3 with CORS-friendly headers
    sidecars = [
        "data/khalid-adaptive.json",
        "data/stress-scenarios.json",
        "data/political-trades.json",
        "data/reversal-radar.json",
        "data/auction-grades.json",
        "data/repo-lending.json",
    ]
    side_results = {}
    for key in sidecars:
        try:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            side_results[key] = {
                "exists": True,
                "size_kb": round(obj["ContentLength"]/1024, 1),
                "modified": obj["LastModified"].isoformat()[:19],
                "content_type": obj.get("ContentType"),
                "cache_control": obj.get("CacheControl"),
            }
        except Exception as e:
            side_results[key] = {"exists": False, "err": str(e)[:100]}
    out["sidecars"] = side_results

    # 4. CORS test — fetch one as a browser would
    cors_url = "https://justhodl-dashboard-live.s3.amazonaws.com/data/khalid-adaptive.json?cb=1"
    try:
        req = urllib.request.Request(cors_url,
            headers={"User-Agent": "Mozilla/5.0", "Origin": "https://justhodl.ai"})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read().decode("utf-8", "replace")
            out["cors_test"] = {
                "status": r.status,
                "size_kb": round(len(body)/1024, 1),
                "is_json": body.lstrip().startswith("{"),
                "headers_acl": r.headers.get("Access-Control-Allow-Origin"),
            }
    except Exception as e:
        out["cors_test_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
