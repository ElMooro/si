"""
ops 1075 — locate real portfolio.json key + test FMP news endpoints for news-wire.

concentration-liquidity expected data/portfolio.json but got NoSuchKey.
news-wire saw 404 on FMP /stable/general-news.

This op:
  1. Lists S3 keys matching "portfolio" to find the real path
  2. Tests FMP /stable/news endpoints variants to find the working one
"""
import json, os, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def list_portfolio_keys(s3):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for prefix in ("data/portfolio", "portfolio", "data/positions"):
        try:
            for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix, MaxKeys=50):
                for obj in page.get("Contents", []):
                    keys.append({
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                    })
        except Exception as e:
            keys.append({"prefix": prefix, "err": str(e)[:120]})
    return keys


def test_fmp(endpoint):
    url = f"https://financialmodelingprep.com{endpoint}?apikey={FMP_KEY}&limit=3"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-recon"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="replace")
            status = r.status
            try:
                parsed = json.loads(body)
                count = len(parsed) if isinstance(parsed, list) else "object"
                sample = parsed[:1] if isinstance(parsed, list) else parsed
                return {"endpoint": endpoint, "status": status, "count": count,
                        "sample_keys": list(sample[0].keys()) if isinstance(sample, list) and sample else None,
                        "first_500": body[:500]}
            except Exception as je:
                return {"endpoint": endpoint, "status": status, "parse_err": str(je)[:120], "first_500": body[:500]}
    except urllib.error.HTTPError as e:
        return {"endpoint": endpoint, "status": e.code, "err": str(e)[:120]}
    except Exception as e:
        return {"endpoint": endpoint, "err": str(e)[:200]}


def main():
    s3 = boto3.client("s3", region_name=REGION)
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    report["portfolio_keys"] = list_portfolio_keys(s3)

    # Try several FMP news endpoint variants
    candidates = [
        "/stable/news/general-latest",
        "/stable/news/stock-latest",
        "/stable/news/press-releases-latest",
        "/stable/news/forex-latest",
        "/stable/fmp-articles",
        "/stable/general-news",          # the one that 404'd
        "/stable/stock-news",
        "/api/v3/general_news",          # old endpoints (should be dead)
    ]
    report["fmp_news_tests"] = [test_fmp(e) for e in candidates]

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws", "ops", "reports", "1075.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:3000])


if __name__ == "__main__":
    main()
