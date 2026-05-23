"""ops 1076 — inspect portfolio schema for concentration-liquidity patch."""
import json, os, boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def fetch(s3, key):
    try:
        r = s3.get_object(Bucket=BUCKET, Key=key)
        body = r["Body"].read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
            return {"key": key, "size": len(body), "top_keys": list(parsed.keys()) if isinstance(parsed, dict) else f"array len={len(parsed)}", "sample": json.dumps(parsed, default=str)[:1500]}
        except Exception as e:
            return {"key": key, "parse_err": str(e), "first_500": body[:500]}
    except Exception as e:
        return {"key": key, "err": str(e)[:120]}


def main():
    s3 = boto3.client("s3", region_name=REGION)
    keys = [
        "portfolio/signal-portfolio-state.json",
        "portfolio/sizing.json",
        "portfolio/snapshot.json",
        "portfolio/state.json",
        "data/firm-book.json",
        "data/firm-stress.json",
    ]
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "files": [fetch(s3, k) for k in keys]}
    out = os.path.join(REPO_ROOT, "aws", "ops", "reports", "1076.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
