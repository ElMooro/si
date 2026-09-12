"""ops_5413 -- Polygon ticker news (paid plan).

GET /v2/reference/news?ticker= per name, limit 5. SSM key.
Writes data/polygon-news.json.
"""
from __future__ import annotations

import json
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"
SSM = "/justhodl/polygon/api-key"
NAMES = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "JPM", "XOM"]
PATH = "/v2/reference/news"
HOSTS = ["https://api.polygon.io", "https://api.massive.com"]
CTX = ssl.create_default_context()


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/5413"})
    with urllib.request.urlopen(req, timeout=30, context=CTX) as r:
        return r.getcode(), json.loads(r.read().decode("utf-8"))


def main():
    with report("ops_5413_polygon_news") as R:
        R.heading("ops 5413 -- Polygon news")
        s3 = boto3.client("s3", region_name=REGION)
        key = boto3.client("ssm", region_name=REGION).get_parameter(
            Name=SSM, WithDecryption=True)["Parameter"]["Value"]
        if not key:
            R.fail("empty SSM")
            sys.exit(1)
        R.ok("SSM present")
        by = {}
        host_used = None
        for name in NAMES:
            items = []
            err = None
            for host in HOSTS:
                url = host + PATH + "?" + urllib.parse.urlencode({
                    "ticker": name, "limit": 5, "order": "desc", "apiKey": key,
                })
                try:
                    code, body = _get(url)
                except urllib.error.HTTPError as e:
                    err = "HTTP %s" % e.code
                    continue
                except Exception as e:
                    err = type(e).__name__
                    continue
                raw = body.get("results") or []
                for n in raw[:5]:
                    items.append({
                        "title": n.get("title"),
                        "published_utc": n.get("published_utc"),
                        "publisher": (n.get("publisher") or {}).get("name"),
                        "article_url": n.get("article_url"),
                    })
                host_used = host
                R.ok("%s HTTP %s n=%s" % (name, code, len(items)))
                break
            by[name] = {"n": len(items), "error": err if not items else None, "items": items}
            time.sleep(0.12)
        n_ok = sum(1 for v in by.values() if v["n"])
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5413",
            "endpoint": PATH,
            "host": host_used,
            "n_ok": n_ok,
            "status": "LIVE" if n_ok else "DATA_HOLD",
            "tickers": by,
        }
        s3.put_object(
            Bucket=B,
            Key="data/polygon-news.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/polygon-news.json")["Body"].read())
        if back.get("schema_version") != 1:
            R.fail("read-back")
            sys.exit(1)
        R.ok("GREEN -- news n_ok=%s status=%s" % (n_ok, payload["status"]))


if __name__ == "__main__":
    main()
