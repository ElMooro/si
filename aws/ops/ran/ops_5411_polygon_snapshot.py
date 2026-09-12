"""ops_5411 -- Polygon snapshot (paid plan), bounded ticker set.

SSM /justhodl/polygon/api-key only. No key in logs.
Writes data/polygon-snapshot.json.
"""
from __future__ import annotations

import json
import ssl
import sys
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
TICKERS = [
    "SPY", "QQQ", "IWM", "DIA", "TLT", "HYG", "LQD", "GLD",
    "USO", "UUP", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL",
    "TSLA", "JPM", "XOM", "IBIT",
]
HOSTS = ["https://api.polygon.io", "https://api.massive.com"]
CTX = ssl.create_default_context()


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/5411"})
    with urllib.request.urlopen(req, timeout=45, context=CTX) as r:
        return r.getcode(), json.loads(r.read().decode("utf-8"))


def main():
    with report("ops_5411_polygon_snapshot") as R:
        R.heading("ops 5411 -- Polygon snapshot")
        s3 = boto3.client("s3", region_name=REGION)
        ssm = boto3.client("ssm", region_name=REGION)
        key = ssm.get_parameter(Name=SSM, WithDecryption=True)["Parameter"]["Value"]
        if not key:
            R.fail("empty SSM")
            sys.exit(1)
        R.ok("SSM present")
        tickers = ",".join(TICKERS)
        rows = []
        meta = {}
        err = None
        for host in HOSTS:
            path = "/v2/snapshot/locale/us/markets/stocks/tickers"
            url = host + path + "?" + urllib.parse.urlencode({
                "tickers": tickers, "apiKey": key,
            })
            safe = host + path + "?tickers=" + tickers
            try:
                code, body = _get(url)
                rows = body.get("tickers") or body.get("results") or []
                meta = {
                    "host": host,
                    "http": code,
                    "status": body.get("status"),
                    "count": body.get("count") or len(rows),
                }
                R.ok("%s HTTP %s n=%s" % (host, code, len(rows)))
                break
            except urllib.error.HTTPError as e:
                err = "HTTP %s %s" % (e.code, safe)
                R.warn(err)
            except Exception as e:
                err = "%s %s" % (type(e).__name__, safe)
                R.warn(err)
        slim = []
        for t in rows[:40]:
            day = t.get("day") or {}
            prev = t.get("prevDay") or {}
            slim.append({
                "ticker": t.get("ticker"),
                "updated": t.get("updated"),
                "last_trade": (t.get("lastTrade") or {}).get("p"),
                "day_c": day.get("c"), "day_v": day.get("v"),
                "todaysChangePerc": t.get("todaysChangePerc"),
                "prev_c": prev.get("c"),
            })
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5411",
            "endpoint": "/v2/snapshot/locale/us/markets/stocks/tickers",
            "requested": TICKERS,
            "meta": meta,
            "n": len(slim),
            "status": "LIVE" if slim else "DATA_HOLD",
            "error": None if slim else err,
            "tickers": slim,
        }
        s3.put_object(
            Bucket=B,
            Key="data/polygon-snapshot.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(
            s3.get_object(Bucket=B, Key="data/polygon-snapshot.json")["Body"].read())
        if back.get("schema_version") != 1:
            R.fail("read-back")
            sys.exit(1)
        R.ok("GREEN -- snapshot n=%s status=%s" % (back.get("n"), back.get("status")))


if __name__ == "__main__":
    main()
