"""ops_5414 -- Polygon options (paid plan probe).

SPY only, 10 contracts. Writes data/polygon-options.json.
403 => DATA_HOLD (options package not on the key).
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
PATH = "/v3/reference/options/contracts"
HOSTS = ["https://api.polygon.io", "https://api.massive.com"]
CTX = ssl.create_default_context()


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/5414"})
    with urllib.request.urlopen(req, timeout=30, context=CTX) as r:
        return r.getcode(), json.loads(r.read().decode("utf-8"))


def main():
    with report("ops_5414_polygon_options") as R:
        R.heading("ops 5414 -- Polygon options SPY")
        s3 = boto3.client("s3", region_name=REGION)
        key = boto3.client("ssm", region_name=REGION).get_parameter(
            Name=SSM, WithDecryption=True)["Parameter"]["Value"]
        if not key:
            R.fail("empty SSM")
            sys.exit(1)
        R.ok("SSM present")
        rows, meta, err = [], {}, None
        for host in HOSTS:
            url = host + PATH + "?" + urllib.parse.urlencode({
                "underlying_ticker": "SPY", "expired": "false",
                "limit": 10, "sort": "expiration_date", "order": "asc",
                "apiKey": key,
            })
            safe = host + PATH + "?underlying_ticker=SPY"
            try:
                code, body = _get(url)
                raw = body.get("results") or []
                for c in raw[:10]:
                    rows.append({
                        "ticker": c.get("ticker"),
                        "expiration_date": c.get("expiration_date"),
                        "strike_price": c.get("strike_price"),
                        "contract_type": c.get("contract_type"),
                    })
                meta = {"host": host, "http": code, "status": body.get("status"), "n": len(rows)}
                R.ok("%s HTTP %s n=%s" % (host, code, len(rows)))
                break
            except urllib.error.HTTPError as e:
                err = "HTTP %s %s" % (e.code, safe)
                R.warn(err)
            except Exception as e:
                err = "%s %s" % (type(e).__name__, safe)
                R.warn(err)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5414",
            "endpoint": PATH,
            "underlying": "SPY",
            "meta": meta,
            "n": len(rows),
            "status": "LIVE" if rows else "DATA_HOLD",
            "error": None if rows else err,
            "contracts": rows,
        }
        s3.put_object(
            Bucket=B,
            Key="data/polygon-options.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/polygon-options.json")["Body"].read())
        if back.get("schema_version") != 1:
            R.fail("read-back")
            sys.exit(1)
        R.ok("GREEN -- options n=%s status=%s" % (back.get("n"), back.get("status")))


if __name__ == "__main__":
    main()
