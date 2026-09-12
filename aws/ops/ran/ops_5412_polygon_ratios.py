"""ops_5412 -- Polygon ratios / financials (paid plan).

Same SSM key. Tries ratios then financials. 8 common stocks only.
Writes data/polygon-ratios.json. DATA_HOLD if plan rejects.
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
PATHS = [
    "/stocks/financials/v1/ratios",
    "/vX/reference/financials",
    "/v3/reference/tickers/{t}",
]
HOSTS = ["https://api.polygon.io", "https://api.massive.com"]
CTX = ssl.create_default_context()


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/5412"})
    with urllib.request.urlopen(req, timeout=30, context=CTX) as r:
        return r.getcode(), json.loads(r.read().decode("utf-8"))


def main():
    with report("ops_5412_polygon_ratios") as R:
        R.heading("ops 5412 -- Polygon ratios")
        s3 = boto3.client("s3", region_name=REGION)
        key = boto3.client("ssm", region_name=REGION).get_parameter(
            Name=SSM, WithDecryption=True)["Parameter"]["Value"]
        if not key:
            R.fail("empty SSM")
            sys.exit(1)
        R.ok("SSM present")
        out = {}
        used = None
        for name in NAMES:
            hit = None
            for host in HOSTS:
                for path in PATHS:
                    p = path.replace("{t}", name)
                    q = {"apiKey": key, "limit": 5}
                    if "{t}" not in path:
                        q["ticker"] = name
                    url = host + p + "?" + urllib.parse.urlencode(q)
                    safe = host + p + "?ticker=" + name
                    try:
                        code, body = _get(url)
                    except urllib.error.HTTPError as e:
                        R.log("%s %s HTTP %s" % (name, p, e.code))
                        continue
                    except Exception as e:
                        R.log("%s %s %s" % (name, p, type(e).__name__))
                        continue
                    results = body.get("results") or body.get("results") or []
                    if isinstance(body, dict) and body.get("ticker") == name:
                        results = [body]
                    if not results and isinstance(body.get("results"), dict):
                        results = [body["results"]]
                    if results:
                        used = host + p
                        slim = results[0] if isinstance(results[0], dict) else {"raw": str(results[0])[:200]}
                        keep = {}
                        for k in ("ticker", "start_date", "end_date", "fiscal_period", "fiscal_year",
                                  "price_to_earnings", "price_to_book", "price_to_sales",
                                  "enterprise_value", "market_cap", "pe_ratio", "ev_to_ebitda",
                                  "return_on_equity", "current_ratio", "name", "description",
                                  "market_cap", "sic_description", "homepage_url"):
                            if k in slim:
                                keep[k] = slim[k]
                        if not keep:
                            keep = {k: slim[k] for k in list(slim)[:12] if not str(k).startswith("_")}
                        hit = {"http": code, "path": p, "fields": keep}
                        R.ok("%s via %s keys=%s" % (name, p, list(keep)[:8]))
                        break
                if hit:
                    break
            out[name] = hit or {"error": "no endpoint returned results"}
            time.sleep(0.15)
        n_ok = sum(1 for v in out.values() if "fields" in v)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5412",
            "used": used,
            "n_ok": n_ok,
            "n": len(NAMES),
            "status": "LIVE" if n_ok else "DATA_HOLD",
            "tickers": out,
        }
        s3.put_object(
            Bucket=B,
            Key="data/polygon-ratios.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/polygon-ratios.json")["Body"].read())
        if back.get("schema_version") != 1:
            R.fail("read-back")
            sys.exit(1)
        R.ok("GREEN -- ratios n_ok=%s/%s status=%s" % (n_ok, len(NAMES), payload["status"]))


if __name__ == "__main__":
    main()
