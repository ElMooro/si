"""ops_5407 -- Polygon short-interest feed (paid plan).

Key from SSM /justhodl/polygon/api-key only. Never print the key.
Tries api.polygon.io then api.massive.com. Bounded pages.
Writes data/polygon-short-interest.json. DATA_HOLD if the plan rejects.
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
HOSTS = ["https://api.polygon.io", "https://api.massive.com"]
PATH = "/stocks/v1/short-interest"
CTX = ssl.create_default_context()


def _key():
    ssm = boto3.client("ssm", region_name=REGION)
    return ssm.get_parameter(Name=SSM, WithDecryption=True)["Parameter"]["Value"]


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/5407"})
    with urllib.request.urlopen(req, timeout=45, context=CTX) as r:
        return r.getcode(), json.loads(r.read().decode("utf-8"))


def main():
    with report("ops_5407_polygon_short_interest") as R:
        R.heading("ops 5407 -- Polygon short-interest")
        s3 = boto3.client("s3", region_name=REGION)
        try:
            key = _key()
        except Exception as e:
            R.fail("SSM key unavailable: %s" % type(e).__name__)
            sys.exit(1)
        if not key:
            R.fail("empty SSM key")
            sys.exit(1)
        R.ok("SSM parameter present (not logged)")

        rows = []
        meta = {"host": None, "http": None, "status": None, "next_url": None}
        err = None
        for host in HOSTS:
            url = host + PATH + "?" + urllib.parse.urlencode({
                "limit": 1000, "sort": "ticker.asc", "apiKey": key,
            })
            safe = host + PATH + "?limit=1000"
            try:
                code, body = _get(url)
                meta = {
                    "host": host,
                    "http": code,
                    "status": body.get("status"),
                    "request_id": body.get("request_id"),
                    "count": body.get("count") or len(body.get("results") or []),
                }
                chunk = body.get("results") or []
                rows.extend(chunk)
                R.ok("%s HTTP %s n=%s" % (host, code, len(chunk)))
                nxt = body.get("next_url")
                pages = 1
                while nxt and pages < 5:
                    join = "&" if "?" in nxt else "?"
                    page = nxt + join + "apiKey=" + key
                    try:
                        code2, body2 = _get(page)
                    except Exception as e2:
                        R.warn("page %s %s" % (pages, type(e2).__name__))
                        break
                    more = body2.get("results") or []
                    rows.extend(more)
                    nxt = body2.get("next_url")
                    pages += 1
                    R.log("page %s +%s" % (pages, len(more)))
                break
            except urllib.error.HTTPError as e:
                err = "HTTP %s on %s" % (e.code, safe)
                R.warn(err)
            except Exception as e:
                err = "%s on %s" % (type(e).__name__, safe)
                R.warn(err)

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5407",
            "endpoint": PATH,
            "meta": meta,
            "n": len(rows),
            "status": "LIVE" if rows else "DATA_HOLD",
            "error": None if rows else err,
            "sample": rows[:8],
            "results": rows,
        }
        s3.put_object(
            Bucket=B,
            Key="data/polygon-short-interest.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(
            s3.get_object(Bucket=B, Key="data/polygon-short-interest.json")["Body"].read())
        if back.get("schema_version") != 1:
            R.fail("read-back")
            sys.exit(1)
        R.section("verdict")
        R.log("n=%s status=%s" % (back.get("n"), back.get("status")))
        if not rows:
            R.warn("DATA_HOLD -- plan or endpoint rejected; FINRA path stays source of truth")
        R.ok("GREEN -- data/polygon-short-interest.json written")


if __name__ == "__main__":
    main()
