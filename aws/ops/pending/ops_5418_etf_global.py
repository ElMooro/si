"""ops_5418 -- pull paid ETF Global add-ons (Massive).

Endpoints (require the $99 add-ons on the key):
  GET /etf-global/v1/fund-flows
  GET /etf-global/v1/constituents
  GET /etf-global/v1/profiles
Bounded: 8 liquid ETFs, limit=50 per call. No new Lambda.
"""
from __future__ import annotations

import json
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
HOSTS = ("https://api.massive.com", "https://api.polygon.io")
TICKERS = ("SPY", "QQQ", "IWM", "DIA", "TLT", "HYG", "GLD", "XLF")
PATHS = {
    "flows": "/etf-global/v1/fund-flows",
    "constituents": "/etf-global/v1/constituents",
    "profiles": "/etf-global/v1/profiles",
}


def _get(host, path, key, extra):
    q = {"apiKey": key, "limit": "50"}
    q.update(extra)
    url = host + path + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops5418"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            raw = r.read()
            return r.status, json.loads(raw.decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:240]
        return e.code, {"error": body, "status": "HTTP_%s" % e.code}
    except Exception as e:
        return 0, {"error": str(e)[:200], "status": "EXC"}


def main():
    with report("ops_5418_etf_global") as R:
        R.heading("ops 5418 -- ETF Global paid add-ons")
        ssm = boto3.client("ssm", region_name=REGION)
        s3 = boto3.client("s3", region_name=REGION)
        key = None
        for name in ("/justhodl/polygon/api-key", "/justhodl/massive/api-key"):
            try:
                key = ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
                R.ok("key from %s" % name)
                break
            except Exception as e:
                R.ok("no %s (%s)" % (name, str(e)[:60]))
        if not key:
            R.fail("no Massive/Polygon key in SSM")
            sys.exit(1)
        out = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5418",
            "tickers": list(TICKERS),
            "products": {},
        }
        for pname, path in PATHS.items():
            pack = {"path": path, "by_ticker": {}, "http": {}, "n_ok": 0}
            for tk in TICKERS:
                status, body, host_used = None, None, None
                for host in HOSTS:
                    status, body = _get(host, path, key, {"composite_ticker": tk, "sort": "processed_date.desc"})
                    host_used = host
                    if status == 200 and isinstance(body, dict) and body.get("results") is not None:
                        break
                rows = (body or {}).get("results") if isinstance(body, dict) else None
                ok = status == 200 and isinstance(rows, list)
                pack["by_ticker"][tk] = {
                    "host": host_used,
                    "http": status,
                    "n": len(rows) if ok else 0,
                    "error": None if ok else (body or {}).get("error") or (body or {}).get("status"),
                    "results": (rows or [])[:40] if ok else [],
                }
                pack["http"][tk] = status
                if ok:
                    pack["n_ok"] += 1
                R.ok("%s %s http=%s n=%s" % (pname, tk, status, pack["by_ticker"][tk]["n"]))
                time.sleep(0.25)
            out["products"][pname] = {
                "path": path,
                "n_ok": pack["n_ok"],
                "http": pack["http"],
                "by_ticker": pack["by_ticker"],
            }
            s3.put_object(
                Bucket=B,
                Key="data/etf-global-%s.json" % pname,
                Body=json.dumps({
                    "generated_at": out["generated_at"],
                    "schema_version": 1,
                    "source": "ops_5418",
                    "product": pname,
                    "path": path,
                    "n_ok": pack["n_ok"],
                    "status": "LIVE" if pack["n_ok"] else "PLAN_HOLD",
                    "by_ticker": pack["by_ticker"],
                }, default=str).encode("utf-8"),
                ContentType="application/json",
            )
        s3.put_object(
            Bucket=B,
            Key="data/etf-global.json",
            Body=json.dumps({
                "generated_at": out["generated_at"],
                "schema_version": 1,
                "source": "ops_5418",
                "status": "LIVE" if any(p["n_ok"] for p in out["products"].values()) else "PLAN_HOLD",
                "n_ok": {k: v["n_ok"] for k, v in out["products"].items()},
                "http": {k: v["http"] for k, v in out["products"].items()},
            }, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/etf-global.json")["Body"].read())
        R.ok("GREEN -- etf-global status=%s n_ok=%s" % (back.get("status"), back.get("n_ok")))
        if back.get("status") == "PLAN_HOLD":
            R.ok("add-on 403/empty -- cancel ETF Global $297 if HTTP is 401/403")


if __name__ == "__main__":
    main()
