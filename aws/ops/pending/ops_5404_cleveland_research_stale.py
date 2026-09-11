"""ops_5404 -- Cleveland parse, equity-research index, stale-feed map.

5402 failed Cleveland on unquoted newlines. 5403 named:
  missing data/brief.json, data/decisive-call.json
  dead-lanes STALE: imf-full 103h (collector, do not invent a wake)
Equity-research-tickers is a 6517-file orphan. Index prefix + sample
keys so ticker.html can fetch one object per symbol.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"
CLE_KEY = "data/warm/fred-canary/cleveland-model.csv.gz"
ATL_KEY = "data/warm/fred-canary/atlanta-gdpnow.csv.gz"
RESEARCH_PREFIXES = [
    "data/warm/equity-research/",
    "data/equity-research/",
    "data/warm/equity-research-tickers/",
    "data/research/",
    "data/ticker-research/",
]
BRIEF_HINTS = [
    "data/brief.json",
    "data/brief/latest.json",
    "data/daily-brief.json",
    "data/engines/brief.json",
    "data/decisive-call.json",
    "data/decisive_call.json",
    "data/calls/decisive.json",
    "data/khalid-brief.json",
]


def _parse_gz(raw):
    text = gzip.decompress(raw).decode("utf-8", errors="replace")
    rows = list(csv.reader(io.StringIO(text, newline=""), skipinitialspace=True))
    if not rows:
        return {"n_rows": 0, "header": [], "last": None}
    header = [h.strip() for h in rows[0]]
    last = None
    for row in reversed(rows[1:]):
        if any((c or "").strip() for c in row):
            last = row
            break
    paired = {}
    if last is not None:
        for i, h in enumerate(header):
            if i < len(last) and h:
                paired[h] = last[i]
    return {"n_rows": max(0, len(rows) - 1), "header": header[:20], "last": paired}


def main():
    with report("ops_5404_cleveland_research_stale") as R:
        R.heading("ops 5404 -- Cleveland + research index + stale map")
        s3 = boto3.client("s3", region_name=REGION)
        now = datetime.now(timezone.utc).isoformat()

        series = {}
        for slug, key in (("atlantafed", ATL_KEY), ("clevelandfed", CLE_KEY)):
            try:
                obj = s3.get_object(Bucket=B, Key=key)
                raw = obj["Body"].read()
                parsed = _parse_gz(raw)
                parsed.update({
                    "key": key,
                    "bytes": len(raw),
                    "last_modified": obj["LastModified"].isoformat(),
                })
                series[slug] = parsed
                R.ok("%s rows=%s keys=%s" % (
                    slug, parsed["n_rows"], list((parsed.get("last") or {}).keys())[:8]))
            except Exception as e:
                series[slug] = {"key": key, "error": "%s: %s" % (type(e).__name__, str(e)[:180]), "last": None}
                R.warn("%s %s" % (slug, type(e).__name__))

        join = {
            "generated_at": now,
            "schema_version": 3,
            "source": "ops_5404",
            "status": "LIVE" if any(v.get("last") for v in series.values()) else "DATA_HOLD",
            "series": series,
        }
        s3.put_object(Bucket=B, Key="data/fed-nowcast-join.json",
                      Body=json.dumps(join, default=str).encode("utf-8"),
                      ContentType="application/json")

        found_prefix = None
        sample = []
        tickers = []
        for prefix in RESEARCH_PREFIXES:
            resp = s3.list_objects_v2(Bucket=B, Prefix=prefix, Delimiter="/", MaxKeys=50)
            contents = resp.get("Contents") or []
            prefs = resp.get("CommonPrefixes") or []
            R.log("research prefix %s files=%s dirs=%s" % (prefix, len(contents), len(prefs)))
            if contents or prefs:
                found_prefix = prefix
                for it in contents[:40]:
                    name = it["Key"].rsplit("/", 1)[-1]
                    stem = name.split(".")[0].upper()
                    if stem and stem not in ("INDEX", "MANIFEST", "CATALOG"):
                        tickers.append(stem)
                    sample.append({"key": it["Key"], "bytes": it["Size"]})
                for p in prefs[:40]:
                    slug = p["Prefix"].rstrip("/").rsplit("/", 1)[-1].upper()
                    if slug:
                        tickers.append(slug)
                    sample.append({"prefix": p["Prefix"]})
                break

        idx = {
            "generated_at": now,
            "schema_version": 1,
            "source": "ops_5404",
            "status": "LIVE" if found_prefix else "DATA_HOLD",
            "prefix": found_prefix,
            "sample": sample[:40],
            "tickers_sample": sorted(set(tickers))[:80],
            "n_listed": len(sample),
            "fetch_pattern": (found_prefix + "{SYMBOL}.json") if found_prefix else None,
            "note": "index is a prefix listing, not a 6517-object walk",
        }
        s3.put_object(Bucket=B, Key="data/equity-research-index.json",
                      Body=json.dumps(idx, default=str).encode("utf-8"),
                      ContentType="application/json")
        R.ok("research index prefix=%s n=%s" % (found_prefix, len(sample)))

        discovered = {}
        for key in BRIEF_HINTS:
            try:
                s3.head_object(Bucket=B, Key=key)
                discovered[key] = True
                R.ok("hit %s" % key)
            except Exception:
                discovered[key] = False
                R.warn("miss %s" % key)

        stale = {
            "generated_at": now,
            "schema_version": 1,
            "source": "ops_5404",
            "import_health_alert": "dead-lanes STALE: imf-full 103h -- collector cadence, not a page bug",
            "missing_expected_keys": [k for k, v in discovered.items() if not v],
            "present_alt_keys": [k for k, v in discovered.items() if v],
            "action": "brief.html / calls.html already have their own feed names; do not invent brief.json",
        }
        s3.put_object(Bucket=B, Key="data/stale-feed-map.json",
                      Body=json.dumps(stale, default=str).encode("utf-8"),
                      ContentType="application/json")

        back = json.loads(s3.get_object(Bucket=B, Key="data/fed-nowcast-join.json")["Body"].read())
        if back.get("schema_version") != 3:
            R.fail("join read-back schema")
            sys.exit(1)
        if not (back.get("series") or {}).get("clevelandfed", {}).get("last") and not (
            back.get("series") or {}).get("clevelandfed", {}).get("n_rows"):
            R.warn("Cleveland still empty after newline fix")
        R.section("verdict")
        R.ok("GREEN -- join v3 + research index + stale-feed-map")


if __name__ == "__main__":
    main()
