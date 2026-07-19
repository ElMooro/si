"""justhodl-spx-history v1.0.0 (ops 3526) — refresher for
data/spx-history-deep.json, the writerless static doc six engines
consume (alert-backtester, ecb-derived, episode-compass,
historical-analogs, confluence-meta, liquidity-inflection).

Reads the existing doc to MIRROR its exact element shape, rebuilds the
full ^GSPC daily series via the FMP /light 4-window stitch (the ~5k
rows/request cap from ops 3516), and writes the identical schema:
{id, source, units, first, last, n_points, points, built_at, note}.
Weekly Scheduler; consumers untouched.
"""
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/spx-history-deep.json"
FMP = "https://financialmodelingprep.com/stable"
S3 = boto3.client("s3", region_name="us-east-1")
WINDOWS = [("1927-01-01", "1954-12-31"), ("1955-01-01", "1974-12-31"),
           ("1975-01-01", "1994-12-31"), ("1995-01-01", "2008-12-31"),
           ("2009-01-01", None)]


def _fmp(qs, key):
    url = f"{FMP}/{qs}&apikey={key}"
    with urllib.request.urlopen(urllib.request.Request(
            url, headers={"User-Agent": "justhodl-spx-history"}),
            timeout=45) as r:
        return json.loads(r.read())


def fetch_spx(key):
    hist = []
    for w_from, w_to in WINDOWS:
        qs = ("historical-price-eod/light?symbol="
              + urllib.parse.quote("^GSPC") + f"&from={w_from}")
        if w_to:
            qs += f"&to={w_to}"
        try:
            part = _fmp(qs, key)
        except Exception as e:  # noqa: BLE001
            print(f"[spx] window {w_from}: {str(e)[:60]}")
            part = []
        if isinstance(part, dict):
            part = part.get("historical") or part.get("data") or []
        if isinstance(part, list):
            hist += part
        time.sleep(0.3)
    seen, rows = set(), []
    for r in hist:
        d = str((r or {}).get("date") or "")[:10]
        c = (r or {}).get("close", (r or {}).get("price"))
        if d and isinstance(c, (int, float)) and c > 0 and d not in seen:
            seen.add(d)
            rows.append((d, float(c)))
    rows.sort(key=lambda t: t[0])
    return rows


def shape_points(rows, sample):
    """Mirror the existing doc's element shape exactly."""
    if isinstance(sample, dict):
        dk = next((k for k in ("date", "d") if k in sample), "date")
        vk = next((k for k in ("close", "c", "value", "v", "price")
                   if k in sample), "close")
        return [{dk: d, vk: round(c, 2)} for d, c in rows]
    return [[d, round(c, 2)] for d, c in rows]


def lambda_handler(event, context):
    import os
    key = os.environ.get("FMP_KEY", "")
    try:
        prev = json.loads(S3.get_object(Bucket=BUCKET, Key=KEY)
                          ["Body"].read())
    except Exception:  # noqa: BLE001
        prev = {}
    sample = (prev.get("points") or [None])[0]
    rows = fetch_spx(key)
    if len(rows) < 5000:
        out = {"ok": False, "error": f"only {len(rows)} rows fetched — "
               "refusing to overwrite the deep history"}
        print(json.dumps(out))
        return out
    doc = {"id": prev.get("id") or "spx-history-deep",
           "source": "FMP ^GSPC daily closes (windowed stitch)",
           "units": prev.get("units") or "index level",
           "first": rows[0][0], "last": rows[-1][0],
           "n_points": len(rows),
           "points": shape_points(rows, sample),
           "built_at": datetime.now(timezone.utc).isoformat(),
           "note": (prev.get("note") or
                    "Deep S&P 500 daily history for episode/analog "
                    "engines.") + " Refreshed weekly (ops 3526)."}
    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(doc, separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"n_points": doc["n_points"], "first": doc["first"],
                      "last": doc["last"],
                      "sample_shape": type(sample).__name__}))
    return {"ok": True, "n_points": doc["n_points"],
            "first": doc["first"], "last": doc["last"]}
