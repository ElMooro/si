"""justhodl-polygon-full v1.0.0 -- full-market US equities warehouse.

4975 evidence: grouped-daily (no limit param!) returns the ENTIRE
market -- 10,561 tickers / 1.09MB per session -- entitled on a
rolling ~5y window (403 before ~2022, 200 after). Doctrine:

  cursor   trading-day cursor from the entitled boundary; the
           engine FINDS the boundary itself (403 at cursor ->
           step forward until 200); weekends/holidays return
           resultsCount 0 -> counted skip, not a gap
  bank     verbatim gz per session at
           data/warm/polygon-full/grouped/YYYY/YYYY-MM-DD.json.gz
           -- each banked date is OURS forever, even after the
           entitlement window rolls past it (that is the point)
  steady   rate(2 hours) advances to the newest closed session
  key      POLYGON_API_KEY env (set by ops from the fleet donor)
"""
import gzip
import json
import os
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3

ENGINE_VERSION = "justhodl-polygon-full v1.0.0 ops4976 grouped"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
KEY = os.environ.get("POLYGON_API_KEY", "")
ROOT = "data/warm/polygon-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)"}
BUDGET_S = int(os.environ.get("PG_BUDGET_S", "660"))
SPACING = 0.30
CHAIN_DEPTH_MAX = 30
START_FLOOR = "2021-06-01"        # search from here; 403s advance it

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()


def _now():
    return datetime.now(timezone.utc)


def _j(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, indent=1).encode(),
                  ContentType="application/json")


def pull_day(d):
    """-> ('ok', n, raw) | ('empty',0,b'') | ('forbidden'|'err',..)"""
    url = ("https://api.polygon.io/v2/aggs/grouped/locale/us/"
           "market/stocks/%s?adjusted=true&apiKey=%s" % (d, KEY))
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=90) as r:
            raw = r.read(60_000_000)
        n = json.loads(raw).get("resultsCount", 0)
        return ("ok", n, raw) if n else ("empty", 0, b"")
    except urllib.error.HTTPError as e:
        if e.code == 403:
            return ("forbidden", 0, b"")
        return ("err%s" % e.code, 0, b"")
    except Exception as e:
        return ("err:" + str(e)[:40], 0, b"")


def lambda_handler(event, ctx=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    if not KEY:
        return {"error": "POLYGON_API_KEY unset"}
    state = _j(STATE_KEY, None) or {
        "version": "1.0.0", "phase": "DRAIN",
        "cursor": START_FLOOR, "sessions": 0, "bytes": 0,
        "skips": 0, "forbidden_advances": 0, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put_json(STATE_KEY, state)

    edge = (_now() - timedelta(hours=8)).date()   # last closed day
    cur = date.fromisoformat(state["cursor"])
    done = 0
    while cur <= edge:
        if time.time() - _t0 > BUDGET_S - 40:
            break
        ds = cur.isoformat()
        verdict, n, raw = pull_day(ds)
        if verdict == "ok":
            s3.put_object(
                Bucket=BUCKET,
                Key=ROOT + "grouped/%s/%s.json.gz" % (ds[:4], ds),
                Body=gzip.compress(raw),
                ContentType="application/gzip",
                Metadata={"engine": "polygon-full",
                          "session": ds, "tickers": str(n)})
            state["sessions"] += 1
            state["bytes"] += len(raw)
            state["last_n"] = n
        elif verdict == "empty":
            state["skips"] += 1               # weekend/holiday
        elif verdict == "forbidden":
            state["forbidden_advances"] += 1  # pre-window: walk on
            state["window_start"] = (
                cur + timedelta(days=1)).isoformat()
        else:
            fl = state["failures"]
            tries = (fl.get(ds) or {}).get("tries", 0) + 1
            fl[ds] = {"err": verdict, "tries": tries}
            if tries < 3:
                _put_json(STATE_KEY, state)
                time.sleep(1.0)
                continue                       # retry same day
        cur += timedelta(days=1)
        state["cursor"] = cur.isoformat()
        done += 1
        if done % 25 == 0:
            _put_json(STATE_KEY, state)
        time.sleep(SPACING)
    state["phase"] = "DRAIN" if cur <= edge else "LIVE"

    behind = state["phase"] == "DRAIN"
    depth = int(event.get("chain_depth") or 0)
    chain = bool(behind and depth < CHAIN_DEPTH_MAX
                 and not event.get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now().isoformat(timespec="seconds")
    _put_json(STATE_KEY, state)
    _put_json(MANIFEST_KEY, {
        "as_of": state["as_of"], "engine": "justhodl-polygon-full",
        "version": "1.0.0", "sessions": state["sessions"],
        "gb": round(state["bytes"] / 1e9, 2),
        "window_start": state.get("window_start"),
        "cursor": state["cursor"], "skips": state["skips"],
        "tickers_last": state.get("last_n"),
        "failures": len(state["failures"]),
        "phase": state["phase"],
        "note": ("full-market grouped-daily warehouse -- every "
                 "session verbatim (~10.5k tickers/day) from the "
                 "entitled boundary; banked sessions persist after "
                 "the window rolls; rate(2h) live edge")})
    if chain:
        try:
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=os.environ.get(
                    "AWS_LAMBDA_FUNCTION_NAME",
                    "justhodl-polygon-full"),
                InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": depth + 1}).encode())
        except Exception:
            chain = False
    return {"ok": True, "phase": state["phase"],
            "cursor": state["cursor"],
            "sessions": state["sessions"],
            "gb": round(state["bytes"] / 1e9, 2),
            "chained": chain,
            "elapsed_s": round(time.time() - _t0, 1)}
