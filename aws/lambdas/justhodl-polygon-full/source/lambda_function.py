"""justhodl-polygon-full v1.1.0 -- full-market US equities warehouse.

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
  holes    v1.1.0 (2026-10-25, ops 6654 found 2026-10-13 and 10-16 missing after the
           Oct 12-16 freeze): after the cursor pass, weekdays between window_start
           and the cursor with no banked file are pulled once (newest first, HOLE_BUDGET_S
           per run): resultsCount 0 = exchange holiday, recorded in state["holidays"]
           and never asked again; rows = a hole closed, banked like any session.
           The per-ticker transpose (justhodl-polygon-eod) refuses windows spanning a hole.
"""
import gzip
import json
import os
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3
import chain_guard  # ops 5260 — bounded-lineage self-chaining (aws/shared)

ENGINE_VERSION = "justhodl-polygon-full v1.1.0 holes"
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
HOLE_BUDGET_S = int(os.environ.get("PG_HOLE_BUDGET_S", "150"))

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


def banked_sessions(window_start):
    """ISO dates of every banked grouped file from window_start's year on."""
    out = set()
    for y in range(int(window_start[:4]), _now().year + 1):
        tok = None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": ROOT + "grouped/%d/" % y, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                k = o["Key"]
                if k.endswith(".json.gz") and len(k) >= 18:
                    out.add(k[-18:-8])
            tok = resp.get("NextContinuationToken")
            if not tok:
                break
    return out


def repair_holes(state, deadline):
    """Pull the weekdays the bank is missing (newest first) until the deadline; holidays are recorded, holes closed."""
    start = state.get("window_start") or state.get("cursor")
    if not start:
        return {"checked": 0}
    have = banked_sessions(start)
    holidays = set(state.get("holidays") or [])
    cur = date.fromisoformat(state["cursor"])
    d = cur - timedelta(days=1)
    first = date.fromisoformat(start)
    missing = []
    while d >= first:
        ds = d.isoformat()
        if d.weekday() < 5 and ds not in have and ds not in holidays:
            missing.append(ds)
        d -= timedelta(days=1)
    state["holes_open"] = len(missing)
    closed, found_holidays, errors = [], [], []
    for ds in missing:
        if time.time() > deadline:
            break
        verdict, n, raw = pull_day(ds)
        if verdict == "ok":
            s3.put_object(Bucket=BUCKET, Key=ROOT + "grouped/%s/%s.json.gz" % (ds[:4], ds), Body=gzip.compress(raw),
                          ContentType="application/gzip", Metadata={"engine": "polygon-full", "session": ds, "tickers": str(n), "repaired": "hole"})
            state["sessions"] += 1
            state["bytes"] += len(raw)
            closed.append(ds)
        elif verdict == "empty":
            holidays.add(ds)
            found_holidays.append(ds)
        else:
            errors.append([ds, verdict])
        time.sleep(SPACING)
    state["holidays"] = sorted(holidays)
    state["holes_open"] = len(missing) - len(closed) - len(found_holidays)
    state["holes_closed_total"] = int(state.get("holes_closed_total") or 0) + len(closed)
    rep = {"checked": len(missing), "closed": closed, "holidays": found_holidays, "errors": errors[:10], "open": state["holes_open"]}
    state["holes_last"] = rep
    return rep


def lambda_handler(event, ctx=None):
    chain_guard.begin(event)  # ops 5260: bounded lineage, parks at hop 12 (AWS drops at 16)
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
    holes = {"checked": 0}
    if state["phase"] == "LIVE" and not event.get("no_holes"):
        try:
            holes = repair_holes(state, min(_t0 + BUDGET_S - 30, time.time() + HOLE_BUDGET_S))
        except Exception as e:  # noqa: BLE001 - the repair never blocks the live edge
            holes = {"error": str(e)[:120]}
            state["holes_last"] = holes

    behind = state["phase"] == "DRAIN"
    depth = int(event.get("chain_depth") or 0)
    chain = bool(behind and depth < CHAIN_DEPTH_MAX
                 and not event.get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now().isoformat(timespec="seconds")
    _put_json(STATE_KEY, state)
    _put_json(MANIFEST_KEY, {
        "as_of": state["as_of"], "engine": "justhodl-polygon-full",
        "version": "1.1.0", "sessions": state["sessions"],
        "gb": round(state["bytes"] / 1e9, 2),
        "window_start": state.get("window_start"),
        "cursor": state["cursor"], "skips": state["skips"],
        "tickers_last": state.get("last_n"),
        "failures": len(state["failures"]),
        "phase": state["phase"],
        "holidays": state.get("holidays") or [],
        "holes_open": state.get("holes_open"),
        "holes_last": state.get("holes_last"),
        "note": ("full-market grouped-daily warehouse -- every "
                 "session verbatim (~10.5k tickers/day) from the "
                 "entitled boundary; banked sessions persist after "
                 "the window rolls; rate(2h) live edge")})
    if chain:
        try:
            chain_guard.chain_invoke({"chain_depth": depth + 1}, context=ctx)
        except Exception:
            chain = False
    return {"ok": True, "phase": state["phase"],
            "cursor": state["cursor"],
            "sessions": state["sessions"],
            "gb": round(state["bytes"] / 1e9, 2),
            "chained": chain, "holes": holes,
            "elapsed_s": round(time.time() - _t0, 1)}
