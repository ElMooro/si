"""justhodl-polygon-today - today's forming daily bar for EVERY US stock and ETF, from ONE Polygon all-tickers snapshot.

Khalid, 2026-11-10 09:46 ET: "my system doesn't seem to be pulling live data, SPY definitely is delayed almost 24 hours,
why? I want all the stocks and ETFs data to be accurate."

Why it was 24 hours behind (ops 6768): every bar bank the pages read is end-of-day - the TradingView bank's US names are
appended from Polygon grouped daily at 22:52 UTC (justhodl-tv-bars polygon-append, since TradingView refused its socket
on 2026-10-12), the Polygon EOD bank folds the session at 03:10 UTC, the desks (strength, leaders, bottom) rank from
that fold.  During the session the chart's last SPY bar was yesterday's close and its AUTO refresh re-read the same bank.
Polygon itself answered at 09:52 ET with the forming day bar for 9,200 names (15-minute delayed - the Stocks Starter
entitlement; /v2/last/trade is NOT_AUTHORIZED).

What this engine does (a member of the jhk-tick-session-5min fan-out: every 5 minutes 09:00-16:55 New York, Mon-Fri):
  one GET /v2/snapshot/locale/us/markets/stocks/tickers (include_otc=false, ~13k tickers in under a second) ->
  data/warm/polygon-full/today.json.gz  {session, as_of, delay_minutes, t (the bank's 09:30 ET bar stamp), rows: {T: [o,h,l,c,v,vw,updated_ms]}}
  data/warm/polygon-full/today-manifest.json  (small: session, as_of, n, delay, generated_at, seconds)
Readers merge it as the LAST bar when their bank's last session is older (justhodl-symdir banked_ohlc -> the Worker /ohlc
-> chart.html; the strength desk's by-day feed).  Pre-market (before 09:30 ET) and when the snapshot carries no forming
day the engine writes nothing and says why.  After the close the last run holds the final day bar until the nightly
appends land; the 03:10 UTC fold makes it moot for that session (a reader only uses it when it is NEWER than the bank).
Real data only: no trade is ever synthesized; the snapshot's own `updated` stamp is the as_of.
"""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import gzip
import json
import os
import time
import urllib.parse
import urllib.request

import boto3

import chain_guard          # the fleet-wide trigger cap: every engine start is admitted, counted and stamped
from managed_secret import managed_secret   # keys live in SSM, never in code or logs

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("OUT_KEY", "data/warm/polygon-full/today.json.gz")
MANIFEST_KEY = os.environ.get("MANIFEST_KEY", "data/warm/polygon-full/today-manifest.json")
DELAY_MINUTES = int(os.environ.get("DELAY_MINUTES", "15"))          # the Stocks Starter tape is 15-minute delayed
HOSTS = ("https://api.massive.com", "https://api.polygon.io")
NY = ZoneInfo("America/New_York")
SESSION_OPEN = (9, 30)
S3 = boto3.client("s3", region_name="us-east-1")


def f(v, nd=4):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float("inf"), float("-inf")):
        return None
    return round(v, nd)


def session_open_ts(d):
    """Bank convention (justhodl-tv-bars): a daily bar is stamped at the regular-session open, 09:30 America/New_York."""
    return int(datetime(d.year, d.month, d.day, SESSION_OPEN[0], SESSION_OPEN[1], tzinfo=NY).timestamp())


def snapshot(key):
    """ONE request: every US stock / ETF's day bar, last minute and prevDay. Massive first, Polygon second."""
    last = None
    for host in HOSTS:
        url = host + "/v2/snapshot/locale/us/markets/stocks/tickers?" + urllib.parse.urlencode({"include_otc": "false", "apiKey": key})
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl-polygon-today/" + VERSION, "Accept-Encoding": "gzip"})
            with urllib.request.urlopen(req, timeout=40) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
            doc = json.loads(raw)
            if doc.get("status") in ("OK", "DELAYED") and isinstance(doc.get("tickers"), list):
                return doc, host
            last = "%s: status %s %s" % (host, doc.get("status"), str(doc.get("message") or "")[:120])
        except Exception as e:  # noqa: BLE001
            last = "%s: %s" % (host, str(e)[:160])
    raise RuntimeError(last or "no host answered")


def forming_rows(tickers, now_utc):
    """The forming day bars of the CURRENT New York session: a ticker counts when its snapshot `updated` stamp falls on
    today's NY date at or after the open and its day block carries a close. Returns (rows, as_of_ms, n_seen, n_stale)."""
    today = now_utc.astimezone(NY).date()
    open_ts = session_open_ts(today)
    rows, newest, n_stale = {}, 0, 0
    for tk in tickers:
        t = str(tk.get("ticker") or "").strip().upper()
        day = tk.get("day") or {}
        upd = tk.get("updated") or 0
        try:
            upd_ms = int(upd) // 1_000_000 if int(upd) > 1e15 else int(upd)
        except (TypeError, ValueError):
            upd_ms = 0
        if not t or not day or day.get("c") in (None, 0) or not upd_ms:
            continue
        if upd_ms < open_ts * 1000 or datetime.fromtimestamp(upd_ms / 1000, tz=NY).date() != today:
            n_stale += 1                                               # last touched on an earlier session: not a forming bar
            continue
        rows[t] = [f(day.get("o")), f(day.get("h")), f(day.get("l")), f(day.get("c")), f(day.get("v"), 0), f(day.get("vw")), upd_ms]
        if upd_ms > newest:
            newest = upd_ms
    return rows, newest, n_stale


def lambda_handler(event, context=None, now=None):
    chain_guard.begin(event, context)
    t0 = time.time()
    now = now or datetime.now(timezone.utc)
    ny = now.astimezone(NY)
    out = {"engine": "justhodl-polygon-today", "version": VERSION, "generated_at": now.isoformat(timespec="seconds"), "ny_time": ny.strftime("%Y-%m-%d %H:%M"), "delay_minutes": DELAY_MINUTES}
    if ny.weekday() >= 5 or (ny.hour, ny.minute) < SESSION_OPEN:
        out.update({"wrote": False, "why": "outside a New York session (weekend or before 09:30 ET)", "seconds": round(time.time() - t0, 2)})
        print(json.dumps(out))
        return out
    key = managed_secret(("POLYGON_API_KEY", "POLYGON_KEY", "POLY_KEY"), ("/justhodl/polygon/api-key",))
    if not key:
        out.update({"wrote": False, "why": "no Polygon key in SSM", "seconds": round(time.time() - t0, 2)})
        print(json.dumps(out))
        return out
    doc, host = snapshot(key)
    tickers = doc.get("tickers") or []
    rows, newest_ms, n_stale = forming_rows(tickers, now)
    out.update({"host": host, "snapshot_status": doc.get("status"), "n_tickers": len(tickers), "n_forming": len(rows), "n_stale": n_stale})
    if not rows:
        out.update({"wrote": False, "why": "the snapshot carries no forming day bar for this session yet (holiday, or the delayed tape has not reached the open)", "seconds": round(time.time() - t0, 2)})
        print(json.dumps(out))
        return out
    session = ny.date()
    as_of = datetime.fromtimestamp(newest_ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")
    body = {"engine": "justhodl-polygon-today", "version": VERSION, "session": session.isoformat(), "t": session_open_ts(session), "as_of": as_of, "delay_minutes": DELAY_MINUTES,
            "generated_at": out["generated_at"], "source": "polygon snapshot v2 (all US tickers, %d-minute delayed): day o/h/l/c/v/vw per ticker, updated stamp" % DELAY_MINUTES,
            "fields": ["o", "h", "l", "c", "v", "vw", "updated_ms"], "n": len(rows), "rows": rows}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=gzip.compress(json.dumps(body, separators=(",", ":")).encode()), ContentType="application/json", ContentEncoding="gzip", CacheControl="public, max-age=60")
    man = {k: body[k] for k in ("engine", "version", "session", "t", "as_of", "delay_minutes", "generated_at", "n")}
    man.update({"key": OUT_KEY, "host": host, "n_tickers": len(tickers), "n_stale": n_stale, "seconds": round(time.time() - t0, 2),
                "sample": {t: rows[t] for t in ("SPY", "QQQ", "AAPL", "TLT", "GLD") if t in rows}})
    S3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=json.dumps(man).encode(), ContentType="application/json", CacheControl="public, max-age=60")
    out.update({"wrote": True, "session": body["session"], "as_of": as_of, "n": len(rows), "sample_spy": rows.get("SPY"), "seconds": man["seconds"]})
    print(json.dumps(out))
    return out
