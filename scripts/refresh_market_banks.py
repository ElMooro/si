#!/usr/bin/env python3
"""Keep the chart's US stock/ETF market banks current (Khalid 2026-10-06: "data has always to stay fresh").

Why this exists: the nightly justhodl-tv-bars universe refresh contacts TradingView first; since the
2026-10-04 custody change a TradingView upgrade refusal (HTTP 400) ends the whole batch with no
fallback, so every bank stopped at 2026-10-02. That rule is left untouched. This job does not contact
TradingView or Yahoo at all: it appends missing sessions to US-listed banks from Polygon's grouped-daily
endpoint -- the same licensed source and API key the approved justhodl-polygon-daily-snapshot (APR-0001)
already uses -- and labels every appended bar "polygon-grouped-daily:<date>".

Rules (append-only, provenance kept):
  * only US-listed banks (US:, NASDAQ:, NYSE:, AMEX:, ARCA:, NYSEARCA:, BATS:, CBOE:, OTC:) are touched;
  * only sessions strictly after the bank's last bar are added -- existing rows are never revised;
  * the bar timestamp follows the bank's own convention (09:30 America/New_York, as the banks already use);
  * identity guard: the first appended close must be within 0.6x-1.6x of the bank's last close, otherwise
    the symbol is skipped and reported (a different instrument or an unadjusted split is never spliced);
  * merged through market_history_integrity.merged_document (bar validation + per-bar source labels).

Usage:  python3 scripts/refresh_market_banks.py [--days 7] [--dry-run] [--only SYM,SYM]
Needs AWS credentials (S3 read/write on the live bucket, SSM read of /justhodl/polygon/api-key).
"""
import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "aws", "lambdas", "justhodl-tv-bars", "source"))
from market_history_integrity import merged_document, read_bank, valid_bar  # noqa: E402

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
INDEX_KEY = "data/warm/tv-bars/universe/_index.json"
US_PREFIXES = ("US", "NASDAQ", "NYSE", "AMEX", "ARCA", "NYSEARCA", "BATS", "CBOE", "OTC")
NY = ZoneInfo("America/New_York")


def us_ticker(sym):
    """'NASDAQ:AAPL' -> 'AAPL'; non-US or spread/expression symbols -> None."""
    if ":" not in sym:
        return None
    ex, core = sym.split(":", 1)
    if ex.upper() not in US_PREFIXES:
        return None
    core = core.upper()
    return core if re.fullmatch(r"[A-Z][A-Z0-9.]{0,9}", core) else None


def session_ts(day):
    """Bank convention: one bar per session stamped at the 09:30 New York open (13:30/14:30 UTC)."""
    d = datetime.strptime(day, "%Y-%m-%d").replace(hour=9, minute=30, tzinfo=NY)
    return int(d.timestamp())


def polygon_row(day, r):
    """Polygon grouped-daily result -> bank row [ts, o, h, l, c, v] or None if invalid."""
    try:
        row = [session_ts(day), float(r["o"]), float(r["h"]), float(r["l"]), float(r["c"]),
               float(r["v"]) if r.get("v") is not None else None]
    except (KeyError, TypeError, ValueError):
        return None
    return row if valid_bar(row) else None


def plan_append(doc, sessions, ticker):
    """New rows for one bank: sessions after its last bar, identity-guarded. Returns (rows, reason)."""
    bars = doc.get("bars") or []
    if not bars:
        return [], "empty bank"
    last_ts, last_close = bars[-1][0], bars[-1][4]
    last_day = datetime.fromtimestamp(last_ts, tz=NY).strftime("%Y-%m-%d")
    rows = []
    for day in sorted(sessions):
        if day <= last_day:
            continue
        r = sessions[day].get(ticker)
        if r is None:
            continue
        row = polygon_row(day, r)
        if row:
            rows.append(row)
    if not rows:
        return [], "current" if last_day >= max(sessions or [""]) else "no polygon rows"
    ratio = rows[0][4] / last_close if last_close else 0
    if not 0.6 <= ratio <= 1.6:
        return [], "identity guard: close %.4g vs bank %.4g" % (rows[0][4], last_close)
    return rows, "append %d" % len(rows)


def trading_days(n, today=None):
    d = (today or datetime.now(NY)).date()
    out = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    return out


def fetch_grouped(day, key):
    url = ("https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/%s?adjusted=true&include_otc=true&apiKey=%s"
           % (day, key))
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"}), timeout=90) as r:
        data = json.loads(r.read())
    return {x["T"]: x for x in (data.get("results") or []) if x.get("T")}


def refresh_series_cache(s3, sym, t, bkey, rows, label, dry_run=False):
    """The symbol directory serves /quote and /series from data/series-cache/ (up to 10 days old). Append the same
    new bars to any cache entry built from this bank so quotes and charts move with the bank; entries built from
    any other source are left alone."""
    import hashlib
    done = []
    venues = [v + ":" + t.upper() for v in ("NYSE", "NASDAQ", "AMEX", "NYSEARCA", "ARCA", "BATS", "CBOE", "US")]
    for sid in dict.fromkeys([sym, sym.upper(), t, t.upper(), "tv:" + sym] + venues):
        h = hashlib.sha1(sid.encode()).hexdigest()
        ck = "data/series-cache/%s/%s.json" % (h[:2], h)
        try:
            c = json.loads(s3.get_object(Bucket=BUCKET, Key=ck)["Body"].read())
        except Exception:  # noqa: BLE001
            continue
        if not str(c.get("source") or "").endswith(bkey) or not isinstance(c.get("obs"), list):
            continue
        last = c["obs"][-1][0] if c["obs"] else ""
        add = [r for r in rows if datetime.fromtimestamp(r[0], tz=NY).strftime("%Y-%m-%d") > last]
        if not add:
            continue
        for r in add:
            d = datetime.fromtimestamp(r[0], tz=NY).strftime("%Y-%m-%d")
            c["obs"].append([d, r[4]])
            if isinstance(c.get("ohlc"), list):
                c["ohlc"].append([d, r[1], r[2], r[3], r[4], r[5] if len(r) > 5 else None])
            if isinstance(c.get("bar_sources"), list):
                c["bar_sources"].append(label)
        c["n"], c["last"] = len(c["obs"]), c["obs"][-1][0]
        c["as_of"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        c["last_modified"] = c["as_of"]
        if not dry_run:
            s3.put_object(Bucket=BUCKET, Key=ck, Body=json.dumps(c).encode(), ContentType="application/json",
                          CacheControl="public, max-age=1800")
        done.append(sid)
    return done


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default="")
    ap.add_argument("--report", default="market-bank-refresh.json")
    a = ap.parse_args()
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    key = os.environ.get("POLYGON_API_KEY") or boto3.client("ssm", region_name="us-east-1").get_parameter(
        Name="/justhodl/polygon/api-key", WithDecryption=True)["Parameter"]["Value"]

    now_ny = datetime.now(NY)
    sessions = {}
    for day in trading_days(a.days):
        # today's session only once it has closed (grouped-daily is final after the close)
        if day == now_ny.strftime("%Y-%m-%d") and (now_ny.hour, now_ny.minute) < (16, 20):
            continue
        try:
            rows = fetch_grouped(day, key)
        except Exception as e:  # noqa: BLE001
            print("grouped %s: %s %s" % (day, type(e).__name__, str(e)[:80]))
            continue
        if rows:
            sessions[day] = rows
        print("grouped %s: %d tickers" % (day, len(rows)))
        time.sleep(0.3)
    if not sessions:
        print("no sessions available; nothing to do")
        return 0

    idx = json.loads(s3.get_object(Bucket=BUCKET, Key=INDEX_KEY)["Body"].read())
    only = {x.strip() for x in a.only.split(",") if x.strip()}
    report = {"started": datetime.now(timezone.utc).isoformat(timespec="seconds"), "sessions": sorted(sessions),
              "source": "polygon grouped-daily (adjusted=true), APR-0001 provider", "updated": {}, "skipped": {}}
    updates = {}
    for sym, meta in sorted((idx.get("symbols") or {}).items()):
        if only and sym not in only:
            continue
        t = us_ticker(sym)
        if not t:
            continue
        bkey = (meta or {}).get("key") or "data/warm/tv-bars/universe/%s.json.gz" % re.sub(r"[^A-Za-z0-9_.\-!]", "__", sym)
        try:
            doc = read_bank(s3, BUCKET, bkey)
            rows, why = plan_append(doc, sessions, t)
            if not rows:
                report["skipped"][sym] = why
                try:   # bank already current (e.g. an earlier run): still bring a lagging series cache up to it
                    cs = refresh_series_cache(s3, sym, t, bkey, (doc.get("bars") or [])[-10:], "bank:" + str(doc.get("last_date")), a.dry_run)
                    if cs:
                        report.setdefault("cache_caught_up", {})[sym] = cs
                except Exception:  # noqa: BLE001
                    pass
                continue
            new = merged_document(doc, rows, doc.get("symbol") or sym, doc.get("tv_symbol") or sym,
                                  "polygon-grouped-daily:" + datetime.fromtimestamp(rows[-1][0], tz=NY).strftime("%Y-%m-%d"))
            if not a.dry_run:
                s3.put_object(Bucket=BUCKET, Key=bkey, Body=gzip.compress(json.dumps(new).encode()),
                              ContentType="application/gzip", CacheControl="public, max-age=900")
            updates[sym] = {"key": bkey, "n": new["n"], "first": new["first_date"], "last": new["last_date"], "as_of": new["as_of"]}
            report["updated"][sym] = {"added": len(rows), "last": new["last_date"], "close": rows[-1][4]}
            try:
                cs = refresh_series_cache(s3, sym, t, bkey, (new.get("bars") or rows)[-10:], "polygon-grouped-daily:" + new["last_date"], a.dry_run)
                if cs:
                    report["updated"][sym]["series_cache"] = cs
            except Exception as e:  # noqa: BLE001
                report["updated"][sym]["series_cache_error"] = str(e)[:100]
        except Exception as e:  # noqa: BLE001
            report["skipped"][sym] = "error %s: %s" % (type(e).__name__, str(e)[:100])
    if updates and not a.dry_run:
        cur = json.loads(s3.get_object(Bucket=BUCKET, Key=INDEX_KEY)["Body"].read())   # re-read: other writers
        cur.setdefault("symbols", {}).update(updates)
        cur["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cur["n_symbols"] = len(cur["symbols"])
        s3.put_object(Bucket=BUCKET, Key=INDEX_KEY, Body=json.dumps(cur, default=str).encode(),
                      ContentType="application/json", CacheControl="no-cache")
    report["finished"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    report["n_updated"], report["n_skipped"] = len(report["updated"]), len(report["skipped"])
    json.dump(report, open(a.report, "w"), indent=1)
    if not a.dry_run:
        s3.put_object(Bucket=BUCKET, Key="data/ops/market-bank-refresh/latest.json", Body=json.dumps(report).encode(),
                      ContentType="application/json", CacheControl="no-cache")
    print("updated %d banks, skipped %d (%s)" % (len(report["updated"]), len(report["skipped"]),
          ", ".join("%s=%s" % kv for kv in list(report["skipped"].items())[:12])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
