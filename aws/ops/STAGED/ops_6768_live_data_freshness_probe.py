"""ops 6768 - READ-ONLY freshness probe (Khalid 2026-11-10 09:46 ET: "my system doesn't seem to be pulling live data, SPY definitely is
delayed almost 24 hours, why? I want all the stocks and ETFs data to be accurate").

For SPY / QQQ / AAPL / TLT / GLD, from the runner during the session, side by side:
  1. what the chart's bar lane serves: the Worker /ohlc (warehouse: symdir banked_ohlc -> tv-bars / us-equities-daily banks)
     last bar date, and the Worker's forming-session tail (/ohlc ... tail=1, chart-pro only);
  2. what the desks' quote lane serves: the Worker /quotes (Polygon all-tickers snapshot, 15-min delayed on Stocks Starter) -
     price, lastTradeTs / updatedTs age in minutes;
  3. the banks in S3: tv-bars universe US__T last bar, the Polygon EOD bank manifest last_session + the T file's last row,
     data/strength.json as_of / benchmark px, justhodl-tv-bars' last run (receipt / manifest);
  4. Polygon itself with the SSM key (never printed): the snapshot's updated / lastTrade.t / min.t / day.c, the latest
     5-minute aggregate's timestamp, /v1/marketstatus/now, and whether /v2/last/trade answers (entitlement).
Nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys
import time

import boto3
import requests
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION, LIVE, SITE = "us-east-1", "justhodl-dashboard-live", "https://justhodl-data-proxy.raafouis.workers.dev"   # PROXY in jh-desk-core.js / jh-chart-engine.js (justhodl.ai routes only /data/*, /factory/*, /api/v1/*)
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)
TICKERS = ["SPY", "QQQ", "AAPL", "TLT", "GLD"]
UA = {"User-Agent": "justhodl-ops-6768"}


def age_min(ms):
    try:
        ms = float(ms)
        if ms > 1e15:
            ms /= 1e6
        if ms <= 0:
            return None
        return round((time.time() * 1000 - ms) / 60000, 1)
    except Exception:  # noqa: BLE001
        return None


def row_date(t):
    try:
        t = float(t)
        if t > 1e12:
            t /= 1000
        return datetime.fromtimestamp(t, tz=timezone.utc).isoformat(timespec="minutes")
    except Exception:  # noqa: BLE001
        return str(t)


def get(url, **kw):
    try:
        r = requests.get(url, headers=UA, timeout=40, **kw)
        try:
            body = r.json()
        except Exception:  # noqa: BLE001
            body = {"_text": r.text[:300]}
        return r.status_code, body, dict(r.headers)
    except Exception as e:  # noqa: BLE001
        return None, {"error": str(e)[:200]}, {}


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6768_live_data_freshness_probe") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6768 - live-data freshness probe %s (%s ET)" % (now.isoformat(timespec="minutes"), now.astimezone(__import__("zoneinfo").ZoneInfo("America/New_York")).strftime("%H:%M")))

        r.section("1. the chart's bar lane: Worker /ohlc (warehouse) and the forming-session tail")
        for t in TICKERS:
            st, body, hd = get(SITE + "/ohlc?ticker=%s&span=day&mult=1&days=30&nocache=%d" % (t, int(time.time())))
            bars = (body or {}).get("bars") or []
            last = bars[-1] if bars else None
            r.log("%-4s /ohlc: HTTP %s source %s X-Source %s count %s | last bar %s close %s | warehouse_key %s" % (
                t, st, (body or {}).get("source"), hd.get("X-Source") or hd.get("x-source"), (body or {}).get("count"),
                row_date((last or {}).get("time")) if last else None, (last or {}).get("close") if last else None, (body or {}).get("warehouse_key")))
            st2, body2, _ = get(SITE + "/ohlc?ticker=%s&span=day&mult=1&days=2&tail=1&nocache=%d" % (t, int(time.time())))
            tb = (body2 or {}).get("bars") or []
            r.log("%-4s /ohlc tail=1: HTTP %s source %s bars %s" % (t, st2, (body2 or {}).get("source"), [(row_date(b.get("time")), b.get("close")) for b in tb]))

        r.section("2. the desks' quote lane: Worker /quotes (Polygon snapshot)")
        st, body, hd = get(SITE + "/quotes?tickers=%s&nocache=%d" % (",".join(TICKERS), int(time.time())))
        r.log("/quotes HTTP %s X-Cache %s ts %s (%s min ago)" % (st, hd.get("X-Cache") or hd.get("x-cache"), (body or {}).get("ts"), age_min((body or {}).get("ts"))))
        for t, q in ((body or {}).get("tickers") or {}).items():
            r.log("  %-4s price %s change %s (%s%%) prevClose %s dayClose %s open %s high %s low %s vol %s | lastTrade %s min ago | updated %s min ago" % (
                t, q.get("price"), q.get("change"), round(q.get("changePct") or 0, 2), q.get("prevClose"), q.get("dayClose"), q.get("open"), q.get("high"), q.get("low"), q.get("volume"),
                age_min(q.get("lastTradeTs")), age_min(q.get("updatedTs"))))
        if st != 200:
            r.log("  body: %s" % json.dumps(body)[:400])

        r.section("3. the banks in S3")
        try:
            man = json.loads(s3.get_object(Bucket=LIVE, Key="data/warm/polygon-full/manifest.json")["Body"].read())
            r.log("polygon-full manifest: last_session %s generated %s sessions %s" % (man.get("last_session"), man.get("generated_at") or man.get("updated_at"), man.get("n_sessions") or man.get("sessions")))
        except Exception as e:  # noqa: BLE001
            r.warn("polygon-full manifest: %s" % type(e).__name__)
        for t in TICKERS:
            for key in ("data/warm/tv-bars/universe/US__%s.json.gz" % t, "data/warm/us-equities-daily/%s.json.gz" % t, "data/warm/polygon-full/eod/%s.json.gz" % t):
                try:
                    obj = s3.get_object(Bucket=LIVE, Key=key)
                except Exception as e:  # noqa: BLE001
                    r.log("  %-4s %s: %s" % (t, key, getattr(e, "response", {}).get("Error", {}).get("Code") or type(e).__name__))
                    continue
                raw = obj["Body"].read()
                doc = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
                rows = (doc.get("bars") or doc.get("rows") or doc.get("ohlc") or []) if isinstance(doc, dict) else doc
                last = rows[-1] if rows else None
                lt = (last[0] if isinstance(last, list) else (last or {}).get("time") or (last or {}).get("t") or (last or {}).get("date")) if last else None
                lc = (last[4] if isinstance(last, list) else (last or {}).get("close") or (last or {}).get("c")) if last else None
                r.log("  %-4s %s: modified %s | %d rows | last %s close %s | span %s source %s" % (
                    t, key, obj["LastModified"].isoformat(timespec="minutes"), len(rows), row_date(lt) if lt is not None else None, lc, (doc.get("span") if isinstance(doc, dict) else None), (doc.get("source") or doc.get("provider")) if isinstance(doc, dict) else None))
        try:
            obj = s3.get_object(Bucket=LIVE, Key="data/strength.json")
            d = json.loads(obj["Body"].read())
            r.log("data/strength.json: version %s as_of %s generated %s SPY px %s" % (d.get("version"), d.get("as_of"), d.get("generated_at"), (d.get("benchmark") or {}).get("px")))
        except Exception as e:  # noqa: BLE001
            r.warn("strength.json: %s" % type(e).__name__)
        lam = boto3.client("lambda", region_name=REGION, config=CFG)
        for fn in ("justhodl-tv-bars", "justhodl-polygon-eod", "justhodl-polygon-full", "justhodl-symdir"):
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: modified %s timeout %s memory %s" % (fn, (cf.get("LastModified") or "")[:19], cf.get("Timeout"), cf.get("MemorySize")))
            except Exception as e:  # noqa: BLE001
                r.log("%s: %s" % (fn, type(e).__name__))

        r.section("4. Polygon itself (SSM key, never printed)")
        key = None
        try:
            from managed_secret import managed_secret
            key = managed_secret(("POLYGON_API_KEY", "POLYGON_KEY", "POLY_KEY"), ("/justhodl/polygon/api-key",))
        except Exception as e:  # noqa: BLE001
            r.warn("managed_secret: %s" % type(e).__name__)
        if not key:
            try:
                ssm = boto3.client("ssm", region_name=REGION, config=CFG)
                key = ssm.get_parameter(Name="/justhodl/polygon/api-key", WithDecryption=True)["Parameter"]["Value"]
            except Exception as e:  # noqa: BLE001
                r.warn("ssm: %s" % type(e).__name__)
        if key:
            hosts = ("https://api.massive.com", "https://api.polygon.io")
            st, body, _ = get(hosts[1] + "/v1/marketstatus/now", params={"apiKey": key})
            r.log("marketstatus/now: HTTP %s market %s exchanges %s serverTime %s" % (st, (body or {}).get("market"), json.dumps((body or {}).get("exchanges")), (body or {}).get("serverTime")))
            for t in ("SPY", "AAPL"):
                st, body, _ = get(hosts[1] + "/v2/snapshot/locale/us/markets/stocks/tickers/%s" % t, params={"apiKey": key})
                tk = (body or {}).get("ticker") or {}
                r.log("snapshot %s: HTTP %s status %s | updated %s min ago | lastTrade p %s t %s min ago | min c %s t %s min ago | day o %s h %s l %s c %s v %s | prevDay c %s | todaysChangePerc %s" % (
                    t, st, (body or {}).get("status"), age_min(tk.get("updated")), (tk.get("lastTrade") or {}).get("p"), age_min((tk.get("lastTrade") or {}).get("t")),
                    (tk.get("min") or {}).get("c"), age_min((tk.get("min") or {}).get("t")), (tk.get("day") or {}).get("o"), (tk.get("day") or {}).get("h"), (tk.get("day") or {}).get("l"), (tk.get("day") or {}).get("c"), (tk.get("day") or {}).get("v"),
                    (tk.get("prevDay") or {}).get("c"), tk.get("todaysChangePerc")))
                if st != 200:
                    r.log("  body: %s" % json.dumps(body)[:300])
            day = now.strftime("%Y-%m-%d")
            st, body, _ = get(hosts[1] + "/v2/aggs/ticker/SPY/range/5/minute/%s/%s" % (day, day), params={"apiKey": key, "adjusted": "true", "sort": "desc", "limit": 3})
            res = (body or {}).get("results") or []
            r.log("SPY 5-minute aggs today: HTTP %s resultsCount %s | latest %s" % (st, (body or {}).get("resultsCount"), [(row_date(x.get("t")), x.get("c"), x.get("v")) for x in res]))
            st, body, _ = get(hosts[1] + "/v2/aggs/ticker/SPY/range/1/day/%s/%s" % (day, day), params={"apiKey": key, "adjusted": "true", "sort": "desc", "limit": 2})
            r.log("SPY daily agg today (the forming bar): HTTP %s %s" % (st, [(row_date(x.get("t")), x.get("o"), x.get("c"), x.get("v")) for x in ((body or {}).get("results") or [])]))
            st, body, _ = get(hosts[1] + "/v2/last/trade/SPY", params={"apiKey": key})
            r.log("/v2/last/trade/SPY: HTTP %s status %s (entitlement) %s" % (st, (body or {}).get("status"), json.dumps(body)[:160] if st != 200 else "price %s t %s min ago" % (((body or {}).get("results") or {}).get("p"), age_min(((body or {}).get("results") or {}).get("t")))))
            t0 = time.time()
            st, body, _ = get(hosts[1] + "/v2/snapshot/locale/us/markets/stocks/tickers", params={"apiKey": key, "include_otc": "false"})
            tks = (body or {}).get("tickers") or []
            ups = [age_min(x.get("updated")) for x in tks if x.get("updated")]
            ups = [u for u in ups if u is not None]
            r.log("ALL-tickers snapshot: HTTP %s count %s in %.1fs | updated age min: newest %s median %s oldest %s | with lastTrade %d | with day.c %d" % (
                st, (body or {}).get("count") or len(tks), time.time() - t0, min(ups) if ups else None, sorted(ups)[len(ups) // 2] if ups else None, max(ups) if ups else None,
                sum(1 for x in tks if (x.get("lastTrade") or {}).get("p")), sum(1 for x in tks if (x.get("day") or {}).get("c"))))
        r.ok("ops 6768 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
