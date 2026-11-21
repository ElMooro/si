"""ops 6880 - READ-ONLY probe: which crypto prices the site can serve, from where, and how old each is.

Khalid 2026-11-21: "i cant pull btc on chart" and "i want all my crypto and stocks and etfs to refresh for my data to
stay accurate without my monthly budget going over $250".  ops 6879 showed three different outcomes for the same coin
depending on the id: INDEX:BTCUSD has no route, BINANCE:BTCUSDT draws nothing, X:BTCUSD draws bars that end yesterday,
BTC-USD draws through today.  Before the router is changed this measures every lane:

  1. the banked crypto bars (data/warm/katlin/crypto-bars/): which coins, when each was last written, its last row;
  2. the data proxy's /ohlc for <COIN>-USD (what the chart's crypto tape reads): bars, last bar, source, and the same
     for hourly bars;
  3. the directory's /series for X:<COIN>USD (what a search pick of the directory's crypto id reads);
  4. Polygon itself with the account's key (never printed): the all-crypto snapshot, today's grouped daily bar, one
     ticker's daily and hourly aggregates - what the plan answers and how old the newest figure is.
Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
_spec = importlib.util.spec_from_file_location("ops6768", str(Path(__file__).resolve().parent / "ops_6768_live_data_freshness_probe.py"))
P = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(P)
COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "DOT", "LINK", "LTC", "BCH", "ATOM", "NEAR", "APT", "SUI", "PEPE", "SHIB", "BNB", "TRX", "TON", "XLM", "UNI", "ETC", "FIL",
         "ICP", "HBAR", "ARB", "OP", "AAVE", "MKR", "INJ", "MATIC", "POL", "ALGO", "XMR", "RNDR", "SEI", "TIA", "WIF", "BONK"]


def bar_time(t):
    try:
        t = float(t)
        if t > 1e12:
            t /= 1000.0
        return t
    except Exception:  # noqa: BLE001
        return None


def age_h(t, now):
    t = bar_time(t)
    return None if t is None else (now - t) / 3600.0


def selftest():
    now = 1795300000.0
    assert age_h(now - 7200, now) == 2.0 and age_h((now - 3600) * 1000, now) == 1.0 and age_h("x", now) is None


def main():
    selftest()
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name=P.REGION, config=P.CFG)
    now = time.time()
    with report("ops_6880_crypto_sources_probe") as r:
        r.heading("ops 6880 - crypto prices: every lane, and how old each is (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. The banked crypto bars")
        objs = []
        tok = None
        while True:
            kw = {"Bucket": P.LIVE, "Prefix": "data/warm/katlin/crypto-bars/", "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            res = s3.list_objects_v2(**kw)
            objs += res.get("Contents", [])
            if not res.get("IsTruncated"):
                break
            tok = res.get("NextContinuationToken")
        names = sorted(o["Key"].split("/")[-1].replace(".json.gz", "") for o in objs)
        r.log("%d coins banked: %s" % (len(names), " ".join(names)))
        if objs:
            lm = sorted(o["LastModified"] for o in objs)
            r.log("last written between %s and %s" % (lm[0].strftime("%m-%d %H:%M"), lm[-1].strftime("%m-%d %H:%M")))
        for c in ("BTC", "ETH", "SOL"):
            try:
                doc = json.loads(gzip.decompress(s3.get_object(Bucket=P.LIVE, Key="data/warm/katlin/crypto-bars/%s.json.gz" % c)["Body"].read()))
                rows = doc.get("rows") or []
                r.log("  %s: %d rows, source %s, banked %s, last rows %s" % (c, len(rows), doc.get("source"), str(doc.get("banked_at"))[:16], rows[-2:]))
            except Exception as e:  # noqa: BLE001
                r.log("  %s: %s" % (c, type(e).__name__))

        r.section("2. The data proxy's /ohlc for <COIN>-USD (the chart's crypto tape)")
        ok_day = []
        for c in COINS:
            st, body, hd = P.get(P.SITE + "/ohlc", params={"ticker": c + "-USD", "span": "day", "mult": 1, "days": 400})
            bars = (body or {}).get("bars") or []
            last = bars[-1] if bars else {}
            a = age_h(last.get("time"), now)
            if len(bars) > 30:
                ok_day.append(c)
            r.log("  %-6s day : HTTP %s %5d bars | last %s (%s h old) close %s | %s%s" % (
                c, st, len(bars), P.row_date(last.get("time")) if bars else "-", "%.1f" % a if a is not None else "-", last.get("close"),
                str((body or {}).get("source"))[:34], (" | " + str((body or {}).get("error"))[:50]) if (body or {}).get("error") else ""))
        r.log("daily bars through /ohlc for %d of %d coins: %s" % (len(ok_day), len(COINS), " ".join(ok_day)))
        for c in ("BTC", "ETH", "SOL", "DOGE"):
            for span, mult, days in (("hour", 1, 5), ("minute", 5, 1)):
                st, body, hd = P.get(P.SITE + "/ohlc", params={"ticker": c + "-USD", "span": span, "mult": mult, "days": days})
                bars = (body or {}).get("bars") or []
                last = bars[-1] if bars else {}
                a = age_h(last.get("time"), now)
                r.log("  %-6s %d %-6s: HTTP %s %5d bars | last %s (%s min old) close %s | %s" % (
                    c, mult, span, st, len(bars), P.row_date(last.get("time")) if bars else "-", "%.0f" % (a * 60) if a is not None else "-", last.get("close"), str((body or {}).get("source") or (body or {}).get("error"))[:50]))

        r.section("3. The directory's /series for X:<COIN>USD (a search pick of the directory's crypto id)")
        ok_x = []
        for c in COINS:
            st, body, hd = P.get(P.SITE + "/series", params={"id": "X:%sUSD" % c})
            obs = (body or {}).get("obs") or (body or {}).get("bars") or (body or {}).get("observations") or []
            last = obs[-1] if obs else None
            if len(obs) > 30:
                ok_x.append(c)
            r.log("  X:%-8s HTTP %s %5d points | last %s | %s" % (c + "USD", st, len(obs), json.dumps(last)[:90] if last is not None else "-",
                                                             str((body or {}).get("source") or (body or {}).get("error") or (body or {}).get("provider"))[:60]))
        r.log("/series answers for %d of %d: %s" % (len(ok_x), len(COINS), " ".join(ok_x)))

        r.section("4. Polygon with the account's key (never printed)")
        key = None
        try:
            from managed_secret import managed_secret
            key = managed_secret(("POLYGON_API_KEY", "POLYGON_KEY", "POLY_KEY"), ("/justhodl/polygon/api-key",))
        except Exception as e:  # noqa: BLE001
            r.warn("managed_secret: %s" % type(e).__name__)
        if not key:
            try:
                key = boto3.client("ssm", region_name=P.REGION, config=P.CFG).get_parameter(Name="/justhodl/polygon/api-key", WithDecryption=True)["Parameter"]["Value"]
            except Exception as e:  # noqa: BLE001
                r.warn("ssm: %s" % type(e).__name__)
        if not key:
            r.warn("no Polygon key readable from the runner; section skipped")
        else:
            host = "https://api.polygon.io"
            day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            st, body, _ = P.get(host + "/v2/snapshot/locale/global/markets/crypto/tickers", params={"apiKey": key})
            tk = (body or {}).get("tickers") or []
            r.log("all-crypto snapshot: HTTP %s status %s, %d tickers%s" % (st, (body or {}).get("status"), len(tk), (" | " + str((body or {}).get("message") or (body or {}).get("error"))[:120]) if not tk else ""))
            if tk:
                ages = sorted(a for a in (P.age_min(t.get("updated")) for t in tk) if a is not None)
                usd = [t for t in tk if str(t.get("ticker", "")).endswith("USD")]
                r.log("  %d USD pairs; age of `updated` in minutes: newest %s, median %s, oldest %s" % (len(usd), ages[0] if ages else "-", ages[len(ages) // 2] if ages else "-", ages[-1] if ages else "-"))
                for t in tk:
                    if t.get("ticker") in ("X:BTCUSD", "X:ETHUSD", "X:SOLUSD"):
                        d, lt, mn = t.get("day") or {}, t.get("lastTrade") or {}, t.get("min") or {}
                        r.log("  %s: day o %s h %s l %s c %s v %s | lastTrade p %s (%s min old) | min c %s | updated %s min ago | todaysChangePerc %s" % (
                            t.get("ticker"), d.get("o"), d.get("h"), d.get("l"), d.get("c"), d.get("v"), lt.get("p"), P.age_min(lt.get("t")), mn.get("c"), P.age_min(t.get("updated")), t.get("todaysChangePerc")))
            st, body, _ = P.get(host + "/v2/aggs/grouped/locale/global/market/crypto/%s" % day, params={"apiKey": key, "adjusted": "true"})
            res = (body or {}).get("results") or []
            r.log("grouped daily for today (%s): HTTP %s status %s, %d rows%s" % (day, st, (body or {}).get("status"), len(res), (" | " + str((body or {}).get("message") or (body or {}).get("error"))[:120]) if not res else ""))
            for x in res:
                if x.get("T") in ("X:BTCUSD", "X:ETHUSD"):
                    r.log("  %s: o %s h %s l %s c %s v %s t %s" % (x.get("T"), x.get("o"), x.get("h"), x.get("l"), x.get("c"), x.get("v"), P.row_date(x.get("t"))))
            for span, mult, lim in (("day", 1, 3), ("hour", 1, 3), ("minute", 5, 3)):
                st, body, _ = P.get(host + "/v2/aggs/ticker/X:BTCUSD/range/%d/%s/%s/%s" % (mult, span, day, day), params={"apiKey": key, "adjusted": "true", "sort": "desc", "limit": lim})
                res = (body or {}).get("results") or []
                r.log("X:BTCUSD %d %s aggregates today: HTTP %s status %s, %d rows | newest %s close %s (%s min old)%s" % (
                    mult, span, st, (body or {}).get("status"), len(res), P.row_date(res[0]["t"]) if res else "-", res[0].get("c") if res else "-", P.age_min(res[0]["t"]) if res else "-",
                    (" | " + str((body or {}).get("message") or (body or {}).get("error"))[:100]) if not res else ""))
            st, body, _ = P.get(host + "/v2/last/trade/X:BTCUSD", params={"apiKey": key})
            r.log("last trade X:BTCUSD: HTTP %s %s" % (st, json.dumps({k: (body or {}).get(k) for k in ("status", "message", "error")})[:160] if st != 200 else "price %s, %s min old" % (((body or {}).get("results") or {}).get("p"), P.age_min(((body or {}).get("results") or {}).get("t")))))
        r.kv(coins_banked=len(names), ohlc_daily_ok=len(ok_day), series_x_ok=len(ok_x), coins_tried=len(COINS))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
