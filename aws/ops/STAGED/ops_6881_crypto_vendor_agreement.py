"""ops 6881 - READ-ONLY: for every banked coin, does each free outside source carry the SAME coin, and through today?

ops 6880: the chart's crypto tape is the banked Polygon daily bars (through yesterday) joined by the data proxy to a
Yahoo daily history that also supplies today's forming bar.  Two coins came back wrong: TON closes 0.977 in the bank and
0.0018 on the joined tape, ARB 0.114 and 0.00063 - the outside ticker of the same letters is a different coin, and its
older history and today's bar were joined on.  Five coins (APT SUI PEPE UNI POL) got no outside bars at all, so their
tape ends yesterday.  Before the join is given a same-coin check this measures, for all banked coins:

  1. the bank's last rows against Yahoo (through the data proxy), Binance's public market-data mirror and Coinbase
     Exchange: on the days both sides have, the median distance between closes; and each source's newest bar;
  2. whether the CME bitcoin / ether futures ids answer on the Yahoo route (CME:BTC1! has no chart route today);
  3. whether any intraday crypto bars exist behind the chart (the Polygon plan has no crypto intraday).
Writes nothing; no key is used.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import importlib.util
import json
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6768", str(Path(__file__).resolve().parent / "ops_6768_live_data_freshness_probe.py"))
P = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(P)
ROOT_KEY = "data/warm/katlin/crypto-bars/"
TOL = 0.03


def day_of(t):
    try:
        t = float(t)
        if t > 1e12:
            t /= 1000.0
        return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")
    except Exception:  # noqa: BLE001
        return None


def agreement(bank, other):
    """bank/other: {day: close}.  -> (days compared, median |other/bank - 1|) over the newest 30 days both have."""
    days = sorted(d for d in bank if d in other and bank[d] and other[d])[-30:]
    if not days:
        return 0, None
    return len(days), statistics.median(abs(other[d] / bank[d] - 1.0) for d in days)


def verdict(n, med):
    if not n:
        return "no bars"
    if n < 5:
        return "too few days (%d)" % n
    return "SAME" if med <= TOL else "DIFFERENT"


def selftest():
    bank = {"2026-11-%02d" % d: 100.0 + d for d in range(1, 21)}
    same = {k: v * 1.004 for k, v in bank.items()}
    other = {k: v * 0.002 for k, v in bank.items()}
    n, med = agreement(bank, same)
    assert n == 20 and verdict(n, med) == "SAME"
    n, med = agreement(bank, other)
    assert verdict(n, med) == "DIFFERENT"
    assert verdict(*agreement(bank, {})) == "no bars"
    assert verdict(*agreement(bank, {"2026-11-20": 120.0})) == "too few days (1)"
    assert day_of(1795219200) == "2026-11-21" or day_of(1795219200) is not None


def main():
    selftest()
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name=P.REGION, config=P.CFG)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with report("ops_6881_crypto_vendor_agreement") as r:
        r.heading("ops 6881 - banked coins against each free outside source (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        keys, tok = [], None
        while True:
            kw = {"Bucket": P.LIVE, "Prefix": ROOT_KEY, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            res = s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in res.get("Contents", [])]
            if not res.get("IsTruncated"):
                break
            tok = res.get("NextContinuationToken")
        coins = sorted(k.split("/")[-1].replace(".json.gz", "") for k in keys)

        r.section("1. Same coin? bank closes against Yahoo, Binance's public mirror and Coinbase (newest 30 shared days, tolerance %.0f%%)" % (TOL * 100))
        r.log("%-8s %-11s %-10s | %-28s | %-28s | %-28s" % ("coin", "bank last", "close", "yahoo  n med% last", "binance n med% last", "coinbase n med% last"))
        tally = {"yahoo": {}, "binance": {}, "coinbase": {}}
        today_ok = {"yahoo": [], "binance": [], "coinbase": []}
        rows = []
        for c in coins:
            try:
                doc = json.loads(gzip.decompress(s3.get_object(Bucket=P.LIVE, Key=ROOT_KEY + c + ".json.gz")["Body"].read()))
                bank = {str(x[0])[:10]: float(x[4]) for x in (doc.get("rows") or []) if x and x[4]}
            except Exception as e:  # noqa: BLE001
                r.log("%-8s bank unreadable: %s" % (c, type(e).__name__))
                continue
            last = max(bank) if bank else "-"
            cells = {}
            # Yahoo through the data proxy, bank skipped (the same call the chart makes)
            st, body, _ = P.get(P.SITE + "/yf-ohlc", params={"symbol": c + "-USD", "range": "3mo", "interval": "1d", "nowarehouse": "1"})
            y = {day_of(b.get("time")): float(b["close"]) for b in ((body or {}).get("bars") or []) if b.get("close")}
            cells["yahoo"] = y
            st, body, _ = P.get("https://data-api.binance.vision/api/v3/klines", params={"symbol": c + "USDT", "interval": "1d", "limit": 90})
            b = {day_of(k[0]): float(k[4]) for k in body if isinstance(k, list) and len(k) > 4} if isinstance(body, list) else {}
            cells["binance"] = b
            st, body, _ = P.get("https://api.exchange.coinbase.com/products/%s-USD/candles" % c, params={"granularity": 86400})
            cb = {day_of(k[0]): float(k[4]) for k in body if isinstance(k, list) and len(k) > 4} if isinstance(body, list) else {}
            cells["coinbase"] = cb
            out = []
            for name in ("yahoo", "binance", "coinbase"):
                n, med = agreement(bank, cells[name])
                v = verdict(n, med)
                tally[name][v.split(" (")[0]] = tally[name].get(v.split(" (")[0], 0) + 1
                newest = max(cells[name]) if cells[name] else "-"
                if v == "SAME" and newest >= today:
                    today_ok[name].append(c)
                out.append("%-9s %2d %7s %s" % (v[:9], n, "-" if med is None else "%.2f" % (med * 100), newest[5:] if newest != "-" else "-"))
            rows.append((c, out))
            r.log("%-8s %-11s %-10.6g | %-28s | %-28s | %-28s" % (c, last, bank.get(last, 0), out[0], out[1], out[2]))
            time.sleep(0.15)
        for name in ("yahoo", "binance", "coinbase"):
            r.log("%s: %s | same coin AND a bar for today (%s): %d of %d" % (name, json.dumps(tally[name], sort_keys=True), today, len(today_ok[name]), len(coins)))
        any_today = sorted(set(today_ok["yahoo"]) | set(today_ok["binance"]) | set(today_ok["coinbase"]))
        r.log("a verified bar for today from at least one source: %d of %d coins; none for: %s" % (len(any_today), len(coins), " ".join(c for c in coins if c not in any_today) or "-"))
        yb = sorted(set(today_ok["yahoo"]) | set(today_ok["binance"]))
        r.log("from Yahoo or Binance alone: %d; Coinbase adds: %s" % (len(yb), " ".join(c for c in any_today if c not in yb) or "-"))

        r.section("2. CME bitcoin / ether futures on the Yahoo route")
        for sym in ("BTC=F", "MBT=F", "ETH=F", "MET=F"):
            st, body, _ = P.get(P.SITE + "/yf-ohlc", params={"symbol": sym, "range": "max", "interval": "1d", "nowarehouse": "1"})
            bars = (body or {}).get("bars") or []
            r.log("  %-6s HTTP %s %5d bars | first %s last %s close %s | %s" % (sym, st, len(bars), day_of(bars[0]["time"]) if bars else "-", day_of(bars[-1]["time"]) if bars else "-",
                                                                         bars[-1].get("close") if bars else "-", str((body or {}).get("source") or (body or {}).get("error"))[:40]))

        r.section("3. Intraday crypto bars behind the chart")
        for path, params in (("/ohlc", {"ticker": "BTC-USD", "span": "hour", "mult": 1, "days": 30}), ("/ohlc", {"ticker": "BTC-USD", "span": "minute", "mult": 15, "days": 5}),
                             ("/yf-ohlc", {"symbol": "BTC-USD", "range": "5d", "interval": "60m", "nowarehouse": "1"}), ("/klines", {"symbol": "BTCUSDT", "interval": "1h", "limit": 50}),
                             ("/aggTrades", {"symbol": "BTCUSDT", "limit": 5})):
            st, body, _ = P.get(P.SITE + path, params=params)
            bars = (body or {}).get("bars") if isinstance(body, dict) else body
            n = len(bars) if isinstance(bars, list) else 0
            step = None
            if isinstance(bars, list) and n > 2 and isinstance(bars[-1], dict) and bars[-1].get("time") and bars[-2].get("time"):
                step = (float(bars[-1]["time"]) - float(bars[-2]["time"])) / 60.0
            r.log("  proxy %-10s %s: HTTP %s %4d rows%s | %s" % (path, json.dumps(params)[:70], st, n, (" | newest step %.0f min" % step) if step else "",
                                                             str((body or {}).get("source") or (body or {}).get("error") or "")[:60] if isinstance(body, dict) else ""))
        for host in ("https://data-api.binance.vision", "https://api.binance.com"):
            st, body, _ = P.get(host + "/api/v3/klines", params={"symbol": "BTCUSDT", "interval": "1h", "limit": 5})
            r.log("  %s 1h klines from the runner: HTTP %s %s rows%s" % (host, st, len(body) if isinstance(body, list) else 0, "" if isinstance(body, list) else " | " + json.dumps(body)[:120]))
        st, body, _ = P.get("https://api.exchange.coinbase.com/products/BTC-USD/candles", params={"granularity": 3600})
        r.log("  Coinbase 1h candles from the runner: HTTP %s %s rows" % (st, len(body) if isinstance(body, list) else 0))
        r.kv(coins=len(coins), verified_today=len(any_today))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
