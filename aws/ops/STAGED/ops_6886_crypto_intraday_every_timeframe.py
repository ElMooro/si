"""ops 6886 - READ-ONLY: crypto intraday bars through the data proxy, every timeframe the chart asks for.

ops 6882 (19:00 UTC, after the worker change): bitcoin's hourly view is now 1,800 real hourly bars from Coinbase, but
the 15-minute request came back empty and fell through.  This asks the proxy for every intraday timeframe the chart
uses, for four coins, and asks Coinbase itself for the same windows, so the failing combination is named.
Writes nothing; no key is used.
"""
from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6768", str(Path(__file__).resolve().parent / "ops_6768_live_data_freshness_probe.py"))
P = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(P)
TFS = [("minute", 1, 7), ("minute", 3, 30), ("minute", 5, 30), ("minute", 15, 90), ("minute", 30, 180), ("minute", 45, 180), ("hour", 1, 730), ("hour", 2, 730), ("hour", 4, 1500), ("hour", 12, 2000)]
COINS = ["BTC", "ETH", "SOL", "TON"]


def spacing(bars):
    if len(bars) < 3:
        return None
    gaps = sorted(float(bars[i]["time"]) - float(bars[i - 1]["time"]) for i in range(max(1, len(bars) - 40), len(bars)))
    return gaps[len(gaps) // 2] / 60.0


def selftest():
    assert spacing([{"time": 0}, {"time": 900}, {"time": 1800}, {"time": 2700}]) == 15.0 and spacing([{"time": 0}]) is None


def main():
    selftest()
    from ops_report import report
    now = time.time()
    with report("ops_6886_crypto_intraday_every_timeframe") as r:
        r.heading("ops 6886 - crypto intraday through the data proxy, every timeframe (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. The proxy's /ohlc")
        good = bad = 0
        for c in COINS:
            for span, mult, days in TFS:
                want = (1 if span == "minute" else 60) * mult
                rows = []
                for attempt in (1, 2):
                    st, body, hd = P.get(P.SITE + "/ohlc", params={"ticker": c + "-USD", "span": span, "mult": mult, "days": days})
                    body = body if isinstance(body, dict) else {}
                    bars = body.get("bars") or []
                    sp = spacing(bars)
                    age = (now - float(bars[-1]["time"])) / 60.0 if bars else None
                    ok = bool(bars) and sp == want and age is not None and age < 3 * want + 5
                    rows.append("try %d: HTTP %s %5d bars, spacing %s min, newest %s min old, %s%s" % (attempt, st, len(bars), "-" if sp is None else "%.0f" % sp, "-" if age is None else "%.0f" % age,
                                                                                                   str(body.get("source") or body.get("error"))[:34], " cache " + str(hd.get("X-Cache") or hd.get("x-cache")) if hd else ""))
                    if ok:
                        break
                    time.sleep(2.5)
                good += int(ok)
                bad += int(not ok)
                r.log("%-4s %2d %-6s (%4d d) %-4s | %s" % (c, mult, span, days, "OK" if ok else "FAIL", " || ".join(rows)))
        r.log("timeframes answered with bars of the asked spacing and a current newest bar: %d of %d" % (good, good + bad))

        r.section("2. Coinbase itself, the same windows (from this runner)")
        for gran in (60, 300, 900, 3600, 21600):
            end = (int(now) // gran + 1) * gran
            start = end - 300 * gran
            iso = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            st, body, _ = P.get("https://api.exchange.coinbase.com/products/BTC-USD/candles", params={"granularity": gran, "start": iso(start), "end": iso(end)})
            r.log("  granularity %5d, start/end 300 candles wide: HTTP %s, %s rows%s" % (gran, st, len(body) if isinstance(body, list) else 0, "" if isinstance(body, list) else " | " + str(body)[:120]))
            st, body, _ = P.get("https://api.exchange.coinbase.com/products/BTC-USD/candles", params={"granularity": gran})
            r.log("  granularity %5d, no window: HTTP %s, %s rows" % (gran, st, len(body) if isinstance(body, list) else 0))
            time.sleep(0.4)
        r.kv(ok=good, failed=bad)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
