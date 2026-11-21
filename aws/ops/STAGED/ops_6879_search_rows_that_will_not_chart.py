"""ops 6879 - READ-ONLY audit: "INDEX:BTCUSD - No supported chart route" - which symbols the chart cannot pull, and why.

Khalid 2026-11-21 12:12 ET: "INDEX:BTCUSD - No supported chart route; chart unchanged. i cant pull btc on chart.
investigate why i cant pull symbols out of search bar we had that problem with sp500 index remember".

Reading the code first: the status line is the watchlist router's (jh-chart-tvwatch.js chartRoute).  chartSymbol looks
the id up in the live symbol map (/data/symbol-map.json, built from aws/shared/series_source.py) BEFORE its own venue
rules.  The map holds INDEX:BTCUSD -> (COINGECKO, "bitcoin").  The router accepts a COINGECKO id only as 2-6 letters
(it expects "btc"), "bitcoin" is 7, and a map hit it cannot use ends the search ("if (hit) return ''") - the INDEX rule
three lines below, which knows BTCUSD, is never reached.  The chart engine itself draws INDEX:BTCUSD.

This op measures the whole class on the live site, the way the screen does it:
  1. the live symbol map: entries by source, and how many of each source the router turns into no route;
  2. every id in his copied TradingView lists (plain rows, not spreads): how many have no route, by namespace;
  3. what the Symbol Search dialog shows for crypto, index, FX, commodity and rate words, and which rows have no route;
  4. the crypto ids opened one by one through the screen's own path: route, bars, and how old the last bar is.
Writes nothing.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6846", str(Path(__file__).resolve().parent / "ops_6846_sp500_chart_route_audit.py"))
A = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(A)
BASE, PROXY = A.BASE, A.PROXY
WORDS = ["btc", "bitcoin", "eth", "ethereum", "sol", "xrp", "doge", "crypto total", "eurusd", "usdjpy", "gold", "silver", "oil", "copper", "natgas", "us10y", "dxy", "vix",
         "nasdaq", "dow", "russell", "dax", "nikkei", "ftse", "hang seng", "tesla", "nvda", "spy", "qqq", "tlt"]
CRYPTO = ["INDEX:BTCUSD", "X:BTCUSD", "BTCUSD", "BTC", "BTC-USD", "COINBASE:BTCUSD", "BINANCE:BTCUSDT", "BITSTAMP:BTCUSD", "CRYPTO:BTCUSD", "KRAKEN:XBTUSD", "CRYPTOCAP:BTC", "CRYPTOCAP:BTC.D",
          "CRYPTOCAP:TOTAL", "INDEX:ETHUSD", "X:ETHUSD", "ETHUSD", "COINBASE:ETHUSD", "BINANCE:ETHUSDT", "X:SOLUSD", "BINANCE:SOLUSDT", "COINBASE:SOLUSD", "X:XRPUSD", "BINANCE:XRPUSDT",
          "X:DOGEUSD", "BINANCE:DOGEUSDT", "BINANCE:ETHBTC", "CME:BTC1!", "AMEX:IBIT", "SPY"]
# how the screen routes an id: jh-uni-search.js pick() and jh-tv-watchlist.js route()
HOW = r"""(ids) => ids.map(id => { const ex = !!(window.JHChartExpr && window.JHChartExpr.isExpr(id));
  const us = /^(NASDAQ|NYSE|AMEX|ARCA|BATS|NYSEARCA|NYSEMKT|CBOE):([A-Z][A-Z0-9.]{0,6})$/i.test(id);
  const tv = /^[A-Z][A-Z0-9_]*:/.test(id) && !/^(X|C|I|CQ|CISS|DESK|DATA):/.test(id);
  let how = ex ? 'spread' : us ? 'us-listing' : tv ? 'router' : 'direct', handoff = null;
  if (how === 'router') { try { handoff = window.jhWatchlistRoute(id) || ''; } catch (e) { handoff = ''; } }
  return [id, how, handoff]; })"""
PICK = r"""(id) => { const tv = /^[A-Z][A-Z0-9_]*:/.test(id) && !/^(X|C|I|CQ|CISS|DESK|DATA):/.test(id); try { if (tv) window.JHTvWatchlist.route(id); else window.jhGoSymbol(id, 'chart'); return tv ? 'watchlist route' : 'direct'; } catch (e) { return 'threw ' + String(e && e.message || e); } }"""


def ns(i):
    return i.split(":")[0].upper() if ":" in i else "(bare)"


def bar_age_h(bar, now):
    t = (bar or {}).get("time")
    if isinstance(t, dict):
        t = datetime(t.get("year", 1970), t.get("month", 1), t.get("day", 1), tzinfo=timezone.utc).timestamp()
    if not isinstance(t, (int, float)):
        return None
    return (now.timestamp() - t) / 3600.0


def selftest():
    now = datetime(2026, 11, 21, 12, tzinfo=timezone.utc)
    assert ns("INDEX:BTCUSD") == "INDEX" and ns("spy") == "(bare)" and ns("FRED:DGS10") == "FRED"
    assert abs(bar_age_h({"time": {"year": 2026, "month": 11, "day": 20}}, now) - 36.0) < 1e-6 and bar_age_h({"time": now.timestamp() - 7200}, now) == 2.0 and bar_age_h(None, now) is None


def main():
    selftest()
    from ops_report import report
    with report("ops_6879_search_rows_that_will_not_chart") as r:
        now = datetime.now(timezone.utc)
        stamp = int(time.time())
        r.heading("ops 6879 - symbols the chart will not pull, and why (read-only), %s" % now.isoformat(timespec="minutes"))
        smap = (A.get("%s/data/symbol-map.json?nocache=%d" % (BASE, stamp)) or {}).get("map") or {}
        upper = {str(k).upper(): v for k, v in smap.items() if isinstance(v, dict)}
        lists = A.get("%s/data/tv-watchlists.json?nocache=%d" % (BASE, stamp))
        arr = lists if isinstance(lists, list) else (lists.get("lists") or [])
        own = []
        for L in arr:
            for it in (L.get("items") or L.get("symbols") or []):
                s = it if isinstance(it, str) else (it.get("id") or it.get("s") or "")
                if s and not s.startswith("###") and s not in own:
                    own.append(s)
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_function("() => typeof window.jhWatchlistOpen === 'function' && typeof window.jhWatchlistRoute === 'function' && !!(window.JHTvWatchlist && window.JHTvWatchlist.route) && (window.lastBars || []).length > 100", timeout=90000)
            page.wait_for_timeout(8000)
            for _ in range(8):                      # the router answers "" for everything until its symbol map has loaded
                if page.evaluate("() => window.jhWatchlistRoute('NASDAQ:AAPL') || window.jhWatchlistRoute('FRED:DGS10')"):
                    break
                page.wait_for_timeout(3000)

            r.section("1. The live symbol map against the router")
            keys = sorted(upper)
            routed = {i: (how, h) for i, how, h in page.evaluate(HOW, keys)}
            by_src = defaultdict(lambda: [0, 0, []])
            for k in keys:
                how, h = routed[k]
                src = str(upper[k].get("source"))
                by_src[src][0] += 1
                if how == "router" and not h:
                    by_src[src][1] += 1
                    if len(by_src[src][2]) < 7:
                        by_src[src][2].append("%s=%s" % (k, str(upper[k].get("id"))[:22]))
            r.log("%d map entries; per source: entries / no route through the router" % len(keys))
            for src, (n, dead, ex) in sorted(by_src.items(), key=lambda t: -t[1][1]):
                r.log("  %-14s %5d / %5d  %s" % (src, n, dead, ", ".join(ex)))
            cg = [(k, upper[k].get("id")) for k in keys if upper[k].get("source") == "COINGECKO"]
            cg_dead = [(k, i) for k, i in cg if routed[k][0] == "router" and not routed[k][1]]
            r.log("COINGECKO entries: %d, no route: %d; ids the router's 2-6 letter rule refuses: %s" % (len(cg), len(cg_dead), sorted({str(i) for _k, i in cg_dead})[:40]))
            cg_wrong = [(k, i, routed[k][1]) for k, i in cg if routed[k][1] and str(routed[k][1]).upper() == str(i).upper() + "-USD" and not k.split(":")[-1].startswith(str(i).upper())]
            r.log("COINGECKO entries routed to <coin id>-USD although the symbol names another ticker: %d %s" % (len(cg_wrong), cg_wrong[:12]))

            r.section("2. His own lists")
            plain = [s for s in own if " " not in s]
            res = page.evaluate(HOW, plain)
            kinds = Counter(how for _i, how, _h in res)
            dead = [i for i, how, h in res if how == "router" and not h]
            r.log("%d lists, %d different rows: %s" % (len(arr), len(plain), dict(kinds)))
            r.log("rows the router has no route for: %d of %d router rows" % (len(dead), kinds.get("router", 0)))
            per = Counter(ns(i) for i in dead)
            tot = Counter(ns(i) for i, how, _h in res if how == "router")
            for n_, c in per.most_common(40):
                ex = [i for i in dead if ns(i) == n_][:6]
                src = Counter(str((upper.get(i.upper()) or {}).get("source")) for i in dead if ns(i) == n_)
                r.log("  %-14s %4d of %4d no route | map source %s | %s" % (n_, c, tot[n_], dict(src.most_common(3)), ", ".join(ex)))
            crypto_ns = ("BINANCE", "COINBASE", "BITSTAMP", "KRAKEN", "BYBIT", "CRYPTO", "CRYPTOCAP", "BITFINEX", "OKX", "KUCOIN", "GEMINI", "INDEX", "COINMETRICS", "GLASSNODE")
            r.log("crypto-venue rows in his lists: %d, of which no route: %d" % (sum(1 for i, how, _h in res if ns(i) in crypto_ns), sum(1 for i in dead if ns(i) in crypto_ns)))

            r.section("3. What the Symbol Search dialog shows, and which rows have no route")
            page.evaluate("() => window.jhOpenSearch && window.jhOpenSearch('')")
            page.wait_for_timeout(1500)
            noroute_rows = []
            for w in WORDS:
                try:
                    page.evaluate("(q) => window.jhOpenSearch(q)", w)
                    page.wait_for_timeout(6500)
                    rows = page.evaluate(A.ROWS)
                except Exception as exc:  # noqa: BLE001
                    r.warn("dialog %r: %s" % (w, str(exc)[:100]))
                    continue
                ids = [x["id"] for x in rows if x.get("id")][:14]
                rr = page.evaluate(HOW, ids)
                bad = [i for i, how, h in rr if how == "router" and not h]
                noroute_rows += [(w, i) for i in bad]
                r.log("%-13s %2d rows, %d with no route | %s" % (w, len(ids), len(bad), " ".join(("[%s]" % i) if i in bad else i for i in ids)))
            try:
                page.keyboard.press("Escape")
            except Exception:  # noqa: BLE001
                pass
            r.log("rows shown by the dialog that the router has no route for: %d over %d words; by namespace %s" % (len(noroute_rows), len(WORDS), dict(Counter(ns(i) for _w, i in noroute_rows).most_common(20))))

            r.section("4. Crypto ids opened the way the screen opens them")
            charted = 0
            for s in CRYPTO:
                before = page.evaluate(A.STATE)
                how = page.evaluate(PICK, s)
                page.wait_for_timeout(1200)
                try:
                    page.wait_for_function("(b) => { const ev = window.jhChartEvidence || {}, n = document.getElementById('watchlist-chart-route'), t = n && !n.hidden ? n.textContent : ''; return (ev.symbol && ev.symbol !== b) || /No supported chart route|Unavailable|chart unchanged|No bars/.test(t); }",
                                           arg=before["evidence"]["symbol"], timeout=14000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(1800)
                st = page.evaluate(A.STATE)
                text = st["text"] or ""
                reason = text.split(" · ")[3] if text.count(" · ") >= 3 else text[:70]
                changed = st["evidence"]["symbol"] != before["evidence"]["symbol"] or s in ("SPY",)
                age = bar_age_h(st["last"], now)
                ok = changed and st["bars"] > 10 and "No supported chart route" not in text
                charted += int(bool(ok))
                r.log("%-18s %-9s %-15s | chart %-10s %5d bars, last %s (%s h old) close %s | src %s | %s" % (
                    s, "CHARTS" if ok else "NO", how, st["evidence"]["symbol"], st["bars"], A.day(st["last"]), "%.0f" % age if age is not None else "-", (st["last"] or {}).get("close"),
                    str(st["evidence"]["source"])[:26], reason[:64]))
            r.log("page errors: %s" % (errors[:4] or "none"))
            r.kv(map_entries=len(keys), coingecko_entries=len(cg), coingecko_no_route=len(cg_dead), own_rows=len(plain), own_router_rows=kinds.get("router", 0), own_no_route=len(dead),
                 dialog_rows_no_route=len(noroute_rows), crypto_ids_tried=len(CRYPTO), crypto_ids_charting=charted)
            browser.close()
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
