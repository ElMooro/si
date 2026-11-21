"""ops 6882 - READ-ONLY, real browser on the live site: what each crypto id does on chart.html, with the requests behind it.

Khalid 2026-11-21: "INDEX:BTCUSD - No supported chart route; chart unchanged. i cant pull btc on chart. investigate why i
cant pull symbols out of search bar".  ops 6879 opened 29 crypto ids and several came back with no chart at all, but it
could not tell a blank chart from one still loading, nor say which lookups were tried.  This one is the measuring stick
for the fix, run before and after it:

  1. each id is opened the way the screen opens it; the probe waits for the chart to settle and records the symbol the
     chart holds, the bars, the newest bar and its price, the source - and every data request made on the way, with its
     status and how many bars came back;
  2. the Symbol Search dialog is driven by hand: the word is typed, the first rows are read, and the row is CLICKED;
  3. bitcoin on the hourly and 15-minute timeframes.
Nothing is written anywhere.

    python3 ops_6882_crypto_ids_on_the_live_chart.py --selftest
"""
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
A = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(A)
BASE = "https://justhodl.ai"
IDS = ["BTC-USD", "INDEX:BTCUSD", "INDEX:ETHUSD", "X:BTCUSD", "X:SOLUSD", "X:ARBUSD", "X:TONUSD", "X:SUIUSD", "BINANCE:BTCUSDT", "BINANCE:SOLUSDT", "COINBASE:SOLUSD", "KRAKEN:XBTUSD",
       "BITSTAMP:BTCUSD", "COINBASE:ETHUSD", "CRYPTO:BTCUSD", "SOLUSD", "CME:BTC1!", "CRYPTOCAP:BTC.D", "AMEX:IBIT", "SPY"]
DIALOG = [("btc", "INDEX:BTCUSD"), ("bitcoin", "INDEX:BTCUSD"), ("eth", "INDEX:ETHUSD"), ("ethereum", "INDEX:ETHUSD"), ("sol", "X:SOLUSD"), ("btc", "X:BTCUSD"), ("btc", "COINBASE:BTCUSD")]
PICK = r"""(id) => { const tv = /^[A-Z][A-Z0-9_]*:/.test(id) && !/^(X|C|I|CQ|CISS|DESK|DATA):/.test(id); try { if (tv) window.JHTvWatchlist.route(id); else window.jhGoSymbol(id, 'chart'); return tv ? 'router' : 'direct'; } catch (e) { return 'threw ' + String(e && e.message || e); } }"""
VIEW = r"""() => { const v = window.jhGetView ? window.jhGetView() : {}; return { symbol: v.symbol || null, tf: v.tf || null }; }"""


def short(url):
    u = urlsplit(url)
    q = u.query
    for drop in ("nocache=", "_="):
        q = "&".join(p for p in q.split("&") if not p.startswith(drop))
    return (u.path + ("?" + q if q else ""))[:86]


def count(body):
    """How many bars / points a data answer carries, and its stated source."""
    if isinstance(body, list):
        return len(body), ""
    if not isinstance(body, dict):
        return 0, ""
    for k in ("bars", "obs", "observations", "data", "rows"):
        if isinstance(body.get(k), list):
            return len(body[k]), str(body.get("source") or body.get("warehouse_key") or body.get("error") or "")[:34]
    return 0, str(body.get("error") or body.get("source") or "")[:34]


def bar_age_h(bar, now):
    if not bar or bar.get("time") is None:
        return None
    t = bar["time"]
    if isinstance(t, dict):
        t = datetime(t.get("year", 1970), t.get("month", 1), t.get("day", 1), tzinfo=timezone.utc).timestamp()
    try:
        return (now - float(t)) / 3600.0
    except Exception:  # noqa: BLE001
        return None


def selftest():
    assert short("https://x.workers.dev/ohlc?ticker=BTC-USD&span=day&nocache=1") == "/ohlc?ticker=BTC-USD&span=day"
    assert count({"bars": [1, 2, 3], "source": "warehouse+yahoo"}) == (3, "warehouse+yahoo")
    assert count({"error": "no data"}) == (0, "no data") and count([1, 2]) == (2, "") and count(None) == (0, "")
    assert bar_age_h({"time": 1000.0}, 4600.0) == 1.0 and bar_age_h(None, 1.0) is None
    assert bar_age_h({"time": {"year": 2026, "month": 11, "day": 21}}, datetime(2026, 11, 21, 12, tzinfo=timezone.utc).timestamp()) == 12.0


def main():
    selftest()
    from ops_report import report
    with report("ops_6882_crypto_ids_on_the_live_chart") as r:
        stamp = int(time.time())
        r.heading("ops 6882 - crypto ids on the live chart, with the requests behind each (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors, net = [], []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))

            def on_response(resp):
                url = resp.url
                if not any(k in url for k in ("workers.dev", "/api/", "binance", "yahoo", "coinbase", "/data/series/")):
                    return
                if any(k in url for k in ("/quote", "/data/symbol-map", "tv-watchlists", "/symdir", "search?")):
                    return
                n, src = 0, ""
                try:
                    n, src = count(resp.json())
                except Exception:  # noqa: BLE001
                    pass
                net.append((short(url), resp.status, n, src))
            page.on("response", on_response)
            page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_function("() => typeof window.jhWatchlistOpen === 'function' && typeof window.jhWatchlistRoute === 'function' && !!(window.JHTvWatchlist && window.JHTvWatchlist.route) && (window.lastBars || []).length > 100", timeout=90000)
            page.wait_for_timeout(8000)
            for _ in range(8):
                if page.evaluate("() => window.jhWatchlistRoute('NASDAQ:AAPL') || window.jhWatchlistRoute('FRED:DGS10')"):
                    break
                page.wait_for_timeout(3000)
            scripts = page.evaluate("() => Array.from(document.scripts).map(s => s.src).filter(s => /jh-chart-tvwatch|jh-chart-engine|jh-uni-search|jh-tv-watchlist/.test(s)).map(s => s.split('/').pop())")
            r.log("page scripts: %s" % " ".join(scripts))

            def settle(before_symbol, want_change=True, limit=26):
                """Wait until the chart holds bars for a new symbol, or the route status says it will not; -> seconds waited."""
                t0 = time.time()
                while time.time() - t0 < limit:
                    st = page.evaluate(A.STATE)
                    text = st["text"] or ""
                    if st["bars"] > 10 and st["evidence"]["symbol"] and (not want_change or st["evidence"]["symbol"] != before_symbol):
                        break
                    if ("No supported chart route" in text or "chart unchanged" in text) and time.time() - t0 > 3 and st["requested"] and st["bars"] > 10:
                        break
                    page.wait_for_timeout(1000)
                page.wait_for_timeout(2500)
                return time.time() - t0

            def line(tag, s, how, before, waited):
                st = page.evaluate(A.STATE)
                view = page.evaluate(VIEW)
                text = st["text"] or ""
                reason = text.split(" · ")[3] if text.count(" · ") >= 3 else text[:70]
                age = bar_age_h(st["last"], time.time())
                noroute = "No supported chart route" in text and (st["requested"] or "").upper() == s.upper()
                changed = bool(st["evidence"]["symbol"]) and st["evidence"]["symbol"] != before["evidence"]["symbol"]
                verdict = "NO ROUTE" if noroute else ("CHARTS" if changed and st["bars"] > 10 else ("same chart" if st["evidence"]["symbol"] == before["evidence"]["symbol"] and st["bars"] > 10 else "BLANK"))
                r.log("%s %-17s %-8s -> %-9s | active %-16s chart %-10s %5d bars, last %s (%s h) close %s | %s | %s | %.0fs" % (
                    tag, s, how, verdict, str(view["symbol"])[:16], str(st["evidence"]["symbol"])[:10], st["bars"], A.day(st["last"]), "%.0f" % age if age is not None else "-",
                    (st["last"] or {}).get("close"), str(st["evidence"]["source"])[:30], reason[:58], waited))
                seen = []
                for path, status, n, src in net:
                    item = "%s [%s, %d%s]" % (path, status, n, (" " + src) if src else "")
                    if item not in seen:
                        seen.append(item)
                for item in seen[:9]:
                    r.log("      %s" % item)
                return verdict, st

            r.section("1. Each id, opened the way the screen opens it")
            tally = {}
            for s in IDS:
                page.evaluate(PICK, "QQQ" if s == "SPY" else "SPY")     # a known other chart first, so a change is a change
                settle(None, want_change=False, limit=12)
                before = page.evaluate(A.STATE)
                del net[:]
                how = page.evaluate(PICK, s)
                waited = settle(before["evidence"]["symbol"])
                v, _st = line("  ", s, how, before, waited)
                tally[v] = tally.get(v, 0) + 1
            r.log("verdicts: %s" % json.dumps(tally, sort_keys=True))

            r.section("2. The Symbol Search dialog: type the word, click the row")
            clicked_ok = 0
            for word, want in DIALOG:
                page.evaluate(PICK, "SPY")                      # a known other chart first, so a change is a change
                settle(None, want_change=False, limit=12)
                before = page.evaluate(A.STATE)
                try:
                    page.evaluate("(q) => window.jhOpenSearch(q)", word)
                    row = None
                    for _ in range(12):
                        page.wait_for_timeout(1000)
                        rows = page.evaluate(A.ROWS)
                        if any(x.get("id") == want for x in rows):
                            row = want
                            break
                    ids = [x["id"] for x in page.evaluate(A.ROWS) if x.get("id")][:8]
                    r.log("  '%s' -> first rows: %s" % (word, " | ".join(ids)))
                    if not row:
                        r.log("     row %s not offered for '%s'" % (want, word))
                        page.keyboard.press("Escape")
                        continue
                    del net[:]
                    page.click('#jhus .us-row[title="%s"]' % want, timeout=8000)
                    waited = settle(before["evidence"]["symbol"])
                    v, st = line("   click", want, "dialog", before, waited)
                    clicked_ok += int(v == "CHARTS")
                except Exception as exc:  # noqa: BLE001
                    r.warn("dialog '%s' / %s: %s" % (word, want, str(exc)[:160]))
                    try:
                        page.keyboard.press("Escape")
                    except Exception:  # noqa: BLE001
                        pass
            r.log("dialog clicks that changed the chart to the picked instrument: %d of %d" % (clicked_ok, len(DIALOG)))

            r.section("3. Bitcoin on intraday timeframes")
            tf0 = page.evaluate(VIEW)["tf"]
            page.evaluate(PICK, "BTC-USD")
            settle(None, want_change=False, limit=15)
            for tf in ("1h", "15m", tf0 or "1d"):
                del net[:]
                page.evaluate("(t) => window.jhSetTf(t)", tf)
                page.wait_for_timeout(9000)
                st = page.evaluate(A.STATE)
                d = page.evaluate("() => { const d = window.lastBars || []; return d.length > 2 ? [d[d.length - 2].time, d[d.length - 1].time] : null; }")
                step = None
                if d and all(isinstance(x, (int, float)) for x in d):
                    step = (d[1] - d[0]) / 60.0
                age = bar_age_h(st["last"], time.time())
                r.log("  BTC-USD at %-4s: %5d bars, newest bar %s (%s h old), spacing %s min, close %s | %s" % (
                    tf, st["bars"], A.day(st["last"]), "%.1f" % age if age is not None else "-", "%.0f" % step if step is not None else "-", (st["last"] or {}).get("close"), str(st["evidence"]["source"])[:40]))
                seen = []
                for path, status, n, src in net:
                    item = "%s [%s, %d%s]" % (path, status, n, (" " + src) if src else "")
                    if item not in seen:
                        seen.append(item)
                for item in seen[:7]:
                    r.log("      %s" % item)
            r.log("page errors: %s" % (errors[:4] or "none"))
            r.kv(ids=len(IDS), charts=tally.get("CHARTS", 0), blank=tally.get("BLANK", 0), no_route=tally.get("NO ROUTE", 0), dialog_clicks_ok=clicked_ok, dialog_clicks=len(DIALOG))
            browser.close()
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
