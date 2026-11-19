"""ops 6846 - READ-ONLY audit: why "VANTAGE:SP500" has no chart route on chart.html (re-runnable).

Khalid 2026-11-19: "investigate this: Requested: VANTAGE:SP500 - Resolved route: unavailable - Handoff: none - No
supported chart route; chart unchanged ... im trying to pull sp500 and i cant seem to be able to pull it out. i can
pull spy fine no problem".

The text is the watchlist's route status (jh-chart-tvwatch.js chartRoute / chartRouteText). The Symbol Search dialog
merges TradingView's own symbol search (/tv-search) into its rows and hands any EXCHANGE:SYMBOL pick to that router,
which knows a fixed set of venues. This op measures, on the live site:
  1. what TradingView's search returns for the headline US indices (the rows the dialog shows him);
  2. what the directory (/symsearch) pins for the same words;
  3. what the live symbol map and his copied TradingView lists hold for these ids;
  4. for every such id, what the live chart does when the id is opened the way a search pick or a watchlist row opens
     it: the route status, the handoff, and whether bars arrive;
  5. the exact path: open the dialog, type sp500, read the rows, click the VANTAGE row.
Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
QUERIES = ["sp500", "spx", "s&p 500", "us500", "nasdaq 100", "ndx", "dow jones", "dji", "russell 2000", "rut", "vix", "dxy"]
FIXED = ["SPY", "VANTAGE:SP500", "SP500", "SPX", "^GSPC", "I:SPX", "SP:SPX", "TVC:SPX", "INDEX:SPX", "CBOE:SPX", "SPCFD:SPX", "FRED:SP500",
         "OANDA:SPX500USD", "FOREXCOM:SPXUSD", "CAPITALCOM:US500", "PEPPERSTONE:US500", "FX:SPX500", "BLACKBULL:SPX500",
         "NASDAQ:NDX", "TVC:NDX", "NDX", "DJ:DJI", "TVC:DJI", "DJI", "TVC:RUT", "RUT", "TVC:VIX", "TVC:DXY"]


def get(url, timeout=60):
    raw = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout).read()
    return json.loads(raw)


def embed(r, name, doc):
    blob = base64.b64encode(gzip.compress(json.dumps(doc, separators=(",", ":"), default=str).encode("utf-8"), 9)).decode("ascii")
    r.log("%s embedded: %d chars" % (name, len(blob)))
    r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN %s" % name)
    r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
    r.lines.append("FEED-PACK-GZ-B64-END -->")


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except ImportError:
        log("installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch(p, log):
    try:
        return p.chromium.launch(channel="chrome", headless=True)
    except Exception as exc:  # noqa: BLE001
        log("system Chrome not usable (%s): installing chromium" % str(exc)[:80])
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        return p.chromium.launch(headless=True)


OPEN = r"""(id) => { const out = { id: id }; try { window.jhWatchlistOpen(id); } catch (e) { out.threw = String(e && e.message || e); }
  const n = document.getElementById('watchlist-chart-route'); out.node = !!n; out.hidden = n ? n.hidden : null; out.requested = n ? n.dataset.requested : null; out.handoff = n ? n.dataset.handoff : null;
  out.text = n ? n.textContent : ''; return out; }"""
STATE = r"""() => { const n = document.getElementById('watchlist-chart-route'), d = window.lastBars || [], ev = window.jhChartEvidence || {}, h = window.jhWatchlistHandoff || null;
  const t = (id) => { const e = document.getElementById(id); return e ? (e.innerText || e.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 200) : null; };
  const bar = (b) => b ? { time: b.time, open: b.open, high: b.high, low: b.low, close: b.close } : null;
  return { hidden: n ? n.hidden : null, requested: n ? n.dataset.requested : null, handoff: n ? n.dataset.handoff : null, text: n ? n.textContent : '', bars: d.length, first: bar(d[0]), last: bar(d[d.length - 1]),
    evidence: { symbol: ev.symbol || null, source: ev.source || null }, relation: h ? h.relation : null, resolved: h ? h.resolved : null, active: window.jhWatchlistActive ? window.jhWatchlistActive() : null,
    st: t('st'), quote: t('quote'), toast: Array.from(document.querySelectorAll('.toast,.jh-toast,#toast')).map(e => (e.textContent || '').trim()).filter(Boolean).slice(-2) }; }"""
ROWS = r"""() => Array.from(document.querySelectorAll('#jhus .us-row[data-i]')).slice(0, 18).map(e => ({ id: e.getAttribute('title'), text: (e.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 150) }))"""


def day(bar):
    if not bar or bar.get("time") is None:
        return "-"
    t = bar["time"]
    if isinstance(t, dict):
        return "%04d-%02d-%02d" % (t.get("year", 0), t.get("month", 0), t.get("day", 0))
    if isinstance(t, (int, float)):
        return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")
    return str(t)[:10]


def main():
    with report("ops_6846_sp500_chart_route_audit") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6846 - VANTAGE:SP500 has no chart route: what the live site does, %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        pack = {"at": now.isoformat(), "tv": {}, "directory": {}, "map": {}, "lists": {}, "routes": [], "dialog": {}}

        r.section("1. what TradingView's search returns (the dialog merges these rows)")
        dynamic = []
        for q in QUERIES:
            try:
                d = get("%s/tv-search?text=%s" % (PROXY, urllib.parse.quote(q)))
                rows = [{"full": s.get("full") or s.get("symbol"), "symbol": s.get("symbol"), "description": s.get("description"), "exchange": s.get("exchange"), "type": s.get("type")} for s in (d.get("symbols") or [])]
            except Exception as exc:  # noqa: BLE001
                rows = []
                r.warn("tv-search %r failed: %s" % (q, str(exc)[:120]))
            pack["tv"][q] = rows
            r.log("%-13s %d rows | %s" % (q, len(rows), " | ".join("%s (%s, %s)" % (x["full"], (x["description"] or "")[:28], x["type"]) for x in rows[:6])))
            dynamic.extend(x["full"] for x in rows[:6] if x.get("full"))

        r.section("2. what the directory pins for the same words")
        for q in ("sp500", "spx", "s&p 500"):
            try:
                d = get("%s/symsearch?q=%s&limit=12" % (PROXY, urllib.parse.quote(q)))
                rows = [{"id": x.get("id"), "name": x.get("name"), "kind": x.get("kind"), "pinned": x.get("pinned"), "provider": x.get("provider")} for x in (d.get("rows") or [])]
            except Exception as exc:  # noqa: BLE001
                rows = []
                r.warn("symsearch %r failed: %s" % (q, str(exc)[:120]))
            pack["directory"][q] = rows
            r.log("%-9s %s" % (q, " | ".join("%s%s (%s)" % ("*" if x["pinned"] else "", x["id"], (x["name"] or "")[:26]) for x in rows[:8])))

        r.section("3. the live symbol map and his copied TradingView lists")
        smap = (get("%s/data/symbol-map.json?nocache=%d" % (BASE, stamp)) or {}).get("map") or {}
        want = sorted(set(FIXED + dynamic))
        upper = {str(k).upper(): v for k, v in smap.items()}
        hits = {k: upper[k.upper()] for k in want if k.upper() in upper}
        pack["map"] = {"n": len(smap), "hits": hits, "to_gspc": sorted(k for k, v in smap.items() if isinstance(v, dict) and str(v.get("id")).upper() == "^GSPC")[:40],
                       "vantage_keys": sorted(k for k in smap if str(k).upper().startswith("VANTAGE:"))[:40]}
        r.log("symbol map: %d keys; of the %d ids tested here it holds %d: %s" % (len(smap), len(want), len(hits), json.dumps(hits)[:900]))
        r.log("map keys that already point at ^GSPC: %s | keys in the VANTAGE namespace: %s" % (pack["map"]["to_gspc"], pack["map"]["vantage_keys"]))
        lists = get("%s/data/tv-watchlists.json?nocache=%d" % (BASE, stamp))
        arr = lists if isinstance(lists, list) else (lists.get("lists") or [])
        exact, legs, sp = [], [], {}
        for L in arr:
            for s in L.get("symbols") or []:
                u = str(s).upper()
                if u == "VANTAGE:SP500":
                    exact.append(L.get("name"))
                elif "VANTAGE:" in u:
                    legs.append("%s [%s]" % (s, L.get("name")))
                if u in ("SP:SPX", "TVC:SPX", "INDEX:SPX", "SPCFD:SPX", "CBOE:SPX", "SPX", "SP500", "FRED:SP500", "AMEX:SPY", "SPY"):
                    sp.setdefault(u, []).append(L.get("name"))
        pack["lists"] = {"n_lists": len(arr), "n_symbols": sum(len(L.get("symbols") or []) for L in arr), "vantage_sp500_exact": exact, "vantage_in_spreads": legs, "sp500_ids": sp}
        r.log("copied lists: %d lists, %d rows; the plain row VANTAGE:SP500 is in %d lists %s; as a leg of a spread in %d rows %s" % (len(arr), pack["lists"]["n_symbols"], len(exact), exact[:6], len(legs), legs[:10]))
        r.log("S&P 500 ids in his lists: %s" % {k: len(v) for k, v in sp.items()})

        r.section("4. the live chart, each id opened the way a search pick or a watchlist row opens it")
        sync_playwright = ensure_playwright(r.log)
        shots = {}
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_function("() => typeof window.jhWatchlistOpen === 'function' && (window.lastBars || []).length > 100", timeout=90000)
            page.wait_for_timeout(7000)
            start = page.evaluate(STATE)
            r.log("page ready: active %s, %d bars, evidence %s" % (start["active"], start["bars"], start["evidence"]))
            ids = []
            for s in FIXED + dynamic:
                if s and s not in ids:
                    ids.append(s)
            ids = ids[:64]
            for s in ids:
                o = page.evaluate(OPEN, s)
                if "Symbol map loading" in (o.get("text") or ""):
                    page.wait_for_timeout(5000)
                    o = page.evaluate(OPEN, s)
                routed = bool(o.get("handoff"))
                if routed:
                    try:
                        page.wait_for_function("() => { const n = document.getElementById('watchlist-chart-route'); const t = n ? n.textContent : ''; return !n || n.hidden || /Retained bars displayed|Unavailable: no accepted plotted bars/.test(t); }", timeout=11000)
                    except Exception:  # noqa: BLE001
                        pass
                    page.wait_for_timeout(700)
                st = page.evaluate(STATE)
                row = {"id": s, "routed": routed, "handoff": o.get("handoff") or None, "open_text": o.get("text"), "threw": o.get("threw"), "state": st}
                pack["routes"].append(row)
                text = st["text"] or o.get("text") or ""
                reason = text.split(" · ")[3] if text.count(" · ") >= 3 else text[:80]
                ok = routed and st["evidence"]["symbol"] and "Retained bars displayed" in text
                r.log("%-22s %-11s handoff %-14s | %s | chart label %s, %d bars %s..%s last close %s | %s" % (
                    s, "CHARTS" if ok else ("routed" if routed else "NO ROUTE"), o.get("handoff") or "-", reason[:70], st["evidence"]["symbol"], st["bars"], day(st["first"]), day(st["last"]),
                    (st["last"] or {}).get("close"), (st["st"] or "")[:60]))
                if s == "VANTAGE:SP500":
                    shots["vantage_sp500_status"] = page.screenshot(type="jpeg", quality=50)

            r.section("5. the exact path: Symbol Search, type sp500, click the VANTAGE row")
            page.evaluate("() => window.jhWatchlistOpen('SPY')")
            page.wait_for_timeout(4000)
            page.evaluate("() => window.jhOpenSearch('sp500')")
            page.wait_for_timeout(9000)
            rows = page.evaluate(ROWS)
            pack["dialog"]["rows"] = rows
            for i, x in enumerate(rows):
                r.log("row %2d  %-22s %s" % (i + 1, x["id"], x["text"][:120]))
            shots["dialog_sp500"] = page.screenshot(type="jpeg", quality=50)
            hit = [x for x in rows if str(x["id"]).upper() == "VANTAGE:SP500"]
            if hit:
                page.evaluate("() => { const e = Array.from(document.querySelectorAll('#jhus .us-row[data-i]')).find(x => String(x.getAttribute('title')).toUpperCase() === 'VANTAGE:SP500'); e.click(); }")
                page.wait_for_timeout(5000)
                st = page.evaluate(STATE)
                pack["dialog"]["after_click"] = st
                r.log("clicked VANTAGE:SP500 -> status: %s" % (st["text"] or "(no status)")[:400])
                r.log("chart after the click: label %s, active %s, %d bars, last %s" % (st["evidence"]["symbol"], st["active"], st["bars"], day(st["last"])))
                shots["after_click_vantage"] = page.screenshot(type="jpeg", quality=50)
            else:
                r.log("the dialog did not list VANTAGE:SP500 among its first %d rows" % len(rows))
            pack["page_errors"] = errors[:10]
            r.log("page errors: %d %s" % (len(errors), errors[:3]))
            ctx.close()
            browser.close()

        embed(r, "sp500-route-audit", pack)
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        dead = [x["id"] for x in pack["routes"] if not x["routed"]]
        r.kv(tested=str(len(pack["routes"])), no_route=str(len(dead)), no_route_ids=", ".join(dead)[:600])
        r.ok("AUDIT: %d ids tested on the live chart, %d have no route" % (len(pack["routes"]), len(dead)))


if __name__ == "__main__":
    main()
    sys.exit(0)
