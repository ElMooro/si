"""ops 6848 - READ-ONLY live proof of the five fixes Khalid asked for on 2026-11-19 ("Fix all of them"), re-runnable.

  1. typing sp500 led with FRED's copy            -> the directory lists its policy pins: I:SPX, SPY, then FRED
  2. the pinned I:SPX answered "No bars"          -> the Polygon index ids take the live index route
  3. his ratio rows built on VANTAGE:SP500        -> every market leg is read from the chart's own bars for that symbol
  4. Nasdaq-100 and Dow ids had no route          -> one reviewed index table (S&P 500, Nasdaq-100, Dow, TVC:VIX, TVC:TNX)
  5. "awaiting chart frame" on a drawn chart      -> a cached frame keeps its own label

CLAUDE.md "CHECK YOUR WORK": this opens the live site in Chrome on the runner and uses the entry points the screen uses:
JHTvWatchlist.route (a watchlist row, a search pick of an EXCHANGE:SYMBOL id), jhGoSymbol (a search pick of a directory
id), the Symbol Search dialog itself. A chart is judged by what the engine shows (its active symbol, its bars, its quote
line), an index against the same index loaded under its own name. Writes nothing.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import re
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
MIN_INDEX_BARS, MIN_SPREAD_BARS = 5000, 500
TABLE = {
    "^GSPC": ["I:SPX", "SP:SPX", "SPCFD:SPX", "VANTAGE:SP500", "OANDA:SPX500USD", "PEPPERSTONE:US500"],
    "^NDX": ["I:NDX", "TVC:NDX", "SPREADEX:NDX", "CFI:US100", "PURPLETRADING:US100", "THINKMARKETS:NAS100", "CXM:NAS100", "FX:NAS100", "FXPRO:NDAQ100", "DARWINEX:NDQ", "EASYMARKETS:NQCUSD"],
    "^DJI": ["I:DJI", "DJ:DJI", "SPREADEX:DJI", "OPO:DJIUSD", "FX:US30", "CAPITALCOM:US30", "VANTAGE:DJ30"],
    "^RUT": ["I:RUT"], "^IXIC": ["I:COMP"], "^VIX": ["I:VIX", "TVC:VIX"], "^TNX": ["TVC:TNX"],
}
PINS = [("I:SPX", "^GSPC"), ("I:NDX", "^NDX"), ("I:DJI", "^DJI"), ("I:RUT", "^RUT"), ("I:COMP", "^IXIC"), ("I:VIX", "^VIX")]
UNROUTED = ["VANTAGE:SP500FT", "PEPPERSTONE:US500.P", "LSE:SPX", "DJ:DJIM", "PEPPERSTONE:VIX"]


def get(path, stamp, as_json=True):
    raw = urllib.request.urlopen(urllib.request.Request("%s/%s%snocache=%d" % (BASE, path, "&" if "?" in path else "?", stamp), headers=UA), timeout=90).read()
    return json.loads(raw) if as_json else raw.decode("utf-8", "replace")


def directory(q, stamp):
    d = json.loads(urllib.request.urlopen(urllib.request.Request("%s/symsearch?q=%s&limit=12&nocache=%d" % (PROXY, urllib.parse.quote(q), stamp), headers=UA), timeout=60).read())
    return [{"id": x.get("id"), "name": x.get("name"), "pinned": bool(x.get("pinned")), "kind": x.get("kind")} for x in (d.get("rows") or [])]


def arithmetic(text):
    """The expression with each leg replaced by its own as-of value, e.g. "(7175.83)/(3630.3)". Numbers and operators only."""
    if not text or not re.fullmatch(r"[0-9eE+\-*/(). ]+", text):
        return None
    try:
        return float(eval(text, {"__builtins__": {}}, {}))        # noqa: S307 - digits, operators and brackets only (checked above)
    except Exception:  # noqa: BLE001
        return None


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


STATE = r"""() => { const n = document.getElementById('watchlist-chart-route'), d = window.lastBars || [], ev = window.jhChartEvidence || {};
  const t = (id) => { const e = document.getElementById(id); return e ? (e.innerText || e.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 200) : null; };
  const iso = (x) => typeof x === 'number' ? new Date(x * 1000).toISOString().slice(0, 10) : (x && x.year ? x.year + '-' + ('0' + x.month).slice(-2) + '-' + ('0' + x.day).slice(-2) : String(x || '').slice(0, 10));
  return { requested: n && !n.hidden ? n.dataset.requested : null, handoff: n && !n.hidden ? n.dataset.handoff : null, text: n && !n.hidden ? n.textContent : '', bars: d.length, first: d.length ? iso(d[0].time) : null,
    last: d.length ? iso(d[d.length - 1].time) : null, close: d.length ? d[d.length - 1].close : null, label: ev.symbol || null, source: String(ev.source || '').slice(0, 400),
    active: window.jhWatchlistActive ? window.jhWatchlistActive() : null, quote: t('quote'), toast: ((document.getElementById('toast') || {}).textContent || '').slice(0, 120) }; }"""
# the engine shows this symbol: it is the active one and the quote line names it (the label is recorded beside it, not waited for)
SHOWN = r"""(a) => { const q = document.getElementById('quote'), t = ((q && (q.innerText || q.textContent)) || '').replace(/\s+/g, ' ').trim(); const s = window.jhWatchlistActive ? String(window.jhWatchlistActive()) : '';
  return s.toUpperCase() === a[0] && t.toUpperCase().indexOf(a[1]) >= 0 && t.toUpperCase().indexOf(a[1]) < 14 && (window.lastBars || []).length > 1 && !/^No bars/i.test(t); }"""
ROWS = r"""() => Array.from(document.querySelectorAll('#jhus .us-row[data-i]')).slice(0, 12).map(e => ({ id: e.getAttribute('title'), text: (e.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 130) }))"""
SPREAD = r"""async (expr) => { const PROXY = 'https://justhodl-data-proxy.raafouis.workers.dev', ev = window.jhChartEvidence || {}, d = window.lastBars || [];
  const iso = (x) => typeof x === 'number' ? new Date(x * 1000).toISOString().slice(0, 10) : (x && x.year ? x.year + '-' + ('0' + x.month).slice(-2) + '-' + ('0' + x.day).slice(-2) : String(x || '').slice(0, 10));
  const lastDay = d.length ? iso(d[d.length - 1].time) : null;
  const out = { n: d.length, first: d.length ? iso(d[0].time) : null, last: lastDay, value: d.length ? d[d.length - 1].close : null, source: String(ev.source || ''), label: ev.symbol || null, legs: [] };
  let text = expr;
  for (const id of window.JHChartExpr.legs(expr).slice().sort((a, b) => b.length - a.length)) {
    const m = /^(NASDAQ|NYSE|AMEX|ARCA|BATS|NYSEARCA|NYSEMKT):([A-Z][A-Z0-9.]{0,6})$/i.exec(id);
    let target = m ? m[2].toUpperCase() : String(window.jhWatchlistRoute(id) || ''); if (target.indexOf(':') >= 0) target = '';
    let obs = [];
    try { if (target) obs = (await window.jhKlines(target, '1d')).map(b => [iso(b.time), b.close]); else { const j = await fetch(PROXY + '/series?id=' + encodeURIComponent(id)).then(x => x.json()); obs = (j.obs || []).filter(o => o && o[1] != null); } } catch (e) { obs = []; }
    let v = null, at = null; for (const o of obs) { if (o[0] <= lastDay) { v = o[1]; at = o[0]; } else break; }
    out.legs.push({ id: id, target: target || null, n: obs.length, first: obs.length ? obs[0][0] : null, last: obs.length ? obs[obs.length - 1][0] : null, asof: at, value: v });
    text = text.split(id).join('(' + v + ')');
  }
  out.arithmetic = text;
  return out; }"""
ALIAS = r"""async () => { const before = (window.jhChartEvidence || {}).symbol || null, shown = window.lastBars;
  const a = await window.jhKlines('SPX', '1d'), b = await window.jhKlines('SP500', '1d'); await window.paint(window.lastBars); await new Promise(r => setTimeout(r, 600));
  const n = document.getElementById('watchlist-chart-route');
  return { before: before, alias_bars: [a.length, b.length], shares_the_array_on_screen: a === shown || b === shown, after: (window.jhChartEvidence || {}).symbol || null, bars: (window.lastBars || []).length, tail: n ? n.textContent.slice(-110) : '' }; }"""


def main():
    with report("ops_6848_index_routes_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6848 - the five chart fixes, live %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        fails, pack, shots = [], {"at": now.isoformat()}, {}
        week = (now - timedelta(days=5)).strftime("%Y-%m-%d")

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        watch, engine, expr = (get(f, stamp, False) for f in ("jh-chart-tvwatch.js", "jh-chart-engine.js", "jh-chart-expr.js"))
        new = "var indexRoutes = {" in watch and "window.jhWatchlistRoute = function" in watch and '"I:SPX":"^GSPC"' in engine and "cachedFrame" in engine and "function chartTarget(id)" in expr
        r.log("served: router %d bytes, engine %d, spread evaluator %d - the new build is live: %s" % (len(watch), len(engine), len(expr), new))
        if not new:
            r.fail("the served site is not the new build yet (Pages deploy pending?)")
            raise SystemExit(1)

        r.section("1. the directory: what a benchmark word lists first")
        pack["directory"] = {}
        for q, want in (("sp500", ["I:SPX", "SPY", "fred:SP500"]), ("spx", ["I:SPX", "SPY", "fred:SP500"]), ("s&p 500", ["I:SPX", "SPY", "fred:SP500"]), ("ndx", ["I:NDX", "QQQ"]), ("dow", ["I:DJI", "DIA"]), ("nasdaq", ["I:NDX", "QQQ", "I:COMP"])):
            rows = directory(q, stamp)
            pack["directory"][q] = rows
            ids = [x["id"] for x in rows]
            ok = ids[:len(want)] == want and all(x["pinned"] for x in rows[:len(want)])
            if q in ("sp500", "spx", "s&p 500"):
                ck(ok, "directory %r leads with %s, not %s" % (q, ids[:4], want))
            r.log("%-8s -> %s%s" % (q, " | ".join("%s%s" % ("*" if x["pinned"] else "", x["id"]) for x in rows[:6]), "" if ok else "   (policy: %s)" % want))
        if [x["id"] for x in pack["directory"]["sp500"]][:2] != ["I:SPX", "SPY"]:
            r.fail("the directory does not list the pins yet (engine deploy pending?)")
            raise SystemExit(1)

        lists = get("data/tv-watchlists.json", stamp)
        arr = lists if isinstance(lists, list) else (lists.get("lists") or [])
        spreads = []
        for L in arr:
            for s in L.get("symbols") or []:
                if isinstance(s, str) and "VANTAGE:SP500" in s.upper() and re.search(r"[+\-*/()]", s) and s not in spreads:
                    spreads.append(s)
        rows14 = sum(1 for L in arr for s in (L.get("symbols") or []) if isinstance(s, str) and "VANTAGE:SP500" in s.upper() and re.search(r"[+\-*/()]", s))
        r.log("his copied lists: %d rows built on VANTAGE:SP500, %d different expressions" % (rows14, len(spreads)))

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_function("() => typeof window.jhWatchlistOpen === 'function' && typeof window.jhWatchlistRoute === 'function' && !!(window.JHTvWatchlist && window.JHTvWatchlist.route) && !!window.JHChartExpr && (window.lastBars || []).length > 100", timeout=90000)
            page.wait_for_timeout(7000)

            def show(how, sym, active, name, timeout=45000):
                """Open sym the way the screen does and wait until the engine shows `active` with `name` at the head of its quote line."""
                page.evaluate("(a) => { if (a[0] === 'go') window.jhGoSymbol(a[1], 'chart'); else window.JHTvWatchlist.route(a[1]); }", [how, sym])
                try:
                    page.wait_for_function(SHOWN, arg=[active.upper(), name.upper()], timeout=timeout)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(700)
                return page.evaluate(STATE)

            def away():
                st = show("route", "SPY", "SPY", "SPY")
                ck(st["active"] == "SPY" and st["bars"] > 1000, "could not switch away to SPY (active %s, %d bars)" % (st["active"], st["bars"]))

            # controls: each index under its own name
            ref = {}
            for target in TABLE:
                away()
                st = show("route", target, target, target)
                ref[target] = st
                ck(st["active"] == target and st["bars"] > MIN_INDEX_BARS and (st["last"] or "") >= week, "control %s: active %s, %d bars, last %s" % (target, st["active"], st["bars"], st["last"]))
            pack["controls"] = ref
            r.section("the indices under their own names (controls)")
            for target, st in ref.items():
                r.log("%-7s %d bars %s..%s, last close %s" % (target, st["bars"], st["first"], st["last"], st["close"]))

            def same(st, target):
                c, rc = st["close"] or 0, ref[target]["close"] or 1
                return st["bars"] == ref[target]["bars"] and st["first"] == ref[target]["first"] and st["last"] == ref[target]["last"] and abs(c / rc - 1) < 0.02

            r.section("1b. the Symbol Search dialog: type sp500, take the first row")
            away()
            page.evaluate("() => window.jhOpenSearch('sp500')")
            t0 = time.time()
            page.wait_for_timeout(2500)
            early = page.evaluate(ROWS)
            try:
                page.wait_for_function("() => { const e = document.querySelector('#jhus .us-row[data-i]'); return !!e && e.getAttribute('title') === 'I:SPX'; }", timeout=60000)
            except Exception:  # noqa: BLE001
                pass
            waited = time.time() - t0
            page.wait_for_timeout(1200)
            rows = page.evaluate(ROWS)
            r.log("2.5 s after typing the first rows were %s; the list settled with I:SPX first after %.1f s" % ([x["id"] for x in early[:3]], waited))
            pack["dialog"] = rows
            for i, x in enumerate(rows[:5]):
                r.log("row %d  %-12s %s" % (i + 1, x["id"], x["text"][:100]))
            ck(len(rows) >= 3 and rows[0]["id"] == "I:SPX" and rows[1]["id"] == "SPY", "the dialog for sp500 starts with %s" % [x["id"] for x in rows[:3]])
            shots["dialog_sp500_1440"] = page.screenshot(type="jpeg", quality=50)
            page.evaluate("() => { const e = document.querySelector('#jhus .us-row[data-i]'); (e.querySelector('.us-sym') || e).click(); }")
            try:
                page.wait_for_function(SHOWN, arg=["I:SPX", "I:SPX"], timeout=45000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(800)
            st = page.evaluate(STATE)
            ck(st["active"] == "I:SPX" and same(st, "^GSPC") and "No bars" not in (st["quote"] or ""), "first row clicked: active %s, %d bars, last %s %s | %s" % (st["active"], st["bars"], st["last"], st["close"], (st["quote"] or "")[:60]))
            r.log("clicked row 1 -> the chart shows %s: %d bars %s..%s, last close %s | %s" % (st["active"], st["bars"], st["first"], st["last"], st["close"], (st["quote"] or "")[:90]))
            pack["dialog_click"] = st
            shots["dialog_first_row_chart_1440"] = page.screenshot(type="jpeg", quality=50)

            r.section("2. the directory's pinned index ids, opened the way the dialog opens them")
            pack["pins"] = []
            for sid, target in PINS:
                away()
                st = show("go", sid, sid, sid)
                ok = st["active"] == sid and same(st, target) and "No bars" not in (st["quote"] or "") and "No bars" not in (st["toast"] or "")
                ck(ok, "%s: active %s, %d bars, last %s %s, toast %r" % (sid, st["active"], st["bars"], st["last"], st["close"], st["toast"]))
                pack["pins"].append({"id": sid, "ok": ok, "state": st})
                r.log("%-7s %s the chart shows %s: %d bars %s..%s, last close %s (%s: %d bars, %s)" % (sid, "CHARTS" if ok else "WRONG ", st["active"], st["bars"], st["first"], st["last"], st["close"], target, ref[target]["bars"], ref[target]["close"]))

            r.section("4. the reviewed index ids, opened the way a watchlist row or a search pick opens them")
            pack["routes"] = []
            for target, ids in TABLE.items():
                away()
                for n, sid in enumerate(ids):
                    st = show("route", sid, target, target)
                    ok = st["active"] == target and st["requested"] == sid and st["handoff"] == target and same(st, target) and "No supported chart route" not in st["text"]
                    ck(ok, "%s: active %s, requested %s, handoff %s, %d bars, last %s %s | %s" % (sid, st["active"], st["requested"], st["handoff"], st["bars"], st["last"], st["close"], st["text"][:120]))
                    pack["routes"].append({"id": sid, "target": target, "ok": ok, "from_another_chart": n == 0, "state": st})
                r.log("%-6s %d of %d ids chart it (%d bars to %s): %s" % (target, len([x for x in pack["routes"] if x["target"] == target and x["ok"]]), len(ids), ref[target]["bars"], ref[target]["last"], " ".join(ids)))
            away()
            st = show("route", "NASDAQ:NDX", "^NDX", "^NDX")
            ck(st["active"] == "^NDX" and same(st, "^NDX"), "NASDAQ:NDX: active %s, %d bars" % (st["active"], st["bars"]))
            r.log("NASDAQ:NDX (a US listing by its venue: the watchlist hands the engine NDX) -> the chart shows %s, %d bars to %s" % (st["active"], st["bars"], st["last"]))
            away()
            st = show("go", "DJI", "^DJI", "^DJI")
            ck(st["active"] == "^DJI" and same(st, "^DJI"), "DJI: active %s, %d bars, %s" % (st["active"], st["bars"], (st["quote"] or "")[:50]))
            r.log("DJI typed bare -> the chart shows %s, %d bars to %s, last close %s" % (st["active"], st["bars"], st["last"], st["close"]))
            for sid in UNROUTED:
                before = page.evaluate(STATE)
                page.evaluate("(s) => window.JHTvWatchlist.route(s)", sid)
                page.wait_for_timeout(900)
                st = page.evaluate(STATE)
                keep = st["requested"] == sid and not st["handoff"] and "No supported chart route; chart unchanged" in st["text"] and st["active"] == before["active"] and st["bars"] == before["bars"]
                ck(keep, "%s should still have no route: handoff %r, active %s | %s" % (sid, st["handoff"], st["active"], st["text"][:100]))
                r.log("%-18s still no route, chart left on %s: %s" % (sid, st["active"], keep))

            r.section("3. his ratio rows built on VANTAGE:SP500")
            pack["spreads"] = []
            for expr_id in spreads:
                away()
                st = show("route", expr_id, expr_id, expr_id.split("/")[0][:8], timeout=60000)
                page.wait_for_timeout(600)
                sp = page.evaluate(SPREAD, expr_id)
                sp["id"], sp["active"], sp["quote"] = expr_id, st["active"], st["quote"]
                sp["expected"] = arithmetic(sp.get("arithmetic"))
                market = [x for x in sp["legs"] if x["target"]]
                series_legs = [x for x in sp["legs"] if not x["target"]]
                value_ok = sp["value"] is not None and sp["expected"] is not None and abs(sp["value"] - sp["expected"]) <= 1e-9 * max(1.0, abs(sp["expected"]))
                ok = (st["active"] == expr_id.upper() and sp["label"] == expr_id.upper() and sp["n"] > MIN_SPREAD_BARS and (sp["last"] or "") >= week and value_ok and "VANTAGE:SP500 as ^GSPC" in sp["source"]
                      and all((x["last"] or "") >= week for x in market) and all(not re.match(r"^(FRED|ECONOMICS):", x["id"], re.I) for x in market) and all(re.match(r"^(FRED|ECONOMICS):", x["id"], re.I) for x in series_legs))
                ck(ok, "%s: active %s, label %s, %d points to %s, value %s vs %s, legs %s | %s" % (expr_id, st["active"], sp["label"], sp["n"], sp["last"], sp["value"], sp["expected"],
                                                                                              [(x["id"], x["target"], x["last"]) for x in sp["legs"]], sp["source"][:160]))
                pack["spreads"].append(dict(sp, ok=ok))
                r.log("%-32s %s %d points %s..%s, last %.6g = %s" % (expr_id, "DRAWS" if ok else "WRONG", sp["n"], sp["first"], sp["last"], sp["value"] or float("nan"),
                                                                    " ; ".join("%s%s %s on %s" % (x["id"], (" as " + x["target"]) if x["target"] else " from /series", x["value"], x["asof"]) for x in sp["legs"])))
                if expr_id.upper() == "VANTAGE:SP500/TVC:GOLD":
                    r.log("source line: %s" % sp["source"][:300])
                    shots["spread_gold_1440"] = page.screenshot(type="jpeg", quality=52)
            ck(len(spreads) >= 8, "only %d VANTAGE:SP500 expressions found in his lists" % len(spreads))

            r.section("5. the status line keeps its label")
            away()
            st = show("route", "VANTAGE:SP500", "^GSPC", "^GSPC")
            try:
                page.wait_for_function("() => (window.jhChartEvidence || {}).symbol === '^GSPC'", timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            al = page.evaluate(ALIAS)
            pack["alias"] = al
            ck(al["before"] == "^GSPC" and al["after"] == "^GSPC" and not al["shares_the_array_on_screen"] and "Retained bars displayed" in al["tail"], "label after two alias lookups and a repaint: %s" % al)
            r.log("VANTAGE:SP500 on screen (label %s); SPX and SP500 looked up under their own names (%s bars), then a repaint -> label %s, the lookups got their own copy: %s | ...%s" % (
                al["before"], al["alias_bars"], al["after"], not al["shares_the_array_on_screen"], al["tail"][-70:]))
            lag, opens = [], 0
            for sid, target in (("SP:SPX", "^GSPC"), ("TVC:NDX", "^NDX"), ("SPCFD:SPX", "^GSPC"), ("DJ:DJI", "^DJI"), ("SPREADEX:SPX", "^GSPC"), ("I:SPX", "^GSPC"), ("VANTAGE:SP500", "^GSPC"), ("TVC:VIX", "^VIX")):
                away()
                show("route", sid, target, target)
                page.wait_for_timeout(9000)                     # the quote line's own lookups and the live repaints happen in here
                st = page.evaluate(STATE)
                opens += 1
                if not (st["label"] == target and "Retained bars displayed" in st["text"]):
                    lag.append("%s (label %s)" % (sid, st["label"]))
            pack["label_lag"] = lag
            ck(not lag, "the status line lost its label on %d of %d opens: %s" % (len(lag), opens, lag))
            r.log("%d opens from another chart, each read 9 s after it was drawn: the status line carried 'Chart label' on %d; lost on %s" % (opens, opens - len(lag), lag))
            bad = [e for e in errors if "is not defined" in e or "TypeError" in e]
            ck(not bad, "page errors: %s" % sorted(set(bad))[:3])
            r.log("page errors: %d %s" % (len(errors), sorted(set(errors))[:3]))
            pack["page_errors"] = sorted(set(errors))[:10]
            ctx.close()

            r.section("at 390 px")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2, is_mobile=True, has_touch=True, user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari")
            page = ctx.new_page()
            page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_function("() => typeof window.jhWatchlistRoute === 'function' && !!(window.JHTvWatchlist && window.JHTvWatchlist.route) && !!window.JHChartExpr && (window.lastBars || []).length > 100", timeout=90000)
            page.wait_for_timeout(7000)
            st = show("go", "I:SPX", "I:SPX", "I:SPX")
            ck(st["active"] == "I:SPX" and same(st, "^GSPC"), "390 px I:SPX: active %s, %d bars" % (st["active"], st["bars"]))
            r.log("390 px: I:SPX -> the chart shows %s, %d bars to %s, last close %s" % (st["active"], st["bars"], st["last"], st["close"]))
            st = show("route", "VANTAGE:SP500/TVC:GOLD", "VANTAGE:SP500/TVC:GOLD", "VANTAGE:", timeout=60000)
            sp = page.evaluate(SPREAD, "VANTAGE:SP500/TVC:GOLD")
            ck(st["active"] == "VANTAGE:SP500/TVC:GOLD" and sp["n"] > MIN_SPREAD_BARS and (sp["last"] or "") >= week and "VANTAGE:SP500 as ^GSPC" in sp["source"], "390 px spread: active %s, %d points to %s" % (st["active"], sp["n"], sp["last"]))
            r.log("390 px: VANTAGE:SP500/TVC:GOLD -> %d points %s..%s, last %.6g" % (sp["n"], sp["first"], sp["last"], sp["value"] or float("nan")))
            shots["spread_gold_390"] = page.screenshot(type="jpeg", quality=55)
            ctx.close()
            browser.close()

        embed(r, "index-routes-proof", pack)
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(failures=str(len(fails)), index_ids=str(len(pack["routes"])), index_ids_charting=str(len([x for x in pack["routes"] if x["ok"]])), pins_charting=str(len([x for x in pack["pins"] if x["ok"]])),
             spreads=str(len(pack["spreads"])), spreads_drawing=str(len([x for x in pack["spreads"] if x["ok"]])), label_lost=str(len(pack["label_lag"])))
        if fails:
            r.fail("LIVE PROOF: %d failed checks - %s" % (len(fails), "; ".join(fails[:6])))
            raise SystemExit(1)
        r.ok("LIVE PROOF: sp500 leads with the index and its first row charts; %d pinned index ids, %d reviewed index ids and %d of his ratio rows draw from live index bars; the status line kept its label on every open" % (
            len(pack["pins"]), len(pack["routes"]), len(pack["spreads"])))


if __name__ == "__main__":
    main()
    sys.exit(0)
