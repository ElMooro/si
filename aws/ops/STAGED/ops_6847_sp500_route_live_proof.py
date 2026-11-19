"""ops 6847 - READ-ONLY live proof: VANTAGE:SP500 (and the other S&P 500 cash-index ids TradingView's search returns)
charts the S&P 500 index on chart.html; plus three measurements of what the audit (ops 6846) found next to it.

Khalid 2026-11-19: "investigate this: Requested: VANTAGE:SP500 - Resolved route: unavailable ... im trying to pull sp500
and i cant seem to be able to pull it out. i can pull spy fine no problem".

CLAUDE.md "CHECK YOUR WORK": a green deploy is not the proof. This op opens the live site in Chrome on the runner and
  A. proof of the change
     1. confirms the new build is what is served;
     2. opens each of the 24 ids through the function a search pick and a watchlist row call (JHTvWatchlist.route) and requires the S&P 500 index chart
        (label ^GSPC, the same bars as SPX, a last bar from this week);
     3. requires the neighbours to be as they were (a futures CFD, a same-named foreign stock, SPY, TVC:SPX);
     4. walks the two real paths: the Symbol Search dialog row and a watchlist row;
     5. requires no "L is not defined" page error and the details card's History line on the index;
     6. repeats his id at 390 px.
  B. measurements, nothing judged
     1. what /series answers for the legs of his VANTAGE:SP500 spreads;
     3. one of those spreads, NASDAQ:AAPL, AMEX:SPY and the directory's pinned index row I:SPX, opened live.
Writes nothing (the watchlist row is added in the runner's own throwaway browser profile).
"""
from datetime import datetime, timedelta, timezone
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
IDS = ["VANTAGE:SP500", "SP:SPX", "SPCFD:SPX", "SPREADEX:SPX", "BLUEBERRY:SP500", "DARWINEX:SP500", "CAPITALCOM:SPX500", "FOREXCOM:SPX500", "BLACKBULL:SPX500", "FX:SPX500", "OANDA:SPX500USD",
       "IC:US500", "CFI:US500", "PEPPERSTONE:US500", "BLACKBULL:US500", "FPMARKETS:US500", "TICKMILL:US500", "FUSIONMARKETS:US500", "GBEBROKERS:US500", "TRADENATION:US500", "PURPLETRADING:US500",
       "ERRANTE:US500", "GOMARKETS:US500", "EQUITI:US500"]
FULL = ["VANTAGE:SP500", "SP:SPX", "SPCFD:SPX", "PEPPERSTONE:US500"]          # switched away from and back to, so the load is real
UNROUTED = ["VANTAGE:SP500FT", "LSE:SPX", "CME:ES", "PEPPERSTONE:US500.P"]
MIN_INDEX_BARS, MIN_SPY_BARS = 10000, 5000
SERIES = ["VANTAGE:SP500", "SP:SPX", "SPCFD:SPX", "TVC:GOLD", "TVC:VIX", "NASDAQ:TLT", "I:SPX"]


def get(path, stamp, as_json=True):
    raw = urllib.request.urlopen(urllib.request.Request("%s/%s%snocache=%d" % (BASE, path, "&" if "?" in path else "?", stamp), headers=UA), timeout=90).read()
    return json.loads(raw) if as_json else raw.decode("utf-8", "replace")


def series(sid):
    """What the directory answers for one id: a spread asks it for every leg and needs observations back."""
    out = {"id": sid}
    try:
        d = json.loads(urllib.request.urlopen(urllib.request.Request("%s/series?id=%s" % (PROXY, urllib.parse.quote(sid, safe="")), headers=UA), timeout=40).read())
    except Exception as exc:  # noqa: BLE001
        body = ""
        try:
            body = exc.read().decode("utf-8", "replace")[:200]       # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        out.update(verdict="no history", error=("%s %s" % (str(exc)[:70], body)).strip())
        return out
    ohlc, obs = d.get("ohlc"), d.get("obs")
    n = len(obs) if isinstance(obs, list) else 0
    out.update(reported_id=d.get("id"), via=d.get("via"), freq=d.get("freq"), n_ohlc=len(ohlc) if isinstance(ohlc, list) else None, n_obs=n, source=str(d.get("source"))[:80],
               error=str(d.get("error"))[:200] if d.get("error") else None, first=obs[0][0] if n else None, last=obs[-1][0] if n else None)
    out["verdict"] = "answers" if n else "no history"
    return out


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


OPEN = r"""(id) => { const out = { id: id }; try { window.JHTvWatchlist.route(id); } catch (e) { out.threw = String(e && e.message || e); }
  const n = document.getElementById('watchlist-chart-route'); out.handoff = n && !n.hidden && n.dataset.requested === id ? n.dataset.handoff : null; out.text = n ? n.textContent : ''; return out; }"""
STATE = r"""() => { const n = document.getElementById('watchlist-chart-route'), d = window.lastBars || [], ev = window.jhChartEvidence || {}, h = window.jhWatchlistHandoff || null;
  const t = (id) => { const e = document.getElementById(id); return e ? (e.innerText || e.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 200) : null; };
  const bar = (b) => b ? { time: b.time, open: b.open, high: b.high, low: b.low, close: b.close } : null;
  return { hidden: n ? n.hidden : null, requested: n ? n.dataset.requested : null, handoff: n ? n.dataset.handoff : null, text: n ? n.textContent : '', bars: d.length, first: bar(d[0]), last: bar(d[d.length - 1]),
    label: ev.symbol || null, source: String(ev.source || '').slice(0, 90), relation: h ? h.relation : null, active: window.jhWatchlistActive ? window.jhWatchlistActive() : null, quote: t('quote'),
    history: Array.from(document.querySelectorAll('.tvd-ks')).map(e => (e.innerText || '').replace(/\s+/g, ' ').trim()).filter(x => /History/.test(x)).slice(-1)[0] || null }; }"""
ARRIVED = "() => { const n = document.getElementById('watchlist-chart-route'); const t = n ? n.textContent : ''; return (window.jhChartEvidence || {}).symbol === '^GSPC' && /Retained bars displayed/.test(t); }"
LOADED = r"""(h) => { const ev = window.jhChartEvidence || {}, n = document.getElementById('watchlist-chart-route'), t = n && !n.hidden ? n.textContent : '';
  return (String(ev.symbol || '').toUpperCase() === h && (window.lastBars || []).length > 0 && /Retained bars displayed/.test(t)) || /Unavailable: no accepted plotted bars/.test(t); }"""
SETTLED = "() => { const n = document.getElementById('watchlist-chart-route'); const t = n ? n.textContent : ''; return !n || n.hidden || /Retained bars displayed|Unavailable: no accepted plotted bars/.test(t); }"
ROWS = r"""() => Array.from(document.querySelectorAll('#jhus .us-row[data-i]')).slice(0, 40).map(e => ({ id: e.getAttribute('title'), text: (e.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 130) }))"""


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
    with report("ops_6847_sp500_route_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6847 - VANTAGE:SP500 charts the S&P 500 index, live %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        fails, pack, shots = [], {"at": now.isoformat(), "routes": [], "series": [], "paths": {}}, {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        watch, details = get("jh-chart-tvwatch.js", stamp, False), get("jh-tv-details.js", stamp, False)
        new = "spxCash" in watch and '"VANTAGE:SP500": 1' in watch and 'fdate(day(L0.time)) + "</span><span>Observations' in details
        r.log("served: jh-chart-tvwatch.js %d bytes, jh-tv-details.js %d bytes - the new build is live: %s" % (len(watch), len(details), new))
        if not new:
            r.fail("the served site is not the new build yet (Pages deploy pending?)")
            raise SystemExit(1)
        week = (now - timedelta(days=5)).strftime("%Y-%m-%d")

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_function("() => typeof window.jhWatchlistOpen === 'function' && !!(window.JHTvWatchlist && window.JHTvWatchlist.route) && (window.lastBars || []).length > 100", timeout=90000)
            page.wait_for_timeout(7000)

            def open_id(s, settle=True):
                o = page.evaluate(OPEN, s)
                if "Symbol map loading" in (o.get("text") or ""):
                    page.wait_for_timeout(5000)
                    o = page.evaluate(OPEN, s)
                if o.get("handoff") and settle:
                    try:
                        page.wait_for_function(LOADED, arg=o["handoff"].upper(), timeout=16000)
                    except Exception:  # noqa: BLE001
                        pass
                    page.wait_for_timeout(900)
                st = page.evaluate(STATE)
                st["open_handoff"] = o.get("handoff")
                return st

            r.section("A2. each id, opened the way a pick or a row opens it")
            ref = open_id("SPX")
            ck(ref["label"] == "^GSPC" and ref["bars"] > MIN_INDEX_BARS and day(ref["last"]) >= week, "control SPX: label %s, %d bars, last %s" % (ref["label"], ref["bars"], day(ref["last"])))
            r.log("control SPX -> chart label %s, %d bars %s..%s, last close %s, source %s" % (ref["label"], ref["bars"], day(ref["first"]), day(ref["last"]), (ref["last"] or {}).get("close"), ref["source"]))
            pack["control"] = ref
            for s in IDS:
                if s in FULL:
                    away = open_id("SPY")
                    ck(away["label"] == "SPY", "%s: the chart did not switch away to SPY first (label %s)" % (s, away["label"]))
                st = open_id(s)
                good = (st["open_handoff"] == "^GSPC" and st["label"] == "^GSPC" and st["active"] == "^GSPC" and st["bars"] == ref["bars"] and (st["last"] or {}).get("close") == (ref["last"] or {}).get("close")
                        and day(st["last"]) == day(ref["last"]) and "Retained bars displayed" in st["text"] and st["text"].startswith("Requested: %s " % s) and "No supported chart route" not in st["text"])
                ck(good, "%s: handoff %s, label %s, active %s, %d bars, last %s %s | %s" % (s, st["open_handoff"], st["label"], st["active"], st["bars"], day(st["last"]), (st["last"] or {}).get("close"), st["text"][:160]))
                pack["routes"].append({"id": s, "ok": good, "state": st})
                r.log("%-20s %s handoff %s | chart label %s, %d bars %s..%s, last close %s%s" % (s, "CHARTS" if good else "WRONG ", st["open_handoff"], st["label"], st["bars"], day(st["first"]), day(st["last"]),
                                                                                             (st["last"] or {}).get("close"), " (after switching away to SPY)" if s in FULL else ""))
                if s == "VANTAGE:SP500":
                    r.log("status line: %s" % st["text"][:520])
                    r.log("quote line: %s" % (st["quote"] or "")[:200])
                    ck(st["history"] and "Observations" in st["history"], "the details card has no History line on the index: %r" % st["history"])
                    r.log("details card: %s" % st["history"])
                    shots["vantage_sp500_1440"] = page.screenshot(type="jpeg", quality=55)

            r.section("A3. the neighbours are as they were")
            for s in UNROUTED:
                before = page.evaluate(STATE)
                st = open_id(s, settle=False)
                same = st["open_handoff"] in (None, "") and "No supported chart route; chart unchanged" in st["text"] and st["label"] == before["label"] and st["bars"] == before["bars"]
                ck(same, "%s should still have no route and leave the chart alone: handoff %r, label %s, %s" % (s, st["open_handoff"], st["label"], st["text"][:120]))
                r.log("%-20s still no route, chart left on %s (%d bars): %s" % (s, st["label"], st["bars"], same))
            spy = open_id("SPY")
            ck(spy["label"] == "SPY" and spy["bars"] > MIN_SPY_BARS and day(spy["last"]) >= week, "SPY: label %s, %d bars, last %s" % (spy["label"], spy["bars"], day(spy["last"])))
            r.log("SPY                  chart label %s, %d bars %s..%s, last close %s" % (spy["label"], spy["bars"], day(spy["first"]), day(spy["last"]), (spy["last"] or {}).get("close")))
            tvc = open_id("TVC:SPX")
            ck(tvc["label"] == "^GSPC" and tvc["bars"] == ref["bars"], "TVC:SPX: label %s, %d bars" % (tvc["label"], tvc["bars"]))
            r.log("TVC:SPX              chart label %s, %d bars" % (tvc["label"], tvc["bars"]))

            r.section("A4. the two real paths")
            open_id("SPY")
            page.evaluate("() => window.jhOpenSearch('sp500')")
            page.wait_for_timeout(9000)
            typed = page.evaluate(ROWS)
            pack["paths"]["dialog_sp500"] = typed
            at = [i + 1 for i, x in enumerate(typed) if str(x["id"]).upper() in IDS]
            r.log("Symbol Search, typed sp500 -> %d rows read; rows that are now chartable S&P 500 index ids sit at %s; first rows: %s" % (len(typed), at[:8], " | ".join(str(x["id"]) for x in typed[:5])))
            page.evaluate("() => { if (window.JHUniSearch && window.JHUniSearch.close) window.JHUniSearch.close(); }")
            page.wait_for_timeout(400)
            page.evaluate("() => window.jhOpenSearch('VANTAGE:SP500')")
            page.wait_for_timeout(9000)
            rows = page.evaluate(ROWS)
            pack["paths"]["dialog_rows"] = rows
            r.log("Symbol Search, typed VANTAGE:SP500 -> %d rows; first: %s" % (len(rows), " | ".join("%s" % x["id"] for x in rows[:6])))
            hit = [x for x in rows if str(x["id"]).upper() == "VANTAGE:SP500"]
            if hit:
                page.evaluate("() => { const e = Array.from(document.querySelectorAll('#jhus .us-row[data-i]')).find(x => String(x.getAttribute('title')).toUpperCase() === 'VANTAGE:SP500'); (e.querySelector('.us-sym') || e).click(); }")
                how = "clicked the dialog's row %r" % hit[0]["text"][:80]
            else:
                page.evaluate("() => { if (window.JHUniSearch && window.JHUniSearch.close) window.JHUniSearch.close(); window.JHTvWatchlist.route('VANTAGE:SP500'); }")
                how = "the dialog listed no plain VANTAGE:SP500 row among %d; called the function a pick calls (JHTvWatchlist.route)" % len(rows)
            try:
                page.wait_for_function(ARRIVED, timeout=15000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(900)
            st = page.evaluate(STATE)
            ck(st["label"] == "^GSPC" and st["bars"] == ref["bars"] and st["requested"] == "VANTAGE:SP500", "search path: label %s, %d bars, requested %s (%s)" % (st["label"], st["bars"], st["requested"], how))
            r.log("search path: %s -> chart label %s, %d bars, last %s" % (how, st["label"], st["bars"], day(st["last"])))
            pack["paths"]["search"] = {"how": how, "state": st}

            open_id("SPY")
            added = page.evaluate("() => { try { return String(window.JHTvWatchlist.add('VANTAGE:SP500', { name: 'S&P 500 (Vantage)', src: 'TradingView' })); } catch (e) { return 'threw ' + e.message; } }")
            page.wait_for_timeout(2500)
            row = page.evaluate("() => { const e = Array.from(document.querySelectorAll('.wl-row[data-id]')).find(x => String(x.getAttribute('data-id')).toUpperCase() === 'VANTAGE:SP500'); if (!e) return null; e.scrollIntoView({ block: 'center' }); e.click(); return (e.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 90); }")
            if row is None:
                r.log("watchlist path: add() answered %s but no row for it is painted in the pane; not judged" % added)
            else:
                try:
                    page.wait_for_function(ARRIVED, timeout=15000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(900)
                st = page.evaluate(STATE)
                ck(st["label"] == "^GSPC" and st["bars"] == ref["bars"] and st["requested"] == "VANTAGE:SP500", "watchlist row: label %s, %d bars, requested %s" % (st["label"], st["bars"], st["requested"]))
                r.log("watchlist path: added it to the active list (%s), clicked its row %r -> chart label %s, %d bars, last %s" % (added, row, st["label"], st["bars"], day(st["last"])))
                pack["paths"]["row"] = {"added": added, "row": row, "state": st}
                shots["watchlist_row_1440"] = page.screenshot(type="jpeg", quality=55)

            r.section("A5. page errors")
            bad = [e for e in errors if "L is not defined" in e]
            ck(not bad, "%d 'L is not defined' page errors" % len(bad))
            r.log("page errors so far: %d (%s); 'L is not defined': %d" % (len(errors), sorted(set(errors))[:3], len(bad)))

            r.section("B3. opened live, not judged")
            for s in ("VANTAGE:SP500/TVC:GOLD", "NASDAQ:AAPL", "AMEX:SPY"):
                open_id("SPY")
                open_id(s, settle=False)
                page.wait_for_timeout(8000)
                st = page.evaluate(STATE)
                pack["paths"][s] = st
                r.log("%-24s handoff %s | chart label %s, %d bars, last %s | %s | %s" % (s, st["handoff"] if st["requested"] == s else None, st["label"], st["bars"], day(st["last"]), (st["quote"] or "")[:70], st["text"].split(" \u00b7 ")[3][:80] if st["text"].count(" \u00b7 ") >= 3 else ""))
            open_id("SPY")
            page.evaluate("() => window.jhGoSymbol && window.jhGoSymbol('I:SPX', 'chart')")
            page.wait_for_timeout(9000)
            st = page.evaluate(STATE)
            pack["paths"]["I:SPX"] = st
            r.log("%-24s (the directory's pinned row, opened the way the dialog opens it) chart label %s, active %s, %d bars, last %s | %s" % ("I:SPX", st["label"], st["active"], st["bars"], day(st["last"]), (st["quote"] or "")[:80]))
            pack["page_errors"] = sorted(set(errors))[:10]
            ctx.close()

            r.section("A6. his id at 390 px")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2, is_mobile=True, has_touch=True, user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari")
            page = ctx.new_page()
            perr = []
            page.on("pageerror", lambda e: perr.append(str(e)[:200]))
            page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_function("() => typeof window.jhWatchlistOpen === 'function' && !!(window.JHTvWatchlist && window.JHTvWatchlist.route) && (window.lastBars || []).length > 100", timeout=90000)
            page.wait_for_timeout(7000)
            st = open_id("VANTAGE:SP500")
            ck(st["open_handoff"] == "^GSPC" and st["label"] == "^GSPC" and st["bars"] == ref["bars"], "390 px: handoff %s, label %s, %d bars" % (st["open_handoff"], st["label"], st["bars"]))
            ck(not [e for e in perr if "L is not defined" in e], "390 px: 'L is not defined' page error")
            r.log("390 px: VANTAGE:SP500 -> handoff %s, chart label %s, %d bars, last %s close %s; page errors %d" % (st["open_handoff"], st["label"], st["bars"], day(st["last"]), (st["last"] or {}).get("close"), len(perr)))
            shots["vantage_sp500_390"] = page.screenshot(type="jpeg", quality=55)
            ctx.close()
            browser.close()

        r.section("B1. what /series answers for the legs of his VANTAGE:SP500 spreads (a spread asks /series for every leg)")
        for s in SERIES:
            x = series(s)
            pack["series"].append(x)
            r.log("%-14s %-10s %s observations %s..%s, served as %s%s%s" % (s, x["verdict"], x.get("n_obs"), x.get("first"), x.get("last"), x.get("reported_id"), (" through " + str(x["via"])) if x.get("via") else "",
                                                                    (" | " + x["error"][:150]) if x.get("error") else ""))

        embed(r, "sp500-route-proof", pack)
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(failures=str(len(fails)), ids=str(len(IDS)), charting=str(len([x for x in pack["routes"] if x["ok"]])), index_bars=str(ref["bars"]), index_last=day(ref["last"]),
             spread_leg=str(next((x["verdict"] for x in pack["series"] if x["id"] == "VANTAGE:SP500"), "")))
        if fails:
            r.fail("LIVE PROOF: %d failed checks - %s" % (len(fails), "; ".join(fails[:6])))
            raise SystemExit(1)
        r.ok("LIVE PROOF: all %d S&P 500 cash-index ids chart the index (%d bars to %s) on the live chart at 1440 px, his id also at 390 px; neighbours unchanged" % (len(IDS), ref["bars"], day(ref["last"])))


if __name__ == "__main__":
    main()
    sys.exit(0)
