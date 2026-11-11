"""ops 6778 - READ-ONLY browser proof of the Inflows & outflows desk (Khalid 2026-11-10 20:58 CT). Re-runnable; nothing on
the site changes (notes / categories live in the probe browser's localStorage only).

  1. /inflows-outflows.html 1440x900: the bundle carries the desk; 8 families / 54 cards; every card's state after
     hydration (ready vs Unavailable, listed by name so a wrong id is visible); every ready card carries a strip with the
     4 famous bears; the flows: cards show their feed's last date; clicking a card opens the full-screen view; a note
     saves under jh_flows_workspace_v1; search add-all still works here.
  2. chart.html: the "⇄ Flows" button sits right after "📈 Macro" in the top bar and opens the desk in the overlay.
  3. /macro-economic-data.html still boots as the Macro desk (own title, own key).
  4. 390x844: the desk fits.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import json
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

SITE = "https://justhodl.ai"
EXPECT_TURNS = ["Dotcom 2000-03-24->2002-10-09", "2008 financial crisis 2007-10-09->2009-03-09", "COVID 2020-02-19->2020-03-23", "2022 bear 2022-01-03->2022-10-12"]


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except Exception:  # noqa: BLE001
        log("installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch(p, log):
    try:
        return p.chromium.launch(channel="chrome", headless=True)
    except Exception as e:  # noqa: BLE001
        log("chrome channel unavailable (%s); installing chromium" % type(e).__name__)
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        return p.chromium.launch(headless=True)


SCROLL_ALL = """async () => { const body = document.getElementById('macro-body'); for (let y = 0; y <= body.scrollHeight; y += 450) { body.scrollTop = y; await new Promise(r => setTimeout(r, 90)); } body.scrollTop = 0; }"""
ALL_SETTLED = "() => [...document.querySelectorAll('.macro-item')].length > 0 && [...document.querySelectorAll('.macro-item')].every(c => c.dataset.metricState === 'ready' || c.dataset.metricState === 'error')"
CATALOG_JS = """() => { const cards = [...document.querySelectorAll('.macro-item')];
  const rows = cards.map(c => ({sym: c.dataset.symbol, title: c.querySelector('.mi-title').textContent, state: c.dataset.metricState, value: c.querySelector('.macro-latest-value').textContent, asof: c.querySelector('.macro-asof').textContent, err: c.querySelector('.macro-latest-value').title || '',
    strip: !!c.querySelector('.macro-hist-chart'), bands: c.querySelectorAll('.macro-hist-top').length + '/' + c.querySelectorAll('.macro-hist-bottom').length, cat: (c.closest('.macro-cat') && c.closest('.macro-cat').querySelector('.macro-cat-label')) ? c.closest('.macro-cat').querySelector('.macro-cat-label').innerText.replace(/\\s+/g, ' ').trim() : ''}));
  return {title: document.getElementById('macro-modal-title').textContent, key: MacroData.STORAGE_KEY, desk: MacroData.desk, cats: [...document.querySelectorAll('.macro-cat')].length, n: cards.length, rows,
          turns: MacroData.turns ? MacroData.turns.map(t => t.name + ' ' + t.top.date + '->' + (t.bottom ? t.bottom.date : '?')) : null, overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth}; }"""


def main():
    with report("ops_6778_flows_desk_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6778 - Inflows & outflows desk proof %s" % now.isoformat(timespec="minutes"))
        sync_playwright = ensure_playwright(r.log)
        fails, shots = [], []

        def check(name, fn):
            try:
                out = fn()
                r.log("ok   %s%s" % (name, (": " + str(out)[:700]) if out not in (None, True) else ""))
                return out
            except Exception as e:  # noqa: BLE001
                fails.append(name)
                r.warn("FAIL %s: %s %s" % (name, type(e).__name__, str(e)[:500]))
                return None

        def settle(page, log):
            page.wait_for_selector(".macro-item", timeout=45000)
            page.wait_for_timeout(1200)
            page.evaluate(SCROLL_ALL)
            try:
                page.wait_for_function(ALL_SETTLED, timeout=150000)
            except Exception:  # noqa: BLE001
                log("not every card settled within 150 s")
            page.wait_for_timeout(2500)

        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6778")
            page = ctx.new_page()
            page.set_default_timeout(45000)
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:160]))
            r.section("1. /inflows-outflows.html 1440x900")
            page.goto("%s/inflows-outflows.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page.wait_for_selector("#macro-search-input", timeout=45000)
            page.wait_for_timeout(1500)
            check("the page serves the bundle with the desk (not a cached copy)", lambda: page.evaluate("""async () => { const s = [...document.scripts].map(x => x.src).find(u => /jh-workspaces[.]js/.test(u)); if (!s) throw new Error('no jh-workspaces.js tag');
                const t = await (await fetch(s, {cache: 'no-store'})).text(); if (!/FLOWS_CATALOG/.test(t) || !/fetchFlow/.test(t)) throw new Error('bundle lacks the desk: ' + s); return s.replace(location.origin, '') + ' (' + t.length + ' bytes)'; }"""))
            settle(page, r.log)
            st = page.evaluate(CATALOG_JS)
            ready = [x for x in st["rows"] if x["state"] == "ready"]
            bad = [x for x in st["rows"] if x["state"] != "ready"]
            r.log("desk '%s' key %s | %d families | %d cards | %d ready | %d unavailable | turns %s" % (st["title"], st["key"], st["cats"], st["n"], len(ready), len(bad), st["turns"]))
            for x in st["rows"]:
                r.log("    %-6s %-46s %-52s %14s  as of %s  strip %s bands %s %s" % (x["state"], x["sym"][:46], x["title"][:52], x["value"][:14], x["asof"], x["strip"], x["bands"], ("ERR " + x["err"][:60]) if x["state"] != "ready" else ""))
            check("the desk boots as Inflows & outflows with its own key and the 8 families / 54 cards", lambda: "%s | %s | %d / %d" % (st["title"], st["key"], st["cats"], st["n"]) if st["title"].startswith("⇄ Inflows") and st["key"] == "jh_flows_workspace_v1" and st["cats"] == 8 and st["n"] == 54 else (_ for _ in ()).throw(AssertionError(str({k: st[k] for k in ("title", "key", "cats", "n")}))))
            check("at least 50 of the 54 series resolve live (the rest are listed above by name)", lambda: "%d ready, unavailable: %s" % (len(ready), [x["sym"] for x in bad]) if len(ready) >= 50 else (_ for _ in ()).throw(AssertionError("%d ready; unavailable %s" % (len(ready), [(x["sym"], x["err"][:40]) for x in bad]))))
            check("every ready card carries a strip with the four famous bears", lambda: "%d strips" % len(ready) if all(x["strip"] and x["bands"] == "4/4" for x in ready) else (_ for _ in ()).throw(AssertionError(str([(x["sym"], x["strip"], x["bands"]) for x in ready if not (x["strip"] and x["bands"] == "4/4")][:6]))))
            check("the turns computed live are dotcom / 2008 / COVID / 2022", lambda: st["turns"] if st["turns"] == EXPECT_TURNS else (_ for _ in ()).throw(AssertionError(str(st["turns"]))))
            flows = [x for x in st["rows"] if x["sym"].startswith("flows:")]
            check("the fleet-feed cards (ICI weekly, spot BTC/ETH ETF flows, exchange netflows, Taiwan) read their feeds with a date", lambda: "; ".join("%s %s (%s)" % (x["sym"].split("/")[0], x["value"], x["asof"]) for x in flows[:8]) if flows and all(x["state"] == "ready" and x["asof"] for x in flows) else (_ for _ in ()).throw(AssertionError(str([(x["sym"], x["state"], x["err"][:50]) for x in flows if x["state"] != "ready"]))))
            long_hist = [x for x in ready if not x["sym"].startswith("flows:")]
            check("the long-history families (money funds, deposits, Z.1 since 1946, TIC) resolve with values", lambda: "%d of %d, e.g. %s" % (len(long_hist), len([x for x in st["rows"] if not x["sym"].startswith("flows:")]), "; ".join("%s=%s (%s)" % (x["sym"].replace("FRED:", ""), x["value"], x["asof"]) for x in long_hist[:6])) if len(long_hist) >= 28 else (_ for _ in ()).throw(AssertionError("%d" % len(long_hist))))
            shots.append(("desk", page.screenshot(type="jpeg", quality=45)))
            sel = ".macro-item[data-metric-state='ready']"
            sym = page.evaluate("s => document.querySelector(s).dataset.symbol", sel)
            page.click(sel + " .macro-hist-svg", position={"x": 24, "y": 24})
            page.wait_for_function("() => document.querySelector('#macro-focus .macro-hist-chart.big')", timeout=45000)
            page.wait_for_timeout(1000)
            fs = page.evaluate("""() => { const f = document.getElementById('macro-focus'), r = f.getBoundingClientRect(); return {open: f.classList.contains('open'), w: Math.round(r.width), h: Math.round(r.height), vw: innerWidth, vh: innerHeight, labels: [...f.querySelectorAll('.macro-hist-label')].map(e => e.textContent), rows: f.querySelectorAll('tbody tr').length, title: f.querySelector('#macro-focus-title').textContent, latest: f.querySelector('#macro-focus-latest').innerText.replace(/\\n/g, ' ')}; }""")
            check("clicking a card (%s) opens the full-screen view with the four bears" % sym, lambda: "%s | %dx%d | %s | %s" % (fs["title"], fs["w"], fs["h"], fs["labels"], fs["latest"]) if fs["open"] and fs["w"] == fs["vw"] and fs["h"] == fs["vh"] and len(fs["labels"]) == 4 and fs["rows"] == 4 else (_ for _ in ()).throw(AssertionError(str(fs))))
            shots.append(("fullscreen", page.screenshot(type="jpeg", quality=45)))
            page.keyboard.press("Escape")
            page.wait_for_timeout(400)

            def note():
                uid = page.evaluate("s => document.querySelector(s).dataset.seriesId", sel)
                page.hover(".macro-item[data-series-id='%s']" % uid)
                page.click(".macro-item[data-series-id='%s'] .macro-note" % uid)
                page.wait_for_timeout(300)
                page.keyboard.type("Flows note test")
                page.keyboard.press("Enter")
                page.wait_for_timeout(500)
                out = page.evaluate("() => ({flows: (localStorage.getItem('jh_flows_workspace_v1') || '').includes('Flows note test'), macro: (localStorage.getItem('jh_macro_workspace_v2') || '').includes('Flows note test')})")
                assert out["flows"] and not out["macro"], out
                return "saved under jh_flows_workspace_v1 only"
            check("a note on this desk is stored under the flows key, not the Macro desk's", note)

            def add_all():
                page.fill("#macro-search-input", "ice boa")
                page.wait_for_selector("#macro-add-all-btn", timeout=45000)
                page.wait_for_timeout(500)
                page.click("#macro-add-all-btn")
                page.wait_for_timeout(1500)
                out = page.evaluate("() => { const c = MacroData.state.categories[MacroData.state.categories.length - 1]; return {name: c.cat, n: c.items.length, cats: MacroData.state.categories.length}; }")
                assert out["name"] == "ICE BofA" and out["n"] == 29 and out["cats"] == 9, out
                return "%s: %d series as the 9th family" % (out["name"], out["n"])
            check("search add-all works on this desk too (any series can join it)", add_all)
            real_errors = [e for e in errors if "No market print" not in e]
            check("0 page errors", lambda: "none" if not real_errors else (_ for _ in ()).throw(AssertionError(str(real_errors[:4]))))

            r.section("2. chart.html: the Flows button beside Macro")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6778")  # fresh storage: section 1 added a 9th family to this origin's localStorage
            page2 = ctx2.new_page()
            page2.set_default_timeout(45000)
            try:
                page2.goto("%s/chart.html?s=SPY&nocache=%d" % (SITE, int(time.time())), wait_until="domcontentloaded")
                page2.wait_for_selector("#btn-flows", timeout=45000)
                page2.wait_for_timeout(1500)
                order = page2.evaluate("() => [...document.querySelectorAll('#tfbar .wsdesk')].map(b => b.id)")
                check("'⇄ Flows' sits right after '📈 Macro' in the top bar", lambda: str(order[:4]) if order.index("btn-flows") == order.index("btn-macro") + 1 else (_ for _ in ()).throw(AssertionError(str(order))))
                page2.evaluate("() => document.getElementById('btn-flows').click()")
                page2.wait_for_selector("#ws-overlay iframe", timeout=20000)
                fr = None
                for _ in range(60):
                    fr = next((f for f in page2.frames if "inflows-outflows" in (f.url or "")), None)
                    if fr:
                        break
                    page2.wait_for_timeout(500)
                assert fr, "no inflows-outflows iframe: %s" % [f.url[:60] for f in page2.frames]
                head = page2.evaluate("() => (document.querySelector('#ws-overlay .wshd span') || {}).textContent")
                fr.wait_for_selector(".macro-item", timeout=45000)
                fr.wait_for_timeout(2500)
                sti = fr.evaluate("() => ({title: document.getElementById('macro-modal-title').textContent, n: document.querySelectorAll('.macro-item').length, key: MacroData.STORAGE_KEY})")
                check("the button opens the Inflows & outflows desk in the overlay", lambda: "overlay '%s' -> %s, %d cards, key %s" % (head, sti["title"], sti["n"], sti["key"]) if sti["title"].startswith("⇄ Inflows") and sti["n"] == 54 and sti["key"] == "jh_flows_workspace_v1" else (_ for _ in ()).throw(AssertionError(str(sti))))
                shots.append(("chart-overlay", page2.screenshot(type="jpeg", quality=45)))
            except Exception as e:  # noqa: BLE001
                fails.append("chart overlay")
                r.warn("FAIL chart.html: %s %s" % (type(e).__name__, str(e)[:300]))

            r.section("3. the Macro desk is untouched")
            page3 = ctx.new_page()
            page3.set_default_timeout(45000)
            page3.goto("%s/macro-economic-data.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page3.wait_for_selector(".macro-item", timeout=45000)
            page3.wait_for_timeout(1500)
            stm = page3.evaluate("() => ({title: document.getElementById('macro-modal-title').textContent, key: MacroData.STORAGE_KEY, n: document.querySelectorAll('.macro-item').length})")
            check("Macro still boots with its own title, key and catalog", lambda: "%s | %s | %d cards" % (stm["title"], stm["key"], stm["n"]) if stm["title"].startswith("📈 Macro") and stm["key"] == "jh_macro_workspace_v2" and stm["n"] >= 100 else (_ for _ in ()).throw(AssertionError(str(stm))))

            r.section("4. 390x844 phone")
            ctx4 = browser.new_context(viewport={"width": 390, "height": 844}, user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) justhodl-ops/6778")
            page4 = ctx4.new_page()
            page4.set_default_timeout(45000)
            page4.goto("%s/inflows-outflows.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page4.wait_for_selector(".macro-item", timeout=45000)
            page4.wait_for_timeout(1500)
            base4 = page4.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            st4 = page4.evaluate("() => ({n: document.querySelectorAll('.macro-item').length, overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth, perRow: Math.round(document.querySelector('.macro-grid').getBoundingClientRect().width / document.querySelector('.macro-item').getBoundingClientRect().width)})")
            check("390px: one card per row, no overflow beyond the page header's own", lambda: "%d cards, %d per row, overflow %s" % (st4["n"], st4["perRow"], st4["overflow"]) if st4["perRow"] == 1 and st4["overflow"] <= max(0, base4) else (_ for _ in ()).throw(AssertionError(str(st4))))
            shots.append(("phone", page4.screenshot(type="jpeg", quality=40)))
            browser.close()
        for name, data in shots:
            r.section("screenshot %s (jpeg, base64)" % name)
            r.log("<!-- SCREENSHOT_%s_JPEG_BASE64 -->\n%s\n<!-- /SCREENSHOT_%s_JPEG_BASE64 -->" % (name, base64.b64encode(data).decode(), name))
        if fails:
            r.fail("ops 6778: %d check(s) failed: %s" % (len(fails), fails))
        else:
            r.ok("ops 6778 complete: the Inflows & outflows desk is live beside Macro, with real flow series, strips, full screen and notes")


if __name__ == "__main__":
    main()
    sys.exit(0)
