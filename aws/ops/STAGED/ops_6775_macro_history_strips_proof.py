"""ops 6775 - READ-ONLY browser proof of the Macro desk history strips, full-screen view and notes (Khalid 2026-11-10 evening:
"historical chart under every single data point starting from 1996 highlighting the famous turns, tops green, bottoms
red" / "click on that data set -> full screen" / "a sentence that's mine, editable, under each metric" / "check for bugs").
Re-runnable; nothing on the site changes (notes and categories live in the probe browser's localStorage only).

Against https://justhodl.ai/macro-economic-data.html (what the chart.html Macro button opens) in headless Chrome:
  1440x900: the bundle carries the feature; every card that has data carries a strip with 4 green + 4 red lines; the
  turns computed live from the S&P 500 are dotcom / 2008 / COVID / 2022 with the known dates; a strip's caption names
  them; clicking a card opens the full-screen view filling the viewport (big chart, 4 labels, 8 dots, 4-row table, the
  latest value); Esc closes and returns focus; the note: add, save (Enter), survive a reload, edit from the full-screen
  view, REMOVE; ⇄ and + ADD untouched; search add-all regression; 0 page errors. chart.html: the Macro button's overlay
  iframe shows the strips. 390x844: the strips and the full-screen view fit, no new overflow.
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
NOTE = "Watch this one: it led every top since 2000."
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
CATALOG_JS = """() => { const cards = [...document.querySelectorAll('.macro-item')]; const ready = cards.filter(c => c.dataset.metricState === 'ready'); const err = cards.filter(c => c.dataset.metricState === 'error');
  const withStrip = cards.filter(c => c.querySelector('.macro-hist-chart')); const bands = withStrip.map(c => c.querySelectorAll('.macro-hist-top').length + '/' + c.querySelectorAll('.macro-hist-bottom').length);
  const bandCounts = {}; bands.forEach(b => { bandCounts[b] = (bandCounts[b] || 0) + 1; });
  const first = withStrip[0]; const r = first ? first.querySelector('.macro-hist-chart').getBoundingClientRect() : null;
  return {n: cards.length, ready: ready.length, error: err.length, errNames: err.slice(0, 6).map(c => c.dataset.symbol), withStrip: withStrip.length, bandCounts, strip: r ? Math.round(r.width) + 'x' + Math.round(r.height) : null,
          perRow: Math.round(document.querySelector('.macro-grid').getBoundingClientRect().width / document.querySelector('.macro-item').getBoundingClientRect().width),
          turns: MacroData.turns ? MacroData.turns.map(t => t.name + ' ' + t.top.date + '->' + (t.bottom ? t.bottom.date : '?')) : null, turnsAsOf: MacroData.turnsAsOf || null,
          caps: withStrip.slice(0, 3).map(c => c.querySelector('.mi-title').textContent + ': ' + c.querySelector('.macro-hist-cap').textContent),
          notes: cards.filter(c => c.querySelector('.macro-note')).length, overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth}; }"""
FOCUS_JS = """() => { const f = document.getElementById('macro-focus'), r = f.getBoundingClientRect(); const svg = f.querySelector('.macro-focus-chart svg');
  return {open: f.classList.contains('open'), w: Math.round(r.width), h: Math.round(r.height), vw: innerWidth, vh: innerHeight, big: !!f.querySelector('.macro-hist-chart.big'), labels: [...f.querySelectorAll('.macro-hist-label')].map(e => e.textContent),
          rows: f.querySelectorAll('tbody tr').length, rowsText: [...f.querySelectorAll('tbody tr')].map(tr => tr.innerText.replace(/\\s+/g, ' ').slice(0, 90)), title: f.querySelector('#macro-focus-title').textContent, latest: f.querySelector('#macro-focus-latest').innerText.replace(/\\n/g, ' '),
          dots: f.querySelectorAll('.macro-hist-dot').length, valuesPresent: [...f.querySelectorAll('tbody tr')].reduce((n, tr) => n + [tr.children[2], tr.children[4]].filter(td => td && td.textContent.trim() !== '—').length, 0),
          chartH: svg ? Math.round(svg.getBoundingClientRect().height) : 0, overflow: f.scrollWidth - f.clientWidth, focused: document.activeElement && document.activeElement.id, note: !!document.getElementById('macro-focus-note')}; }"""


def main():
    with report("ops_6775_macro_history_strips_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6775 - Macro desk history strips / full screen / notes proof %s" % now.isoformat(timespec="minutes"))
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
                page.wait_for_function(ALL_SETTLED, timeout=120000)
            except Exception:  # noqa: BLE001
                log("not every card settled within 120 s")
            page.wait_for_timeout(2500)

        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6775")
            page = ctx.new_page()
            page.set_default_timeout(45000)
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:160]))
            r.section("1. /macro-economic-data.html 1440x900")
            page.goto("%s/macro-economic-data.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page.wait_for_selector("#macro-search-input", timeout=45000)
            page.wait_for_timeout(1500)
            check("the page serves the bundle with the feature (not a cached copy)", lambda: page.evaluate("""async () => { const s = [...document.scripts].map(x => x.src).find(u => /jh-workspaces[.]js/.test(u)); if (!s) throw new Error('no jh-workspaces.js tag');
                const t = await (await fetch(s, {cache: 'no-store'})).text(); if (!/historySvg/.test(t) || !/openFocus/.test(t) || !/editNote/.test(t)) throw new Error('bundle lacks the feature: ' + s); return s.replace(location.origin, '') + ' (' + t.length + ' bytes)'; }"""))
            settle(page, r.log)
            base_overflow = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            st = page.evaluate(CATALOG_JS)
            r.log("catalog: %d cards | %d ready | %d error %s | %d strips | bands %s | strip %s | %d per row | turns as of %s | %d note slots" % (st["n"], st["ready"], st["error"], st["errNames"], st["withStrip"], json.dumps(st["bandCounts"]), st["strip"], st["perRow"], st["turnsAsOf"], st["notes"]))
            for cap in st["caps"]:
                r.log("    " + cap)
            check("every card with data carries a history strip", lambda: "%d strips for %d ready cards (of %d)" % (st["withStrip"], st["ready"], st["n"]) if st["withStrip"] == st["ready"] and st["ready"] >= 20 else (_ for _ in ()).throw(AssertionError("%s" % {k: st[k] for k in ("n", "ready", "error", "withStrip")})))
            check("every strip marks the four famous bears: 4 green tops + 4 red bottoms", lambda: st["bandCounts"] if list(st["bandCounts"].keys()) == ["4/4"] else (_ for _ in ()).throw(AssertionError(str(st["bandCounts"]))))
            check("the turns computed live from the S&P 500 are dotcom / 2008 / COVID / 2022 with the known dates", lambda: "%s (closes through %s)" % (st["turns"], st["turnsAsOf"]) if st["turns"] == EXPECT_TURNS else (_ for _ in ()).throw(AssertionError(str(st["turns"]))))
            check("cards are 3 per row with a 132 px strip (the sample's size)", lambda: "%s, %d per row" % (st["strip"], st["perRow"]) if st["perRow"] == 3 and st["strip"] and st["strip"].endswith("x132") else (_ for _ in ()).throw(AssertionError("%s %s" % (st["strip"], st["perRow"]))))
            check("every card has its note slot", lambda: st["notes"] if st["notes"] == st["n"] else (_ for _ in ()).throw(AssertionError("%d of %d" % (st["notes"], st["n"]))))
            shots.append(("catalog", page.screenshot(type="jpeg", quality=45)))
            sel = ".macro-item[data-metric-state='ready']"
            uid = page.evaluate("s => document.querySelector(s).dataset.seriesId", sel)
            sym = page.evaluate("s => document.querySelector(s).dataset.symbol", sel)
            page.click(sel + " .macro-hist-svg", position={"x": 24, "y": 24})
            page.wait_for_function("() => document.querySelector('#macro-focus .macro-hist-chart.big')", timeout=45000)
            page.wait_for_timeout(1000)
            fs = page.evaluate(FOCUS_JS)
            r.log("full screen (%s): %s" % (sym, json.dumps({k: fs[k] for k in ("w", "h", "vw", "vh", "labels", "rows", "dots", "chartH", "overflow", "focused", "latest")})))
            for row in fs["rowsText"]:
                r.log("    " + row)
            check("clicking a card opens the full-screen view filling the viewport", lambda: "%dx%d of %dx%d" % (fs["w"], fs["h"], fs["vw"], fs["vh"]) if fs["open"] and fs["w"] == fs["vw"] and fs["h"] == fs["vh"] else (_ for _ in ()).throw(AssertionError(str(fs))))
            # a series that starts after a turn (1M Treasury begins 2001-07) has no dot and a "—" value cell for it: one dot per value present
            check("full-screen view: big chart, the 4 bears labelled, a dot for every turn the series existed at, a 4-row table, the latest value, close button focused",
                  lambda: "%s | %d dots for %d values | %s" % (fs["labels"], fs["dots"], fs["valuesPresent"], fs["latest"]) if fs["big"] and len(fs["labels"]) == 4 and fs["rows"] == 4 and fs["dots"] == fs["valuesPresent"] and "as of" in fs["latest"] and fs["focused"] == "macro-focus-close" and fs["overflow"] <= 0 and fs["note"] else (_ for _ in ()).throw(AssertionError(str(fs))))
            shots.append(("fullscreen", page.screenshot(type="jpeg", quality=45)))
            page.keyboard.press("Escape")
            page.wait_for_timeout(400)
            check("Esc closes the full-screen view and returns focus to the card", lambda: page.evaluate("() => { const open = document.getElementById('macro-focus').classList.contains('open'); const back = document.activeElement && document.activeElement.classList.contains('macro-item'); if (open || !back) throw new Error('open ' + open + ' back ' + back); return 'closed, focus on the card'; }"))
            page.keyboard.press("Enter")
            page.wait_for_timeout(700)
            check("Enter on the focused card opens it again; × closes", lambda: (page.evaluate("() => { if (!document.getElementById('macro-focus').classList.contains('open')) throw new Error('not open'); document.getElementById('macro-focus-close').click(); return true; }"), page.wait_for_timeout(300), page.evaluate("() => { if (document.getElementById('macro-focus').classList.contains('open')) throw new Error('still open'); return 'opened by Enter, closed by ×'; }"))[-1])

            def add_note():
                page.hover(".macro-item[data-series-id='%s']" % uid)
                page.click(".macro-item[data-series-id='%s'] .macro-note" % uid)
                page.wait_for_timeout(300)
                st1 = page.evaluate("uid => { const n = document.querySelector('.macro-item[data-series-id=\"' + uid + '\"] .macro-note'); return {editing: n.classList.contains('editing'), ta: !!n.querySelector('textarea'), focusOpen: document.getElementById('macro-focus').classList.contains('open'), draggable: n.closest('.macro-item').getAttribute('draggable')}; }", uid)
                assert st1["editing"] and st1["ta"] and not st1["focusOpen"] and st1["draggable"] == "false", st1
                page.keyboard.type(NOTE)
                page.keyboard.press("Enter")
                page.wait_for_timeout(500)
                st2 = page.evaluate("uid => { const n = document.querySelector('.macro-item[data-series-id=\"' + uid + '\"] .macro-note'); const st = JSON.parse(localStorage.getItem('jh_macro_workspace_v2')); let note = null; st.categories.forEach(c => c.items.forEach(i => { if (i.uid === uid) note = i.note; })); return {text: n.textContent.trim(), stored: note, draggable: n.closest('.macro-item').getAttribute('draggable'), toast: (document.getElementById('jh-toast') || {}).innerText}; }", uid)
                assert st2["text"] == NOTE and st2["stored"] == NOTE and st2["draggable"] == "true", st2
                return "editor opened without the full screen, Enter saved '%s' (toast '%s'), stored in jh_macro_workspace_v2" % (st2["text"], st2["toast"])
            check("the note: click, type, Enter -> saved under the metric", add_note)
            page.reload(wait_until="load")
            page.wait_for_selector(".macro-item", timeout=45000)
            page.wait_for_timeout(1200)
            check("the note survives a reload", lambda: page.evaluate("uid => { const t = ((document.querySelector('.macro-item[data-series-id=\"' + uid + '\"] .macro-note') || {}).textContent || '').trim(); if (t !== %r) throw new Error('got: ' + t); return t; }" % NOTE, uid))
            settle(page, r.log)

            def edit_on_big_view():
                page.click(".macro-item[data-series-id='%s'] .macro-hist-svg" % uid, position={"x": 24, "y": 24})
                page.wait_for_function("() => document.querySelector('#macro-focus .macro-hist-chart.big')", timeout=45000)
                page.click("#macro-focus-note")
                page.wait_for_timeout(300)
                page.keyboard.press("End")
                page.keyboard.type(" (edited on the big view)")
                page.keyboard.press("Enter")
                page.wait_for_timeout(500)
                both = page.evaluate("uid => ({focus: ((document.getElementById('macro-focus-note') || {}).textContent || '').trim(), card: document.querySelector('.macro-item[data-series-id=\"' + uid + '\"] .macro-note').textContent.trim()})", uid)
                assert both["focus"].endswith("(edited on the big view)") and both["card"] == both["focus"], both
                page.keyboard.press("Escape")
                page.wait_for_timeout(300)
                return both["card"]
            check("the note can be edited on the full-screen view and the card follows", edit_on_big_view)

            def remove_note():
                page.hover(".macro-item[data-series-id='%s']" % uid)
                page.click(".macro-item[data-series-id='%s'] .macro-note" % uid)
                page.wait_for_timeout(300)
                page.click(".macro-item[data-series-id='%s'] [data-note='clear']" % uid)
                page.wait_for_timeout(400)
                out = page.evaluate("uid => { const n = document.querySelector('.macro-item[data-series-id=\"' + uid + '\"] .macro-note'); const st = JSON.parse(localStorage.getItem('jh_macro_workspace_v2')); let note = null; st.categories.forEach(c => c.items.forEach(i => { if (i.uid === uid) note = i.note; })); return {empty: n.classList.contains('empty'), stored: note}; }", uid)
                assert out["empty"] and out["stored"] == "", out
                return "REMOVE emptied it on the card and in storage"
            check("REMOVE clears the note", remove_note)

            def escape_cancels():
                page.hover(".macro-item[data-series-id='%s']" % uid)
                page.click(".macro-item[data-series-id='%s'] .macro-note" % uid)
                page.wait_for_timeout(200)
                page.keyboard.type("draft that must not be kept")
                page.keyboard.press("Escape")
                page.wait_for_timeout(300)
                out = page.evaluate("uid => ({empty: document.querySelector('.macro-item[data-series-id=\"' + uid + '\"] .macro-note').classList.contains('empty'), modalOpen: document.getElementById('macro-modal').classList.contains('open'), focusOpen: document.getElementById('macro-focus').classList.contains('open')})", uid)
                assert out["empty"] and out["modalOpen"] and not out["focusOpen"], out
                return "draft dropped, desk still open"
            check("Esc inside the note cancels the draft without closing anything", escape_cancels)

            def compare_untouched():
                page.evaluate("() => { window.__opened = []; window.open = u => { window.__opened.push(u); return {}; }; }")
                page.hover(".macro-item[data-series-id='%s']" % uid)
                page.click(".macro-item[data-series-id='%s'] [data-macro-action='compare-series']" % uid)
                page.wait_for_timeout(400)
                out = page.evaluate("() => ({opened: window.__opened, focusOpen: document.getElementById('macro-focus').classList.contains('open')})")
                assert len(out["opened"]) == 1 and "cmp=" in out["opened"][0] and not out["focusOpen"], out
                return out["opened"][0]
            check("the card's ⇄ still opens Chart Pro's comparison and does not open the full screen", compare_untouched)

            def add_all_regression():
                page.fill("#macro-search-input", "ice boa")
                page.wait_for_selector("#macro-add-all-btn", timeout=45000)
                page.wait_for_timeout(500)
                page.click("#macro-add-all-btn")
                page.wait_for_timeout(1500)
                out = page.evaluate("() => { const c = MacroData.state.categories[MacroData.state.categories.length - 1]; return {name: c.cat, n: c.items.length, strips: document.querySelectorAll('.macro-cat[data-category-id=\"' + c.id + '\"] .macro-hist').length, notes: document.querySelectorAll('.macro-cat[data-category-id=\"' + c.id + '\"] .macro-note').length}; }")
                assert out["name"] == "ICE BofA" and out["n"] == 29 and out["strips"] == 29 and out["notes"] == 29, out
                return "%s: %d series, %d strips, %d note slots" % (out["name"], out["n"], out["strips"], out["notes"])
            check("search add-all still works and the new cards carry strips and note slots", add_all_regression)
            ov = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            check("no new horizontal overflow", lambda: "overflow %s (baseline %s)" % (ov, base_overflow) if ov <= max(0, base_overflow) else (_ for _ in ()).throw(AssertionError("%s > %s" % (ov, base_overflow))))
            real_errors = [e for e in errors if "No market print" not in e]
            check("0 page errors", lambda: "none" if not real_errors else (_ for _ in ()).throw(AssertionError(str(real_errors[:4]))))

            r.section("2. chart.html -> Macro button -> overlay iframe")
            page2 = ctx.new_page()
            page2.set_default_timeout(45000)
            try:
                page2.goto("%s/chart.html?s=SPY&nocache=%d" % (SITE, int(time.time())), wait_until="domcontentloaded")
                page2.wait_for_selector("#btn-macro", timeout=45000)
                page2.wait_for_timeout(1500)
                page2.evaluate("() => document.getElementById('btn-macro').click()")
                page2.wait_for_selector("#ws-overlay iframe", timeout=20000)
                fr = None
                for _ in range(60):
                    fr = next((f for f in page2.frames if "macro-economic-data" in (f.url or "")), None)
                    if fr:
                        break
                    page2.wait_for_timeout(500)
                assert fr, "no macro iframe"
                fr.wait_for_selector(".macro-item", timeout=45000)
                fr.wait_for_timeout(1500)
                fr.evaluate(SCROLL_ALL)
                try:
                    fr.wait_for_function(ALL_SETTLED, timeout=120000)
                except Exception:  # noqa: BLE001
                    pass
                fr.wait_for_timeout(2000)
                sti = fr.evaluate(CATALOG_JS)
                check("inside the chart.html overlay the cards carry the strips with the four bears", lambda: "%d strips / %d ready, bands %s, turns %s" % (sti["withStrip"], sti["ready"], json.dumps(sti["bandCounts"]), sti["turns"]) if sti["withStrip"] == sti["ready"] and sti["ready"] > 0 and list(sti["bandCounts"].keys()) == ["4/4"] else (_ for _ in ()).throw(AssertionError(str({k: sti[k] for k in ("ready", "withStrip", "bandCounts")}))))
                fr.click(".macro-item[data-metric-state='ready'] .macro-hist-svg", position={"x": 24, "y": 24})
                fr.wait_for_function("() => document.querySelector('#macro-focus .macro-hist-chart.big')", timeout=45000)
                fr.wait_for_timeout(800)
                fsi = fr.evaluate(FOCUS_JS)
                check("inside the overlay the full-screen view fills the overlay", lambda: "%dx%d of %dx%d, %d labels" % (fsi["w"], fsi["h"], fsi["vw"], fsi["vh"], len(fsi["labels"])) if fsi["open"] and fsi["w"] == fsi["vw"] and fsi["h"] == fsi["vh"] and len(fsi["labels"]) == 4 else (_ for _ in ()).throw(AssertionError(str(fsi))))
                shots.append(("chart-overlay", page2.screenshot(type="jpeg", quality=45)))
            except Exception as e:  # noqa: BLE001
                fails.append("chart overlay")
                r.warn("FAIL chart.html overlay: %s %s" % (type(e).__name__, str(e)[:300]))

            r.section("3. 390x844 phone")
            ctx3 = browser.new_context(viewport={"width": 390, "height": 844}, user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) justhodl-ops/6775")
            page3 = ctx3.new_page()
            page3.set_default_timeout(45000)
            page3.goto("%s/macro-economic-data.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page3.wait_for_selector(".macro-item", timeout=45000)
            page3.wait_for_timeout(1200)
            base3 = page3.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            settle(page3, r.log)
            st3 = page3.evaluate(CATALOG_JS)
            check("390px: strips on every card with data, one card per row, no new overflow", lambda: "%d strips / %d ready, strip %s, overflow %s (baseline %s)" % (st3["withStrip"], st3["ready"], st3["strip"], st3["overflow"], base3) if st3["withStrip"] == st3["ready"] and st3["ready"] > 0 and st3["perRow"] == 1 and st3["overflow"] <= max(0, base3) else (_ for _ in ()).throw(AssertionError(str({k: st3[k] for k in ("ready", "withStrip", "perRow", "overflow")}))))
            page3.click(".macro-item[data-metric-state='ready'] .macro-hist-svg", position={"x": 24, "y": 24})
            page3.wait_for_function("() => document.querySelector('#macro-focus .macro-hist-chart.big')", timeout=45000)
            page3.wait_for_timeout(800)
            fs3 = page3.evaluate(FOCUS_JS)
            check("390px: the full-screen view fills the phone screen and fits", lambda: "%dx%d, chart %dpx, overflow %s" % (fs3["w"], fs3["h"], fs3["chartH"], fs3["overflow"]) if fs3["open"] and fs3["w"] == fs3["vw"] and fs3["h"] == fs3["vh"] and fs3["overflow"] <= 0 else (_ for _ in ()).throw(AssertionError(str(fs3))))
            shots.append(("phone-fullscreen", page3.screenshot(type="jpeg", quality=45)))
            browser.close()
        for name, data in shots:
            r.section("screenshot %s (jpeg, base64)" % name)
            r.log("<!-- SCREENSHOT_%s_JPEG_BASE64 -->\n%s\n<!-- /SCREENSHOT_%s_JPEG_BASE64 -->" % (name, base64.b64encode(data).decode(), name))
        if fails:
            r.fail("ops 6775: %d check(s) failed: %s" % (len(fails), fails))
        else:
            r.ok("ops 6775 complete: history strips, the full-screen view and the notes work on the live Macro desk, inside chart.html and on a phone")


if __name__ == "__main__":
    main()
    sys.exit(0)
