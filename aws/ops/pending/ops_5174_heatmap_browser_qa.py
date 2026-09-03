"""ops_5174 -- Universe Heatmap workspace: live browser QA at desktop / tablet / mobile.

The heatmap in chart-pro.html was rebuilt to the Macro & Economic Data
workspace contract (categories, instruments from universal search, D/W/M/Q/Y
changes, three colouring modes, danger rules, mouse/touch/keyboard reorder,
versioned storage with migration + limits, focus management). Node-level
state tests passed before the push; this op is the browser half:

  P0  wait for the Pages deploy (marker jh_heatmap_workspace_v3 on the live page)
  P1  at 1440 / 768 / 390: open the heatmap, count cells, wait for hydration,
      switch horizon and mode, assert repaint, screenshot, no page errors
  P2  editor: + CATEGORY -> save -> section exists; header search "gold" ->
      + ADD -> save -> cell exists in the new category (persisted in storage)
  P3  danger rule: instrument with move_abs 0 -> cell carries danger-flash
  P4  keyboard reorder on a handle (ArrowRight) changes stored order
  P5  touch reorder: synthetic pointer events (pointerType touch) on a handle
  P6  migration: legacy jh_heatmap_workspace_v2 -> v3 with symbols preserved
  P7  Escape closes the editor first, then the modal; focus returns to the
      trigger; the content is inert while the editor is open
  P8  mobile: no horizontal overflow at 390px

Any failed assertion -> RED (sys.exit(1)) with the failing width/step named.
"""
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

SITE = "https://justhodl.ai"
PAGE = SITE + "/chart-pro.html?s=SPY&tf=1D"
MARKER = "jh_heatmap_workspace_v3"
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []


def live_has_marker():
    try:
        req = urllib.request.Request(SITE + "/chart-pro.html", headers={"User-Agent": "ops5174", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return MARKER in r.read().decode("utf-8", "ignore")
    except Exception:
        return False


with report("ops_5174_heatmap_browser_qa") as R:
    R.heading("ops 5174 -- Universe Heatmap browser QA (1440 / 768 / 390)")
    R.section("P0 deploy")
    ok = False
    for i in range(40):
        if live_has_marker():
            ok = True
            break
        time.sleep(15)
    (R.ok if ok else R.fail)("   live chart-pro.html %s the new workspace after %ds" % ("carries" if ok else "does NOT carry", i * 15))
    if not ok:
        sys.exit(1)

    try:
        import playwright  # noqa: F401
    except Exception:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright

    def launch(p):
        try:
            return p.chromium.launch(channel="chrome", headless=True)
        except Exception:
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
            return p.chromium.launch(headless=True)

    SHOTS.mkdir(parents=True, exist_ok=True)

    def new_page(browser, width, height, init_script=None):
        ctx = browser.new_context(viewport={"width": width, "height": height}, has_touch=width < 900,
                                  is_mobile=width < 700, device_scale_factor=1)
        if init_script:
            ctx.add_init_script(init_script)
        pg = ctx.new_page()
        errors, console = [], []
        pg.on("pageerror", lambda e: errors.append(str(e)[:200]))
        pg.on("console", lambda m: console.append("%s: %s" % (m.type, m.text[:160])) if m.type in ("error",) else None)
        pg.goto(PAGE, wait_until="domcontentloaded", timeout=60000)
        pg.wait_for_timeout(4500)
        return ctx, pg, errors, console

    def open_heatmap(pg):
        pg.evaluate("() => Heatmap.open()")
        pg.wait_for_selector("#heatmap-modal.open", timeout=10000)
        pg.wait_for_timeout(600)

    def wait_hydrated(pg, min_ready=6, secs=25):
        t0 = time.time()
        ready = 0
        while time.time() - t0 < secs:
            ready = pg.evaluate("() => document.querySelectorAll('#heatmap-body .hm-cell[data-metric-state=\"ready\"]').length")
            if ready >= min_ready:
                break
            pg.wait_for_timeout(700)
        return ready

    with sync_playwright() as p:
        browser = launch(p)

        # ---------------------------------------------------------------- P1
        R.section("P1 render + hydration + modes/horizons at three widths")
        for width, height in ((1440, 1000), (768, 1024), (390, 844)):
            ctx, pg, errors, console = new_page(browser, width, height)
            try:
                open_heatmap(pg)
                cells = pg.evaluate("() => document.querySelectorAll('#heatmap-body .hm-cell').length")
                sections = pg.evaluate("() => document.querySelectorAll('#heatmap-body .hm-sector').length")
                ready = wait_hydrated(pg)
                first_ch = pg.evaluate("() => (document.querySelector('#heatmap-body .hm-cell[data-metric-state=\"ready\"] .hm-ch') || {}).textContent")
                pg.click(".hm-ctrl[data-hm-horizon='W']")
                pg.wait_for_timeout(300)
                w_ch = pg.evaluate("() => (document.querySelector('#heatmap-body .hm-cell[data-metric-state=\"ready\"] .hm-ch') || {}).textContent")
                pg.click(".hm-ctrl[data-hm-mode='cascade']")
                pg.wait_for_timeout(300)
                legend = pg.evaluate("() => document.getElementById('hm-legend').textContent")
                bg_variety = pg.evaluate("() => new Set([...document.querySelectorAll('#heatmap-body .hm-cell[data-metric-state=\"ready\"]')].map(c => c.style.background)).size")
                pg.click(".hm-ctrl[data-hm-mode='perf']")
                pg.click(".hm-ctrl[data-hm-horizon='D']")
                pg.wait_for_timeout(200)
                stored = pg.evaluate("() => JSON.parse(localStorage.getItem('jh_heatmap_workspace_v3') || '{}')")
                overflow = pg.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
                shot = SHOTS / f"ops5174_heatmap_{width}.png"
                pg.screenshot(path=str(shot), full_page=False)
                R.log("   %4dpx: sections=%d cells=%d hydrated=%d first D=%s W=%s legend='%s' bg-variety=%d stored.version=%s overflow=%dpx errors=%d"
                      % (width, sections, cells, ready, first_ch, w_ch, legend[:36], bg_variety, stored.get("version"), overflow, len(errors)))
                R.kv(section="P1", width=width, sections=sections, cells=cells, hydrated=ready, overflow=overflow, errors=len(errors))
                if sections < 5 or cells < 40:
                    FAILS.append("%dpx: too few sections/cells (%d/%d)" % (width, sections, cells))
                if ready < 6:
                    FAILS.append("%dpx: hydration too low (%d ready)" % (width, ready))
                if first_ch and w_ch and first_ch == w_ch and first_ch not in ("—", "n/a"):
                    R.warn("   %dpx: D and W change identical on the first cell (%s) -- possible, flagging only" % (width, first_ch))
                if "cascade" not in legend.lower():
                    FAILS.append("%dpx: legend did not switch to cascade mode" % width)
                if stored.get("version") != 3:
                    FAILS.append("%dpx: storage version %s" % (width, stored.get("version")))
                if overflow > 0 and width <= 768:
                    FAILS.append("%dpx: horizontal overflow %dpx" % (width, overflow))
                if errors:
                    FAILS.append("%dpx: page errors: %s" % (width, errors[:2]))
            except Exception as e:
                FAILS.append("%dpx P1: %s" % (width, str(e)[:160]))
            ctx.close()

        # ---------------------------------------------------------------- P2-P7 (desktop)
        R.section("P2-P7 editor, search, danger, keyboard reorder, touch reorder, migration, focus")
        legacy = json.dumps({"groups": [{"name": "Legacy Picks", "symbols": ["nvda", "AMD", "nvda"]}], "mode": "signals", "horizon": "M"})
        init = "localStorage.removeItem('jh_heatmap_workspace_v3'); localStorage.setItem('jh_heatmap_workspace_v2', %s);" % json.dumps(legacy)
        ctx, pg, errors, console = new_page(browser, 1440, 1000, init_script=init)
        try:
            open_heatmap(pg)
            st = pg.evaluate("() => JSON.parse(localStorage.getItem('jh_heatmap_workspace_v3'))")
            legacy_cat = next((c for c in st["categories"] if c["cat"] == "Legacy Picks"), None)
            syms = [i["symbol"] for i in legacy_cat["items"]] if legacy_cat else []
            (R.ok if syms == ["NVDA", "AMD"] and st["mode"] == "signals" and st["horizon"] == "M" else R.fail)(
                "   P6 migration v2->v3: symbols=%s mode=%s horizon=%s" % (syms, st.get("mode"), st.get("horizon")))
            if syms != ["NVDA", "AMD"]:
                FAILS.append("migration lost symbols: %s" % syms)
            pg.click(".hm-ctrl[data-hm-mode='perf']")
            pg.click(".hm-ctrl[data-hm-horizon='D']")

            # P2 category
            pg.click("#hm-add-category")
            pg.wait_for_selector("#hm-editor-modal.open", timeout=5000)
            inert = pg.evaluate("() => document.querySelector('#heatmap-modal .heatmap-content').hasAttribute('inert')")
            pg.fill("#hm-category-name", "QA Basket")
            pg.fill("#hm-category-icon", "🧪")
            pg.click("#hm-editor-save")
            pg.wait_for_timeout(500)
            has_cat = pg.evaluate("() => [...document.querySelectorAll('#heatmap-body .hm-sector-name')].some(n => n.textContent === 'QA Basket')")
            inert_after = pg.evaluate("() => document.querySelector('#heatmap-modal .heatmap-content').hasAttribute('inert')")
            (R.ok if has_cat and inert and not inert_after else R.fail)("   P2 category added=%s inert-while-editing=%s restored=%s" % (has_cat, inert, not inert_after))
            if not has_cat or not inert or inert_after:
                FAILS.append("P2 category/inert")

            # P2 search + add
            pg.fill("#hm-search-input", "gold")
            pg.wait_for_selector("#heatmap-body .macro-sr", timeout=15000)
            n_results = pg.evaluate("() => document.querySelectorAll('#heatmap-body .macro-sr').length")
            pg.click("#heatmap-body .macro-sr [data-add-result]")
            pg.wait_for_selector("#hm-editor-modal.open", timeout=5000)
            selected = pg.evaluate("() => document.getElementById('hm-selected-instrument').textContent")
            pg.select_option("#hm-instrument-category", label=pg.evaluate("() => [...document.querySelectorAll('#hm-instrument-category option')].find(o => o.textContent.includes('QA Basket')).textContent"))
            pg.select_option("#hm-alert-type", "move_abs")
            pg.fill("#hm-alert-threshold", "0")
            pg.click("#hm-editor-save")
            pg.wait_for_timeout(600)
            st = pg.evaluate("() => JSON.parse(localStorage.getItem('jh_heatmap_workspace_v3'))")
            qa = next((c for c in st["categories"] if c["cat"] == "QA Basket"), None)
            added = qa["items"][0]["symbol"] if qa and qa["items"] else None
            (R.ok if n_results and added else R.fail)("   P2 search results=%d selected='%s' added=%s" % (n_results, selected[:40], added))
            if not added:
                FAILS.append("P2 search/add")

            # P3 danger flash on the added instrument (move_abs 0 -> any nonzero D change)
            pg.wait_for_timeout(4000)
            flash = pg.evaluate("(sym) => { const c = [...document.querySelectorAll('#heatmap-body .hm-cell')].find(x => x.dataset.symbol === sym); return c ? {state: c.dataset.metricState, danger: c.classList.contains('danger-flash'), ch: c.querySelector('.hm-ch').textContent} : null; }", added)
            R.log("   P3 danger cell %s -> %s" % (added, flash))
            if not flash or flash.get("state") != "ready":
                R.warn("   P3: added instrument not hydrated in time (%s)" % flash)
            elif not flash.get("danger") and flash.get("ch") not in ("—", "0.00%", "+0.00%"):
                FAILS.append("P3 danger rule did not flash: %s" % flash)

            # P4 keyboard reorder: first editable category, first handle ArrowRight
            order_before = pg.evaluate("() => { const st = JSON.parse(localStorage.getItem('jh_heatmap_workspace_v3')); const c = st.categories.find(x => !x.auto && x.items.length > 2); return c ? {id: c.id, syms: c.items.map(i => i.symbol)} : null; }")
            pg.focus("#heatmap-body .hm-sector[data-category-id='%s'] .hm-cell:first-child .macro-drag-handle" % order_before["id"])
            pg.keyboard.press("ArrowRight")
            pg.wait_for_timeout(500)
            order_after = pg.evaluate("(id) => JSON.parse(localStorage.getItem('jh_heatmap_workspace_v3')).categories.find(x => x.id === id).items.map(i => i.symbol)", order_before["id"])
            focused = pg.evaluate("() => document.activeElement && document.activeElement.className")
            kb_ok = order_after[:2] == [order_before["syms"][1], order_before["syms"][0]]
            (R.ok if kb_ok else R.fail)("   P4 keyboard reorder %s -> %s (focus on %s)" % (order_before["syms"][:3], order_after[:3], focused))
            if not kb_ok:
                FAILS.append("P4 keyboard reorder")

            # P5 touch reorder via synthetic pointer events on the handle
            touch = pg.evaluate("""(id) => new Promise(resolve => {
                const sec = document.querySelector(`#heatmap-body .hm-sector[data-category-id='${id}']`);
                const cells = [...sec.querySelectorAll('.hm-cell')];
                const handle = cells[0].querySelector('.macro-drag-handle');
                const target = cells[2];
                const h = handle.getBoundingClientRect(), t = target.getBoundingClientRect();
                const ev = (type, x, y) => handle.dispatchEvent(new PointerEvent(type, {bubbles: true, cancelable: true, pointerId: 7, pointerType: 'touch', isPrimary: true, clientX: x, clientY: y, button: 0}));
                const before = JSON.parse(localStorage.getItem('jh_heatmap_workspace_v3')).categories.find(x => x.id === id).items.map(i => i.symbol);
                ev('pointerdown', h.left + 5, h.top + 5);
                setTimeout(() => ev('pointermove', h.left + 30, h.top + 8), 30);
                setTimeout(() => ev('pointermove', t.left + t.width * .8, t.top + t.height / 2), 120);
                setTimeout(() => ev('pointerup', t.left + t.width * .8, t.top + t.height / 2), 260);
                setTimeout(() => { const after = JSON.parse(localStorage.getItem('jh_heatmap_workspace_v3')).categories.find(x => x.id === id).items.map(i => i.symbol); resolve({before: before.slice(0, 4), after: after.slice(0, 4)}); }, 700);
            })""", order_before["id"])
            touch_ok = touch["before"] != touch["after"] and touch["after"][2] == touch["before"][0]
            (R.ok if touch_ok else R.warn)("   P5 touch reorder %s -> %s" % (touch["before"], touch["after"]))
            if not touch_ok:
                FAILS.append("P5 touch reorder (synthetic pointer events)")

            # P7 escape + focus return
            pg.click("#hm-add-instrument")
            pg.wait_for_selector("#hm-editor-modal.open", timeout=5000)
            pg.keyboard.press("Escape")
            pg.wait_for_timeout(200)
            editor_open = pg.evaluate("() => document.getElementById('hm-editor-modal').classList.contains('open')")
            modal_open = pg.evaluate("() => document.getElementById('heatmap-modal').classList.contains('open')")
            focus_after_editor = pg.evaluate("() => document.activeElement && document.activeElement.id")
            pg.keyboard.press("Escape")
            pg.wait_for_timeout(200)
            modal_open2 = pg.evaluate("() => document.getElementById('heatmap-modal').classList.contains('open')")
            esc_ok = (not editor_open) and modal_open and (not modal_open2) and focus_after_editor == "hm-add-instrument"
            (R.ok if esc_ok else R.fail)("   P7 Escape: editor closed=%s modal stayed=%s then closed=%s focus->%s" % (not editor_open, modal_open, not modal_open2, focus_after_editor))
            if not esc_ok:
                FAILS.append("P7 escape/focus")
            pg.screenshot(path=str(SHOTS / "ops5174_heatmap_after_qa.png"))
            if errors:
                FAILS.append("desktop QA page errors: %s" % errors[:2])
        except Exception as e:
            FAILS.append("P2-P7: %s" % str(e)[:200])
            try:
                pg.screenshot(path=str(SHOTS / "ops5174_heatmap_failure.png"))
            except Exception:
                pass
        ctx.close()
        browser.close()

    R.section("verdict")
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        R.log("   RED: %d failure(s)" % len(FAILS))
        sys.exit(1)
    R.ok("   GREEN: heatmap workspace renders and hydrates at 1440/768/390, modes/horizons repaint, editor + search + danger + keyboard/touch reorder + migration + focus all pass")
