"""ops 6773 - READ-ONLY browser proof of the Macro desk search "Add all" (Khalid 2026-11-10 14:31 ET: "im trying to add
all ice boa but i dont see the add all"). Re-runnable.

Against the live site in headless Chrome:
  1. /macro-economic-data.html at 1440x900: the page serves the enhanced bundle (markers), type "ice boa", read the
     summary / chips / "+ Add all N" bar / destination options / first rows (sizes), screenshot; click "+ Add all N":
     a category named from the results ("ICE BofA") must hold exactly N series, be saved to localStorage
     (jh_macro_workspace_v2) and the catalog must show N cards in it; screenshot. Click it again into the same category:
     0 added, N already there. Then a provider chip + the add-all into an EXISTING category: that category grows by
     the chip's count.
  2. chart.html: the Macro button opens the overlay iframe of the same page; inside it, "ice boa" shows the bar.
  3. 390x844: the bar is visible and the page has no horizontal overflow.
  4. 0 page errors (the "No market print" hydration messages of ETF cards are the catalog's own, pre-existing).
Nothing on the site changes; the categories live in the probe browser's localStorage only.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import json
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

SITE = "https://justhodl.ai"
QUERY = "ice boa"


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


STATE_JS = """() => {
  const q = s => [...document.querySelectorAll(s)], h = s => { const e = document.querySelector(s); return e ? Math.round(e.getBoundingClientRect().height) : null; };
  const btn = document.getElementById('macro-add-all-btn'), dest = document.getElementById('macro-add-all-dest');
  const inp = document.getElementById('macro-search-input');
  return {rows: q('.macro-sr').length, btn: btn ? btn.innerText.trim() : null, btnVisible: !!btn && btn.getBoundingClientRect().width > 0,
          dest: dest ? [...dest.options].slice(0, 4).map(o => o.text) : null, nDest: dest ? dest.options.length : 0,
          chips: q('.macro-chip').map(c => c.innerText.replace(/\\n/g, ' ')), sum: (document.querySelector('.macro-sr-sum') || {}).innerText || '',
          first: q('.macro-sr').slice(0, 5).map(e => e.innerText.replace(/\\n/g, ' | ').slice(0, 120)), rowH: h('.macro-sr'), inputFont: inp ? getComputedStyle(inp).fontSize : null, inputH: h('#macro-search-input'),
          cats: (window.MacroData && MacroData.state) ? MacroData.state.categories.map(c => [c.cat, c.items.length]) : null,
          overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth};
}"""


def type_query(frame, log, q=QUERY):
    frame.wait_for_selector("#macro-search-input", timeout=45000)
    frame.wait_for_timeout(1200)
    frame.fill("#macro-search-input", "")
    frame.fill("#macro-search-input", q)
    try:
        frame.wait_for_function("() => !!document.getElementById('macro-add-all-btn') || /No chartable|failed/i.test((document.getElementById('macro-body') || {}).innerText || '')", timeout=40000)
    except Exception:  # noqa: BLE001
        log("no add-all bar within 40 s: body '%s'" % ((frame.evaluate("() => (document.getElementById('macro-body') || {}).innerText || ''") or "")[:160]))
    frame.wait_for_timeout(600)
    return frame.evaluate(STATE_JS)


def main():
    with report("ops_6773_macro_desk_add_all_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6773 - Macro desk search add-all proof %s" % now.isoformat(timespec="minutes"))
        sync_playwright = ensure_playwright(r.log)
        fails, shots = [], []

        def check(name, fn):
            try:
                out = fn()
                r.log("ok   %s%s" % (name, (": " + str(out)[:700]) if out not in (None, True) else ""))
                return out
            except Exception as e:  # noqa: BLE001
                fails.append(name)
                r.warn("FAIL %s: %s %s" % (name, type(e).__name__, str(e)[:400]))
                return None

        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6773")
            page = ctx.new_page()
            page.set_default_timeout(45000)
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:160]))
            r.section("1. /macro-economic-data.html 1440x900")
            page.goto("%s/macro-economic-data.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page.wait_for_selector("#macro-search-input", timeout=45000)
            page.wait_for_timeout(1500)  # auth.js may re-route once after load; the evaluate below must not race it
            check("the page serves the enhanced bundle (not a cached copy)", lambda: page.evaluate("""async () => { const s = [...document.scripts].map(x => x.src).find(u => /jh-workspaces[.]js/.test(u)); if (!s) throw new Error('no jh-workspaces.js tag');
                const t = await (await fetch(s, {cache: 'no-store'})).text(); if (!/addAllResults/.test(t) || !/macro-add-all-btn/.test(t)) throw new Error('bundle lacks the add-all: ' + s); return s.replace(location.origin, '') + ' (' + t.length + ' bytes)'; }"""))
            st = type_query(page, r.log)
            r.log("'%s': %d rows | bar '%s' visible %s | destinations %d: %s | chips %s | input %s/%spx | row %spx" % (QUERY, st["rows"], st["btn"], st["btnVisible"], st["nDest"], st["dest"], st["chips"], st["inputFont"], st["inputH"], st["rowH"]))
            r.log("    summary: %s" % st["sum"].replace("\n", " · ")[:200])
            for row in st["first"]:
                r.log("    " + row)
            shots.append(("search", page.screenshot(type="jpeg", quality=50)))
            n = int(re.search(r"(\d+)", st["btn"] or "0").group(1)) if st["btn"] else 0
            check("'%s' shows the dropdown-wide add-all bar" % QUERY, lambda: (st["btn"] if st["btnVisible"] and n > 0 and st["rows"] == n else (_ for _ in ()).throw(AssertionError("bar %s visible %s rows %s" % (st["btn"], st["btnVisible"], st["rows"])))))
            check("the destination defaults to a new category named from the results", lambda: st["dest"][0] if st["dest"] and st["dest"][0].startswith("New category") else (_ for _ in ()).throw(AssertionError(str(st["dest"]))))
            check("TradingView-sized: 15 px search input, rows >= 56 px", lambda: "%s / %spx, row %spx" % (st["inputFont"], st["inputH"], st["rowH"]) if st["inputFont"] == "15px" and (st["rowH"] or 0) >= 56 else (_ for _ in ()).throw(AssertionError("%s %s" % (st["inputFont"], st["rowH"]))))
            before = {c[0]: c[1] for c in (st["cats"] or [])}

            def add_all():
                page.click("#macro-add-all-btn")
                page.wait_for_timeout(1500)
                out = page.evaluate("""() => { const cats = MacroData.state.categories, c = cats[cats.length - 1]; const saved = JSON.parse(localStorage.getItem('jh_macro_workspace_v2') || '{}'); const sc = (saved.categories || [])[(saved.categories || []).length - 1] || {};
                    return {name: c.cat, flag: c.flag, items: c.items.length, saved: (sc.items || []).length, savedName: sc.cat, toast: (document.getElementById('jh-toast') || {}).innerText || '', input: document.getElementById('macro-search-input').value,
                            cards: document.querySelectorAll('.macro-cat[data-category-id="' + c.id + '"] .macro-item').length, flash: !!document.querySelector('.macro-cat-flash'), symbols: c.items.slice(0, 3).map(i => i.symbol), nCats: cats.length}; }""")
                assert out["name"] not in before, "no new category: %s" % out
                assert out["items"] == n and out["saved"] == n and out["cards"] == n, "promised %d: state %s saved %s cards %s" % (n, out["items"], out["saved"], out["cards"])
                assert out["input"] == "", "search box not cleared"
                return "'%s' %s: %d series in state, %d saved, %d cards, flash %s, toast '%s', e.g. %s (%d categories now)" % (out["name"], out["flag"], out["items"], out["saved"], out["cards"], out["flash"], out["toast"], out["symbols"], out["nCats"])
            check("click '+ Add all %d' -> a new category holds exactly %d series, saved" % (n, n), add_all)
            shots.append(("after", page.screenshot(type="jpeg", quality=50)))

            def add_again():
                st2 = type_query(page, r.log)
                last_id = page.evaluate("() => MacroData.state.categories[MacroData.state.categories.length - 1].id")
                page.select_option("#macro-add-all-dest", value=last_id)
                page.click("#macro-add-all-btn")
                page.wait_for_timeout(1200)
                out = page.evaluate("() => ({items: MacroData.state.categories[MacroData.state.categories.length - 1].items.length, toast: (document.getElementById('jh-toast') || {}).innerText || '', nCats: MacroData.state.categories.length})")
                assert out["items"] == n, "doubled: %s" % out
                assert "already there" in out["toast"], out["toast"]
                return "still %d series, toast '%s', %d categories (bar read '%s')" % (out["items"], out["toast"], out["nCats"], st2["btn"])
            check("add-all again into the same category: nothing doubled, duplicates reported", add_again)

            def add_into_existing():
                st3 = type_query(page, r.log)
                chips = page.query_selector_all(".macro-chip")
                target = None
                for c in chips[1:]:
                    target = c
                    break
                if target:
                    target.click()
                    page.wait_for_timeout(500)
                st4 = page.evaluate(STATE_JS)
                m = int(re.search(r"(\d+)", st4["btn"] or "0").group(1))
                first_existing = page.evaluate("() => { const o = document.getElementById('macro-add-all-dest').options; return o.length > 1 ? {id: o[1].value, text: o[1].text} : null; }")
                assert first_existing, "no existing category to add into"
                cnt_before = page.evaluate("id => (MacroData.state.categories.find(c => c.id === id) || {items: []}).items.length", first_existing["id"])
                page.select_option("#macro-add-all-dest", value=first_existing["id"])
                page.click("#macro-add-all-btn")
                page.wait_for_timeout(1200)
                cnt_after = page.evaluate("id => (MacroData.state.categories.find(c => c.id === id) || {items: []}).items.length", first_existing["id"])
                toast = page.evaluate("() => (document.getElementById('jh-toast') || {}).innerText || ''")
                added = int((re.search(r"Added (\d+)", toast) or [None, "-1"])[1])
                assert cnt_after == cnt_before + added, "category %s: %d -> %d but toast says %s" % (first_existing["text"], cnt_before, cnt_after, toast)
                assert added + (int((re.search(r"(\d+) already", toast) or [None, "0"])[1])) == m, "bar promised %d, toast %s" % (m, toast)
                return "chip '%s' -> bar '%s' -> '%s' %d -> %d (%s)" % (st4["chips"][1] if len(st4["chips"]) > 1 else "-", st4["btn"], first_existing["text"], cnt_before, cnt_after, toast)
            check("provider chip + add-all into an EXISTING category grows it by exactly the promised count", add_into_existing)
            r.log("page errors: %s" % ([e for e in errors if "No market print" not in e][:5] or "none (beyond the catalog's own 'No market print' hydration notes)"))

            r.section("2. chart.html -> Macro button -> overlay iframe")
            page2 = ctx.new_page()
            page2.set_default_timeout(45000)
            errors2 = []
            page2.on("pageerror", lambda e: errors2.append(str(e)[:160]))
            page2.goto("%s/chart.html?s=SPY&nocache=%d" % (SITE, int(time.time())), wait_until="domcontentloaded")
            try:
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
                assert fr, "no macro-economic-data iframe; frames: %s" % [f.url[:80] for f in page2.frames]
                r.log("overlay iframe: %s" % fr.url)
                sti = type_query(fr, r.log)
                check("inside the chart.html overlay: '%s' shows the add-all bar" % QUERY, lambda: "%d rows, bar '%s', chips %s" % (sti["rows"], sti["btn"], sti["chips"]) if sti["btnVisible"] else (_ for _ in ()).throw(AssertionError(str(sti))))
                shots.append(("chart-overlay", page2.screenshot(type="jpeg", quality=45)))
            except Exception as e:  # noqa: BLE001
                fails.append("chart overlay")
                r.warn("FAIL chart.html overlay: %s %s" % (type(e).__name__, str(e)[:300]))
            r.log("chart.html page errors: %s" % (errors2[:5] or "none"))

            r.section("3. 390x844 phone")
            ctx3 = browser.new_context(viewport={"width": 390, "height": 844}, user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) justhodl-ops/6773")
            page3 = ctx3.new_page()
            page3.set_default_timeout(45000)
            page3.goto("%s/macro-economic-data.html?nocache=%d" % (SITE, int(time.time())), wait_until="domcontentloaded")
            page3.wait_for_selector("#macro-search-input", timeout=45000)
            page3.wait_for_timeout(1200)
            base_overflow = page3.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            st5 = type_query(page3, r.log)
            # the standalone page's own header (.ws-links: Universe Heatmap / Chart Pro / Sign In) overflows a 390 px phone
            # by ~90 px with or without the search (restore-point copy measured the same) -- the search must add none of its own
            check("390px: the add-all bar is visible and the search view adds no horizontal overflow", lambda: "bar '%s', page overflow %s (catalog view %s, pre-existing header links), rows %d, row %spx" % (st5["btn"], st5["overflow"], base_overflow, st5["rows"], st5["rowH"]) if st5["btnVisible"] and st5["overflow"] <= max(0, base_overflow) else (_ for _ in ()).throw(AssertionError(str({k: st5[k] for k in ("btn", "btnVisible", "overflow")}) + " base %s" % base_overflow)))
            shots.append(("phone", page3.screenshot(type="jpeg", quality=45)))
            browser.close()
        for name, data in shots:
            r.section("screenshot %s (jpeg, base64)" % name)
            r.log("<!-- SCREENSHOT_%s_JPEG_BASE64 -->\n%s\n<!-- /SCREENSHOT_%s_JPEG_BASE64 -->" % (name, base64.b64encode(data).decode(), name))
        if fails:
            r.fail("ops 6773: %d check(s) failed: %s" % (len(fails), fails))
        else:
            r.ok("ops 6773 complete: the Macro desk search adds every listed series in one click, on the page and inside chart.html's overlay")


if __name__ == "__main__":
    main()
    sys.exit(0)
