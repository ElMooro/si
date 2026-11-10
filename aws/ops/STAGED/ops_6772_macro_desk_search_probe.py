"""ops 6772 - READ-ONLY probe: where does Khalid see "ice boa" without an add-all? (2026-11-10 14:31 ET: "im trying to add
all ice boa but i dont see the add all. i see them they pull all but i cant see the add all").

Two search bars live on chart.html: the Symbol Search dialog (jh-uni-search.js, add-all shipped a5855cda) and the
"Macro" desk button -> ws-overlay iframe of /macro-economic-data.html whose own search (#macro-search-input, MacroData.search)
lists every matching series with a per-row "+ ADD" and no add-all. This probe, in headless Chrome at 1440x900:
  1. GET chart.html with NO cache-buster: the jh-uni-search.js ?v= stamp the edge serves + cache headers;
  2. Symbol Search "ice boa": rows, add controls (is the add-all bar there?), screenshot;
  3. click #btn-macro, wait for the overlay iframe, type "ice boa" into its search: rows, any add-all control, screenshot;
  4. the same on the standalone /macro-economic-data.html.
Nothing is written anywhere; search state lives in the probe browser only.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import json
import subprocess
import sys
import time

import requests

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


MACRO_JS = """() => {
  const rows = [...document.querySelectorAll('#macro-body .macro-sr')];
  const head = document.querySelector('#macro-body .macro-cat-name');
  const btns = [...document.querySelectorAll('#macro-body button, #macro-modal .macro-head-actions button')].map(b => b.innerText.trim()).filter(Boolean);
  const inp = document.getElementById('macro-search-input'), cs = inp ? getComputedStyle(inp) : null, r = inp ? inp.getBoundingClientRect() : null;
  const box = document.querySelector('#macro-modal .heatmap-content'), br = box ? box.getBoundingClientRect() : null;
  return {n: rows.length, head: head ? head.innerText.replace(/\\n/g, ' ') : null, first: rows.slice(0, 8).map(e => e.innerText.replace(/\\n/g, ' | ').slice(0, 110)),
          buttons: [...new Set(btns)].slice(0, 12), addAll: btns.filter(t => /add all/i.test(t)), inputFont: cs ? cs.fontSize : null, inputH: r ? Math.round(r.height) : null,
          box: br ? {w: Math.round(br.width), h: Math.round(br.height)} : null, body: (document.getElementById('macro-body') || {}).innerText ? (document.getElementById('macro-body').innerText || '').slice(0, 160) : null};
}"""


def macro_search(frame, log):
    frame.wait_for_selector("#macro-search-input", timeout=45000)
    frame.wait_for_timeout(1500)
    frame.fill("#macro-search-input", QUERY)
    try:
        frame.wait_for_function("() => document.querySelectorAll('#macro-body .macro-sr').length > 0 || /No chartable|failed/i.test((document.getElementById('macro-body') || {}).innerText || '')", timeout=30000)
    except Exception:  # noqa: BLE001
        log("macro search: no rows within 30 s")
    frame.wait_for_timeout(800)
    return frame.evaluate(MACRO_JS)


def main():
    with report("ops_6772_macro_desk_search_probe") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6772 - where is the add-all missing? probe %s" % now.isoformat(timespec="minutes"))
        try:
            h = requests.get(SITE + "/chart.html", timeout=40, headers={"User-Agent": "Mozilla/5.0 justhodl-ops/6772"})
            import re
            stamp = re.search(r'jh-uni-search\.js\?v=([^"\']+)', h.text)
            r.log("chart.html (no cache-buster): HTTP %s | jh-uni-search.js ?v=%s | cache-control %s | age %s | x-cache %s | etag %s | last-modified %s" % (
                h.status_code, stamp.group(1) if stamp else None, h.headers.get("cache-control"), h.headers.get("age"), h.headers.get("x-cache"), h.headers.get("etag"), h.headers.get("last-modified")))
        except Exception as e:  # noqa: BLE001
            r.warn("chart.html fetch: %s %s" % (type(e).__name__, str(e)[:200]))
        sync_playwright = ensure_playwright(r.log)
        shots = []
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6772")
            page = ctx.new_page()
            page.set_default_timeout(45000)
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto(SITE + "/chart.html?s=SPY", wait_until="domcontentloaded")
            try:
                page.wait_for_function("() => window.jhOpenSearch && window.JHTvWatchlist", timeout=45000)
            except Exception as e:  # noqa: BLE001
                r.warn("search module not ready (%s)" % type(e).__name__)
            page.wait_for_timeout(2500)
            r.section("1. Symbol Search dialog, query '%s'" % QUERY)
            page.evaluate("q => window.jhOpenSearch(q, 'chart')", QUERY)
            try:
                page.wait_for_function("() => document.querySelectorAll('#jhus .us-row').length > 0 && !/Searching|Loading/.test((document.querySelector('#jhus .us-status') || {}).innerText || '')", timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(1500)
            st = page.evaluate("""() => { const q = s => [...document.querySelectorAll(s)];
              return {served: ([...document.scripts].map(x => x.src).find(u => /jh-uni-search[.]js/.test(u)) || '').replace(location.origin, ''), nRows: q('#jhus .us-row').length,
                      rows: q('#jhus .us-row').slice(0, 8).map(e => ((e.querySelector('.us-sym') || {}).innerText || '') + ' - ' + ((e.querySelector('.us-desc .n') || {}).innerText || '').slice(0, 70)),
                      adds: q('#jhus [data-act="addloaded"], #jhus [data-act="addgrp"]').map(b => b.innerText.trim().slice(0, 50)), groups: q('#jhus .us-grp').map(e => e.innerText.trim().slice(0, 40)),
                      status: (document.querySelector('#jhus .us-status') || {}).innerText || ''}; }""")
            r.log("served %s | %d rows | add controls %s | groups %s | status %s" % (st["served"], st["nRows"], st["adds"], st["groups"], st["status"][:90]))
            for row in st["rows"]:
                r.log("    " + row)
            shots.append(("symbol-search", page.screenshot(type="jpeg", quality=45)))
            page.evaluate("() => { const x = document.querySelector('#jhus [data-act=close]'); if (x) x.click(); }")
            page.wait_for_timeout(400)
            r.section("2. chart.html -> Macro desk button -> overlay iframe, query '%s'" % QUERY)
            try:
                page.wait_for_selector("#btn-macro", timeout=20000)
                page.click("#btn-macro")
                page.wait_for_selector("#ws-overlay iframe", timeout=20000)
                page.wait_for_timeout(1500)
                fr = None
                for _ in range(40):
                    fr = next((f for f in page.frames if "macro-economic-data" in (f.url or "")), None)
                    if fr:
                        break
                    page.wait_for_timeout(500)
                if not fr:
                    r.warn("no macro-economic-data iframe; frames: %s" % [f.url[:80] for f in page.frames])
                else:
                    r.log("overlay iframe: %s" % fr.url)
                    ms = macro_search(fr, r.log)
                    r.log("macro desk search: %d rows | header '%s' | input %s / %spx | box %s | buttons %s | ADD-ALL CONTROLS: %s" % (
                        ms["n"], ms["head"], ms["inputFont"], ms["inputH"], json.dumps(ms["box"]), ms["buttons"], ms["addAll"] or "NONE"))
                    for row in ms["first"]:
                        r.log("    " + row)
                    shots.append(("macro-desk-overlay", page.screenshot(type="jpeg", quality=45)))
            except Exception as e:  # noqa: BLE001
                r.warn("macro desk overlay: %s %s" % (type(e).__name__, str(e)[:300]))
            r.section("3. standalone /macro-economic-data.html, query '%s'" % QUERY)
            try:
                page2 = ctx.new_page()
                page2.goto(SITE + "/macro-economic-data.html", wait_until="domcontentloaded")
                ms2 = macro_search(page2, r.log)
                r.log("standalone page search: %d rows | header '%s' | input %s / %spx | box %s | ADD-ALL CONTROLS: %s" % (ms2["n"], ms2["head"], ms2["inputFont"], ms2["inputH"], json.dumps(ms2["box"]), ms2["addAll"] or "NONE"))
                shots.append(("macro-page", page2.screenshot(type="jpeg", quality=45)))
            except Exception as e:  # noqa: BLE001
                r.warn("standalone page: %s %s" % (type(e).__name__, str(e)[:300]))
            r.log("page errors: %s" % (errors[:5] or "none"))
            browser.close()
        for name, data in shots:
            r.section("screenshot %s (jpeg, base64)" % name)
            r.log("<!-- SCREENSHOT_%s_JPEG_BASE64 -->\n%s\n<!-- /SCREENSHOT_%s_JPEG_BASE64 -->" % (name, base64.b64encode(data).decode(), name))
        r.ok("ops 6772 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
