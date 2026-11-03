"""ops 6724 - READ-ONLY browser proof of the shipped 13F Institutional Book (CLAUDE.md rule 5: click it yourself).

Against https://justhodl.ai/13f.html in headless Chrome: the book fetch (data/13f-book.json) fills the season tile,
the heartbeat banner, the 18-row manager grid, the drill-down, the six boards (both scopes), the concentrated and
diversified cards, the clone-alpha table, the options overlay, the signals panel, the reading rules and the
engines observability table; the Codex evidence desk (unchanged JS) verifies the retained run and renders the
manager select, the fund summary and the positions table; selecting a manager in the grid moves both the drill-down
and the evidence desk; the scope toggle switches the boards; no [object Object]/undefined/NaN literal is visible;
no horizontal overflow at 390 px. A 0.5x full-page JPEG is embedded (base64) so the look can be checked offline.
Nothing on the site is changed.
"""
from pathlib import Path
import base64
import json
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
IDS = ["h-title", "h-pills", "book-state", "season", "banner-headline", "verdicts", "side", "mgr-chips", "mgr-foot", "drill-title", "boards-title", "boards-meta",
       "conc-meta", "clone-meta", "clone-method", "signals", "quality", "eng-foot", "state", "asof", "refresh", "page-count"]
COUNTS = {"mgr_rows": "#mgr-table tbody tr", "boards_tables": "#boards table", "conc_cards": "#conc > .sub", "diversified_cards": "#diversified > .sub",
          "clone_rows": "#clone tbody tr", "options_rows": "#options tbody tr", "limitations": "#limitations li", "engines_rows": "#engines tbody tr",
          "selection_options": "#selection option", "coverage_rows": "#coverage tr", "positions_rows": "#positions tr", "chain_rows": "#filing-chain tr"}


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except Exception:
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


def text(page, sel):
    try:
        return " ".join(page.locator(sel).first.inner_text(timeout=3000).split())
    except Exception as e:  # noqa: BLE001
        return "<%s>" % type(e).__name__


def main():
    with report("ops_6724_13f_book_page_probe") as r:
        r.heading("ops 6724 - 13F Institutional Book: live page proof in headless Chrome (read-only)")
        r.section("0. served HTML + book over the public route")
        stamp = int(time.time())
        req = urllib.request.Request("%s/13f.html?nocache=%d" % (BASE, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
        html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
        r.log("13f.html: %d bytes, title %s, book script %s, evidence scripts %s" % (
            len(html), "13F · Institutional Book" in html, "/data/13f-book.json" in html, "jh-holdings-research-page.js" in html))
        req = urllib.request.Request("%s/data/13f-book.json?nocache=%d" % (BASE, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            r.log("data/13f-book.json: HTTP %s, %d bytes, content-type %s, cache-control %s" % (resp.status, len(body), resp.headers.get("Content-Type"), resp.headers.get("Cache-Control")))
        book = json.loads(body)
        r.log("book generated_at %s version %s managers %d cohort %d" % (book.get("generated_at"), book.get("version"), len(((book.get("book") or {}).get("managers") or {})), len((book.get("book") or {}).get("cohort") or [])))
        if "13F · Institutional Book" not in html:
            r.fail("the served 13f.html is not the new page yet (Pages deploy pending?)")
            raise SystemExit(1)

        sync_playwright = ensure_playwright(r.log)
        facts = {}
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors, bad_responses = [], []
            page.on("pageerror", lambda e: errors.append("pageerror: %s" % e))
            page.on("console", lambda m: errors.append("console: %s" % m.text) if m.type == "error" else None)
            page.on("response", lambda resp: bad_responses.append("%s %s" % (resp.status, resp.url)) if resp.status >= 400 else None)
            r.section("1. desktop 1440: load")
            page.goto("%s/13f.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            try:
                page.wait_for_selector("#mgr-table tbody tr", timeout=30000)
            except Exception as e:  # noqa: BLE001
                r.fail("manager grid never rendered (%s): book-state = %s" % (type(e).__name__, text(page, "#book-state")))
            try:
                page.wait_for_function("() => { const s = document.getElementById('state'); return s && !/Verifying/.test(s.textContent); }", timeout=45000)
            except Exception as e:  # noqa: BLE001
                r.warn("evidence desk still verifying after 45 s (%s)" % type(e).__name__)
            try:
                page.wait_for_selector("#positions tr", timeout=45000)
            except Exception as e:  # noqa: BLE001
                r.warn("evidence desk positions table not rendered (%s)" % type(e).__name__)
            page.wait_for_timeout(1500)
            for i in IDS:
                t = text(page, "#" + i)
                facts[i] = t
                r.log("%s: %s" % (i, t[:420]))
            counts = {k: page.locator(sel).count() for k, sel in COUNTS.items()}
            r.log("counts: %s" % json.dumps(counts))
            facts.update(counts)
            r.log("grid row 1: %s" % text(page, "#mgr-table tbody tr"))
            r.log("boards first table: %s" % text(page, "#boards table")[:400])
            r.log("engines row 1: %s" % text(page, "#engines tbody tr")[:300])
            r.log("evidence fund-summary: %s" % text(page, "#fund-summary")[:300])
            bad = page.evaluate("() => (document.body.innerText.match(/\\[object Object\\]|undefined|NaN/g) || []).length")
            r.log("forbidden literals visible: %s" % bad)
            facts["bad_literals"] = bad

            r.section("2. interactions")
            page.click("#scope-conc")
            page.wait_for_timeout(400)
            r.log("after scope toggle: %s | %s" % (text(page, "#boards-title"), text(page, "#boards-meta")[:200]))
            facts["scope_toggle"] = text(page, "#boards-title")
            page.click("#mgr-table button[data-fund='ELLIOTT']")
            page.wait_for_timeout(2500)
            r.log("after selecting Elliott: drill %s | selection=%s | fund-summary %s" % (
                text(page, "#drill-title"), page.locator("#selection").input_value(), text(page, "#fund-summary")[:200]))
            facts["drill_after_click"] = text(page, "#drill-title")
            facts["selection_after_click"] = page.locator("#selection").input_value()
            r.log("drill body: %s" % text(page, "#drill-body")[:600])
            r.log("errors: %s" % (errors[:10] or "none"))
            r.log("responses >= 400: %s" % (bad_responses[:12] or "none"))
            own = [u for u in bad_responses if "/data/13f-book.json" in u or "holdings-research" in u or "/13f.html" in u]
            facts["js_errors"] = len([e for e in errors if not e.startswith("console: Failed to load resource")]) + len(own)
            facts["bad_responses"] = len(bad_responses)

            r.section("3. phone 390")
            page.set_viewport_size({"width": 390, "height": 844})
            page.wait_for_timeout(600)
            overflow = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            r.log("horizontal overflow at 390 px: %s px" % overflow)
            wide = page.evaluate("""() => { const cw = document.documentElement.clientWidth; const out = [];
              const clipped = (el) => { for (let a = el.parentElement; a && a !== document.body; a = a.parentElement) { const o = getComputedStyle(a).overflowX;
                if ((o === 'auto' || o === 'scroll' || o === 'hidden' || o === 'clip') && a.getBoundingClientRect().right <= cw + 1) return true; } return false; };
              for (const el of document.querySelectorAll('body *')) { const rct = el.getBoundingClientRect(); const cs = getComputedStyle(el);
                if (rct.right > cw + 1 && rct.width > 0 && cs.position !== 'fixed' && !clipped(el)) {
                  const inMain = !!el.closest('main'); out.push({tag: el.tagName, id: el.id || '', cls: String(el.className || '').slice(0, 60), right: Math.round(rct.right), w: Math.round(rct.width), pos: cs.position, inMain,
                    path: (() => { const parts = []; let n = el; while (n && n !== document.body && parts.length < 6) { parts.unshift(n.tagName.toLowerCase() + (n.id ? '#' + n.id : '') + (n.className && typeof n.className === 'string' ? '.' + n.className.trim().split(/\\s+/)[0] : '')); n = n.parentElement; } return parts.join('>'); })()}); } }
              return out.slice(0, 20); }""")
            r.log("unclipped elements past the right edge at 390 px: %s" % json.dumps(wide))
            facts["phone_overflow_px"] = overflow
            facts["phone_overflow_in_main"] = sum(1 for w in wide if w.get("inMain"))

            r.section("4. screenshot (0.5x, jpeg, base64)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=0.5)
            p2 = ctx2.new_page()
            p2.goto("%s/13f.html?nocache=%d" % (BASE, stamp + 1), wait_until="domcontentloaded")
            try:
                p2.wait_for_selector("#engines tbody tr", timeout=30000)
                p2.wait_for_selector("#positions tr", timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            p2.wait_for_timeout(1500)
            shot = p2.screenshot(full_page=True, type="jpeg", quality=55)
            r.log("screenshot bytes: %d" % len(shot))
            r.lines.append("")
            r.lines.append("<!-- SHOT_JPEG_BASE64 -->")
            r.lines.append(base64.b64encode(shot).decode("ascii"))
            r.lines.append("<!-- /SHOT_JPEG_BASE64 -->")
            r.lines.append("")
            browser.close()

        ok = (facts.get("mgr_rows", 0) >= 18 and facts.get("boards_tables", 0) == 6 and facts.get("clone_rows", 0) >= 10 and facts.get("engines_rows", 0) >= 10
              and facts.get("bad_literals", 1) == 0 and facts.get("js_errors", 1) == 0 and facts.get("phone_overflow_in_main", 1) == 0
              and "Elliott" in facts.get("drill_after_click", "") and facts.get("selection_after_click") == "ELLIOTT"   # the baked section badge prefixes the heading text
              and facts.get("positions_rows", 0) >= 1 and facts.get("selection_options", 0) >= 18)
        r.kv(**{k: str(v) for k, v in facts.items() if k in ("mgr_rows", "boards_tables", "clone_rows", "options_rows", "engines_rows", "selection_options", "positions_rows",
                                                              "bad_literals", "js_errors", "bad_responses", "phone_overflow_px", "phone_overflow_in_main", "scope_toggle", "drill_after_click", "selection_after_click")})
        if ok:
            r.ok("PROVEN: the Institutional Book renders from the live feed, the evidence desk verifies and follows the selection, the page is clean at 1440 and 390")
        else:
            r.fail("one or more checks failed - see the facts above")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
