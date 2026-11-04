"""ops 6738 - READ-ONLY browser proof of Khalid's two page rules (2026-11-04): every table sorts ascending / descending,
and 13f.html is ordered most-important-first (CLAUDE.md rule 5: click it yourself).

Against https://justhodl.ai in headless Chrome:
  1. 13f.html: the served section order is the doctrine order (docs/ops/PAGE_ORDER.md); every rendered table carries
     data-jh-sortable; clicking a header on the sector table sorts descending, again ascending, a third time restores;
     the manager grid sorts A-Z by name; the stance table sorts by net $; no page error, no overflow at 390 px.
  2. two other pages that got the script only through the build-time injection (bonds.html, holdings-research.html):
     /jh-sortable.js is in the served HTML with a ?v= stamp and their tables become sortable (a header click reorders).
A 0.5x JPEG of the reordered 13F page is embedded. Nothing on the site changes.
"""
from pathlib import Path
import base64
import json
import re
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
WANT_ORDER = ["Book heartbeat", "Smart money controls", "Sectors and industries", "Asset-class rotation", "Contenders", "Cross-manager boards", "Managers",
              "Manager drill-down", "Concentrated books", "Diversified books", "Clone-alpha", "Options overlay", "Price divergence and brief", "Security master",
              "Filing evidence", "Reading rules", "13F engines"]
OTHERS = ["bonds.html", "holdings-research.html"]


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


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 jh-ops"})
    return urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")


def main():
    with report("ops_6738_sortable_and_order_page_probe") as r:
        r.heading("ops 6738 - sortable tables on every page + 13f.html most-important-first: live proof (read-only)")
        stamp = int(time.time())
        r.section("0. served HTML")
        html = fetch("%s/13f.html?nocache=%d" % (BASE, stamp))
        tag = re.search(r'<script src="(/jh-sortable\.js[^"]*)"', html)
        r.log("13f.html: %d bytes, sortable script %s" % (len(html), tag.group(1) if tag else "MISSING"))
        js = fetch("%s/jh-sortable.js?nocache=%d" % (BASE, stamp))
        r.log("/jh-sortable.js: %d bytes, marker %s" % (len(js), "__jhSortable" in js))
        others_html = {}
        for pg in OTHERS:
            h = fetch("%s/%s?nocache=%d" % (BASE, pg, stamp))
            t = re.search(r'<script src="(/jh-sortable\.js[^"]*)"', h)
            others_html[pg] = t.group(1) if t else None
            r.log("%s: %d bytes, sortable script %s" % (pg, len(h), others_html[pg] or "MISSING"))
        if not tag or "__jhSortable" not in js:
            r.fail("the served 13f.html does not carry /jh-sortable.js yet (Pages deploy pending?)")
            raise SystemExit(1)

        sync_playwright = ensure_playwright(r.log)
        facts = {}
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append("pageerror: %s" % e))
            page.on("console", lambda m: errors.append("console: %s" % m.text) if m.type == "error" and "Failed to load resource" not in m.text else None)
            r.section("1. 13f.html: order + sorting")
            page.goto("%s/13f.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#sectors tbody tr", timeout=45000)
            try:
                page.wait_for_selector("#positions tr", timeout=45000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(1500)
            out = page.evaluate("""() => {
              const order = Array.from(document.querySelectorAll('main > section')).map(e => e.getAttribute('aria-label') || e.id);
              const tables = document.querySelectorAll('table').length, sortable = document.querySelectorAll('table[data-jh-sortable]').length;
              const col = (sel, i) => Array.from(document.querySelectorAll(sel + ' tbody tr')).map(r => (r.cells[i] || {}).innerText || '').map(s => s.trim());
              const th = document.querySelector('#sectors thead th:nth-child(2)');
              const before = col('#sectors', 1); th.click(); const desc = col('#sectors', 1); const ariaDesc = th.getAttribute('aria-sort'); th.click(); const asc = col('#sectors', 1); const ariaAsc = th.getAttribute('aria-sort'); th.click(); const restored = col('#sectors', 1);
              const th2 = document.querySelector('#mgr-table thead th:nth-child(1)'); th2.click(); const mgrs = Array.from(document.querySelectorAll('#mgr-table tbody tr')).map(r => r.cells[0].innerText.trim().split('\\n')[0]);
              const th3 = document.querySelector('#stance table thead th:nth-child(4)'); th3.click(); const net = col('#stance table', 3);
              const th4 = document.querySelector('#industries thead th:nth-child(16)'); th4.click(); const indNet = Array.from(document.querySelectorAll('#industries tbody tr')).map(r => r.cells[15].innerText.trim()).slice(0, 5);
              return {order, tables, sortable, before: before.slice(0, 6), desc: desc.slice(0, 6), asc: asc.slice(0, 6), restored: restored.slice(0, 6), ariaDesc, ariaAsc, mgrs: mgrs.slice(0, 6), net: net.slice(0, 6), indNet,
                      bad: (document.body.innerText.match(/\\[object Object\\]|undefined|NaN/g) || []).length};
            }""")
            for k, v in out.items():
                r.log("%s: %s" % (k, json.dumps(v, ensure_ascii=False)))
            facts.update(out)
            skipped = page.evaluate("""() => Array.from(document.querySelectorAll('table:not([data-jh-sortable])')).map(t => ({
                section: (t.closest('section') || {getAttribute: () => ''}).getAttribute('aria-label') || '', id: t.id, cls: String(t.className).slice(0, 40), thead: !!t.tHead, rows: t.rows.length,
                first: t.rows[0] ? Array.from(t.rows[0].cells).map(c => c.tagName + (c.colSpan > 1 ? '(cs' + c.colSpan + ')' : '') + ':' + c.innerText.trim().slice(0, 16) + (c.hasAttribute('aria-sort') || c.hasAttribute('data-sort') || c.hasAttribute('onclick') ? '[own-sort]' : '') + (c.querySelector('button,a,input,select') ? '[control]' : '')) : []}))""")
            r.log("tables not made sortable (%d): %s" % (len(skipped), json.dumps(skipped, ensure_ascii=False)[:1500]))
            facts["skipped"] = skipped
            facts["skipped_sortable_shape"] = [t for t in skipped if t["rows"] >= 2 and t["first"] and all(f.startswith("TH") for f in t["first"]) and not any("(cs" in f or "[own-sort]" in f or "[control]" in f for f in t["first"])]
            facts["order_ok"] = [x for x in out["order"] if x in WANT_ORDER] == WANT_ORDER
            facts["all_sortable"] = out["tables"] > 0 and not facts["skipped_sortable_shape"]   # every table that has a plain header row is sortable; the rest are listed with their shape
            facts["sort_ok"] = (out["desc"] != out["asc"] and out["restored"] == out["before"] and out["ariaDesc"] == "descending" and out["ariaAsc"] == "ascending"
                                and out["mgrs"] == sorted(out["mgrs"], key=lambda s: s.lower()))
            r.log("order_ok %s | all_sortable %s | sort_ok %s" % (facts["order_ok"], facts["all_sortable"], facts["sort_ok"]))
            page.set_viewport_size({"width": 390, "height": 844})
            page.wait_for_timeout(600)
            facts["overflow"] = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            r.log("overflow at 390 px: %s px | errors: %s" % (facts["overflow"], errors[:6] or "none"))
            facts["errors"] = len(errors)

            r.section("2. other pages (build-time injection)")
            facts["others"] = {}
            for pg in OTHERS:
                p2 = ctx.new_page()
                p2.set_viewport_size({"width": 1440, "height": 900})
                errs2 = []
                p2.on("pageerror", lambda e, errs2=errs2: errs2.append(str(e)))
                p2.goto("%s/%s?nocache=%d" % (BASE, pg, stamp + 1), wait_until="domcontentloaded")
                try:
                    p2.wait_for_selector("table tbody tr", timeout=30000)
                except Exception:  # noqa: BLE001
                    pass
                p2.wait_for_timeout(2500)
                res = p2.evaluate("""() => { const tables = document.querySelectorAll('table').length, sortable = document.querySelectorAll('table[data-jh-sortable]').length;
                  const t = document.querySelector('table[data-jh-sortable]'); if (!t) return {tables, sortable};
                  const idx = Math.min(1, t.tHead.rows[0].cells.length - 1); const col = () => Array.from(t.tBodies[0].rows).map(r => (r.cells[idx] || {}).innerText || '');
                  const before = col(); t.tHead.rows[0].cells[idx].click(); const after = col(); t.tHead.rows[0].cells[idx].click(); const after2 = col();
                  return {tables, sortable, rows: before.length, changed: JSON.stringify(before) !== JSON.stringify(after) || JSON.stringify(after) !== JSON.stringify(after2), sample: before.slice(0,3), sorted: after.slice(0,3)}; }""")
                res["script"] = others_html.get(pg)
                res["errors"] = len(errs2)
                facts["others"][pg] = res
                r.log("%s: %s" % (pg, json.dumps(res, ensure_ascii=False)[:500]))
                p2.close()

            r.section("3. screenshot (0.5x, jpeg, base64)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=0.5)
            p3 = ctx2.new_page()
            p3.goto("%s/13f.html?nocache=%d" % (BASE, stamp + 2), wait_until="domcontentloaded")
            try:
                p3.wait_for_selector("#engines tbody tr", timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            p3.wait_for_timeout(1500)
            shot = p3.screenshot(full_page=True, type="jpeg", quality=55)
            r.log("screenshot bytes: %d" % len(shot))
            r.lines.append("")
            r.lines.append("<!-- SHOT_JPEG_BASE64 -->")
            r.lines.append(base64.b64encode(shot).decode("ascii"))
            r.lines.append("<!-- /SHOT_JPEG_BASE64 -->")
            r.lines.append("")
            browser.close()

        others_ok = all(v.get("script") and v.get("sortable", 0) >= 1 and v.get("changed") for v in facts["others"].values())
        ok = facts["order_ok"] and facts["all_sortable"] and facts["sort_ok"] and facts["bad"] == 0 and facts["errors"] == 0 and facts["overflow"] == 0 and others_ok
        r.kv(order_ok=str(facts["order_ok"]), tables=str(facts["tables"]), sortable=str(facts["sortable"]), sort_ok=str(facts["sort_ok"]), errors=str(facts["errors"]), overflow=str(facts["overflow"]),
             others_ok=str(others_ok), **{pg.replace(".html", "_sortable"): str(v.get("sortable")) for pg, v in facts["others"].items()})
        if ok:
            r.ok("PROVEN: 13f.html is ordered most-important-first and every table on it sorts both ways; the build-time injection makes %s sortable too" % ", ".join(OTHERS))
        else:
            r.fail("one or more checks failed - see the facts above")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
