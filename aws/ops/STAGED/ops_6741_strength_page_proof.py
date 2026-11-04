"""ops 6741 - READ-ONLY browser proof of strong.html v2 (Khalid 2026-11-04; CLAUDE.md rule 5: click it yourself).

Against https://justhodl.ai in headless Chrome, after ops 6740 published data/strength.json and the Pages build served
the new page:
  1. served HTML carries the v2 engine script and no longer the client-side desk verification scripts;
  2. the page renders from the artifact: the desk table (every desk name with bars), the coil board, leaders /
     laggards, the focus board, the sources table; section order = verdict -> long accumulation -> leaders / laggards
     -> focus -> desk -> reading rules -> sources;
  3. the flows column: how many desk rows show a fund-flow chip, a 13F chip, or neither; "Unavailable" must be gone
     from the page body;
  4. sorting: the desk header "RS 1M" sorts largest-first, again smallest-first; the coil board sorts by base length;
     the scope chips (DESK / ALL / S&P 500) and the sleeve chips (STOCK) filter; no page error; no overflow at 390 px.
A 0.5x JPEG of the page is embedded. Nothing on the site changes.
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
WANT_ORDER = ["Verdict: index regime, breadth and bases", "Long accumulation with a tight price spread", "Leaders and laggards by strength",
              "Focus: holding up while the index is heavy", "The desk: every sleeve against SPY", "Reading rules and definitions", "Sources and engine observability"]


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
    with report("ops_6741_strength_page_proof") as r:
        r.heading("ops 6741 - strong.html v2: live proof (read-only)")
        stamp = int(time.time())
        r.section("0. served HTML + artifact")
        html = fetch("%s/strong.html?nocache=%d" % (BASE, stamp))
        tag = re.search(r'<script src="(/jh-strong-engine\.js[^"]*)"', html)
        r.log("strong.html: %d bytes, engine script %s, desk-verification scripts present: %s, sortable script %s" % (
            len(html), tag.group(1) if tag else "MISSING", "jh-etf-fuse.js" in html, bool(re.search(r"/jh-sortable\.js", html))))
        js = fetch("%s/jh-strong-engine.js?nocache=%d" % (BASE, stamp))
        r.log("/jh-strong-engine.js: %d bytes, v2 marker %s" % (len(js), "strength.v1" in js))
        art = json.loads(fetch("%s/data/strength.json?nocache=%d" % (BASE, stamp)))
        r.log("data/strength.json: contract %s version %s generated %s as_of %s rows %d coil %d" % (
            art.get("contract"), art.get("version"), art.get("generated_at"), art.get("as_of"), len(art.get("rows") or []), len(((art.get("boards") or {}).get("coil")) or [])))
        if not tag or "strength.v1" not in js or art.get("contract") != "strength.v1":
            r.fail("the served page / script / artifact are not v2 yet (Pages deploy or ops 6740 pending)")
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
            r.section("1. render + order + flows")
            page.goto("%s/strong.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#tbl tbody tr td.nm", timeout=60000)
            page.wait_for_selector("#coil tbody tr", timeout=30000)
            page.wait_for_timeout(1500)
            out = page.evaluate("""() => {
              const order = Array.from(document.querySelectorAll('main > section')).map(e => e.getAttribute('aria-label') || e.id);
              const rows = s => Array.from(document.querySelectorAll(s + ' tbody tr'));
              const desk = rows('#tbl'), coil = rows('#coil'), leaders = rows('#leaders'), laggards = rows('#laggards'), focus = rows('#focus'), sources = rows('#sources');
              const flowsIdx = Array.from(document.querySelectorAll('#tbl thead th')).findIndex(th => /Flows/.test(th.textContent));
              let withFlow = 0, with13f = 0, neither = 0, unavailable = 0;
              desk.forEach(tr => { const t = (tr.cells[flowsIdx] || {}).innerText || ''; const f = /flow /.test(t), s = /13F \\d/.test(t); if (f) withFlow++; if (s) with13f++; if (!f && !s) neither++; if (/Unavailable/.test(t)) unavailable++; });
              const kpis = Array.from(document.querySelectorAll('#kpis .jh-kpi .v')).map(e => e.textContent.trim());
              const text = document.body.innerText;
              return {order, desk: desk.length, coil: coil.length, leaders: leaders.length, laggards: laggards.length, focus: focus.length, sources: sources.length,
                      withFlow, with13f, neither, unavailable, unavailableAnywhere: (text.match(/Unavailable/g) || []).length, kpis,
                      coilTop: coil.slice(0, 8).map(tr => [tr.cells[0].innerText.trim(), tr.cells[3].innerText.trim(), tr.cells[5].innerText.trim(), tr.cells[15].innerText.trim()]),
                      leadersTop: leaders.slice(0, 5).map(tr => tr.cells[0].innerText.trim() + ' ' + tr.cells[3].innerText.trim()),
                      laggardsTop: laggards.slice(0, 5).map(tr => tr.cells[0].innerText.trim() + ' ' + tr.cells[3].innerText.trim()),
                      deskSample: desk.slice(0, 4).map(tr => Array.from(tr.cells).map(c => c.innerText.trim().slice(0, 40))),
                      stamp: document.getElementById('stamp').innerText, bad: (text.match(/\\[object Object\\]|undefined|NaN/g) || []).length,
                      sortableInjected: document.querySelectorAll('table[data-jh-sortable]').length};
            }""")
            for k, v in out.items():
                r.log("%s: %s" % (k, json.dumps(v, ensure_ascii=False)[:1200]))
            facts.update(out)
            facts["order_ok"] = [x for x in out["order"] if x in WANT_ORDER] == WANT_ORDER
            r.section("2. clicks: sorting, scope chips, sleeve chips")
            clicks = page.evaluate("""() => {
              const col = (s, i) => Array.from(document.querySelectorAll(s + ' tbody tr')).map(tr => (tr.cells[i] || {}).innerText || '').map(x => x.trim());
              const num = v => parseFloat(String(v).replace(/[+%,×]/g, '').replace('−', '-'));
              const mi = Array.from(document.querySelectorAll('#tbl thead th')).findIndex(th => th.textContent.trim() === 'RS 1M');
              const th = document.querySelector('#tbl thead th[data-sort="vm"]'); th.click();
              const desc = col('#tbl', mi).map(num).filter(x => !isNaN(x));
              const th2 = document.querySelector('#tbl thead th[data-sort="vm"]'); const aria1 = th2.getAttribute('aria-sort'); th2.click();
              const asc = col('#tbl', mi).map(num).filter(x => !isNaN(x));
              const aria2 = document.querySelector('#tbl thead th[data-sort="vm"]').getAttribute('aria-sort');
              const isDesc = desc.every((v, i) => i === 0 || desc[i-1] >= v), isAsc = asc.every((v, i) => i === 0 || asc[i-1] <= v);
              const cb = document.querySelector('#coil thead th[data-sort="base_len"]'); cb.click();
              const bases = col('#coil', 4).map(num); const basesDesc = bases.every((v, i) => i === 0 || bases[i-1] >= v);
              document.querySelector('#coil-scope [data-s="DESK"]').click(); const coilDesk = col('#coil', 2);
              document.querySelector('#coil-scope [data-s="S&P 500"]').click(); const coilSp = col('#coil', 2);
              document.querySelector('#coil-scope [data-s="ALL"]').click(); const coilAll = col('#coil', 0).length;
              document.querySelector('#lead-scope [data-s="ALL"]').click(); const leadAll = col('#leaders', 0).slice(0, 5); const leadAllScopes = col('#leaders', 2);
              document.querySelector('#lead-scope [data-s="DESK"]').click(); const leadDesk = col('#leaders', 2);
              document.querySelector('#chips [data-s="STOCK"]').click(); const stocks = col('#tbl', 2);
              document.querySelector('#chips [data-s="ALL"]').click(); const all = col('#tbl', 0).length;
              return {isDesc, isAsc, aria1, aria2, n: desc.length, basesDesc, basesTop: bases.slice(0, 5), coilDeskScopes: Array.from(new Set(coilDesk)), coilSpScopes: Array.from(new Set(coilSp)), coilAll,
                      leadAll, leadAllScopes: Array.from(new Set(leadAllScopes)), leadDeskScopes: Array.from(new Set(leadDesk)), stockSleeves: Array.from(new Set(stocks)), all};
            }""")
            for k, v in clicks.items():
                r.log("%s: %s" % (k, json.dumps(v, ensure_ascii=False)[:600]))
            facts["sort_ok"] = clicks["isDesc"] and clicks["isAsc"] and clicks["aria1"] == "descending" and clicks["aria2"] == "ascending" and clicks["n"] >= 40 and clicks["basesDesc"]
            facts["chips_ok"] = set(clicks["coilDeskScopes"]) <= {"DESK"} and set(clicks["leadDeskScopes"]) <= {"DESK"} and set(clicks["stockSleeves"]) == {"STOCK"} and clicks["all"] >= 40 and len(clicks["leadAllScopes"]) >= 1
            page.set_viewport_size({"width": 390, "height": 844})
            page.wait_for_timeout(600)
            facts["overflow"] = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            r.log("sort_ok %s | chips_ok %s | overflow at 390 px: %s px | errors: %s" % (facts["sort_ok"], facts["chips_ok"], facts["overflow"], errors[:6] or "none"))
            facts["errors"] = len(errors)

            r.section("3. screenshot (0.5x, jpeg, base64)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=0.5)
            p3 = ctx2.new_page()
            p3.goto("%s/strong.html?nocache=%d" % (BASE, stamp + 2), wait_until="domcontentloaded")
            try:
                p3.wait_for_selector("#sources tbody tr", timeout=45000)
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

        flows_ok = facts["unavailable"] == 0 and facts["unavailableAnywhere"] == 0 and facts["neither"] <= 3 and facts["withFlow"] >= 20 and facts["with13f"] >= 15
        ok = facts["order_ok"] and facts["sort_ok"] and facts["chips_ok"] and flows_ok and facts["bad"] == 0 and facts["errors"] == 0 and facts["overflow"] == 0 and facts["coil"] >= 1 and facts["desk"] >= 40
        r.kv(order_ok=str(facts["order_ok"]), desk=str(facts["desk"]), coil=str(facts["coil"]), with_flow=str(facts["withFlow"]), with_13f=str(facts["with13f"]), neither=str(facts["neither"]),
             unavailable=str(facts["unavailableAnywhere"]), sort_ok=str(facts["sort_ok"]), chips_ok=str(facts["chips_ok"]), errors=str(facts["errors"]), overflow=str(facts["overflow"]))
        if ok:
            r.ok("STRONG V2 PROVEN LIVE: %d desk rows, flows on %d funds + 13F on %d stocks (neither %d, 'Unavailable' %d), coil board %d names, sorting and chips click-verified" % (
                facts["desk"], facts["withFlow"], facts["with13f"], facts["neither"], facts["unavailableAnywhere"], facts["coil"]))
        else:
            r.fail("NOT PROVEN: order %s sort %s chips %s flows %s bad %s errors %s overflow %s coil %s desk %s" % (
                facts["order_ok"], facts["sort_ok"], facts["chips_ok"], flows_ok, facts["bad"], facts["errors"], facts["overflow"], facts["coil"], facts["desk"]))
            raise SystemExit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
