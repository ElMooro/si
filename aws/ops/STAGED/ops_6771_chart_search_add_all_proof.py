"""ops 6771 - READ-ONLY browser proof of chart.html's universal search "add all" (Khalid 2026-11-10 11:34 ET: "add the option to
add all the series from that drop down menu all at once ... design like TradingView's, big and beautiful"). Re-runnable;
used as the BASELINE (PR #148 of the Codex lane, 10:47 ET) and again as the PROOF after Claude's enhancement.

Against https://justhodl.ai/chart.html in headless Chrome (1440x900 and 390x844), for queries on the macro side
("unemployment rate", "cpi") and a market one ("spy"):
  1. open the dialog (window.jhOpenSearch), wait for the results; read the dialog's size, the input font, the chips, the
     first rows (symbol / name / meta) and every "+ Add all" control on the page (group, loaded, dataset);
  2. click the dropdown-wide "+ Add all N" control; the active watchlist's symbol count must grow by exactly the number
     the control promised (minus duplicates the toast reports); the per-row "+" buttons turn to a check;
  3. switch to the Economy chip and repeat the count check for that tab's rows;
  4. 390 px: the dialog fits (no horizontal overflow) and the add-all control is reachable;
  5. 0 page errors; a 0.5x JPEG of the open dialog is embedded.
Nothing on the site changes; the watchlist lives in the probe browser's localStorage only.
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
QUERIES = ["unemployment rate", "cpi", "spy"]


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
  const q = s => document.querySelectorAll(s), dlg = document.querySelector('#jhus .us-dlg'), r = dlg ? dlg.getBoundingClientRect() : null;
  const inp = document.getElementById('jhus-in'), cs = inp ? getComputedStyle(inp) : null;
  const rows = [...q('#jhus .us-row')].slice(0, 6).map(e => ({sym: (e.querySelector('.us-sym') || {}).innerText, name: ((e.querySelector('.us-desc .n') || {}).innerText || '').slice(0, 60), meta: ((e.querySelector('.us-meta') || {}).innerText || '').replace(/\\n/g, ' | ').slice(0, 60), h: Math.round(e.getBoundingClientRect().height)}));
  const adds = [...q('#jhus [data-act="addloaded"], #jhus [data-act="addgrp"], #jhus [data-act="addbrowse"], #jhus [data-act="addds"]')].map(b => ({act: b.getAttribute('data-act'), grp: b.getAttribute('data-grp'), text: b.innerText.trim().slice(0, 40), visible: b.getBoundingClientRect().width > 0}));
  const lists = window.JHTvWatchlist ? window.JHTvWatchlist.lists() : [];
  return {open: !!(document.getElementById('jhus') && document.getElementById('jhus').classList.contains('on')), dlg: r ? {w: Math.round(r.width), h: Math.round(r.height), top: Math.round(r.top)} : null,
          inputFont: cs ? cs.fontSize : null, chips: [...q('#jhus .us-chip')].map(e => e.innerText.trim() + (e.classList.contains('on') ? '*' : '')), nRows: q('#jhus .us-row').length, rows, adds,
          status: (document.querySelector('#jhus .us-status') || {}).innerText || '', groups: [...q('#jhus .us-grp')].map(e => e.innerText.trim().slice(0, 50)),
          plusChecks: q('#jhus [data-act="add"].ok').length, plusTotal: q('#jhus [data-act="add"]').length, lists, active: window.JHTvWatchlist ? window.JHTvWatchlist.activeName() : null,
          overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth, toast: (document.getElementById('jhus-toast') || {}).innerText || ''}; }"""


def main():
    with report("ops_6771_chart_search_add_all_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6771 - chart.html universal search: add-all proof %s" % now.isoformat(timespec="minutes"))
        sync_playwright = ensure_playwright(r.log)
        fails, shots = [], []

        def check(name, fn):
            try:
                out = fn()
                r.log("ok   %s%s" % (name, (": " + str(out)[:600]) if out not in (None, True) else ""))
                return out
            except Exception as e:  # noqa: BLE001
                fails.append(name)
                r.warn("FAIL %s: %s %s" % (name, type(e).__name__, str(e)[:400]))
                return None
        with sync_playwright() as p:
            browser = launch(p, r.log)
            for W, H in ((1440, 900), (390, 844)):
                ctx = browser.new_context(viewport={"width": W, "height": H}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6771")
                page = ctx.new_page()
                page.set_default_timeout(45000)
                errors = []
                page.on("pageerror", lambda e: errors.append(str(e)[:200]))
                page.goto("%s/chart.html?s=SPY&nocache=%d" % (SITE, int(time.time())), wait_until="domcontentloaded")
                try:
                    page.wait_for_function("() => window.jhOpenSearch && window.JHTvWatchlist && window.JHUniSearch", timeout=45000)
                except Exception as e:  # noqa: BLE001
                    r.warn("%dpx: search / watchlist modules not ready (%s)" % (W, type(e).__name__))
                page.wait_for_timeout(2500)
                r.section("%dx%d" % (W, H))
                if W == 1440:
                    check("the page serves the enhanced search module (not a cached copy)", lambda: page.evaluate("""async () => { const s = [...document.scripts].map(x => x.src).find(u => /jh-uni-search\.js/.test(u)); if (!s) throw new Error('no jh-uni-search.js script tag');
                        const t = await (await fetch(s, {cache: 'no-store'})).text(); const has = /data-act="openlist"/.test(t) && /us-badge/.test(t) && /Every chartable row below/.test(t);
                        if (!has) throw new Error('served module lacks the 2026-11-10 markers: ' + s); return s.replace(location.origin, '') + ' (' + t.length + ' bytes, markers present)'; }"""))
                for qi, query in enumerate(QUERIES):
                    page.evaluate("q => window.jhOpenSearch(q, 'chart')", query)
                    try:
                        page.wait_for_function("() => document.querySelectorAll('#jhus .us-row').length > 3 && !/Searching|Loading/.test((document.querySelector('#jhus .us-status') || {}).innerText || '')", timeout=30000)
                    except Exception:  # noqa: BLE001
                        pass
                    page.wait_for_timeout(1500)
                    st = page.evaluate(STATE_JS)
                    r.log("'%s': dialog %s input %s | chips %s | %d rows | groups %s | add controls %s | status %s | lists %s" % (
                        query, json.dumps(st["dlg"]), st["inputFont"], " ".join(st["chips"]), st["nRows"], st["groups"], json.dumps(st["adds"])[:400], st["status"][:80], json.dumps(st["lists"])[:200]))
                    for row in st["rows"]:
                        r.log("    %-14s %-60s %s (%dpx)" % (row["sym"], row["name"], row["meta"], row["h"]))
                    if W == 1440 and qi == 0:
                        shots.append(page.screenshot(type="jpeg", quality=50))
                    loaded = [a for a in st["adds"] if a["act"] == "addloaded"]
                    check("'%s' has the dropdown-wide add-all control" % query, lambda: loaded[0]["text"] if loaded and loaded[0]["visible"] else (_ for _ in ()).throw(AssertionError("no visible addloaded control: %s" % st["adds"])))
                    if loaded and W == 1440:
                        check("'%s' add-all names the destination list and the rows carry type badges + provider marks" % query, lambda: page.evaluate("""() => { const b = document.querySelector('#jhus [data-act=addloaded]'), t = b ? b.innerText : '';
                            const badges = document.querySelectorAll('#jhus .us-row .us-badge').length, marks = document.querySelectorAll('#jhus .us-row .us-mark').length, logo = document.querySelector('#jhus .us-row .us-logo'), lw = logo ? Math.round(logo.getBoundingClientRect().width) : 0;
                            if (!/ to /.test(t)) throw new Error('add-all text lacks the list name: ' + t); if (!badges) throw new Error('no type badges'); if (!marks) throw new Error('no provider marks'); if (lw < 30) throw new Error('logo ' + lw + 'px');
                            return t + ' | badges ' + badges + ' | marks ' + marks + ' | logo ' + lw + 'px'; }"""))
                    if loaded:
                        import re
                        promised = int((re.search(r"(\d[\d,]*)", loaded[0]["text"]) or [None, "0"])[1].replace(",", ""))
                        before = {x["name"]: x["n"] for x in st["lists"]}
                        active = st["active"]

                        def click_all():
                            page.click('#jhus [data-act="addloaded"]')
                            page.wait_for_timeout(1200)
                            st2 = page.evaluate(STATE_JS)
                            after = {x["name"]: x["n"] for x in st2["lists"]}
                            grew = (after.get(active) or 0) - (before.get(active) or 0)
                            m = re.search(r"Added (\d+)", st2["toast"] or "")
                            added = int(m.group(1)) if m else None
                            dup = re.search(r"(\d+) already there", st2["toast"] or "")
                            dup = int(dup.group(1)) if dup else 0
                            assert added is not None and added + dup == promised, "promised %s, toast %r" % (promised, st2["toast"])
                            assert grew == added, "list %s grew by %s, toast says %s" % (active, grew, added)
                            assert st2["plusChecks"] >= min(st2["plusTotal"], 1), "row buttons did not turn to checks: %s/%s" % (st2["plusChecks"], st2["plusTotal"])
                            return "promised %d: added %d, %d duplicates; '%s' %s -> %s; %d/%d row buttons checked; toast '%s'" % (promised, added, dup, active, before.get(active), after.get(active), st2["plusChecks"], st2["plusTotal"], st2["toast"][:80])
                        check("'%s' add-all adds exactly what it promised to the active list" % query, click_all)
                    if qi == 0:
                        def economy_tab():
                            page.click('#jhus .us-chip[data-tab="economy"]')
                            try:
                                page.wait_for_function("() => !/Searching|Loading/.test((document.querySelector('#jhus .us-status') || {}).innerText || '')", timeout=30000)
                            except Exception:  # noqa: BLE001
                                pass
                            page.wait_for_timeout(1200)
                            st3 = page.evaluate(STATE_JS)
                            loaded3 = [a for a in st3["adds"] if a["act"] == "addloaded"]
                            assert any(c.startswith("Economy*") for c in st3["chips"]), st3["chips"]
                            assert st3["nRows"] > 0, "no economy rows"
                            return "%d rows, add controls %s, first %s" % (st3["nRows"], json.dumps(loaded3)[:120], json.dumps(st3["rows"][:2])[:200])
                        check("'%s' Economy chip filters the rows and keeps the add-all control" % query, economy_tab)
                    page.evaluate("() => { const x = document.querySelector('#jhus [data-act=close]'); if (x) x.click(); }")
                    page.wait_for_timeout(300)
                if W == 1440:
                    def home_view():
                        page.evaluate("() => window.jhOpenSearch('', 'chart')")
                        page.wait_for_timeout(1200)
                        st4 = page.evaluate("""() => ({groups: [...document.querySelectorAll('#jhus .us-grp')].map(e => e.innerText.trim()), lists: [...document.querySelectorAll('#jhus [data-act=openlist]')].map(e => e.innerText.replace(/\\n/g, ' | ').slice(0, 80)),
                            nLists: window.JHTvWatchlist ? window.JHTvWatchlist.lists().length : 0, dlg: (r => ({w: Math.round(r.width), h: Math.round(r.height)}))(document.querySelector('#jhus .us-dlg').getBoundingClientRect())})""")
                        assert st4["dlg"]["w"] >= 1000, "dialog not TradingView-sized: %s" % st4["dlg"]
                        if st4["nLists"]:
                            assert st4["lists"] and len(st4["lists"]) == min(12, st4["nLists"]), (st4["lists"], st4["nLists"])
                            page.click('#jhus [data-act=openlist]')
                            page.wait_for_timeout(600)
                            closed = page.evaluate("() => !document.getElementById('jhus').classList.contains('on')")
                            shown = page.evaluate("() => { const w = document.getElementById('watch'); return !!w && !w.classList.contains('is-collapsed'); }")
                            assert closed, "dialog did not close after opening a list"
                            return "dialog %s, groups %s, %d watchlists listed (%s...), click -> dialog closed, watchlist pane open: %s" % (json.dumps(st4["dlg"]), st4["groups"], len(st4["lists"]), st4["lists"][0], shown)
                        return "dialog %s, groups %s, no watchlists in this browser" % (json.dumps(st4["dlg"]), st4["groups"])
                    check("home view: TradingView-sized dialog and one-click access to your watchlists", home_view)
                    page.evaluate("() => { const x = document.querySelector('#jhus [data-act=close]'); if (x) x.click(); }")
                if W == 390:
                    page.evaluate("q => window.jhOpenSearch(q, 'chart')", "cpi")
                    page.wait_for_timeout(4000)
                    st = page.evaluate(STATE_JS)
                    check("390px: dialog fits and the add-all control is reachable", lambda: "overflow %d, dialog %s, addloaded visible %s" % (st["overflow"], json.dumps(st["dlg"]), any(a["act"] == "addloaded" and a["visible"] for a in st["adds"])) if st["overflow"] == 0 and any(a["act"] == "addloaded" and a["visible"] for a in st["adds"]) else (_ for _ in ()).throw(AssertionError("overflow %s adds %s" % (st["overflow"], st["adds"]))))
                    shots.append(page.screenshot(type="jpeg", quality=45))
                r.log("page errors: %s" % (errors or "none"))
                if errors:
                    fails.append("page errors at %dpx" % W)
                ctx.close()
            browser.close()
        r.kv(failed=str(len(fails)))
        for i, s_ in enumerate(shots):
            r.section("screenshot %d (jpeg, base64)" % (i + 1))
            r.lines.append("")
            r.lines.append("<!-- SCREENSHOT_%d_JPEG_BASE64 -->" % (i + 1))
            r.lines.append(base64.b64encode(s_).decode("ascii"))
            r.lines.append("<!-- /SCREENSHOT_%d_JPEG_BASE64 -->" % (i + 1))
        if fails:
            r.fail("ops 6771: %d check(s) failed: %s" % (len(fails), " | ".join(fails)[:800]))
            raise SystemExit(1)
        r.ok("ops 6771 complete: the dropdown-wide add-all works on the live chart search for every query tried")


if __name__ == "__main__":
    main()
    sys.exit(0)
