"""ops 6845 - READ-ONLY live proof: the eight Wyckoff desks behind the chart's icons, in the tearsheet design, and
the all-stock boards for Accumulation / Bottom / Distribution (re-runnable).

Khalid 2026-11-19: "now do the same design for climax, capitulation, bc climax, nrspread, sp spring, accumulation
(all these have icons on chart.html) by the way, accumulation, bottom, distribution engines should be built for the
icons there it should show all the corresponding stocks and then give their ui/ux that design".

CLAUDE.md "CHECK YOUR WORK": a green deploy is not the proof. This op opens the live site in Chrome on the runner and
  1. confirms the new build is what is served;
  2. reads the five live feeds and works out, here in Python, what each sheet must say (counts, the ring's parts, the
     first row of the size and volume lists);
  3. opens wyckoff-desk.html at 1440 px and at 390 px, walks all ten tabs and compares every sheet with (2); on the
     Accumulation, Bottom and Distribution tabs it reads the all-stock board, sorts it both ways and filters it;
  4. opens capitulation-desk.html;
  5. opens chart.html and clicks every one of the eight icons: the five desk icons open their sheet; Acc, Bot and Dist
     light up, open their all-stock desk, and go dark on the next click; a row of a board loads that symbol on the
     chart with the marks on; a link with ?marks= arrives with the marks on.
Writes nothing.
"""
from datetime import datetime, timezone
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
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
TABS = ["sc", "capit", "bc", "spread", "spring", "accum", "bottom", "pump", "dist", "top"]
WORD = {"sc": "Selling climax", "capit": "Capitulation", "bc": "Buying climax", "spread": "Candle spread", "spring": "Spring", "accum": "Accumulation", "bottom": "Bottom", "dist": "Distribution"}
BOARD = {"accum": ("accumulation", ["ABOVE", "BASE", "LOST"]), "bottom": ("bottom", ["HELD", "UNDERCUT", "BROKEN"]), "dist": ("distribution", ["BELOW", "RANGE", "RECLAIMED"])}
ICONS = [("btn-wysc", "tab=sc", "sc"), ("btn-capit", "capitulation-desk.html", "capit"), ("btn-bc", "tab=bc", "bc"), ("btn-nr", "tab=spread", "spread"), ("btn-spr", "tab=spring", "spring")]
MARKS = [("btn-acc", "accum", "tab=accum"), ("btn-bot", "bottom", "tab=bottom"), ("btn-dist", "dist", "tab=dist")]


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


def get(path, stamp, as_json=True):
    raw = urllib.request.urlopen(urllib.request.Request("%s/%s%snocache=%d" % (BASE, path, "&" if "?" in path else "?", stamp), headers=UA), timeout=90).read()
    return json.loads(raw) if as_json else raw.decode("utf-8", "replace")


def num(n):
    return "{:,}".format(n)


def expected(feeds):
    """What each sheet must say, from the feeds alone."""
    scan, bottom, phase, katlin, capit = feeds["scan"], feeds["bottom"], feeds["phase"], feeds["katlin"], feeds["capit"]
    E = {}
    for tab, (board, statuses) in BOARD.items():
        for kind in ("stock", "etf"):
            rows = [x for x in scan["boards"][board] if x["kind"] == kind]
            E[(tab, kind)] = {"n": len(rows), "parts": [len([x for x in rows if x["status"] == s]) for s in statuses],
                              "largest": max(rows, key=lambda x: x.get("mcap") or 0)["ticker"] if rows else None,
                              "tickers": [x["ticker"] for x in rows],
                              "since_top": sorted([x["since_pct"] for x in rows if isinstance(x.get("since_pct"), (int, float))], reverse=True)[:3],
                              "scanned": scan["universe"]["scanned_etf" if kind == "etf" else "scanned_stock"]}
    sc = [x for x in bottom.get("board") or [] if x.get("frame") == "D" and x.get("state") in ("CLIMAX", "TESTING")]
    E["sc"] = {"n": len(sc), "first_volume": max(sc, key=lambda x: x.get("sc_vol_x") or 0)["ticker"] if sc else None, "climax": len([x for x in sc if x["state"] == "CLIMAX"])}
    E["capit"] = {"n": capit.get("n_prints_60"), "sum_5": (capit.get("breadth") or {}).get("sum_5"), "parts": sorted([v.get("prints_60") or 0 for v in (capit.get("counts") or {}).values() if v.get("prints_60")], reverse=True),
                  "first_latest": max(capit.get("latest") or [{}], key=lambda x: x.get("score") or 0).get("ticker")}
    tk = phase.get("tickers") or {}
    E["bc"] = {"n": len([t for t, v in tk.items() if any((e or {}).get("type") == "BC" for e in (v or {}).get("events") or [])])}
    picks = [x for x in katlin.get("picks") or [] if x and x.get("bbw_pctile") is not None and x["bbw_pctile"] <= 20 and (x.get("squeeze") or ((x.get("accum_legs") or {}).get("bb_tight") or 0) >= 70)]
    E["spread"] = {"n": len(picks), "squeeze": len([x for x in picks if x.get("squeeze")]), "tightest": min(picks, key=lambda x: x["bbw_pctile"])["bbw_pctile"] if picks else None}
    fired = forming = failed = 0
    for t, v in tk.items():
        v = v or {}
        spring = [e for e in v.get("events") or [] if (e or {}).get("type") == "SPRING"]
        if v.get("phase") == "ACCUMULATION" and v.get("range_low") and v.get("price"):
            dist = (v["price"] - v["range_low"]) / v["range_low"] * 100
            if spring:
                fired += 1
            elif (v.get("days_in_phase") or 0) >= 10 and dist <= 3:
                forming += 1
        elif spring and v.get("phase") == "DISTRIBUTION":
            failed += 1
    E["spring"] = {"n": fired + forming + failed, "parts": [fired, forming, failed]}
    return E


SHEET = r"""() => { const el = document.getElementById('jh-tearsheet'); if (!el) return null; const q = (s) => Array.from(el.querySelectorAll(s)); const t = (e) => e ? (e.innerText || e.textContent || '').replace(/\s+/g, ' ').trim() : '';
  const r = el.getBoundingClientRect(), ring = el.querySelector('[data-jts-list="ring"]');
  return { state: el.dataset.state, hidden: el.hidden, eye: t(el.querySelector('.jts-eye')), title: t(el.querySelector('.jts-h')), tiles: q('.jts-kpi').map(t), ctl: q('[data-jts-ctl]').map(b => t(b) + (b.getAttribute('aria-pressed') === 'true' ? '*' : '')),
    center: t(el.querySelector('.jts-rc')), total: ring ? +ring.dataset.total : null, arcs: q('.jts-arc').length, legend: q('.jts-leg tr').map(tr => t(tr.querySelector('.jts-nm')) + ' = ' + t(tr.querySelector('.jts-big'))),
    lists: q('[data-jts-list]').filter(c => c.getAttribute('data-jts-list') !== 'ring').map(c => ({ k: c.getAttribute('data-jts-list'), n: c.querySelectorAll('tbody tr').length, first: t(c.querySelector('tbody tr .jts-nm')) })),
    bad: (t(el).match(/undefined|NaN|\bnull\b|\[object|Infinity/g) || []).length, w: Math.round(r.width), h: Math.round(r.height), ox: document.documentElement.scrollWidth - document.documentElement.clientWidth, version: (window.JHTearsheet || {}).version }; }"""
PANEL = r"""() => { const p = document.getElementById('scan-panel'); if (!p) return null; const t = (e) => e ? (e.innerText || e.textContent || '').replace(/\s+/g, ' ').trim() : '';
  const own = document.querySelector('.panel'), rows = Array.from(p.querySelectorAll('tbody tr[data-t]'));
  return { h2: t(p.querySelector('h2')), rows: rows.length, tickers: rows.slice(0, 400).map(tr => tr.getAttribute('data-t')), heads: Array.from(p.querySelectorAll('th')).map(t), n: t(p.querySelector('.sc-n')), kinds: Array.from(p.querySelectorAll('[data-sc-kind]')).map(b => t(b) + (b.getAttribute('aria-pressed') === 'true' ? '*' : '')),
    first: rows.slice(0, 3).map(tr => Array.from(tr.children).map(t)), ownHidden: own ? own.hidden : null, above: own ? !!(p.compareDocumentPosition(own) & 4) : null, w: Math.round(p.getBoundingClientRect().width) }; }"""
CHART = r"""() => ({ camp: Object.assign({}, window.__jhCampLayer), dist: window.__jhDistOn || 0, lit: ['btn-acc', 'btn-bot', 'btn-dist'].filter(id => (document.getElementById(id) || { classList: { contains() {} } }).classList.contains('on')),
  overlay: (document.getElementById('ws-overlay') || {}).className || '', head: ((document.querySelector('#ws-overlay .wshd span') || {}).textContent || '').trim(), src: document.querySelector('#ws-overlay iframe') ? document.querySelector('#ws-overlay iframe').getAttribute('src') : null,
  bars: (window.lastBars || []).length, wrapped: [!!(window.jhCampToggle || {}).__jhDesk, !!(window.jhDistToggle || {}).__jhDesk, typeof window.jhEnsureMarks] })"""
DRAWN = r"""() => { const d = window.lastBars || [], iso = (s) => new Date(s * 1000).toISOString().slice(0, 10);
  const c = window.jhCampaignMarks ? window.jhCampaignMarks(d, 'X', '1d', 'candles') : [], x = window.jhDistributionMarks ? window.jhDistributionMarks(d, '1d', 'candles') : [];
  return { bars: d.length, camp: c.length, campLast: c.slice(-2).map(m => m.text + ' ' + iso(m.time)), dist: x.length, distLast: x.slice(-2).map(m => m.text + ' ' + iso(m.time)) }; }"""


def kit_minor(kit):
    """The kit's 1.x version: this proof was written for 1.3.0 and holds for every later kit (1.4.0 added the Volatility adapter)."""
    m = re.search(r"version: '1\.(\d+)\.\d+'", kit)
    return int(m.group(1)) if m else -1


def main():
    with report("ops_6845_wyckoff_desks_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6845 - the Wyckoff desks and the all-stock boards, live %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        wy, kit, rail, chart, scanjs = (get(p, stamp, False) for p in ("wyckoff-desk.html", "jh-tearsheet.js", "jh-chart-tvrail.js", "chart.html", "jh-wyckoff-scan.js"))
        new = 'data-jts="wyckoff"' in wy and kit_minor(kit) >= 3 and "ADAPTERS.wyckoff" in kit and "jhEnsureMarks" in rail and "marks=20261119" in chart and "JHWyckoffHook" in scanjs
        r.log("served: wyckoff-desk.html %d bytes, kit %d, rail %d, chart.html %d, scan script %d - the new build is live: %s" % (len(wy), len(kit), len(rail), len(chart), len(scanjs), new))
        if not new:
            r.fail("the served site is not the new build yet (Pages deploy pending?)")
            raise SystemExit(1)
        feeds = {"scan": get("data/campaign-scan.json", stamp), "bottom": get("data/bottom.json", stamp), "phase": get("data/phase-detector.json", stamp), "katlin": get("data/katlin.json", stamp), "capit": get("data/capitulation-desk.json", stamp)}
        scan = feeds["scan"]
        r.log("campaign scan: engine %s, bars as of %s, published %s, scanned %s stocks / %s ETFs; boards %s; flagged rows %s" % (
            scan.get("version"), scan.get("as_of"), scan.get("generated_at"), scan["universe"]["scanned_stock"], scan["universe"]["scanned_etf"], {b: len(v) for b, v in scan["boards"].items()},
            {b: len([x for x in v if x.get("jump")]) for b, v in scan["boards"].items()}))
        E = expected(feeds)
        r.log("worked out here: %s" % json.dumps({(k if isinstance(k, str) else "%s/%s" % k): {a: b for a, b in v.items() if a not in ("tickers",)} for k, v in E.items()}))
        sync_playwright = ensure_playwright(r.log)
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL: " + msg)
            return bool(cond)

        def judge(tab, s, width, kind="stock"):
            tag = "%d/%s" % (width, tab)
            if tab in ("pump", "top"):
                ck(s["hidden"] is True, "%s: the sheet should step aside on a tab without one" % tag)
                return
            ck(s["state"] == "ready" and not s["hidden"] and s["bad"] == 0 and s["ox"] <= 1, "%s: state %s hidden %s, %d broken values, overflow %s" % (tag, s["state"], s["hidden"], s["bad"], s["ox"]))
            ck(s["eye"].lower().startswith(WORD[tab].lower() + " tearsheet"), "%s: eyebrow %r" % (tag, s["eye"][:60]))
            ck(len(s["tiles"]) == 6, "%s: %d tiles" % (tag, len(s["tiles"])))
            if tab in BOARD:
                e = E[(tab, kind)]
                ck(s["total"] == e["n"] and s["center"] == num(e["n"]), "%s/%s: ring %s / centre %s, the feed has %d" % (tag, kind, s["total"], s["center"], e["n"]))
                ck([x.split(" = ")[1] for x in s["legend"]] == [num(x) for x in e["parts"]], "%s/%s: ring parts %s, the feed has %s" % (tag, kind, s["legend"], e["parts"]))
                ck(s["title"].startswith(num(e["n"]) + " ") and num(e["scanned"]) in s["eye"], "%s/%s: headline %r" % (tag, kind, s["title"][:70]))
                largest = next((x for x in s["lists"] if x["k"] == "largest"), None)
                ck(largest and largest["first"].split(" · ")[0] == e["largest"], "%s/%s: the size list starts with %r, the feed's largest is %s" % (tag, kind, largest and largest["first"][:30], e["largest"]))
                ck(s["ctl"] == (["STOCKS*", "ETFS"] if kind == "stock" else ["STOCKS", "ETFS*"]), "%s/%s: controls %s" % (tag, kind, s["ctl"]))
            elif tab == "sc":
                ck(s["total"] == E["sc"]["n"] and num(E["sc"]["n"]) + " names" in s["title"], "%s: ring %s, the board has %d daily rows in climax or testing" % (tag, s["total"], E["sc"]["n"]))
                vol = next((x for x in s["lists"] if x["k"] == "volume"), None)
                ck(vol and vol["first"].split(" · ")[0] == E["sc"]["first_volume"], "%s: the volume list starts with %r, the board's heaviest is %s" % (tag, vol and vol["first"][:30], E["sc"]["first_volume"]))
            elif tab == "capit":
                ck(s["total"] == E["capit"]["n"] and [int(x.split(" = ")[1].replace(",", "")) for x in s["legend"]] == E["capit"]["parts"], "%s: ring %s parts %s, the feed has %s %s" % (tag, s["total"], s["legend"], E["capit"]["n"], E["capit"]["parts"]))
                ck(s["title"].startswith(num(E["capit"]["sum_5"]) + " capitulation prints"), "%s: headline %r, the feed's 5-session sum is %s" % (tag, s["title"][:60], E["capit"]["sum_5"]))
                lat = next((x for x in s["lists"] if x["k"] == "latest"), None)
                ck(lat and lat["first"].split(" · ")[0] == E["capit"]["first_latest"], "%s: the latest list starts with %r, the feed's highest score is %s" % (tag, lat and lat["first"][:30], E["capit"]["first_latest"]))
            elif tab == "bc":
                ck(s["total"] == E["bc"]["n"], "%s: ring %s, the book has %d names with a buying climax" % (tag, s["total"], E["bc"]["n"]))
            elif tab == "spread":
                ck(s["total"] == E["spread"]["n"] and ("%d in a squeeze" % E["spread"]["squeeze"]) in s["title"], "%s: ring %s title %r, the feed has %d coiled / %d squeezes" % (tag, s["total"], s["title"][:60], E["spread"]["n"], E["spread"]["squeeze"]))
            elif tab == "spring":
                f, g, x = E["spring"]["parts"]
                ck(s["total"] == E["spring"]["n"] and ("%d fired, %d forming, %d failed" % (f, g, x)) in s["title"], "%s: ring %s title %r, the book has %s" % (tag, s["total"], s["title"][:70], E["spring"]["parts"]))

        with sync_playwright() as p:
            browser = launch(p, r.log)
            # ---- 3. the Wyckoff desk, ten tabs, two widths
            for width in (1440, 390):
                r.section("wyckoff-desk.html at %d px" % width)
                ctx = browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=1 if width == 1440 else 2, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
                page = ctx.new_page()
                errs = []
                page.on("pageerror", lambda e: errs.append(str(e)[:140]))
                page.goto("%s/wyckoff-desk.html?tab=sc&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_function("() => (document.getElementById('jh-tearsheet') || { dataset: {} }).dataset.state === 'ready'", timeout=60000)
                page.wait_for_function("() => window.JHWyckoffDocs && window.JHWyckoffDocs.scan && window.JHWyckoffDocs.capit", timeout=60000)
                for tab in TABS:
                    page.evaluate("(t) => Array.from(document.querySelectorAll('#tabs button')).find(b => b.dataset.t === t).click()", tab)
                    page.wait_for_timeout(350)
                    s = page.evaluate(SHEET)
                    judge(tab, s, width)
                    pn = page.evaluate(PANEL)
                    if s["hidden"]:
                        r.log("%-7s the sheet steps aside (no sheet for this tab)" % tab)
                    else:
                        r.log("%-7s %s | %s | ring %s = %s | lists %s | %dx%d" % (tab, s["eye"][:52], s["title"][:110], s["center"], "; ".join(s["legend"])[:150], " ".join("%s:%d" % (x["k"], x["n"]) for x in s["lists"]), s["w"], s["h"]))
                    if tab in BOARD:
                        e = E[(tab, "stock")]
                        ok = ck(pn is not None and pn["rows"] == min(300, e["n"]) and pn["tickers"] == e["tickers"][:300] and pn["above"] is True, "%d/%s: the all-stock board draws %s rows (the feed has %d stocks), above the desk's table %s" % (
                            width, tab, pn and pn["rows"], e["n"], pn and pn["above"]))
                        ck(pn and ("STOCKS %s*" % num(e["n"])) in pn["kinds"] and ("ETFS %s" % num(E[(tab, "etf")]["n"])) in pn["kinds"], "%d/%s: board buttons %s" % (width, tab, pn and pn["kinds"]))
                        ck(pn and pn["ownHidden"] == (tab == "dist"), "%d/%s: the desk's own table hidden=%s" % (width, tab, pn and pn["ownHidden"]))
                        if ok:
                            i = [h.lower() for h in pn["heads"]].index("since")          # innerText carries the header's uppercase styling
                            page.evaluate("(i) => document.querySelector('#scan-panel th[data-sc-col=\"' + i + '\"]').click()", i)
                            page.wait_for_timeout(200)
                            top = page.evaluate(PANEL)["first"]
                            want = ["%s%.1f%%" % ("+" if v > 0 else "−" if round(v, 1) < 0 else "", abs(v)) for v in e["since_top"]]
                            ck([x[i] for x in top] == want, "%d/%s: sorted by Since, the board shows %s, the feed's largest are %s" % (width, tab, [x[i] for x in top], want))
                            page.evaluate("(i) => document.querySelector('#scan-panel th[data-sc-col=\"' + i + '\"]').click()", i)
                            page.wait_for_timeout(200)
                            low = page.evaluate(PANEL)["first"]
                            r.log("         board: %s | %s | sorted by Since: largest %s, then reversed %s" % (pn["h2"], pn["n"], [x[0] + " " + x[i] for x in top], [x[0] + " " + x[i] for x in low]))
                        # the sheet's ETFS button
                        page.evaluate("() => document.querySelector('#jh-tearsheet [data-jts-ctl=\"etf\"]').click()")
                        page.wait_for_timeout(250)
                        judge(tab, page.evaluate(SHEET), width, "etf")
                        page.evaluate("() => document.querySelector('#jh-tearsheet [data-jts-ctl=\"stock\"]').click()")
                        page.wait_for_timeout(200)
                    if (tab, width) in (("dist", 1440), ("bottom", 1440), ("sc", 1440)):
                        shots["desk_%s_%d" % (tab, width)] = page.locator("#jh-tearsheet").screenshot(type="jpeg", quality=50)
                    if (tab, width) in (("accum", 390), ("dist", 390)):
                        shots["desk_%s_%d" % (tab, width)] = page.screenshot(type="jpeg", quality=55)
                    if (tab, width) == ("bottom", 1440):
                        shots["board_bottom_1440"] = page.locator("#scan-panel").screenshot(type="jpeg", quality=50)
                # filter and SHOW ALL on the largest board
                page.evaluate("() => Array.from(document.querySelectorAll('#tabs button')).find(b => b.dataset.t === 'bottom').click()")
                page.wait_for_timeout(300)
                page.fill("#scan-q", "technology")
                page.wait_for_timeout(300)
                f = page.evaluate(PANEL)
                tech = len([x for x in feeds["scan"]["boards"]["bottom"] if x["kind"] == "stock" and "technology" in (" ".join(str(x.get(k) or "") for k in ("ticker", "name", "sector", "industry", "status", "mark"))).lower()])
                ck(f["n"].startswith(num(tech) + " shown"), "%d: filter 'technology' shows %r, the feed has %d" % (width, f["n"], tech))
                page.fill("#scan-q", "")
                page.wait_for_timeout(200)
                page.evaluate("() => document.querySelector('#scan-panel [data-sc-all]').click()")
                page.wait_for_timeout(600)
                allrows = page.evaluate("() => document.querySelectorAll('#scan-panel tbody tr[data-t]').length")
                ck(allrows == E[("bottom", "stock")]["n"], "%d: SHOW ALL draws %d rows, the feed has %d" % (width, allrows, E[("bottom", "stock")]["n"]))
                r.log("filter 'technology': %s; SHOW ALL: %d rows; page errors %s" % (f["n"], allrows, errs[:3] or "none"))
                ck(not errs, "%d: page errors %s" % (width, errs[:3]))
                ctx.close()

            # ---- 4. the capitulation desk
            r.section("capitulation-desk.html")
            for width in (1440, 390):
                ctx = browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=1 if width == 1440 else 2, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
                page = ctx.new_page()
                page.goto("%s/capitulation-desk.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_function("() => (document.getElementById('jh-tearsheet') || { dataset: {} }).dataset.state === 'ready'", timeout=60000)
                page.wait_for_timeout(500)
                s = page.evaluate(SHEET)
                judge("capit", s, width)
                desk = page.evaluate("() => ({ rows: document.querySelectorAll('#tbl tbody tr').length, read: (document.getElementById('read') || {}).textContent || '' })")
                ck(desk["rows"] > 0, "%d: the capitulation desk under the sheet has %d rows" % (width, desk["rows"]))
                r.log("%d px: %s | ring %s | the desk under it: %d rows | %dx%d overflow %s" % (width, s["title"][:110], s["center"], desk["rows"], s["w"], s["h"], s["ox"]))
                if width == 1440:
                    shots["capitulation_1440"] = page.locator("#jh-tearsheet").screenshot(type="jpeg", quality=50)
                ctx.close()

            # ---- 5. the chart: every icon
            for width in (1440, 390):
                r.section("chart.html at %d px: the eight icons" % width)
                ctx = browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=1 if width == 1440 else 2, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
                page = ctx.new_page()
                asked = []
                page.on("request", lambda q: asked.append(q.url) if ("ohlc" in q.url or "symbol=" in q.url or "/bars" in q.url) else None)
                page.goto("%s/chart.html?s=NVDA&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_selector("#btn-dist", timeout=60000, state="attached")
                page.wait_for_function("() => (window.lastBars || []).length > 300", timeout=60000)
                page.wait_for_timeout(1200)
                st = page.evaluate(CHART)
                ck(st["wrapped"] == [True, True, "function"] and st["lit"] == [] and st["overlay"] == "", "%d: chart start state %s" % (width, st))
                r.log("start: %d bars, marks %s / dist %s, wrapped %s" % (st["bars"], st["camp"], st["dist"], st["wrapped"]))

                def click(i):
                    page.evaluate("(id) => document.getElementById(id).click()", i)
                    page.wait_for_timeout(450)

                def frame(part):
                    for _ in range(60):
                        fr = next((f for f in page.frames if part in f.url), None)
                        if fr:
                            return fr
                        page.wait_for_timeout(250)
                    return None

                for btn, part, tab in ICONS:
                    click(btn)
                    fr = frame(part)
                    if not ck(fr is not None, "%d/%s: no workspace frame for %s" % (width, btn, part)):
                        continue
                    fr.wait_for_function("() => (document.getElementById('jh-tearsheet') || { dataset: {} }).dataset.state === 'ready'", timeout=60000)
                    s = fr.evaluate(SHEET)
                    st = page.evaluate(CHART)
                    judge(tab, s, width)
                    r.log("%-9s -> %s | %s | sheet: %s | ring %s" % (btn, st["head"], st["src"], s["title"][:80], s["center"]))
                    click("wsx")
                    ck(page.evaluate(CHART)["overlay"] == "", "%d/%s: the overlay did not close" % (width, btn))
                for btn, which, part in MARKS:
                    click(btn)
                    st = page.evaluate(CHART)
                    on = st["dist"] == 1 if which == "dist" else st["camp"].get(which) == 1
                    fr = frame(part)
                    if not ck(on and btn in st["lit"] and st["overlay"] == "on" and fr is not None and part in (st["src"] or ""), "%d/%s: click 1 should light the icon, switch the marks on and open %s: %s" % (width, btn, part, st)):
                        continue
                    fr.wait_for_function("() => (document.getElementById('jh-tearsheet') || { dataset: {} }).dataset.state === 'ready' && document.querySelectorAll('#scan-panel tbody tr[data-t]').length > 0", timeout=60000)
                    s, pn, drawn = fr.evaluate(SHEET), fr.evaluate(PANEL), page.evaluate(DRAWN)
                    judge(which, s, width)
                    ck(pn["rows"] == min(300, E[(which, "stock")]["n"]), "%d/%s: the board in the overlay has %d rows" % (width, btn, pn["rows"]))
                    r.log("%-9s click 1 -> lit %s, marks %s / dist %s | %s | %s | sheet: %s | board %s | marks on NVDA behind it: %s %s" % (
                        btn, st["lit"], st["camp"], st["dist"], st["head"], st["src"], s["title"][:70], pn["n"], drawn["campLast"], drawn["distLast"]))
                    if width == 1440 and which == "accum":
                        shots["chart_overlay_accum_1440"] = page.screenshot(type="jpeg", quality=52)
                    if width == 390 and which == "dist":
                        shots["chart_overlay_dist_390"] = page.screenshot(type="jpeg", quality=55)
                    if which == "dist":
                        # a row of the board: the symbol loads behind the overlay, the marks stay on
                        n0 = len(asked)
                        t = fr.evaluate("() => { const tr = document.querySelector('#scan-panel tbody tr[data-t]'); tr.click(); return tr.getAttribute('data-t'); }")
                        page.wait_for_timeout(3500)
                        st2 = page.evaluate(CHART)
                        loaded = [u for u in asked[n0:] if t.upper() in u.upper()]
                        ck(st2["overlay"] == "" and st2["dist"] == 1 and "btn-dist" in st2["lit"] and loaded, "%d: row %s clicked - overlay %r, dist %s, lit %s, requests for it %d" % (width, t, st2["overlay"], st2["dist"], st2["lit"], len(loaded)))
                        d2 = page.evaluate(DRAWN)
                        r.log("          row %s clicked -> overlay closed %s, the chart asked for it (%d requests, e.g. %s), DIST marks on it: %s" % (t, st2["overlay"] == "", len(loaded), (loaded[:1] or [""])[0][:110], d2["distLast"]))
                        if width == 1440:
                            shots["chart_marks_after_row_1440"] = page.screenshot(type="jpeg", quality=52)
                    else:
                        click("wsx")
                    click(btn)
                    st3 = page.evaluate(CHART)
                    off = st3["dist"] == 0 if which == "dist" else st3["camp"].get(which) == 0
                    ck(off and btn not in st3["lit"] and st3["overlay"] == "", "%d/%s: click 2 should switch the marks off and open nothing: %s" % (width, btn, st3))
                    r.log("          click 2 -> lit %s, marks %s / dist %s, overlay %r" % (st3["lit"], st3["camp"], st3["dist"], st3["overlay"]))
                ctx.close()
            # a link with ?marks=
            r.section("chart.html?s=PFE&marks=bottom,dist")
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            page.goto("%s/chart.html?s=PFE&marks=bottom,dist&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-dist", timeout=60000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300", timeout=60000)
            page.wait_for_timeout(1500)
            st, drawn = page.evaluate(CHART), page.evaluate(DRAWN)
            ck(st["camp"].get("bottom") == 1 and st["camp"].get("accum") == 0 and st["dist"] == 1 and st["lit"] == ["btn-bot", "btn-dist"] and st["overlay"] == "", "?marks=bottom,dist: %s" % st)
            ck(drawn["camp"] > 0, "?marks=: no CLIMAX / BOTTOM mark computed on PFE's %d bars" % drawn["bars"])
            r.log("arrives with lit %s, marks %s / dist %s, no desk opened; on PFE's %d bars the chart draws %d bottom-layer marks (last %s) and %d distribution marks (last %s)" % (
                st["lit"], st["camp"], st["dist"], drawn["bars"], drawn["camp"], drawn["campLast"], drawn["dist"], drawn["distLast"]))
            shots["chart_link_marks_pfe"] = page.screenshot(type="jpeg", quality=52)
            ctx.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(failures=str(len(fails)), scan_version=str(scan.get("version")), as_of=str(scan.get("as_of")), accumulation=str(len(scan["boards"]["accumulation"])), bottom=str(len(scan["boards"]["bottom"])), distribution=str(len(scan["boards"]["distribution"])))
        if fails:
            r.fail("LIVE PROOF: %d failed checks - %s" % (len(fails), "; ".join(fails[:6])))
            raise SystemExit(1)
        r.ok("LIVE PROOF: eight sheets and three all-stock boards match the live feeds at 1440 and 390 px; every icon on chart.html does what it says")


if __name__ == "__main__":
    main()
    sys.exit(0)
