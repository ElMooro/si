"""ops 6960 - the Credit button on chart.html: a real browser on the live site (read-only).

Khalid 2026-11-25, after Credit Before Equity 3.0.0 ran on its own: "now make sure this have an icon on chart.html".
The chart's bar had Bonds and CDS but no button for the credit board. It has one now, between the two.

Nothing is invoked and nothing is written. In a real browser on justhodl.ai:

  1. chart.html: the button is there once, between Bonds and CDS, and every other desk button is where it was.
  2. The pointer goes to the bar and clicks it: the board opens in the chart's workspace, with its own page behind
     "Open page", one tile an industry and the document's company count.
  3. A company's sheet inside the workspace: its Chart link loads that company in the chart behind and closes the workspace.
  4. The neighbours (Bonds, CDS, Repo, Sovereign, Risk Map) still open their own desks.
  5. The board by itself (not under the chart): the same link is an ordinary link to the chart.
  6. A phone: the button is on the bar, a tap opens the board, nothing scrolls sideways.

Safe to run again.
"""
from pathlib import Path
import base64
import importlib.util
import json
import re
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops")]
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
NAME = "ops_6960_credit_button_on_chart_live_proof"
BAR = ["btn-macro", "btn-flows", "btn-heat", "btn-rmap", "btn-auc", "btn-corm", "btn-etf", "btn-str", "btn-bnd", "btn-cbe", "btn-cds", "btn-repo", "btn-sov", "btn-usd", "btn-soma", "btn-bofa", "btn-13f", "btn-ldr"]
TITLE = "Credit Before Equity · company bond spreads, CDS, stress and the fall risk they carry"
HEAD = "Credit Before Equity · company bond spreads, CDS, stress, fall risk"
NEIGHBOURS = (("btn-bnd", "/bonds.html"), ("btn-cds", "/cds.html"), ("btn-repo", "/repo.html"), ("btn-sov", "/sovereign-desk.html"), ("btn-rmap", "/regime-map.html"))
CHART = r"""() => { const b = (id) => document.getElementById(id), btn = b('btn-cbe'), ov = b('ws-overlay'), f = ov ? ov.querySelector('iframe') : null, r = btn ? btn.getBoundingClientRect() : null;
  return { btn: !!btn, n: document.querySelectorAll('#btn-cbe').length, prev: btn && btn.previousElementSibling ? btn.previousElementSibling.id : null, next: btn && btn.nextElementSibling ? btn.nextElementSibling.id : null,
    glyph: btn ? btn.querySelector('.g').textContent : null, label: btn ? btn.querySelector('.l').textContent : null, title: btn ? btn.title : null,
    box: r ? [Math.round(r.left), Math.round(r.top), Math.round(r.width), Math.round(r.height)] : null, view: [innerWidth, innerHeight],
    overlay: ov ? ov.className : null, src: f ? f.getAttribute('src') : null, head: ov && ov.querySelector('.wshd span') ? ov.querySelector('.wshd span').textContent : null,
    open: ov && ov.querySelector('.wshd a') ? ov.querySelector('.wshd a').getAttribute('href') : null,
    desks: Array.from(document.querySelectorAll('#tfbar .wsdesk')).map(x => x.id), on: (document.querySelector('#tabs .tab.on') || {dataset: {}}).dataset.id || null,
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
BOARD = r"""() => { const map = document.getElementById('map'), hero = document.getElementById('hero');
  return { tiles: document.querySelectorAll('#map .cb-box').length, hero: hero ? hero.innerText.replace(/\s+/g, ' ').slice(0, 400) : null, embedded: window.top !== window,
    bad: (((map ? map.innerText : '') + ' ' + (hero ? hero.innerText : '')).match(/\bNaN\b|\bundefined\b|\bnull\b|Infinity(?! [A-Z])/g) || []),
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


def fetch(path, timeout=90):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def on_screen(c):
    """The button's whole box is inside the window."""
    b, v = c.get("box"), c.get("view")
    return bool(b and v and b[2] > 0 and b[3] > 0 and b[0] >= 0 and b[1] >= 0 and b[0] + b[2] <= v[0] and b[1] + b[3] <= v[1])


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp = int(time.time())
    shots, fails = {}, []

    with report(NAME) as r:
        r.heading("ops 6960 - the Credit button on chart.html, a real browser on the live site (read-only)")

        def ck(cond, msg):
            if cond:
                return True
            fails.append(msg)
            r.warn("CHECK FAILED: " + msg)
            return False

        # ------------------------------------------------------------------ 0. served
        r.section("0. The live site serves the new files")
        served = {}
        for attempt in range(60):
            try:
                page_html = fetch("/credit-before-equity.html?probe=%d" % (stamp + attempt))
                tag = re.search(rb'<script src="(/jh-credit-board\.js\?v=[0-9a-f]{8})"', page_html)       # the build stamps each script with its content hash
                tag2 = re.search(rb'<script src="(/jh-credit-sheet\.js\?v=[0-9a-f]{8})"', page_html)
                served = {"engine": b"id=btn-cbe " in fetch("/jh-chart-engine.js?probe=%d" % (stamp + attempt)),
                          "rail": b'credit: ["Credit Before Equity' in fetch("/jh-chart-tvrail.js?probe=%d" % (stamp + attempt)),
                          "chart": fetch("/chart.html?probe=%d" % (stamp + attempt)).count(b"&cbe=20261125") == 2,
                          "board": bool(tag) and b"chartHandoff" in fetch(tag.group(1).decode() + "&probe=%d" % (stamp + attempt)),
                          "sheet": bool(tag2) and b'class="cb-sheet-a" data-t="' in fetch(tag2.group(1).decode() + "&probe=%d" % (stamp + attempt))}
            except Exception as e:  # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served and all(v is True for v in served.values()):
                break
            time.sleep(20)
        r.log("served after %d tries: %s" % (attempt + 1, json.dumps(served)))
        if not (served and all(v is True for v in served.values())):
            r.fail("the live site does not serve the Credit button yet; nothing was judged")
            sys.exit(1)

        doc = json.loads(fetch("/data/credit-before-equity.json?probe=%d" % stamp))
        cols = doc["issuers"]["cols"]
        rows = [dict(zip(cols, x)) for x in doc["issuers"]["rows"]]
        listed = [x for x in rows if x.get("ticker")]
        n_ind = len(doc["industries"]["all"])
        pick = next((x for x in listed if x["ticker"] == "ORCL"), None) or max(listed, key=lambda x: x.get("value") or 0)
        sym = pick["ticker"]
        r.log("document %s generated %s: %d companies (%d listed), %d industries | the company opened here: %s %s" % (
            doc.get("version"), doc.get("generated_at"), len(rows), len(listed), n_ind, sym, pick.get("name")))

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)

            # ------------------------------------------------------------------ 1. the button
            r.section("1. The button on the chart's bar")
            ctx = browser.new_context(viewport={"width": 1600, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?s=SPY&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-cbe", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function' && typeof window.jhOpenSymbol === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            c0 = page.evaluate(CHART)
            r.log("button: %s" % json.dumps({k: c0[k] for k in ("btn", "n", "prev", "next", "glyph", "label", "title", "overlay", "on")}))
            r.log("desk buttons on the bar (%d): %s" % (len(c0["desks"]), " ".join(c0["desks"])))
            ck(c0["btn"] and c0["n"] == 1 and c0["prev"] == "btn-bnd" and c0["next"] == "btn-cds" and c0["glyph"] == "Cr" and c0["label"] == "Credit" and c0["title"] == TITLE,
               "the button: %s" % {k: c0[k] for k in ("btn", "n", "prev", "next", "glyph", "label", "title")})
            ck(c0["desks"][:len(BAR)] == BAR and len(set(c0["desks"])) == len(c0["desks"]), "the bar's desks moved: %s" % c0["desks"][:len(BAR)])
            ck(c0["overlay"] == "" and c0["on"] == "SPY", "the chart did not open on SPY with the workspace closed: %s %s" % (c0["overlay"], c0["on"]))

            # ------------------------------------------------------------------ 2. the click
            r.section("2. The pointer clicks it: the board opens in the chart's workspace")
            how = None
            for _ in range(3):
                page.mouse.move(700, 3)                     # the toolbar hides until the pointer reaches the top edge
                try:
                    page.wait_for_selector("#jh-ah-top.show", timeout=6000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(500)
                page.evaluate("() => document.getElementById('btn-cbe').scrollIntoView({ inline: 'center', block: 'nearest' })")
                page.wait_for_timeout(300)
                seen = page.evaluate(CHART)
                if not on_screen(seen):
                    continue
                shots["1-the-bar"] = page.screenshot(type="jpeg", quality=80, clip={"x": max(0, seen["box"][0] - 420), "y": max(0, seen["box"][1] - 14), "width": 900, "height": seen["box"][3] + 30})
                try:
                    page.mouse.move(seen["box"][0] + seen["box"][2] / 2, seen["box"][1] + seen["box"][3] / 2)
                    page.click("#btn-cbe", timeout=6000)
                    how = "a real click at %s" % seen["box"][:2]
                    break
                except Exception:  # noqa: BLE001
                    continue
            if not how:
                page.evaluate("() => document.getElementById('btn-cbe').click()")
            ck(bool(how), "the pointer could not reach the button on the bar (it was pressed by script instead)")
            page.wait_for_selector("#ws-overlay.on iframe", timeout=15000)
            frame = None
            for _ in range(60):
                frame = next((f for f in page.frames if "/credit-before-equity.html" in f.url), None)
                if frame:
                    break
                page.wait_for_timeout(250)
            ck(frame is not None, "the workspace holds no credit board frame")
            frame.locator("#map .cb-box").first.wait_for(timeout=90000)
            page.wait_for_timeout(1500)
            c1 = page.evaluate(CHART)
            b1 = frame.evaluate(BOARD)
            r.log("opened by %s: %s" % (how or "script", json.dumps({k: c1[k] for k in ("overlay", "src", "head", "open")})))
            r.log("inside: %d tiles | %s | odd figures %s" % (b1["tiles"], b1["hero"][:160], b1["bad"]))
            ck(c1["overlay"] == "on" and c1["src"] == "/credit-before-equity.html" and c1["head"] == HEAD and c1["open"] == "/credit-before-equity.html", "the workspace: %s" % {k: c1[k] for k in ("overlay", "src", "head", "open")})
            ck(b1["embedded"] and b1["tiles"] == n_ind and "{:,}".format(len(rows)) in (b1["hero"] or "") and not b1["bad"],
               "the board inside the chart: %d tiles for %d industries, %s companies in the document, odd figures %s" % (b1["tiles"], n_ind, "{:,}".format(len(rows)), b1["bad"]))
            ck(c1["on"] == "SPY", "the chart behind changed symbol on opening: %s" % c1["on"])
            shots["2-the-board-in-the-chart"] = page.screenshot(type="jpeg", quality=72)

            # ------------------------------------------------------------------ 3. a company's Chart link
            r.section("3. A company's sheet inside the workspace: Chart loads it in the chart behind")
            frame.locator("input[data-q]").first.fill(sym)
            opener = frame.locator("button[data-sheet='T:%s']" % sym).first
            opener.wait_for(timeout=20000)
            opener.scroll_into_view_if_needed()
            opener.click()
            link = frame.locator("#sheet a.cb-sheet-a")
            link.wait_for(timeout=20000)
            attrs = link.evaluate("a => [a.getAttribute('data-t'), a.getAttribute('href'), a.textContent]")
            page.wait_for_timeout(900)
            shots["3-a-sheet-in-the-chart"] = page.screenshot(type="jpeg", quality=72)
            ck(attrs == [sym, "/chart.html?symbol=%s" % sym, "Chart"], "the sheet's Chart link: %s" % attrs)
            link.click()
            try:
                page.wait_for_function("(s) => { const t = document.querySelector('#tabs .tab.on'); return t && t.dataset.id === s && document.getElementById('ws-overlay').className === ''; }", arg=sym, timeout=30000)
                page.wait_for_function("() => (window.lastBars || []).length > 100", timeout=60000)
            except Exception as e:  # noqa: BLE001
                r.log("after Chart: %s" % str(e)[:120])
            page.wait_for_timeout(1500)
            c2 = page.evaluate(CHART)
            bars = page.evaluate("() => (window.lastBars || []).length")
            still = "/chart.html" in page.url and "credit-before-equity" not in page.url
            r.log("Chart on %s's sheet: workspace %r, the chart is on %s with %d bars, the address is still the chart's: %s" % (sym, c2["overlay"], c2["on"], bars, still))
            ck(c2["overlay"] == "" and c2["on"] == sym and bars > 100 and still, "the Chart link did not load %s in the chart behind: %s" % (sym, {k: c2[k] for k in ("overlay", "on")}))
            shots["4-the-chart-after"] = page.screenshot(type="jpeg", quality=70)
            r.log("page errors on the chart and in the board, to here: %d %s" % (len(errors), errors[:3]))
            ck(not errors, "chart.html or the board inside it threw: %s" % errors[:3])
            n_before = len(errors)

            # ------------------------------------------------------------------ 4. the neighbours
            r.section("4. The neighbours still open their own desks")
            got = {}
            for bid, want in NEIGHBOURS:
                page.evaluate("(i) => document.getElementById(i).click()", bid)
                try:
                    page.wait_for_selector("#ws-overlay.on iframe", timeout=10000)
                    got[bid] = page.evaluate(CHART)["src"]
                except Exception:  # noqa: BLE001
                    got[bid] = None
                page.evaluate("() => { const x = document.getElementById('wsx'); if (x) x.click(); }")
                page.wait_for_timeout(250)
            r.log("pressed in turn: %s" % json.dumps(got))
            ck(all((got[bid] or "").split("?")[0] == want for bid, want in NEIGHBOURS), "a neighbour opens another page: %s" % got)
            r.log("thrown while the five neighbours were open (their own pages; listed, not judged here): %d %s" % (len(errors) - n_before, errors[n_before:n_before + 3]))
            ctx.close()

            # ------------------------------------------------------------------ 5. the board by itself
            r.section("5. The board by itself: the same link is an ordinary link")
            ctx = browser.new_context(viewport={"width": 1500, "height": 950}, user_agent=UA)
            page = ctx.new_page()
            errors2 = []
            page.on("pageerror", lambda e: errors2.append(str(e)[:200]))
            page.goto("%s/credit-before-equity.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.locator("#map .cb-box").first.wait_for(timeout=90000)
            page.locator("input[data-q]").first.fill(sym)
            alone = page.locator("button[data-sheet='T:%s']" % sym).first
            alone.wait_for(timeout=20000)
            alone.scroll_into_view_if_needed()
            alone.click()
            page.locator("#sheet a.cb-sheet-a").wait_for(timeout=20000)
            b5 = page.evaluate(BOARD)
            page.locator("#sheet a.cb-sheet-a").click()
            went = None
            try:
                page.wait_for_url(re.compile(r"/chart\.html"), timeout=30000)
                page.wait_for_function("(s) => { const t = document.querySelector('#tabs .tab.on'); return !!t && t.dataset.id === s; }", arg=sym, timeout=60000)
                went = page.evaluate("() => (document.querySelector('#tabs .tab.on') || {dataset: {}}).dataset.id || null")
            except Exception as e:  # noqa: BLE001
                r.log("after Chart: %s" % str(e)[:120])
            r.log("not under the chart (%s): Chart went to %s, which is on %s | page errors on the board %d" % (not b5["embedded"], page.url[:90], went, len(errors2)))
            ck(not b5["embedded"] and "/chart.html" in page.url and "credit-before-equity" not in page.url and went == sym, "by itself the Chart link went to %s (on %s)" % (page.url[:120], went))
            ctx.close()

            # ------------------------------------------------------------------ 6. a phone
            r.section("6. A phone")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, is_mobile=True, has_touch=True, device_scale_factor=2)
            page = ctx.new_page()
            errors3 = []
            page.on("pageerror", lambda e: errors3.append(str(e)[:200]))
            page.goto("%s/chart.html?s=SPY&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-cbe", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            page.evaluate("() => document.getElementById('btn-cbe').scrollIntoView({ inline: 'center', block: 'center' })")
            page.wait_for_timeout(500)
            m0 = page.evaluate(CHART)
            shots["5-phone-bar"] = page.screenshot(type="jpeg", quality=70)
            try:
                page.tap("#btn-cbe", timeout=6000)
                tapped = "a real tap at %s" % m0["box"][:2]
            except Exception:  # noqa: BLE001
                page.evaluate("() => document.getElementById('btn-cbe').click()")
                tapped = None
            page.wait_for_selector("#ws-overlay.on iframe", timeout=15000)
            mframe = None
            for _ in range(60):
                mframe = next((f for f in page.frames if "/credit-before-equity.html" in f.url), None)
                if mframe:
                    break
                page.wait_for_timeout(250)
            mframe.locator("#map .cb-box").first.wait_for(timeout=90000)
            page.wait_for_timeout(1500)
            m1 = page.evaluate(CHART)
            mb = mframe.evaluate(BOARD)
            r.log("phone: button between %s and %s at %s, on screen %s, %s; workspace %r on %s; %d tiles; sideways scroll %d px on the chart, %d px in the board; page errors %d" % (
                m0["prev"], m0["next"], m0["box"], on_screen(m0), tapped or "pressed by script", m1["overlay"], m1["src"], mb["tiles"], m1["ox"], mb["ox"], len(errors3)))
            ck(m0["prev"] == "btn-bnd" and m0["next"] == "btn-cds" and on_screen(m0) and bool(tapped), "the phone's button: %s" % {k: m0[k] for k in ("prev", "next", "box", "view")})
            ck(m1["overlay"] == "on" and m1["src"] == "/credit-before-equity.html" and mb["tiles"] == n_ind and m0["ox"] <= 0 and m1["ox"] <= 0, "the phone's workspace: %s, %d tiles, sideways %s / %s" % (m1["src"], mb["tiles"], m0["ox"], m1["ox"]))
            ck(not errors3, "chart.html threw on the phone: %s" % errors3[:3])
            shots["6-phone-board"] = page.screenshot(type="jpeg", quality=70)
            ctx.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(button="btn-cbe", between="Bonds | Credit | CDS", company=sym, industries=n_ind, companies=len(rows), failed_checks=len(fails), document=str(doc.get("generated_at"))[:16])
        for f in fails[:40]:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("LIVE: chart.html has a Credit button between Bonds and CDS; a real click opens Credit Before Equity %s in the chart's workspace (%d industries, %s companies); "
             "%s's sheet sends it to the chart behind and closes the workspace; the neighbours, the page by itself and a phone all behave" % (doc.get("version"), n_ind, "{:,}".format(len(rows)), sym))


if __name__ == "__main__":
    main()
