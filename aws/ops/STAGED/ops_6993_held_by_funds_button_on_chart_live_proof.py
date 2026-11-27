"""ops 6993 - the Held by funds button on chart.html and the section's headings: a real browser on the live site (read-only).

Khalid 2026-11-26 22:43 ET: "fix it also: Held by funds to have its own button on the chart bar that opens straight to that
section". "It" was a loose section badge above the list of the section.

Nothing is invoked and nothing is written. In a real browser on justhodl.ai:

  0. the live site serves the new chart engine, rail and chart.html, and the page with its three headed blocks
  1. the button is on the chart's bar once, right after Credit and before CDS, under his name for it; Credit is where it was
  2. the pointer clicks it: Credit Before Equity opens in the chart's workspace on the section Held by funds - the section's
     heading is at the top of the frame and stays there, with the document's figures in its tiles
  3. the site's section numbers sit in headings: the section has no loose badge, and each of its three blocks carries its
     number in its own heading; the fund document arrives and the lists are there
  4. the Credit button still opens the board at its top
  5. the page alone: the address with #s-funds lands on the section, on a desk screen and on a phone; without it the page
     opens at its top

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
NAME = "ops_6993_held_by_funds_button_on_chart_live_proof"
TITLE = "Held by funds · who holds each bond, how far apart funds mark it, and the swaps funds hold"
HEAD = "Held by funds · who holds each bond, fund marks, fund swaps"
HREF = "/credit-before-equity.html#s-funds"
BLOCKS = [["funds-file", "The fund file at a glance"], ["funds-pictures", "In pictures"], ["funds-lists", "The lists, each one whole"]]
CHART = r"""(id) => { const b = (x) => document.getElementById(x), btn = b(id), ov = b('ws-overlay'), f = ov ? ov.querySelector('iframe') : null, r = btn ? btn.getBoundingClientRect() : null;
  return { btn: !!btn, n: document.querySelectorAll('#' + id).length, prev: btn && btn.previousElementSibling ? btn.previousElementSibling.id : null, next: btn && btn.nextElementSibling ? btn.nextElementSibling.id : null,
    glyph: btn ? btn.querySelector('.g').textContent : null, label: btn ? btn.querySelector('.l').textContent : null, title: btn ? btn.title : null,
    box: r ? [Math.round(r.left), Math.round(r.top), Math.round(r.width), Math.round(r.height)] : null, view: [innerWidth, innerHeight],
    overlay: ov ? ov.className : null, src: f ? f.getAttribute('src') : null, head: ov && ov.querySelector('.wshd span') ? ov.querySelector('.wshd span').textContent : null,
    open: ov && ov.querySelector('.wshd a') ? ov.querySelector('.wshd a').getAttribute('href') : null,
    desks: Array.from(document.querySelectorAll('#tfbar .wsdesk')).map(x => x.id), ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
SECTION = r"""() => { const s = document.getElementById('s-funds'), h = s ? s.querySelector('h2.sec') : null, top = document.getElementById('s-glance');
  const badges = s ? Array.from(s.querySelectorAll('.jh-secbadge')).map(b => ({ t: b.textContent, float: b.classList.contains('float'), in: b.parentElement.tagName, block: (b.closest('.cb-fu-block') || {}).id || null })) : [];
  const blocks = s ? Array.from(s.querySelectorAll('#funds > .cb-fu-block')).map(d => [d.id, d.firstElementChild && d.firstElementChild.tagName === 'H3' ? d.firstElementChild.textContent.replace(/^§[\d.]+/, '').trim() : null]) : [];
  return { there: !!s, top: h ? Math.round(h.getBoundingClientRect().top) : null, y: Math.round(window.scrollY), vh: window.innerHeight, hash: location.hash, embedded: window.top !== window,
    tiles: Array.from(document.querySelectorAll('#funds .cb-tile-h')).map(e => [e.querySelector('.cb-lbl').innerText.trim(), e.querySelector('.cb-big').innerText.trim()]),
    badges: badges, blocks: blocks, loose_on_page: document.querySelectorAll('#s-funds .jh-secbadge.float').length, sections_script: !!document.querySelector('script[src*="jh-sections.js"]'),
    first: top ? Math.round(top.getBoundingClientRect().top) : null, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    bad: ((document.getElementById('funds') || { innerText: '' }).innerText.match(/\bNaN\b|\bundefined\b|\[object/g) || []).length }; }"""


def fetch(path, timeout=90):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def on_screen(c):
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
        r.heading("ops 6993 - the Held by funds button on chart.html, and the section's headings: a real browser on the live site (read-only)")

        def ck(ok, text):
            (r.ok if ok else r.fail)(text)
            if not ok:
                fails.append(text)
            return ok

        # ------------------------------------------------------------------ 0. served
        r.section("0. The live site serves the new files")
        served = {}
        for attempt in range(60):
            try:
                page_html = fetch("/credit-before-equity.html?probe=%d" % (stamp + attempt))
                tag = re.search(rb'<script src="(/jh-credit-funds\.js\?v=[0-9A-Za-z._-]+)"', page_html)        # the build stamps each script with its content hash
                tag2 = re.search(rb'<script src="(/jh-credit-board\.js\?v=[0-9A-Za-z._-]+)"', page_html)
                served = {"engine": b"id=btn-hbf " in fetch("/jh-chart-engine.js?probe=%d" % (stamp + attempt)),
                          "rail": b'heldby: ["Held by funds' in fetch("/jh-chart-tvrail.js?probe=%d" % (stamp + attempt)),
                          "chart": fetch("/chart.html?probe=%d" % (stamp + attempt)).count(b"&hbf=20261127") == 2,
                          "funds script": bool(tag) and b'id="funds-lists"' in fetch(tag.group(1).decode() + "&probe=%d" % (stamp + attempt)),
                          "board script": bool(tag2) and b"sectionOf" in fetch(tag2.group(1).decode() + "&probe=%d" % (stamp + attempt))}
            except Exception as e:  # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served and all(v is True for v in served.values()):
                break
            time.sleep(20)
        r.log("served after %d tries: %s" % (attempt + 1, json.dumps(served)))
        if not (served and all(v is True for v in served.values())):
            r.fail("the live site does not serve the Held by funds button yet; nothing was judged")
            sys.exit(1)
        doc = json.loads(fetch("/data/credit-before-equity.json?probe=%d" % stamp))
        block = doc.get("funds") or {}
        tied = block.get("tied") or {}
        want_tiles = [format(tied.get("securities") or 0, ","), format(tied.get("companies") or 0, ",")]
        r.log("the published document: generated %s, fund layer %s, %s securities tied to %s companies" % (doc.get("generated_at"), block.get("version"), want_tiles[0], want_tiles[1]))

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)

            def read(frame_or_page, tries=5, wait=None):
                """The section, read once the frame that holds it has settled (the workspace may load its page twice)."""
                for _ in range(tries):
                    try:
                        return frame_or_page.evaluate(SECTION)
                    except Exception:  # noqa: BLE001
                        (wait or frame_or_page).wait_for_timeout(1200)
                return frame_or_page.evaluate(SECTION)

            def judge_section(s, where, landed):
                ck(s["there"] and [t[1] for t in s["tiles"][:2]] == want_tiles and len(s["tiles"]) == 5 and s["bad"] == 0, "%s: the section is there with the document's figures (%s)" % (where, [t[1] for t in s["tiles"]]))
                if landed:
                    ck(s["top"] is not None and -4 <= s["top"] <= 60 and s["y"] > 2000, "%s: it lands on the section - the heading stands %s px from the top of the window after scrolling %s px" % (where, s["top"], s["y"]))
                else:
                    ck(s["y"] == 0 and (s["top"] or 0) > s["vh"], "%s: it opens at the top of the board (scrolled %s px; the section is %s px down)" % (where, s["y"], s["top"]))
                ck(s["blocks"] == BLOCKS, "%s: the section is three blocks, each under its own heading: %s" % (where, s["blocks"]))
                if s["sections_script"]:
                    subs = [b for b in s["badges"] if b["block"]]
                    ck(s["loose_on_page"] == 0 and not [b for b in s["badges"] if b["float"]], "%s: no section number hangs loose in the section (%d numbers, %d loose)" % (where, len(s["badges"]), s["loose_on_page"]))
                    ck(len(s["badges"]) == 4 and s["badges"][0]["in"] == "H2" and [b["block"] for b in subs] == [x[0] for x in BLOCKS] and all(b["in"] == "H3" for b in subs),
                       "%s: the section's number is in its heading and each block's in its own: %s" % (where, [(b["t"], b["in"], b["block"]) for b in s["badges"]]))
                else:
                    ck(False, "%s: the site's section script is not on the page, so its numbers could not be judged" % where)

            # ------------------------------------------------------------------ 1. the button
            r.section("1. The button on the chart's bar")
            ctx = browser.new_context(viewport={"width": 1600, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?s=SPY&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-hbf", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            c0, k0 = page.evaluate(CHART, "btn-hbf"), page.evaluate(CHART, "btn-cbe")
            r.log("button: %s" % json.dumps({k: c0[k] for k in ("btn", "n", "prev", "next", "glyph", "label", "title", "overlay")}))
            at = c0["desks"].index("btn-bnd") if "btn-bnd" in c0["desks"] else -1
            r.log("the bar around it: %s (of %d desk buttons)" % (" ".join(c0["desks"][max(0, at - 1):at + 5]), len(c0["desks"])))
            ck(c0["btn"] and c0["n"] == 1 and c0["prev"] == "btn-cbe" and c0["next"] == "btn-cds" and c0["glyph"] == "Hf" and c0["label"] == "Held by funds" and c0["title"] == TITLE,
               "the button is on the bar once, right after Credit and before CDS, under its name: %s" % {k: c0[k] for k in ("n", "prev", "next", "glyph", "label")})
            ck(k0["btn"] and k0["n"] == 1 and k0["prev"] == "btn-bnd" and k0["next"] == "btn-hbf" and k0["label"] == "Credit", "Credit is where it was, right after Bonds")
            ck(len(set(c0["desks"])) == len(c0["desks"]) and c0["overlay"] == "", "every desk button is on the bar once and the workspace is closed")

            def click(bid, shot=None):
                how = None
                for _ in range(3):
                    page.mouse.move(700, 3)                     # the toolbar hides until the pointer reaches the top edge
                    try:
                        page.wait_for_selector("#jh-ah-top.show", timeout=6000)
                    except Exception:  # noqa: BLE001
                        pass
                    page.wait_for_timeout(500)
                    page.evaluate("(id) => document.getElementById(id).scrollIntoView({ inline: 'center', block: 'nearest' })", bid)
                    page.wait_for_timeout(300)
                    seen = page.evaluate(CHART, bid)
                    if not on_screen(seen):
                        continue
                    if shot:
                        shots[shot] = page.screenshot(type="jpeg", quality=82, clip={"x": max(0, seen["box"][0] - 430), "y": max(0, seen["box"][1] - 14), "width": 940, "height": seen["box"][3] + 30})
                    try:
                        page.mouse.move(seen["box"][0] + seen["box"][2] / 2, seen["box"][1] + seen["box"][3] / 2)
                        page.click("#" + bid, timeout=6000)
                        how = "a real click at %s" % seen["box"][:2]
                        break
                    except Exception:  # noqa: BLE001
                        continue
                if not how:
                    page.evaluate("(id) => document.getElementById(id).click()", bid)
                return how

            def the_frame():
                for _ in range(80):
                    f = next((x for x in page.frames if "/credit-before-equity.html" in x.url), None)
                    if f:
                        return f
                    page.wait_for_timeout(250)
                return None

            # ------------------------------------------------------------------ 2 and 3. the click
            r.section("2. The pointer clicks it: the board opens in the workspace on the section")
            how = click("btn-hbf", "1-the-bar")
            ck(bool(how), "the pointer reaches the button and clicks it (%s)" % how)
            page.wait_for_selector("#ws-overlay.on iframe", timeout=15000)
            c1 = page.evaluate(CHART, "btn-hbf")
            r.log("workspace: %s" % json.dumps({k: c1[k] for k in ("overlay", "src", "head", "open")}))
            ck(c1["overlay"] == "on" and c1["src"] == HREF and c1["head"] == HEAD and c1["open"] == HREF, "the workspace opens the board on its section, under the button's name, and Open page goes to the same place")
            frame = the_frame()
            ck(frame is not None, "the workspace holds the board's frame")
            if frame is not None:
                frame.locator("#funds .cb-tile-h").first.wait_for(timeout=120000)
                page.wait_for_timeout(3500)                 # past the second landing and the site's own scripts
                frame = the_frame()
                frame.locator("#funds .cb-tile-h").first.wait_for(timeout=120000)
                s1 = read(frame, wait=page)
                r.log("inside the workspace: heading %s px from the top, scrolled %s px, address %s, tiles %s" % (s1["top"], s1["y"], s1["hash"], [t[1] for t in s1["tiles"]]))
                judge_section(s1, "from the chart's button", True)
                ck(s1["embedded"] and s1["hash"] == "#s-funds", "the board is inside the chart's workspace, at #s-funds")
                r.section("3. The fund document arrives, and the lists are there")
                frame.wait_for_function("() => { const b = document.querySelector('#funds [data-fu-view=\"gaps\"]'); return b && !document.querySelector('#funds [data-fu-load]'); }", timeout=120000)
                page.wait_for_timeout(1500)
                label = frame.locator('#funds [data-fu-view="gaps"]').inner_text()
                ck(label == "Marked apart (%s)" % format(block.get("n_gaps") or 0, ","), "the fund document arrives inside the workspace: %s" % label)
                s2 = read(frame, wait=page)
                ck(s2["top"] is not None and -4 <= s2["top"] <= 60, "the section's heading is still at the top once its lists have filled in (%s px)" % s2["top"])
                judge_section(s2, "after the lists filled in", True)
                shots["2-opened-on-the-section"] = page.screenshot(type="jpeg", quality=72)
                frame.locator("#funds-lists").scroll_into_view_if_needed()
                page.wait_for_timeout(600)
                shots["3-the-lists"] = page.screenshot(type="jpeg", quality=72)

            # ------------------------------------------------------------------ 4. Credit still opens the top
            r.section("4. The Credit button still opens the board at its top")
            page.click("#wsx")
            page.wait_for_function("() => document.getElementById('ws-overlay').className === ''", timeout=10000)
            how = click("btn-cbe")
            ck(bool(how), "the pointer clicks Credit (%s)" % how)
            page.wait_for_selector("#ws-overlay.on iframe", timeout=15000)
            c2 = page.evaluate(CHART, "btn-cbe")
            ck(c2["src"] == "/credit-before-equity.html" and (c2["head"] or "").startswith("Credit Before Equity · "), "Credit opens the board itself: %s" % c2["src"])
            frame = the_frame()
            if ck(frame is not None, "the workspace holds the board's frame"):
                frame.locator("#funds .cb-tile-h").first.wait_for(timeout=120000)
                page.wait_for_timeout(3500)
                frame = the_frame()
                frame.locator("#funds .cb-tile-h").first.wait_for(timeout=120000)
                s3 = read(frame, wait=page)
                r.log("from Credit: scrolled %s px, the section %s px down, address %r" % (s3["y"], s3["top"], s3["hash"]))
                judge_section(s3, "from Credit", False)
            ck(not errors, "no script error on the chart: %s" % errors[:3])

            # ------------------------------------------------------------------ 5. the page alone
            r.section("5. The page alone, with and without the section in its address; a phone")
            for name, kw, shot in (("a desk screen", {"viewport": {"width": 1500, "height": 950}}, "4-the-page-alone"),
                                   ("a phone", {"viewport": {"width": 390, "height": 844}, "device_scale_factor": 2, "is_mobile": True, "has_touch": True}, "5-phone")):
                pg = browser.new_context(user_agent=UA, **kw).new_page()
                perr = []
                pg.on("pageerror", lambda e: perr.append(str(e)[:200]))
                pg.goto("%s/credit-before-equity.html?nocache=%d#s-funds" % (BASE, stamp), wait_until="domcontentloaded")
                pg.locator("#funds .cb-tile-h").first.wait_for(timeout=120000)
                pg.wait_for_timeout(3500)
                s = read(pg)
                r.log("%s, #s-funds: heading %s px from the top, scrolled %s px, sideways overflow %s px" % (name, s["top"], s["y"], s["ox"]))
                judge_section(s, "the page alone on %s" % name, True)
                ck(s["ox"] <= 1 and not perr, "%s: the page fits the screen and throws no error" % name)
                shots[shot] = pg.screenshot(type="jpeg", quality=66)
                pg.goto("%s/credit-before-equity.html?nocache=%d" % (BASE, stamp + 1), wait_until="domcontentloaded")
                pg.locator("#funds .cb-tile-h").first.wait_for(timeout=120000)
                pg.wait_for_timeout(2500)
                s = read(pg)
                ck(s["y"] == 0 and s["hash"] == "", "%s: without the section in its address the page opens at its top (scrolled %s px)" % (name, s["y"]))
                pg.context.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(failed_checks=len(fails))
        if fails:
            r.fail("%d check(s) failed" % len(fails))
            sys.exit(1)
        r.ok("LIVE: the chart has a Held by funds button right after Credit; a click opens Credit Before Equity on that section; the section's numbers sit in its headings, none loose; Credit still opens the board at its top")


if __name__ == "__main__":
    main()
