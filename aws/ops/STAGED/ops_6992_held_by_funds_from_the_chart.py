"""ops 6992 - "does it have an icon on chart.html?" (Khalid 2026-11-26 22:35 ET): the chart's Credit button, then Held by funds inside it.

Read-only: nothing is invoked and nothing is written. A real browser on justhodl.ai opens chart.html, moves the pointer to
the bar, clicks the Credit button (btn-cbe, between Bonds and CDS), and inside the workspace it opens: finds the section
"Held by funds", reads its five tiles against the published document, waits for the fund document, and opens a company's
sheet from the picture of protection sold and bought.

Safe to run again.
"""
from pathlib import Path
import base64
import importlib.util
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops")]
NAME = "ops_6992_held_by_funds_from_the_chart"
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
CHART = r"""() => { const btn = document.getElementById('btn-cbe'), r = btn ? btn.getBoundingClientRect() : null, ov = document.getElementById('ws-overlay');
  return { btn: !!btn, prev: btn && btn.previousElementSibling ? btn.previousElementSibling.id : null, next: btn && btn.nextElementSibling ? btn.nextElementSibling.id : null,
    label: btn ? btn.innerText.replace(/\s+/g, ' ').trim() : null, title: btn ? btn.title : null, box: r ? [r.left, r.top, r.width, r.height] : null, overlay: ov ? ov.className : null,
    vw: window.innerWidth, vh: window.innerHeight }; }"""
TILES = r"""() => Array.from(document.querySelectorAll('#funds .cb-tile-h')).map(e => [e.querySelector('.cb-lbl').innerText.trim(), e.querySelector('.cb-big').innerText.trim()])"""


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp, shots, fails = int(time.time()), {}, []
    with report(NAME) as r:
        def ck(ok, text):
            (r.ok if ok else r.fail)(text)
            if not ok:
                fails.append(text)

        r.heading("ops 6992 - Held by funds, reached from the chart's Credit button (read-only)")
        req = urllib.request.Request("%s/data/credit-before-equity.json?probe=%d" % (BASE, stamp), headers={"User-Agent": UA, "Cache-Control": "no-cache"})
        doc = json.loads(urllib.request.urlopen(req, timeout=120).read())
        block = doc.get("funds") or {}
        r.log("the published document: generated %s, fund layer %s, %s securities tied to %s companies" % (doc.get("generated_at"), block.get("version"), (block.get("tied") or {}).get("securities"), (block.get("tied") or {}).get("companies")))
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            page = browser.new_context(viewport={"width": 1600, "height": 900}, user_agent=UA).new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?s=SPY&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-cbe", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            c0 = page.evaluate(CHART)
            r.log("the button: %s" % json.dumps({k: c0[k] for k in ("btn", "prev", "next", "label", "title")}))
            ck(c0["btn"] and c0["prev"] == "btn-bnd" and c0["next"] in ("btn-hbf", "btn-cds") and (c0["label"] or "").lower() == "cr credit", "chart.html has the Credit button, after Bonds (since 2026-11-27 Held by funds stands between it and CDS)")
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
                b = seen["box"]
                if not b or b[2] <= 0 or b[0] < 0 or b[0] + b[2] > seen["vw"] or b[1] < 0 or b[1] + b[3] > seen["vh"]:
                    continue
                shots["1-the-bar"] = page.screenshot(type="jpeg", quality=80, clip={"x": max(0, b[0] - 420), "y": max(0, b[1] - 14), "width": 900, "height": b[3] + 30})
                try:
                    page.mouse.move(b[0] + b[2] / 2, b[1] + b[3] / 2)
                    page.click("#btn-cbe", timeout=6000)
                    how = "a real click at %s" % [round(x) for x in b[:2]]
                    break
                except Exception:  # noqa: BLE001
                    continue
            ck(bool(how), "the pointer reaches the button on the bar and clicks it (%s)" % how)
            if not how:
                page.evaluate("() => document.getElementById('btn-cbe').click()")
            page.wait_for_selector("#ws-overlay.on iframe", timeout=15000)

            def the_frame():
                for _ in range(80):
                    f = next((x for x in page.frames if "/credit-before-equity.html" in x.url), None)
                    if f:
                        return f
                    page.wait_for_timeout(250)
                return None
            frame = the_frame()
            ck(frame is not None, "the click opens Credit Before Equity in the chart's workspace")
            if frame is not None:
                frame.locator("#funds .cb-tile-h").first.wait_for(timeout=120000)
                page.wait_for_timeout(3000)                 # the workspace may load its page a second time: let it settle, then read the frame that stayed
                frame = the_frame()
                frame.locator("#funds .cb-tile-h").first.wait_for(timeout=120000)
                title = frame.locator("#s-funds h2.sec").inner_text()
                tiles = None
                for _ in range(4):
                    try:
                        tiles = frame.evaluate(TILES)
                        break
                    except Exception:  # noqa: BLE001
                        page.wait_for_timeout(1500)
                        frame = the_frame()
                        frame.locator("#funds .cb-tile-h").first.wait_for(timeout=120000)
                r.log("inside the workspace: %s | tiles %s" % (title[:80], tiles))
                tied = block.get("tied") or {}
                ck(len(tiles) == 5 and tiles[0][1] == format(tied.get("securities") or 0, ",") and tiles[1][1] == format(tied.get("companies") or 0, ","), "the section Held by funds is there with the document's figures (%s securities, %s companies)" % (tiles[0][1] if tiles else None, tiles[1][1] if len(tiles) > 1 else None))
                frame.locator("#s-funds").scroll_into_view_if_needed()
                frame.wait_for_function("() => { const b = document.querySelector('#funds [data-fu-view=\"gaps\"]'); return b && !document.querySelector('#funds [data-fu-load]'); }", timeout=120000)
                page.wait_for_timeout(800)
                label = frame.locator('#funds [data-fu-view="gaps"]').inner_text()
                ck(label == "Marked apart (%s)" % format(block.get("n_gaps") or 0, ","), "the fund document arrives inside the workspace: %s" % label)
                shots["2-held-by-funds-in-the-chart"] = page.screenshot(type="jpeg", quality=72)
                first = frame.locator("#funds .cb-fu-bf").first
                who = first.get_attribute("data-sheet")
                first.click()
                frame.locator("#sheet #sheet-funds").wait_for(timeout=30000)
                frame.wait_for_function("() => document.querySelector('#sheet table.cb-bonds th button[data-fund-sort]')", timeout=60000)
                n = frame.evaluate("() => document.querySelectorAll('#sheet table.cb-bonds:has(button[data-fund-sort]) tbody tr').length")
                cols = doc["issuers"]["cols"]
                row = next(dict(zip(cols, x)) for x in doc["issuers"]["rows"] if x[0] == who)
                ck(n == row["f_n"], "a company opened from the picture (%s) lists its %s fund-held securities in its sheet" % (who, n))
                frame.locator("#sheet #sheet-funds").scroll_into_view_if_needed()
                page.wait_for_timeout(500)
                shots["3-a-company-sheet-in-the-chart"] = page.screenshot(type="jpeg", quality=72)
            ck(not errors, "no script error on the chart: %s" % errors[:3])
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
        r.ok("chart.html has the Credit button; a click opens Credit Before Equity in the workspace, and Held by funds is on it with its lists and company sheets")


if __name__ == "__main__":
    main()
