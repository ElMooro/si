"""ops 7000 - live proof in a real browser: "What the record says" on canaries.html, and the chart's Canaries button.

Khalid, 2026-11-27 11:06 ET: "next improve the engine : canaries https://justhodl.ai/canaries.html and add an icon for it in
chart.html also test it against historical data to calibrate it".

Read-only: a browser on justhodl.ai. It checks that
  * the page shows today's level and turn with the published document's own figures, what followed each band, the
    barometer through history, and every canary's record whole - sortable both ways, filtered, saved as a file;
  * the board that was there before is still there;
  * the chart's Canaries button (on the bar since 2026-11-26) opens the page in the workspace with the same reading.

Safe to run again.
"""
from pathlib import Path
import base64
import importlib.util
import json
import math
import re
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops")]
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
NAME = "ops_7000_canaries_record_live_proof"
BLOCKS = [["cr-now", "Today on the record’s scale"], ["cr-follow", "What followed each reading"], ["cr-replay", "The barometer through history"],
          ["cr-canaries", "Every canary’s record"], ["cr-method", "How it was counted, and what it cannot say"]]
PAGE = r"""() => { const s = document.getElementById('s-record'), q = (x) => s ? s.querySelector(x) : null, qa = (x) => s ? Array.from(s.querySelectorAll(x)) : [];
  const txt = (e) => e ? e.innerText.replace(/\s+/g, ' ').trim() : null;
  const rows = qa('.cr-all tbody tr');
  const col = (name) => { const heads = qa('.cr-all thead th').map(h => h.getAttribute('data-cr-sort')); const i = heads.indexOf(name); return rows.map(r => r.children[i] ? r.children[i].innerText.trim() : null); };
  return { there: !!s, h2: txt(q('h2.cr-h2')), y: Math.round(scrollY), embedded: window.top !== window,
    blocks: qa('.cr-block').map(b => [b.id, b.firstElementChild && b.firstElementChild.tagName === 'H3' ? b.firstElementChild.textContent.replace(/^§[\d.]+\s*/, '').trim() : null]),
    level: txt(q('#cr-level')), turn: txt(q('#cr-turn')), level_big: txt(q('#cr-level .cr-big')), turn_big: txt(q('#cr-turn .cr-big')),
    follow_turn: qa('#cr-follow-turn tbody tr').map(txt), follow_level: qa('#cr-follow-level tbody tr').map(txt), today_rows: qa('#cr-follow .cr-on').map(txt),
    cross_today: qa('#cr-cross td').filter(td => (td.getAttribute('style') || '').indexOf('outline:2px solid') >= 0).map(txt),
    paths: qa('#cr-svg path').length, rects: qa('#cr-svg rect').length, read: txt(q('#cr-read')),
    rows: rows.length, count: txt(q('.cr-count')), chips: qa('[data-cr-verdict]').map(txt), first: rows.slice(0, 3).map(r => txt(r.children[0]).slice(0, 60)),
    scores: col('auc_early').slice(0, 400), verdicts: col('verdict'), sorted_head: qa('.cr-all thead th').filter(h => h.getAttribute('aria-sort') !== 'none').map(h => [h.getAttribute('data-cr-sort'), h.getAttribute('aria-sort')]),
    method: txt(q('#cr-method')), not_rebuilt: txt(q('#cr-not')),
    badges: qa('.jh-secbadge').map(b => [b.textContent, b.classList.contains('float'), b.parentElement.tagName]), sections_script: !!document.querySelector('script[src*="jh-sections.js"]'),
    bad: ((s ? s.innerText : '').match(/\bNaN\b|\bundefined\b|\[object|\bnull\b/g) || []).length,
    before: { baro: !!document.querySelector('#baro svg'), mechs: document.querySelectorAll('#mechs .card').length, tbl: document.querySelectorAll('#tbl tbody tr').length,
              replay: !!document.querySelector('#replayWrap svg'), hero: (document.getElementById('hero') || { innerText: '' }).innerText.replace(/\s+/g, ' ').trim().slice(0, 200),
              sub: (document.getElementById('sub') || { innerText: '' }).innerText, views: Array.from(document.querySelectorAll('#baro button')).map(b => b.innerText.trim()) },
    kit: Array.from(document.querySelectorAll('[data-jed] *')).some(e => e.children.length === 0 && /Canaries record/.test(e.textContent || '')),
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
CHART = r"""(id) => { const b = (x) => document.getElementById(x), btn = b(id), ov = b('ws-overlay'), f = ov ? ov.querySelector('iframe') : null, r = btn ? btn.getBoundingClientRect() : null;
  return { btn: !!btn, n: document.querySelectorAll('#' + id).length, glyph: btn && btn.querySelector('.g') ? btn.querySelector('.g').textContent : null, label: btn && btn.querySelector('.l') ? btn.querySelector('.l').textContent : null,
    title: btn ? btn.title : null, box: r ? [Math.round(r.left), Math.round(r.top), Math.round(r.width), Math.round(r.height)] : null, view: [innerWidth, innerHeight],
    prev: btn && btn.previousElementSibling ? btn.previousElementSibling.id : null, next: btn && btn.nextElementSibling ? btn.nextElementSibling.id : null,
    overlay: ov ? ov.className : null, src: f ? f.getAttribute('src') : null, head: ov && ov.querySelector('.wshd span') ? ov.querySelector('.wshd span').textContent : null,
    desks: Array.from(document.querySelectorAll('#tfbar .wsdesk')).map(x => x.id) }; }"""


def fetch(path, timeout=90):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def ordinal(v):
    n = int(math.floor(v + 0.5))                    # as the page rounds (half up)
    return "%d%s" % (n, "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th"))


def signed(v, d=1):
    return ("+" if v > 0 else "−" if v < 0 else "") + ("%.*f" % (d, abs(v)))


def share(v):
    return "%d%%" % int(math.floor(v * 100 + 0.5)) if v is not None else "—"


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp = int(time.time())
    shots, fails = {}, []

    with report(NAME) as r:
        r.heading("ops 7000 - What the record says on canaries.html, and the chart's Canaries button: a real browser on the live site (read-only)")

        def ck(ok, text):
            (r.ok if ok else r.fail)(text)
            if not ok:
                fails.append(text)
            return ok

        r.section("0. The live site serves the new page and the new documents")
        served = {}
        for attempt in range(60):
            try:
                html = fetch("/canaries.html?probe=%d" % (stamp + attempt))
                tag = re.search(rb'<script src="(/jh-canary-record\.js\?v=[0-9A-Za-z._-]+)"', html)
                doc = json.loads(fetch("/data/canary-warroom.json?probe=%d" % (stamp + attempt)))
                rec = json.loads(fetch("/data/canary-record.json?probe=%d" % (stamp + attempt)))
                served = {"page": b'<section id="s-record"></section>' in html, "script": bool(tag) and b"What the record says" in fetch(tag.group(1).decode() + "&probe=%d" % (stamp + attempt)),
                          "board 2.0.0 with the reading": doc.get("version") == "2.0.0" and bool(doc.get("calibrated")), "record": rec.get("document") == "canary-record.v1",
                          "board read this record": ((doc.get("calibrated") or {}).get("record") or {}).get("generated_at") == rec.get("generated_at")}
            except Exception as e:  # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served and all(v is True for v in served.values()):
                break
            time.sleep(20)
        r.log("served after %d tries: %s" % (attempt + 1, json.dumps(served)))
        if not (served and all(v is True for v in served.values())):
            r.fail("the live site does not serve the record yet; nothing was judged")
            sys.exit(1)
        cal = doc["calibrated"]
        L, T = cal["level"], cal["turn"]
        r.log("the documents: board generated %s; record of %s, %s of %s canaries; LEVEL %s (%s pct, %s); TURN %s (%s pct, %s)" % (
            doc.get("generated_at"), rec.get("cut_day"), rec.get("tested"), rec.get("defined"), L["score"], L["percentile"], L["band"], T["change"], T["percentile"], T["band"]))
        months = [m for m in rec["barometer"]["months"] if m.get("per_mechanism") is not None]
        falls = [f for f in rec["market"]["falls"] if f["low"] >= months[0]["day"]]
        n_all = len(rec["canaries"])

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)

            def judge(s, where):
                ck(s["there"] and s["blocks"] == BLOCKS and s["bad"] == 0, "%s: the section is five blocks, each under its own heading, with no broken figure" % where)
                ck(s["level_big"] == ordinal(L["percentile"]) + " percentile" and L["band"] in s["level"] and ("score %.1f" % L["score"]) in s["level"],
                   "%s: the level reads the document's figures: %s, %s, score %.1f" % (where, s["level_big"], L["band"], L["score"]))
                ck(("in %d of %s months (%s)" % (L["after"]["fell"], format(L["after"]["months"], ","), share(L["after"]["fall_share"]))) in s["level"], "%s: and what followed its band: %d of %d months" % (where, L["after"]["fell"], L["after"]["months"]))
                ck(s["turn_big"] == signed(T["change"]) + " points" and T["band"] in s["turn"] and ("%d days back" % T["days_back"]) in s["turn"],
                   "%s: the turn reads the document's figures: %s, %s" % (where, s["turn_big"], T["band"]))
                ck(("in %d of %s months (%s)" % (T["after"]["fell"], format(T["after"]["months"], ","), share(T["after"]["fall_share"]))) in s["turn"]
                   and ("in %d of %d (%s)" % (T["after"]["early_fell"], T["after"]["early_months"], share(T["after"]["early_fall_share"]))) in s["turn"],
                   "%s: and what followed its band: %d of %d months, near a high %d of %d" % (where, T["after"]["fell"], T["after"]["months"], T["after"]["early_fell"], T["after"]["early_months"]))
                ck(len(s["follow_turn"]) == 4 and len(s["follow_level"]) == 5 and len(s["today_rows"]) == 2 and T["band"] in s["today_rows"][0] and L["band"] in s["today_rows"][1],
                   "%s: both tables of what followed are whole, with today's band marked in each" % where)
                cell = cal.get("cell") or {}
                ck(len(s["cross_today"]) == 1 and ("%d of %d" % (cell.get("fell"), cell.get("months"))) in s["cross_today"][0], "%s: level against turn marks today's cell (%s)" % (where, s["cross_today"]))
                ck(s["paths"] == 2 and s["rects"] == len(falls), "%s: the chart draws the level, the turn and %d falls of the index" % (where, len(falls)))
                ck(s["rows"] == n_all and ("%d of %d canaries" % (n_all, n_all)) in (s["count"] or ""), "%s: every canary is listed: %d rows" % (where, s["rows"]))
                ck("not a forecast" in (s["method"] or "") and "one in twenty" in s["method"], "%s: the method and its limits are stated" % where)

            # ── 1. the page alone ──
            r.section("1. The page alone, on a desk screen")
            ctx = browser.new_context(viewport={"width": 1600, "height": 1000}, user_agent=UA, accept_downloads=True)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/canaries.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_function("(n) => document.querySelectorAll('#s-record .cr-all tbody tr').length === n", arg=n_all, timeout=120000)
            page.wait_for_timeout(2500)
            s = page.evaluate(PAGE)
            r.log("heading: %s" % s["h2"])
            r.log("level card: %s" % s["level"])
            r.log("turn card: %s" % s["turn"])
            r.log("what followed the turn: %s" % json.dumps(s["follow_turn"], ensure_ascii=False))
            r.log("verdict chips: %s | first rows: %s" % (json.dumps(s["chips"], ensure_ascii=False), json.dumps(s["first"], ensure_ascii=False)))
            judge(s, "the page alone")
            for v, n in (rec.get("verdicts") or {}).items():
                if not any(c.endswith(" %d" % n) for c in s["chips"]):
                    ck(False, "a verdict's count is missing from the chips: %s %d" % (v, n))
            if s["sections_script"]:
                loose = [b for b in s["badges"] if b[1]]
                ck(not loose and all(b[2] in ("H2", "H3") for b in s["badges"]), "the site's section numbers sit in the section's headings, none loose (%d numbers)" % len(s["badges"]))
            ck(s["before"]["baro"] and s["before"]["mechs"] == len(doc["mechanisms"]) and s["before"]["tbl"] > 0 and s["before"]["replay"] and len(s["before"]["views"]) >= 3,
               "the board that was there is still there: gauge, %d mechanism cards, %d canary rows, the proxy replay, the three views %s" % (s["before"]["mechs"], s["before"]["tbl"], s["before"]["views"][:3]))
            ck("12 mechanisms" in s["before"]["sub"] and "voting today" in s["before"]["sub"], "the page's own line counts the mechanisms as the document does: %s" % s["before"]["sub"][:80])
            ck(s["kit"], "the page's Engine data section lists the record's document")
            shots["1-the-readings"] = page.locator("#cr-now").screenshot(type="jpeg", quality=72)
            shots["2-what-followed"] = page.locator("#cr-follow").screenshot(type="jpeg", quality=66)
            # the chart answers the pointer
            box = page.locator("#cr-svg").bounding_box()
            page.locator("#cr-svg").scroll_into_view_if_needed()
            box = page.locator("#cr-svg").bounding_box()
            page.mouse.move(box["x"] + box["width"] * 0.80, box["y"] + box["height"] * 0.4)
            page.wait_for_timeout(400)
            read = page.evaluate("() => document.getElementById('cr-read').innerText")
            ck(re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (19|20)\d\d · level ", read or "") is not None and not read.startswith("Nov 2026"), "the chart reads out the month under the pointer: %s" % read)
            shots["3-through-history"] = page.locator("#cr-replay").screenshot(type="jpeg", quality=66)
            # sorting, both ways
            page.locator('#s-record th[data-cr-sort="auc_early"]').click()
            page.wait_for_timeout(500)
            d1 = page.evaluate(PAGE)
            nums = [float(x) for x in d1["scores"] if x not in ("—", None)]
            ck(d1["sorted_head"] == [["auc_early", "descending"]] and nums == sorted(nums, reverse=True) and d1["rows"] == n_all and d1["scores"][-1] == "—", "a click sorts by early score, highest first, blanks last (%s ... %s)" % (nums[:2], nums[-1:]))
            page.locator('#s-record th[data-cr-sort="auc_early"]').click()
            page.wait_for_timeout(500)
            d2 = page.evaluate(PAGE)
            nums2 = [float(x) for x in d2["scores"] if x not in ("—", None)]
            ck(d2["sorted_head"] == [["auc_early", "ascending"]] and nums2 == sorted(nums2) and d2["scores"][-1] == "—", "a second click turns it round, blanks still last (%s ...)" % nums2[:2])
            page.locator("#s-record [data-cr-reset]").click()
            page.wait_for_timeout(300)
            page.locator('#s-record [data-cr-verdict="leads"]').click()
            page.wait_for_timeout(400)
            d3 = page.evaluate(PAGE)
            ck(d3["rows"] == rec["verdicts"]["leads"] and set(d3["verdicts"]) == {"Leads"}, "the Leads chip shows the %d canaries that lead and no other" % d3["rows"])
            r.log("the canaries that lead: %s" % json.dumps(page.evaluate("() => Array.from(document.querySelectorAll('#s-record .cr-all tbody tr')).map(r => r.children[0].querySelector('b').innerText + ' ' + r.children[8].innerText)"), ensure_ascii=False)[:1500])
            shots["4-the-canaries-that-lead"] = page.locator("#cr-canaries").screenshot(type="jpeg", quality=64)
            with page.expect_download(timeout=30000) as dl:
                page.locator("#cr-csv").click()
            body = Path(dl.value.path()).read_bytes().decode("utf-8")
            lines = [x for x in body.split("\r\n") if x]
            ck(len(lines) == d3["rows"] + 1 and lines[0].startswith("id,Canary,Mechanism,Verdict") and dl.value.suggested_filename == "canaries-record-%s.csv" % rec["cut_day"],
               "the file saved holds the %d rows shown and a header (%s)" % (len(lines) - 1, dl.value.suggested_filename))
            page.locator('#s-record [data-cr-verdict=""]').click()
            page.locator("#cr-q").fill("OAS")
            page.wait_for_timeout(500)
            d4 = page.evaluate(PAGE)
            ck(0 < d4["rows"] < 20 and all("oas" in x.lower() for x in page.evaluate("() => Array.from(document.querySelectorAll('#s-record .cr-all tbody tr')).map(r => r.innerText)")), "the search box narrows the list (%d rows for OAS)" % d4["rows"])
            ck(s["ox"] <= 1 and not errors, "the page fits the screen and throws no error (%s)" % errors[:2])
            ctx.close()

            r.section("2. On a phone")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, device_scale_factor=2, is_mobile=True, has_touch=True)
            pg = ctx.new_page()
            perr = []
            pg.on("pageerror", lambda e: perr.append(str(e)[:200]))
            pg.goto("%s/canaries.html?nocache=%d" % (BASE, stamp + 1), wait_until="domcontentloaded")
            pg.wait_for_function("(n) => document.querySelectorAll('#s-record .cr-all tbody tr').length === n", arg=n_all, timeout=120000)
            pg.wait_for_timeout(2000)
            sp = pg.evaluate(PAGE)
            judge(sp, "on a phone")
            ck(sp["ox"] <= 1 and not perr, "on a phone the page fits the screen (sideways overflow %s px) and throws no error" % sp["ox"])
            pg.locator("#cr-now").scroll_into_view_if_needed()
            shots["5-phone"] = pg.screenshot(type="jpeg", quality=62)
            ctx.close()

            r.section("3. From the chart's Canaries button")
            ctx = browser.new_context(viewport={"width": 1600, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            cerr = []
            page.on("pageerror", lambda e: cerr.append(str(e)[:200]))
            page.goto("%s/chart.html?s=SPY&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-can", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            c0 = page.evaluate(CHART, "btn-can")
            r.log("button: %s" % json.dumps({k: c0[k] for k in ("btn", "n", "glyph", "label", "title", "prev", "next")}, ensure_ascii=False))
            ck(c0["btn"] and c0["n"] == 1 and c0["label"] == "Canaries" and c0["glyph"] == "Cn", "the chart's bar has the Canaries button once (Cn Canaries, between %s and %s; %d desk buttons)" % (c0["prev"], c0["next"], len(c0["desks"])))
            chart_errors_before = list(cerr)
            clicked = None
            for _ in range(3):
                page.mouse.move(700, 3)
                try:
                    page.wait_for_selector("#jh-ah-top.show", timeout=6000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(500)
                page.evaluate("(id) => document.getElementById(id).scrollIntoView({ inline: 'center', block: 'nearest' })", "btn-can")
                page.wait_for_timeout(300)
                seen = page.evaluate(CHART, "btn-can")
                b, v = seen.get("box"), seen.get("view")
                if not (b and b[2] > 0 and b[3] > 0 and b[0] >= 0 and b[1] >= 0 and b[0] + b[2] <= v[0] and b[1] + b[3] <= v[1]):
                    continue
                shots["6-the-button-on-the-chart"] = page.screenshot(type="jpeg", quality=82, clip={"x": max(0, b[0] - 430), "y": max(0, b[1] - 14), "width": 940, "height": b[3] + 30})
                page.mouse.move(b[0] + b[2] / 2, b[1] + b[3] / 2)
                page.mouse.down()
                page.mouse.up()
                clicked = [int(b[0] + b[2] / 2), int(b[1] + b[3] / 2)]
                break
            ck(clicked is not None, "the pointer reaches the button and clicks it (a real click at %s)" % clicked)
            page.wait_for_function("() => { const o = document.getElementById('ws-overlay'); return o && /\\bon\\b/.test(o.className) && o.querySelector('iframe'); }", timeout=20000)
            c1 = page.evaluate(CHART, "btn-can")
            ck(c1["src"] == "/canaries.html", "the workspace opens the page: %s under \"%s\"" % (c1["src"], c1["head"]))
            frame, sf = None, None
            for _ in range(30):
                page.wait_for_timeout(1500)
                frame = next((f for f in page.frames if f != page.main_frame and "canaries.html" in (f.url or "")), None)
                if not frame:
                    continue
                try:
                    if frame.evaluate("(n) => document.querySelectorAll('#s-record .cr-all tbody tr').length === n", n_all):
                        sf = frame.evaluate(PAGE)
                        break
                except Exception:  # noqa: BLE001
                    continue
            if ck(sf is not None and sf["embedded"], "the page is inside the chart's workspace with its record drawn"):
                judge(sf, "from the chart's button")
            ck(not chart_errors_before, "no script error on the chart before the workspace opened: %s" % chart_errors_before[:2])
            page.wait_for_timeout(800)
            shots["7-opened-from-the-chart"] = page.screenshot(type="jpeg", quality=66)
            ctx.close()
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
        r.ok("LIVE: canaries.html shows what the record says with the documents' own figures, every canary's record whole, the board as it was beneath it; the chart's Canaries button opens it")


if __name__ == "__main__":
    main()
