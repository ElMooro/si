"""ops 6822 - the tearsheet's features across 13f.html, live page proof in headless Chrome (read-only, re-runnable).

Khalid 2026-11-17: "add the same features wherever needed on my 13F". Drives https://justhodl.ai/13f.html at 1440 px
and 390 px and checks, against the served book (1.5.0):
  1. nothing was removed: every earlier section is present and non-empty, the grid keeps its 18 columns, the drill-down
     keeps its blocks and the manager tearsheet still renders for every manager with the book's numbers;
  2. the scope tearsheet ("The quarter on one sheet"): tiles, the two notable cards naming their manager, the ring of
     books with one distinct fill per slice, the books table, the four lists + largest lines for all three views in
     both scopes with the book's counts, a manager chip on every row, MORE ROWS, sorting, the ring tooltip, the print
     view on white, Copy link (?scope=&lines=#cohort-ts-sec) and that link restoring the state, a chip opening that
     manager's own tearsheet;
  3. the cross-manager boards carry "10 of N" and the two added columns with the book's values; every manager card
     carries its tearsheet lines and a TEARSHEET button that opens the manager; the options overlay carries its four
     added columns; the header shows the all-lines total; the banner shows the largest-single-moves tile;
  4. no 'undefined' / 'NaN' anywhere in the page, no horizontal overflow, no page errors.
Two JPEGs are embedded (base64). Nothing on the site changes.
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
SECTIONS = ["banner", "smart", "stance", "cohort-ts-sec", "sectors", "industries", "rotation", "contenders", "boards", "mgr-table", "drill", "conc", "diversified", "clone",
            "options", "signals", "master-lists", "evidence-desk", "limitations", "engines", "season", "verdicts"]
DRILL = ["REPORTED POSITIONS · TOP OF BOOK", "NEWLY PRESENT", "ABSENT", "QUANTITY UP", "QUANTITY DOWN", "ASSET MIX", "CLONE-ALPHA", "FILING CHAIN", "Reported equity value", "Effective accession"]
LISTS = ("initiations", "increases", "reductions", "terminations")
KINDS = ("all", "non_option", "option")


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


def n(v):
    return "{:,}".format(v)


SHEET = """() => { const t = document.getElementById('cohort-tearsheet'); const vis = r => r.offsetHeight > 0;
  return { scope: t.dataset.scope, kind: t.dataset.kind, head: t.querySelector('.ts-h').textContent, tiles: [...t.querySelectorAll('.ts-score .kpi .v')].map(e => e.textContent.trim()),
    cards: [...t.querySelectorAll('.ts-card')].map(c => ({big: c.querySelector('.big').textContent.trim(), who: (c.querySelector('.eyebrow .ts-m') || {}).textContent || null})),
    fills: [...t.querySelectorAll('.ts-donut path')].map(p => { const c = getComputedStyle(p); return c.fill + '/' + c.fillOpacity; }), books: t.querySelectorAll('#cts-pos tbody tr').length,
    cols: [...t.querySelectorAll('.ts-lists .ts-col')].map(c => ({n: c.querySelector('header span:last-child').textContent, rows: c.querySelectorAll('tbody tr').length,
      shown: [...c.querySelectorAll('tbody tr')].filter(vis).length, chips: c.querySelectorAll('tbody tr button.ts-m').length, opts: c.querySelectorAll('tbody tr .c-am').length})),
    bad: (t.innerText.match(/undefined|NaN|\\[object|null/g) || []).length, title: document.getElementById('cts-title').textContent }; }"""

MGR = """() => { const t = document.getElementById('tearsheet');
  return { fund: t.dataset.fund, tiles: [...t.querySelectorAll('.ts-score .kpi .v')].map(e => e.textContent.trim()), paths: t.querySelectorAll('.ts-donut path').length,
    lists: [...t.querySelectorAll('.ts-lists .ts-col')].map(c => [c.querySelector('header span:last-child').textContent, c.querySelectorAll('tbody tr').length]),
    bad: (t.innerText.match(/undefined|NaN|\\[object|null/g) || []).length, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""

PARTS = """() => { const q = s => document.querySelector(s), all = s => [...document.querySelectorAll(s)], de = document.documentElement;
  return { ox: de.scrollWidth - de.clientWidth, bad: (q('main').innerText.match(/undefined|NaN|\\[object/g) || []).length,
    pill: all('#h-pills .pill').map(e => e.innerText.replace(/\\n/g, ' ')).filter(t => /13F value · all lines/.test(t)),
    tiles: all('#verdicts .sub').map(e => e.querySelector('.eyebrow').innerText), boardTitles: all('#boards h3').map(h => h.innerText),
    boardHeads: all('#boards table').map(t => [...t.querySelectorAll('th')].map(x => x.innerText).slice(-2)),
    board0: all('#boards table')[0] ? [...all('#boards table')[0].querySelector('tbody tr').cells].map(c => c.innerText.replace(/\\n/g, ' ')) : null,
    conc: all('#conc > .sub').length, concMini: all('#conc .ts-mini').length, div: all('#diversified > .sub').length, divMini: all('#diversified .ts-mini').length,
    mini0: q('#conc .ts-mini') ? q('#conc .ts-mini').innerText.replace(/\\n/g, ' | ') : null,
    optHeads: all('#options th').map(t => t.innerText), optRows: all('#options tbody tr').map(r => [r.querySelector('button').dataset.fund].concat([...r.cells].slice(-4).map(c => c.innerText))),
    grid: all('#mgr-table thead th').length, h2: all('main h2').length }; }"""


def main():
    with report("ops_6822_13f_tearsheet_everywhere_page_probe") as r:
        r.heading("ops 6822 - the tearsheet's features across 13f.html, live page proof")
        stamp = int(time.time())
        html = urllib.request.urlopen(urllib.request.Request("%s/13f.html?nocache=%d" % (BASE, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"}), timeout=30).read().decode("utf-8", "replace")
        book = json.loads(urllib.request.urlopen(urllib.request.Request("%s/data/13f-book.json?nocache=%d" % (BASE, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"}), timeout=30).read())
        bk = book["book"]
        M, T = bk["managers"], bk.get("tearsheet") or {}
        r.log("13f.html: %d bytes, scope sheet code %s | book %s generated %s, scope tearsheets %s" % (len(html), "cohortSheet" in html, book.get("version"), book.get("generated_at"), sorted(T)))
        if "cohortSheet" not in html:
            r.fail("the served 13f.html is not the new page yet (Pages deploy pending?)")
            raise SystemExit(1)
        if book.get("version") != "1.5.0" or not T.get("cohort"):
            r.fail("the served book carries no scope tearsheets yet (version %s)" % book.get("version"))
            raise SystemExit(1)

        sync_playwright = ensure_playwright(r.log)
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)

        with sync_playwright() as p:
            browser = launch(p, r.log)
            for width in (1440, 390):
                r.section("%d px" % width)
                ctx = browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
                page = ctx.new_page()
                errors = []
                page.on("pageerror", lambda e: errors.append("pageerror: %s" % e))
                page.goto("%s/13f.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_selector("#mgr-table tbody tr", timeout=40000)
                page.wait_for_selector("#cohort-tearsheet", timeout=20000)
                page.wait_for_selector("#tearsheet", timeout=20000)
                page.wait_for_timeout(1500)
                # 1. nothing removed
                missing = [i for i in SECTIONS if page.locator("#" + i).count() == 0]
                empty = [i for i in SECTIONS if page.locator("#" + i).count() and not page.locator("#" + i).first.inner_text().strip()]
                ck(not missing and not empty, "%d: sections missing %s empty %s" % (width, missing, empty))
                drill = page.locator("#drill-body").inner_text().lower()
                ck(not [x for x in DRILL if x.lower() not in drill], "%d: the drill-down lost a block" % width)
                parts = page.evaluate(PARTS)
                ck(parts["ox"] <= 0 and parts["bad"] == 0, "%d: overflow %s px, bad literals %s" % (width, parts["ox"], parts["bad"]))
                ck(parts["grid"] == 18, "%d: grid columns %s" % (width, parts["grid"]))
                # 3. boards, cards, options, header, banner
                bd = bk["boards"]["cohort"]["all"]
                want_titles = [("MOST WIDELY HELD", bd["n_held"]), ("LARGEST REPORTED VALUE", bd["n_held"]), ("NEWLY PRESENT", bd["n_newly_present"]), ("ABSENT FROM DISCLOSURE", bd["n_dropped"]),
                               ("REPORTED QUANTITY UP", bd["n_increased"]), ("REPORTED QUANTITY DOWN", bd["n_decreased"])]
                for (lab, tot), got in zip(want_titles, parts["boardTitles"]):
                    ck(got.upper().startswith(lab) and got.endswith("of %s" % n(tot)), "%d: board title '%s' should end 'of %s'" % (width, got, n(tot)))
                ck(len(parts["boardTitles"]) == 6 and all(h == ["VALUE PRIOR", "NET EST. $"] for h in parts["boardHeads"]), "%d: board columns %s" % (width, parts["boardHeads"]))
                ck(parts["concMini"] == parts["conc"] > 0 and parts["divMini"] == parts["div"] > 0, "%d: cards %s/%s concentrated, %s/%s diversified carry the tearsheet lines" % (width, parts["concMini"], parts["conc"], parts["divMini"], parts["div"]))
                ck(len(parts["optHeads"]) == 11 and parts["optHeads"][7:] == ["OPTIONS · SHARE OF 13F VALUE", "LARGEST CALL LINES", "LARGEST PUT LINES", "OPTION LINES NEW · UP · DOWN · ABSENT"], "%d: options columns %s" % (width, parts["optHeads"]))
                for row in parts["optRows"]:
                    o = M[row[0]]["tearsheet"]["options"]
                    kc = M[row[0]]["tearsheet"]["counts_by_kind"]["option"]
                    ck(row[1].startswith("%.1f%%" % o["share_pct"]) and row[4].replace(" ", "") == "·".join(n(kc[k]) for k in ("new", "increased", "reduced", "closed")), "%d: options row %s" % (width, row))
                    ck(all((z.get("ticker") or "") in row[2] for z in o["top_calls"]) and all((z.get("ticker") or "") in row[3] for z in o["top_puts"]), "%d: options row %s lacks its largest lines" % (width, row[0]))
                ck(len(parts["pill"]) == 1 and "%.1f%%" % T["cohort"]["options"]["share_pct"] in parts["pill"][0], "%d: header pill %s" % (width, parts["pill"]))
                ck(len(parts["tiles"]) == 5 and parts["tiles"][-1].upper().startswith("LARGEST SINGLE MOVES"), "%d: verdict tiles %s" % (width, parts["tiles"]))
                r.log("sections %d/%d present, none empty | overflow %s px, bad literals %s, grid %s columns, %s h2 | boards: %s | board columns %s | first board row %s" % (
                    len(SECTIONS) - len(missing), len(SECTIONS), parts["ox"], parts["bad"], parts["grid"], parts["h2"], " · ".join(parts["boardTitles"]), parts["boardHeads"][0], parts["board0"]))
                r.log("cards with tearsheet lines: %s/%s concentrated, %s/%s diversified - e.g. %s | options overlay %d columns, rows %s | header pill %s | verdict tiles %s" % (
                    parts["concMini"], parts["conc"], parts["divMini"], parts["div"], parts["mini0"], len(parts["optHeads"]), json.dumps(parts["optRows"][:2]), parts["pill"], parts["tiles"]))
                # 2. the scope tearsheet: both scopes x three views
                for scope, btn in (("cohort", "#scope-cohort"), ("concentrated", "#scope-conc")):
                    page.locator(btn).click()
                    page.wait_for_function("s=>document.getElementById('cohort-tearsheet').dataset.scope===s", arg=scope)
                    t = T[scope]
                    for kind in KINDS:
                        page.locator("#cohort-tearsheet button[data-kind='%s']" % kind).click()
                        page.wait_for_function("k=>document.getElementById('cohort-tearsheet').dataset.kind===k", arg=kind)
                        s, v = page.evaluate(SHEET), t["views"][kind]
                        c = t["counts"] if kind == "all" else t["counts_by_kind"][kind]
                        tag = "%d %s/%s" % (width, scope, kind)
                        ck(s["bad"] == 0, "%s: %d bad literals" % (tag, s["bad"]))
                        ck(s["tiles"][1] == n(v["n_lines"]) and s["tiles"][3].replace(" ", "") == "%s/%s" % (n(c["new"]), n(c["increased"])) and s["tiles"][4].replace(" ", "") == "%s/%s" % (n(c["reduced"]), n(c["closed"])),
                           "%s: tiles %s" % (tag, s["tiles"]))
                        for i, lab in enumerate(LISTS):
                            L, col = v[lab], s["cols"][i]
                            ck(col["n"] == n(L["n"]) and col["rows"] == len(L["rows"]) and col["shown"] == min(8, len(L["rows"])) and col["chips"] == len(L["rows"]), "%s: %s %s vs n %s rows %s" % (tag, lab, col, L["n"], len(L["rows"])))
                            ck(kind == "all" or col["opts"] == (len(L["rows"]) if kind == "option" else 0), "%s: %s holds rows of the wrong kind" % (tag, lab))
                        ck(s["cols"][4]["rows"] == len(v["lines"]), "%s: largest lines %s vs %s" % (tag, s["cols"][4]["rows"], len(v["lines"])))
                        for card, key in zip(s["cards"], ("cut", "add")):
                            nt = v["notable"][key]
                            ck((card["who"] is None) == (nt is None), "%s: notable %s card names %s for %s" % (tag, key, card["who"], (nt or {}).get("fk")))
                        ck(len(set(s["fills"])) == len(s["fills"]) == min(6, len(t["by_manager"])) + (1 if len(t["by_manager"]) > 6 else 0) and s["books"] == len(t["members"]), "%s: ring %s, books %s" % (tag, s["fills"], s["books"]))
                        if width == 1440:
                            r.log("%-13s %-10s %s | tiles %s | cards %s | lists %s | ring %d slices, %d books" % (scope, kind, s["head"] if kind == "all" else "", s["tiles"], [(x["big"], x["who"]) for x in s["cards"]],
                                                                                                         [(x["n"], x["shown"], x["rows"]) for x in s["cols"]], len(s["fills"]), s["books"]))
                    page.locator("#cohort-tearsheet button[data-kind='all']").click()
                    page.wait_for_function("document.getElementById('cohort-tearsheet').dataset.kind==='all'")
                page.wait_for_timeout(300)
                if width == 1440:
                    r.section("scope sheet controls (1440 px, concentrated)")
                    shots["desktop"] = page.locator("#cohort-tearsheet").screenshot(type="jpeg", quality=60)
                    page.locator("#cts-rows").click()
                    page.wait_for_timeout(200)
                    s = page.evaluate(SHEET)
                    ck(all(x["shown"] == x["rows"] for x in s["cols"]) and page.locator("#cts-rows").inner_text() == "FEWER ROWS", "MORE ROWS did not show every kept row: %s" % [(x["shown"], x["rows"]) for x in s["cols"]])
                    col = page.locator("#cohort-tearsheet .ts-col.k-up")
                    vals = lambda: page.evaluate("[...document.querySelectorAll('#cohort-tearsheet .ts-col.k-up tbody tr')].map(r=>r.cells[2].textContent)")  # noqa: E731
                    v0 = vals()
                    col.locator("thead th").nth(2).click()
                    page.wait_for_timeout(200)
                    col.locator("thead th").nth(2).click()
                    page.wait_for_timeout(200)
                    v2 = vals()
                    r.log("MORE ROWS -> %s | increases by Est.: as rendered %s | after two clicks %s" % ([(x["shown"], x["rows"]) for x in s["cols"]], v0[:4], v2[:4]))
                    ck(len(v0) < 2 or (v0 != v2 and sorted(v0) == sorted(v2)), "the sorter did not reorder the scope increases")
                    page.locator("#cohort-tearsheet .ts-donut path").nth(1).hover()   # the second book: a slice narrow enough that its box centre lies on the slice
                    page.wait_for_timeout(200)
                    tip = page.locator("#cohort-tearsheet .ts-tip")
                    ck(tip.is_visible() and "% of the 13F value" in tip.inner_text() and page.locator("#cts-pos tbody tr.on").count() == 1, "ring tooltip / lit row")
                    r.log("ring tooltip: %s" % (tip.inner_text() if tip.is_visible() else None))
                    page.evaluate("window.print=()=>{window.__printed=document.documentElement.className}")
                    page.locator("#cts-print").click()
                    ck("print-cts" in (page.evaluate("window.__printed") or ""), "print mode not set while printing")
                    page.evaluate("window.dispatchEvent(new Event('afterprint'))")
                    page.emulate_media(media="print")
                    page.evaluate("document.documentElement.classList.add('print-cts')")
                    page.wait_for_timeout(200)
                    vis = page.evaluate("()=>({sheet:document.getElementById('cohort-tearsheet').offsetHeight,banner:document.getElementById('banner').offsetHeight,stance:document.getElementById('stance').offsetHeight,drill:document.getElementById('drill').offsetHeight,buttons:document.querySelector('#cohort-tearsheet .ts-actions').offsetHeight,bg:getComputedStyle(document.body).backgroundColor})")
                    r.log("print view: %s" % json.dumps(vis))
                    ck(vis["sheet"] > 0 and not (vis["banner"] or vis["stance"] or vis["drill"] or vis["buttons"]) and vis["bg"] == "rgb(255, 255, 255)", "print isolation %s" % vis)
                    page.emulate_media(media="screen")
                    page.evaluate("document.documentElement.classList.remove('print-cts')")
                    page.locator("#cohort-tearsheet button[data-kind='non_option']").click()
                    page.wait_for_timeout(200)
                    try:
                        ctx.grant_permissions(["clipboard-read", "clipboard-write"], origin=BASE)
                        page.locator("#cts-link").click()
                        page.wait_for_timeout(300)
                        clip = page.evaluate("navigator.clipboard.readText()")
                        r.log("Copy link: %s" % clip)
                        ck("scope=concentrated" in clip and "lines=non_option" in clip and clip.endswith("#cohort-ts-sec"), "copied link %s" % clip)
                    except Exception as e:  # noqa: BLE001
                        r.warn("clipboard not readable (%s)" % type(e).__name__)
                    p2 = ctx.new_page()
                    p2.goto("%s/13f.html?scope=concentrated&lines=non_option#cohort-ts-sec" % BASE, wait_until="domcontentloaded")
                    p2.wait_for_selector("#cohort-tearsheet", timeout=30000)
                    p2.wait_for_timeout(800)
                    st = p2.evaluate("({scope:document.getElementById('cohort-tearsheet').dataset.scope,kind:document.getElementById('cohort-tearsheet').dataset.kind,on:document.getElementById('scope-conc').className,stance:document.getElementById('smart-meta').textContent.slice(0,40)})")
                    r.log("deep link ?scope=concentrated&lines=non_option -> %s" % json.dumps(st))
                    ck(st["scope"] == "concentrated" and st["kind"] == "non_option" and " on" in st["on"], "deep link state %s" % st)
                    p2.close()
                else:
                    page.locator("#cohort-tearsheet").scroll_into_view_if_needed()
                    box = page.locator("#cohort-tearsheet").bounding_box()
                    shots["mobile"] = page.screenshot(type="jpeg", quality=58, full_page=True, clip={"x": 0, "y": box["y"] + page.evaluate("window.scrollY") - 4, "width": 390, "height": 1500})
                # a book chip inside the sheet opens that manager's own tearsheet; a card's TEARSHEET button too
                chip = page.locator("#cohort-tearsheet .ts-lists button.ts-m").first
                want = chip.get_attribute("data-fund")
                chip.click()
                page.wait_for_function("fk=>document.getElementById('tearsheet').dataset.fund===fk", arg=want, timeout=10000)
                card = page.locator("#conc .ts-mini button.ts-m").last
                want2 = card.get_attribute("data-fund")
                card.click()
                page.wait_for_function("fk=>document.getElementById('tearsheet').dataset.fund===fk", arg=want2, timeout=10000)
                r.log("chip in the sheet -> %s tearsheet; card TEARSHEET button -> %s tearsheet" % (want, want2))
                # 1b. the manager tearsheet, every manager, as before
                for fk, x in M.items():
                    page.evaluate("fk=>document.querySelector('#mgr-table button[data-fund=\"'+fk+'\"]').click()", fk)
                    page.wait_for_function("fk=>document.getElementById('tearsheet').dataset.fund===fk", arg=fk, timeout=10000)
                    t, info = x["tearsheet"], page.evaluate(MGR)
                    exp = [[n(t[k]["n"]), len(t[k]["rows"])] for k in LISTS]
                    nl = len([z for z in t["lines"] if (z["value_usd"] or 0) > 0])
                    ck(info["bad"] == 0 and info["ox"] <= 0 and info["tiles"][1] == n(t["n_lines"]) and info["lists"] == exp and info["paths"] in (min(nl, 6), min(nl, 6) + 1), "%d %s: manager tearsheet %s" % (width, fk, info))
                ck(not errors, "%d: page errors %s" % (width, errors[:3]))
                r.log("%d managers' own tearsheets re-checked at %d px; page errors %d" % (len(M), width, len(errors)))
                ctx.close()
            browser.close()
        r.section("screenshots (jpeg, base64)")
        for k, shot in shots.items():
            r.log("%s: %d bytes" % (k, len(shot)))
            r.lines.append("<!-- SHOT_%s_JPEG_BASE64 -->" % k.upper())
            r.lines.append(base64.b64encode(shot).decode("ascii"))
            r.lines.append("<!-- /SHOT_%s_JPEG_BASE64 -->" % k.upper())
        r.kv(page_bytes=str(len(html)), book_version=str(book.get("version")), managers=str(len(M)), failures=str(len(fails)))
        if fails:
            for f in fails[:40]:
                r.fail(f)
            raise SystemExit(1)
        r.ok("PAGE PROOF: the scope tearsheet (2 scopes x 3 views), boards, cards, options overlay, header and banner carry the tearsheet's features; %d manager tearsheets and every earlier section intact at both widths" % len(M))


if __name__ == "__main__":
    main()
    sys.exit(0)
