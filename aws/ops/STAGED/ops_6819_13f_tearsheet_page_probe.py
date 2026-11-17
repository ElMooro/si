"""ops 6819 - 13F manager tearsheet, live page proof in headless Chrome (read-only, re-runnable).

Drives https://justhodl.ai/13f.html the way a reader does, at 1440 px and at 390 px:
  1. the served page is the new one and the served book is 1.4.0;
  2. every section the page had before is still there (banner, smart money, sectors, rotation, contenders, boards, the
     manager grid, the drill-down with its top-of-book table / newly present / absent / quantity up / down / asset mix /
     clone-alpha / filing chain, concentrated + diversified cards, clone-alpha, options, signals, security master,
     the evidence desk, reading rules, engines) - nothing was removed;
  3. the grid carries the three new columns; clicking every manager renders that manager's tearsheet - scoreboard
     tiles, the two notable cards, the donut, the current-positions table and the four lists - with the numbers the
     book holds, no 'undefined' / 'NaN', no horizontal overflow, and the evidence desk still follows the selection;
  4. the controls work: a tearsheet table sorts both ways, a donut slice shows its tooltip and lights its row, the
     print view isolates the tearsheet on white, Copy link yields ?fund=<manager>#drill and that link lands there.
Two JPEGs are embedded (base64): one tearsheet at 1440 px, the top of one at 390 px. Nothing on the site changes.
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
SECTIONS = ["banner", "smart", "stance", "sectors", "industries", "rotation", "contenders", "boards", "mgr-table", "drill", "conc", "diversified", "clone", "options",
            "signals", "master-lists", "evidence-desk", "limitations", "engines", "season", "verdicts"]
DRILL = ["REPORTED POSITIONS · TOP OF BOOK", "NEWLY PRESENT", "ABSENT", "QUANTITY UP", "QUANTITY DOWN", "ASSET MIX", "CLONE-ALPHA", "FILING CHAIN", "Reported equity value", "Effective accession"]
LISTS = ("initiations", "increases", "reductions", "terminations")


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


READ = """() => { const t = document.getElementById('tearsheet'), de = document.documentElement, sel = document.getElementById('selection');
  const row = [...document.querySelectorAll('#mgr-table tbody tr')].find(r => r.querySelector('button').dataset.fund === t.dataset.fund);
  return { head: t.querySelector('.ts-h').textContent, tiles: [...t.querySelectorAll('.ts-score .kpi .v')].map(e => e.textContent.trim()),
    cards: [...t.querySelectorAll('.ts-card .big')].map(e => e.textContent.trim()), paths: t.querySelectorAll('.ts-donut path').length,
    pos: t.querySelectorAll('#ts-pos tbody tr').length, lists: [...t.querySelectorAll('.ts-lists .ts-col')].map(c => [c.querySelector('header span:last-child').textContent, c.querySelectorAll('tbody tr').length]),
    rc: t.querySelector('.ts-rc') ? t.querySelectorAll('.ts-rc tbody tr').length : 0, ox: de.scrollWidth - de.clientWidth, url: location.search,
    bad: (t.innerText.match(/undefined|NaN|\\[object|null/g) || []).length, foot: t.querySelector('.ts-foot').textContent.slice(0, 150),
    cells: row ? [...row.cells].slice(-3).map(c => c.innerText.trim()) : null, desk: sel ? sel.value : null, deskHas: sel ? [...sel.options].some(o => o.value === t.dataset.fund) : false,
    title: document.getElementById('drill-title').textContent }; }"""


def main():
    with report("ops_6819_13f_tearsheet_page_probe") as r:
        r.heading("ops 6819 - 13F manager tearsheet, live page proof")
        stamp = int(time.time())
        req = urllib.request.Request("%s/13f.html?nocache=%d" % (BASE, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
        html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
        req = urllib.request.Request("%s/data/13f-book.json?nocache=%d" % (BASE, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            r.log("data/13f-book.json: HTTP %s, %d bytes on the wire, encoding %s, cache-control %s" % (resp.status, len(body), resp.headers.get("Content-Encoding"), resp.headers.get("Cache-Control")))
        book = json.loads(body)
        M = book["book"]["managers"]
        r.log("13f.html: %d bytes, tearsheet code %s, grid columns %s | book version %s generated %s, tearsheets %d/%d" % (
            len(html), "tearsheetHTML" in html, "Top addition · est." in html, book.get("version"), book.get("generated_at"), sum(1 for x in M.values() if x.get("tearsheet")), len(M)))
        if "tearsheetHTML" not in html:
            r.fail("the served 13f.html is not the new page yet (Pages deploy pending?)")
            raise SystemExit(1)
        if book.get("version") != "1.4.0" or not all(x.get("tearsheet") for x in M.values()):
            r.fail("the served book carries no tearsheets yet (version %s)" % book.get("version"))
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
                page.wait_for_selector("#tearsheet", timeout=20000)
                page.wait_for_timeout(1500)
                missing = [i for i in SECTIONS if page.locator("#" + i).count() == 0]
                empty = [i for i in SECTIONS if page.locator("#" + i).count() and not page.locator("#" + i).first.inner_text().strip()]
                ck(not missing, "%d: sections missing %s" % (width, missing))
                ck(not empty, "%d: sections empty %s" % (width, empty))
                n_rows, n_cols = page.locator("#mgr-table tbody tr").count(), page.locator("#mgr-table thead th").count()
                heads = page.evaluate("[...document.querySelectorAll('#mgr-table thead th')].map(t=>t.textContent)")
                ck(n_rows == len(M), "%d: grid rows %d vs %d managers" % (width, n_rows, len(M)))
                ck(n_cols == 18 and heads[:15] == ["Manager", "Period · form", "Filed", "Equity · option rows", "Reported value", "vs prior (incl. price)", "Top-10 %", "New", "Absent", "Up", "Down", "Flat", "Chain", "Clone SKILL", "Status"],
                   "%d: grid headers %s" % (width, heads))
                h2 = page.evaluate("[...document.querySelectorAll('main h2')].map(h=>h.textContent)")
                r.log("sections present %d/%d, none empty %s | grid %d rows x %d columns (new: %s) | %d h2 headings: %s" % (
                    len(SECTIONS) - len(missing), len(SECTIONS), not empty, n_rows, n_cols, heads[15:], len(h2), " | ".join(h2)))
                drill = page.locator("#drill-body").inner_text().lower()
                lost = [x for x in DRILL if x.lower() not in drill]
                ck(not lost, "%d: the drill-down lost %s" % (width, lost))
                for fk, x in M.items():
                    page.evaluate("fk=>document.querySelector('#mgr-table button[data-fund=\"'+fk+'\"]').click()", fk)
                    page.wait_for_function("fk=>{const t=document.getElementById('tearsheet');return t&&t.dataset.fund===fk}", arg=fk, timeout=10000)
                    page.wait_for_timeout(120)
                    t, info = x["tearsheet"], page.evaluate(READ)
                    ck(info["ox"] <= 0, "%d %s: page overflows by %s px" % (width, fk, info["ox"]))
                    ck(info["bad"] == 0, "%d %s: %d bad literals" % (width, fk, info["bad"]))
                    ck(info["url"] == "?nocache=%d&fund=%s" % (stamp, fk), "%d %s: url %s" % (width, fk, info["url"]))
                    ck(info["tiles"][1] == "{:,}".format(t["n_lines"]) and info["tiles"][5] == "{:,}".format(t["counts"]["unchanged"]), "%d %s: tiles %s" % (width, fk, info["tiles"]))
                    ck(info["tiles"][3].replace(" ", "") == "%s/%s" % ("{:,}".format(t["counts"]["new"]), "{:,}".format(t["counts"]["increased"])), "%d %s: new/increased tile %s" % (width, fk, info["tiles"][3]))
                    exp = [["{:,}".format(t[k]["n"]), len(t[k]["rows"])] for k in LISTS]
                    ck(info["lists"] == exp, "%d %s: lists %s vs %s" % (width, fk, info["lists"], exp))
                    nl = len([z for z in t["lines"] if (z["value_usd"] or 0) > 0])
                    ck(info["paths"] in (min(nl, 8), min(nl, 8) + 1) and info["pos"] >= nl, "%d %s: donut %s paths, %s rows for %s lines" % (width, fk, info["paths"], info["pos"], nl))
                    ck(info["rc"] == len(t["reclassified"]["rows"]), "%d %s: reclassified rows" % (width, fk))
                    ck(len(info["cards"]) == 2, "%d %s: notable cards" % (width, fk))
                    ck(not info["deskHas"] or info["desk"] == fk, "%d %s: the evidence desk did not follow (%s)" % (width, fk, info["desk"]))
                    ck(x["display_name"] in info["title"], "%d %s: drill title %s" % (width, fk, info["title"]))
                    if width == 1440:
                        r.log("%-12s %s | tiles %s | cards %s | donut %d | lists %s | reclassified %d | grid cells %s | desk %s" % (
                            fk, info["head"], info["tiles"], info["cards"], info["paths"], info["lists"], info["rc"], info["cells"], info["desk"]))
                drill = page.locator("#drill-body").inner_text().lower()
                ck(not [x for x in DRILL if x.lower() not in drill], "%d: the drill-down lost a block after switching managers" % width)
                ck(not errors, "%d: page errors %s" % (width, errors[:3]))
                r.log("%d managers clicked through at %d px; page errors %d" % (len(M), width, len(errors)))
                pick = "ELLIOTT" if "ELLIOTT" in M else next(iter(M))
                page.evaluate("fk=>document.querySelector('#mgr-table button[data-fund=\"'+fk+'\"]').click()", pick)
                page.wait_for_function("fk=>document.getElementById('tearsheet').dataset.fund===fk", arg=pick)
                page.wait_for_timeout(700)
                if width == 1440:
                    shots["desktop"] = page.locator("#tearsheet").screenshot(type="jpeg", quality=62)
                    r.section("controls (1440 px, %s)" % pick)
                    col = page.locator(".ts-col.k-up")
                    vals = lambda: page.evaluate("[...document.querySelectorAll('.ts-col.k-up tbody tr')].map(r=>r.cells[2].textContent)")  # noqa: E731
                    v0 = vals()
                    col.locator("thead th").nth(2).click()
                    page.wait_for_timeout(200)
                    v1 = vals()
                    col.locator("thead th").nth(2).click()
                    page.wait_for_timeout(200)
                    v2 = vals()
                    r.log("increases by Est.: as rendered %s | click 1 %s | click 2 %s" % (v0[:5], v1[:5], v2[:5]))
                    ck(len(v0) < 2 or (v1 != v2 and sorted(v1) == sorted(v2)), "the sorter did not reorder the increases table")
                    page.locator(".ts-donut path").first.hover()
                    page.wait_for_timeout(200)
                    tip = page.locator(".ts-tip")
                    ck(tip.is_visible() and "% of 13F value" in tip.inner_text(), "donut tooltip")
                    ck(page.locator("#ts-pos tbody tr.on").count() == 1, "the legend row did not light up")
                    r.log("donut tooltip: %s | lit rows %d" % (tip.inner_text() if tip.is_visible() else None, page.locator("#ts-pos tbody tr.on").count()))
                    page.evaluate("window.print=()=>{window.__printed=document.documentElement.classList.contains('print-ts')}")
                    page.locator("#ts-print").click()
                    ck(page.evaluate("window.__printed") is True, "print mode not set while printing")
                    page.evaluate("window.dispatchEvent(new Event('afterprint'))")
                    ck(page.evaluate("document.documentElement.classList.contains('print-ts')") is False, "print mode not cleared")
                    page.emulate_media(media="print")
                    page.evaluate("document.documentElement.classList.add('print-ts')")
                    page.wait_for_timeout(200)
                    vis = page.evaluate("()=>({tearsheet:document.getElementById('tearsheet').offsetHeight,banner:document.getElementById('banner').offsetHeight,grid:document.getElementById('mgr-table').offsetHeight,evidence:document.getElementById('evidence-desk').offsetHeight,buttons:document.querySelector('.ts-actions').offsetHeight,bg:getComputedStyle(document.body).backgroundColor})")
                    r.log("print view: %s" % json.dumps(vis))
                    ck(vis["tearsheet"] > 0 and not (vis["banner"] or vis["grid"] or vis["evidence"] or vis["buttons"]) and vis["bg"] == "rgb(255, 255, 255)", "print isolation %s" % vis)
                    page.emulate_media(media="screen")
                    page.evaluate("document.documentElement.classList.remove('print-ts')")
                    try:
                        ctx.grant_permissions(["clipboard-read", "clipboard-write"], origin=BASE)
                        page.locator("#ts-link").click()
                        page.wait_for_timeout(300)
                        clip = page.evaluate("navigator.clipboard.readText()")
                        r.log("Copy link: %s (button now reads '%s')" % (clip, page.locator("#ts-link").inner_text()))
                        ck("fund=%s" % pick in clip and clip.endswith("#drill"), "copied link %s" % clip)
                    except Exception as e:  # noqa: BLE001
                        r.warn("clipboard not readable in this browser (%s) - link checked through the deep link instead" % type(e).__name__)
                    p2 = ctx.new_page()
                    deep = "SCION" if "SCION" in M else pick
                    p2.goto("%s/13f.html?fund=%s#drill" % (BASE, deep), wait_until="domcontentloaded")
                    p2.wait_for_selector("#tearsheet", timeout=30000)
                    p2.wait_for_timeout(1200)
                    got, y = p2.evaluate("document.getElementById('tearsheet').dataset.fund"), p2.evaluate("window.scrollY")
                    r.log("deep link ?fund=%s#drill -> tearsheet %s, scrolled to %d px, headline: %s" % (deep, got, y, p2.locator(".ts-h").inner_text()))
                    ck(got == deep and y > 400, "deep link landed on %s at %s px" % (got, y))
                    p2.close()
                else:
                    page.locator("#tearsheet").scroll_into_view_if_needed()
                    box = page.locator("#tearsheet").bounding_box()
                    shots["mobile"] = page.screenshot(type="jpeg", quality=60, full_page=True, clip={"x": 0, "y": box["y"] + page.evaluate("window.scrollY") - 4, "width": 390, "height": 1500})
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
        r.ok("PAGE PROOF: %d managers x 2 widths render their tearsheet from the live book; every earlier section is present; sort, tooltip, print view and deep link work" % len(M))


if __name__ == "__main__":
    main()
    sys.exit(0)
