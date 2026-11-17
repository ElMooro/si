"""ops 6825 - Baupost's value unit and the CUSIP successions on 13f.html, live page proof in headless Chrome
(read-only, re-runnable).

Khalid 2026-11-17: "Fix these: Baupost shows as $5M because of its units, and Exxon has no row in the ticker table
this quarter." Drives https://justhodl.ai/13f.html at 1440 px and 390 px and checks, against the served book (1.6.0):
  1. nothing was removed: every earlier section is present and non-empty, the grid keeps its 18 columns, the
     drill-down keeps its blocks, every manager's tearsheet still shows the book's 13F value;
  2. Baupost: opened from the grid, its tearsheet reads the book's ~$5B, wears the FILED IN $ THOUSANDS chip and
     the value-unit note, its cover-page line says how it was filed, its grid row and the grid footnote agree - and
     no other manager wears any of it;
  3. the evidence desk: selecting Baupost shows the as-filed note, selecting another manager hides it;
  4. "One security, new CUSIP or class text": one row per merged identity in the book, Exxon's row with its two
     CUSIPs, holders now and before, up / down and values; the scope and universe switches change the rows;
  5. no 'undefined' / 'NaN', no horizontal overflow, no page errors.
Three JPEGs are embedded (base64). Nothing on the site changes.
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
WANT = "1.6.0"
SECTIONS = ["banner", "smart", "stance", "cohort-ts-sec", "sectors", "industries", "rotation", "contenders", "boards", "mgr-table", "drill", "conc", "diversified", "clone",
            "options", "signals", "master-lists", "evidence-desk", "limitations", "engines", "season", "verdicts"]
DRILL = ["REPORTED POSITIONS · TOP OF BOOK", "NEWLY PRESENT", "ABSENT", "QUANTITY UP", "QUANTITY DOWN", "ASSET MIX", "CLONE-ALPHA", "FILING CHAIN", "Reported equity value", "Effective accession"]


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


def money(v):
    """The page's own money() for a billion and up (below that the checks do not need it)."""
    a, s = abs(v), "−" if v < 0 else ""
    if a >= 1e11:
        return "%s$%sB" % (s, "{:,.1f}".format(a / 1e9))
    if a >= 1e9:
        return "%s$%.2fB" % (s, a / 1e9)
    return None


MGR = """() => { const t = document.getElementById('tearsheet'), fk = t.dataset.fund, q = s => t.querySelector(s);
  const row = [...document.querySelectorAll('#mgr-table tbody tr')].find(r => r.querySelector('button').dataset.fund === fk);
  return { fund: fk, head: q('.ts-h').textContent, tiles: [...t.querySelectorAll('.ts-score .kpi')].map(e => e.innerText.replace(/\\s+/g, ' ').trim()), units: (q('.ts-units') || {}).innerText || null,
    chips: [...document.querySelectorAll('#drill-body .chips .c')].map(e => e.textContent), row: [...row.cells].map(c => c.innerText.replace(/\\s+/g, ' ').trim()),
    cover: [...t.querySelectorAll('p')].map(e => e.innerText).filter(x => /cover page/.test(x)).join(' '), lines: [...t.querySelectorAll('#ts-pos tbody tr')].slice(0, 4).map(r => r.innerText.replace(/\\s+/g, ' ').trim()),
    bad: (t.innerText.match(/undefined|NaN|\\[object/g) || []).length, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""

SUCC = """() => { const box = document.getElementById('succ-box'), trs = [...document.querySelectorAll('#succ-table tbody tr')];
  const row = t => { const tr = trs.find(r => (r.querySelector('.tk') || {}).textContent === t); return tr ? [...tr.cells].map(c => c.innerText.replace(/\\s+/g, ' ').trim()) : null; };
  return { hidden: box.hidden, h: (box.querySelector('h3') || {}).innerText || null, n: trs.length, xom: row('XOM'), xpro: row('XPRO'), nfe: row('NFE'), fwonk: row('FWONK'),
    labels: [...new Set([...document.querySelectorAll('#succ-table .succ')].map(c => c.textContent))], title: (trs[0] && trs[0].querySelector('.succ') || {}).title || null,
    blurb: (box.querySelector('.blurb') || {}).innerText || null, note: (document.getElementById('succ-note') || {}).innerText || null,
    bad: (box.innerText.match(/undefined|NaN|\\[object|null/g) || []).length, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


def main():
    with report("ops_6825_13f_units_succession_page_probe") as r:
        r.heading("ops 6825 - Baupost's value unit + CUSIP successions on 13f.html, live page proof")
        stamp = int(time.time())
        ua = {"User-Agent": "Mozilla/5.0 jh-ops"}
        html = urllib.request.urlopen(urllib.request.Request("%s/13f.html?nocache=%d" % (BASE, stamp), headers=ua), timeout=30).read().decode("utf-8", "replace")
        book = json.loads(urllib.request.urlopen(urllib.request.Request("%s/data/13f-book.json?nocache=%d" % (BASE, stamp), headers=ua), timeout=30).read())
        bk = book["book"]
        M, S = bk["managers"], bk.get("successions") or {}
        r.log("13f.html: %d bytes, new code %s | book %s generated %s, successions %s" % (len(html), "succTable" in html and "unitsNote" in html, book.get("version"), book.get("generated_at"),
                                                                                    {k: len(v) for k, v in S.items()}))
        if "succTable" not in html or "unitsNote" not in html:
            r.fail("the served 13f.html is not the new page yet (Pages deploy pending?)")
            raise SystemExit(1)
        if book.get("version") != WANT or not S.get("cohort"):
            r.fail("the served book is not %s (version %s)" % (WANT, book.get("version")))
            raise SystemExit(1)
        bp = M["BAUPOST"]
        want_value = money(bp["tearsheet"]["value_usd"])
        cover_filed = "{:,}".format(int(float(bp["tearsheet"]["source"]["current"]["reported_value_sum"])))
        rows = S["cohort"]
        xom = next(z for z in rows if z["ticker"] == "XOM")
        r.log("book says: Baupost 13F value %s (units %s / %s, cover filed %s x%s) | cohort successions %d, XOM %s -> %s holders %d <- %d, up %d / down %d, value %s <- %s" % (
            want_value, bp["units"]["current"]["status"], bp["units"]["prior"]["status"], cover_filed, bp["tearsheet"]["source"]["cover_scale"], len(rows), xom["succession"]["from_cusip"], xom["cusip"],
            xom["n_holders"], xom["n_prior_holders"], xom["n_inc"], xom["n_dec"], money(xom["value_usd"]), money(xom["value_prior_usd"])))

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
                ck(page.locator("#mgr-table thead th").count() == 18 and page.locator("#mgr-table tbody tr").count() == len(M), "%d: the managers grid changed shape" % width)
                drill = page.locator("#drill-body").inner_text().lower()
                ck(not [x for x in DRILL if x.lower() not in drill], "%d: the drill-down lost a block" % width)
                r.log("sections %d present, %d missing, %d empty | grid %d columns x %d managers" % (len(SECTIONS) - len(missing), len(missing), len(empty), page.locator("#mgr-table thead th").count(),
                                                                                                 page.locator("#mgr-table tbody tr").count()))

                # 2. Baupost - clicked from the grid, like a reader would
                page.locator('#mgr-table button[data-fund="BAUPOST"]').click()
                page.wait_for_function("() => { const t = document.getElementById('tearsheet'); return t && t.dataset.fund === 'BAUPOST'; }", timeout=15000)
                page.wait_for_timeout(500)
                b = page.evaluate(MGR)
                r.log("**Baupost after the click**: %s" % b["head"])
                r.log("    tiles: %s" % " | ".join(b["tiles"][:3]))
                r.log("    chips: %s" % b["chips"])
                r.log("    note: %s" % b["units"])
                r.log("    grid row: %s" % b["row"][:6])
                r.log("    cover line: ...%s" % b["cover"][b["cover"].find("The lines add up"):][:120])
                r.log("    top lines: %s" % b["lines"])
                ck(want_value and want_value in b["head"] and want_value in " ".join(b["tiles"]), "%d: Baupost's tearsheet does not show %s: %s" % (width, want_value, b["tiles"][:1]))
                ck(any("READ ×1,000" in c for c in b["chips"]), "%d: Baupost has no value-unit chip: %s" % (width, b["chips"]))
                ck(b["units"] and "thousands of dollars" in b["units"] and "read ×1,000" in b["units"], "%d: Baupost's value-unit note: %s" % (width, b["units"]))
                ck(("filed as %s in thousands" % cover_filed) in b["cover"], "%d: the cover line does not say how it was filed" % width)
                ck(want_value in " ".join(b["row"]), "%d: Baupost's grid row does not show %s: %s" % (width, want_value, b["row"][:6]))
                foot = page.locator("#mgr-foot").inner_text()
                ck("Baupost" in foot and "read ×1,000" in foot, "%d: the grid footnote lacks the value-unit note" % width)
                ck(b["bad"] == 0 and b["ox"] <= 0, "%d: Baupost tearsheet bad literals %s overflow %s" % (width, b["bad"], b["ox"]))
                if width == 1440:
                    box = page.locator("#tearsheet").bounding_box()
                    shots["baupost_desktop"] = page.screenshot(type="jpeg", quality=62, full_page=True, clip={"x": box["x"], "y": box["y"] + page.evaluate("window.scrollY"), "width": box["width"], "height": min(box["height"], 760)})
                else:
                    box = page.locator("#tearsheet").bounding_box()
                    shots["baupost_mobile"] = page.screenshot(type="jpeg", quality=58, full_page=True, clip={"x": 0, "y": box["y"] + page.evaluate("window.scrollY") - 4, "width": 390, "height": 1150})
                # ... and every other manager: the book's value, no unit note
                shown = []
                for fk, x in M.items():
                    if fk == "BAUPOST":
                        continue
                    page.evaluate("fk => document.querySelector('#mgr-table button[data-fund=\"' + fk + '\"]').click()", fk)
                    page.wait_for_function("fk => { const t = document.getElementById('tearsheet'); return t && t.dataset.fund === fk; }", arg=fk, timeout=15000)
                    o = page.evaluate(MGR)
                    want = money(x["tearsheet"]["value_usd"])
                    ck(o["units"] is None and not any("×1,000" in c or "UNVERIFIED" in c for c in o["chips"]), "%d: %s wears a value-unit note or chip" % (width, fk))
                    ck(want is None or want in o["tiles"][0], "%d: %s tearsheet shows %s, the book says %s" % (width, fk, o["tiles"][0], want))
                    ck(o["bad"] == 0 and o["ox"] <= 0, "%d: %s bad literals %s overflow %s" % (width, fk, o["bad"], o["ox"]))
                    shown.append("%s %s" % (fk, o["tiles"][0].split(" ")[2] if len(o["tiles"][0].split(" ")) > 2 else "?"))
                r.log("the other %d managers (13F value shown, none with a unit note): %s" % (len(shown), " · ".join(shown)))

                # 3. the evidence desk keeps the table as filed and says so
                try:
                    page.wait_for_function("() => document.querySelectorAll('#selection option').length > 3", timeout=30000)
                    page.select_option("#selection", "BAUPOST")
                    page.wait_for_function("() => { const e = document.getElementById('evid-units'); return e && !e.hidden && document.getElementById('fund-summary').innerText.trim().length > 0; }", timeout=30000)
                    note = page.locator("#evid-units").inner_text()
                    first = page.evaluate("() => { const tr = document.querySelector('#positions tbody tr'); return tr ? tr.innerText.replace(/\\s+/g, ' ').trim().slice(0, 170) : null; }")
                    r.log("evidence desk, Baupost selected: %s" % note)
                    r.log("    its first row as filed: %s" % first)
                    ck("thousands of dollars" in note, "%d: the evidence desk note: %s" % (width, note))
                    other = next(fk for fk in bk["cohort"] if fk != "BAUPOST")
                    page.select_option("#selection", other)
                    page.wait_for_function("() => document.getElementById('evid-units').hidden", timeout=30000)
                    r.log("evidence desk, %s selected: note hidden %s" % (other, page.evaluate("() => document.getElementById('evid-units').hidden")))
                except Exception as e:  # noqa: BLE001
                    ck(False, "%d: the evidence desk note did not appear / hide: %s" % (width, str(e)[:160]))

                # 4. one security, new CUSIP or class text
                page.click("#scope-cohort")
                page.click("#univ-all")
                page.wait_for_timeout(400)
                s = page.evaluate(SUCC)
                r.log("**%s** | rows %d (book %d) | chips %s" % (s["h"], s["n"], len(rows), s["labels"]))
                for k in ("xom", "xpro", "fwonk", "nfe"):
                    r.log("    %s: %s" % (k.upper(), s[k]))
                r.log("    hover on the first chip: %s" % s["title"])
                r.log("    footnote: %s" % s["note"])
                ck(not s["hidden"] and s["n"] == len(rows), "%d: succession rows %s, the book has %s" % (width, s["n"], len(rows)))
                x = s["xom"] or [""] * 7
                ck(("%s → %s" % (xom["succession"]["from_cusip"], xom["cusip"])) in x[1] and x[2].startswith("%d ← %d" % (xom["n_holders"], xom["n_prior_holders"])) and x[3] == "%d / %d" % (xom["n_inc"], xom["n_dec"])
                   and money(xom["value_usd"]) in x[4] and money(xom["value_prior_usd"]) in x[4] and "CUSIP CHANGED" in x[0], "%d: Exxon's row is not the book's: %s" % (width, s["xom"]))
                ck(s["nfe"] and s["nfe"][3] == "not compared" and "QTY NOT COMPARED" in s["nfe"][0], "%d: a reverse split must not be compared: %s" % (width, s["nfe"]))
                ck(s["fwonk"] and "CLASS TEXT CHANGED" in s["fwonk"][0] and s["fwonk"][6] == "same CUSIP", "%d: a same-CUSIP relabel: %s" % (width, s["fwonk"]))
                ck(s["bad"] == 0 and s["ox"] <= 0, "%d: succession table bad literals %s overflow %s" % (width, s["bad"], s["ox"]))
                ck(s["note"] and str(len(rows)) in s["note"], "%d: the boards footnote does not count the successions" % width)
                if width == 1440:
                    page.locator("#succ-box").scroll_into_view_if_needed()
                    page.wait_for_timeout(300)
                    shots["successions_desktop"] = page.locator("#succ-box").screenshot(type="jpeg", quality=62)
                counts = []
                for sc, un, want in (("concentrated", "all", len(S["concentrated"])), ("cohort", "etfs", 0), ("cohort", "stocks", sum(1 for z in rows if z["kind"] == "stock")), ("cohort", "all", len(rows))):
                    page.click("#scope-conc" if sc == "concentrated" else "#scope-cohort")
                    page.click("#univ-" + un)
                    page.wait_for_timeout(300)
                    got = 0 if page.evaluate("() => document.getElementById('succ-box').hidden") else page.locator("#succ-table tbody tr").count()
                    counts.append("%s/%s %d" % (sc, un, got))
                    ck(got == want, "%d: %s/%s shows %s succession rows, the book has %s" % (width, sc, un, got, want))
                r.log("scope / universe switches: %s" % " · ".join(counts))

                # 5. the whole page
                whole = page.evaluate("() => ({ ox: document.documentElement.scrollWidth - document.documentElement.clientWidth, bad: (document.querySelector('main').innerText.match(/undefined|NaN|\\[object/g) || []).length })")
                ck(whole["ox"] <= 0 and whole["bad"] == 0, "%d: page overflow %s px, bad literals %s" % (width, whole["ox"], whole["bad"]))
                ck(not errors, "%d: page errors %s" % (width, errors[:3]))
                r.log("page: overflow %s px, bad literals %s, page errors %d" % (whole["ox"], whole["bad"], len(errors)))
                ctx.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            r.log("%s: %d bytes" % (k, len(img)))
            r.lines.append("<!-- SHOT_%s_JPEG_BASE64 -->" % k.upper())
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("<!-- /SHOT_%s_JPEG_BASE64 -->" % k.upper())
        r.kv(book=book.get("version"), baupost_shown=want_value, succession_rows=str(len(rows)), widths="1440,390", failures=str(len(fails)))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("LIVE PAGE: Baupost reads %s with its value-unit note at both widths; %d securities under a new CUSIP or class text are one row each, Exxon among them with holders now and before; nothing removed, 0 failures" % (
            want_value, len(rows)))


if __name__ == "__main__":
    main()
    sys.exit(0)
