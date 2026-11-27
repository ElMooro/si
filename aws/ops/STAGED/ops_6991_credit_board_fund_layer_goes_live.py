"""ops 6991 - Credit Before Equity, held by funds: the engine's first publication of the fund layer, then a real browser on the live site.

Khalid 2026-11-26 20:32 ET: "go" - Credit Before Equity first: who holds each bond, how far apart funds mark it, and how
funds are positioned in each company's credit default swaps.

Parts, each with arithmetic written here and not the engine's:

  1. Guard: the deployed function is this checkout's (cbe_funds.py among its files); what is published now.
  2. One dry run inside Lambda: it reports the fund layer and writes nothing.
  3. One publication: a one-off run outside the 22:35 UTC schedule (disclosed; the engine runs once a day). Skipped when the
     live document already carries this fund layer, so the op is safe to run again without a second run of the engine.
  4. Read back: the board is what it was (same companies, same spreads, its 69 columns where they were, 13 after them); every
     company's fund columns are its own rows of the fund document added up here; every row of the fund document is a row of
     the SEC bank's table; every company's swap figures are its names added up; a dealer's name enters no company; the lists
     are whole; nothing is shaped like an identifier; the site serves both documents.
  5. The page in a real browser on justhodl.ai (after the site has deployed): the five tiles are the document's; the fund
     document arrives when the section is reached; protection sold and bought is drawn for the right companies in the
     palette's colours; the widest gaps are drawn; every list opens whole and sorts both ways; the size choice narrows the
     gaps; Oracle's and CoreWeave's sheets carry what funds hold of them, sorted and saved as a file; the company table shows
     the fund columns and sorts by one; the whole-document view lists the fund key and file; a phone.

Safe to run again.
"""
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import base64
import gzip
import hashlib
import importlib.util
import io
import json
import re
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source"
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared")]
NAME = "ops_6991_credit_board_fund_layer_goes_live"
FN = "justhodl-credit-before-equity"
LIVE = "justhodl-dashboard-live"
BASE = "https://justhodl.ai"
OUT_KEY, FUNDS_KEY, HEAD_KEY = "data/credit-before-equity.json", "data/credit-before-equity-funds.json", "data/sec-fund-holdings.json"
FILES = ("lambda_function.py", "cbe_core.py", "cbe_names.py", "cbe_themes.py", "cbe_study.py", "cbe_funds.py", "leaders_tape.py")
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
FUND_COLS = ["f_n", "f_funds", "f_face", "f_value", "f_px", "f_gap", "f_def", "f_pik", "f_loans", "fc_sold", "fc_bought", "fc_net", "fc_funds"]
VERSION_TAG = "20261127a"
MINUS = "−"
PALETTE = {"bad": (242, 54, 69), "good": (8, 153, 129)}          # the page's first palette (red and green), as the browser computes it


def fx(v, d):
    """A figure as the page's own toFixed writes it (half away from zero on the double's exact value)."""
    q = Decimal(v).quantize(Decimal(1).scaleb(-d), rounding=ROUND_HALF_UP)
    return "%.*f" % (d, q if q != 0 else 0)


def usd(v, signed=False):
    if v is None:
        return "–"
    a = abs(v)
    s = fx(a / 1e12, 2) + "T" if a >= 1e12 else fx(a / 1e9, 1) + "B" if a >= 1e9 else fx(a / 1e6, 0) + "M" if a >= 1e6 else fx(a / 1e3, 0) + "K" if a >= 1e3 else fx(a, 0)
    return (MINUS if v < 0 else "+" if signed and v > 0 else "") + "$" + s


def n0(v):
    return format(int(round(v)), ",")


def load(pat):
    spec = importlib.util.spec_from_file_location(pat, str(sorted(Path(__file__).resolve().parent.glob(pat + "_*.py"))[0]))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


TILES = r"""() => Array.from(document.querySelectorAll('#funds .cb-tile-h')).map(e => [e.querySelector('.cb-lbl').innerText.trim(), e.querySelector('.cb-big').innerText.trim(), e.querySelector('.cb-sub').innerText.trim()])"""
ROWS = r"""(sel) => Array.from(document.querySelectorAll(sel + ' tbody tr')).map(tr => Array.from(tr.children).map(td => td.innerText.trim()))"""
HEAD = r"""(sel) => Array.from(document.querySelectorAll(sel + ' thead th')).map(th => th.innerText.trim())"""
BARS = r"""() => Array.from(document.querySelectorAll('#funds .cb-fu-bf')).map(b => { const i = b.querySelectorAll('i'); return { id: b.dataset.sheet, name: b.querySelector('.cb-fu-n').innerText.trim(),
  bought: getComputedStyle(i[0]).backgroundColor, sold: getComputedStyle(i[1]).backgroundColor, wb: i[0].getBoundingClientRect().width, ws: i[1].getBoundingClientRect().width,
  texts: Array.from(b.querySelectorAll('.cb-fu-v')).map(v => v.innerText.trim()) }; })"""
RANGES = r"""(root) => Array.from(document.querySelectorAll(root + ' .cb-fu-rg')).map(b => { const t = b.querySelector('.cb-fu-range').getBoundingClientRect(), i = b.querySelector('.cb-fu-range i').getBoundingClientRect(),
  d = b.querySelector('.cb-fu-range b').getBoundingClientRect(); return { id: b.dataset.sheet || null, name: b.querySelector('.cb-fu-n').innerText.trim(), gap: b.querySelector('.cb-fu-v').innerText.trim(),
  left: (i.left - t.left) / t.width, width: i.width / t.width, dot: (d.left + d.width / 2 - t.left) / t.width, dot_px: d.width }; })"""
KPIS = r"""() => { const h = document.querySelector('#sheet #sheet-funds'); if (!h) return null; const out = {}; let e = h.nextElementSibling;
  while (e && e.tagName !== 'H4') { if (e.classList.contains('cb-kpis')) Array.from(e.children).forEach(k => { out[k.querySelector('.cb-lbl').innerText.trim()] = [k.querySelector('.cb-big').innerText.trim(), k.querySelector('.cb-sub').innerText.trim()]; }); e = e.nextElementSibling; }
  return out; }"""
BETWEEN = r"""(ids) => { const a = document.querySelector(ids[0]); if (!a) return null; let e = a.nextElementSibling, t = ''; while (e && !(ids[1] && e.matches(ids[1])) && e.tagName !== 'H4') { t += ' ' + e.innerText; e = e.nextElementSibling; } return t.replace(/\s+/g, ' ').trim(); }"""
OVERFLOW = r"""() => { const w = document.documentElement.clientWidth; return { doc: document.documentElement.scrollWidth - w,
  wide: Array.from(document.querySelectorAll('#s-funds *')).filter(e => { const r = e.getBoundingClientRect(); return r.width > 0 && r.right > w + 1 && !e.closest('.cb-scroll') && !e.closest('.cb-seg'); }).slice(0, 5).map(e => e.className) }; }"""
BAD = r"""() => (document.getElementById('funds').innerText.match(/\bNaN\b|\bundefined\b|\[object/g) || []).length"""


def rgb(text):
    m = re.match(r"rgba?\((\d+),\s*(\d+),\s*(\d+)", text or "")
    return tuple(int(x) for x in m.groups()) if m else None


def browse(new_page, url, doc, fdoc, ck, shots, log):
    block, cds = doc["funds"], doc["funds"]["cds"]
    cols = doc["issuers"]["cols"]
    table = [dict(zip(cols, r)) for r in doc["issuers"]["rows"]]
    by_id = {x["id"]: x for x in table}
    frows = [dict(zip(fdoc["cols"], r)) for r in fdoc["rows"]]
    for x in frows:
        x["gap"] = round(x["px_hi"] - x["px_lo"], 2) if x["px_hi"] is not None and x["px_lo"] is not None and (x["n_px"] or 0) >= fdoc["min_marks"] else None
    gaps = [x for x in frows if x["gap"] is not None]
    page, errors = new_page()
    seen = []
    page.on("response", lambda resp: seen.append((resp.url.split("?")[0].rsplit("/", 1)[-1], resp.status)) if "credit-before-equity" in resp.url and resp.url.split("?")[0].endswith(".json") else None)
    page.goto(url, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_selector("#funds .cb-tile-h", timeout=120000)
    scripts = page.evaluate("() => Array.from(document.scripts).map(s => s.getAttribute('src')).filter(s => s && s.indexOf('jh-credit-') >= 0)")
    log("scripts on the page: %s" % scripts)
    ck(scripts == ["/jh-credit-board.js?v=" + VERSION_TAG, "/jh-credit-sheet.js?v=" + VERSION_TAG, "/jh-credit-funds.js?v=" + VERSION_TAG], "the page loads the board, the sheet and the fund script of %s, in that order" % VERSION_TAG)
    served = page.evaluate("() => fetch('https://justhodl.ai/data/credit-before-equity.json?t=' + Date.now()).then(r => r.json()).then(d => [d.generated_at, d.funds && d.funds.n])")
    ck(served == [doc["generated_at"], block["n"]], "the document the browser is served is the one read back from the bucket (%s)" % served)

    # --- the five tiles
    tiles = page.evaluate(TILES)
    for t in tiles:
        log("  tile: %s | %s | %s" % tuple(t))
    tied = block["tied"]
    five = sum(1 for x in gaps if x["gap"] >= 5)
    flagged = [dict(zip(block["flag_cols"], f)) for f in block["flagged"]]
    n_def = sum(1 for f in flagged if f["n_default"] and f["n_default"] * 2 >= f["rows"])
    n_pik = sum(1 for f in flagged if f["n_pik"] and f["n_pik"] * 2 >= f["rows"])
    want = [("Bonds and loans tied to a company", n0(tied["securities"]), usd(tied["value"])), ("Companies on the board funds hold", n0(tied["companies"]), "%s with a listed stock" % n0(tied["listed"])),
            ("Marked 5 points or more apart", None, "widest:"), ("Flagged by most of their holders", n0(block["n_flagged"]), "%d in default · %d paid in kind" % (n_def, n_pik)),
            ("Credit default swaps funds hold", usd(cds["totals"]["notional_usd"]), "%s names, %s tied to a company" % (n0(cds["totals"]["reference_names"]), n0(cds["tied"])))]
    ck(len(tiles) == 5 and all(t[0] == w[0] and (w[1] is None or t[1] == w[1]) and w[2] in t[2] for t, w in zip(tiles, want)), "the five tiles are the document's figures")
    dates = page.inner_text("#funds .cb-dates")
    ck(block["file"].replace(".zip", "") in dates and (n0(block["quarter"]["summary"]["funds"]) + " funds") in dates, "the dates line names the SEC file and its funds: %s" % dates[:200])

    # --- the fund document arrives when the section is reached
    before = [s for s in seen if s[0] == "credit-before-equity-funds.json"]
    page.locator("#s-funds").scroll_into_view_if_needed()
    page.wait_for_function("() => { const b = document.querySelector('#funds [data-fu-view=\"gaps\"]'); return b && !document.querySelector('#funds [data-fu-load]') && /\\(/.test(b.innerText); }", timeout=120000)
    page.wait_for_timeout(600)
    got = [s for s in seen if s[0] == "credit-before-equity-funds.json"]
    log("requests for the fund document: before the section was reached %d, after %d %s" % (len(before), len(got), got[:2]))
    ck(len(got) == 1 and got[0][1] == 200, "the fund document is asked for once, when its section is reached, and arrives")
    label = page.inner_text('#funds [data-fu-view="gaps"]')
    ck(label == "Marked apart (%s)" % n0(len(gaps)) and len(gaps) == block["n_gaps"], "marked apart counts every security %d funds or more mark: %s; worked here %d, the block says %d" % (fdoc["min_marks"], label, len(gaps), block["n_gaps"]))
    tiles = page.evaluate(TILES)
    ck(tiles[2][1] == n0(five), "securities marked 5 points or more apart: the page says %s, worked here from the fund document %d" % (tiles[2][1], five))
    ck(page.evaluate(BAD) == 0, "nothing unreadable (NaN, undefined) is drawn in the section")
    shots["1-held-by-funds"] = page.locator("#s-funds").screenshot(type="jpeg", quality=70)

    # --- protection sold and bought
    gross = lambda x: (x["fc_sold"] or 0) + (x["fc_bought"] or 0)   # noqa: E731
    top = sorted((x for x in table if gross(x) > 0), key=lambda x: -gross(x))[:14]
    bars = page.evaluate(BARS)
    log("protection sold and bought: %s" % "; ".join("%s %s" % (b["name"], "/".join(t for t in b["texts"] if t)) for b in bars))
    ck([b["id"] for b in bars] == [x["id"] for x in top], "the fourteen companies with the most protection written, in order")
    ck(all(rgb(b["bought"]) == PALETTE["bad"] and rgb(b["sold"]) == PALETTE["good"] for b in bars), "bought is drawn in the colour of trouble and sold in the colour of safety (%s, %s)" % (bars[0]["bought"], bars[0]["sold"]))
    biggest = max(max(x["fc_sold"] or 0, x["fc_bought"] or 0) for x in top)
    ok_w = True
    full = max(max(b["ws"], b["wb"]) for b in bars)
    for b, x in zip(bars, top):
        if abs(b["ws"] / full - (x["fc_sold"] or 0) / biggest) > 0.02 or abs(b["wb"] / full - (x["fc_bought"] or 0) / biggest) > 0.02:
            ok_w = False
    ck(ok_w, "each bar is as long as its dollars against the largest")
    ck(all([t for t in b["texts"]] == [usd(x["fc_bought"]) if x["fc_bought"] else "", usd(x["fc_sold"]) if x["fc_sold"] else ""] for b, x in zip(bars, top)), "each bar is labelled with its dollars")

    # --- the widest gaps
    widest = sorted(gaps, key=lambda x: (-x["gap"], -(x["value"] or 0)))[:12]
    rng = page.evaluate(RANGES, "#funds")
    log("widest gaps: %s" % "; ".join("%s %s" % (g["name"].split("\n")[0][:22], g["gap"]) for g in rng))
    ck([g["id"] for g in rng] == [x["co"] for x in widest] and [g["gap"] for g in rng] == [fx(x["gap"], 2) for x in widest], "the twelve securities funds mark furthest apart, widest first")
    span = max(max(abs(x["px_lo"] - x["px"]), abs(x["px_hi"] - x["px"])) for x in widest)
    ck(all(abs(g["dot"] - 0.5) < 0.02 and g["dot_px"] >= 8 and abs(g["left"] - (0.5 + 0.5 * (x["px_lo"] - x["px"]) / span)) < 0.02 for g, x in zip(rng, widest)), "each range starts at its 10th percentile mark and its dot sits on the middle mark")

    # --- a list sorts both ways on a column
    T = "#funds table.cb-fu-t"
    page.click('#funds button[data-fu-sort="gaps:value"]')
    first = page.evaluate(ROWS, T)
    page.click('#funds button[data-fu-sort="gaps:value"]')
    second = page.evaluate(ROWS, T)
    vmax, vmin = max(x["value"] for x in gaps), min(x["value"] for x in gaps)
    log("marked apart by value held: largest first %s, then smallest first %s" % (first[0][-1], second[0][-1]))
    ck(first[0][-1] == usd(vmax) and second[0][-1] == usd(vmin) and len(first) == 25, "the list sorts by value held both ways: %s then %s (worked here %s and %s)" % (first[0][-1], second[0][-1], usd(vmax), usd(vmin)))
    ck(page.evaluate("() => document.activeElement && document.activeElement.getAttribute('data-fu-sort')") == "gaps:value", "the header that was clicked keeps the focus")
    page.click('#funds button[data-fu-sort="gaps:gap"]')
    ck(page.evaluate(ROWS, T)[0][9] == fx(widest[0]["gap"], 2), "sorted by the gap, the widest comes first")
    page.click('#funds [data-fu-held="250000000"]')
    big = [x for x in gaps if x["value"] >= 250e6]
    note = page.inner_text("#funds .cb-scroll ~ .cb-note")
    ck("of %s shown (%s before the choice above)" % (n0(len(big)), n0(len(gaps))) in note, "worth $250M or more: the page lists %s, worked here %d" % (note[:60], len(big)))
    page.click('#funds [data-fu-held="0"]')

    # --- every list opens whole
    sizes = {"flagged": block["n_flagged"], "names": len(cds["names"]), "indices": len(cds["indices"]), "dealers": len(cds["counterparties"])}
    for view, n in sizes.items():
        page.click('#funds [data-fu-view="%s"]' % view)
        if page.locator('#funds [data-fu-all="%s"]' % view).count():
            page.click('#funds [data-fu-all="%s"]' % view)
        rows = page.evaluate(ROWS, T)
        ck(len(rows) == n, "%s opens whole: %d rows on the page, %d in the document" % (view, len(rows), n))
        if view == "names":
            at = {c: i for i, c in enumerate(cds["cols"])}
            ref = sorted(cds["names"], key=lambda x: -((x[at["sold"]] or 0) + (x[at["bought"]] or 0) + (x[at["unclear"]] or 0)))[0]
            ck(rows[0][0] == ref[0], "swap names open with the most written: %s" % rows[0][0])
            dealer = [x for x in cds["names"] if x[at["dealer"]]]
            said = sum(1 for x in rows if x[1] == "also a dealer's name: tied to none")
            ck(said == len(dealer) and all(x[at["co"]] is None for x in dealer), "%d reference names are also dealers' names: each says so and is tied to no company" % said)
            shots["2-swap-names"] = page.screenshot(type="jpeg", quality=70)
            page.click("#funds [data-fu-tied]")
            ck(len(page.evaluate(ROWS, T)) == cds["tied"], "the choice 'only names tied to a company' leaves %d" % cds["tied"])
            page.click("#funds [data-fu-tied]")
    page.click('#funds [data-fu-view="ties"]')
    ck(page.locator(T).count() == 3, "how names were tied shows its three lists")
    page.click('#funds [data-fu-all="ties_by_rule"]')
    n_rule = page.evaluate("() => document.querySelectorAll('#funds table.cb-fu-t')[0].querySelectorAll('tbody tr').length")
    page.click('#funds [data-fu-all="not_on_board"]')
    n_abs = page.evaluate("() => document.querySelectorAll('#funds table.cb-fu-t')[1].querySelectorAll('tbody tr').length")
    page.click('#funds [data-fu-all="untied"]')
    n_unt = page.evaluate("() => document.querySelectorAll('#funds table.cb-fu-t')[2].querySelectorAll('tbody tr').length")
    ck((n_rule, n_abs, n_unt) == (block["n_ties_by_rule"], block["n_not_on_board"], block["n_untied"]), "the three lists open whole: %d names tied by a rule, %d listed companies not on the board, %d issuers left untied (the document: %d, %d, %d)" % (
        n_rule, n_abs, n_unt, block["n_ties_by_rule"], block["n_not_on_board"], block["n_untied"]))
    shots["3-how-names-were-tied"] = page.screenshot(type="jpeg", quality=70)
    page.click('#funds [data-fu-view="gaps"]')

    # --- a company's sheet, opened from the picture: Oracle
    def sheet_checks(cid, tag):
        x, mine = by_id[cid], [f for f in frows if f["co"] == cid]
        page.wait_for_selector("#sheet #sheet-funds", timeout=30000)
        page.wait_for_function("() => document.querySelector('#sheet table.cb-bonds th button[data-fund-sort]')", timeout=60000)
        k = page.evaluate(KPIS)
        log("  %s: %s" % (cid, json.dumps(k)[:420]))
        ck(k["Bonds and loans funds hold"][0] == n0(x["f_n"]) and "%s in the most widely held" % ("1 fund" if x["f_funds"] == 1 else n0(x["f_funds"]) + " funds") in k["Bonds and loans funds hold"][1], "%s: funds hold %s of its bonds and loans" % (tag, n0(x["f_n"])))
        ck(k["Held, at funds' own marks"] == [usd(x["f_value"]), "face " + usd(x["f_face"])] and k["Middle mark"][0] == fx(x["f_px"], 2), "%s: %s held at a middle mark of %s" % (tag, usd(x["f_value"]), fx(x["f_px"], 2)))
        ck(k["Widest gap between marks"][0] == (fx(x["f_gap"], 2) + " pts" if x["f_gap"] is not None else "–"), "%s: its widest gap between marks is %s" % (tag, x["f_gap"]))
        sel = "#sheet table.cb-bonds:has(button[data-fund-sort])"
        rows = page.evaluate(ROWS, sel)
        head = page.evaluate(HEAD, sel)
        vi, fi = next(i for i, h in enumerate(head) if h.startswith("VALUE") or h.startswith("Value")), next(i for i, h in enumerate(head) if h.upper().startswith("FUNDS HOLDING"))
        ck(len(rows) == len(mine) == x["f_n"] and [r[vi] for r in rows] == [usd(f["value"]) for f in sorted(mine, key=lambda f: -f["value"])], "%s: every one of its %d fund-held securities is listed, largest first" % (tag, len(mine)))
        page.click('#sheet button[data-fund-sort="funds"]')
        desc = [int(r[fi].replace(",", "")) for r in page.evaluate(ROWS, sel)]
        page.click('#sheet button[data-fund-sort="funds"]')
        asc = [int(r[fi].replace(",", "")) for r in page.evaluate(ROWS, sel)]
        ck(desc == sorted(desc, reverse=True) and asc == sorted(asc) and desc[0] == x["f_funds"], "%s: its list sorts by funds holding both ways (%d at most)" % (tag, desc[0]))
        with page.expect_download(timeout=30000) as dl:
            page.click('#sheet button[data-csv="funds"]')
        saved = Path(dl.value.path()).read_bytes().decode("utf-8").strip().split("\r\n")          # bytes: reading as text would turn the file's own line ends into others
        ck(len(saved) == len(mine) + 1 and saved[0].startswith("co,title,issuer,cls,cpn,mat,funds,rows,face,value,px,px_lo,px_hi,gap"), "%s: its list saves as a file of %d rows (%s)" % (tag, len(saved) - 1, dl.value.suggested_filename))
        rr = page.evaluate(RANGES, "#sheet")
        marked = sorted((f for f in mine if f["gap"] is not None), key=lambda f: -f["value"])[:10]
        ck(len(rr) == len(marked) and [g["gap"] for g in rr] == [fx(f["gap"], 2) for f in marked], "%s: the picture of its marks draws its %d largest securities enough funds mark" % (tag, len(marked)))
        return x, mine

    page.click('#funds .cb-fu-bf[data-sheet="T:ORCL"]')
    orcl, _ = sheet_checks("T:ORCL", "Oracle")
    words = page.evaluate(BETWEEN, ["#sheet #sheet-fund-swaps", None])
    log("  Oracle, the swaps funds hold: %s" % words[:300])
    ck("Funds have sold %s of protection on it and bought %s" % (usd(orcl["fc_sold"]), usd(orcl["fc_bought"])) in words and "on balance they carry its credit risk" in words, "Oracle: funds sold %s of protection and bought %s" % (usd(orcl["fc_sold"]), usd(orcl["fc_bought"])))
    page.locator("#sheet #sheet-funds").scroll_into_view_if_needed()
    shots["4-oracle-held-by-funds"] = page.screenshot(type="jpeg", quality=70)
    order = page.evaluate("() => Array.from(document.querySelectorAll('#sheet h4')).map(h => h.id)")
    ck([i for i in order if i in ("sheet-bonds", "sheet-funds", "sheet-fund-swaps", "sheet-figures")] == ["sheet-bonds", "sheet-funds", "sheet-fund-swaps", "sheet-figures"], "what funds hold sits after its bonds and before every figure: %s" % order)
    page.keyboard.press("Escape")

    # --- CoreWeave, found through the company table's own search
    page.locator("#s-companies").scroll_into_view_if_needed()
    page.fill("input[data-q]", "CRWV")
    page.wait_for_selector('#table button.cb-name[data-sheet="T:CRWV"]', timeout=30000)
    page.click('#table button.cb-name[data-sheet="T:CRWV"]')
    crwv, mine = sheet_checks("T:CRWV", "CoreWeave")
    loans = sum(1 for f in mine if f["cls"] == "LON")
    ck(loans == crwv["f_loans"] and (loans == 0 or "%d loan" % loans in page.evaluate(KPIS)["Bonds and loans funds hold"][1]), "CoreWeave: %d of them are loans" % loans)
    page.locator("#sheet #sheet-funds").scroll_into_view_if_needed()
    shots["5-coreweave-held-by-funds"] = page.screenshot(type="jpeg", quality=70)
    page.keyboard.press("Escape")
    page.fill("input[data-q]", "")
    page.wait_for_timeout(500)

    # --- the fund columns in the company table
    page.click('#table [data-cols="all"]')
    head = page.evaluate(HEAD, "#table table")
    need = ["FUND-HELD BONDS AND LOANS", "FUND-HELD VALUE", "FUNDS' MIDDLE MARK", "WIDEST GAP BETWEEN MARKS", "FUND PROTECTION SOLD", "FUND PROTECTION, NET SOLD"]
    up = [h.upper().replace(" ▲", "").replace(" ▼", "") for h in head]
    ck(len(head) == len(cols) and all(n in up for n in need), "Every column shows the %d published columns, the fund columns among them" % len(head))
    page.click('#table th button[data-sort="f_value"]')
    at_v = up.index("FUND-HELD VALUE")
    first = page.evaluate(ROWS, "#table table")[0]
    most = max((x for x in table if x["f_value"] is not None), key=lambda x: x["f_value"])
    log("the company table by fund-held value: %s %s (worked here: %s %s)" % (first[0][:40].replace("\n", " "), first[at_v], most["id"], usd(most["f_value"])))
    ck(first[at_v] == usd(most["f_value"]) and first[0] == most["id"], "sorted by fund-held value, the company table opens with %s at %s" % (most["ticker"] or most["name"], usd(most["f_value"])))
    shots["6-company-table-fund-columns"] = page.screenshot(type="jpeg", quality=70)
    page.click('#table [data-cols="std"]')

    # --- the whole-document view
    data = page.inner_html("#data")
    ck('href="https://justhodl.ai/data/credit-before-equity-funds.json"' in data and 'data-jh-title="funds"' in data, "the whole-document view lists the key `funds` and links the fund document")
    ck(not errors, "no script error on the page: %s" % errors[:3])

    # --- a phone
    ph, perr = new_page(True)
    ph.goto(url, wait_until="domcontentloaded", timeout=120000)
    ph.wait_for_selector("#funds .cb-tile-h", timeout=120000)
    ph.locator("#s-funds").scroll_into_view_if_needed()
    ph.wait_for_timeout(2500)
    o = ph.evaluate(OVERFLOW)
    log("phone: the page is %d px wider than the screen; elements of the section past the edge: %s" % (o["doc"], o["wide"]))
    ck(o["doc"] <= 1 and not o["wide"] and len(ph.evaluate(BARS)) == 14 and len(ph.evaluate(RANGES, "#funds")) == 12 and not perr, "on a phone the section fits the screen, with both pictures drawn")
    shots["7-phone"] = ph.locator("#s-funds").screenshot(type="jpeg", quality=62)


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=930, connect_timeout=30, retries={"max_attempts": 0}))
    s3 = boto3.client("s3", region_name="us-east-1")
    A = load("ops_6846")
    fails, shots = [], {}

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        raw = o["Body"].read()
        return raw, o

    def parse(raw):
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    def site(path, raw=False):
        req = urllib.request.Request(BASE + "/" + path + ("&" if "?" in path else "?") + "probe=%d" % int(time.time()), headers={"User-Agent": UA, "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read()
            return (body, dict(resp.headers)) if raw else json.loads(body)

    with report(NAME) as r:
        def ck(ok, text):
            (r.ok if ok else r.fail)(text)
            if not ok:
                fails.append(text)

        def tail_of(resp, words):
            try:
                for line in base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace").splitlines():
                    if any(w in line for w in words):
                        r.log("  lambda: %s" % line.strip()[:360])
            except Exception:  # noqa: BLE001
                pass

        r.heading("ops 6991 - Credit Before Equity, held by funds: first publication and live proof, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Guard: the deployed function is this checkout's")
        g = lam.get_function(FunctionName=FN)
        cfg = g["Configuration"]
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {n: (n in z.namelist() and z.read(n) == (SOURCE / n).read_bytes()) for n in FILES}
        r.log("%s updated %s | memory %s MB, timeout %s s | files as in this checkout: %s" % (FN, (cfg.get("LastModified") or "")[:19], cfg.get("MemorySize"), cfg.get("Timeout"), same))
        if not all(same.values()):
            r.fail("the deployed package is not this checkout's; nothing was invoked")
            sys.exit(1)
        deployed_at = datetime.strptime(cfg["LastModified"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        raw0, _ = get(OUT_KEY)
        was = json.loads(raw0)
        was_cols = was["issuers"]["cols"]
        was_rows = {row[0]: dict(zip(was_cols, row)) for row in was["issuers"]["rows"]}
        head = parse(get(HEAD_KEY)[0])
        r.log("before: %s version %s, %d bytes, generated %s, holdings of %s, %d companies, %d columns, fund layer %s" % (
            OUT_KEY, was.get("version"), len(raw0), was.get("generated_at"), was.get("as_of"), len(was_rows), len(was_cols), (was.get("funds") or {}).get("version")))
        r.log("the SEC bank's document: generated %s, as of %s, file %s, set aside %s" % (head.get("generated_at"), head.get("as_of"), (head.get("source") or {}).get("file"), head.get("set_aside")))
        already = isinstance(was.get("funds"), dict) and datetime.fromisoformat(was["generated_at"]) > deployed_at

        if already:
            r.section("2 and 3. Already published by this deployment")
            r.log("the live document was generated %s, after the function was updated %s, and carries the fund layer: the engine is not run again" % (was["generated_at"], cfg["LastModified"][:19]))
            board_before = None
        else:
            r.section("2. One dry run inside Lambda (nothing written)")
            t0 = time.time()
            resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=json.dumps({"dry_run": True}).encode())
            dry = json.loads(resp["Payload"].read() or b"{}")
            r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(dry)[:900]))
            tail_of(resp, ("REPORT", "Error", "Traceback"))
            fund = dry.get("funds") or {}
            if resp.get("FunctionError") or not dry.get("dry_run") or (dry.get("companies") or 0) < 3500 or (fund.get("companies") or 0) < 3000 or (fund.get("cds_names_tied") or 0) < 250:
                r.fail("the dry run inside Lambda failed, or its fund layer reaches too few companies; nothing was published: %s" % json.dumps(dry)[:600])
                sys.exit(1)
            if hashlib.sha256(get(OUT_KEY)[0]).hexdigest() != hashlib.sha256(raw0).hexdigest():
                r.fail("the dry run wrote the document")
                sys.exit(1)
            try:
                s3.head_object(Bucket=LIVE, Key=FUNDS_KEY)
                r.log("a fund document was already there before this publication (an earlier run of this op)")
            except Exception:  # noqa: BLE001
                r.log("the document is what it was and no fund document exists yet: the dry run wrote nothing")

            r.section("3. One publication (a one-off run outside the 22:35 UTC schedule)")
            t0 = time.time()
            resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
            out = json.loads(resp["Payload"].read() or b"{}")
            r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(out)[:500]))
            tail_of(resp, ("REPORT", "Error", "Traceback"))
            if resp.get("FunctionError") or out.get("statusCode") != 200:
                r.fail("the engine did not publish: %s" % json.dumps(out)[:600])
                sys.exit(1)
            board_before = was_rows

        r.section("4. Read back")
        raw, obj = get(OUT_KEY)
        doc = json.loads(raw)
        fraw, fobj = get(FUNDS_KEY)
        fdoc = json.loads(fraw)
        block = doc.get("funds")
        if not isinstance(block, dict):
            r.fail("the published document carries no fund layer: %s" % [d for d in doc.get("degraded") or [] if "fund" in d.lower()][:2])
            sys.exit(1)
        cols = doc["issuers"]["cols"]
        table = [dict(zip(cols, row)) for row in doc["issuers"]["rows"]]
        by_id = {x["id"]: x for x in table}
        r.log("%s: %d bytes (%d gzipped), generated %s, version %s, holdings of %s | %s: %d bytes (%d gzipped), %s rows, written %s" % (
            OUT_KEY, len(raw), len(gzip.compress(raw, 6)), doc["generated_at"], doc["version"], doc["as_of"], FUNDS_KEY, len(fraw), len(gzip.compress(fraw, 6)), format(len(fdoc["rows"]), ","), fobj["LastModified"].strftime("%H:%M:%S")))
        r.log("degraded: %s" % doc.get("degraded"))
        ck(doc["version"] == "3.0.0" and cols[:69] == was_cols[:69] and cols[69:] == FUND_COLS and all(len(row) == 82 for row in doc["issuers"]["rows"]), "the board's 69 columns are where they were and the 13 fund columns follow them")
        ck(not any("fund" in d.lower() for d in doc.get("degraded") or []), "the engine reports nothing wrong with the fund layer")
        if board_before is not None:
            gone = [i for i in board_before if i not in by_id]
            same_day = was.get("as_of") == doc.get("as_of")
            moved = [i for i, w in board_before.items() if i in by_id and same_day and any(w.get(c) != by_id[i].get(c) for c in ("spread_bp", "stress", "book", "industry", "tie", "n_bonds", "value", "signal"))]
            r.log("against the document it replaces (holdings of %s then, %s now): %d companies then, %d now, %d gone, %d with another spread, rank, book, tie or signal" % (was.get("as_of"), doc.get("as_of"), len(board_before), len(by_id), len(gone), len(moved)))
            ck(not gone and (not same_day or not moved), "the board is what it was: the fund layer moved no company's spread, rank, book or signal (%s)" % (moved[:4] or gone[:4] or "none"))
        ck(block["version"] == fdoc["version"] and block["as_of"] == fdoc["as_of"] == head["as_of"] and block["file"] == (head.get("source") or {}).get("file") and block["key"] == FUNDS_KEY, "the fund layer is of the quarter the SEC bank published whole: %s, as of %s" % (block["file"], block["as_of"]))

        # every company's fund columns from its own rows, added up here
        frows = [dict(zip(fdoc["cols"], x)) for x in fdoc["rows"]]
        per = defaultdict(list)
        for x in frows:
            per[x["co"]].append(x)
        wrong = []
        for x in table:
            mine = per.get(x["id"])
            if not mine:
                if x["f_n"] is not None:
                    wrong.append((x["id"], "f_n with no rows"))
                continue
            marked = [m for m in mine if m["px"] is not None and (m["value"] or 0) > 0]
            wide = [m["px_hi"] - m["px_lo"] for m in mine if (m["n_px"] or 0) >= fdoc["min_marks"] and m["px_hi"] is not None and m["px_lo"] is not None]
            most = lambda k: sum(1 for m in mine if m["rows"] and (m[k] or 0) > 0 and (m[k] or 0) * 2 >= m["rows"])   # noqa: E731
            calc = {"f_n": len(mine), "f_funds": max(m["funds"] or 0 for m in mine), "f_face": int(sum(m["face"] or 0 for m in mine)), "f_value": int(sum(m["value"] or 0 for m in mine)),
                    "f_px": round(sum(m["px"] * m["value"] for m in marked) / sum(m["value"] for m in marked), 2) if marked else None, "f_gap": round(max(wide), 2) if wide else None,
                    "f_def": most("n_default"), "f_pik": most("n_pik"), "f_loans": sum(1 for m in mine if m["cls"] == "LON")}
            bad = [k for k, v in calc.items() if x[k] != v and not (isinstance(v, (int, float)) and isinstance(x[k], (int, float)) and abs(x[k] - v) <= (1 if k in ("f_face", "f_value") else 0.011 if k in ("f_px", "f_gap") else 0))]
            if bad:
                wrong.append((x["id"], bad, {k: (x[k], calc[k]) for k in bad}))
        held = [x for x in table if x["f_n"]]
        r.log("companies with fund holdings %d (%d listed), %s securities worth %s | most widely held: %s" % (
            len(held), sum(1 for x in held if x["ticker"]), format(len(frows), ","), usd(sum(x["f_value"] for x in held)),
            "; ".join("%s %d funds" % (x["ticker"] or x["name"][:20], x["f_funds"]) for x in sorted(held, key=lambda x: -x["f_funds"])[:8])))
        ck(not wrong and len(held) == len(per) == block["tied"]["companies"] and len(frows) == block["n"] == block["tied"]["securities"], "every company's nine holding columns are its own rows of the fund document added up here (%d companies, %s rows)%s" % (
            len(per), format(len(frows), ","), "" if not wrong else ": " + json.dumps(wrong[:3])[:300]))
        ck(all(x["co"] in by_id for x in frows) and all(x["cls"] in ("DBT", "LON") for x in frows), "every row sits under a company on the board and is a bond or a loan")

        # every row of the fund document is a row of the SEC bank's table
        tbl = parse(get(head["tables"]["bonds"])[0])
        tc = {c: i for i, c in enumerate(tbl["cols"])}
        source = defaultdict(int)
        for b in tbl["rows"]:
            source[(b[tc["issuer"]], b[tc["title"]], b[tc["mat"]], b[tc["value"]], b[tc["funds"]], b[tc["px"]])] += 1
        missing = sum(1 for x in frows if not source.get((x["issuer"], x["title"], x["mat"], x["value"], x["funds"], x["px"])))
        corporate = [b for b in tbl["rows"] if b[tc["cls"]] in ("DBT", "LON") and b[tc["itype"]] not in ("MUN", "UST", "USGA", "USGSE", "NUSS")]
        total = sum(b[tc["value"]] or 0 for b in corporate)
        r.log("the SEC bank's table: %s securities, %s of them a company's bonds and loans worth %s; tied to the board %s worth %s (%.1f%%; the block says %s%%)" % (
            format(len(tbl["rows"]), ","), format(len(corporate), ","), usd(total), format(len(frows), ","), usd(sum(x["value"] or 0 for x in frows)), 100.0 * sum(x["value"] or 0 for x in frows) / total, block["tied"]["value_pct"]))
        ck(missing == 0 and len(corporate) == block["read"]["securities"] and abs(100.0 * sum(x["value"] or 0 for x in frows) / total - block["tied"]["value_pct"]) < 0.06, "every row of the fund document is a row of the SEC bank's table, and the share tied is the block's (%d not found)" % missing)
        for cid, pat in (("T:ORCL", r"^ORACLE"), ("T:CRWV", r"^COREWEAVE INC"), ("T:F", r"^FORD MOTOR"), ("T:BA", r"^BOEING"), ("T:CHTR", r"^CHARTER COMM|^CCO H"), ("I:MERIDIAN ARC HOLDCO", r"MERIDIAN ARC")):
            rx = re.compile(pat, re.I)
            raw_rows = [b for b in corporate if rx.search(b[tc["issuer"]] or "")]
            x = by_id.get(cid) or {}
            r.log("  %-22s the board: %4s securities %9s, most funds %4s, mark %7s, gap %5s | the SEC table by the pattern %r: %4d securities %9s" % (
                cid, x.get("f_n"), usd(x.get("f_value")), x.get("f_funds"), x.get("f_px"), x.get("f_gap"), pat, len(raw_rows), usd(sum(b[tc["value"]] or 0 for b in raw_rows))))

        # the swaps
        cds = block["cds"]
        at = {c: i for i, c in enumerate(cds["cols"])}
        sums = defaultdict(lambda: [0, 0, 0])
        for n in cds["names"]:
            if n[at["co"]]:
                s = sums[n[at["co"]]]
                s[0] += n[at["sold"]] or 0
                s[1] += n[at["bought"]] or 0
                s[2] = max(s[2], n[at["funds"]] or 0)
        off = [i for i, s in sums.items() if (by_id[i]["fc_sold"], by_id[i]["fc_bought"], by_id[i]["fc_net"], by_id[i]["fc_funds"]) != (s[0], s[1], s[0] - s[1], s[2])]
        stray = [x["id"] for x in table if x["fc_funds"] is not None and x["id"] not in sums]
        swaps = parse(get(head["tables"]["cds"])[0])
        dealers = [n for n in cds["names"] if n[at["dealer"]]]
        r.log("credit default swaps: %d names in the SEC bank's table, %d on the page, %d tied to %d companies; dealers' names %d, tied %d: %s" % (
            len(swaps["names"]), len(cds["names"]), cds["tied"], len(sums), len(dealers), sum(1 for n in dealers if n[at["co"]]), "; ".join(str(n[0])[:28] for n in dealers[:10])))
        net = sorted(((x["ticker"] or x["name"][:18], x["fc_net"]) for x in table if x["fc_funds"]), key=lambda kv: kv[1])
        r.log("funds are net sellers of protection on: %s | net buyers on: %s" % ("; ".join("%s %s" % (k, usd(v)) for k, v in net[::-1][:8]), "; ".join("%s %s" % (k, usd(-v)) for k, v in net[:8] if v < 0)))
        ck(not off and not stray and len(cds["names"]) == len(swaps["names"]) and cds["tied"] == sum(1 for n in cds["names"] if n[at["co"]]), "every company's swap figures are its reference names added up here, and every name of the SEC bank's table is listed")
        ck(dealers and all(n[at["co"]] is None for n in dealers) and cds["dealer_names_not_tied"] == len(dealers), "a reference name funds also write as a dealer enters no company (%d names)" % len(dealers))

        # the lists, whole; nothing shaped like an identifier
        gaps = [x for x in frows if (x["n_px"] or 0) >= fdoc["min_marks"] and x["px_hi"] is not None and x["px_lo"] is not None]
        ck(len(fdoc["ties_by_rule"]) == block["n_ties_by_rule"] and len(fdoc["not_on_board"]) == block["n_not_on_board"] and len(fdoc["untied"]) == block["n_untied"] and len(gaps) == block["n_gaps"],
           "the fund document carries the lists whole: %d names tied by a rule, %d listed companies not on the board, %d issuers left untied, %s securities enough funds mark" % (
               len(fdoc["ties_by_rule"]), len(fdoc["not_on_board"]), len(fdoc["untied"]), format(len(gaps), ",")))
        ident = re.compile(r"(?<![A-Za-z0-9])(?:[A-Z]{2}[A-Z0-9]{9}\d|(?=[A-Z0-9]*\d[A-Z0-9]*\d[A-Z0-9]*\d)[A-Z0-9]{9})(?![A-Za-z0-9])")
        texts = [str(x[c]) for x in frows for c in ("issuer", "title") if x.get(c)] + [str(x[0]) for x in fdoc["untied"]] + [str(x[0]) for x in fdoc["ties_by_rule"]] + [str(n[0]) for n in cds["names"]]
        shaped = [t for t in texts if ident.search(t)]
        ck(not shaped and not {"cusip", "isin", "figi", "k"} & {c.lower() for c in fdoc["cols"] + cds["cols"]}, "nothing published is, or is shaped like, an identifier (%s names and titles read; %d shaped like one)" % (format(len(texts), ","), len(shaped)))

        # the site
        body, hdr = site("data/credit-before-equity-funds.json", raw=True)
        sdoc = json.loads(body)
        main_site = site("data/credit-before-equity.json")
        r.log("the site: the board's document generated %s; the fund document %d bytes, %s rows, content-encoding %s, cache-control %s" % (
            main_site.get("generated_at"), len(body), format(len(sdoc["rows"]), ","), hdr.get("Content-Encoding") or hdr.get("content-encoding"), hdr.get("Cache-Control") or hdr.get("cache-control")))
        ck(main_site.get("generated_at") == doc["generated_at"] and sdoc.get("generated_at") == fdoc.get("generated_at") and len(sdoc["rows"]) == len(fdoc["rows"]), "the site serves both documents as they are in the bucket")

        r.section("5. The page in a real browser on the live site")
        html = ""
        for attempt in range(40):
            try:
                html = site("credit-before-equity.html", raw=True)[0].decode("utf-8", "replace")
            except Exception as exc:  # noqa: BLE001
                html = "(%s)" % type(exc).__name__
            if "/jh-credit-funds.js?v=" + VERSION_TAG in html:
                break
            time.sleep(30)
        r.log("the live page names the fund script of %s: %s (after %d checks, 30 s apart)" % (VERSION_TAG, "/jh-credit-funds.js?v=" + VERSION_TAG in html, attempt + 1))
        if "/jh-credit-funds.js?v=" + VERSION_TAG not in html:
            r.fail("the site has not deployed the page yet; the engine side is published and checked above. Run this op again once the site is out: it will not run the engine a second time")
            sys.exit(1)
        stamp = int(time.time())
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)

            def new_page(phone=False):
                ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, device_scale_factor=2, is_mobile=True, has_touch=True, accept_downloads=True) if phone else \
                    browser.new_context(viewport={"width": 1500, "height": 950}, user_agent=UA, accept_downloads=True)
                page, errors = ctx.new_page(), []
                page.on("pageerror", lambda e: errors.append(str(e)[:200]))
                return page, errors
            try:
                browse(new_page, BASE + "/credit-before-equity.html?probe=%d" % stamp, doc, fdoc, ck, shots, r.log)
            except Exception as exc:  # noqa: BLE001
                ck(False, "the browser part stopped: %s: %s" % (type(exc).__name__, str(exc)[:500]))
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(companies_with_fund_holdings=len(held), fund_rows=len(frows), tied_value_pct=block["tied"]["value_pct"], swap_names_tied=cds["tied"], failed_checks=len(fails), document=doc["generated_at"][:16])
        if fails:
            r.fail("%d check(s) failed" % len(fails))
            sys.exit(1)
        r.ok("LIVE: funds hold %s bonds and loans of %d companies on the board (%s at their own marks, %s%% of the company debt in the SEC file); %d swap names are tied to a company; the page draws it, every list whole and sortable, and Oracle's and CoreWeave's sheets list what funds hold of them" % (
            format(len(frows), ","), len(held), usd(block["tied"]["value"]), block["tied"]["value_pct"], cds["tied"]))


if __name__ == "__main__":
    main()
