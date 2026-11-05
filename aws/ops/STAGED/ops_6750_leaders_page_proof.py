"""ops 6750 - READ-ONLY browser proof of leaders.html and of the chart's Leaders button (Khalid 2026-11-04: "I want to
be able to sort them ascending and descending. Make sure i can access it from chart.html put an icon for it next to
13f"; CLAUDE.md rule 5: click it yourself).

Against https://justhodl.ai in headless Chrome, after ops 6749 published data/leaders.json and the Pages build served
the page:
  0. the served HTML carries the leaders script, the script carries the contract, the edge serves the artifact;
  1. the page renders from the artifact: verdict tiles, the boards, industries, leaders, industry funds, reading rules,
     sources - in that order; no "undefined" / "NaN" / "[object Object]" anywhere;
  2. every table sorts both ways by real clicks on its headers (aria-sort flips, the column is ordered, blanks stay
     last): leaders, industries, industry funds, the board; the 20 / 50 / 100 / 200 / 250 average columns of the leaders
     AND of the funds; a text column A-Z / Z-A; a header from the keyboard;
  3. every board chip (golden / death cross, near, double top / bottom, buying / selling climax, tight price,
     accumulation / distribution, smart money, stacks, RS highs / lows) shows as many rows as its count, for leaders
     and for funds; a verdict tile jumps to its board;
  4. filters: an industry name, top-5 US, search, a signal chip, a sector, show all, current-flow funds; the state
     survives a reload through the URL hash; 0 horizontal overflow at 390 px; 0 page errors;
  5. chart.html: the Leaders button sits immediately after 13F in the toolbar; clicking it opens the Leaders desk in
     the workspace; clicking a ticker there loads that symbol in the chart and closes the workspace.
A 0.5x JPEG of the page and one of the chart with the desk open are embedded. Nothing on the site changes.
"""
from pathlib import Path
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = os.environ.get("JH_PROOF_BASE", "https://justhodl.ai")
WANT_KEYS = ["verdict", "printing-now", "industries", "leaders", "industry-funds", "reading-rules", "sources"]
WANT_BOARDS = ["golden_cross", "death_cross", "golden_near", "death_near", "double_top", "double_bottom", "buying_climax", "selling_climax", "tight", "accumulation", "distribution",
               "smart_buying", "smart_selling", "bull_stack", "bear_stack", "rs_new_highs", "rs_new_lows"]
MA = ["d20", "d50", "d100", "d200", "d250"]
ROWS = "#lead table tbody tr td a.tk"


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
        log("chrome channel unavailable (%s); using chromium" % type(e).__name__)
        try:
            return p.chromium.launch(headless=True)
        except Exception:  # noqa: BLE001
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
            return p.chromium.launch(headless=True)


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 jh-ops"})
    return urllib.request.urlopen(req, timeout=60).read().decode("utf-8", "replace")


def num(s):
    t = re.sub(r"[,$%×+]", "", str(s).replace("−", "-").replace("–", "-")).strip()
    m = re.search(r"-?\d+(\.\d+)?", t)
    if not m:
        return None
    v = float(m.group(0))
    for unit, mul in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if re.search(unit + r"\b", t):
            return v * mul
    return v


def col_vals(page, host, col):
    return page.evaluate("""([host, col]) => {
      const ths = [...document.querySelectorAll('#' + host + ' thead th')], i = ths.findIndex(t => t.getAttribute('data-sort') === col);
      if (i < 0) return null;
      return {aria: ths[i].getAttribute('aria-sort'), vals: [...document.querySelectorAll('#' + host + ' tbody tr')].map(tr => tr.children[i].innerText.trim())};
    }""", [host, col])


def ordered(vals, direction, text=False):
    """(n values, n blanks) when the column is ordered in `direction` with its blanks last, else raises."""
    xs = [None if v in ("—", "", "-") else (v.lower() if text else num(v)) for v in vals]
    first_blank = next((i for i, v in enumerate(xs) if v is None), len(xs))
    assert all(v is None for v in xs[first_blank:]), "blanks are not last"
    body = xs[:first_blank]
    for i in range(1, len(body)):
        a, b = body[i - 1], body[i]
        assert (b <= a if direction == "descending" else b >= a) if text else (b <= a + 1e-9 if direction == "descending" else b >= a - 1e-9), "not %s at row %d: %s then %s" % (direction, i, a, b)
    return len(body), len(xs) - len(body)


def sort_both(page, host, col, text=False):
    sel = '#%s th[data-sort="%s"]' % (host, col)
    page.click(sel)
    page.wait_for_timeout(150)
    a = col_vals(page, host, col)
    n1, blanks = ordered(a["vals"], a["aria"], text)
    page.click(sel)
    page.wait_for_timeout(150)
    b = col_vals(page, host, col)
    ordered(b["vals"], b["aria"], text)
    assert a["aria"] in ("ascending", "descending") and b["aria"] in ("ascending", "descending") and a["aria"] != b["aria"], "aria-sort did not flip (%s -> %s)" % (a["aria"], b["aria"])
    return "%s.%s %s then %s (%d values, %d blank last; first %s / %s)" % (host, col, a["aria"], b["aria"], n1, blanks, a["vals"][0] if a["vals"] else None, b["vals"][0] if b["vals"] else None)


def settle(page):
    """First visit of a fresh profile: the site's one-time cache guard reloads the page once."""
    page.wait_for_selector(ROWS, timeout=90000)
    page.wait_for_timeout(2500)
    page.wait_for_selector(ROWS, timeout=90000)
    page.wait_for_timeout(500)


def main():
    with report("ops_6750_leaders_page_proof") as r:
        r.heading("ops 6750 - leaders.html and the chart's Leaders button: live proof (read-only) on %s" % BASE)
        stamp = int(time.time())
        fails = []

        def check(name, fn):
            try:
                out = fn()
                r.log("ok   %s: %s" % (name, out if isinstance(out, str) else json.dumps(out, ensure_ascii=False)[:1500]))
                return out
            except Exception as e:  # noqa: BLE001
                fails.append(name)
                r.fail("%s: %s: %s" % (name, type(e).__name__, str(e)[:400]))
                return None

        r.section("0. served HTML, script and artifact")
        html = fetch("%s/leaders.html?nocache=%d" % (BASE, stamp))
        tag = re.search(r'<script src="(/jh-leaders\.js[^"]*)"', html)
        keys = re.findall(r'data-jh-key="([^"]+)"', html)
        r.log("leaders.html: %d bytes, script %s, section keys %s, sortable injected %s" % (len(html), tag.group(1) if tag else "MISSING", keys, bool(re.search(r"/jh-sortable\.js", html))))
        js = fetch("%s%s" % (BASE, tag.group(1))) if tag else ""
        r.log("%s: %d bytes, contract marker %s" % (tag.group(1) if tag else "/jh-leaders.js", len(js), '"leaders.v1"' in js))
        art = json.loads(fetch("%s/data/leaders.json?nocache=%d" % (BASE, stamp)))
        r.log("data/leaders.json at the edge: contract %s version %s generated %s as_of %s | %d leaders, %d industries, %d funds | flows current %s/%s by source %s | trigger %s" % (
            art.get("contract"), art.get("version"), art.get("generated_at"), art.get("as_of"), len(art.get("rows") or []), len(art.get("industries") or []), len(art.get("etfs") or []),
            (art.get("flows") or {}).get("n_current"), (art.get("flows") or {}).get("n_funds"), json.dumps((art.get("flows") or {}).get("by_source")), json.dumps(art.get("trigger"))))
        chart_html = fetch("%s/chart.html?nocache=%d" % (BASE, stamp))
        ctag = re.search(r'<script[^>]+src="(/jh-chart-engine\.js[^"]*)"', chart_html)
        rtag = re.search(r'<script[^>]+src="(/jh-chart-tvrail\.js[^"]*)"', chart_html)
        cjs = fetch("%s%s" % (BASE, ctag.group(1))) if ctag else ""
        rjs = fetch("%s%s" % (BASE, rtag.group(1))) if rtag else ""
        r.log("chart.html: engine %s (btn-ldr %s), rail %s (leaders workspace %s)" % (ctag.group(1) if ctag else "MISSING", "id=btn-ldr" in cjs, rtag.group(1) if rtag else "MISSING", '"/leaders.html"' in rjs))
        if not tag or '"leaders.v1"' not in js or art.get("contract") != "leaders.v1" or "id=btn-ldr" not in cjs or '"/leaders.html"' not in rjs:
            r.fail("the served page / script / artifact / chart toolbar are not the release yet (Pages deploy or ops 6749 pending)")
            raise SystemExit(1)
        n_rows, n_inds, n_etfs = len(art["rows"]), len(art["industries"]), len(art["etfs"])
        boards = art["boards"]

        sync_playwright = ensure_playwright(r.log)
        facts = {}
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome", accept_downloads=True)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append("pageerror: %s" % str(e)[:300]))
            page.on("console", lambda m: errors.append("console: %s" % m.text[:300]) if m.type == "error" and "Failed to load resource" not in m.text else None)

            r.section("1. render and order")
            page.goto("%s/leaders.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            settle(page)
            out = page.evaluate("""() => {
              const q = s => document.querySelectorAll(s).length, text = document.body.innerText, de = document.documentElement;
              return {order: [...document.querySelectorAll('main > section')].map(e => e.getAttribute('data-jh-key') || e.id), kpis: q('#kpis .jh-kpi'), boardChips: q('#board-chips [data-v]'),
                      board: q('#board tbody tr'), ind: q('#ind tbody tr'), lead: q('#lead tbody tr'), etf: q('#etf tbody tr'), rules: q('#rules dt, #rules li, #rules p'), sources: q('#sources tbody tr'),
                      history: q('#history tbody tr'), stamp: document.getElementById('stamp').innerText.replace(/\\n/g, ' | '), verdict: document.getElementById('verdict-line').innerText,
                      meta: ['board-meta', 'ind-meta', 'lead-meta', 'etf-meta'].map(i => document.getElementById(i).innerText),
                      bad: (text.match(/\\[object Object\\]|undefined|NaN/g) || []).length, overflow: de.scrollWidth - de.clientWidth,
                      ownSort: q('th[data-sort]'), foreignSort: q('table[data-jh-sortable]'),
                      kpiText: [...document.querySelectorAll('#kpis .jh-kpi')].map(e => e.innerText.replace(/\\s+/g, ' ').trim().slice(0, 150))};
            }""")
            for k, v in out.items():
                r.log("%s: %s" % (k, json.dumps(v, ensure_ascii=False)[:1400]))
            facts.update(out)
            check("section order", lambda: (_ for _ in ()).throw(AssertionError(out["order"])) if [x for x in out["order"] if x in WANT_KEYS] != WANT_KEYS else " -> ".join(out["order"]))
            check("row counts match the artifact", lambda: (_ for _ in ()).throw(AssertionError("ind %s/%s etf %s/%s lead %s" % (out["ind"], n_inds, out["etf"], n_etfs, out["lead"])))
                  if not (out["ind"] == n_inds and out["etf"] == n_etfs and out["lead"] == min(200, n_rows) and out["kpis"] == 8 and out["boardChips"] == 17 and out["sources"] >= 9) else
                  "%d industries, %d funds, %d of %d leaders shown, 8 tiles, 17 boards, %d sources" % (out["ind"], out["etf"], out["lead"], n_rows, out["sources"]))
            check("no undefined / NaN / [object Object]", lambda: (_ for _ in ()).throw(AssertionError(out["bad"])) if out["bad"] else "0 in the rendered text")

            r.section("2. sorting, both directions, by real clicks")
            for host, cols in (("lead", ["vs_m", "vs_q", "strength"]), ("ind", ["vs_m", "mf21", "strength"]), ("etf", ["flow21", "flow5", "vs_m"]), ("board", ["cross_since", "cross_ago"])):
                for col in cols:
                    check("sort %s.%s" % (host, col), lambda host=host, col=col: sort_both(page, host, col))
            check("sort lead.ticker (text)", lambda: sort_both(page, "lead", "ticker", text=True))

            def keyboard():
                page.focus('#ind th[data-sort="strength"]')
                page.keyboard.press("Enter")
                page.wait_for_timeout(150)
                a = col_vals(page, "ind", "strength")
                ordered(a["vals"], a["aria"])
                page.focus('#ind th[data-sort="strength"]')
                page.keyboard.press("Space")
                page.wait_for_timeout(150)
                b = col_vals(page, "ind", "strength")
                ordered(b["vals"], b["aria"])
                assert a["aria"] != b["aria"]
                return "Enter -> %s, Space -> %s" % (a["aria"], b["aria"])
            check("sort from the keyboard", keyboard)
            page.click('#lead-view [data-v="ma"]')
            page.wait_for_timeout(200)
            for col in MA:
                check("leaders vs the %s-session average" % col[1:], lambda col=col: sort_both(page, "lead", col))
            page.click('#etf-view [data-v="ma"]')
            page.wait_for_timeout(200)
            for col in MA:
                check("funds vs the %s-session average" % col[1:], lambda col=col: sort_both(page, "etf", col))
            views = {}
            for v in ("rs", "ma", "ad", "smart", "pat", "bars", "overview"):
                page.click('#lead-view [data-v="%s"]' % v)
                page.wait_for_timeout(150)
                views[v] = page.evaluate("() => document.querySelectorAll('#lead thead th').length + ' columns: ' + [...document.querySelectorAll('#lead thead th')].map(t => t.innerText.trim()).join(', ')")
            r.log("leader views: %s" % json.dumps(views, ensure_ascii=False)[:2500])
            fviews = {}
            for v in ("flows", "rs", "ma", "pat"):
                page.click('#etf-view [data-v="%s"]' % v)
                page.wait_for_timeout(150)
                fviews[v] = page.evaluate("() => [...document.querySelectorAll('#etf thead th')].map(t => t.innerText.trim()).join(', ')")
            r.log("fund views: %s" % json.dumps(fviews, ensure_ascii=False)[:1500])
            page.click('#etf-view [data-v="flows"]')

            r.section("3. boards")
            def board_counts(scope):
                page.click('#board-scope [data-v="%s"]' % scope)
                page.wait_for_timeout(200)
                res = {}
                ids = page.evaluate("() => [...document.querySelectorAll('#board-chips [data-v]')].map(b => b.getAttribute('data-v'))")
                assert ids == [b for b in WANT_BOARDS if scope == "leaders" or not b.startswith("smart_")], ids      # funds carry no 13F / insider verdict
                for b in ids:
                    page.click('#board-chips [data-v="%s"]' % b)
                    page.wait_for_timeout(120)
                    label, rows = page.evaluate("(b) => [document.querySelector('#board-chips [data-v=\"' + b + '\"]').textContent.trim(), document.querySelectorAll('#board tbody tr td a.tk').length]", b)
                    want = len(boards[scope][b])
                    assert int(label.split(" ")[-1]) == rows == want, "%s.%s: chip %s, rows %s, artifact %s" % (scope, b, label, rows, want)
                    res[b] = rows
                return res
            facts["boards_leaders"] = check("leader boards: chip = rows = artifact", lambda: board_counts("leaders"))
            facts["boards_funds"] = check("fund boards: chip = rows = artifact", lambda: board_counts("etfs"))
            page.click('#board-scope [data-v="leaders"]')

            def kpi_jump():
                page.click('#kpis [data-board="death_cross"]')
                page.wait_for_timeout(250)
                t = page.evaluate("() => document.querySelector('#board-chips .on').textContent.trim() + ' -> ' + document.getElementById('board-meta').innerText")
                assert re.search("death cross", t, re.I), t
                return t
            check("a verdict tile jumps to its board", kpi_jump)

            r.section("4. filters, show all, state in the URL, phone width")
            def industry_click():
                name = page.evaluate("() => document.querySelector('#ind td a.ld-ind').getAttribute('data-ind')")
                page.click("#ind td a.ld-ind")
                page.wait_for_timeout(300)
                meta = page.evaluate("() => document.getElementById('lead-meta').innerText + ' | ' + [...document.querySelectorAll('#lead tbody tr td:first-child')].map(x => x.innerText.trim()).join(' ')")
                want = sorted(x["ticker"] for x in art["rows"] if x["industry"] == name)
                got = sorted(page.evaluate("() => [...document.querySelectorAll('#lead tbody tr td a.tk')].map(a => a.getAttribute('data-t'))"))
                assert got == want, "%s: page %s, artifact %s" % (name, got, want)
                page.click("#lead-reset")
                page.wait_for_timeout(200)
                return "%s -> %s" % (name, meta)
            check("an industry name lists its leaders", industry_click)

            def rank_us():
                page.click('#lead-rank [data-v="us"]')
                page.wait_for_timeout(200)
                got = page.evaluate("() => [document.getElementById('lead-meta').innerText, document.querySelectorAll('#lead tbody tr').length]")
                page.click("#lead-reset")
                page.wait_for_timeout(200)
                want = sum(1 for x in art["rows"] if x.get("rank_us") is not None and x["rank_us"] <= 5)
                assert got[1] == min(200, want) and str(want) in got[0], "%s, artifact %s" % (got, want)
                return got[0]
            check("top-5 US filter", rank_us)

            def search():
                page.fill("#lead-q", "bank")
                page.wait_for_timeout(300)
                got = page.evaluate("() => [document.getElementById('lead-meta').innerText, document.querySelectorAll('#lead tbody tr').length]")
                page.fill("#lead-q", "")
                page.wait_for_timeout(200)
                assert 0 < got[1] < n_rows, got
                return got[0]
            check("search", search)

            def signal():
                page.click('#lead-sig [data-v="GOLDEN CROSS"]')
                page.wait_for_timeout(200)
                got = page.evaluate("() => [document.getElementById('lead-meta').innerText, [...document.querySelectorAll('#lead tbody tr td a.tk')].map(a => a.getAttribute('data-t'))]")
                page.click('#lead-sig [data-v="GOLDEN CROSS"]')
                page.wait_for_timeout(150)
                assert sorted(got[1]) == sorted(boards["leaders"]["golden_cross"]), got
                return got[0]
            check("a signal chip filters the leaders to that board", signal)

            def sector():
                page.select_option("#lead-sector", "Energy")
                page.wait_for_timeout(200)
                got = page.evaluate("() => [document.getElementById('lead-meta').innerText, document.querySelectorAll('#lead tbody tr').length]")
                page.click("#lead-reset")
                page.wait_for_timeout(200)
                assert got[1] == min(200, sum(1 for x in art["rows"] if x["sector"] == "Energy")), got
                return got[0]
            check("sector filter", sector)

            def show_all():
                page.click("#lead-more")
                page.wait_for_timeout(600)
                got = page.evaluate("() => [document.querySelectorAll('#lead tbody tr').length, document.getElementById('lead-more').hidden]")
                assert got == [n_rows, True], got
                return "%d rows (every leader)" % got[0]
            check("show all leaders", show_all)

            def funds_current():
                page.check("#etf-current")
                page.wait_for_timeout(200)
                got = page.evaluate("() => [document.getElementById('etf-meta').innerText, document.querySelectorAll('#etf tbody tr').length]")
                page.uncheck("#etf-current")
                want = sum(1 for x in art["etfs"] if (x.get("flow") or {}).get("current"))
                assert got[1] == want, "%s rows, artifact %s" % (got[1], want)
                return got[0]
            check("funds with a current reported flow", funds_current)

            def flows_shown():
                page.click('#etf-view [data-v="flows"]')
                page.wait_for_timeout(200)
                res = {}
                for col in ("flow1", "flow5", "flow21", "flow63"):
                    a = col_vals(page, "etf", col)
                    res[col] = sum(1 for v in a["vals"] if v not in ("—", ""))
                return res
            facts["flows_shown"] = check("fund flow cells with a figure", flows_shown)

            def csv():
                with page.expect_download() as dl:
                    page.click('[data-csv="ind"]')
                d = dl.value
                lines = Path(d.path()).read_text(encoding="utf-8").split("\n")
                assert len(lines) >= n_inds + 1, len(lines)
                return "%s: %d lines; header %s" % (d.suggested_filename, len(lines), lines[0][:120])
            check("CSV download", csv)

            def reload_state():
                page.click('#lead-view [data-v="ma"]')
                page.click('#lead th[data-sort="d200"]')
                page.click('#lead th[data-sort="d200"]')
                page.wait_for_timeout(200)
                h = page.evaluate("() => location.hash")
                page.reload(wait_until="domcontentloaded")
                page.wait_for_selector(ROWS, timeout=90000)
                page.wait_for_timeout(600)
                got = page.evaluate("""() => [document.querySelector('#lead-view .on').textContent.trim(), (document.querySelector('#lead th[aria-sort]') || {textContent: ''}).textContent.trim(),
                                              (document.querySelector('#lead th[aria-sort]') || {getAttribute() {}}).getAttribute('aria-sort'), document.querySelector('#board-chips .on').textContent.trim()]""")
                assert re.search("moving averages", got[0], re.I) and got[2] in ("ascending", "descending"), got
                a = col_vals(page, "lead", "d200")
                ordered(a["vals"], a["aria"])
                return "%s | after reload: %s" % (h, got)
            check("the view and the sort survive a reload", reload_state)

            def link():
                got = page.evaluate("() => { const a = document.querySelector('#lead a.tk'); return [a.getAttribute('href'), a.target, a.getAttribute('data-t')]; }")
                assert got[0] == "/chart.html?s=" + got[2].replace(".", ".") or got[0].startswith("/chart.html?s="), got
                assert got[1] == "_top"
                return "%s target=%s" % (got[0], got[1])
            check("ticker links open the chart", link)

            def phone():
                page.set_viewport_size({"width": 390, "height": 844})
                page.wait_for_timeout(700)
                got = page.evaluate("() => [document.documentElement.scrollWidth - document.documentElement.clientWidth, document.querySelectorAll('#kpis .jh-kpi').length, document.querySelectorAll('#lead tbody tr').length]")
                page.set_viewport_size({"width": 1440, "height": 900})
                assert got[0] == 0, "horizontal overflow %s px" % got[0]
                return "0 px overflow at 390 px (%d tiles, %d leader rows)" % (got[1], got[2])
            check("phone width", phone)
            facts["errors"] = list(errors)
            check("no page or console error on leaders.html", lambda: (_ for _ in ()).throw(AssertionError(errors[:6])) if errors else "0")

            r.section("5. chart.html: the Leaders button beside 13F")
            cpage = ctx.new_page()
            cerrors = []
            cpage.on("pageerror", lambda e: cerrors.append("pageerror: %s" % str(e)[:200]))
            cpage.goto("%s/chart.html?s=AAPL&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            cpage.wait_for_selector("#btn-ldr", timeout=90000)
            cpage.wait_for_timeout(2500)
            cpage.wait_for_selector("#btn-ldr", timeout=90000)

            def toolbar():
                # the chart is full-page: its bars live off-screen until the pointer reaches the top edge (jh-chart-autohide.js)
                cpage.mouse.move(720, 300)
                cpage.wait_for_timeout(300)
                cpage.mouse.move(720, 2)
                cpage.wait_for_function("() => { const s = document.getElementById('jh-ah-top'); return !document.documentElement.classList.contains('jh-ah') || (s && s.classList.contains('show')); }", timeout=15000)
                cpage.wait_for_timeout(500)
                got = cpage.evaluate("""() => {
                  const a = document.getElementById('btn-13f'), b = document.getElementById('btn-ldr'), ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect();
                  return {next: a.nextElementSibling && a.nextElementSibling.id, title: b.title, label: b.innerText.replace(/\\s+/g, ' ').trim(), a: [Math.round(ra.left), Math.round(ra.top), Math.round(ra.width), Math.round(ra.height)],
                          b: [Math.round(rb.left), Math.round(rb.top), Math.round(rb.width), Math.round(rb.height)], visible: rb.width > 0 && rb.height > 0,
                          row: [...a.parentElement.children].map(x => x.id || x.className).slice(0, 40)};
                }""")
                got["autohide"] = cpage.evaluate("() => document.documentElement.classList.contains('jh-ah')")
                assert got["next"] == "btn-ldr" and got["visible"] and abs(got["a"][1] - got["b"][1]) <= 2 and got["b"][0] >= got["a"][0] + got["a"][2] - 1, got
                assert got["b"][1] >= 0 and got["b"][0] + got["b"][2] <= 1440, "the button is outside the viewport: %s" % got["b"]
                facts["toolbar_shot"] = cpage.screenshot(type="jpeg", quality=80, scale="css", clip={"x": 0, "y": 0, "width": 1440, "height": max(110, got["b"][1] + got["b"][3] + 12)})
                return got
            facts["toolbar"] = check("btn-ldr is the button right after btn-13f, on the same row", toolbar)

            def open_desk():
                before = cpage.evaluate("() => [document.getElementById('symchip-id').textContent.trim(), (document.querySelector('#tabs .tab.on') || {dataset: {}}).dataset.id || null]")
                cpage.click("#btn-ldr")
                cpage.wait_for_selector("#ws-overlay.on iframe", timeout=30000)
                head = cpage.evaluate("() => [document.querySelector('#ws-overlay .wshd span').textContent, document.querySelector('#ws-overlay iframe').getAttribute('src'), document.querySelector('#ws-overlay .wshd a').getAttribute('href')]")
                assert head[1] == "/leaders.html" and "Leaders" in head[0], head
                fl = cpage.frame_locator("#ws-overlay iframe")
                fl.locator(ROWS).first.wait_for(timeout=90000)
                cpage.wait_for_timeout(1500)
                inside = fl.locator("body").evaluate("""() => ({leaders: document.querySelectorAll('#lead tbody tr').length, industries: document.querySelectorAll('#ind tbody tr').length, funds: document.querySelectorAll('#etf tbody tr').length,
                                                             tiles: document.querySelectorAll('#kpis .jh-kpi').length, stamp: document.getElementById('stamp').innerText.replace(/\\n/g, ' | ')})""")
                assert inside["leaders"] >= 100 and inside["industries"] == n_inds and inside["funds"] == n_etfs, inside
                facts["chart_shot"] = cpage.screenshot(type="jpeg", quality=60, scale="css")
                pick = fl.locator(ROWS).nth(3)
                t = pick.get_attribute("data-t")
                pick.click()
                cpage.wait_for_function("(t) => { const ov = document.getElementById('ws-overlay'); const tab = document.querySelector('#tabs .tab.on'); return ov && ov.className !== 'on' && tab && tab.dataset.id === t; }",
                                        arg=re.sub(r"[^A-Z0-9.\-]", "", t.upper()), timeout=30000)
                after = cpage.evaluate("() => [document.getElementById('symchip-id').textContent.trim(), document.querySelector('#tabs .tab.on').dataset.id, document.getElementById('ws-overlay').className]")
                return {"before": before, "workspace": head, "inside": inside, "clicked": t, "after": after}
            facts["chart"] = check("the button opens the desk; a ticker there loads in the chart", open_desk)
            r.log("chart.html page errors (not gated; the chart is its own page): %s" % (cerrors[:5] or "none"))

            r.section("6. screenshots (0.5x, jpeg, base64)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=0.5)
            p3 = ctx2.new_page()
            p3.goto("%s/leaders.html?nocache=%d" % (BASE, stamp + 2), wait_until="domcontentloaded")
            try:
                settle(p3)
            except Exception:  # noqa: BLE001
                pass
            shot = p3.screenshot(full_page=True, type="jpeg", quality=55)
            r.log("leaders.html screenshot bytes: %d; chart with the desk open: %d; chart toolbar: %d" % (len(shot), len(facts.get("chart_shot") or b""), len(facts.get("toolbar_shot") or b"")))
            r.lines.append("")
            r.lines.append("<!-- SHOT_JPEG_BASE64 -->")
            r.lines.append(base64.b64encode(shot).decode("ascii"))
            r.lines.append("<!-- /SHOT_JPEG_BASE64 -->")
            for tag, key in (("CHART_SHOT_JPEG_BASE64", "chart_shot"), ("TOOLBAR_SHOT_JPEG_BASE64", "toolbar_shot")):
                if facts.get(key):
                    r.lines.append("<!-- %s -->" % tag)
                    r.lines.append(base64.b64encode(facts[key]).decode("ascii"))
                    r.lines.append("<!-- /%s -->" % tag)
            r.lines.append("")
            browser.close()

        bl = facts.get("boards_leaders") or {}
        r.kv(as_of=str(art.get("as_of")), leaders=str(n_rows), industries=str(n_inds), funds=str(n_etfs), failed=str(len(fails)), errors=str(len(facts.get("errors") or [])), overflow=str(facts.get("overflow")),
             golden=str(bl.get("golden_cross")), death=str(bl.get("death_cross")), dtop=str(bl.get("double_top")), dbot=str(bl.get("double_bottom")), tight=str(bl.get("tight")),
             flows_21=str((facts.get("flows_shown") or {}).get("flow21")), flows_63=str((facts.get("flows_shown") or {}).get("flow63")), chart=str(bool(facts.get("chart"))))
        if fails:
            r.fail("NOT PROVEN: %d checks failed: %s" % (len(fails), fails))
            raise SystemExit(1)
        ch = facts["chart"]
        r.ok("LEADERS PROVEN LIVE: %d leaders / %d industries / %d funds as of %s render; every table sorts both ways (20 / 50 / 100 / 200 / 250 averages included, leaders and funds); 17 boards x 2 scopes match the artifact; "
             "filters, CSV, reload and 390 px pass; chart.html: Leaders sits right after 13F, opens the desk, and clicking %s there switched the chart from %s to %s" % (
                 n_rows, n_inds, n_etfs, art.get("as_of"), ch["clicked"], ch["before"][0], ch["after"][0]))


if __name__ == "__main__":
    main()
    sys.exit(0)
