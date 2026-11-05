"""ops 6754 - READ-ONLY browser proof of capitulation-desk.html and of the chart's Capitulation button (Khalid
2026-11-05: "Capitulation · panic volume at the lows which is right on chart.html under capitulation. audit it and see
if we can improve it. maybe add etfs, crypto, metal, bonds and other asset class too"; CLAUDE.md rule 5: click it).

Against https://justhodl.ai in headless Chrome, after ops 6753 published data/capitulation-desk.json and the Pages build
served the page:
  0. the served page carries the artifact key and contract, the edge serves the artifact, chart.html's rail routes the
     Capitulation workspace to /capitulation-desk.html;
  1. the page renders from the artifact: the regime chip and the read line, six verdict tiles, the breadth chart (one
     bar per session, the 5-session line, cluster bars red), the desk table, the prints table (latest window), the
     follow-through tiles and the holding board, the benchmarks, reading rules, sources - in that order; counts match
     the artifact; no "undefined" / "NaN" / "[object Object]";
  2. the prints table sorts both ways by real clicks (score, volume multiple, return, date, since, a text column), the
     desk chips show as many rows as the artifact's counts, the 60-session window switches, a desk-table row jumps to
     its desk; the holding and benchmark tables sort; 0 horizontal overflow at 390 px; 0 page errors;
  3. chart.html: the Capitulation button opens /capitulation-desk.html in the workspace; clicking a ticker there loads
     that symbol in the chart and closes the workspace; the Wyckoff desk's tab text matches.
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
ROWS = "#tbl tbody tr[data-t], #tbl tbody tr"
WANT = ["Verdict: panic regime", "Breadth: prints per session", "Prints by asset class", "Follow-through", "The market's own prints", "Reading rules", "Sources and engine observability"]


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
      return {aria: ths[i].getAttribute('aria-sort'), vals: [...document.querySelectorAll('#' + host + ' tbody tr')].map(tr => tr.children[i] ? tr.children[i].innerText.trim() : '')};
    }""", [host, col])


def ordered(vals, direction, text=False):
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
    page.wait_for_selector("#kpis .jh-kpi", timeout=90000)
    page.wait_for_timeout(2500)
    page.wait_for_selector("#kpis .jh-kpi", timeout=90000)
    page.wait_for_timeout(500)


def main():
    with report("ops_6754_capitulation_page_proof") as r:
        r.heading("ops 6754 - capitulation-desk.html and the chart's Capitulation button: live proof (read-only) on %s" % BASE)
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

        r.section("0. served HTML, artifact and the chart's rail")
        html = fetch("%s/capitulation-desk.html?nocache=%d" % (BASE, stamp))
        r.log("capitulation-desk.html: %d bytes, key %s, contract marker %s, primary engine %s, sortable injected %s" % (
            len(html), 'KEY="data/capitulation-desk.json"' in html, 'DOC.contract!=="capitulation.v1"' in html, '<meta name="jh-primary-engine" content="justhodl-bottom">' in html, bool(re.search(r"/jh-sortable\.js", html))))
        art = json.loads(fetch("%s/data/capitulation-desk.json?nocache=%d" % (BASE, stamp)))
        br = art.get("breadth") or {}
        r.log("data/capitulation-desk.json at the edge: contract %s engine %s %s module %s session %s generated %s | scored %s | regime %s sum_5 %s pct %s | prints_60 %s latest %s | degraded %s" % (
            art.get("contract"), art.get("engine"), art.get("engine_version"), art.get("version"), art.get("session"), art.get("generated_at"), br.get("n_scored"), br.get("regime"), br.get("sum_5"), br.get("sum_5_percentile"),
            art.get("n_prints_60"), len(art.get("latest") or []), art.get("degraded")))
        chart_html = fetch("%s/chart.html?nocache=%d" % (BASE, stamp))
        rtag = re.search(r'<script[^>]+src="(/jh-chart-tvrail\.js[^"]*)"', chart_html)
        rjs = fetch("%s%s" % (BASE, rtag.group(1))) if rtag else ""
        wy = fetch("%s/wyckoff-desk.html?nocache=%d" % (BASE, stamp))
        r.log("chart.html rail %s: capit -> /capitulation-desk.html %s | wyckoff-desk.html: full-desk link %s, old 1.75x words gone %s" % (
            rtag.group(1) if rtag else "MISSING", 'capit: ["Capitulation · panic volume at the lows", "/capitulation-desk.html"]' in rjs, "/capitulation-desk.html" in wy, "volume ≥1.75×" not in wy))
        if 'KEY="data/capitulation-desk.json"' not in html or art.get("contract") != "capitulation.v1" or '"/capitulation-desk.html"' not in rjs:
            r.fail("the served page / artifact / chart rail are not the release yet (Pages deploy or ops 6753 pending)")
            raise SystemExit(1)
        n_latest, n60 = len(art.get("latest") or []), art.get("n_prints_60") or 0
        counts = art.get("counts") or {}
        desks = art.get("desk_order") or []

        sync_playwright = ensure_playwright(r.log)
        facts = {}
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append("pageerror: %s" % str(e)[:300]))
            page.on("console", lambda m: errors.append("console: %s" % m.text[:300]) if m.type == "error" and "Failed to load resource" not in m.text else None)

            r.section("1. render and order")
            page.goto("%s/capitulation-desk.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            settle(page)
            out = page.evaluate("""() => {
              const q = s => document.querySelectorAll(s).length, text = document.body.innerText, de = document.documentElement;
              return {order: [...document.querySelectorAll('main > section')].map(e => e.getAttribute('aria-label')), kpis: q('#kpis .jh-kpi'), ftKpis: q('#ft-kpis .jh-kpi'),
                      regime: (document.querySelector('#read .cp-regime') || {}).textContent, read: document.getElementById('read').innerText.slice(0, 300),
                      bars: q('#chart rect[data-i]'), redBars: [...document.querySelectorAll('#chart rect[data-i]')].filter(e => e.getAttribute('fill') === '#ff5577').length, line: q('#chart path'),
                      desks: q('#desks tbody tr'), chips: q('#chips .jh-chip'), chipText: [...document.querySelectorAll('#chips .jh-chip')].map(e => e.innerText.trim()),
                      rows: q('#tbl tbody tr'), rowsWithTicker: q('#tbl tbody tr a.tk'), hold: q('#hold tbody tr'), bench: q('#bench tbody tr'), defs: q('#defs dt'), sources: q('#sources tbody tr'),
                      stamp: document.getElementById('stamp').innerText.replace(/\\n/g, ' | '), meta: ['breadth-meta', 'prints-meta', 'ft-meta', 'bench-meta', 'hold-meta'].map(i => document.getElementById(i).innerText),
                      bad: (text.match(/\\[object Object\\]|undefined|NaN/g) || []).length, overflow: de.scrollWidth - de.clientWidth, ownSort: q('th[data-sort]'), foreignSort: q('table[data-jh-sortable]'),
                      kpiText: [...document.querySelectorAll('#kpis .jh-kpi')].map(e => e.innerText.replace(/\\s+/g, ' ').trim().slice(0, 160)),
                      firstRows: [...document.querySelectorAll('#tbl tbody tr')].slice(0, 6).map(tr => [...tr.children].map(td => td.innerText.trim()).join(' | ').slice(0, 260))};
            }""")
            for k, v in out.items():
                r.log("%s: %s" % (k, json.dumps(v, ensure_ascii=False)[:1400]))
            facts.update(out)
            check("section order", lambda: (_ for _ in ()).throw(AssertionError(out["order"])) if [w for w in WANT if not any((o or "").startswith(w) for o in out["order"])] or [next(i for i, o in enumerate(out["order"]) if (o or "").startswith(w)) for w in WANT] != sorted(next(i for i, o in enumerate(out["order"]) if (o or "").startswith(w)) for w in WANT) else " -> ".join(out["order"]))
            check("regime chip and read line", lambda: (_ for _ in ()).throw(AssertionError((out["regime"], out["read"]))) if (out["regime"] or "").strip() != (br.get("regime") or "") or not out["read"] else "%s · %s" % (out["regime"], out["read"][:160]))
            check("counts match the artifact", lambda: (_ for _ in ()).throw(AssertionError("rows %s/%s kpis %s bars %s/%s desks %s/%s chips %s" % (out["rows"], n_latest, out["kpis"], out["bars"], len(br.get("sessions") or []), out["desks"], len(desks), out["chips"])))
                  if not ((out["rows"] == n_latest or (n_latest == 0 and out["rows"] == 1)) and out["kpis"] == 6 and out["ftKpis"] >= 1 and out["bars"] == len(br.get("sessions") or []) and out["desks"] == len(desks) and out["chips"] == len(desks) + 1 and out["defs"] >= 8 and out["sources"] >= 7) else
                  "%d prints rows (latest %d), 6 tiles, %d breadth bars (%d cluster), %d desk rows, %d chips, %d holding rows, %d benchmark rows, %d definitions, %d sources" % (
                      out["rows"], n_latest, out["bars"], out["redBars"], out["desks"], out["chips"], out["hold"], out["bench"], out["defs"], out["sources"]))
            check("cluster bars match the artifact", lambda: (_ for _ in ()).throw(AssertionError((out["redBars"], len([s for s in (br.get("sessions") or []) if (s.get("pct_of_scored") or 0) >= (art.get("params") or {}).get("cluster_share_pct", 2.5)])))) if out["redBars"] != len([s for s in (br.get("sessions") or []) if (s.get("pct_of_scored") or 0) >= (art.get("params") or {}).get("cluster_share_pct", 2.5)]) else "%d red bars" % out["redBars"])
            check("no undefined / NaN / [object Object]", lambda: (_ for _ in ()).throw(AssertionError(out["bad"])) if out["bad"] else "0 in the rendered text")
            check("own sortable headers only", lambda: (_ for _ in ()).throw(AssertionError((out["ownSort"], out["foreignSort"]))) if out["ownSort"] < 40 or out["foreignSort"] else "%d own data-sort headers, 0 foreign" % out["ownSort"])

            r.section("2. sorting, chips, windows")
            if n_latest >= 2:
                for col in ("score", "vol_x", "ret_pct", "date", "since", "dd20_pct"):
                    check("sort tbl.%s" % col, lambda col=col: sort_both(page, "tbl", col))
                check("sort tbl.ticker (text)", lambda: sort_both(page, "tbl", "ticker", text=True))
            else:
                r.warn("fewer than two prints in the latest window: sorting proven on the 60-session window instead")

            def chips():
                got = {}
                for d in desks:
                    page.click('#chips .jh-chip[data-desk="%s"]' % d)
                    page.wait_for_timeout(120)
                    n = page.evaluate("() => document.querySelectorAll('#tbl tbody tr a.tk').length")
                    want = (counts.get(d) or {}).get("prints_5") or 0
                    assert n == want, "%s shows %d, artifact %d" % (d, n, want)
                    got[d] = n
                page.click('#chips .jh-chip[data-desk="ALL"]')
                page.wait_for_timeout(120)
                return got
            facts["chips"] = check("every desk chip shows as many rows as the artifact's latest-window count", chips)

            def window60():
                page.click('#window .jh-chip[data-win="ALL"]')
                page.wait_for_timeout(200)
                n = page.evaluate("() => document.querySelectorAll('#tbl tbody tr a.tk').length")
                assert n == n60, "60-session window shows %d, artifact %d" % (n, n60)
                res = {}
                if n60 >= 2:
                    res["sort_score"] = sort_both(page, "tbl", "score")
                    res["sort_vol"] = sort_both(page, "tbl", "vol_x")
                    res["sort_held"] = sort_both(page, "tbl", "held")
                    res["sort_desk"] = sort_both(page, "tbl", "desk", text=True)
                for d in desks:
                    page.click('#chips .jh-chip[data-desk="%s"]' % d)
                    page.wait_for_timeout(120)
                    m = page.evaluate("() => document.querySelectorAll('#tbl tbody tr a.tk').length")
                    want = min(80, (counts.get(d) or {}).get("prints_60") or 0)
                    assert m == want, "%s/60 shows %d, artifact %d" % (d, m, want)
                    res[d] = m
                page.click('#chips .jh-chip[data-desk="ALL"]')
                page.click('#window .jh-chip[data-win="LATEST"]')
                page.wait_for_timeout(150)
                return res
            facts["w60"] = check("the 60-session window and its desk chips match the artifact's boards", window60)

            def desk_row():
                page.click('#desks tbody tr[data-desk="stocks"]')
                page.wait_for_timeout(250)
                got = page.evaluate("() => [document.querySelector('#chips .jh-chip.on').getAttribute('data-desk'), document.querySelector('#window .jh-chip.on').getAttribute('data-win'), document.querySelectorAll('#tbl tbody tr a.tk').length]")
                assert got[0] == "stocks" and got[1] == "ALL", got
                page.click('#chips .jh-chip[data-desk="ALL"]')
                page.click('#window .jh-chip[data-win="LATEST"]')
                page.wait_for_timeout(150)
                return got
            check("a desk-table row jumps to that desk's 60-session board", desk_row)
            if out["hold"] >= 2:
                check("sort hold.score", lambda: sort_both(page, "hold-tbl", "score"))
                check("sort hold.vol_after", lambda: sort_both(page, "hold-tbl", "vol_after"))
            if out["bench"] >= 2:
                check("sort bench.date", lambda: sort_both(page, "bench-tbl", "date"))
            check("sort desks.prints_5", lambda: sort_both(page, "desks-tbl", "prints_5"))

            def tooltip():
                page.hover("#chart rect[data-i]:last-of-type")
                page.wait_for_timeout(200)
                t = page.evaluate("() => { const e = document.getElementById('tip'); return [getComputedStyle(e).display, e.innerText.slice(0, 160)]; }")
                assert t[0] == "block" and t[1], t
                return t
            check("breadth bar hover shows the session's tooltip", tooltip)

            def link():
                got = page.evaluate("() => { const a = document.querySelector('#tbl tbody tr a.tk'); return a ? [a.getAttribute('href'), a.getAttribute('target'), a.getAttribute('data-t'), a.textContent] : null; }")
                if got is None:
                    return "no print row to link (latest window empty)"
                assert got[0].startswith("/chart.html?s=") and got[1] == "_top" and got[2], got
                return got
            check("ticker links target the chart", link)

            def phone():
                page.set_viewport_size({"width": 390, "height": 844})
                page.wait_for_timeout(400)
                got = page.evaluate("() => [document.documentElement.scrollWidth - document.documentElement.clientWidth, document.querySelectorAll('#kpis .jh-kpi').length]")
                page.set_viewport_size({"width": 1440, "height": 900})
                assert got[0] == 0, "horizontal overflow %s px" % got[0]
                return "0 px overflow at 390 px (%d tiles)" % got[1]
            facts["overflow"] = check("390 px: no horizontal overflow", phone)
            facts["errors"] = errors
            check("0 page errors", lambda: (_ for _ in ()).throw(AssertionError(errors[:5])) if errors else "none")

            r.section("3. chart.html: the Capitulation button opens the desk")
            cpage = ctx.new_page()
            cerrors = []
            cpage.on("pageerror", lambda e: cerrors.append("pageerror: %s" % str(e)[:200]))
            cpage.goto("%s/chart.html?s=AAPL&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            cpage.wait_for_selector("#btn-capit", timeout=90000)
            cpage.wait_for_timeout(2500)
            cpage.wait_for_selector("#btn-capit", timeout=90000)

            def open_desk():
                cpage.mouse.move(720, 300)
                cpage.wait_for_timeout(300)
                cpage.mouse.move(720, 2)
                cpage.wait_for_function("() => { const s = document.getElementById('jh-ah-top'); return !document.documentElement.classList.contains('jh-ah') || (s && s.classList.contains('show')); }", timeout=15000)
                cpage.wait_for_timeout(400)
                before = cpage.evaluate("() => [document.getElementById('symchip-id').textContent.trim(), (document.querySelector('#tabs .tab.on') || {dataset: {}}).dataset.id || null, document.getElementById('btn-capit').title]")
                cpage.click("#btn-capit")
                cpage.wait_for_selector("#ws-overlay.on iframe", timeout=30000)
                head = cpage.evaluate("() => [document.querySelector('#ws-overlay .wshd span').textContent, document.querySelector('#ws-overlay iframe').getAttribute('src'), document.querySelector('#ws-overlay .wshd a').getAttribute('href')]")
                assert head[1] == "/capitulation-desk.html" and "Capitulation" in head[0], head
                fl = cpage.frame_locator("#ws-overlay iframe")
                fl.locator("#kpis .jh-kpi").first.wait_for(timeout=90000)
                cpage.wait_for_timeout(1500)
                inside = fl.locator("body").evaluate("""() => ({rows: document.querySelectorAll('#tbl tbody tr a.tk').length, bars: document.querySelectorAll('#chart rect[data-i]').length, tiles: document.querySelectorAll('#kpis .jh-kpi').length,
                                                             regime: (document.querySelector('#read .cp-regime') || {}).textContent, stamp: document.getElementById('stamp').innerText.replace(/\\n/g, ' | ')})""")
                assert inside["tiles"] == 6 and inside["bars"] == len(br.get("sessions") or []), inside
                facts["chart_shot"] = cpage.screenshot(type="jpeg", quality=60, scale="css")
                if inside["rows"] == 0:
                    fl.locator('#window .jh-chip[data-win="ALL"]').click()
                    cpage.wait_for_timeout(300)
                pick = fl.locator("#tbl tbody tr a.tk").first
                t = pick.get_attribute("data-t")
                pick.click()
                cpage.wait_for_function("(t) => { const ov = document.getElementById('ws-overlay'); const tab = document.querySelector('#tabs .tab.on'); return ov && ov.className !== 'on' && tab && tab.dataset.id === t; }",
                                        arg=re.sub(r"[^A-Z0-9.\-]", "", t.upper()), timeout=30000)
                after = cpage.evaluate("() => [document.getElementById('symchip-id').textContent.trim(), document.querySelector('#tabs .tab.on').dataset.id, document.getElementById('ws-overlay').className]")
                return {"before": before, "workspace": head, "inside": inside, "clicked": t, "after": after}
            facts["chart"] = check("the Capitulation button opens the desk; a ticker there loads in the chart", open_desk)
            r.log("chart.html page errors (not gated; the chart is its own page): %s" % (cerrors[:5] or "none"))

            r.section("4. screenshots (0.5x, jpeg, base64)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=0.5)
            p3 = ctx2.new_page()
            p3.goto("%s/capitulation-desk.html?nocache=%d" % (BASE, stamp + 2), wait_until="domcontentloaded")
            try:
                settle(p3)
            except Exception:  # noqa: BLE001
                pass
            shot = p3.screenshot(full_page=True, type="jpeg", quality=55)
            r.log("capitulation-desk.html screenshot bytes: %d; chart with the desk open: %d" % (len(shot), len(facts.get("chart_shot") or b"")))
            r.lines.append("")
            r.lines.append("<!-- SHOT_JPEG_BASE64 -->")
            r.lines.append(base64.b64encode(shot).decode("ascii"))
            r.lines.append("<!-- /SHOT_JPEG_BASE64 -->")
            if facts.get("chart_shot"):
                r.lines.append("<!-- CHART_SHOT_JPEG_BASE64 -->")
                r.lines.append(base64.b64encode(facts["chart_shot"]).decode("ascii"))
                r.lines.append("<!-- /CHART_SHOT_JPEG_BASE64 -->")
            r.lines.append("")
            browser.close()

        r.kv(session=str(art.get("session")), regime=str(br.get("regime")), sum_5=str(br.get("sum_5")), latest=str(n_latest), prints_60=str(n60), failed=str(len(fails)), errors=str(len(facts.get("errors") or [])), overflow=str(facts.get("overflow")), chart=str(bool(facts.get("chart"))))
        if fails:
            r.fail("NOT PROVEN: %d checks failed: %s" % (len(fails), fails))
            raise SystemExit(1)
        ch = facts["chart"]
        r.ok("CAPITULATION DESK PROVEN LIVE: session %s, regime %s (%s prints in 5 sessions, %s in 60) renders in order; the prints table sorts both ways, every desk chip and the 60-session window match the artifact, "
             "390 px and 0 errors pass; chart.html: the Capitulation button opens /capitulation-desk.html and clicking %s there switched the chart from %s to %s" % (
                 art.get("session"), br.get("regime"), br.get("sum_5"), n60, ch["clicked"], ch["before"][0], ch["after"][0]))


if __name__ == "__main__":
    main()
    sys.exit(0)
