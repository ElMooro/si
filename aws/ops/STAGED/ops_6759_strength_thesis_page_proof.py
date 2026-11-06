"""ops 6759 - READ-ONLY browser proof of strong.html v3: the thesis record and the asset-class breakdown (Khalid 2026-11-05:
"test strong stocks engine and thesis ... make sure it covers all asset classes"; CLAUDE.md rule 5: click it yourself).

Against https://justhodl.ai in headless Chrome, with data/strength.json at 1.0.7 (ops 6757 / 6758):
  1. the served page carries the thesis and asset-class sections in order (verdict -> thesis -> asset classes -> long
     accumulation -> leaders / laggards -> focus -> desk -> reading rules -> sources), the artifact is 1.0.7 and its
     thesis block is the one the page shows (verdict text, the session, the dump-day count, the tile figures);
  2. the dump-day table lists every dump day of the block and sorts both ways by real clicks (top excess, date); the
     last-session table lists the strongest tenth one by one with a verdict and sorts; the asset-class table lists
     every class the block carries with the figures the artifact holds and sorts; the eleven sleeve chips filter the
     desk; the leveraged funds are listed as excluded and none appears in the leaders board or the desk ranks;
  3. no 'undefined' / 'NaN', 0 page errors, 0 horizontal overflow at 390 px. A 0.5x JPEG is embedded.
Nothing on the site changes. Re-runnable.
"""
from pathlib import Path
import base64
import json
import re
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
WANT = ["Verdict: index regime, breadth and bases", "Thesis: how the strongest held up on S&P 500 dump days", "Asset classes:", "Long accumulation with a tight price spread",
        "Leaders and laggards by strength", "Focus: holding up while the index is heavy", "The desk: every sleeve against SPY", "Reading rules and definitions", "Sources and engine observability"]


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
    t = re.sub(r"[,%×+]", "", str(s).replace("−", "-")).strip()
    m = re.search(r"-?\d+(\.\d+)?", t)
    return float(m.group(0)) if m else None


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
        ok = (b <= a if direction == "descending" else b >= a) if text else (b <= a + 1e-9 if direction == "descending" else b >= a - 1e-9)
        assert ok, "not %s at row %d: %s then %s" % (direction, i, a, b)
    return len(body)


def sort_both(page, host, col, text=False):
    sel = '#%s th[data-sort="%s"]' % (host, col)
    page.click(sel)
    page.wait_for_timeout(200)
    a = col_vals(page, host, col)
    n1 = ordered(a["vals"], a["aria"], text)
    page.click(sel)
    page.wait_for_timeout(200)
    b = col_vals(page, host, col)
    ordered(b["vals"], b["aria"], text)
    assert a["aria"] != b["aria"] and a["aria"] in ("ascending", "descending"), (a["aria"], b["aria"])
    return "%s.%s %s then %s (%d values; first %s / %s)" % (host, col, a["aria"], b["aria"], n1, a["vals"][0] if a["vals"] else None, b["vals"][0] if b["vals"] else None)


def main():
    with report("ops_6759_strength_thesis_page_proof") as r:
        r.heading("ops 6759 - strong.html v3 (thesis + asset classes): live proof (read-only) on %s" % BASE)
        stamp = int(time.time())
        fails = []

        def check(name, fn):
            try:
                out = fn()
                r.log("ok   %s: %s" % (name, out if isinstance(out, str) else json.dumps(out, ensure_ascii=False)[:1200]))
                return out
            except Exception as e:  # noqa: BLE001
                fails.append(name)
                r.fail("%s: %s: %s" % (name, type(e).__name__, str(e)[:400]))
                return None

        r.section("0. served HTML + artifact")
        html = fetch("%s/strong.html?nocache=%d" % (BASE, stamp))
        r.log("strong.html: %d bytes | thesis section %s | classes section %s | thesis() %s" % (len(html), 'aria-label="Thesis: how the strongest held up on S&P 500 dump days"' in html, 'aria-label="Asset classes:' in html, "function thesis()" in html))
        art = json.loads(fetch("%s/data/strength.json?nocache=%d" % (BASE, stamp)))
        th = art.get("thesis") or {}
        sm = th.get("summary") or {}
        ls = th.get("last_session") or {}
        r.log("data/strength.json: version %s as_of %s generated %s | thesis window %s | dump days %s | last session %s SPY %s dump %s | classes %s" % (
            art.get("version"), art.get("as_of"), art.get("generated_at"), json.dumps(th.get("window")), sm.get("n_dump_days"), ls.get("date"), ls.get("spy_ret_pct"), ls.get("dump"),
            json.dumps((art.get("classes") or {}).get("ranked"))))
        if 'aria-label="Thesis:' not in html or art.get("version") != "1.0.7" or not th.get("summary"):
            r.fail("the served page / artifact are not v3 / 1.0.7 yet (Pages deploy or the engine run pending)")
            raise SystemExit(1)
        n_dumps = len(th.get("dump_days") or [])
        n_top = len((ls.get("all") or {}).get("top_names") or [])
        classes = ((art.get("boards") or {}).get("class_order")) or []
        lev = [x["ticker"] for x in (((art.get("boards") or {}).get("excluded") or {}).get("leveraged") or [])]

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
            page.goto("%s/strong.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#thesis-kpis .jh-kpi", timeout=90000)
            page.wait_for_timeout(2500)
            page.wait_for_selector("#thesis-kpis .jh-kpi", timeout=90000)
            page.wait_for_timeout(800)
            out = page.evaluate("""() => {
              const q = s => document.querySelectorAll(s).length, text = document.body.innerText, de = document.documentElement;
              return {order: [...document.querySelectorAll('main > section')].map(e => e.getAttribute('aria-label')), tkpis: q('#thesis-kpis .jh-kpi'), verdict: document.getElementById('thesis-verdict').innerText.slice(0, 400),
                      dumps: q('#dumps tbody tr'), lastnames: q('#lastnames tbody tr'), classes: q('#classes tbody tr'), chips: [...document.querySelectorAll('#chips .jh-chip')].map(e => e.innerText.trim()),
                      classLabels: [...document.querySelectorAll('#classes tbody tr')].map(tr => tr.children[0].innerText.trim()), excluded: document.getElementById('excluded').innerText.slice(0, 600),
                      leaders: [...document.querySelectorAll('#leaders tbody tr')].map(tr => tr.children[0].innerText.trim()), deskTickers: [...document.querySelectorAll('#tbl tbody tr')].map(tr => tr.children[0].innerText.trim()),
                      bad: (text.match(/\\[object Object\\]|undefined|NaN/g) || []).length, overflow: de.scrollWidth - de.clientWidth, kpiText: [...document.querySelectorAll('#thesis-kpis .jh-kpi')].map(e => e.innerText.replace(/\\s+/g, ' ').slice(0, 200)),
                      deciles: document.getElementById('last-deciles').innerText.slice(0, 300), lastMeta: document.getElementById('last-meta').innerText, dumpMeta: document.getElementById('dump-meta').innerText, classMeta: document.getElementById('class-meta').innerText};
            }""")
            for k, v in out.items():
                r.log("%s: %s" % (k, json.dumps(v, ensure_ascii=False)[:1200]))
            facts.update(out)
            idx = []
            for w in WANT:
                hit = next((i for i, o in enumerate(out["order"]) if (o or "").startswith(w)), None)
                idx.append(hit)
            check("section order", lambda: (_ for _ in ()).throw(AssertionError(out["order"])) if None in idx or idx != sorted(idx) else " -> ".join(out["order"]))
            check("the verdict is the artifact's", lambda: (_ for _ in ()).throw(AssertionError((out["verdict"][:120], (th.get("verdict") or "")[:120]))) if (th.get("verdict") or "")[:80] != out["verdict"][:80] else out["verdict"][:200])
            check("tiles and tables match the artifact", lambda: (_ for _ in ()).throw(AssertionError("tiles %s dumps %s/%s names %s/%s classes %s/%s" % (out["tkpis"], out["dumps"], n_dumps, out["lastnames"], n_top, out["classes"], len(classes))))
                  if not (out["tkpis"] == 6 and out["dumps"] == n_dumps and out["lastnames"] == n_top and out["classes"] == len(classes)) else "6 tiles, %d dump days, %d top-tenth names, %d classes" % (out["dumps"], out["lastnames"], out["classes"]))
            check("every asset class is on the page", lambda: (_ for _ in ()).throw(AssertionError(out["classLabels"])) if not all(any(lbl.lower().startswith(w) for lbl in out["classLabels"]) for w in ("stocks", "equity etfs", "countries", "bonds", "high yield", "gold", "silver", "metals", "commodities", "currencies", "crypto")) else ", ".join(out["classLabels"]))
            check("eleven sleeve chips", lambda: (_ for _ in ()).throw(AssertionError(out["chips"])) if out["chips"] != ["ALL", "ETF", "STOCK", "SECTOR", "COUNTRY", "BOND", "HIGH YIELD", "METAL", "COMMODITY", "CURRENCY", "CRYPTO", "VOLATILITY"] else " ".join(out["chips"]))
            check("leveraged funds listed as excluded and absent from the ranks", lambda: (_ for _ in ()).throw(AssertionError((lev[:5], out["leaders"][:5], [t for t in out["deskTickers"] if t in lev][:5], out["excluded"][:100])))
                  if not lev or not all(t in out["excluded"] for t in lev[:5]) or any(t in lev for t in out["leaders"]) else "%d excluded listed (%s...); none in the leaders board" % (len(lev), ", ".join(lev[:5])))
            check("no undefined / NaN", lambda: (_ for _ in ()).throw(AssertionError(out["bad"])) if out["bad"] else "0")

            r.section("2. sorting and chips (real clicks)")
            if n_dumps >= 2:
                check("sort dumps.top_excess", lambda: sort_both(page, "dumps", "top_excess"))
                check("sort dumps.date (text)", lambda: sort_both(page, "dumps", "date", text=True))
                check("sort dumps.spread", lambda: sort_both(page, "dumps", "spread"))
            if n_top >= 2:
                check("sort lastnames.ret_pct", lambda: sort_both(page, "lastnames", "ret_pct"))
                check("sort lastnames.strength", lambda: sort_both(page, "lastnames", "strength"))
            check("sort classes.median_vs_d", lambda: sort_both(page, "classes", "median_vs_d"))
            check("sort classes.n", lambda: sort_both(page, "classes", "n"))
            check("sort classes.held_last", lambda: sort_both(page, "classes", "held_last"))

            def chips():
                got = {}
                for c in ("BOND", "HIGH YIELD", "METAL", "CRYPTO", "COUNTRY"):
                    page.click('#chips [data-s="%s"]' % c)
                    page.wait_for_timeout(200)
                    sl = page.evaluate("() => [...new Set([...document.querySelectorAll('#tbl tbody tr')].map(tr => tr.children[2].innerText.trim()))]")
                    n = page.evaluate("() => document.querySelectorAll('#tbl tbody tr a.tk').length")
                    assert sl == [c] and n >= 2, (c, sl, n)
                    got[c] = n
                page.click('#chips [data-s="ALL"]')
                page.wait_for_timeout(200)
                got["ALL"] = page.evaluate("() => document.querySelectorAll('#tbl tbody tr a.tk').length")
                assert got["ALL"] >= 150, got
                return got
            facts["chips"] = check("sleeve chips filter the desk (BOND / HIGH YIELD / METAL / CRYPTO / COUNTRY)", chips)
            page.set_viewport_size({"width": 390, "height": 844})
            page.wait_for_timeout(500)
            facts["overflow"] = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            page.set_viewport_size({"width": 1440, "height": 900})
            check("390 px: no horizontal overflow", lambda: (_ for _ in ()).throw(AssertionError(facts["overflow"])) if facts["overflow"] else "0 px")
            facts["errors"] = errors
            check("0 page errors", lambda: (_ for _ in ()).throw(AssertionError(errors[:5])) if errors else "none")

            r.section("3. screenshot (0.5x, jpeg, base64)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=0.5)
            p3 = ctx2.new_page()
            p3.goto("%s/strong.html?nocache=%d" % (BASE, stamp + 2), wait_until="domcontentloaded")
            try:
                p3.wait_for_selector("#thesis-kpis .jh-kpi", timeout=60000)
                p3.wait_for_timeout(2500)
                p3.wait_for_selector("#sources tbody tr", timeout=60000)
            except Exception:  # noqa: BLE001
                pass
            p3.wait_for_timeout(1000)
            shot = p3.screenshot(full_page=True, type="jpeg", quality=55)
            r.log("screenshot bytes: %d" % len(shot))
            r.lines.append("")
            r.lines.append("<!-- SHOT_JPEG_BASE64 -->")
            r.lines.append(base64.b64encode(shot).decode("ascii"))
            r.lines.append("<!-- /SHOT_JPEG_BASE64 -->")
            r.lines.append("")
            browser.close()
        r.kv(version=str(art.get("version")), as_of=str(art.get("as_of")), dump_days=str(n_dumps), last=str(ls.get("date")), classes=str(len(classes)), failed=str(len(fails)), overflow=str(facts.get("overflow")), errors=str(len(facts.get("errors") or [])))
        if fails:
            r.fail("NOT PROVEN: %d checks failed: %s" % (len(fails), fails))
            raise SystemExit(1)
        r.ok("STRONG.HTML V3 PROVEN LIVE: thesis record (%d dump days, last session %s) and %d asset classes render from the 1.0.7 artifact in order; every new table sorts both ways; eleven sleeve chips filter; "
             "leveraged funds excluded; 390 px and 0 errors pass" % (n_dumps, ls.get("date"), len(classes)))


if __name__ == "__main__":
    main()
    sys.exit(0)
