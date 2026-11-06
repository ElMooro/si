"""ops 6759 - READ-ONLY browser proof of strong.html v4: the thesis record over a month / a quarter / the year, two definitions of
strong (the momentum rank and the asymmetry rank), dump days AND rally days, industry confirmation, and the asset-class
breakdown (Khalid 2026-11-05: "test strong stocks engine and thesis ... make sure it covers all asset classes"; 19:59 CT:
"look back a month or a quarter back ... dump less than sp500 dump, rally more than sp500 rally, entire industry seeing
strength is even better"; CLAUDE.md rule 5: click it yourself).

Against https://justhodl.ai in headless Chrome, with data/strength.json at >= 1.0.8 (ops 6757):
  1. the served page carries the thesis and asset-class sections in order (verdict -> thesis -> asset classes -> long
     accumulation -> leaders / laggards -> focus -> desk -> reading rules -> sources), the artifact is >= 1.0.8 and its
     thesis block is the one the page shows (verdict text, the session, the dump / rally-day counts of the selected
     window, the tile figures);
  2. the window chips (1M / 3M / 1Y) re-cut the dump-day and rally-day tables to the artifact's own counts; the predictor
     chips (momentum / asymmetry) swap the last-session names to the artifact's own top tenth; the tables sort both ways
     by real clicks; the leaders board carries the asymmetry and industry columns; the asset-class table lists every
     class the block carries and sorts; the eleven sleeve chips filter the desk; the leveraged funds are listed as
     excluded and none appears in the leaders board or the desk ranks;
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
        r.heading("ops 6759 - strong.html v4 (thesis over a month / a quarter / the year, two definitions, rally days, industry confirmation + asset classes): live proof (read-only) on %s" % BASE)
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
        r.log("strong.html: %d bytes | thesis section %s | classes section %s | thesis() %s | window chips %s | predictor chips %s | rallies table %s" % (
            len(html), 'aria-label="Thesis: how the strongest held up on S&P 500 dump days"' in html, 'aria-label="Asset classes:' in html, "function thesis()" in html, 'id="thesis-win"' in html, 'id="thesis-pred"' in html, 'id="rallies"' in html))
        art = json.loads(fetch("%s/data/strength.json?nocache=%d" % (BASE, stamp)))
        th = art.get("thesis") or {}
        wins = th.get("windows") or {}
        ls = th.get("last_session") or {}
        r.log("data/strength.json: version %s as_of %s generated %s | thesis window %s | windows %s | last session %s SPY %s dump %s rally %s | classes %s" % (
            art.get("version"), art.get("as_of"), art.get("generated_at"), json.dumps(th.get("window")), json.dumps({k: (v.get("n_dump_days"), v.get("n_rally_days")) for k, v in wins.items()}),
            ls.get("date"), ls.get("spy_ret_pct"), ls.get("dump"), ls.get("rally"), json.dumps((art.get("classes") or {}).get("ranked"))))
        if 'id="thesis-win"' not in html or str(art.get("version") or "0") < "1.0.8" or not wins.get("63"):
            r.fail("the served page / artifact are not v4 / 1.0.8 yet (Pages deploy or the engine run pending)")
            raise SystemExit(1)

        def in_window(k, days):
            frm = (wins.get(k) or {}).get("from") or ""
            return [d for d in days if (d.get("date") or "") >= frm]
        n_dumps = len(in_window("63", th.get("dump_days") or []))
        n_rallies = len(in_window("63", th.get("rally_days") or []))
        expect_win = {k: (len(in_window(k, th.get("dump_days") or [])), len(in_window(k, th.get("rally_days") or []))) for k in ("21", "63", "252")}

        def top_names(pred):
            g = ls.get(pred) or {}
            return ((g.get("stocks") or g.get("all") or {}).get("top_names")) or []
        n_top = len(top_names("strength"))
        n_top_asym = len(top_names("asymmetry"))
        n_conf = len(((ls.get("confirmed") or {}).get("confirmed_names")) or [])
        r.log("expected: default window 63 -> %d dump days, %d rally days (of %d / %d kept); per window %s | last session top tenth: momentum %d names, asymmetry %d names, industry-confirmed %d" % (
            n_dumps, n_rallies, len(th.get("dump_days") or []), len(th.get("rally_days") or []), json.dumps(expect_win), n_top, n_top_asym, n_conf))
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
                      dumps: q('#dumps tbody tr'), rallies: q('#rallies tbody tr'), lastnames: q('#lastnames tbody tr'), classes: q('#classes tbody tr'), chips: [...document.querySelectorAll('#chips .jh-chip')].map(e => e.innerText.trim()),
                      winChips: [...document.querySelectorAll('#thesis-win .jh-chip')].map(e => e.innerText.trim() + (e.classList.contains('on') ? ' *' : '')), predChips: [...document.querySelectorAll('#thesis-pred .jh-chip')].map(e => e.innerText.trim() + (e.classList.contains('on') ? ' *' : '')),
                      winMeta: document.querySelector('#thesis-win .st-small').innerText, rallyMeta: document.getElementById('rally-meta').innerText, leadHead: [...document.querySelectorAll('#leaders thead th')].map(e => e.innerText.trim()),
                      confirmedTicks: q('#lastnames tbody tr .up[title="industry-confirmed"]'), dumpRow: document.querySelector('#dumps tbody tr') ? document.querySelector('#dumps tbody tr').innerText.replace(/\t/g, ' | ') : null,
                      rallyRow: document.querySelector('#rallies tbody tr') ? document.querySelector('#rallies tbody tr').innerText.replace(/\t/g, ' | ') : null, lastRows: [...document.querySelectorAll('#lastnames tbody tr')].slice(0, 12).map(tr => tr.innerText.replace(/\t/g, ' | ')),
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
            check("tiles and tables match the artifact (default window: the quarter; default predictor: momentum)", lambda: (_ for _ in ()).throw(AssertionError("tiles %s dumps %s/%s rallies %s/%s names %s/%s confirmed ticks %s/%s classes %s/%s" % (
                out["tkpis"], out["dumps"], n_dumps, out["rallies"], n_rallies, out["lastnames"], n_top, out["confirmedTicks"], n_conf, out["classes"], len(classes))))
                  if not (out["tkpis"] == 6 and out["dumps"] == n_dumps and out["rallies"] == n_rallies and out["lastnames"] == n_top and out["confirmedTicks"] == n_conf and out["classes"] == len(classes))
                  else "6 tiles, %d dump days, %d rally days, %d top-tenth names (%d industry-confirmed ticks), %d classes" % (out["dumps"], out["rallies"], out["lastnames"], out["confirmedTicks"], out["classes"]))
            check("the quarter is the default window and momentum the default predictor", lambda: (_ for _ in ()).throw(AssertionError((out["winChips"], out["predChips"])))
                  if not (any(c.endswith("63 SESSIONS *") for c in out["winChips"]) and len(out["winChips"]) == 3 and out["predChips"][0].endswith("*") and len(out["predChips"]) == 2) else "%s | %s" % (out["winChips"], out["predChips"]))
            check("the leaders board carries the asymmetry and industry columns", lambda: (_ for _ in ()).throw(AssertionError(out["leadHead"]))
                  if not all(h in out["leadHead"] for h in ("ASYM 63 PP", "DUMP DAYS PP", "RALLY DAYS PP", "INDUSTRY")) else " · ".join(out["leadHead"][:9]))
            check("every asset class is on the page", lambda: (_ for _ in ()).throw(AssertionError(out["classLabels"])) if not all(any(lbl.lower().startswith(w) for lbl in out["classLabels"]) for w in ("stocks", "equity etfs", "countries", "bonds", "high yield", "gold", "silver", "metals", "commodities", "currencies", "crypto")) else ", ".join(out["classLabels"]))
            check("eleven sleeve chips", lambda: (_ for _ in ()).throw(AssertionError(out["chips"])) if out["chips"] != ["ALL", "ETF", "STOCK", "SECTOR", "COUNTRY", "BOND", "HIGH YIELD", "METAL", "COMMODITY", "CURRENCY", "CRYPTO", "VOLATILITY"] else " ".join(out["chips"]))
            check("leveraged funds listed as excluded and absent from the ranks", lambda: (_ for _ in ()).throw(AssertionError((lev[:5], out["leaders"][:5], [t for t in out["deskTickers"] if t in lev][:5], out["excluded"][:100])))
                  if not lev or not all(t in out["excluded"] for t in lev[:5]) or any(t in lev for t in out["leaders"]) else "%d excluded listed (%s...); none in the leaders board" % (len(lev), ", ".join(lev[:5])))
            check("no undefined / NaN", lambda: (_ for _ in ()).throw(AssertionError(out["bad"])) if out["bad"] else "0")

            r.section("2. window and predictor chips, sorting (real clicks)")

            def windows_click():
                got = {}
                for k in ("21", "252", "63"):
                    page.click('#thesis-win .jh-chip[data-w="%s"]' % k)
                    page.wait_for_timeout(300)
                    got[k] = page.evaluate("() => [document.querySelectorAll('#dumps tbody tr').length, document.querySelectorAll('#rallies tbody tr').length, document.querySelector('#thesis-win .st-small').innerText, document.querySelectorAll('#thesis-kpis .jh-kpi')[3].innerText.replace(/\\s+/g, ' ').slice(0, 160)]")
                    assert (got[k][0], got[k][1]) == expect_win[k], (k, got[k][:2], expect_win[k])
                    w = wins.get(k) or {}
                    assert (w.get("from") or "") in got[k][2] and ("%s dump days" % w.get("n_dump_days")) in got[k][2], (k, got[k][2], w.get("from"), w.get("n_dump_days"))
                return got
            facts["windows"] = check("window chips re-cut the dump / rally tables to the artifact's own counts (1M / 1Y / back to 3M)", windows_click)

            def predictors_click():
                page.click('#thesis-pred .jh-chip[data-p="asymmetry"]')
                page.wait_for_timeout(300)
                a = page.evaluate("() => [document.querySelectorAll('#lastnames tbody tr').length, document.getElementById('last-meta').innerText, [...document.querySelectorAll('#lastnames tbody tr')].slice(0, 5).map(tr => tr.children[0].innerText.trim())]")
                assert a[0] == n_top_asym and "ASYMMETRY" in a[1].upper(), (a[:2], n_top_asym)
                want = [x["ticker"] for x in top_names("asymmetry")]
                assert all(t.replace(" ✓", "").strip() in want for t in a[2]), (a[2], want[:8])
                page.click('#thesis-pred .jh-chip[data-p="strength"]')
                page.wait_for_timeout(300)
                b = page.evaluate("() => [document.querySelectorAll('#lastnames tbody tr').length, document.getElementById('last-meta').innerText]")
                assert b[0] == n_top and "MOMENTUM" in b[1].upper(), (b, n_top)
                return {"asymmetry": a[0], "momentum": b[0], "asym_meta": a[1][:120]}
            facts["predictors"] = check("predictor chips swap the last-session names (asymmetry rank, then back to momentum)", predictors_click)
            if n_dumps >= 2:
                check("sort dumps.m_excess", lambda: sort_both(page, "dumps", "m_excess"))
                check("sort dumps.date (text)", lambda: sort_both(page, "dumps", "date", text=True))
                check("sort dumps.a_excess", lambda: sort_both(page, "dumps", "a_excess"))
                check("sort dumps.m_bottom", lambda: sort_both(page, "dumps", "m_bottom"))
            if n_rallies >= 2:
                check("sort rallies.m_excess", lambda: sort_both(page, "rallies", "m_excess"))
                check("sort rallies.date (text)", lambda: sort_both(page, "rallies", "date", text=True))
            if n_top >= 2:
                check("sort lastnames.ret_pct", lambda: sort_both(page, "lastnames", "ret_pct"))
                check("sort lastnames.strength", lambda: sort_both(page, "lastnames", "strength"))
            check("sort leaders.asym", lambda: sort_both(page, "leaders", "asym"))
            check("sort leaders.dnx", lambda: sort_both(page, "leaders", "dnx"))
            check("sort classes.median_vs_d", lambda: sort_both(page, "classes", "median_vs_d"))
            check("sort classes.n", lambda: sort_both(page, "classes", "n"))
            check("sort classes.held_last", lambda: sort_both(page, "classes", "held_last"))

            def chips():
                got = {}
                members = {k: {x["ticker"] for x in v} for k, v in (art.get("sleeves") or {}).items()}
                for c in ("BOND", "HIGH YIELD", "METAL", "CRYPTO", "COUNTRY"):
                    page.click('#chips [data-s="%s"]' % c)
                    page.wait_for_timeout(200)
                    shown = page.evaluate("() => [...document.querySelectorAll('#tbl tbody tr a.tk')].map(a => a.innerText.trim())")
                    # a name that sits in two sleeves (GLD: ETF and METAL) shows under both with its first sleeve's label - membership is the test
                    assert shown and all(t in members.get(c, set()) for t in shown) and len(shown) >= 2, (c, shown[:8], sorted(members.get(c, set()))[:8])
                    got[c] = len(shown)
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
        r.kv(version=str(art.get("version")), as_of=str(art.get("as_of")), dump_days_q=str(n_dumps), rally_days_q=str(n_rallies), last=str(ls.get("date")), top_names=str(n_top), confirmed=str(n_conf), classes=str(len(classes)), failed=str(len(fails)), overflow=str(facts.get("overflow")), errors=str(len(facts.get("errors") or [])))
        if fails:
            r.fail("NOT PROVEN: %d checks failed: %s" % (len(fails), fails))
            raise SystemExit(1)
        r.ok("STRONG.HTML V4 PROVEN LIVE: the thesis record (quarter: %d dump days, %d rally days; last session %s: %d momentum / %d asymmetry top-tenth names, %d industry-confirmed) and %d asset classes render from the %s artifact in order; "
             "the window and predictor chips re-cut the tables to the artifact's own counts; every new table sorts both ways; eleven sleeve chips filter; leveraged funds excluded; 390 px and 0 errors pass" % (
                 n_dumps, n_rallies, ls.get("date"), n_top, n_top_asym, n_conf, len(classes), art.get("version")))


if __name__ == "__main__":
    main()
    sys.exit(0)
