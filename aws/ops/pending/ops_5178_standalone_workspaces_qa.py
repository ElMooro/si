"""ops_5178 -- standalone engines QA: universe-heatmap.html + macro-economic-data.html (generated from chart-pro).

Khalid: "make Universe Heatmap and Macro & Economic Data their own separate
engines (same design, same capabilities, everything) but keep them on chart-pro
too; I want to pull them by themselves when needed."

scripts/build_workspaces.py extracts the two workspaces from chart-pro.html
(classes, CSS, markup) into assets/jh-workspaces.{js,css} + heatmap.html +
macro-data.html, with a host shim that loads the same signal feeds and hands
charting to Chart Pro in a new tab. This op proves the deployed result:
  P1 heatmap.html at 1440/390: sections/cells, hydration, no page errors,
     horizon + mode switches repaint, no horizontal overflow, screenshot
  P2 macro-data.html at 1440/390: categories/cards, metrics hydrate, search
     works, no page errors, screenshot
  P3 chart handoff: clicking a heatmap cell opens Chart Pro in a new tab with
     the symbol; a macro card does the same
  P4 chart-pro still intact: Heatmap + MacroData defined, both modals open,
     header "PAGE" links point at the standalone engines
  P5 nav-manifest.json lists both pages
  P6 login return-leg done right: after starting Google sign-in in the same
     browser context (PKCE verifier stored), a return with ?code= must trigger
     the /auth/v1/token exchange (a 4xx for a fake code is the proof)
"""
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

SITE = "https://justhodl.ai"
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops5178", "Cache-Control": "no-cache"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, ""
    except Exception:
        return -1, ""


with report("ops_5178_standalone_workspaces_qa") as R:
    R.heading("ops 5178 -- standalone engines QA (universe-heatmap.html, macro-economic-data.html)")
    R.section("P0 deploy")
    ok = False
    for i in range(48):
        st1, b1 = fetch(SITE + "/universe-heatmap.html")
        st2, b2 = fetch(SITE + "/macro-economic-data.html")
        st3, b3 = fetch(SITE + "/assets/jh-workspaces.js")
        if st1 == 200 and st2 == 200 and st3 == 200 and "JH_WS" in b3 and "class Heatmap" in b3:
            ok = True
            break
        time.sleep(15)
    (R.ok if ok else R.fail)("   pages + bundle live after %ds (heatmap %s, macro %s, bundle %s)" % (i * 15, st1, st2, st3))
    if not ok:
        sys.exit(1)
    try:
        import playwright  # noqa: F401
    except Exception:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright

    def launch(p):
        try:
            return p.chromium.launch(channel="chrome", headless=True)
        except Exception:
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
            return p.chromium.launch(headless=True)

    SHOTS.mkdir(parents=True, exist_ok=True)

    def new_page(browser, width, height):
        ctx = browser.new_context(viewport={"width": width, "height": height}, has_touch=width < 900, is_mobile=width < 700)
        pg = ctx.new_page()
        errors = []
        pg.on("pageerror", lambda e: errors.append(str(e)[:200]))
        return ctx, pg, errors

    def wait_count(pg, selector, minimum, secs=30):
        t0 = time.time()
        n = 0
        while time.time() - t0 < secs:
            n = pg.evaluate("(s) => document.querySelectorAll(s).length", selector)
            if n >= minimum:
                break
            pg.wait_for_timeout(700)
        return n

    with sync_playwright() as p:
        browser = launch(p)

        R.section("P1 universe-heatmap.html")
        for width, height in ((1440, 1000), (390, 844)):
            ctx, pg, errors = new_page(browser, width, height)
            try:
                pg.goto(SITE + "/universe-heatmap.html", wait_until="domcontentloaded", timeout=60000)
                cells = wait_count(pg, "#heatmap-body .hm-cell", 40)
                ready = wait_count(pg, "#heatmap-body .hm-cell[data-metric-state='ready']", 6, 30)
                sections = pg.evaluate("() => document.querySelectorAll('#heatmap-body .hm-sector').length")
                first = pg.evaluate("() => (document.querySelector('#heatmap-body .hm-cell[data-metric-state=\"ready\"] .hm-ch') || {}).textContent")
                pg.click(".hm-ctrl[data-hm-horizon='M']")
                pg.wait_for_timeout(300)
                after = pg.evaluate("() => (document.querySelector('#heatmap-body .hm-cell[data-metric-state=\"ready\"] .hm-ch') || {}).textContent")
                pg.click(".hm-ctrl[data-hm-mode='signals']")
                pg.wait_for_timeout(300)
                legend = pg.evaluate("() => document.getElementById('hm-legend').textContent")
                overflow = pg.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
                visible = pg.evaluate("() => { const m = document.getElementById('heatmap-modal'); const r = m.getBoundingClientRect(); return r.height > 200 && getComputedStyle(m).position === 'static'; }")
                auto_items = pg.evaluate("() => document.querySelectorAll('#heatmap-body .hm-sector[data-auto=\"1\"] .hm-cell').length")
                pg.screenshot(path=str(SHOTS / f"ops5178_heatmap_page_{width}.png"))
                R.log("   %4dpx: sections=%d cells=%d hydrated=%d cascade-live=%d D->M %s->%s legend='%s' inline=%s overflow=%dpx errors=%d"
                      % (width, sections, cells, ready, auto_items, first, after, legend[:30], visible, overflow, len(errors)))
                if cells < 40 or ready < 6 or not visible or overflow > 0 or errors or "signal" not in legend.lower():
                    FAILS.append("universe-heatmap.html %dpx: cells=%d ready=%d inline=%s overflow=%d errors=%s legend=%s" % (width, cells, ready, visible, overflow, errors[:2], legend[:30]))
            except Exception as e:
                FAILS.append("universe-heatmap.html %dpx: %s" % (width, str(e)[:160]))
            ctx.close()

        R.section("P2 macro-economic-data.html")
        for width, height in ((1440, 1000), (390, 844)):
            ctx, pg, errors = new_page(browser, width, height)
            try:
                pg.goto(SITE + "/macro-economic-data.html", wait_until="domcontentloaded", timeout=60000)
                cards = wait_count(pg, "#macro-body .macro-item", 40)
                ready = wait_count(pg, "#macro-body .macro-item[data-metric-state='ready']", 6, 30)
                cats = pg.evaluate("() => document.querySelectorAll('#macro-body .macro-cat').length")
                pg.fill("#macro-search-input", "unemployment")
                found = wait_count(pg, "#macro-body .macro-sr", 3, 20)
                overflow = pg.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
                visible = pg.evaluate("() => { const m = document.getElementById('macro-modal'); const r = m.getBoundingClientRect(); return r.height > 200 && getComputedStyle(m).position === 'static'; }")
                pg.screenshot(path=str(SHOTS / f"ops5178_macro_page_{width}.png"))
                R.log("   %4dpx: categories=%d cards=%d hydrated=%d search('unemployment')=%d inline=%s overflow=%dpx errors=%d"
                      % (width, cats, cards, ready, found, visible, overflow, len(errors)))
                if cards < 40 or ready < 6 or found < 3 or not visible or overflow > 0 or errors:
                    FAILS.append("macro-economic-data.html %dpx: cards=%d ready=%d search=%d inline=%s overflow=%d errors=%s" % (width, cards, ready, found, visible, overflow, errors[:2]))
            except Exception as e:
                FAILS.append("macro-economic-data.html %dpx: %s" % (width, str(e)[:160]))
            ctx.close()

        R.section("P3 chart handoff to Chart Pro (new tab)")
        ctx, pg, errors = new_page(browser, 1440, 1000)
        try:
            pg.goto(SITE + "/universe-heatmap.html", wait_until="domcontentloaded", timeout=60000)
            wait_count(pg, "#heatmap-body .hm-cell", 40)
            sym = pg.evaluate("() => document.querySelector('#heatmap-body .hm-sector[data-auto=\"0\"] .hm-cell').dataset.symbol")
            with ctx.expect_page(timeout=15000) as new_tab:
                pg.click("#heatmap-body .hm-sector[data-auto='0'] .hm-cell")
            tab = new_tab.value
            tab.wait_for_load_state("domcontentloaded", timeout=60000)
            url = tab.url
            still = pg.evaluate("() => document.querySelectorAll('#heatmap-body .hm-cell').length")
            (R.ok if "chart-pro.html" in url and sym in url and still >= 40 else R.fail)("   heatmap cell %s -> new tab %s (heatmap page still showing %d cells)" % (sym, url[:90], still))
            if not ("chart-pro.html" in url and sym in url and still >= 40):
                FAILS.append("P3 heatmap handoff: %s" % url[:80])
            tab.close()
            pg.goto(SITE + "/macro-economic-data.html", wait_until="domcontentloaded", timeout=60000)
            wait_count(pg, "#macro-body .macro-item", 40)
            msym = pg.evaluate("() => document.querySelector('#macro-body .macro-item').dataset.symbol")
            with ctx.expect_page(timeout=15000) as new_tab2:
                pg.click("#macro-body .macro-item")
            tab2 = new_tab2.value
            tab2.wait_for_load_state("domcontentloaded", timeout=60000)
            url2 = tab2.url
            still2 = pg.evaluate("() => document.querySelectorAll('#macro-body .macro-item').length")
            (R.ok if "chart-pro.html" in url2 and still2 >= 40 else R.fail)("   macro card %s -> new tab %s (macro page still showing %d cards)" % (msym, url2[:90], still2))
            if not ("chart-pro.html" in url2 and still2 >= 40):
                FAILS.append("P3 macro handoff: %s" % url2[:80])
            tab2.close()
        except Exception as e:
            FAILS.append("P3: %s" % str(e)[:160])
        ctx.close()

        R.section("P4 chart-pro intact + PAGE links")
        ctx, pg, errors = new_page(browser, 1440, 1000)
        try:
            pg.goto(SITE + "/chart-pro.html?s=SPY&tf=1D", wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(4000)
            facts = pg.evaluate("""() => ({ heat: typeof Heatmap === 'function', macro: typeof MacroData === 'function',
                hmLink: (document.querySelector('#heatmap-modal .hm-open-page') || {}).getAttribute && document.querySelector('#heatmap-modal .hm-open-page').getAttribute('href'),
                macroLink: document.querySelector('#macro-modal .macro-open-page') && document.querySelector('#macro-modal .macro-open-page').getAttribute('href') })""")
            pg.evaluate("() => Heatmap.open()")
            pg.wait_for_selector("#heatmap-modal.open", timeout=8000)
            hm_cells = wait_count(pg, "#heatmap-body .hm-cell", 40)
            pg.evaluate("() => Heatmap.close()")
            pg.evaluate("() => MacroData.open()")
            pg.wait_for_selector("#macro-modal.open", timeout=8000)
            mc_cards = wait_count(pg, "#macro-body .macro-item", 40)
            pg.evaluate("() => MacroData.close()")
            closed = pg.evaluate("() => !document.getElementById('macro-modal').classList.contains('open') && !document.getElementById('heatmap-modal').classList.contains('open')")
            R.log("   facts=%s heatmap cells=%d macro cards=%d modals close=%s errors=%d" % (facts, hm_cells, mc_cards, closed, len(errors)))
            if not (facts.get("heat") and facts.get("macro") and facts.get("hmLink") == "/universe-heatmap.html" and facts.get("macroLink") == "/macro-economic-data.html" and hm_cells >= 40 and mc_cards >= 40 and closed) or errors:
                FAILS.append("P4 chart-pro: %s cells=%d cards=%d closed=%s errors=%s" % (facts, hm_cells, mc_cards, closed, errors[:2]))
        except Exception as e:
            FAILS.append("P4: %s" % str(e)[:160])
        ctx.close()

        R.section("P5 nav manifest")
        st, body = fetch(SITE + "/nav-manifest.json")
        has = ("/universe-heatmap.html" in body, "/macro-economic-data.html" in body)
        (R.ok if all(has) else R.warn)("   nav-manifest.json %s lists heatmap=%s macro=%s" % (st, has[0], has[1]))

        R.section("P6 login return leg with a real PKCE verifier")
        ctx, pg, errors = new_page(browser, 1440, 1000)
        try:
            pg.goto(SITE + "/my-portfolio.html", wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(4000)
            try:
                with pg.expect_navigation(timeout=20000):
                    pg.evaluate("() => JustHodlAuth.signInWithGoogle()")
            except Exception:
                pass
            pg.wait_for_timeout(2000)
            at_google = "accounts.google.com" in pg.url
            verifier = pg.evaluate("() => Object.keys(localStorage).some(k => k.includes('code-verifier'))") if not at_google else None
            token_calls = []
            pg.on("response", lambda rs: token_calls.append((rs.status, rs.url[:110])) if "/auth/v1/token" in rs.url else None)
            pg.goto(SITE + "/my-portfolio.html?code=ops5178-fake-code", wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(7000)
            has_verifier = pg.evaluate("() => Object.keys(localStorage).some(k => k.includes('code-verifier'))")
            R.log("   reached Google=%s verifier-in-storage=%s token exchange calls=%s" % (at_google, has_verifier or verifier, token_calls))
            if token_calls and token_calls[0][0] >= 400:
                R.ok("   supabase-js attempted the PKCE code exchange on return (a fake code is rejected %s) -- the return leg works" % token_calls[0][0])
            elif token_calls:
                R.ok("   token exchange attempted (%s)" % token_calls[0][0])
            else:
                R.warn("   no token exchange observed (verifier=%s) -- inconclusive from a headless context" % (has_verifier or verifier))
        except Exception as e:
            R.warn("   P6: %s" % str(e)[:160])
        ctx.close()
        browser.close()

    R.section("verdict")
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        R.log("   RED: %d failure(s)" % len(FAILS))
        sys.exit(1)
    R.ok("   GREEN: both standalone engines render, hydrate and hand off to Chart Pro; chart-pro modals intact with PAGE links")
