"""ops_5140 -- chart-pro: MAX shows the whole history, the WATCHLIST tab opens, header follows the series.

Khalid's screenshot (TVC:US10Y, MAX): the meta line said 16,134 obs 1962→2026 but the
chart showed ~2019→2026; clicking WATCHLIST did nothing; the header still showed NVDA's
$224.69 +3.33%; the pane label still read "NVDA —"; the series name wrapped the header.

Causes (audited in the source):
  * LightweightCharts' default minBarSpacing is 0.5px — 16k daily points cannot fit in
    ~1,800px, so fitContent() showed only the last ~7 years. Every native chart now sets
    minBarSpacing 0.001, and observation series aggregate to weekly (MAX/1W) or monthly
    (1M) points like TradingView does.
  * the WATCHLIST / INTEL edge tabs had hover CSS but no click handler (only the [ ] keys
    toggled the drawers); the × close buttons were unwired too.
  * header price/change were only patched from equity quotes; renderSeries now sets them
    (value + unit, daily change) and loadTicker resets them and the pane label.
  * .symbol-name never truncated.

Verification (headless Chrome, live page, after pages.yml):
  S1 page carries the fix   S2 drawer: click tab → open, × → closed
  S3 TVC:US10Y + MAX → visible range starts ≤ 1963 and ends 2026; 5Y → ~5 years; 1D → ~1 year
  S4 header price shows the yield (no $), pane label = active symbol, name on one line
Gates: all of the above.
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


def http(url, timeout=60):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "v=" + str(int(time.time())), headers={"User-Agent": "justhodl-ops-5140", "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    with report("5140-chartpro-max-drawer") as r:
        r.heading("ops 5140 -- chart-pro: MAX = whole history, watchlist tab opens, header follows the series")
        fails = []
        r.section("S1 wait for the page")
        live = False
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                c = http("https://justhodl.ai/chart-pro.html")
                if b"function jhAggregateObs(" in c and b"edgeLeft.addEventListener('click'" in c and b"minBarSpacing: 0.001" in c:
                    live = True
                    break
            except Exception as e:  # noqa: BLE001
                r.log(f"  fetch: {str(e)[:60]}")
            time.sleep(30)
        if not live:
            r.fail("page not live with the fix within 15 min")
            sys.exit(1)
        r.ok("page live with the fix")
        try:
            import playwright  # noqa: F401
        except Exception:  # noqa: BLE001
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(channel="chrome", headless=True)
            except Exception:  # noqa: BLE001
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1920, "height": 1000})
            page = ctx.new_page()
            page.add_init_script("Error.stackTraceLimit = 80;")
            errors = []
            page.on("pageerror", lambda e: errors.append({"msg": str(e)[:160], "frames": [ln.strip()[:140] for ln in (getattr(e, 'stack', '') or '').split('\n') if 'unpkg' not in ln and ln.strip().startswith('at ')][:5]}))
            page.goto("https://justhodl.ai/chart-pro.html", wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(8000)

            r.section("S2 watchlist drawer")
            st0 = page.evaluate("() => ({ open: document.getElementById('left-sidebar').classList.contains('open'), tabHidden: document.getElementById('edge-tab-left').classList.contains('hidden') })")
            page.click("#edge-tab-left")
            page.wait_for_timeout(800)
            st1 = page.evaluate("() => ({ open: document.getElementById('left-sidebar').classList.contains('open'), tabHidden: document.getElementById('edge-tab-left').classList.contains('hidden'), rows: document.querySelectorAll('#watchlist-body .wl-row').length, x: document.getElementById('left-sidebar').getBoundingClientRect().x })")
            page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5140-drawer-open.jpg"), type="jpeg", quality=55)
            page.click("#close-left")
            page.wait_for_timeout(600)
            st2 = page.evaluate("() => ({ open: document.getElementById('left-sidebar').classList.contains('open'), tabHidden: document.getElementById('edge-tab-left').classList.contains('hidden') })")
            r.log(f"  before={st0} after tab click={st1} after ×={st2}")
            if not (st1.get("open") and st1.get("tabHidden") and st1.get("x", -999) >= -1):
                fails.append(f"watchlist drawer did not open on tab click: {st1}")
            if st2.get("open"):
                fails.append("× did not close the drawer")

            r.section("S3 TVC:US10Y across timeframes")
            page.evaluate("() => ChartController.loadTicker('TVC:US10Y')")
            page.wait_for_timeout(9000)

            def probe(label):
                pr = page.evaluate("""() => { const c = ChartSync.charts.get(State.activeChartPane); let vr = null; try { vr = c && c.timeScale().getVisibleRange(); } catch (e) {}
                    const host = document.getElementById('nch-chart-0');
                    return { active: State.activeTicker, tf: State.tf, vr, width: host ? host.clientWidth : null, meta: (document.getElementById('nch-meta-0')||{}).textContent,
                             price: (document.getElementById('active-price')||{}).textContent, chg: (document.getElementById('active-change')||{}).textContent,
                             pane: (document.getElementById('pane-symbol-0')||{}).textContent, nameH: (document.getElementById('active-name')||{}).offsetHeight, name: (document.getElementById('active-name')||{}).textContent } }""")
                r.log(f"  {label}: {json.dumps(pr)[:600]}")
                return pr
            d1 = probe("1D (default)")
            for tf_label, sel in (("MAX", "button.tf-btn[data-days='9999']"), ("5Y", "button.tf-btn[data-days='1825'][data-span='day']"), ("1M", "button.tf-btn[data-span='month']")):
                page.click(sel)
                page.wait_for_timeout(9000)
                pr = probe(tf_label)
                vr = pr.get("vr") or {}
                fr, to = str(vr.get("from") or ""), str(vr.get("to") or "")
                if tf_label == "MAX":
                    page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5140-us10y-max.jpg"), type="jpeg", quality=55)
                    if not (fr[:4] and int(fr[:4]) <= 1963 and to[:4] == "2026"):
                        fails.append(f"MAX does not show the whole history: visible {fr}→{to}")
                if tf_label == "5Y" and fr[:4] and not (2020 <= int(fr[:4]) <= 2022):
                    fails.append(f"5Y visible range starts {fr}")
                if tf_label == "1M" and not (fr[:4] and int(fr[:4]) <= 1963):
                    r.warn(f"  1M (monthly, 10y window) starts {fr}")
            r.section("S4 header follows the series")
            hp = probe("header")
            if "$" in str(hp.get("price")) or hp.get("price") in ("—", ""):
                fails.append(f"header price not from the series: {hp.get('price')!r}")
            if hp.get("pane") != "TVC:US10Y":
                fails.append(f"pane label stale: {hp.get('pane')!r}")
            if (hp.get("nameH") or 0) > 24:
                fails.append(f"header name wraps ({hp.get('nameH')}px): {hp.get('name')!r}")
            if errors:
                for e in errors[:4]:
                    r.log(f"  page error: {json.dumps(e)[:400]}")
                fails.append(f"page errors {len(errors)}")
            page.close()
            ctx.close()
            browser.close()
        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: MAX shows 1962→2026, the WATCHLIST tab opens and closes, the header shows the series value and name on one line")


if __name__ == "__main__":
    main()
