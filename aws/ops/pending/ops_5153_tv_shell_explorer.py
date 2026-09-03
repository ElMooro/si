"""ops_5153 -- chart-pro as TradingView: docked rail, symbol card, Data Explorer over every provider.

Khalid: "a creative way where I can see every single data available on my data.html on my
chart-pro.html and look it up and once I double click it I should see it on the chart and I can
add it to my watchlist; chart-pro design exactly like TradingView; keep the existing features".

  * symdir v1.8.0 /explorer: no provider -> every provider with what it holds (hub counts, GB,
    freshness, directory counts); provider=slug -> its datasets/series (paged, filtered, most
    popular first), hub catalog fallback for providers the directory does not index
  * chart-pro TradingView shell (opt-out to the classic layout with one click): docked right
    rail WATCHLIST · DATA · INFO (the existing drawers moved in, every handler intact), symbol
    card (name, price, change, day/52wk range for equities; first/last/cadence/obs for series),
    right icon strip, page-scoped light/dark theme with a theme-aware chart palette, bottom
    range strip 1D 5D 1M 3M 6M YTD 1Y 5Y All + log + ET clock
  * Data Explorer (Ctrl+E / rail tab / icon): providers tree + chips with counts, paged rows,
    info pane, double-click charts, + adds, Enter/arrows/Esc keyboard

  S1 deploy symdir v1.8.0, /explorer probes   S2 page: shell mounted, explorer opens with 50+
  providers, FRED rows load, double-click charts DGS10, + adds to a watchlist, classic toggle
  restores the drawers, theme toggle re-renders, no page errors
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-symdir"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))


def http(url, timeout=180):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "v=" + str(int(time.time())), headers={"User-Agent": "justhodl-ops-5153", "Accept-Encoding": "identity"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        return e.read()


def http_json(url, timeout=180):
    return json.loads(http(url, timeout).decode("utf-8", "replace"))


def main():
    with report("5153-tv-shell-explorer") as r:
        r.heading("ops 5153 -- chart-pro as TradingView + Data Explorer over every provider")
        fails = []
        r.section("S1 deploy + /explorer")
        cur = lam.get_function_configuration(FunctionName=FN)
        env = (cur.get("Environment") or {}).get("Variables") or {"S3_BUCKET": B}
        desc = json.load(open(ROOT / "aws" / "lambdas" / FN / "config.json"))["description"]
        deploy_lambda(report=r, function_name=FN, source_dir=ROOT / "aws" / "lambdas" / FN / "source", env_vars=env, timeout=900, memory=6144, create_function_url=True, smoke=False, description=desc[:255])
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
        http_json(url + "/warm", timeout=180)
        d = http_json(url + "/explorer")
        provs = d.get("providers") or []
        r.log(f"  /explorer: providers={len(provs)} directory_docs={d.get('directory_docs')} totals={d.get('totals')} first={[(p['slug'], p.get('in_directory')) for p in provs[:4]]}")
        if len(provs) < 50:
            fails.append(f"explorer lists only {len(provs)} providers")
        for prov in ("fred", "eurostat", "gdelt", "boj"):
            d = http_json(url + f"/explorer?provider={prov}&limit=5")
            r.log(f"  {prov}: total={d.get('total')} kinds={d.get('kinds')} hub={d.get('hub')} first={[x['id'] for x in d.get('rows', [])[:3]]} err={d.get('error')}")
            if not d.get("rows"):
                fails.append(f"explorer {prov} returned no rows")
        d = http_json(url + "/explorer?provider=fred&q=unemployment%20rate&limit=5")
        r.log(f"  fred + 'unemployment rate': total={d.get('total')} first={[x['id'] for x in d.get('rows', [])[:5]]}")
        # worker route
        for _ in range(20):
            try:
                w = http_json(PROXY + "/explorer?provider=bls&limit=3", timeout=120)
                if w.get("rows"):
                    r.ok(f"  worker /explorer live: bls total={w.get('total')}")
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(15)
        else:
            fails.append("worker /explorer route not live")

        r.section("S2 live page")
        live = False
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                c = http("https://justhodl.ai/chart-pro.html", timeout=60)
                if b"class DataExplorer" in c and b'id="tv-rail"' in c and b"body[data-chart-theme" in c:
                    live = True
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        if not live:
            fails.append("page not live within 15 min")
        else:
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
                errors = []
                page.on("pageerror", lambda e: errors.append(str(e)[:200]))
                page.goto("https://justhodl.ai/chart-pro.html?v=" + str(int(time.time())), wait_until="domcontentloaded", timeout=90000)
                page.wait_for_timeout(10000)
                st = page.evaluate("""() => ({ shell: document.body.classList.contains('tv-shell'), theme: document.body.dataset.chartTheme, wlInRail: !!document.querySelector('#tv-wl-host #left-sidebar'), infoInRail: !!document.querySelector('#tv-info-host #right-sidebar'),
                    rangebar: !!document.getElementById('tv-rangebar'), symcard: (document.getElementById('tv-symcard')||{}).textContent.slice(0, 80), wlRows: document.querySelectorAll('#watchlist-body .wl-row').length, mainRight: getComputedStyle(document.querySelector('.main-area')).right })""")
                r.log(f"  shell: {json.dumps(st)[:500]}")
                if not (st.get("shell") and st.get("wlInRail") and st.get("infoInRail") and st.get("rangebar")):
                    fails.append(f"shell not mounted: {json.dumps(st)[:200]}")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5153-tv-shell.jpg"), type="jpeg", quality=55)
                page.keyboard.press("Control+e")
                page.wait_for_timeout(4000)
                ex = page.evaluate("() => ({ open: document.getElementById('dx-modal').classList.contains('open'), providers: document.querySelectorAll('#dx-tree .dx-prov').length, chips: document.querySelectorAll('#dx-chips .dx-chip').length })")
                r.log(f"  explorer: {json.dumps(ex)}")
                if not (ex.get("open") and (ex.get("providers") or 0) >= 50):
                    fails.append(f"explorer did not open with the providers: {ex}")
                page.click("#dx-tree .dx-prov[data-prov='fred']")
                page.wait_for_timeout(5000)
                rows = page.evaluate("() => ({ rows: document.querySelectorAll('#dx-list .dx-row').length, count: document.getElementById('dx-count').textContent, first: (document.querySelector('#dx-list .dx-row')||{}).textContent, info: (document.getElementById('dx-info')||{}).textContent.slice(0, 120) })")
                r.log(f"  fred rows: {json.dumps(rows)[:400]}")
                if not (rows.get("rows") or 0):
                    fails.append("explorer fred list empty")
                page.fill("#dx-search", "10-year treasury")
                page.wait_for_timeout(4000)
                page.evaluate("() => { const rows = Array.from(document.querySelectorAll('#dx-list .dx-row')); const dg = rows.find(e => e.title === 'fred:DGS10') || rows[0]; if (dg) dg.dispatchEvent(new MouseEvent('dblclick', { bubbles: true })); }")
                page.wait_for_timeout(9000)
                ch = page.evaluate("() => ({ active: State.activeTicker, meta: (document.getElementById('nch-meta-0')||{}).textContent, closed: !document.getElementById('dx-modal').classList.contains('open'), card: (document.getElementById('tv-symcard')||{}).textContent.slice(0, 100) })")
                r.log(f"  after double-click: {json.dumps(ch)[:400]}")
                if not (ch.get("closed") and "obs" in str(ch.get("meta"))):
                    fails.append(f"double-click did not chart: {json.dumps(ch)[:200]}")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5153-explorer-chart.jpg"), type="jpeg", quality=55)
                added = page.evaluate("""() => { State.customWatchlists['custom_ops5153'] = { name: 'ops5153', color: null, tickers: [] }; State.activeWatchlistId = 'custom_ops5153';
                    DataExplorer.open(); return 'ok'; }""")
                page.wait_for_timeout(3000)
                page.click("#dx-tree .dx-prov[data-prov='nyfed']")
                page.wait_for_timeout(4000)
                page.evaluate("() => { const b = document.querySelector('#dx-list .dx-row .hs-add'); if (b) b.click(); }")
                page.wait_for_timeout(1500)
                wl = page.evaluate("() => State.customWatchlists['custom_ops5153'].tickers")
                r.log(f"  + from explorer -> watchlist: {wl}")
                if not wl:
                    fails.append("explorer + did not add to the watchlist")
                page.keyboard.press("Escape")
                page.click(".tv-ico[data-ico='theme']")
                page.wait_for_timeout(8000)
                th = page.evaluate("() => ({ theme: document.body.dataset.chartTheme, meta: (document.getElementById('nch-meta-0')||{}).textContent.slice(0, 60) })")
                r.log(f"  theme toggle: {json.dumps(th)}")
                page.click(".tv-ico[data-ico='theme']")
                page.wait_for_timeout(8000)
                page.click(".tv-ico[data-ico='classic']")
                page.wait_for_timeout(1500)
                cl = page.evaluate("() => ({ shell: document.body.classList.contains('tv-shell'), leftHome: !!document.querySelector('.app > #left-sidebar'), leftOpen: document.getElementById('left-sidebar').classList.contains('open') })")
                r.log(f"  classic layout: {json.dumps(cl)}")
                if cl.get("shell") or not cl.get("leftHome"):
                    fails.append(f"classic toggle failed: {cl}")
                page.evaluate("() => TVShell.enable()")
                page.wait_for_timeout(1000)
                if errors:
                    fails.append(f"page errors: {errors[:3]}")
                page.close()
                ctx.close()
                browser.close()
        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: TradingView shell live with the Data Explorer over every provider; double-click charts, + adds, classic layout one click away")


if __name__ == "__main__":
    main()
