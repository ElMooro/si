"""ops_5158 -- 5157 follow-up: policy pins lead the dropdown across kinds ("Best match" group: crude -> WTI benchmark first).

5157 -- exact symbols that carry a signal were filtered out of the instant list (SPY/MSFT); probe waits for the answered query.

Khalid pasted an audit: BTC loads the Grayscale ETF, SPY buried under prefix matches, signal rows
outrank exact AAPL/MSFT, the frontend re-sorts server results, mobile overflows (2,127px at 390px).

  * symdir v1.9.0: the backend is the single ranking authority -- SEARCH_POLICY pins (btc/bitcoin ->
    X:BTCUSD spot before the BTC trust, eth, spy, eurusd, dxy, sp500/spx, nasdaq, dow, vix, crude/oil/
    wti -> WTI benchmark + front contract before ticker-prefix products, brent, gold, silver, copper,
    natgas, 10y, fed funds, sofr); any known crypto code pins its spot pair; an exact bare instrument
    symbol outranks everything but a pin; every row carries asset_class; `ambiguous` flag
  * chart-pro: server order is authoritative (instant client rows only until the answer lands),
    JustHodl signals are badges on the matching symbol and a trailing group, never the default;
    mobile: below 768px one column, search first, no horizontal scroll; the TV shell only mounts >= 900px

  S1 deploy   S2 the audit's acceptance battery against /search (first result per query)
  S3 live page: typed queries -> first dropdown row; 390px viewport -> no overflow, search visible, chart width
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
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
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))


def http(url, timeout=180):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "v=" + str(int(time.time())), headers={"User-Agent": "justhodl-ops-5158", "Accept-Encoding": "identity"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        return e.read()


def http_json(url, timeout=180):
    return json.loads(http(url, timeout).decode("utf-8", "replace"))


BATTERY = [("AAPL", "AAPL"), ("MSFT", "MSFT"), ("SPY", "SPY"), ("BTC", "X:BTCUSD"), ("bitcoin", "X:BTCUSD"), ("ETH", "X:ETHUSD"), ("BTCUSD", "X:BTCUSD"), ("EURUSD", "C:EURUSD"),
           ("BRK.B", "BRK.B"), ("crude", "fred:DCOILWTICO"), ("oil", "fred:DCOILWTICO"), ("WTI", "fred:DCOILWTICO"), ("brent", "fred:DCOILBRENTEU"), ("gold", "fred:GOLDAMGBD228NLBM"),
           ("vix", "TVC:VIX"), ("spx", "I:SPX"), ("nasdaq", "I:NDX"), ("dxy", "TVC:DXY"), ("sofr", "nyfed:sofr"), ("NVDA", "NVDA"), ("TSLA", "TSLA"), ("dgs10", "fred:DGS10")]


def main():
    with report("5158-search-policy") as r:
        r.heading("ops 5158 -- search ranking authority: pins lead across kinds (final)")
        fails = []
        r.section("S1 deploy symdir v1.9.0")
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
        h = http_json(url + "/health")
        if h.get("version") != "1.9.0":
            fails.append(f"version {h.get('version')} != 1.9.0")

        r.section("S2 acceptance battery (/search first result)")
        for q, want in BATTERY:
            d = http_json(url + "/search?q=" + urllib.parse.quote(q) + "&limit=6", timeout=120)
            ids = [x["id"] for x in d.get("rows", [])]
            ok = bool(ids) and ids[0].upper() == want.upper()
            (r.ok if ok else r.fail)(f"  {q!r:<10} -> {ids[:5]} (want {want}) policy={d.get('policy')} ambiguous={d.get('ambiguous')}")
            if not ok:
                fails.append(f"{q!r} first={ids[:1]} want {want}")

        r.section("S3 live page")
        live = False
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                c = http("https://justhodl.ai/chart-pro.html", timeout=60)
                if b"ops 5158: policy pins" in c:
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
                page.wait_for_timeout(9000)
                for q, want in (("AAPL", "AAPL"), ("SPY", "SPY"), ("BTC", "X:BTCUSD"), ("MSFT", "MSFT"), ("crude", "fred:DCOILWTICO"), ("gold", "fred:GOLDAMGBD228NLBM"), ("vix", "TVC:VIX"), ("EURUSD", "C:EURUSD")):
                    page.click("#search-input")
                    page.fill("#search-input", "")
                    page.type("#search-input", q, delay=25)
                    for _ in range(40):
                        page.wait_for_timeout(500)
                        if page.evaluate("(q) => HeaderSearch._answeredQ === q", q):
                            break
                    page.wait_for_timeout(600)
                    first = page.evaluate("() => { const rows = Array.from(document.querySelectorAll('#hsearch-dropdown .hs-row')).map(e => e.dataset.ticker); return { first: rows[0], top5: rows.slice(0, 5), groups: Array.from(document.querySelectorAll('#hsearch-dropdown .hs-group')).map(g => g.textContent.slice(0, 20)) }; }")
                    ok = str(first.get("first") or "").upper() == want.upper()
                    (r.ok if ok else r.fail)(f"  page {q!r}: first={first.get('first')} top5={first.get('top5')} groups={first.get('groups')}")
                    if not ok:
                        fails.append(f"page {q!r} first={first.get('first')} want {want}")
                page.keyboard.press("Escape")
                page.close()
                # mobile
                mctx = browser.new_context(viewport={"width": 390, "height": 844}, is_mobile=True, has_touch=True, device_scale_factor=2)
                mp = mctx.new_page()
                mp.goto("https://justhodl.ai/chart-pro.html?v=" + str(int(time.time())), wait_until="domcontentloaded", timeout=90000)
                mp.wait_for_timeout(9000)
                m = mp.evaluate("""() => { const inp = document.getElementById('search-input'); const rc = inp ? inp.getBoundingClientRect() : null; const ch = document.getElementById('tv-container-0'); const cr = ch ? ch.getBoundingClientRect() : null;
                    return { scrollW: document.documentElement.scrollWidth, docW: document.documentElement.clientWidth, shell: document.body.classList.contains('tv-shell'), search: rc && { x: Math.round(rc.x), w: Math.round(rc.width), y: Math.round(rc.y) }, chart: cr && { w: Math.round(cr.width), h: Math.round(cr.height) } }; }""")
                r.log(f"  mobile 390px: {json.dumps(m)}")
                mp.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5158-mobile.jpg"), type="jpeg", quality=55)
                if (m.get("scrollW") or 9999) > 400:
                    fails.append(f"mobile horizontal overflow: scrollWidth {m.get('scrollW')}")
                if not (m.get("search") and 0 <= m["search"]["x"] < 390 and m["search"]["w"] > 150):
                    fails.append(f"mobile search not reachable: {m.get('search')}")
                if not (m.get("chart") and m["chart"]["w"] > 300 and m["chart"]["h"] > 200):
                    fails.append(f"mobile chart collapsed: {m.get('chart')}")
                mp.close()
                mctx.close()
                ctx.close()
                browser.close()
                if errors:
                    fails.append(f"page errors: {errors[:3]}")
        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: exact symbols and intent pins rank first everywhere, signals are badges, phones get a one-column composition")


if __name__ == "__main__":
    main()
