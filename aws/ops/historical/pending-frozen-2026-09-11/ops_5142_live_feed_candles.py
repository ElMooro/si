"""ops_5142 -- live feed at the source's cadence, candles everywhere, bare ids resolve.

Khalid: (1) data must update as soon as the source updates -- "you see how often the data
is updated at the sources and that's when it's updated so I always get a live feed";
(2) add candles + the TradingView chart features chart-pro is missing;
(3) "HQMCB10YRP: no bars in the JustHodl warehouse yet" -- lots of tickers don't pull.

  * FRED: mode=fredupdates every 15 min reads FRED's own change feed (fred/series/updates
    since the last cursor), intersects with the 277k banked series and heals exactly those
    tails -- the warehouse follows the source's release clock. Universe banks (equities,
    TV symbols) heal on open when older than the last two weekdays; nightly refresh stays.
  * Candles: TradingView symbols now bank their OHLC first (TVC:US10Y -> ^TNX candles since
    1962) and only fall back to the close-only dictionary series when no bars source exists.
    Chart types: candles / bars / line / area / Heikin-Ashi; an O H L C Δ% V legend follows
    the crosshair; US equities get the forming session from Polygon on top of the bank.
  * Bare ids: a typed id that is not an instrument resolves through the directory's bare
    index (HQMCB10YRP -> fred:HQMCB10YRP); Enter in the search bar picks the top result.

  S1 deploy tv-bars + symdir v1.4.0, fredupdates schedule (rate 15 min) + one sync run
  S2 /series: HQMCB10YRP bare -> fred; TVC:US10Y -> ohlc from ^TNX; AAPL bank healed if stale
  S3 page: chart-type buttons switch, legend shows O/H/L/C, live tail on AAPL, HQMCB10YRP
     via the search bar + Enter charts FRED
"""
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=900, retries={"max_attempts": 1}))
sch = boto3.client("scheduler", region_name=REGION)


def http(url, timeout=120):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "v=" + str(int(time.time())), headers={"User-Agent": "justhodl-ops-5142", "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_json(url, timeout=240):
    return json.loads(http(url, timeout).decode("utf-8", "replace"))


def main():
    with report("5142-live-feed-candles") as r:
        r.heading("ops 5142 -- live feed at the source's cadence, candles everywhere, bare ids resolve")
        fails = []
        r.section("S1 deploy + fredupdates schedule")
        for fn, mem, to in (("justhodl-tv-bars", 1024, 600), ("justhodl-symdir", 6144, 900)):
            cur = lam.get_function_configuration(FunctionName=fn)
            env = (cur.get("Environment") or {}).get("Variables") or {"S3_BUCKET": B}
            desc = json.load(open(ROOT / "aws" / "lambdas" / fn / "config.json"))["description"]
            deploy_lambda(report=r, function_name=fn, source_dir=ROOT / "aws" / "lambdas" / fn / "source", env_vars=env, timeout=to, memory=mem, create_function_url=(fn == "justhodl-symdir"), smoke=False, description=desc[:255])
            for _ in range(40):
                cfg = lam.get_function_configuration(FunctionName=fn)
                if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(3)
        url = lam.get_function_url_config(FunctionName="justhodl-symdir")["FunctionUrl"].rstrip("/")
        h = http_json(url + "/health")
        if h.get("version") != "1.4.0":
            fails.append(f"symdir version {h.get('version')} != 1.4.0")
        sd = {"Name": "justhodl-symdir-fredupdates", "ScheduleExpression": "rate(15 minutes)", "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
              "Target": {"Arn": cfg["FunctionArn"], "RoleArn": SCHED_ROLE, "Input": '{"mode":"fredupdates"}', "RetryPolicy": {"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 600}},
              "State": "ENABLED", "Description": "FRED change feed (fred/series/updates) -> heal the banked tails that FRED itself reports as updated"}
        try:
            sch.create_schedule(**sd)
            r.ok("schedule created: justhodl-symdir-fredupdates rate(15 minutes)")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**sd)
            r.ok("schedule updated")
        resp = lam.invoke(FunctionName="justhodl-symdir", InvocationType="RequestResponse", Payload=json.dumps({"mode": "fredupdates"}).encode())
        body = json.loads(resp["Payload"].read() or b"{}")
        r.log(f"  fredupdates: {json.dumps(body)[:300]}")
        if not body.get("ok"):
            fails.append(f"fredupdates failed: {body}")

        r.section("S2 /series")
        d = http_json(url + "/series?id=HQMCB10YRP&nocache=1")
        r.log(f"  HQMCB10YRP -> id={d.get('id')} prov={d.get('provider')} n={d.get('n')} first={d.get('first')} last={d.get('last')} src={str(d.get('source'))[:80]} err={d.get('error')}")
        if d.get("provider") != "fred" or not d.get("n"):
            fails.append(f"HQMCB10YRP did not resolve to FRED: {json.dumps(d)[:200]}")
        d = http_json(url + "/series?id=TVC:US10Y&nocache=1")
        r.log(f"  TVC:US10Y -> prov={d.get('provider')} n={d.get('n')} ohlc={len(d.get('ohlc') or [])} first={d.get('first')} last={d.get('last')} src={str(d.get('source'))[:90]} via={d.get('via')}")
        if not (d.get("ohlc") and len(d["ohlc"]) > 5000):
            fails.append(f"TVC:US10Y has no OHLC candles: {json.dumps({k: d.get(k) for k in ('n', 'source', 'via', 'error')})}")
        today = datetime.now(timezone.utc).date()
        for sid in ("AAPL", "XETR:DAX", "fred:DGS10"):
            d = http_json(url + "/series?id=" + urllib.parse.quote(sid) + "&nocache=1")
            last = d.get("last") or ""
            lag = (today - datetime.fromisoformat(last[:10]).date()).days if last else 999
            r.log(f"  {sid:<10} last={last} lag={lag}d src={str(d.get('source'))[:90]}")
            if lag > 5:
                fails.append(f"{sid} stale: last {last}")

        r.section("S3 live page")
        live = False
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                c = http("https://justhodl.ai/chart-pro.html", timeout=60)
                if b"static attachLegend(" in c and b'data-ct="ha"' in c:
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
                page.goto("https://justhodl.ai/chart-pro.html", wait_until="domcontentloaded", timeout=90000)
                page.wait_for_timeout(8000)
                # HQMCB10YRP via the search bar + Enter
                page.click("#search-input")
                page.fill("#search-input", "HQMCB10YRP")
                page.wait_for_timeout(3000)
                top = page.evaluate("() => { const e = document.querySelector('#hsearch-dropdown .hs-row'); return e ? e.dataset.ticker : null; }")
                page.keyboard.press("Enter")
                page.wait_for_timeout(8000)
                pr = page.evaluate("() => ({ active: State.activeTicker, meta: (document.getElementById('nch-meta-0')||{}).textContent, price: (document.getElementById('active-price')||{}).textContent })")
                r.log(f"  search top={top} after Enter: {json.dumps(pr)[:300]}")
                if not (str(pr.get('active')).lower() == "fred:hqmcb10yrp" and "obs" in str(pr.get("meta"))):
                    fails.append(f"HQMCB10YRP via search bar did not chart FRED: {json.dumps(pr)[:200]}")
                # TVC:US10Y candles + legend + chart types
                page.evaluate("() => ChartController.loadTicker('TVC:US10Y')")
                page.wait_for_timeout(9000)
                lg = page.evaluate("() => ({ legend: (document.querySelector('#tv-container-0 .nch-legend')||{}).textContent, meta: (document.getElementById('nch-meta-0')||{}).textContent, ct: State.chartType })")
                r.log(f"  TVC:US10Y: {json.dumps(lg)[:400]}")
                if not (lg.get("legend") and " O " in " " + str(lg.get("legend")) + " " and "H " in str(lg.get("legend"))):
                    fails.append(f"no OHLC legend for TVC:US10Y: {json.dumps(lg)[:200]}")
                for ct in ("bars", "line", "ha", "candles"):
                    page.click(f"button.ct-btn[data-ct='{ct}']")
                    page.wait_for_timeout(6000)
                    st = page.evaluate("() => ({ ct: State.chartType, live: (NativeChart._live||[]).length, meta: (document.getElementById('nch-meta-0')||{}).textContent })")
                    r.log(f"  chart type {ct}: {json.dumps(st)[:200]}")
                    if st.get("ct") != ct or "obs" not in str(st.get("meta")):
                        fails.append(f"chart type {ct} did not render: {json.dumps(st)[:160]}")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5142-us10y-candles.jpg"), type="jpeg", quality=55)
                # AAPL live tail
                page.evaluate("() => ChartController.loadTicker('AAPL')")
                page.wait_for_timeout(9000)
                ap = page.evaluate("() => ({ src: State.nativeSrc, legend: (document.querySelector('#tv-container-0 .nch-legend')||{}).textContent })")
                r.log(f"  AAPL: {json.dumps(ap)[:300]}")
                if "warehouse" not in str(ap.get("src")):
                    fails.append(f"AAPL not from the warehouse: {ap.get('src')}")
                if errors:
                    fails.append(f"page errors: {errors[:2]}")
                page.close()
                ctx.close()
                browser.close()
        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: FRED follows its own change feed every 15 min, TV symbols chart as candles from banked OHLC, chart types + legend live, bare ids resolve")


if __name__ == "__main__":
    main()
