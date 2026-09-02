"""ops_5150 -- v1.6.1 (5149 timed out banking 37 years of the Treasury par curve inside a request): the first bank runs as mode=ustbank (Event + daily 21:30 UTC schedule); requests never fetch history inline.

Original 5149 note -- v1.6.0: FRED has no 2-/4-month CMT (verified 400 "series does not exist"); the Treasury par yield curve lane (home.treasury.gov, daily since 1990, all 13 tenors) now serves TVC:US02MY/US04MY, Bundesbank daily serves TVC:DE10Y; dropdown render guarded and console captured.

Khalid: TVC:US02MY failed ("no bars ... tv: all endpoints refused; yahoo ^US02MY: HTTP E").
Redesign the search bar around how data is retrieved: consolidate every source under it and
show what each source has.

symdir v1.5.0
  * tv_equivalents(): TradingView-only concepts map to the warehouse series that carry them --
    TVC:US02MY -> fred:DGS2MO, TVC:DE10Y -> fred:IRLTLT01DEUM156N, TVC:US03MY -> fred:DGS3MO +
    fred:DTB3, ECONOMICS:USINTR -> fred:FEDFUNDS ... The resolver serves the first that answers.
  * /search: `facets` = every provider with a hit and its count (chips in the dropdown, click
    to filter); TradingView rows carry `sources` (provider, id, cadence, span, banked) or an
    explicit "TradingView-only" marker.
  * /series errors carry `alternatives`; the chart offers them as buttons instead of a RuntimeError.
chart-pro: facet chips, source lines, alternatives; the series legend now uses the page's own
floating legend (a duplicate static method had swallowed the earlier call).

  S1 deploy   S2 /series TVC:US02MY via fred:DGS2MO, ECONOMICS:USINTR via FEDFUNDS, /search facets + sources
  S3 page: type US02MY -> chips + source line; Enter -> chart with obs; TVC:US10Y legend present
"""
import json
import subprocess
import sys
import time
import urllib.parse
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
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))


def http(url, timeout=120):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "v=" + str(int(time.time())), headers={"User-Agent": "justhodl-ops-5150", "Accept-Encoding": "identity"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        return e.read()          # the API's error body (trace, alternatives) is the evidence


def http_json(url, timeout=240):
    return json.loads(http(url, timeout).decode("utf-8", "replace"))


def main():
    with report("5150-search-sources") as r:
        r.heading("ops 5150 -- Treasury par curve banked offline + Bundesbank sources; search chips (v1.6.1)")
        fails = []
        r.section("S1 deploy symdir v1.5.0")
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
        h = http_json(url + "/health")
        if h.get("version") != "1.6.1":
            fails.append(f"version {h.get('version')} != 1.6.1")

        r.section("S1b Treasury par curve: bank offline, daily schedule")
        sch = boto3.client("scheduler", region_name=REGION)
        sd = {"Name": "justhodl-symdir-ustbank", "ScheduleExpression": "cron(30 21 ? * MON-FRI *)", "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
              "Target": {"Arn": cfg["FunctionArn"], "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "Input": '{"mode":"ustbank"}', "RetryPolicy": {"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 900}},
              "State": "ENABLED", "Description": "Treasury daily par yield curve (13 tenors incl. 2M/4M) — current year refresh after the 15:30–17:00 ET post"}
        try:
            sch.create_schedule(**sd)
            r.ok("schedule created: justhodl-symdir-ustbank weekdays 21:30 UTC")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**sd)
            r.ok("schedule updated")
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"mode": "ustbank", "full": True}).encode())
        body = json.loads(resp["Payload"].read() or b"{}")
        r.log(f"  ustbank: {json.dumps(body)[:400]}")
        if not body.get("ok") or (body.get("n_days") or 0) < 8000:
            fails.append(f"Treasury par curve bank incomplete: {json.dumps(body)[:200]}")

        r.section("S2 resolution + search")
        for sid in ("ustpar:2M", "ustpar:10Y", "official-yields:de-10y-bbk"):
            d = http_json(url + "/series?id=" + sid + "&nocache=1", timeout=240)
            r.log(f"  direct {sid}: n={d.get('n')} first={d.get('first')} last={d.get('last')} src={str(d.get('source'))[:80]} err={str(d.get('error') or '')[:200]}")
            if sid == "ustpar:2M" and not d.get("n"):
                fails.append(f"Treasury par curve lane returned nothing: {d.get('error')}")
        for sid, want in (("TVC:US02MY", "ustpar:2M"), ("ECONOMICS:USINTR", "fred:FEDFUNDS"), ("TVC:DE10Y", "official-yields:de-10y-bbk"), ("TVC:US03MY", "fred:DGS3MO")):
            d = http_json(url + "/series?id=" + urllib.parse.quote(sid) + "&nocache=1")
            r.log(f"  {sid:<18} n={d.get('n')} first={d.get('first')} last={d.get('last')} via={d.get('via')} src={str(d.get('source'))[:90]} err={str(d.get('error') or '')[:80]} alts={d.get('alternatives')}")
            if not d.get("n") or d.get("via") != want:
                fails.append(f"{sid} did not resolve via {want}: via={d.get('via')} err={d.get('error')}")
        d = http_json(url + "/search?q=US02MY&limit=8")
        rows = d.get("rows") or []
        tvrow = next((x for x in rows if x["id"].upper() == "TVC:US02MY"), None)
        r.log(f"  search US02MY: facets={d.get('facets')} rows={[x['id'] for x in rows[:6]]} tv sources={tvrow and tvrow.get('sources')}")
        if not d.get("facets"):
            fails.append("search has no facets")
        if not (tvrow and tvrow.get("sources") and tvrow["sources"][0]["id"] == "ustpar:2M"):
            fails.append(f"TVC:US02MY row lacks its warehouse source: {tvrow}")
        d2 = http_json(url + "/search?q=unemployment%20rate&limit=5&provider=eurostat")
        r.log(f"  provider filter eurostat: rows={[x['id'] for x in d2.get('rows', [])]} facets={[(f['provider'], f['n']) for f in (d2.get('facets') or [])][:6]}")
        if any(x["provider"] != "eurostat" for x in d2.get("rows", [])) or len([f for f in d2.get("facets") or [] if f["provider"] != "eurostat"]) == 0:
            fails.append("provider filter / pre-filter facets wrong")

        r.section("S3 live page")
        live = False
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                c = http("https://justhodl.ai/chart-pro.html", timeout=60)
                if b"static _render(partial)" in c:
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
                page.on("console", lambda m: errors.append("console " + m.text[:300]) if m.type == "error" and "HeaderSearch" in m.text else None)
                page.goto("https://justhodl.ai/chart-pro.html?v=" + str(int(time.time())), wait_until="domcontentloaded", timeout=90000)
                page.wait_for_timeout(9000)
                page.click("#search-input")
                page.type("#search-input", "US02MY", delay=40)
                page.wait_for_timeout(6000)
                raw = page.evaluate("""async () => { const r = await fetch(PROXY + '/symsearch?q=US02MY&limit=40&nocache=1'); const j = await r.json(); return { facets: j.facets, rows: (j.rows||[]).slice(0,2).map(x => ({ id: x.id, sources: x.sources })), err: j.error }; }""")
                r.log(f"  page fetch /symsearch: {json.dumps(raw)[:500]} | HeaderSearch._facets={json.dumps(page.evaluate('() => HeaderSearch._facets || null'))[:200]}")
                dd = page.evaluate("""() => { const dd = document.getElementById('hsearch-dropdown'); return { chips: Array.from(dd.querySelectorAll('.hs-facet')).map(e => e.textContent.trim()).slice(0, 8),
                    rows: Array.from(dd.querySelectorAll('.hs-row')).slice(0, 4).map(e => ({ id: e.dataset.ticker, src: (e.querySelector('.hs-sources')||{}).textContent })) }; }""")
                r.log(f"  dropdown: {json.dumps(dd)[:700]}")
                if not dd.get("chips"):
                    fails.append("no facet chips in the dropdown")
                tv = next((x for x in dd.get("rows", []) if str(x.get("id")).upper() == "TVC:US02MY"), None)
                if not (tv and tv.get("src") and "2M" in str(tv.get("src"))):
                    fails.append(f"TVC:US02MY row has no source line: {tv}")
                # click the TradingView row -> chart served via FRED
                page.evaluate("() => { const e = Array.from(document.querySelectorAll('#hsearch-dropdown .hs-row')).find(x => x.dataset.ticker.toUpperCase() === 'TVC:US02MY'); if (e) e.click(); }")
                page.wait_for_timeout(9000)
                pr = page.evaluate("() => ({ active: State.activeTicker, meta: (document.getElementById('nch-meta-0')||{}).textContent, loading: (document.getElementById('nch-loading-0')||{}).textContent })")
                r.log(f"  TVC:US02MY chart: {json.dumps(pr)[:400]}")
                if "obs" not in str(pr.get("meta")):
                    fails.append(f"TVC:US02MY did not chart: {json.dumps(pr)[:200]}")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5150-us02my.jpg"), type="jpeg", quality=55)
                page.evaluate("() => ChartController.loadTicker('TVC:US10Y')")
                page.wait_for_timeout(9000)
                lg = page.evaluate("() => ({ legend: (document.querySelector('#tv-container-0 .nch-legend')||{}).textContent })")
                r.log(f"  TVC:US10Y legend: {json.dumps(lg)[:200]}")
                if not (lg.get("legend") and " O " in " " + str(lg.get("legend"))):
                    fails.append(f"no legend for TVC:US10Y: {lg}")
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
        r.ok("PASS_ALL: the search bar shows every source with counts and cadence; TradingView-only symbols chart from their warehouse equivalent")


if __name__ == "__main__":
    main()
