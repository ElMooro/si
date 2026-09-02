"""ops_5152 -- "TVC:US10" (a typed prefix of TVC:US10Y) must chart, not error.

symdir v1.7.0: a typed id that is a prefix of a real directory id resolves to it (closest_ids:
directory ids extending the typed id, most popular first; auto-resolve when the top candidate is
clearly best); otherwise the error carries the closest ids as alternatives. chart-pro: Enter while
the directory is still answering waits for it (no raw-text load), a resolved prefix renames the
chart to the real symbol with a toast, and the empty state says "Did you mean" with buttons.

  S1 deploy   S2 /series TVC:US10 -> TVC:US10Y (ohlc), TVC:US1 -> alternatives, TVC:US10Y unchanged
  S3 page: type "TVC:US10" + Enter immediately -> chart shows TVC:US10Y with obs/candles
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
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "v=" + str(int(time.time())), headers={"User-Agent": "justhodl-ops-5152", "Accept-Encoding": "identity"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        return e.read()


def http_json(url, timeout=180):
    return json.loads(http(url, timeout).decode("utf-8", "replace"))


def main():
    with report("5152-typed-prefix") as r:
        r.heading("ops 5152 -- typed prefixes resolve to the real symbol (TVC:US10 -> TVC:US10Y)")
        fails = []
        r.section("S1 deploy")
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
        if h.get("version") != "1.7.0":
            fails.append(f"version {h.get('version')} != 1.7.0")

        r.section("S2 resolution")
        d = http_json(url + "/series?id=TVC:US10&nocache=1", timeout=240)
        r.log(f"  TVC:US10 -> id={d.get('id')} resolved_from={d.get('resolved_from')} n={d.get('n')} ohlc={len(d.get('ohlc') or [])} src={str(d.get('source'))[:90]} err={str(d.get('error') or '')[:120]} alts={[a.get('id') for a in (d.get('alternatives') or [])]}")
        if not (d.get("n") and d.get("resolved_from") == "TVC:US10" and d.get("id") == "TVC:US10Y"):
            fails.append(f"TVC:US10 did not resolve to TVC:US10Y: {json.dumps({k: d.get(k) for k in ('id', 'resolved_from', 'n', 'error')})[:200]}")
        d = http_json(url + "/series?id=TVC:US1&nocache=1", timeout=240)
        r.log(f"  TVC:US1 -> id={d.get('id')} n={d.get('n')} resolved_from={d.get('resolved_from')} err={str(d.get('error') or '')[:100]} alts={[a.get('id') for a in (d.get('alternatives') or [])]}")
        if not (d.get("n") or d.get("alternatives")):
            fails.append("TVC:US1 gave neither a resolution nor alternatives")
        d = http_json(url + "/series?id=TVC:US10Y", timeout=240)
        r.log(f"  TVC:US10Y -> n={d.get('n')} ohlc={len(d.get('ohlc') or [])} resolved_from={d.get('resolved_from')}")
        if d.get("resolved_from"):
            fails.append("an exact id must not be rewritten")

        r.section("S3 live page: type TVC:US10 + Enter immediately")
        live = False
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                c = http("https://justhodl.ai/chart-pro.html", timeout=60)
                if b"ops 5152: Enter before the directory answered" in c:
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
                for typed in ("TVC:US10", "US02MY", "TVC:US02MY"):
                    page.click("#search-input")
                    page.fill("#search-input", "")
                    page.type("#search-input", typed, delay=25)
                    page.keyboard.press("Enter")            # immediately -- before the directory can answer
                    page.wait_for_timeout(14000)
                    pr = page.evaluate("() => ({ active: State.activeTicker, meta: (document.getElementById('nch-meta-0')||{}).textContent, loading: (document.getElementById('nch-loading-0')||{}).textContent, ticker: (document.getElementById('active-ticker')||{}).textContent })")
                    r.log(f"  typed {typed!r} + Enter: {json.dumps(pr)[:400]}")
                    if "obs" not in str(pr.get("meta")):
                        fails.append(f"{typed}: no chart: {json.dumps(pr)[:200]}")
                    if typed == "TVC:US10" and str(pr.get("active")).upper() != "TVC:US10Y":
                        fails.append(f"TVC:US10 charted as {pr.get('active')}, not TVC:US10Y")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5152-us10.jpg"), type="jpeg", quality=55)
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
        r.ok("PASS_ALL: typed prefixes resolve to the real symbol; Enter never loads raw text before the directory answers")


if __name__ == "__main__":
    main()
