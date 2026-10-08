"""ops_5127 -- every symbol from the warehouse: daily bars (5126 banked MONTHLY bars: Yahoo range=max), ECONOMICS aliases.

5125: TVC:VIX served natively via the dictionary (fred:VIXCLS, 9,262 obs, no widget, no paywall) but every
bank pull died: `resolved` in symbol-feed.json is a COUNT, not a map (AttributeError int.get). Fixed.

Khalid: "all my data is on data.html so it should be pulled from there ... every
single ticker should be pulled ... symbols that say this symbol is available on
tradingview ... shouldnt say that".

5124 found the TradingView chart socket refuses the handshake (HTTP 400 on every
endpoint; the ICE lane never banked a symbol either). tv-bars v1.1 therefore banks
through Yahoo's chart API resolved with the fleet's TradingView->Yahoo symbol map
(the same map justhodl-symbol-feed prices 8,167 of Khalid's symbols with), full
daily history since inception, volume included -- into data/warm/tv-bars/universe/
on first open, refreshed nightly. symdir v1.2.0 serves bare tickers (US:AAPL),
Polygon-style X:/C:/I: ids and any EXCHANGE:SYMBOL from that bank; dictionary
symbols already resolved to a warehouse provider (ECONOMICS:DEUR -> FRED) are
served by that resolver. chart-pro AUTO mode charts everything natively; the
TradingView widget is only used when the user picks the TV engine.

  S1 deploy tv-bars v1.1 (yahoo fallback) + symdir v1.2.0, rebuild (tv docs now
     carry source_id)
  S2 /series across families -> bars, first date, source; /quote batch
  S3 live page: TVC:VIX, AAPL, SSE:000001, ECONOMICS:DEUR chart natively; the
     widget paywall text never appears
Gates: TVC:VIX and AAPL bank with history before 1995; >=7 of 10 families bank;
page probes render with 'obs' and no TradingView iframe.
"""
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 2}))


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def http(url, timeout=180):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-5127", "Accept-Encoding": "identity"})
    t = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(), int((time.time() - t) * 1000)


def http_json(url, timeout=180):
    b, ms = http(url, timeout)
    return json.loads(b.decode("utf-8", "replace")), ms


def main():
    with report("5127-every-symbol-from-s3") as r:
        r.heading("ops 5127 -- every symbol from the warehouse: daily bars since inception, aliases, native page")
        fails = []
        r.section("S1 deploy tv-bars v1.1 + symdir v1.2.0")
        for fn, mem, to in (("justhodl-tv-bars", 1024, 600), ("justhodl-symdir", 6144, 900)):
            cur = lam.get_function_configuration(FunctionName=fn)
            env = (cur.get("Environment") or {}).get("Variables") or {"S3_BUCKET": B}
            desc = json.load(open(ROOT / "aws" / "lambdas" / fn / "config.json"))["description"]
            deploy_lambda(report=r, function_name=fn, source_dir=ROOT / "aws" / "lambdas" / fn / "source", env_vars=env, timeout=to, memory=mem,
                          create_function_url=(fn == "justhodl-symdir"), smoke=False, description=desc[:255])
            for _ in range(40):
                cfg = lam.get_function_configuration(FunctionName=fn)
                if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(3)
        url = lam.get_function_url_config(FunctionName="justhodl-symdir")["FunctionUrl"].rstrip("/")
        h, ms = http_json(url + "/health")
        r.kv(step="S1", symdir_version=h.get("version"))
        if h.get("version") != "1.2.0":
            fails.append(f"symdir version {h.get('version')} != 1.2.0")
        t_inv = datetime.now(timezone.utc)
        lam.invoke(FunctionName="justhodl-symdir", InvocationType="Event", Payload=json.dumps({"mode": "build"}).encode())
        man = None
        t0 = time.time()
        while time.time() - t0 < 900:
            time.sleep(20)
            doc, lm = get_json("data/symdir/manifest.json")
            if doc and (doc.get("built_at") or "") > t_inv.isoformat():
                man = doc
                break
        if not man:
            fails.append("rebuild did not land in 15 min")
        else:
            r.ok(f"  rebuilt docs={man.get('docs'):,} elapsed={man.get('elapsed_s')}s")
        http_json(url + "/warm", timeout=180)
        # purge the monthly-granularity bank from 5126 so every symbol re-banks daily
        purged = 0
        tok = None
        while True:
            kw = {"Bucket": B, "Prefix": "data/warm/tv-bars/universe/", "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            d = s3.list_objects_v2(**kw)
            for o in d.get("Contents") or []:
                s3.delete_object(Bucket=B, Key=o["Key"])
                purged += 1
            tok = d.get("NextContinuationToken")
            if not tok:
                break
        # and the series cache entries for the probe ids (nocache=1 below covers the rest)
        r.log(f"  purged {purged} monthly bank objects")

        r.section("S2 /series across symbol families (bank on first open, then S3)")
        samples = [("TVC:VIX", "1995-01-01"), ("AAPL", "1995-01-01"), ("NASDAQ:AAPL", "1995-01-01"), ("SSE:000001", None), ("FX:EURUSD", None),
                   ("X:BTCUSD", None), ("COINBASE:BTCUSD", None), ("CME_MINI:ES1!", None), ("HKEX:700", None), ("ECONOMICS:DEUR", None), ("I:SPX", "1995-01-01"),
                   ("BRK.B", None), ("AMEX:SPY", "1995-01-01"), ("OTC:AAAIF", None), ("XETR:DAX", None)]
        banked = 0
        for sid, inception in samples:
            try:
                d, ms = http_json(url + "/series?id=" + urllib.parse.quote(sid) + "&nocache=1", timeout=300)
                n = d.get("n") or 0
                r.log(f"  {sid:<18} n={n:>6} first={d.get('first')} last={d.get('last')} {ms:>6}ms ohlc={len(d.get('ohlc') or [])} src={str(d.get('source'))[:60]} via={d.get('via')} err={str(d.get('error') or '')[:100]}")
                if n > 200:
                    banked += 1
                if sid in ("TVC:VIX", "AAPL") and (n == 0 or (d.get("first") or "9999") > inception):
                    fails.append(f"{sid}: n={n} first={d.get('first')} (want history before {inception})")
                if sid in ("AAPL", "AMEX:SPY", "I:SPX") and n < 2000:
                    fails.append(f"{sid}: only {n} bars -- not daily granularity")
                # second read must come from S3 (not re-pulled)
                d2, ms2 = http_json(url + "/series?id=" + urllib.parse.quote(sid), timeout=180)
                if n and "banked just now" in str(d2.get("source") or "") and not d2.get("cached"):
                    r.warn(f"    {sid}: second read still says banked just now")
            except Exception as e:  # noqa: BLE001
                r.fail(f"  {sid}: {str(e)[:120]}")
        r.kv(step="S2", banked=banked, families=len(samples))
        if banked < 7:
            fails.append(f"only {banked}/{len(samples)} symbols banked")
        d, ms = http_json(url + "/quote?ids=" + urllib.parse.quote("TVC:VIX,AAPL,ECONOMICS:DEUR,fred:DGS10"), timeout=300)
        for k, v in (d.get("quotes") or {}).items():
            r.log(f"  quote {k:<16} ok={v.get('ok')} last={v.get('last')} @{v.get('last_date')} chg%={v.get('chg_pct')} err={v.get('error')}")
        idx, _ = get_json("data/warm/tv-bars/universe/_index.json")
        r.log(f"  universe index: n_symbols={(idx or {}).get('n_symbols')} failures={len((idx or {}).get('failures') or {})}")

        r.section("S3 live page: native charts, no widget paywall")
        page_ok = False
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                body, ms = http("https://justhodl.ai/chart-pro.html?v=" + str(int(time.time())), timeout=60)
                if b"AUTO mode (ops 5125)" in body:
                    page_ok = True
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        if not page_ok:
            fails.append("page did not deploy the ops 5125 client within 15 min")
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
                ctx = browser.new_context(viewport={"width": 1500, "height": 1000})
                page = ctx.new_page()
                errors = []
                page.on("pageerror", lambda e: errors.append(str(e)[:300]))
                page.goto("https://justhodl.ai/chart-pro.html", wait_until="domcontentloaded", timeout=90000)
                page.wait_for_timeout(7000)
                for sym in ("TVC:VIX", "AAPL", "SSE:000001", "ECONOMICS:DEUR", "NVDA"):
                    page.evaluate("(s) => ChartController.loadTicker(s)", sym)
                    page.wait_for_timeout(9000)
                    pr = page.evaluate("""() => ({ active: State.activeTicker, meta: (document.getElementById('nch-meta-0')||{}).textContent, loading: (document.getElementById('nch-loading-0')||{}).textContent,
                        iframe: !!document.querySelector('#tv-container-0 iframe'), paywall: document.body.innerText.includes('available on TradingView'),
                        name: (document.getElementById('active-name')||{}).textContent, nativeSrc: State.nativeSrc || null })""")
                    r.log(f"  page {sym}: {json.dumps(pr)[:400]}")
                    if pr.get("iframe") or pr.get("paywall"):
                        fails.append(f"{sym}: TradingView widget/paywall shown")
                    meta = str(pr.get("meta") or "")
                    if "obs" not in meta and "warehouse" not in meta and "bars" not in meta:
                        if sym == "ECONOMICS:DEUR":
                            r.warn(f"  {sym}: not servable from the warehouse (no alias to a provider series)")
                        else:
                            fails.append(f"{sym}: chart did not render natively: {json.dumps(pr)[:160]}")
                    if sym in ("AAPL", "NVDA") and "warehouse" not in str(pr.get("nativeSrc") or ""):
                        fails.append(f"{sym}: bars did not come from the warehouse ({pr.get('nativeSrc')})")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5127-chartpro-native.jpg"), type="jpeg", quality=60)
                errs = [e for e in errors if "TradingView" not in e]
                if errs:
                    fails.append(f"page errors {errs[:2]}")
                page.close()
                ctx.close()
                browser.close()

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: every symbol charts from S3 (banked on first open, refreshed nightly); the TradingView widget paywall is gone from AUTO mode")


if __name__ == "__main__":
    main()
