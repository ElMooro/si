"""ops_5184 -- auction desk v1.1.0: plain-English explanations, grade explainer, tenors on
bought securities. Push code directly if the workflow did not, run, verify the feed carries
`explain`/`score_parts`/`ttm_label`, and drive the page: card explanations visible, grade
click opens the explainer (card + tape), securities table shows tenor columns."""
import json
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

FN = "justhodl-auction-desk"
CFG = Config(retries={"max_attempts": 4, "mode": "adaptive"}, read_timeout=330)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []

with report("ops_5184_auction_desk_explain") as R:
    R.heading("ops 5184 -- auction desk v1.1.0 (explanations)")
    c = lam.get_function_configuration(FunctionName=FN)
    env = (c.get("Environment") or {}).get("Variables") or {}
    cfg_json = json.loads((ROOT / "aws" / "lambdas" / FN / "config.json").read_text())
    create_or_update_lambda(report=R, function_name=FN, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FN / "source"),
                            env_vars=env or cfg_json.get("env") or {}, timeout=int(cfg_json.get("timeout") or 300), memory=int(cfg_json.get("memory") or 1024),
                            description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
    for _ in range(20):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") in (None, "Successful"):
            break
        time.sleep(5)
    out = json.loads(lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")["Payload"].read() or b"{}")
    R.log("   run -> %s" % json.dumps(out)[:300])
    D = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-desk.json")["Body"].read())
    T = D["today"]
    R.log("   version %s today %s: %s" % (D.get("version"), T["date"], T["verdict"]["headline"]))
    a0 = (T["auctions"] or [None])[0]
    if a0:
        R.log("   %s explain lines=%d score_parts=%d" % (a0["term"], len(a0.get("explain") or []), len(a0.get("score_parts") or [])))
        for l in (a0.get("explain") or [])[:3]:
            R.log("     %s -> %s" % (l["metric"], l["text"][:150]))
        if not a0.get("explain") or not a0.get("score_parts"):
            FAILS.append("explain/score_parts missing on today's auctions")
    b0 = (T["buybacks"] or [None])[0]
    if b0:
        secs = b0.get("securities") or []
        R.log("   buyback securities %d, first: %s" % (len(secs), {k: secs[0].get(k) for k in ("cusip", "ttm_label", "orig_term", "maturity", "par_accepted")} if secs else None))
        if secs and not secs[0].get("ttm_label"):
            FAILS.append("ttm_label missing on securities")
    R.section("page")
    import urllib.request
    live = False
    for _ in range(40):
        try:
            with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/auctions.html", headers={"User-Agent": "ops5184", "Cache-Control": "no-cache"}), timeout=30) as r:
                live = b"gx-overlay" in r.read()
        except Exception:
            live = False
        if live:
            break
        time.sleep(15)
    R.log("   page carries the explainer: %s" % live)
    if live:
        try:
            import playwright  # noqa: F401
        except Exception:
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
        from playwright.sync_api import sync_playwright
        SHOTS.mkdir(parents=True, exist_ok=True)
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(channel="chrome", headless=True)
            except Exception:
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1440, "height": 1000})
            pg = ctx.new_page()
            errors = []
            pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
            pg.goto("https://justhodl.ai/auctions.html", wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(7000)
            facts = pg.evaluate("""() => ({ plain: document.querySelectorAll('#desk-ops .op-plain li').length, gradeBtns: document.querySelectorAll('#desk-ops button.op-grade').length,
                tapeBtns: document.querySelectorAll('#desk-tape button.grade-pill').length, secTenor: (document.querySelector('#desk-ops .op-secs tbody td.tk') || {}).textContent })""")
            pg.click("#desk-ops button.op-grade")
            pg.wait_for_timeout(400)
            gx1 = pg.evaluate("() => ({ open: document.getElementById('gx-overlay').classList.contains('open'), title: document.getElementById('gx-title').textContent, rows: document.querySelectorAll('#gx-body table tbody tr').length, plain: document.querySelectorAll('#gx-body ul li').length })")
            pg.keyboard.press("Escape")
            pg.wait_for_timeout(200)
            pg.click("#desk-tape button.grade-pill")
            pg.wait_for_timeout(400)
            gx2 = pg.evaluate("() => ({ open: document.getElementById('gx-overlay').classList.contains('open'), title: document.getElementById('gx-title').textContent })")
            pg.screenshot(path=str(SHOTS / "ops5184_grade_explainer.png"))
            R.log("   facts=%s card-click=%s tape-click=%s errors=%s" % (facts, gx1, gx2, errors[:2]))
            if facts["plain"] < 6 or not gx1["open"] or gx1["rows"] < 3 or not gx2["open"] or errors:
                FAILS.append("page: %s %s %s %s" % (facts, gx1, gx2, errors[:2]))
            ctx.close()
            browser.close()
    else:
        FAILS.append("page deploy not observed")
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN: explanations live, grade explainer works from cards and tape, tenors on securities")
