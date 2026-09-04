"""ops_5186 -- auction desk v1.2.0: composite history 1996->today (detector math, vendored) and the
cross-asset reaction model (14 assets, conditional forward returns by auction-day class) + realised
scoreboard. Push code directly, run with history+assets build, assert, drive the page."""
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
CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=330)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []

with report("ops_5186_auction_desk_reactions") as R:
    R.heading("ops 5186 -- auction desk v1.2.0: 1996 composite + cross-asset reactions")
    c = lam.get_function_configuration(FunctionName=FN)
    env = (c.get("Environment") or {}).get("Variables") or {}
    cfg_json = json.loads((ROOT / "aws" / "lambdas" / FN / "config.json").read_text())
    create_or_update_lambda(report=R, function_name=FN, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FN / "source"),
                            env_vars=env, timeout=300, memory=1536, description=cfg_json.get("description", "")[:250],
                            reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
    for _ in range(20):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") in (None, "Successful"):
            break
        time.sleep(5)
    t0 = time.time()
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"history": True, "assets": True}).encode())
    out = json.loads(resp["Payload"].read() or b"{}")
    R.log("   run (%.0fs, error=%s) -> %s" % (time.time() - t0, resp.get("FunctionError"), json.dumps(out)[:400]))
    if resp.get("FunctionError"):
        FAILS.append("engine error: %s" % json.dumps(out)[:200])
    D = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-desk.json")["Body"].read())
    R.section("composite history")
    ch = D.get("composite_history") or {}
    ser = ch.get("series") or []
    R.log("   points=%d first=%s last=%s scored=%s bank=%s err=%s" % (len(ser), ch.get("first"), ch.get("last"), ch.get("n_auctions_scored"), ch.get("bank"), ch.get("error")))
    if ser:
        for y in ("1998-10", "2008-10", "2020-03", "2022-06", "2023-03", "2026-08"):
            pts = [p for p in ser if p["date"].startswith(y)]
            R.log("   %s: %s" % (y, [(p["date"], p["composite"]) for p in pts[:3]]))
        R.log("   last 3: %s" % ser[-3:])
    if not ser or (ch.get("first") or "9999") > "1997-03-01" or len(ser) < 1000:
        FAILS.append("composite history not 1997->: first=%s points=%d" % (ch.get("first"), len(ser)))
    R.section("reactions")
    RX = D.get("reactions") or {}
    R.log("   note=%s today_classes=%s n_events=%s" % (RX.get("note"), RX.get("today_classes"), RX.get("n_events")))
    for p in RX.get("prediction") or []:
        R.log("   %-20s basis=%-14s n=%-4s same=%s d1=%s d5=%s d20=%s call=%s %s" % (p["name"], p["basis"], p["n"], (p.get("same_day") or {}).get("median"), p["d1"].get("median"), p["d5"].get("median"), p["d20"].get("median"), p["call"], p["confidence"]))
    if len(RX.get("prediction") or []) < 8:
        FAILS.append("prediction rows %d" % len(RX.get("prediction") or []))
    for b in (RX.get("scoreboard") or [])[:4]:
        R.log("   board %s %s grades=%s bb=%s SPY=%s BTC=%s TLT=%s" % (b["date"], b["classes"], "".join(b["grades"]), b["buyback_accepted"], b["moves"].get("SPY"), b["moves"].get("BTC-USD"), b["moves"].get("TLT")))
    if not RX.get("scoreboard"):
        FAILS.append("no scoreboard")
    st = RX.get("stats") or {}
    bs = st.get("buyback_strong", {}).get("BTC-USD")
    cw = st.get("coupon_weak", {}).get("SPY")
    R.log("   buyback_strong x BTC: %s | coupon_weak x SPY: %s" % (bs, cw))

    R.section("page")
    import urllib.request
    live = False
    for _ in range(40):
        try:
            with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/auctions.html", headers={"User-Agent": "ops5186", "Cache-Control": "no-cache"}), timeout=30) as r:
                live = b"rx-pred" in r.read()
        except Exception:
            live = False
        if live:
            break
        time.sleep(15)
    R.log("   page carries the reactions block: %s" % live)
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
            ctx = browser.new_context(viewport={"width": 1440, "height": 1100})
            pg = ctx.new_page()
            errors = []
            pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
            pg.goto("https://justhodl.ai/auctions.html", wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(9000)
            facts = pg.evaluate("""() => ({ pred: document.querySelectorAll('#rx-pred tr').length, board: document.querySelectorAll('#rx-board tr').length,
                calls: [...document.querySelectorAll('#rx-pred .rx-call')].map(x => x.textContent).join(''),
                viz: !!document.getElementById('jhviz-body'), vizText: (document.getElementById('jhviz-body') || {}).textContent ? document.getElementById('jhviz-body').textContent.slice(0, 80) : null,
                vizSvg: !!document.querySelector('#jhviz-body svg, #jhviz-body canvas') })""")
            pg.screenshot(path=str(SHOTS / "ops5186_auctions_top.png"))
            R.log("   facts=%s errors=%s" % (json.dumps(facts)[:400], errors[:2]))
            if facts["pred"] < 8 or facts["board"] < 3 or not facts["vizSvg"] or errors:
                FAILS.append("page: %s errors=%s" % (json.dumps(facts)[:200], errors[:2]))
            ctx.close()
            browser.close()
    else:
        FAILS.append("page deploy not observed")
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN")
