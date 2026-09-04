"""ops_5181 -- launch the Treasury auction desk engine + verify the rebuilt auctions.html.

Khalid (2026-09-03): auctions.html "needs a major major overhaul -- make sure it
gets data from the Treasury API daily; I need a daily analysis of the auction:
today's huge buyback should be flagged as easy monetary policy and a pump in
risk assets."

Delivered in this commit: Lambda justhodl-auction-desk (same-day TreasuryDirect
results, FiscalData buybacks, 3y bank, grades vs trailing-12, day verdict with
liquidity / rates / risk-asset implications, grounded Claude note) and the desk
sections at the top of auctions.html. This op:
  P1 waits for the deploy (function exists, Claude key inherited), runs the
     engine once with backfill, prints today's verdict + operations
  P2 asserts the feed: today's date, buyback rows, the $12.5B Sep-03 buyback
     if FiscalData has published it (tags must include EASY-POLICY SIGNAL),
     graded auctions with z-scores, calendar rows, freshness
  P3 schedules (America/New_York): 09:15 morning calendar, 12:10 buyback
     results, 13:40 auction results, 16:35 late catch -- Mon-Fri
  P4 headless Chrome: auctions.html at 1440/390 renders the banner, cards,
     buyback table, calendar, tape, no page errors; screenshots committed
"""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-auction-desk"
BUCKET = "justhodl-dashboard-live"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=330)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []
NOW = datetime.now(timezone.utc)


with report("ops_5181_auction_desk_launch") as R:
    R.heading("ops 5181 -- Treasury auction desk: launch + verify")
    R.section("P1 deploy (direct from the runner if the workflow did not create it) + first run")
    sys.path.insert(0, str(ROOT / "aws" / "ops"))
    from _lambda_deploy_helpers import build_zip, create_or_update_lambda, function_exists  # noqa: E402
    matches = [f["FunctionName"] for p in lam.get_paginator("list_functions").paginate() for f in p["Functions"] if "auction-desk" in f["FunctionName"]]
    R.log("   functions matching 'auction-desk' before: %s" % matches)
    cfg_json = json.loads((ROOT / "aws" / "lambdas" / FN / "config.json").read_text())
    env_vars = dict(cfg_json.get("env") or {})
    try:
        src_env = (lam.get_function_configuration(FunctionName="justhodl-auction-interpreter").get("Environment") or {}).get("Variables") or {}
        for k in ("ANTHROPIC_API_KEY",):
            if src_env.get(k):
                env_vars[k] = src_env[k]
    except Exception as e:
        R.warn("   inherit env: %s" % str(e)[:100])
    if not function_exists(FN):
        R.log("   not deployed by the workflow -- creating from aws/lambdas/%s/source" % FN)
        create_or_update_lambda(report=R, function_name=FN, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FN / "source"),
                                env_vars=env_vars, timeout=int(cfg_json.get("timeout") or 300), memory=int(cfg_json.get("memory") or 1024),
                                description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
    else:
        R.log("   function exists -- pushing current code + env")
        create_or_update_lambda(report=R, function_name=FN, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FN / "source"),
                                env_vars=env_vars, timeout=int(cfg_json.get("timeout") or 300), memory=int(cfg_json.get("memory") or 1024),
                                description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
    cfg = None
    for _ in range(30):
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("LastUpdateStatus") in (None, "Successful") and cfg.get("State") == "Active":
                break
        except Exception:
            cfg = None
        time.sleep(10)
    if not cfg:
        R.fail("   function still not active")
        sys.exit(1)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    R.log("   deployed %s mem=%s timeout=%s key=%s" % (cfg.get("LastModified"), cfg.get("MemorySize"), cfg.get("Timeout"), "yes" if env.get("ANTHROPIC_API_KEY") else "MISSING"))
    if not env.get("ANTHROPIC_API_KEY"):
        R.warn("   ANTHROPIC_API_KEY not inherited -- the AI note will be skipped (deterministic analysis unaffected)")
    t0 = time.time()
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"backfill": True}).encode())
    out = json.loads(resp["Payload"].read() or b"{}")
    R.log("   first run (%.0fs) -> %s" % (time.time() - t0, json.dumps(out)[:600]))
    if resp.get("FunctionError") or not out.get("ok"):
        FAILS.append("engine run failed: %s" % json.dumps(out)[:200])

    R.section("P2 feed assertions")
    D = json.loads(s3.get_object(Bucket=BUCKET, Key="data/auction-desk.json")["Body"].read())
    T = D.get("today") or {}
    v = T.get("verdict") or {}
    R.log("   generated_at=%s freshness=%s notes=%s" % (D.get("generated_at"), D.get("freshness"), D.get("notes")))
    R.log("   TODAY %s: %s" % (T.get("date"), v.get("headline")))
    R.log("   tags=%s liquidity=%s rates=%s risk=%s" % (v.get("tags"), v.get("liquidity"), v.get("rates"), v.get("risk_assets")))
    for b in v.get("bullets") or []:
        R.log("     - %s" % b[:220])
    for a in T.get("auctions") or []:
        R.log("   auction %s %s %s grade=%s btc=%s z=%s tail=%s indirect=%s pd=%s" % (a["auction_date"], a["term"], a["type"], a["grade"], a.get("btc"), a.get("z"), a.get("tail_bp"), a.get("indirect_pct"), a.get("pd_pct")))
    for b in T.get("buybacks") or []:
        R.log("   buyback %s max=%s offered=%s accepted=%s fill=%s cover=%s tags=%s" % (b["operation_date"], b.get("max_par"), b.get("offered"), b.get("accepted"), b.get("fill_pct"), b.get("coverage"), b.get("tags")))
    ops = ((D.get("buybacks") or {}).get("operations") or [])
    R.log("   buyback operations in feed: %d, newest %s, program total %s, raw fields sample %s" % (len(ops), ops[0]["operation_date"] if ops else None, (D.get("buybacks") or {}).get("program", {}).get("total_accepted"), (ops[0].get("raw_fields") if ops else None)))
    if ops and ops[0].get("accepted") is None:
        FAILS.append("buyback amounts not mapped (accepted None) -- raw fields %s" % ops[0].get("raw_fields"))
    sep3 = [o for o in ops if o["operation_date"] == "2026-09-03"]
    if sep3:
        o = sep3[0]
        ok = (o.get("accepted") or 0) >= 12.4e9 and "EASY-POLICY SIGNAL" in (o.get("tags") or [])
        (R.ok if ok else R.fail)("   Sep-03 buyback in feed: accepted %s tags %s" % (o.get("accepted"), o.get("tags")))
        if not ok:
            FAILS.append("Sep-03 buyback present but not flagged")
    else:
        R.warn("   Sep-03 buyback not yet published by FiscalData (newest %s) -- the 12:10 / 16:35 runs pick it up when it lands" % (ops[0]["operation_date"] if ops else None))
    if not (D.get("auctions") or []):
        FAILS.append("no graded auctions in feed")
    graded = [a for a in D.get("auctions", []) if a.get("grade") not in (None, "n/a")]
    R.log("   graded auctions %d / %d; calendar rows %d; recent days %d; by_term %d" % (len(graded), len(D.get("auctions", [])), len((D.get("calendar") or {}).get("auctions") or []), len(D.get("recent_days") or []), len(D.get("by_term") or {})))
    if len(graded) < 20:
        FAILS.append("too few graded auctions (%d) -- history bank thin?" % len(graded))
    if T.get("ai_note"):
        R.log("   AI note: %s" % json.dumps(T["ai_note"])[:400])

    R.section("P3 schedules (America/New_York, Mon-Fri)")
    plan = [("justhodl-auction-desk-morning", "cron(15 9 ? * MON-FRI *)", {}), ("justhodl-auction-desk-buyback", "cron(10 12 ? * MON-FRI *)", {}),
            ("justhodl-auction-desk-results", "cron(40 13 ? * MON-FRI *)", {}), ("justhodl-auction-desk-late", "cron(35 16 ? * MON-FRI *)", {})]
    for name, expr, payload in plan:
        tgt = {"Arn": cfg["FunctionArn"], "RoleArn": SCHED_ROLE, "Input": json.dumps(payload), "RetryPolicy": {"MaximumRetryAttempts": 1}}
        try:
            sch.get_schedule(Name=name, GroupName="default")
            sch.update_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="America/New_York",
                                FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description="auction desk (ops 5181)")
            R.ok("   %s updated %s ET" % (name, expr))
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="America/New_York",
                                FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description="auction desk (ops 5181)")
            R.ok("   %s created %s ET" % (name, expr))
        except Exception as e:
            FAILS.append("schedule %s: %s" % (name, str(e)[:120]))

    R.section("P4 page render")
    try:
        import urllib.request
        live = False
        for _ in range(40):
            try:
                with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/auctions.html", headers={"User-Agent": "ops5181", "Cache-Control": "no-cache"}), timeout=30) as r:
                    live = b"desk-banner" in r.read()
            except Exception:
                live = False
            if live:
                break
            time.sleep(15)
        R.log("   page carries the desk block: %s" % live)
        if not live:
            FAILS.append("auctions.html deploy not observed")
        else:
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
                for width, height in ((1440, 1000), (390, 844)):
                    ctx = browser.new_context(viewport={"width": width, "height": height}, is_mobile=width < 700)
                    pg = ctx.new_page()
                    errors = []
                    pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
                    pg.goto("https://justhodl.ai/auctions.html", wait_until="domcontentloaded", timeout=60000)
                    pg.wait_for_timeout(7000)
                    facts = pg.evaluate("""() => ({ headline: document.getElementById('desk-headline').textContent, tags: document.querySelectorAll('#desk-tags .desk-tag').length,
                        cards: document.querySelectorAll('#desk-ops .op-card').length, bbRows: document.querySelectorAll('#bb-table tr').length, cal: document.querySelectorAll('#cal-table tr').length,
                        days: document.querySelectorAll('#days-table tr').length, tape: document.querySelectorAll('#desk-tape tr').length, tenors: document.querySelectorAll('#tenor-grid .tenor-card').length,
                        ai: !document.getElementById('desk-ai').hidden, overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth })""")
                    pg.screenshot(path=str(SHOTS / f"ops5181_auctions_{width}.png"), full_page=False)
                    R.log("   %4dpx: %s errors=%s" % (width, json.dumps(facts)[:400], errors[:2]))
                    if facts["cards"] < 1 or facts["tape"] < 20 or facts["bbRows"] < 3 or "Loading" in facts["headline"] or errors:
                        FAILS.append("%dpx render: %s errors=%s" % (width, json.dumps(facts)[:200], errors[:2]))
                    if width == 390 and facts["overflow"] > 0:
                        FAILS.append("390px horizontal overflow %dpx" % facts["overflow"])
                    ctx.close()
                browser.close()
    except Exception as e:
        FAILS.append("P4: %s" % str(e)[:160])

    R.section("verdict")
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        R.log("   RED: %d failure(s)" % len(FAILS))
        sys.exit(1)
    R.ok("   GREEN: auction desk live -- same-day Treasury data, graded operations, day verdict, buyback tracker, schedules armed, page renders")
