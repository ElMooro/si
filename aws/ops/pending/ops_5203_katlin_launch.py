"""ops_5203 -- launch justhodl-katlin (the buy desk) + verify katlin.html + arm schedules.
Creates the function directly from the runner (deploy-lambdas does not create new functions), copies FMP/POLYGON
from justhodl-equity-research (Khalid's standing rule), runs it ASYNC and gates on data/katlin.json freshness
(the engine takes minutes -- never a sync invoke), audits the artifact (war-room legs, tiers, joins, field coverage),
arms EventBridge Scheduler (daily + weekly backtest), waits for the page at the edge and drives it in Chrome at
1440/390, then kicks the first walk-forward backtest so the Validation tab fills by tomorrow."""
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

FN = "justhodl-katlin"
DONOR = "justhodl-equity-research"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
BUCKET = "justhodl-dashboard-live"
OUT = "data/katlin.json"
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


with report("ops_5203_katlin_launch") as R:
    R.heading("ops 5203 -- KATLIN launch")
    cfg_json = json.loads((ROOT / "aws" / "lambdas" / FN / "config.json").read_text())
    # env: donor keys (standing rule) + anything declared
    env = dict(cfg_json.get("env") or {})
    try:
        denv = lam.get_function_configuration(FunctionName=DONOR).get("Environment", {}).get("Variables", {})
        for k in (cfg_json.get("inherit_env") or {}).get("keys") or []:
            if denv.get(k):
                env[k] = denv[k]
        R.log("   env keys from %s: %s" % (DONOR, sorted(k for k in env if k in ((cfg_json.get("inherit_env") or {}).get("keys") or []))))
    except Exception as e:
        FAILS.append("donor env: %s" % str(e)[:120])
    if not env.get("POLYGON_API_KEY"):
        FAILS.append("POLYGON_API_KEY missing -- crypto lane and 4h sniper would be dead")
    create_or_update_lambda(report=R, function_name=FN, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FN / "source"),
                            env_vars=env, timeout=int(cfg_json.get("timeout") or 900), memory=int(cfg_json.get("memory") or 8192),
                            description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=2048)
    cfg = None
    for _ in range(40):
        cfg = lam.get_function_configuration(FunctionName=FN)
        if cfg.get("LastUpdateStatus") in (None, "Successful") and cfg.get("State") == "Active":
            break
        time.sleep(5)
    R.log("   function state %s / %s, %sMB / %ss" % (cfg.get("State"), cfg.get("LastUpdateStatus"), cfg.get("MemorySize"), cfg.get("Timeout")))
    # ---- async run + freshness gate
    before = None
    try:
        before = get_json(OUT).get("generated_at")
    except Exception:
        pass
    t0 = time.time()
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    R.log("   invoked async (prior generated_at=%s); polling %s" % (before, OUT))
    D = None
    while time.time() - t0 < 870:
        time.sleep(20)
        try:
            d = get_json(OUT)
        except Exception:
            continue
        if d.get("generated_at") and d.get("generated_at") != before:
            D = d
            break
    if not D:
        FAILS.append("no fresh %s after %.0fs (check CloudWatch /aws/lambda/%s)" % (OUT, time.time() - t0, FN))
    else:
        R.log("   fresh artifact after %.0fs: v%s session %s elapsed %ss" % (time.time() - t0, D.get("version"), D.get("session"), D.get("elapsed_s")))
        R.section("war room")
        wr = D.get("war_room") or {}
        R.log("   posture %s cap %s%% thermometer %s legs %d red %s vetoes %s" % (wr.get("posture"), wr.get("exposure_cap_pct"), wr.get("thermometer"), len(wr.get("legs") or []), wr.get("n_red"), wr.get("vetoes")))
        R.log("   missing legs: %s" % (wr.get("missing"),))
        for l in wr.get("legs") or []:
            R.log("      %-34s %3s %-5s %s" % (l["leg"], l["risk"], l["flag"], (l["read"] or "")[:110]))
        R.log("   brief: %s" % (wr.get("brief") or "")[:600])
        if len(wr.get("legs") or []) < 8:
            FAILS.append("war room thin: %d legs (missing %s)" % (len(wr.get("legs") or []), wr.get("missing")))
        if wr.get("posture") in (None, "UNKNOWN"):
            FAILS.append("posture unresolved")
        R.section("universe / tiers / gates")
        U, T, G = D.get("universe") or {}, D.get("tiers") or {}, D.get("gates") or {}
        R.log("   universe %s" % json.dumps(U))
        R.log("   tiers %s" % json.dumps(T))
        R.log("   gates %s" % json.dumps(G))
        R.log("   degraded %s" % (D.get("degraded"),))
        if (U.get("stocks_scored") or 0) < 1500:
            FAILS.append("stocks scored %s < 1500" % U.get("stocks_scored"))
        if (U.get("etfs_scored") or 0) < 80:
            FAILS.append("etfs scored %s < 80" % U.get("etfs_scored"))
        if (U.get("crypto_scored") or 0) < 15:
            FAILS.append("crypto scored %s < 15 (Polygon crypto lane)" % U.get("crypto_scored"))
        if (G.get("location") or 0) < 200:
            FAILS.append("location gate passes only %s names" % G.get("location"))
        picks = D.get("picks") or []
        R.section("joins on the picks (PROBE-THEN-WIRE: a silent zero here is a bug, not a quiet market)")
        def cov(fn):
            return sum(1 for r in picks if fn(r))
        n = max(1, len(picks))
        joins = {"inflow_legs": cov(lambda r: (r.get("inflow_legs") or {}).get("n_legs")), "inflow_evidence": cov(lambda r: r.get("inflow_evidence")),
                 "catalysts": cov(lambda r: r.get("catalysts")), "fleet_accum": cov(lambda r: r.get("fleet_accumulation")),
                 "quality_pe": cov(lambda r: (r.get("quality") or {}).get("pe") is not None), "dilution": cov(lambda r: (r.get("quality") or {}).get("share_count_yoy_pct") is not None),
                 "dark_pool": cov(lambda r: (r.get("inflow_legs") or {}).get("dark_pool_pct") is not None), "f13": cov(lambda r: (r.get("inflow_legs") or {}).get("inst_net_usd") is not None),
                 "industry_flow": cov(lambda r: ((r.get("inflow_legs") or {}).get("industry_flow") or {}).get("score") is not None),
                 "sniper": cov(lambda r: (r.get("sniper") or {}).get("state")), "why": cov(lambda r: len(r.get("why") or "") > 120), "plan": cov(lambda r: (r.get("plan") or {}).get("stop"))}
        for k, v in joins.items():
            R.log("   %-16s %4d / %d picks (%.0f%%)" % (k, v, len(picks), 100.0 * v / n))
        stock_picks = [r for r in picks if r.get("asset_class") == "stock"]
        ns = max(1, len(stock_picks))
        if picks and joins["why"] < 0.9 * len(picks):
            FAILS.append("plain-English why missing on %d picks" % (len(picks) - joins["why"]))
        if stock_picks and sum(1 for r in stock_picks if (r.get("inflow_legs") or {}).get("n_legs")) < 0.6 * ns:
            FAILS.append("inflow legs joined on <60%% of stock picks -- feed keys drifted")
        if stock_picks and sum(1 for r in stock_picks if (r.get("quality") or {}).get("pe") is not None) < 0.5 * ns:
            FAILS.append("finviz valuation joined on <50%% of stock picks")
        if stock_picks and sum(1 for r in stock_picks if (r.get("quality") or {}).get("share_count_yoy_pct") is not None) < 0.3 * ns:
            R.warn("   census dilution joined on <30% of stock picks (census matrix coverage) -- documented, not fatal")
        if picks and joins["sniper"] == 0:
            FAILS.append("4h sniper lane produced nothing (Polygon aggs)")
        R.section("top of the board")
        for r in picks[:12]:
            R.log("   %-6s %-12s %-6s score %5s conv %5s vs200 %6s rsiW %4s %-9s acc %4s inf %4s cat %4s rr %4s 4h %s" % (
                r.get("ticker"), r.get("tier"), r.get("asset_class"), r.get("composite"), r.get("conviction"), r.get("dist_sma200_pct"), r.get("rsi_w"), r.get("structure_state"),
                (r.get("pillars") or {}).get("accumulation"), (r.get("pillars") or {}).get("inflows"), (r.get("pillars") or {}).get("catalyst"), r.get("rr"), (r.get("sniper") or {}).get("state")))
        if picks:
            R.log("   WHY[%s]: %s" % (picks[0].get("ticker"), (picks[0].get("why") or "")[:900]))
        R.section("field coverage (artifact keys)")
        R.log("   top-level: %s" % sorted(D.keys()))
        R.log("   panels: %s" % {k: len(v) for k, v in (D.get("panels") or {}).items()})
        if picks:
            R.log("   pick row keys (%d): %s" % (len(picks[0]), sorted(picks[0].keys())))
        page = (ROOT / "katlin.html").read_text(encoding="utf-8")
        for k in ("war_room", "picks", "watch", "changes", "validation", "base_rates", "definitions", "degraded", "tiers", "universe", "market", "feeds_asof"):
            if k not in page:
                FAILS.append("page never renders top-level key %s" % k)
        for k in ("why", "plan", "sniper", "accum_legs", "inflow_legs", "catalysts", "quality", "pillars", "gates", "double_bottom_w", "fleet_accumulation", "mom_evidence", "posture_note"):
            if k not in page:
                FAILS.append("page never renders pick key %s" % k)
        if D.get("top_picks") is not None and not all("ticker" in x and "score" in x for x in D.get("top_picks") or []):
            FAILS.append("top_picks rows lack ticker/score (harvester contract)")
    # ---- schedules
    R.section("schedules (EventBridge Scheduler, UTC)")
    arn = cfg["FunctionArn"]
    for name, expr, inp, desc in (("justhodl-katlin-daily", "cron(10 4 ? * TUE-SAT *)", "{}", "KATLIN daily 04:10 UTC Tue-Sat after fortress + polygon-full (ops 5203)"),
                                  ("justhodl-katlin-backtest-weekly", "cron(30 9 ? * SUN *)", json.dumps({"mode": "backtest"}), "KATLIN weekly walk-forward (ops 5203)")):
        tgt = {"Arn": arn, "RoleArn": SCHED_ROLE, "Input": inp, "RetryPolicy": {"MaximumRetryAttempts": 1}}
        try:
            sch.get_schedule(Name=name, GroupName="default")
            sch.update_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description=desc)
            R.ok("   %s updated %s" % (name, expr))
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description=desc)
            R.ok("   %s created %s" % (name, expr))
        except Exception as e:
            FAILS.append("schedule %s: %s" % (name, str(e)[:120]))
    # ---- page at the edge
    R.section("page")
    live = False
    for _ in range(40):
        try:
            with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/katlin.html?v=%d" % int(time.time()), headers={"User-Agent": "ops5203", "Cache-Control": "no-cache", "Pragma": "no-cache"}), timeout=30) as r:
                live = b"KATLIN_DESK_V1" in r.read()
        except Exception:
            live = False
        if live:
            break
        time.sleep(15)
    R.log("   katlin.html carries marker KATLIN_DESK_V1 at the edge: %s" % live)
    if live and D:
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
            for width, height in ((1440, 1100), (390, 844)):
                ctx = browser.new_context(viewport={"width": width, "height": height}, is_mobile=width < 700)
                pg = ctx.new_page()
                errors = []
                pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
                pg.goto("https://justhodl.ai/katlin.html?v=%d" % int(time.time()), wait_until="domcontentloaded", timeout=60000)
                pg.wait_for_timeout(9000)
                # if the Prime bucket is empty today, the render gate looks at Basing (the gates are strict by design)
                pg.evaluate("""() => { const rows = document.querySelectorAll('#board tbody tr.pick').length; if (!rows) { const b = document.querySelector('#tabs button[data-t=basing]'); if (b) b.click(); } }""")
                pg.wait_for_timeout(1500)
                pg.evaluate("""() => { const r = document.querySelector('#board tbody tr.pick'); if (r) r.click(); }""")
                pg.wait_for_timeout(800)
                facts = pg.evaluate("""() => ({ score: document.getElementById('wr-score').textContent, posture: document.getElementById('wr-posture').textContent,
                    headline: document.getElementById('wr-headline').textContent.slice(0, 80), legs: document.querySelectorAll('#legs tbody tr').length,
                    board: document.querySelectorAll('#board tbody tr.pick').length, evid: (document.querySelector('#evid .why') || {}).textContent ? document.querySelector('#evid .why').textContent.length : 0,
                    helps: document.querySelectorAll('#board th .help').length, defs: document.querySelectorAll('#defs dt').length,
                    overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth, err: document.getElementById('errbox').textContent })""")
                pg.screenshot(path=str(SHOTS / f"ops5203_katlin_{width}.png"))
                R.log("   %4dpx: %s errors=%s" % (width, json.dumps(facts)[:400], errors[:2]))
                if facts["score"] in ("—", "") or facts["legs"] < 8 or facts["helps"] < 10 or facts["defs"] < 10 or errors or facts["err"]:
                    FAILS.append("%dpx render: %s errors=%s" % (width, json.dumps(facts)[:220], errors[:2]))
                if facts["board"] == 0:
                    R.warn("   %dpx: no PRIME/BASING rows rendered today (strict gates) -- check Watch tab manually" % width)
                if facts["board"] and facts["evid"] < 120:
                    FAILS.append("%dpx: evidence panel did not fill after clicking a row" % width)
                if width == 390 and facts["overflow"] > 0:
                    FAILS.append("390px overflow %dpx" % facts["overflow"])
                ctx.close()
            browser.close()
    elif not live:
        FAILS.append("page deploy not observed at the edge")
    # ---- kick the first walk-forward (async; ~10-13 min; lands in data/katlin-backtest.json)
    if D:
        try:
            lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"mode": "backtest"}).encode())
            R.ok("   first walk-forward backtest kicked async (data/katlin-backtest.json)")
        except Exception as e:
            R.warn("   backtest kick failed: %s" % str(e)[:120])
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN: KATLIN live -- engine, feed, schedules, page")
    sys.exit(0)
