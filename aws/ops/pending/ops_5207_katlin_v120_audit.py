"""ops_5207 -- KATLIN v1.2.0 audit-only re-check: same gates as 5206 but the overlay-ETF audit uses the ENGINE's own regexes
(5206 flagged GSKH on a naive substring -- "unhedged" contains "hedged"). No redeploy, no invoke: audits the live artifact + page.: redeploy from the runner, async run + freshness gate, war-room read, tiers, joins,
catalyst/dilution lanes, top board, page render at 1440/390. Schedules already exist (ops 5203) and are only checked.
(original launch docstring follows)
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


with report("ops_5207_katlin_v120_audit") as R:
    R.heading("ops 5207 -- KATLIN v1.2.0 audit (no redeploy)")
    cfg = lam.get_function_configuration(FunctionName=FN)
    R.log("   fn state %s mem %s timeout %s" % (cfg.get("State"), cfg.get("MemorySize"), cfg.get("Timeout")))
    D = get_json(OUT)
    t0 = time.time()
    R.log("   artifact v%s generated %s session %s elapsed %ss" % (D.get("version"), D.get("generated_at"), D.get("session"), D.get("elapsed_s")))
    if D.get("version") != "1.2.0":
        FAILS.append("artifact version %s != 1.2.0" % D.get("version"))
    if D:
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
        R.section("v1.2.0 lanes")
        if D.get("version") != "1.2.0":
            FAILS.append("artifact version %s != 1.2.0 (stale code ran)" % D.get("version"))
        R.log("   washout gate passes: %s of %s scored" % ((D.get("gates") or {}).get("washout"), (D.get("universe") or {}).get("scored")))
        lowvol = [(r.get("ticker"), r.get("vol_ann_pct"), r.get("dd_52w_pct")) for r in picks if (r.get("vol_ann_pct") or 0) < 10 or ((r.get("dd_52w_pct") or -99) > -10 and (r.get("dist_sma200_pct") or -99) > -6)]
        if lowvol:
            FAILS.append("non-washed-out names in the buy tiers: %s" % lowvol[:8])
        n_ports = sum(1 for r in picks for c in (r.get("catalysts") or []) if c.get("kind") == "ports")
        R.log("   ports catalysts on picks: %d; T1 labels: %s" % (n_ports, sorted({str((r.get("plan") or {}).get("target_1_label")) for r in picks})))
        for line in (D.get("log") or []):
            if any(k in line for k in ("dilution lane", "crypto lane", "sniper", "feeds in", "war room", "stocks scored", "row errors", "wrote")):
                R.log("   " + line[:300])
        named = sum(1 for r in picks if (r.get("n_named_catalysts") or 0) >= 1)
        R.log("   picks with a NAMED catalyst: %d / %d; catalyst gate passes: %s" % (named, len(picks), (D.get("gates") or {}).get("catalyst")))
        kinds = {}
        for r in picks:
            for c in r.get("catalysts") or []:
                kinds[c.get("kind")] = kinds.get(c.get("kind"), 0) + 1
        R.log("   catalyst kinds across picks: %s" % sorted(kinds.items(), key=lambda kv: -kv[1]))
        dil = [r for r in picks if r.get("asset_class") == "stock"]
        R.log("   dilution coverage on stock picks: %d / %d (fmp-sourced %d)" % (sum(1 for r in dil if (r.get("quality") or {}).get("share_count_yoy_pct") is not None), len(dil), sum(1 for r in dil if (r.get("quality") or {}).get("shares_source") == "fmp")))
        for r in picks[:6]:
            R.log("   %s catalysts: %s" % (r.get("ticker"), [c.get("text", "")[:70] for c in (r.get("catalysts") or [])][:3]))
            R.log("   %s plan: %s | 3m %s | days<200 %s | knife %s" % (r.get("ticker"), r.get("plan"), r.get("ret_3m_pct"), r.get("days_below_sma200"), r.get("knife")))
        import importlib.util
        spec = importlib.util.spec_from_file_location("katlin_engine", str(ROOT / "aws" / "lambdas" / FN / "source" / "lambda_function.py"))
        eng = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(eng)
        etf_overlay = [(r.get("ticker"), (r.get("name") or "")[:40]) for r in picks if r.get("asset_class") == "etf" and (eng.OVERLAY_RX.search(r.get("name") or "") or eng.MONEY_RX.search(r.get("name") or "") or eng.LEV_RX.search(r.get("name") or ""))]
        R.log("   ETF names on the board flagged by a naive substring check (for the record): %s" % [(r.get("ticker"), (r.get("name") or "")[:40]) for r in picks if r.get("asset_class") == "etf" and any(k in str(r.get("name") or "").lower() for k in ("option", "buffer", "hedged", "covered"))][:6])
        if etf_overlay:
            FAILS.append("overlay ETFs still on the board: %s" % etf_overlay[:8])
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
    # ---- schedules: created by ops 5203; here we only confirm they exist
    R.section("schedules")
    for name in ("justhodl-katlin-daily", "justhodl-katlin-backtest-weekly"):
        try:
            sc = sch.get_schedule(Name=name, GroupName="default")
            R.log("   %s %s %s" % (name, sc.get("ScheduleExpression"), sc.get("State")))
        except Exception as e:
            FAILS.append("schedule %s missing: %s" % (name, str(e)[:80]))
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
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN: KATLIN live -- engine, feed, schedules, page")
    sys.exit(0)
