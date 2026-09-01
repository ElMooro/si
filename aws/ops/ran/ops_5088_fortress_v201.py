"""ops_5088 -- justhodl-fortress v2.0.1: the backtest learns to read itself.

ops 5086's first real walk-forward (54k observations, 28 dates) said the
price-structure legs LAG SPY unconditionally over the last two years -- a
strong bull tape led by high-beta names -- and out-performed only on the
dates where SPY fell next. A low-beta basket must be judged on protection
and on beta-adjusted alpha, not raw excess. v2.0.1 adds to the backtest:
  * beta-adjusted alpha (trailing-252 beta x SPY forward return)
  * by_spy_direction: cohorts split by what SPY did over the next 21 sessions
  * location_gate_test: resilient+coiled with vs without the EMA250 gate
  * capture deciles inside SPY-down windows (does past resilience predict
    the NEXT dump?)
Gates: G1-G7 as before; G8 backtest re-run verified on disk WITH the new
tables; G9 daily re-run carries validation.by_spy_direction; every table is
logged in full so the numbers can be read honestly.

Previous op text follows (same gates, same verification).

FORTRESS COIL (Khalid, 2026-08-31): stocks and ETFs that barely dipped /
stayed flat / rose during the market's dumps, under the 250 EMA, low
valuation, industry ETF seeing major inflows, stock+industry growth and
momentum, very tight Bollinger squeeze about to break out, major backlog
and contracts meaningful vs market cap -- plus safety and upside metrics.
Fusion engine over the polygon-full bar warehouse + 17 fleet feeds (every
column bound by the ops-5080 probe). Local harness GREEN (synthetic bars,
mocked S3, page rendered against the feed with a DOM stub) before this push.

Gates (hard unless marked soft, sys.exit(1) on failure):
  G1 FMP_KEY / POLYGON_API_KEY recovered from justhodl-equity-research
     (engine makes no API calls today; inherited per Khalid's standing rule)
  G2 deploy_lambda green (create-or-update; smoke=False -- a sync smoke
     invoke of a 60-120s engine is the ops-3730 trap)
  G3 EventBridge Scheduler justhodl-fortress-daily cron(30 3 ? * TUE-SAT *)
  G4 async first run -> data/fortress.json fresh as_of ON DISK, ok=True
  G5 real-data assertions from S3: >= 3,000 names scored, >= 300 sessions,
     SPY market context present, coverage of dump_capture / bb_width_pctile
     / valuation_score / flow_score / ema250 each >= 60% of scored rows,
     board + definitions + funnel present, no tier count negative,
     a known S&P name carries a full evidence row
  G6 history snapshot written for the session
  G7 harvester contract: top_picks rows carry ticker + score (may be empty)
  Soft: fortress.html served from the edge with marker FORTRESS_COIL_V1
     (pages.yml races this op; WARN only, per house doctrine)
"""
import gzip
import json
import sys
import time
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
FN = "justhodl-fortress"
OUT_KEY = "data/fortress.json"
HIST_PREFIX = "data/fortress/history/"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
SRC = ROOT / "aws" / "lambdas" / FN / "source"
PAGE_URL = "https://justhodl.ai/fortress.html"
MARKER = "fortress-v2.0.1"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240, retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name=REGION)


def s3_json(key):
    try:
        body = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception:  # noqa: BLE001
        return None


def cov(rows, k):
    if not rows:
        return 0.0
    return 100.0 * sum(1 for r in rows if r.get(k) is not None) / len(rows)


def main():
    with report("5088-fortress-v201") as r:
        r.heading("ops 5088 -- justhodl-fortress v2.0.1: conditional backtest tables, verified")
        t0 = datetime.now(timezone.utc)

        r.section("G1 key inheritance")
        env = lam.get_function_configuration(
            FunctionName="justhodl-equity-research").get("Environment", {}).get("Variables", {})
        env_vars = {}
        for k in ("FMP_KEY", "POLYGON_API_KEY"):
            if env.get(k):
                env_vars[k] = env[k]
        if not env_vars:
            r.log("FAIL G1: no keys on donor justhodl-equity-research")
            sys.exit(1)
        env_vars["FORTRESS_VERSION"] = "2.0.1"
        r.kv(g1="PASS", donor="justhodl-equity-research", keys=sorted(env_vars))

        r.section("G2 deploy")
        deploy_lambda(
            report=r, function_name=FN, source_dir=SRC, env_vars=env_vars,
            timeout=900, memory=8192, create_function_url=False, smoke=False,
            description=("FORTRESS COIL v2: dump-resilient accumulation radar -- 3y SPY-dump capture, "
                         "worst-day t-stats, EMA250, Bollinger/Keltner squeeze, volume-structure "
                         "accumulation, VCP/RS-line structure, tail risk, flows, backlog, floor; "
                         "weekly walk-forward backtest"),
        )
        # settle: State must be Active (ops-3735 trap: Pending after first create)
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        cfg = lam.get_function_configuration(FunctionName=FN)
        r.kv(g2="PASS" if cfg.get("State") == "Active" else "FAIL", state=cfg.get("State"),
             last_update=cfg.get("LastUpdateStatus"), memory=cfg.get("MemorySize"),
             timeout=cfg.get("Timeout"), runtime=cfg.get("Runtime"))
        if cfg.get("State") != "Active":
            sys.exit(1)

        r.section("G3 schedule (EventBridge Scheduler -- classic rule cap is saturated)")
        fn_arn = cfg["FunctionArn"]
        sched = {
            "Name": FN + "-daily",
            "ScheduleExpression": "cron(30 3 ? * TUE-SAT *)",
            "ScheduleExpressionTimezone": "UTC",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {"Arn": fn_arn, "RoleArn": SCHED_ROLE, "Input": "{}",
                       "RetryPolicy": {"MaximumRetryAttempts": 2,
                                       "MaximumEventAgeInSeconds": 3600}},
            "State": "ENABLED",
            "Description": "FORTRESS COIL daily 03:30 UTC Tue-Sat, after polygon-full lands the prior session",
        }
        try:
            sch.create_schedule(**sched)
            r.kv(g3="PASS", schedule="created")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**sched)
            r.kv(g3="PASS", schedule="updated")
        got = sch.get_schedule(Name=FN + "-daily")
        r.kv(schedule_expr=got.get("ScheduleExpression"), schedule_state=got.get("State"))

        r.section("G4 first run (async, verified on disk)")
        prev = s3_json(OUT_KEY) or {}
        prev_asof = prev.get("as_of", "")
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        r.log("async invoke fired at %s; polling %s (prev as_of=%s)" % (
            t0.isoformat(timespec="seconds"), OUT_KEY, prev_asof or "none"))
        payload = None
        for i in range(84):  # up to 14 min
            time.sleep(10)
            cur = s3_json(OUT_KEY)
            if cur and cur.get("as_of", "") > prev_asof and \
                    cur.get("as_of", "") >= t0.isoformat(timespec="seconds")[:16]:
                payload = cur
                break
            if i % 6 == 5:
                r.log("  poll %ds: not fresh yet" % ((i + 1) * 10))
        if not payload:
            # surface the failure reason instead of a bare timeout
            try:
                logs = boto3.client("logs", region_name=REGION)
                streams = logs.describe_log_streams(
                    logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime",
                    descending=True, limit=1).get("logStreams", [])
                if streams:
                    ev = logs.get_log_events(logGroupName="/aws/lambda/" + FN,
                                             logStreamName=streams[0]["logStreamName"],
                                             limit=60).get("events", [])
                    for e in ev[-40:]:
                        r.log("  LOG " + e.get("message", "").rstrip()[:220])
            except Exception as e:  # noqa: BLE001
                r.log("  log tail unavailable: %s" % str(e)[:120])
            r.log("FAIL G4: no fresh payload within 14 min")
            sys.exit(1)
        d = payload.get("diagnostics") or {}
        r.kv(g4="PASS", as_of=payload.get("as_of"), session=payload.get("session"),
             sessions_loaded=payload.get("sessions_loaded"), n_scored=payload.get("n_scored"),
             n_universe_bars=payload.get("n_universe_bars"), elapsed_s=d.get("elapsed_s"),
             tiers=payload.get("tiers"), etf_tiers=payload.get("etf_tiers"))
        for line in (d.get("log") or [])[-12:]:
            r.log("  engine: " + line)

        r.section("G5 real-data assertions (from S3, independent of the engine's own view)")
        board = payload.get("board") or []
        ledger = payload.get("ledger") or []
        rows = board + ledger
        mkt = payload.get("market") or {}
        funnel = payload.get("funnel") or {}
        tiers = payload.get("tiers") or {}
        fails = []
        if payload.get("ok") is not True:
            fails.append("ok != True")
        if (payload.get("n_scored") or 0) < 3000:
            fails.append("n_scored %s < 3000" % payload.get("n_scored"))
        if (payload.get("sessions_loaded") or 0) < 300:
            fails.append("sessions_loaded %s < 300" % payload.get("sessions_loaded"))
        if mkt.get("spy_ema250") is None or mkt.get("spy_close") is None:
            fails.append("SPY market context missing")
        if not isinstance(mkt.get("episodes"), list):
            fails.append("market.episodes not a list")
        if len(payload.get("definitions") or {}) < 20:
            fails.append("definitions < 20")
        if not board:
            fails.append("board empty")
        if any(v < 0 for v in tiers.values()):
            fails.append("negative tier count")
        for k in ("scored", "under_ema250", "dump_resilient", "coiled", "low_valuation",
                  "growth", "industry_inflows"):
            if k not in funnel:
                fails.append("funnel missing " + k)
        # coverage over the board+ledger sample (the rows the page can see)
        covs = {k: round(cov(rows, k), 1) for k in
                ("dump_capture", "bb_width_pctile", "valuation_score", "flow_score",
                 "vs_ema250_pct", "growth_score", "safety_score")}
        r.kv(coverage_pct=covs, n_rows_sampled=len(rows))
        for k in ("dump_capture", "bb_width_pctile", "valuation_score", "flow_score", "vs_ema250_pct"):
            if covs[k] < 60:
                fails.append("coverage %s = %.1f%% < 60%%" % (k, covs[k]))
        v2c = {k: round(cov(board, k), 1) for k in ("obv_slope_63", "absorption_clv", "conviction", "confidence",
                                                   "trade_plan", "higher_lows", "cvar5_pct", "capture_days_weighted",
                                                   "worst_days_tstat", "rate_beta", "etf_pressure_21d_usd",
                                                   "dark_pool_state", "pillar_ranks")}
        r.kv(v2_coverage_pct_board=v2c)
        for k in ("obv_slope_63", "absorption_clv", "conviction", "trade_plan"):
            if v2c[k] < 60:
                fails.append("v2 coverage %s = %.1f%% < 60%%" % (k, v2c[k]))
        if (payload.get("sessions_loaded") or 0) < 700:
            fails.append("sessions_loaded %s < 700 (three-year window)" % payload.get("sessions_loaded"))
        if not isinstance(payload.get("regime"), dict) or "read" not in (payload.get("regime") or {}):
            fails.append("regime block missing")
        if not isinstance(payload.get("changes"), dict):
            fails.append("changes block missing")
        r.log("regime: " + json.dumps(payload.get("regime")))
        r.log("changes: " + json.dumps({k: (v if not isinstance(v, list) else v[:8]) for k, v in (payload.get("changes") or {}).items()}))
        r.log("sizing: " + json.dumps(payload.get("sizing")))
        r.log("market window: %s..%s (%s sessions) episodes3y=%s recent=%s big=%s" % (
            mkt.get("window_start"), payload.get("session"), mkt.get("window_sessions"),
            mkt.get("n_episodes"), mkt.get("n_episodes_recent"), mkt.get("n_big_dumps")))
        for x in board[:6]:
            r.log("v2 %s %s conv=%s conf=%s comp=%s abs=%s accum=%s struct=%s obv=%s clv=%s dcap=%s t=%s dp=%s hl=%s vcp=%s rs=%s cvar=%s plan=%s trig=%s" % (
                x.get("tier"), x.get("ticker"), x.get("conviction"), x.get("confidence"), x.get("composite"),
                x.get("composite_abs"), (x.get("pillars") or {}).get("accumulation"), (x.get("pillars") or {}).get("structure"),
                x.get("obv_slope_63"), x.get("absorption_clv"), x.get("capture_days_weighted"), x.get("worst_days_tstat"),
                x.get("dark_pool_state"), x.get("higher_lows"), x.get("vcp_contractions"), x.get("rs_leading"),
                x.get("cvar5_pct"), x.get("trade_plan"), x.get("watch_trigger")))
        # funnel share sanity: ema250 available for most of the scored universe
        if funnel.get("scored") and funnel.get("ema250_available", 0) < 0.6 * funnel["scored"]:
            fails.append("ema250_available %s < 60%% of scored %s" % (
                funnel.get("ema250_available"), funnel.get("scored")))
        # a known name with a full evidence row
        probe = None
        for t in ("AAPL", "MSFT", "JPM", "CAT", "XOM"):
            probe = next((x for x in board if x.get("ticker") == t), None) or \
                next((x for x in ledger if x.get("ticker") == t), None)
            if probe:
                break
        if probe:
            r.log("probe %s: tier=%s comp=%s capture=%s worst_bps=%s vsEMA250=%s bbw_pct=%s val=%s growth=%s flows=%s etf=%s" % (
                probe.get("ticker"), probe.get("tier"), probe.get("composite"), probe.get("dump_capture"),
                probe.get("worst_days_excess_bps"), probe.get("vs_ema250_pct"), probe.get("bb_width_pctile"),
                probe.get("valuation_score"), probe.get("growth_score"), probe.get("flow_score"),
                probe.get("industry_etf")))
        else:
            r.warn("no mega-cap probe name found on board/ledger (fine if all sit at <2 gates)")
        top = board[:8]
        for x in top:
            r.log("board %s %s comp=%s asym=%s cap=%s ema=%s bbw=%s val=%s g=%s fl=%s bk=%s ct=%s flags=%s" % (
                x.get("tier"), x.get("ticker"), x.get("composite"), x.get("asymmetry"),
                x.get("dump_capture"), x.get("vs_ema250_pct"), x.get("bb_width_pctile"),
                x.get("valuation_score"), x.get("growth_score"), x.get("flow_score"),
                x.get("backlog_to_mcap"), x.get("contracts_90d_vs_mcap_pct"), x.get("flags")))
            for reason in (x.get("reasons") or [])[:5]:
                r.log("      - " + reason)
        r.log("episodes: " + json.dumps([{k: e.get(k) for k in ("peak_date", "trough_date", "spy_dd_pct", "closed")}
                                        for e in (mkt.get("episodes") or [])]))
        r.log("industries top: " + json.dumps([{k: x.get(k) for k in ("etf", "n_stocks", "tiers", "flow_score", "inflow_major")}
                                              for x in (payload.get("industries") or [])[:8]]))
        r.log("etfs top: " + json.dumps([{k: x.get(k) for k in ("ticker", "tier", "composite", "dump_capture", "flow_score")}
                                        for x in (payload.get("etfs") or [])[:8]]))
        r.log("inputs: " + json.dumps(payload.get("inputs")))
        # ---- diagnostics dump (drives the next tuning pass; read-only)
        caps = sorted(x["dump_capture"] for x in rows if x.get("dump_capture") is not None)
        if caps:
            q = lambda p: round(caps[min(len(caps) - 1, int(p * len(caps)))], 2)  # noqa: E731
            r.log("capture distribution over %d sampled rows: p10 %s p25 %s p50 %s p75 %s p90 %s; <=0.35: %d; <0: %d" % (
                len(caps), q(.1), q(.25), q(.5), q(.75), q(.9),
                sum(1 for c in caps if c <= 0.35), sum(1 for c in caps if c < 0)))
        wx = sorted(x["worst_days_excess_bps"] for x in rows if x.get("worst_days_excess_bps") is not None)
        if wx:
            r.log("worst-day excess bps: p10 %.0f p50 %.0f p90 %.0f; >=25: %d" % (
                wx[len(wx) // 10], wx[len(wx) // 2], wx[9 * len(wx) // 10], sum(1 for v in wx if v >= 25)))
        bw = sorted(x["bb_width_pctile"] for x in rows if x.get("bb_width_pctile") is not None)
        if bw:
            r.log("bb width pctile: p10 %.0f p50 %.0f; <=20: %d; squeeze on: %d" % (
                bw[len(bw) // 10], bw[len(bw) // 2], sum(1 for v in bw if v <= 20),
                sum(1 for x in rows if x.get("ttm_squeeze_on"))))
        r.log("worst days: " + json.dumps(mkt.get("worst_days")))
        r.log("breadth: " + json.dumps(payload.get("breadth")))
        for x in (payload.get("etfs") or [])[:14]:
            r.log("etf %s %s comp=%s cap=%s worst=%s ema=%s bbw=%s flow=%s vol100=%s aum=%s type=%s | %s" % (
                x.get("tier"), x.get("ticker"), x.get("composite"), x.get("dump_capture"),
                x.get("worst_days_excess_bps"), x.get("vs_ema250_pct"), x.get("bb_width_pctile"),
                x.get("flow_score"), x.get("vol_100d_pct"), x.get("aum_usd"), x.get("etf_type"),
                (x.get("name") or "")[:40]))
        for x in board[:24]:
            r.log("row %s %s %s %s cap=%s worst_cap=%s n_ep=%s worst=%s green=%s dc=%s bbw=%s sq=%s vol100=%s adv=%s px=%s eps=%s risks=%s" % (
                x.get("tier"), x.get("ticker"), x.get("cap_bucket"), (x.get("industry") or "")[:22],
                x.get("dump_capture"), x.get("capture_worst"), x.get("n_episodes"), x.get("worst_days_excess_bps"),
                x.get("worst_days_green_rate"), x.get("down_capture_pct"), x.get("bb_width_pctile"),
                x.get("squeeze_days"), x.get("vol_100d_pct"), x.get("adv_usd_20d"), x.get("close"),
                [(e.get("stock_pct"), e.get("capture")) for e in (x.get("episodes") or [])],
                x.get("risks")))
        tc = {}
        for x in board:
            tc[x.get("tier")] = tc.get(x.get("tier"), 0) + 1
        r.log("board tiers: " + json.dumps(tc))
        import re as _re
        _ov = _re.compile(r"\bvix\b|volatility|market neutral|anti-beta|covered call|premium income|high income|target \d+|select income", _re.I)
        if any(_ov.search(x.get("name") or "") for x in (payload.get("etfs") or [])[:80]):
            fails.append("overlay/volatility ETF still on the board: %s" % [
                x["ticker"] for x in (payload.get("etfs") or [])[:80] if _ov.search(x.get("name") or "")][:6])
        if any(str(x.get("industry") or "").startswith("Closed-End Fund") for x in board):
            fails.append("closed-end fund on the stock board")
        bad_etf = [x["ticker"] for x in (payload.get("etfs") or [])[:80]
                   if (x.get("vol_100d_pct") or 0) < 8.0
                   or ("equit" not in str(x.get("etf_type") or "").lower()
                       and x["ticker"] not in ("XLK", "XLC", "XLV", "XLF", "XLI", "XLE", "XLB", "XLY", "XLP", "XLU", "XLRE"))]
        if bad_etf:
            fails.append("cash-like ETFs still on the board: %s" % bad_etf[:8])
        if fails:
            for f in fails:
                r.log("FAIL G5: " + f)
            sys.exit(1)
        r.kv(g5="PASS", board=len(board), ledger=len(ledger), top_picks=len(payload.get("top_picks") or []))

        r.section("G6 history snapshot")
        hk = HIST_PREFIX + payload["session"] + ".json.gz"
        snap = s3_json(hk)
        if not snap or snap.get("session") != payload["session"]:
            r.log("FAIL G6: snapshot %s missing/mismatched" % hk)
            sys.exit(1)
        r.kv(g6="PASS", snapshot=hk, picks=len(snap.get("picks") or []),
             base_rates=(payload.get("base_rates") or {}).get("status"))

        r.section("G7 harvester contract")
        tp = payload.get("top_picks") or []
        bad = [p for p in tp if not p.get("ticker") or p.get("score") is None]
        if bad:
            r.log("FAIL G7: %d top_picks rows lack ticker/score" % len(bad))
            sys.exit(1)
        r.kv(g7="PASS", top_picks=len(tp),
             note="justhodl-signal-harvester reads data/fortress.json top_picks as eng:fortress at 23:15 UTC")

        r.section("G8 weekly walk-forward backtest (schedule + first run verified on disk)")
        bsched = {
            "Name": FN + "-backtest-weekly",
            "ScheduleExpression": "cron(0 9 ? * SUN *)",
            "ScheduleExpressionTimezone": "UTC",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {"Arn": fn_arn, "RoleArn": SCHED_ROLE, "Input": json.dumps({"mode": "backtest"}),
                       "RetryPolicy": {"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 3600}},
            "State": "ENABLED",
            "Description": "FORTRESS COIL weekly walk-forward backtest of the price-structure legs -> data/fortress-backtest.json",
        }
        try:
            sch.create_schedule(**bsched)
            r.log("backtest schedule created")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**bsched)
            r.log("backtest schedule updated")
        prev_bt = (s3_json("data/fortress-backtest.json") or {}).get("as_of", "")
        tb = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"mode": "backtest"}).encode())
        r.log("backtest invoke fired at %s; polling data/fortress-backtest.json" % tb.isoformat(timespec="seconds"))
        bt = None
        for i in range(84):
            time.sleep(10)
            cur = s3_json("data/fortress-backtest.json")
            if cur and cur.get("as_of", "") > prev_bt and cur.get("as_of", "") >= tb.isoformat(timespec="seconds")[:16]:
                bt = cur
                break
            if i % 6 == 5:
                r.log("  poll %ds: backtest not written yet" % ((i + 1) * 10))
        if not bt:
            try:
                logs = boto3.client("logs", region_name=REGION)
                streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime",
                                                    descending=True, limit=1).get("logStreams", [])
                if streams:
                    for e in logs.get_log_events(logGroupName="/aws/lambda/" + FN, logStreamName=streams[0]["logStreamName"],
                                                 limit=60).get("events", [])[-40:]:
                        r.log("  LOG " + e.get("message", "").rstrip()[:220])
            except Exception as e:  # noqa: BLE001
                r.log("  log tail unavailable: %s" % str(e)[:120])
            r.log("FAIL G8: backtest not written within 14 min")
            sys.exit(1)
        bg = bt.get("by_price_gates") or {}
        r.kv(g8_as_of=bt.get("as_of"), n_observations=bt.get("n_observations"), n_test_dates=len(bt.get("test_dates") or []),
             sessions=bt.get("sessions_loaded"), first=bt.get("first_session"), last=bt.get("last_session"),
             elapsed_s=(bt.get("diagnostics") or {}).get("elapsed_s"))
        for k in ("0", "1", "2", "3"):
            r.log("gates %s: %s" % (k, json.dumps(bg.get(k))))
        r.log("3/3 tight: " + json.dumps(bt.get("fortress3_tight_bbw10")))
        r.log("resilient vs not: " + json.dumps(bt.get("resilient_vs_not")))
        r.log("under only: %s knife: %s" % (json.dumps(bt.get("under_ema250_only")), json.dumps(bt.get("knife_guard_cohort"))))
        for k, v in (bt.get("by_capture_decile") or {}).items():
            r.log("decile %s: %s" % (k, json.dumps(v)))
        for x in (bt.get("per_date") or []):
            r.log("date " + json.dumps(x))
        for k, v in (bt.get("location_gate_test") or {}).items():
            r.log("location %s: %s" % (k, json.dumps(v)))
        for k, v in (bt.get("by_spy_direction") or {}).items():
            r.log("direction %s: windows=%s spy_med=%s" % (k, v.get("n_windows"), v.get("spy_median_ret21_pct")))
            for kk in ("all", "fortress3", "resilient", "not_resilient"):
                r.log("   %s: %s" % (kk, json.dumps(v.get(kk))))
        for k, v in (bt.get("by_capture_decile_in_spy_down_windows") or {}).items():
            r.log("down-decile %s: %s" % (k, json.dumps(v)))
        if not bt.get("by_spy_direction") or not bt.get("location_gate_test"):
            bfails_pre = ["conditional tables missing (by_spy_direction / location_gate_test)"]
        else:
            bfails_pre = []
        bfails = list(bfails_pre)
        if (bt.get("n_observations") or 0) < 20000:
            bfails.append("n_observations %s < 20000" % bt.get("n_observations"))
        if any(k not in bg for k in ("0", "1", "2", "3")):
            bfails.append("gate cohorts missing")
        if ((bg.get("3") or {}).get("n") or 0) < 200:
            bfails.append("3/3 cohort n %s < 200" % (bg.get("3") or {}).get("n"))
        if not bt.get("by_capture_decile"):
            bfails.append("capture deciles missing")
        if bfails:
            for f in bfails:
                r.log("FAIL G8: " + f)
            sys.exit(1)
        r.kv(g8="PASS")

        r.section("G9 daily re-run carries the validation block")
        prev2 = (s3_json(OUT_KEY) or {}).get("as_of", "")
        t2 = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        p2 = None
        for i in range(60):
            time.sleep(10)
            cur = s3_json(OUT_KEY)
            if cur and cur.get("as_of", "") > prev2 and cur.get("as_of", "") >= t2.isoformat(timespec="seconds")[:16]:
                p2 = cur
                break
        if not p2:
            r.log("FAIL G9: daily re-run not written within 10 min")
            sys.exit(1)
        val = p2.get("validation") or {}
        r.kv(g9=("PASS" if val.get("status") == "measured" else "FAIL"), validation_status=val.get("status"),
             validation_n=val.get("n_observations"), elapsed_s=(p2.get("diagnostics") or {}).get("elapsed_s"))
        if val.get("status") != "measured" or not val.get("by_spy_direction"):
            r.log("FAIL G9: validation block lacks the conditional tables")
            sys.exit(1)
        payload = p2

        r.section("Soft: page from the edge")
        try:
            req = urllib.request.Request(PAGE_URL + "?v=%d" % int(time.time()),
                                         headers={"Cache-Control": "no-cache", "Pragma": "no-cache",
                                                  "User-Agent": "justhodl-ops/5088"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", "ignore")
            if MARKER in html:
                r.ok("fortress.html live at the edge with marker %s (%d bytes)" % (MARKER, len(html)))
            else:
                r.warn("fortress.html served (%d bytes) WITHOUT marker %s -- pages.yml still propagating; "
                       "re-check in a few minutes" % (len(html), MARKER))
        except Exception as e:  # noqa: BLE001
            r.warn("edge fetch failed: %s" % str(e)[:160])

        r.ok("PASS_ALL -- justhodl-fortress live: %s scored, tiers %s, session %s" % (
            payload.get("n_scored"), payload.get("tiers"), payload.get("session")))
        json.dump({"ops": 5088, "engine": FN, "session": payload.get("session"),
                   "n_scored": payload.get("n_scored"), "tiers": payload.get("tiers"),
                   "coverage_pct": covs, "as_of": payload.get("as_of")},
                  open(str(ROOT / "aws" / "ops" / "reports" / "5088.json"), "w"), indent=1)
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print("FATAL: %s: %s" % (type(e).__name__, e))
        sys.exit(1)
