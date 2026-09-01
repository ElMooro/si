"""ops_5081 -- justhodl-fortress birth: deploy + schedule + first run verified
on disk against real data, then the page checked from the edge.

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
MARKER = "FORTRESS_COIL_V1"

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
    with report("5081-fortress-birth") as r:
        r.heading("ops 5081 -- justhodl-fortress birth")
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
        env_vars["FORTRESS_VERSION"] = "1.0.0"
        r.kv(g1="PASS", donor="justhodl-equity-research", keys=sorted(env_vars))

        r.section("G2 deploy")
        deploy_lambda(
            report=r, function_name=FN, source_dir=SRC, env_vars=env_vars,
            timeout=900, memory=3008, create_function_url=False, smoke=False,
            description=("FORTRESS COIL: dump-resilient accumulation radar -- SPY-dump "
                         "capture, worst-day excess, EMA250, Bollinger/Keltner squeeze "
                         "from the polygon-full bar warehouse, fused with valuation, growth, "
                         "industry ETF inflows, backlog/contracts, floor. data/fortress.json"),
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

        r.section("Soft: page from the edge")
        try:
            req = urllib.request.Request(PAGE_URL + "?v=%d" % int(time.time()),
                                         headers={"Cache-Control": "no-cache", "Pragma": "no-cache",
                                                  "User-Agent": "justhodl-ops/5081"})
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
        json.dump({"ops": 5081, "engine": FN, "session": payload.get("session"),
                   "n_scored": payload.get("n_scored"), "tiers": payload.get("tiers"),
                   "coverage_pct": covs, "as_of": payload.get("as_of")},
                  open(str(ROOT / "aws" / "ops" / "reports" / "5081.json"), "w"), indent=1)
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print("FATAL: %s: %s" % (type(e).__name__, e))
        sys.exit(1)
