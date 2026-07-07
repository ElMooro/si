#!/usr/bin/env python3
"""ops 2966 -- Asset Compass: the forward-looking cross-asset Expected-
Return + Asymmetry engine (the capital-allocation layer the fleet lacked).

Sequence: (0) pull the fleet env bundle and probe FRED (DGS1/DGS2/
EXPINF1YR), Polygon (GLD) and CoinGecko (BTC) from the runner so every
feed is proven live before deploy; (1) create justhodl-asset-compass
with an EventBridge Scheduler schedule at 22:15 UTC daily; (2) invoke
synchronously and hard-verify data/asset-compass.json -- schema,
freshness, the market-implied macro-forward block (next-12m risk-free
rate + inflation), >=9 ER-modeled assets, per-asset asymmetry + breakout
states, honest er=None on anchor-less assets (commodities/crypto), the
survival-gate invariant (no can-die asset in a downtrend is ACTIONABLE),
and the acid test: the engine must REDISCOVER the gold<->real-rate
inversion (negative OLS beta, >=250 obs) from raw daily data; (3) warn-
level sibling-context checks (risk-regime / cross-asset-regime / rv).
Status PROVISIONAL per the Edge-Accuracy standard.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
IAM = boto3.client("iam")
SCHED = boto3.client("scheduler", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-asset-compass"
OUT_KEY = "data/asset-compass.json"
VALID_BO = {"SQUEEZE", "COILED", "BREAKOUT", "EXTENDED", "TRENDING", "NONE"}


def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2966",
                                               "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def env_of(fn):
    cfg = LAM.get_function_configuration(FunctionName=fn)
    return (cfg.get("Environment") or {}).get("Variables") or {}


def invoke(rep, fn, fails, label):
    t0 = time.time()
    resp = LAM.invoke(FunctionName=fn, InvocationType="RequestResponse",
                      Payload=json.dumps({"source": "ops-2966"}).encode())
    body = json.loads(resp["Payload"].read() or b"{}")
    rep.kv(**{label + "_seconds": round(time.time() - t0, 1),
              label + "_status": resp.get("StatusCode"),
              label + "_fn_error": resp.get("FunctionError"),
              label + "_body": json.dumps(body)[:250]})
    if resp.get("FunctionError") or body.get("statusCode") != 200:
        fails.append("%s invoke failed: %s" % (fn, json.dumps(body)[:300]))
    return body


def main():
    fails, warns = [], []
    with report("2966_asset_compass") as rep:
        rep.section("0. Env bundle + live-feed probes from the runner")
        base = env_of("justhodl-confluence-meta")
        env = {k: v for k, v in base.items()
               if any(t in k for t in ("FRED", "FMP", "POLYGON"))}
        env["S3_BUCKET"] = BUCKET
        rep.kv(env_keys=sorted(env.keys()))
        fkey = env.get("FRED_API_KEY") or env.get("FRED_KEY") or ""
        if not fkey:
            try:
                fkey = SSM.get_parameter(Name="/justhodl/fred/api-key",
                                         WithDecryption=True
                                         )["Parameter"]["Value"]
            except Exception:
                pass
        pkey = env.get("POLYGON_API_KEY") or env.get("POLYGON_KEY") or ""
        try:
            probes = {}
            for sid in ("DGS1", "DGS2", "EXPINF1YR"):
                d = json.loads(http_get(
                    "https://api.stlouisfed.org/fred/series/observations"
                    "?series_id=%s&api_key=%s&file_type=json&sort_order=desc"
                    "&limit=5" % (sid, fkey)))
                vals = [o["value"] for o in d.get("observations", [])
                        if o.get("value") not in (".", "", None)]
                probes[sid] = vals[0] if vals else None
            rep.kv(**{("fred_" + k): v for k, v in probes.items()})
            if not probes.get("DGS1") or not probes.get("DGS2"):
                fails.append("FRED curve probe empty (DGS1/DGS2)")
            if not probes.get("EXPINF1YR"):
                warns.append("EXPINF1YR empty -- engine falls back to T10YIE")
        except Exception as e:
            fails.append("FRED probe failed: %s" % e)
        try:
            g = json.loads(http_get(
                "https://api.polygon.io/v2/aggs/ticker/GLD/range/1/day/"
                "2026-06-25/2026-07-07?adjusted=true&limit=10&apiKey=" + pkey))
            n = len(g.get("results") or [])
            rep.kv(polygon_gld_bars=n)
            if n < 3:
                fails.append("Polygon GLD probe returned %s bars" % n)
        except Exception as e:
            fails.append("Polygon probe failed: %s" % e)
        try:
            cg = json.loads(http_get(
                "https://api.coingecko.com/api/v3/simple/price"
                "?ids=bitcoin&vs_currencies=usd", timeout=20))
            btc = (cg.get("bitcoin") or {}).get("usd")
            rep.kv(coingecko_btc_usd=btc)
            if not btc:
                warns.append("CoinGecko probe empty -- crypto rows may lack "
                             "data on first run")
        except Exception as e:
            warns.append("CoinGecko probe failed: %s" % e)
        if fails:
            rep.log("aborting before deploy: %s" % "; ".join(fails))
            _write(rep, fails, warns, {})
            sys.exit(1)

        rep.section("1. Deploy justhodl-asset-compass")
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, timeout=240, memory=256,
                      description='Asset Compass - forward cross-asset expected-return + asymmetry engine. Market-implied next-12m rf/inflation, Grinold-Kroner ER per class, asymmetry with survival gate, gold/silver breakouts. data/asset-compass.json. PROVISIONAL.',
                      create_function_url=False, smoke=False)

        rep.section("2. EventBridge Scheduler schedule")
        role_arn = None
        for rn in ("justhodl-scheduler-invoke", "justhodl-scheduler-role"):
            try:
                role_arn = IAM.get_role(RoleName=rn)["Role"]["Arn"]
                break
            except Exception:
                continue
        if not role_arn:
            fails.append("no scheduler role found")
        else:
            fn_arn = LAM.get_function_configuration(
                FunctionName=FN)["FunctionArn"]
            sched_kw = dict(
                Name="justhodl-asset-compass-daily", GroupName="default",
                ScheduleExpression="cron(15 22 * * ? *)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Description=("Asset Compass - daily after FRED H.15 "
                             "publication and US equity close."),
                Target={"Arn": fn_arn, "RoleArn": role_arn,
                        "Input": json.dumps({"source": "scheduler"}),
                        "RetryPolicy": {"MaximumRetryAttempts": 1}})
            try:
                SCHED.get_schedule(Name="justhodl-asset-compass-daily",
                                   GroupName="default")
                SCHED.update_schedule(**sched_kw)
                rep.kv(schedule="updated", role=role_arn)
            except SCHED.exceptions.ResourceNotFoundException:
                SCHED.create_schedule(**sched_kw)
                rep.kv(schedule="created", role=role_arn)

        rep.section("3. Synchronous first run + hard verify")
        invoke(rep, FN, fails, "compass")
        d = {}
        try:
            d = s3_json(OUT_KEY)
        except Exception as e:
            fails.append("cannot read %s: %s" % (OUT_KEY, e))
        if d:
            age_min = None
            try:
                age_min = (datetime.now(timezone.utc) -
                           datetime.fromisoformat(d["generated_at"])
                           ).total_seconds() / 60.0
            except Exception:
                pass
            mf = d.get("macro_forward") or {}
            betas = d.get("betas") or {}
            assets = d.get("assets") or []
            by = {a.get("ticker"): a for a in assets}
            er_n = sum(1 for a in assets
                       if isinstance(a.get("er_1y_pct"), (int, float)))
            rep.kv(schema=d.get("schema_version"),
                   age_min=round(age_min or -1, 1),
                   rf_now=mf.get("rf_now_pct"),
                   rf_1y_fwd=mf.get("rf_1y_forward_pct"),
                   rf_dir=mf.get("rf_direction_next_year"),
                   infl_1y=mf.get("infl_1y_expected_pct"),
                   infl_src=mf.get("infl_source"),
                   real_1y_fwd=mf.get("real_1y_forward_pct"),
                   growth_proxy=(mf.get("growth") or {})
                   .get("real_growth_proxy_pct"),
                   assets_n=len(assets), er_modeled=er_n,
                   gold_beta=betas.get("gold_vs_real_rate_pct_per_100bp"),
                   gold_beta_obs=betas.get("gold_beta_obs"),
                   gsr=betas.get("gold_silver_ratio"),
                   gsr_z=betas.get("gsr_z_10y"),
                   gld_bo=(by.get("GLD", {}).get("breakout") or {})
                   .get("state"),
                   slv_bo=(by.get("SLV", {}).get("breakout") or {})
                   .get("state"),
                   top_asym=json.dumps((d.get("boards") or {})
                                       .get("asymmetry_ranking", [])[:3])[:400],
                   warns_engine=len(d.get("warns") or []))
            if d.get("schema_version") != "1.0":
                fails.append("schema != 1.0")
            if age_min is None or age_min > 10:
                fails.append("stale output (age_min=%s)" % age_min)
            rf, rff = mf.get("rf_now_pct"), mf.get("rf_1y_forward_pct")
            infl = mf.get("infl_1y_expected_pct")
            if not (isinstance(rf, (int, float)) and 0.05 < rf < 12):
                fails.append("rf_now invalid: %s" % rf)
            if not isinstance(rff, (int, float)):
                fails.append("rf_1y_forward missing")
            if not (isinstance(infl, (int, float)) and 0 < infl < 8):
                fails.append("infl_1y_expected invalid: %s" % infl)
            if mf.get("rf_direction_next_year") not in ("LOWER", "HIGHER",
                                                        "FLAT"):
                fails.append("rf_direction invalid")
            g = (mf.get("growth") or {}).get("real_growth_proxy_pct")
            if not (isinstance(g, (int, float)) and 0 <= g <= 3):
                fails.append("growth proxy out of band: %s" % g)
            priced = [a for a in assets if a.get("price")]
            if len(priced) < 16:
                fails.append("only %s priced assets" % len(priced))
            if er_n < 9:
                fails.append("only %s ER-modeled assets" % er_n)
            # acid test: rediscover the gold<->real-rate inversion from raw data
            gb, gbn = betas.get("gold_vs_real_rate_pct_per_100bp"), \
                betas.get("gold_beta_obs") or 0
            if not (isinstance(gb, (int, float)) and gb < 0 and gbn >= 250):
                fails.append("gold/real-rate acid test failed "
                             "(beta=%s obs=%s; must be negative, >=250 obs)"
                             % (gb, gbn))
            if not isinstance(betas.get("gsr_z_10y"), (int, float)):
                fails.append("gold/silver ratio z missing")
            slv_ec = (by.get("SLV", {}).get("er_components") or {})
            if "gsr_reversion_pct" not in slv_ec:
                fails.append("SLV missing GSR reversion component")
            for a in priced:
                if a["ticker"] == "CASH":
                    continue
                if (a.get("breakout") or {}).get("state") not in VALID_BO:
                    fails.append("%s invalid breakout state" % a["ticker"])
                    break
                asym = a.get("asym") or {}
                if not isinstance(asym.get("ratio"), (int, float)):
                    fails.append("%s missing asym ratio" % a["ticker"])
                    break
                if (not a.get("structural")
                        and (a.get("trend") or {}).get("label") == "DOWNTREND"
                        and asym.get("status") == "ACTIONABLE"):
                    fails.append("survival-gate breach on %s" % a["ticker"])
                    break
            for t in ("BTC", "ETH", "USO", "DBC", "CPER"):
                if by.get(t, {}).get("er_1y_pct") is not None:
                    fails.append("%s has fabricated ER (must be None)" % t)
            if by.get("BTC", {}).get("price") and \
                    not any("LOW_N" in f for f in
                            by["BTC"].get("flags", [])):
                fails.append("BTC missing LOW_N honesty flag")
            for t in ("BTC", "ETH"):
                if by.get(t) and not by[t].get("price"):
                    warns.append("%s: no price from CoinGecko or Polygon "
                                 "X: pair this run" % t)
            boards = d.get("boards") or {}
            if not boards.get("er_ranking") or \
                    not boards.get("asymmetry_ranking"):
                fails.append("boards empty")

        rep.section("4. Sibling context (warn-only)")
        ctx = (d.get("context") or {})
        rep.kv(context=json.dumps(ctx)[:250])
        if not ctx.get("risk_regime"):
            warns.append("risk-regime context not attached")

        rep.section("verdict")
        rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
        _write(rep, fails, warns, d)
        if fails:
            sys.exit(1)


def _write(rep, fails, warns, d):
    mf = (d.get("macro_forward") or {})
    boards = (d.get("boards") or {})
    out = {"ops": 2966, "function": FN, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "rf_now": mf.get("rf_now_pct"),
           "rf_1y_fwd": mf.get("rf_1y_forward_pct"),
           "rf_dir": mf.get("rf_direction_next_year"),
           "infl_1y": mf.get("infl_1y_expected_pct"),
           "gold_beta": (d.get("betas") or {})
           .get("gold_vs_real_rate_pct_per_100bp"),
           "er_top3": boards.get("er_ranking", [])[:3],
           "asym_top3": boards.get("asymmetry_ranking", [])[:3],
           "breakout_watch": boards.get("breakout_watch"),
           "ts": datetime.now(timezone.utc).isoformat()}
    rp = AWS_DIR / "ops" / "reports" / "2966.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("report written: %s" % rp)


main()
sys.exit(0)
