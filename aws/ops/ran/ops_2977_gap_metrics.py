#!/usr/bin/env python3
"""ops 2977 -- BUILD THE GAP MATRIX: deploy justhodl-gap-metrics, the
engine that closes 11 verified gaps from the fleet audit (ops 2974-76)
in one governed Lambda: M1 SLOOS, M13 stock-bond corr regime, M14 global
USD-M2 impulse, M16 OFR FSI, M22 muni/UST ratio, M11 revision breadth,
M15 miner margins, M19 EM carry proxy, M20 freight pulse (BDRY), M4
Treasury bill share, M9 implied correlation (best-effort). Each module
writes its own key for granular page wiring; combined index at
data/gap-metrics.json.

Sequence: (0) source probes from the runner (FRED SLOOS series, Polygon
SPY, TreasuryDirect, OFR CSV reachability); (1) deploy via shared helper
with env inherited from confluence-meta (FRED/POLYGON/FMP) + S3_BUCKET;
(2) EventBridge Scheduler daily 21:45 UTC (before compass 22:15 so
future compass versions can consume stock-bond corr); (3) synchronous
first run; (4) hard verify: index fresh, >=8/11 modules OK, and
module-specific value assertions on every OK module -- numbers in sane
bands, series lengths, honest DEGRADED notes on the rest.
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
                   config=Config(read_timeout=310, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
IAM = boto3.client("iam", region_name="us-east-1")
SCHED = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-gap-metrics"


def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2977"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def env_of(fn):
    return (LAM.get_function_configuration(FunctionName=fn)
            .get("Environment") or {}).get("Variables") or {}


def main():
    fails, warns = [], []
    hl = {}
    with report("2977_gap_metrics") as rep:

        rep.section("0. Source probes from the runner")
        base = env_of("justhodl-confluence-meta")
        env = {k: v for k, v in base.items()
               if any(t in k for t in ("FRED", "FMP", "POLYGON"))}
        env["S3_BUCKET"] = BUCKET
        rep.kv(env_keys=sorted(env))
        fkey = env.get("FRED_KEY") or env.get("FRED_API_KEY") or ""
        pkey = env.get("POLYGON_API_KEY") or env.get("POLYGON_KEY") or ""
        try:
            d = json.loads(http_get(
                "https://api.stlouisfed.org/fred/series/observations"
                "?series_id=DRTSCILM&api_key=%s&file_type=json"
                "&sort_order=desc&limit=3" % fkey))
            obs = [o for o in d.get("observations", [])
                   if o.get("value") not in (".", "", None)]
            rep.kv(fred_sloos_latest=obs[0]["value"] if obs else None)
            if not obs:
                fails.append("FRED DRTSCILM probe empty")
        except Exception as e:
            fails.append("FRED probe failed: %s" % e)
        try:
            g = json.loads(http_get(
                "https://api.polygon.io/v2/aggs/ticker/SPY/prev"
                "?adjusted=true&apiKey=" + pkey))
            rep.kv(polygon_spy_prev=bool(g.get("results")))
            if not g.get("results"):
                fails.append("Polygon SPY probe empty")
        except Exception as e:
            fails.append("Polygon probe failed: %s" % e)
        for name, url in (
                ("treasurydirect", "https://www.treasurydirect.gov/TA_WS/"
                 "securities/auctioned?days=10&format=json"),
                ("ofr_csv", "https://www.financialresearch.gov/"
                 "financial-stress-index/data/fsi.csv")):
            try:
                body = http_get(url, timeout=30)
                rep.kv(**{name + "_bytes": len(body)})
                if len(body) < 200:
                    warns.append("%s probe thin (%d bytes) -- module "
                                 "will DEGRADE honestly" % (name,
                                                            len(body)))
            except Exception as e:
                warns.append("%s unreachable from runner: %s -- module "
                             "degrades honestly" % (name, str(e)[:60]))
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("1. Deploy " + FN)
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, timeout=240, memory=256,
                      description="Gap-metrics: 11 fleet-audit gaps built "
                      "(SLOOS, stock-bond corr, global M2, OFR FSI, muni "
                      "ratio, revision breadth, miner margins, EM carry, "
                      "freight, bill share, implied corr). One key per "
                      "module + data/gap-metrics.json.",
                      create_function_url=False, smoke=False)

        rep.section("2. Scheduler daily 21:45 UTC")
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
            kw = dict(Name="justhodl-gap-metrics-daily",
                      GroupName="default",
                      ScheduleExpression="cron(45 21 * * ? *)",
                      ScheduleExpressionTimezone="UTC",
                      FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
                      Description="Gap metrics daily -- 21:45 UTC, before "
                                  "asset-compass 22:15.",
                      Target={"Arn": fn_arn, "RoleArn": role_arn,
                              "Input": json.dumps({"source": "scheduler"}),
                              "RetryPolicy": {"MaximumRetryAttempts": 1}})
            try:
                SCHED.get_schedule(Name="justhodl-gap-metrics-daily",
                                   GroupName="default")
                SCHED.update_schedule(**kw)
                rep.kv(schedule="updated")
            except SCHED.exceptions.ResourceNotFoundException:
                SCHED.create_schedule(**kw)
                rep.kv(schedule="created")

        rep.section("3. Synchronous first run")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FN, Payload=b"{}")
        body = json.loads(resp["Payload"].read() or b"{}")
        rep.kv(invoke_seconds=round(time.time() - t0, 1),
               fn_error=resp.get("FunctionError"),
               body=json.dumps(body)[:200])
        if resp.get("FunctionError"):
            fails.append("invoke FunctionError: %s" % json.dumps(body)[:300])
            _write(rep, fails, warns, hl)
            return

        rep.section("4. Hard verify")
        idx = s3_json("data/gap-metrics.json")
        age_min = (datetime.now(timezone.utc) - datetime.fromisoformat(
            idx["generated_at"].replace("Z", "+00:00"))
        ).total_seconds() / 60.0
        mods = idx.get("modules") or {}
        ok_names = [n for n, m in mods.items() if m["status"] == "OK"]
        deg = {n: (m.get("headline") or {}).get("note")
               for n, m in mods.items() if m["status"] != "OK"}
        rep.kv(age_min=round(age_min, 1), modules_ok=len(ok_names),
               ok=sorted(ok_names), degraded=json.dumps(deg)[:300])
        hl["modules_ok"] = len(ok_names)
        hl["ok"] = sorted(ok_names)
        hl["degraded"] = deg
        if age_min > 10:
            fails.append("index stale %.1f min" % age_min)
        if len(ok_names) < 8:
            fails.append("only %d/11 modules OK: degraded=%s"
                         % (len(ok_names), json.dumps(deg)[:400]))

        def mod(key):
            return s3_json(key)

        checks = {
            "sloos": lambda d: isinstance(
                d.get("headline_pct_tightening"), (int, float))
                and -60 <= d["headline_pct_tightening"] <= 100
                and d.get("direction") in ("TIGHTENING", "EASING",
                                           "NEUTRAL"),
            "stock_bond_corr": lambda d:
                -1 <= d.get("current_63d_corr", 9) <= 1
                and len(d.get("series_260d") or []) >= 100
                and d.get("regime") in ("POSITIVE", "NEGATIVE",
                                        "TRANSITION"),
            "global_m2": lambda d: isinstance(
                d.get("yoy_impulse_pct"), (int, float))
                and -30 < d["yoy_impulse_pct"] < 40
                and d.get("total_usd_bn", 0) > 20000,
            "ofr_fsi": lambda d: isinstance(d.get("fsi"), (int, float))
                and -8 < d["fsi"] < 15
                and len(d.get("series_1y") or []) >= 100,
            "muni_ratio": lambda d:
                0.3 < d.get("muni_treasury_ratio", 0) < 1.4,
            "revision_breadth": lambda d:
                0 <= d.get("breadth_pct_positive", -1) <= 100
                and d.get("names_covered", 0) >= 20,
            "miner_margin": lambda d:
                len(d.get("miners") or {}) >= 5
                and isinstance(d.get("median_margin_delta_pp"),
                               (int, float)),
            "em_carry": lambda d: isinstance(
                d.get("long_em_basket_63d_pct"), (int, float))
                and -40 < d["long_em_basket_63d_pct"] < 40,
            "baltic_dry": lambda d: d.get("bdry_close", 0) > 0
                and d.get("pctile_52w") is not None,
            "bill_share": lambda d:
                0 < d.get("bill_share_pct", -1) < 100
                and d.get("gross_issuance_usd_bn", 0) > 100,
            "cor3m": lambda d:
                0 < d.get("implied_corr_3m", -1) < 100,
        }
        for name, m in mods.items():
            if m["status"] != "OK":
                continue
            try:
                d = mod(m["key"])
                if not checks[name](d):
                    fails.append("%s: value assertions failed: %s"
                                 % (name, json.dumps(
                                     {k: v for k, v in d.items()
                                      if not isinstance(v, (list, dict))}
                                 )[:200]))
                else:
                    rep.kv(**{name: "verified"})
            except Exception as e:
                fails.append("%s: key read failed: %s" % (name,
                                                          str(e)[:80]))

        # pull the marquee numbers for the report
        try:
            hl["sloos"] = mod("data/sloos.json").get(
                "headline_pct_tightening")
            hl["stock_bond"] = {k: v for k, v in mod(
                "data/stock-bond-corr.json").items()
                if k in ("current_63d_corr", "regime")}
            hl["bill_share"] = mod("data/bill-share.json").get(
                "bill_share_pct")
            hl["m2_yoy"] = mod("data/global-m2.json").get(
                "yoy_impulse_pct")
            hl["ofr_fsi"] = mod("data/ofr-fsi.json").get("fsi")
            hl["muni_ratio"] = mod("data/muni-ratio.json").get(
                "muni_treasury_ratio")
        except Exception:
            pass

        if not fails:
            rep.ok("gap-metrics LIVE: %d/11 OK; SLOOS %s%% | stock-bond "
                   "%s | bills %s%% | M2 yoy %s%% | OFR %s | muni %s"
                   % (len(ok_names), hl.get("sloos"),
                      json.dumps(hl.get("stock_bond")),
                      hl.get("bill_share"), hl.get("m2_yoy"),
                      hl.get("ofr_fsi"), hl.get("muni_ratio")))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2977, "function": FN, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2977.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    if fails:
        sys.exit(1)


main()
sys.exit(0)
