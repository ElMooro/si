#!/usr/bin/env python3
"""ops 2965 -- Repo Market Desk: the dedicated overnight-funding engine,
fused fleet-wide, plus the crisis-plumbing SOFR-IORB sign-inversion fix.

Sequence: (0) probe the NY Fed markets API from the runner and print the
actual field shapes; (1) create justhodl-repo-market (env bundle copied
from confluence-meta + dollar-radar Telegram creds) with an EventBridge
Scheduler schedule at 13:35/21:35 UTC; (2) invoke it synchronously and
hard-verify data/repo-market.json -- schema, freshness, 9-component
score, the p99 tail series depth, and the acid test: the engine must
REDISCOVER 2019-09-17 as the worst tail day since 2018 from raw data;
(3) deploy + invoke the three fused consumers -- dollar-radar (13th
canary), risk-regime (funding block, weight 0.15) and crisis-plumbing
(direction-corrected SOFR-IORB buckets + new REPO_TAIL_P99 signal) --
and verify each fusion landed in its published JSON; (4) warn-level
live-path checks (CF worker + repo.html panel, which lag pages.yml).
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
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-repo-market"
OUT_KEY = "data/repo-market.json"
NYFED = "https://markets.newyorkfed.org/api"


def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2965",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def invoke(rep, fn, fails, label):
    t0 = time.time()
    resp = LAM.invoke(FunctionName=fn, InvocationType="RequestResponse",
                      Payload=json.dumps({"source": "ops-2965"}).encode())
    body = json.loads(resp["Payload"].read() or b"{}")
    rep.kv(**{label + "_seconds": round(time.time() - t0, 1),
              label + "_status": resp.get("StatusCode"),
              label + "_fn_error": resp.get("FunctionError"),
              label + "_body_status": body.get("statusCode")})
    if resp.get("FunctionError") or body.get("statusCode") != 200:
        fails.append("%s invoke failed: %s" % (fn, json.dumps(body)[:300]))
    return body


def env_of(fn):
    cfg = LAM.get_function_configuration(FunctionName=fn)
    return (cfg.get("Environment") or {}).get("Variables") or {}


def main():
    fails, warns = [], []
    with report("2965_repo_market") as rep:
        rep.section("0. Probe NY Fed markets API from the runner")
        try:
            raw = json.loads(http_get(
                NYFED + "/rates/secured/all/search.json?"
                "startDate=2019-09-10&endDate=2019-09-20"))
            rows = raw.get("refRates") or []
            sofr_919 = [r for r in rows
                        if str(r.get("type", "")).upper() == "SOFR"
                        and str(r.get("effectiveDate", ""))[:10] ==
                        "2019-09-17"]
            rep.kv(probe_rows=len(rows),
                   first_row_keys=sorted((rows[0] if rows else {}).keys()),
                   sep17_row=json.dumps(sofr_919[:1])[:500])
            if not rows:
                fails.append("NY Fed secured search returned no rows")
        except Exception as e:
            fails.append("NY Fed probe failed: %s" % e)
        try:
            srf = json.loads(http_get(NYFED + "/rp/srf/results/latest.json"))
            rep.kv(srf_probe_keys=sorted(srf.keys()),
                   srf_repo_rows=len(srf.get("repo") or []))
        except Exception as e:
            warns.append("SRF latest probe failed: %s" % e)
        if fails:
            rep.log("aborting before deploy: %s" % "; ".join(fails))
            _write(rep, fails, warns, {})
            sys.exit(1)

        rep.section("1. Env bundle + create justhodl-repo-market")
        base = env_of("justhodl-confluence-meta")
        dr_env = env_of("justhodl-dollar-radar")
        env = {k: v for k, v in base.items()
               if any(t in k for t in ("FRED", "FMP", "POLYGON"))}
        for k in ("TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"):
            if dr_env.get(k):
                env[k] = dr_env[k]
        env["S3_BUCKET"] = BUCKET
        rep.kv(env_keys=sorted(env.keys()))
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, timeout=240, memory=256,
                      description=("Repo Market Desk - dedicated overnight-"
                                   "funding stress engine. NY Fed SOFR "
                                   "distribution (p1/p25/p75/p99 + volume), "
                                   "TGCR/BGCR/EFFR/OBFR, SOFR-IORB with "
                                   "IOER splice, RRP/SRF/discount-window/"
                                   "swap-line buffers, reserves drain, "
                                   "calendar context, episode ranking since "
                                   "2018, 9-component 0-100 score + "
                                   "Telegram regime tripwires. "
                                   "data/repo-market.json."),
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
                Name="justhodl-repo-market-daily", GroupName="default",
                ScheduleExpression="cron(35 13,21 * * ? *)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Description=("Repo Market Desk - after the 8am ET NY Fed "
                             "publication and post-close."),
                Target={"Arn": fn_arn, "RoleArn": role_arn,
                        "Input": json.dumps({"source": "scheduler"}),
                        "RetryPolicy": {"MaximumRetryAttempts": 1}})
            try:
                SCHED.get_schedule(Name="justhodl-repo-market-daily",
                                   GroupName="default")
                SCHED.update_schedule(**sched_kw)
                rep.kv(schedule="updated", role=role_arn)
            except SCHED.exceptions.ResourceNotFoundException:
                SCHED.create_schedule(**sched_kw)
                rep.kv(schedule="created", role=role_arn)

        rep.section("3. Synchronous first run + verify repo-market.json")
        invoke(rep, FN, fails, "repo_market")
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
            sc = d.get("repo_stress_score")
            dist = d.get("distribution") or {}
            si = (d.get("spreads") or {}).get("sofr_iorb") or {}
            eps = (d.get("episodes") or {}).get("top_tail_days") or []
            rep.kv(schema=d.get("schema_version"),
                   age_min=round(age_min or -1, 1),
                   source=d.get("source_primary"),
                   score=sc, regime=d.get("regime"),
                   components_live=d.get("components_live"),
                   tail_bps=dist.get("tail_bps"),
                   tail_pctile=dist.get("tail_pctile_since_2018"),
                   tail_series_n=len(dist.get("series_tail_1y") or []),
                   sofr_iorb_bps=si.get("bps"),
                   si_series_n=len(si.get("series_1y") or []),
                   rrp_bn=(d.get("facilities") or {}).get("rrp_usd_bn"),
                   srf_bn=(d.get("facilities") or {}).get("srf_usd_bn"),
                   top_episodes=json.dumps(eps)[:400])
            if d.get("schema_version") != "1.0":
                fails.append("schema != 1.0")
            if age_min is None or age_min > 10:
                fails.append("stale output (age_min=%s)" % age_min)
            if not isinstance(sc, (int, float)) or not (0 <= sc <= 100):
                fails.append("score invalid: %s" % sc)
            if d.get("regime") not in ("CALM", "FIRM", "ELEVATED",
                                       "STRESSED", "SEIZING"):
                fails.append("regime invalid")
            if (d.get("components_live") or 0) < 7:
                fails.append("components_live < 7")
            primary = d.get("source_primary") == "NY Fed markets API"
            if primary:
                if not dist.get("available"):
                    fails.append("distribution unavailable on primary src")
                if not isinstance(dist.get("tail_bps"), (int, float)):
                    fails.append("tail_bps missing")
                if len(dist.get("series_tail_1y") or []) < 200:
                    fails.append("tail series too short")
                if not eps or eps[0].get("date") != "2019-09-17":
                    fails.append("acid test failed: worst tail day is %s, "
                                 "expected 2019-09-17"
                                 % (eps[0].get("date") if eps else None))
            else:
                warns.append("running on FRED fallback -- no distribution")
            if not isinstance(si.get("bps"), (int, float)):
                fails.append("sofr_iorb missing")
            if len(si.get("series_1y") or []) < 200:
                fails.append("sofr_iorb series too short")
            if not isinstance((d.get("facilities") or {}).get("rrp_usd_bn"),
                              (int, float)):
                fails.append("rrp missing")

        rep.section("4. Deploy + verify the three fused consumers")
        for cfn, tmo, mem in (("justhodl-dollar-radar", 180, 256),
                              ("justhodl-risk-regime", 120, 256),
                              ("justhodl-crisis-plumbing", 300, 512)):
            live = LAM.get_function_configuration(FunctionName=cfn)
            deploy_lambda(report=rep, function_name=cfn,
                          source_dir=AWS_DIR / "lambdas" / cfn / "source",
                          env_vars=(live.get("Environment") or {}
                                    ).get("Variables") or {},
                          timeout=max(live.get("Timeout") or tmo, tmo),
                          memory=max(live.get("MemorySize") or mem, mem),
                          description=live.get("Description") or "",
                          create_function_url=False, smoke=False)
            invoke(rep, cfn, fails, cfn.split("-")[-1])

        # dollar-radar: 13th canary
        try:
            dr = s3_json("data/dollar-radar.json")
            cans = dr.get("canaries") or []
            labels = [c.get("label") for c in cans]
            hit = [c for c in cans
                   if c.get("label") == "Onshore repo stress (SOFR plumbing)"]
            rep.kv(dr_canaries=len(cans), dr_repo_canary=json.dumps(
                hit[:1])[:300])
            if len(cans) != 13 or not hit:
                fails.append("dollar-radar fusion missing (canaries=%s, "
                             "labels=%s)" % (len(cans), labels))
        except Exception as e:
            fails.append("dollar-radar verify: %s" % e)

        # risk-regime: funding block (published under "components")
        try:
            rr = s3_json("data/risk-regime.json")
            fund = (rr.get("components") or {}).get("funding") or {}
            rep.kv(rr_keys=sorted(rr.keys())[:12],
                   rr_funding=json.dumps(fund)[:300])
            if not fund or not isinstance(fund.get("repo_stress_score"),
                                          (int, float)):
                fails.append("risk-regime funding block missing")
        except Exception as e:
            fails.append("risk-regime verify: %s" % e)

        # crisis-plumbing: sign fix + REPO_TAIL_P99
        try:
            cp = s3_json("data/crisis-plumbing.json")
            fc = cp.get("funding_credit_signals") or {}
            sib = fc.get("SOFR_IORB_SPREAD") or {}
            rt = fc.get("REPO_TAIL_P99") or {}
            spread = sib.get("spread_bps")
            expected = ("CRISIS" if (spread or 0) >= 20 else
                        "ELEVATED" if (spread or 0) >= 8 else
                        "WATCH" if (spread or 0) >= 3 else "NORMAL")
            rep.kv(cp_sofr_iorb_bps=spread, cp_signal=sib.get("signal"),
                   cp_expected_signal=expected,
                   cp_interp=(sib.get("interpretation") or "")[:120],
                   repo_tail=json.dumps(rt)[:300])
            if sib.get("available") and sib.get("signal") != expected:
                fails.append("crisis-plumbing SOFR-IORB buckets still "
                             "wrong: spread=%s signal=%s expected=%s"
                             % (spread, sib.get("signal"), expected))
            if not rt:
                fails.append("REPO_TAIL_P99 absent from crisis-plumbing")
            elif rt.get("available") and not isinstance(
                    rt.get("tail_bps"), (int, float)):
                fails.append("REPO_TAIL_P99 malformed")
        except Exception as e:
            fails.append("crisis-plumbing verify: %s" % e)

        rep.section("5. Live-path checks (warn-level, CDN/pages lag)")
        try:
            live = json.loads(http_get(
                "https://justhodl.ai/data/repo-market.json?ops=2965"))
            rep.kv(cf_worker_score=live.get("repo_stress_score"),
                   cf_worker_regime=live.get("regime"))
        except Exception as e:
            warns.append("CF worker path not yet serving: %s" % e)
        html_ok = False
        for _ in range(6):
            try:
                if "REPO MARKET DESK" in http_get(
                        "https://justhodl.ai/repo.html?ops=2965"):
                    html_ok = True
                    break
            except Exception:
                pass
            time.sleep(25)
        rep.kv(repo_html_panel_live=html_ok)
        if not html_ok:
            warns.append("repo.html panel not yet on Pages -- "
                         "re-check in a few minutes")

        rep.section("verdict")
        rep.kv(fails=fails, warns=warns)
        _write(rep, fails, warns, d)
        if fails:
            rep.log("FAILED: %s" % "; ".join(fails))
            sys.exit(1)
        rep.log("PASS -- repo market engine live and fused into "
                "dollar-radar, risk-regime and crisis-plumbing")


def _write(rep, fails, warns, d):
    out = {"ops": 2965, "function": FN, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "score": d.get("repo_stress_score"), "regime": d.get("regime"),
           "tail_bps": (d.get("distribution") or {}).get("tail_bps"),
           "sofr_iorb_bps": ((d.get("spreads") or {}).get("sofr_iorb")
                             or {}).get("bps"),
           "top_episode": ((d.get("episodes") or {}).get("top_tail_days")
                           or [{}])[0],
           "ts": datetime.now(timezone.utc).isoformat()}
    rp = AWS_DIR / "ops" / "reports" / "2965.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("report written: %s" % rp)


main()
sys.exit(0)
