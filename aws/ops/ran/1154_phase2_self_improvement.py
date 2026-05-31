"""ops 1154 — Phase 2 closed-loop SELF-IMPROVEMENT rollout.

Phase 1 (1152+1153): predictions logged + skill cockpit displays accuracy.
Phase 2 (THIS): system applies what it learned — calibrator detects per-engine
miscalibration and writes confidence_scale overrides; signal-logger reads
that config and applies the overrides to all new predictions. The loop closes.

Plus an opportunity ranker that surfaces the best CURRENT setups, weighted
by each engine's track record.

Deploys:
  1. Patched router with:
     - avg_claimed_confidence + calibration_error per engine in skill index
     - generate_self_improvement_check() function
     - generate_opportunity_ranker() function
     - Two new contexts: self-improvement-calibrator, opportunity-ranker
  2. Patched signal-logger with:
     - Reads data/_skill/calibration-config.json on every cycle
     - _apply_calibration(engine, raw_conf) applies engine-specific scale
     - All 5 prediction sites stamped with calibration_version + raw_confidence
       in metadata (so before/after improvement can be measured)
  3. Two EventBridge rules:
     - justhodl-self-improvement-daily       cron(0 11 * * ? *)  11:00 UTC
     - justhodl-opportunity-ranker-4h        rate(4 hours)
  4. Initial calibration-config.json with version 1.0 (no overrides yet —
     all engines at scale=1.0 until enough data accumulates)
  5. Frontend updates: /skill.html hero says "Phase 2 LIVE", cockpit kit
     now renders 3 new sections (opportunities at top, improvement log,
     existing skill sections below)

Test fires:
  - Self-improvement calibrator: runs, finds engines have <20 scored
    predictions, returns "calibrated/insufficient_data" status for all,
    keeps confidence_scale = 1.0 across the board (correct day-1 behavior)
  - Opportunity ranker: runs, reads skill + sniffer briefs, produces
    ranked list using whatever hit-rates we have (default 0.50 for
    untracked engines)
  - Signal-logger: re-fires, this time reads calibration-config, applies
    scale=1.0 (no-op), stamps calibration_version=1.0 on new predictions
"""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone, timedelta
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
BUCKET = "justhodl-dashboard-live"
REGISTRY_KEY = "config/ai-brief-contexts.json"
ACCOUNT_ID = "857687956942"
ROUTER_FN = "justhodl-ai-brief-router"
LOGGER_FN = "justhodl-signal-logger"

EB_CALIB_RULE = "justhodl-self-improvement-daily"
EB_CALIB_SCHED = "cron(0 11 * * ? *)"  # 11:00 UTC daily (30min after aggregator)

EB_OPP_RULE = "justhodl-opportunity-ranker-4h"
EB_OPP_SCHED = "rate(4 hours)"  # every 4h, matches sniffer cadence

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)
eb = boto3.client("events", region_name=REGION, config=_cfg)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(fn, t=300):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") in ("Successful", None)):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(3)
    return False


def update_with_retries(fn, zipped, max_tries=8):
    last_err = None
    for attempt in range(1, max_tries + 1):
        wait_active(fn)
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zipped, Publish=False)
            return {"attempt": attempt, "status": "OK"}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            last_err = str(e)[:200]
            if code == "ResourceConflictException":
                time.sleep(10 + attempt * 2)
                continue
            raise
    return {"attempt": max_tries, "status": "EXHAUSTED", "err": last_err}


def setup_eb(rule_name, schedule, context_name, description):
    eb.put_rule(Name=rule_name, ScheduleExpression=schedule,
                 State="ENABLED", Description=description)
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{ROUTER_FN}"
    eb.put_targets(Rule=rule_name, Targets=[{
        "Id": "1", "Arn": fn_arn,
        "Input": json.dumps({"contexts": [context_name]}),
    }])
    stmt_id = f"eb-invoke-{rule_name}"
    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}"
    try:
        lam.add_permission(FunctionName=ROUTER_FN, StatementId=stmt_id,
                            Action="lambda:InvokeFunction",
                            Principal="events.amazonaws.com",
                            SourceArn=rule_arn)
        return {"rule": "created", "permission": "added"}
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            return {"rule": "updated", "permission": "existed"}
        return {"rule": "updated", "permission_err": str(e)[:200]}


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Upload 40-context registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        body = open(registry_path).read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        reg = json.loads(body)
        rpt["registry"] = {
            "n_contexts": len(reg.get("contexts") or {}),
            "calibrator_present":  "self-improvement-calibrator" in reg["contexts"],
            "opp_ranker_present":  "opportunity-ranker" in reg["contexts"],
        }

        # 2) Write initial calibration-config.json (version 1.0, no overrides)
        initial_cal = {
            "version":          "1.0",
            "version_int":      1,
            "updated_at":       datetime.now(timezone.utc).isoformat(),
            "engine_overrides": {},
            "history":          [],
            "pending_proposals": [],
            "skipped":          [],
            "thresholds": {
                "min_n_scored":     20,
                "err_threshold":    0.10,
                "max_step":         0.15,
                "scale_min":        0.5,
                "scale_max":        1.5,
                "step_factor":      0.5,
            },
        }
        s3.put_object(Bucket=BUCKET, Key="data/_skill/calibration-config.json",
                       Body=json.dumps(initial_cal, indent=2).encode("utf-8"),
                       ContentType="application/json",
                       CacheControl="no-store")
        rpt["initial_calibration_config"] = "written (v1.0, no overrides)"

        # 3) Redeploy router + signal-logger
        router_src = os.path.join(REPO_ROOT, "aws/lambdas", ROUTER_FN, "source")
        rpt["redeploy_router"] = update_with_retries(ROUTER_FN, zip_src(router_src))
        wait_active(ROUTER_FN)
        logger_src = os.path.join(REPO_ROOT, "aws/lambdas", LOGGER_FN, "source")
        rpt["redeploy_logger"] = update_with_retries(LOGGER_FN, zip_src(logger_src))
        wait_active(LOGGER_FN)

        # 4) Create EB rules
        rpt["eb_calibrator"] = setup_eb(EB_CALIB_RULE, EB_CALIB_SCHED,
                                         "self-improvement-calibrator",
                                         "Daily self-improvement calibrator")
        rpt["eb_opp_ranker"] = setup_eb(EB_OPP_RULE, EB_OPP_SCHED,
                                         "opportunity-ranker",
                                         "Every-4h opportunity ranker")

        # 5) Fire calibrator
        print("[1154] firing self-improvement-calibrator …")
        time.sleep(2)
        wait_active(ROUTER_FN)
        inv1 = lam.invoke(FunctionName=ROUTER_FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"contexts": ["self-improvement-calibrator"]}).encode())
        b1 = json.loads(inv1["Payload"].read() or b"{}")
        if isinstance(b1, dict) and "body" in b1:
            try: b1 = json.loads(b1["body"])
            except: pass
        rpt["calibrator_fire"] = {
            "fn_err": inv1.get("FunctionError"),
            "n_ok": b1.get("n_ok") if isinstance(b1, dict) else None,
            "results": b1.get("results") if isinstance(b1, dict) else None,
        }

        # 6) Fire opportunity ranker
        print("[1154] firing opportunity-ranker …")
        time.sleep(3)
        wait_active(ROUTER_FN)
        inv2 = lam.invoke(FunctionName=ROUTER_FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"contexts": ["opportunity-ranker"]}).encode())
        b2 = json.loads(inv2["Payload"].read() or b"{}")
        if isinstance(b2, dict) and "body" in b2:
            try: b2 = json.loads(b2["body"])
            except: pass
        rpt["opp_ranker_fire"] = {
            "fn_err": inv2.get("FunctionError"),
            "n_ok": b2.get("n_ok") if isinstance(b2, dict) else None,
            "results": b2.get("results") if isinstance(b2, dict) else None,
        }

        # 7) Re-fire signal-logger to verify it picks up calibration-config
        print("[1154] firing signal-logger to verify calibration-config read …")
        time.sleep(3)
        wait_active(LOGGER_FN)
        inv3 = lam.invoke(FunctionName=LOGGER_FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"action": "1154_calibration_verify"}).encode(),
                          LogType="Tail")
        b3 = json.loads(inv3["Payload"].read() or b"{}")
        if isinstance(b3, dict) and "body" in b3:
            try: b3 = json.loads(b3["body"])
            except: pass
        rpt["logger_fire"] = {
            "fn_err": inv3.get("FunctionError"),
            "logged": b3.get("logged") if isinstance(b3, dict) else None,
        }
        # Extract front-run log lines + look for FRONTRUN-PREDICTIONS errors
        logs = base64.b64decode(inv3.get("LogResult","")).decode("utf-8","replace")
        front_lines = [L for L in logs.split("\n")
                        if any(k in L for k in ["frontrun_sniffer_setup",
                                                "macro_frontrun_sniffer_setup",
                                                "convergence_fingerprint",
                                                "sustained_target_equity",
                                                "FRONTRUN-PREDICTIONS"])]
        rpt["frontrun_lines"] = "\n".join(front_lines[:15])

        # 8) Read back all 3 output files
        for key, label in [
            ("data/_skill/calibration-config.json", "calibration_config"),
            ("data/_skill/opportunity-rankings.json", "opportunity_rankings"),
            ("data/_skill/frontrun-skill-index.json", "skill_index"),
        ]:
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                doc = json.loads(obj["Body"].read())
                if label == "calibration_config":
                    rpt[label] = {
                        "version":           doc.get("version"),
                        "n_engine_overrides": len(doc.get("engine_overrides") or {}),
                        "n_history":         len(doc.get("history") or []),
                        "n_pending":         len(doc.get("pending_proposals") or []),
                        "n_skipped":         len(doc.get("skipped") or []),
                        "thresholds":        doc.get("thresholds"),
                    }
                elif label == "opportunity_rankings":
                    rpt[label] = {
                        "generated_at":  doc.get("generated_at"),
                        "n_total":       doc.get("n_total"),
                        "engine_hit_rates": doc.get("engine_hit_rates"),
                        "top_5":         [{"rank": o["rank"], "asset": o["asset"],
                                            "direction": o["direction"], "score": o["score"],
                                            "engine": o["engine"]}
                                           for o in (doc.get("ranked_opportunities") or [])[:5]],
                    }
                else:
                    rpt[label] = {
                        "n_total":   doc.get("n_total_predictions"),
                        "n_scored":  doc.get("n_scored"),
                        "n_pending": doc.get("n_pending"),
                        "engines":   list((doc.get("by_engine") or {}).keys()),
                    }
            except ClientError as e:
                rpt[label] = f"NOT_FOUND: {e.response['Error']['Code']}"

        # 9) Verify EB rules
        for rname in [EB_CALIB_RULE, EB_OPP_RULE]:
            try:
                d = eb.describe_rule(Name=rname)
                t = eb.list_targets_by_rule(Rule=rname)
                rpt[f"verify_{rname}"] = {
                    "state":      d.get("State"),
                    "schedule":   d.get("ScheduleExpression"),
                    "input":      (t.get("Targets") or [{}])[0].get("Input"),
                }
            except Exception as e:
                rpt[f"verify_{rname}"] = f"ERR: {str(e)[:150]}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1154.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "traceback"},
                     indent=2, default=str)[:6000])


if __name__ == "__main__":
    main()
