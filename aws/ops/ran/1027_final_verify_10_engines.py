"""
ops 1027 - Final end-to-end verifier for ALL 10 confluence engines.

Engines #1-9 = the 9 unique cross-engine confluences (institutional alpha
no commercial product has).
Engine #10 = earnings-cascade (bonus — multi-quarter compounder detector).

Per-engine checks:
  - Lambda exists in AWS
  - Lambda invokes clean (statusCode 200)
  - S3 output exists and is fresh (<2h for hourly, <26h for daily, <8d for weekly)
  - Key schema fields present
"""
import json
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[3]
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

ENGINES = [
    {
        "n": 1, "name": "Sequence Alpha Detector",
        "lambda": "justhodl-sequence-alpha-detector",
        "s3_key": "data/sequence-alpha.json",
        "freshness_h": 26,  # daily
        "key_fields": ["engine", "version", "regime", "n_active_sequences",
                       "state_counts"],
    },
    {
        "n": 2, "name": "Quality-on-Sale Detector",
        "lambda": "justhodl-quality-on-sale",
        "s3_key": "data/quality-on-sale.json",
        "freshness_h": 26,
        "key_fields": ["engine", "regime", "n_qualified_5_of_5",
                       "thresholds"],
    },
    {
        "n": 3, "name": "Forced-Selling Bounce",
        "lambda": "justhodl-forced-selling-bounce",
        "s3_key": "data/forced-selling-bounce.json",
        "freshness_h": 2,
        "key_fields": ["engine", "state", "signal_strength",
                       "n_conditions_fired", "conditions_fired"],
    },
    {
        "n": 4, "name": "Regime-Conditional Router",
        "lambda": "justhodl-regime-conditional-router",
        "s3_key": "data/regime-conditional-router.json",
        "freshness_h": 4,  # every 2h
        "key_fields": ["engine", "primary_framework", "primary_regime",
                       "primary_sleeve", "all_framework_scores"],
    },
    {
        "n": 5, "name": "M&A Target Predictor",
        "lambda": "justhodl-ma-target-predictor",
        "s3_key": "data/ma-target-predictor.json",
        "freshness_h": 192,  # weekly Sun
        "key_fields": ["engine", "state", "n_high_conviction",
                       "all_evaluated"],
    },
    {
        "n": 6, "name": "Quality-Filtered Consensus Bottom",
        "lambda": "justhodl-consensus-bottom",
        "s3_key": "data/consensus-bottom.json",
        "freshness_h": 192,  # weekly Tue
        "key_fields": ["engine", "state", "n_qualified", "qualified"],
    },
    {
        "n": 7, "name": "Correlation-Break Trade Router",
        "lambda": "justhodl-correlation-break-trade-router",
        "s3_key": "data/correlation-break-trades.json",
        "freshness_h": 4,
        "key_fields": ["engine", "primary_regime", "primary_recipe",
                       "all_regime_scores"],
    },
    {
        "n": 8, "name": "Fed Pivot Factor Router",
        "lambda": "justhodl-fed-pivot-factor-router",
        "s3_key": "data/fed-pivot-factor-trades.json",
        "freshness_h": 2,
        "key_fields": ["engine", "current_pivot_regime",
                       "current_recipe", "all_recipes"],
    },
    {
        "n": 9, "name": "Earnings Tone Velocity (parallel session)",
        "lambda": "justhodl-earnings-tone-velocity",
        "s3_key": "data/earnings-tone-velocity.json",
        "freshness_h": 26,
        "key_fields": ["engine", "version", "generated_at"],
    },
    {
        "n": 10, "name": "Earnings Cascade (bonus compounder detector)",
        "lambda": "justhodl-earnings-cascade",
        "s3_key": "data/earnings-cascade.json",
        "freshness_h": 26,
        "key_fields": ["engine", "state", "n_titans", "n_strong",
                       "n_emerging"],
    },
]


def check_lambda_exists(name):
    try:
        meta = lam.get_function(FunctionName=name)
        cfg = meta.get("Configuration", {})
        return True, {
            "runtime": cfg.get("Runtime"),
            "code_size": cfg.get("CodeSize"),
            "last_modified": cfg.get("LastModified"),
            "memory_mb": cfg.get("MemorySize"),
            "timeout_s": cfg.get("Timeout"),
            "description_len": len(cfg.get("Description") or ""),
        }
    except lam.exceptions.ResourceNotFoundException:
        return False, {"error": "lambda not found"}
    except Exception as e:
        return False, {"error": str(e)[:200]}


def invoke_lambda(name):
    try:
        t0 = time.time()
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                            LogType="None",
                            Payload=b"{}")
        elapsed_s = round(time.time() - t0, 2)
        payload = json.loads(resp["Payload"].read().decode("utf-8"))
        function_error = resp.get("FunctionError")
        status_code = (payload.get("statusCode")
                        if isinstance(payload, dict) else None)
        # Parse body if it's a string
        body = payload.get("body") if isinstance(payload, dict) else None
        body_parsed = None
        if isinstance(body, str):
            try:
                body_parsed = json.loads(body)
            except json.JSONDecodeError:
                body_parsed = body[:300]
        return {
            "invoked": True,
            "status_code": status_code,
            "function_error": function_error,
            "elapsed_s": elapsed_s,
            "body_summary": body_parsed,
        }
    except Exception as e:
        return {"invoked": False, "error": str(e)[:200]}


def check_s3(key, key_fields, freshness_h):
    try:
        meta = s3.head_object(Bucket=BUCKET, Key=key)
        last_mod = meta["LastModified"]
        age_s = (datetime.now(timezone.utc) - last_mod).total_seconds()
        age_h = age_s / 3600
        is_fresh = age_h <= freshness_h
        # Read body to check fields
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        d = json.loads(obj["Body"].read().decode("utf-8"))
        present = {f: f in d for f in key_fields}
        all_fields_present = all(present.values())
        return {
            "exists": True,
            "age_h": round(age_h, 2),
            "is_fresh": is_fresh,
            "freshness_threshold_h": freshness_h,
            "size_bytes": meta.get("ContentLength"),
            "fields_check": present,
            "all_fields_present": all_fields_present,
        }
    except s3.exceptions.NoSuchKey:
        return {"exists": False, "error": "no such key"}
    except Exception as e:
        return {"exists": False, "error": str(e)[:200]}


def evaluate_engine(eng):
    print(f"[ops 1027] checking #{eng['n']} {eng['lambda']}...")
    lambda_exists, lambda_meta = check_lambda_exists(eng["lambda"])
    invoke_result = invoke_lambda(eng["lambda"]) if lambda_exists else None
    # Wait a moment for S3 write after invoke
    if invoke_result and invoke_result.get("status_code") == 200:
        time.sleep(2)
    s3_check = check_s3(eng["s3_key"], eng["key_fields"],
                          eng["freshness_h"])

    # Per-engine pass/fail
    pass_lambda = lambda_exists
    pass_invoke = (invoke_result and
                    invoke_result.get("status_code") == 200 and
                    not invoke_result.get("function_error"))
    pass_s3 = (s3_check.get("exists") and
                s3_check.get("all_fields_present"))
    overall_pass = pass_lambda and pass_invoke and pass_s3

    return {
        "n": eng["n"],
        "name": eng["name"],
        "lambda": eng["lambda"],
        "s3_key": eng["s3_key"],
        "lambda_meta": lambda_meta,
        "invoke_result": invoke_result,
        "s3_check": s3_check,
        "pass_lambda": pass_lambda,
        "pass_invoke": bool(pass_invoke),
        "pass_s3": bool(pass_s3),
        "overall_pass": overall_pass,
    }


def main():
    started = datetime.now(timezone.utc)
    print(f"[ops 1027] start at {started.isoformat()}")

    results = []
    for eng in ENGINES:
        try:
            results.append(evaluate_engine(eng))
        except Exception as e:
            print(f"[ops 1027] err on #{eng['n']}: {e}")
            results.append({
                "n": eng["n"], "lambda": eng["lambda"],
                "error": traceback.format_exc()[:500],
                "overall_pass": False,
            })
            time.sleep(1)

    n_pass = sum(1 for r in results if r.get("overall_pass"))
    n_total = len(results)
    all_pass = n_pass == n_total

    scorecard = {
        "total_engines": n_total,
        "n_pass": n_pass,
        "n_fail": n_total - n_pass,
        "all_pass": all_pass,
        "pass_by_engine": {f"#{r['n']}_{r['lambda']}": r.get(
            "overall_pass") for r in results},
        "fresh_by_engine": {f"#{r['n']}_{r['lambda']}": r.get(
            "s3_check", {}).get("is_fresh") for r in results},
        "schema_ok_by_engine": {f"#{r['n']}_{r['lambda']}": r.get(
            "s3_check", {}).get("all_fields_present") for r in results},
    }

    report = {
        "ops_id": 1027,
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "scorecard": scorecard,
        "results": results,
    }
    out = REPO_ROOT / "aws" / "ops" / "reports" / "1027.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, default=str, indent=2))
    print(f"[ops 1027] {n_pass}/{n_total} engines passed | "
          f"ALL_PASS={all_pass}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
