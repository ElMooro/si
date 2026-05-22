"""
ops 1028 - Re-run verifier with corrected field-name expectation for
Engine #9 (earnings-tone-velocity uses schema_version/method/as_of, not
engine/version/generated_at).

ops 1027 caught 9/10 PASS; only failure was verifier mis-guessing field
names for #9. Engine is healthy — this is a verifier fix, not an engine fix.
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
        "n": 1, "lambda": "justhodl-sequence-alpha-detector",
        "s3_key": "data/sequence-alpha.json", "freshness_h": 26,
        "key_fields": ["engine", "version", "regime",
                       "n_active_sequences"],
    },
    {
        "n": 2, "lambda": "justhodl-quality-on-sale",
        "s3_key": "data/quality-on-sale.json", "freshness_h": 26,
        "key_fields": ["engine", "regime", "n_qualified_5_of_5"],
    },
    {
        "n": 3, "lambda": "justhodl-forced-selling-bounce",
        "s3_key": "data/forced-selling-bounce.json", "freshness_h": 2,
        "key_fields": ["engine", "state", "signal_strength",
                       "n_conditions_fired"],
    },
    {
        "n": 4, "lambda": "justhodl-regime-conditional-router",
        "s3_key": "data/regime-conditional-router.json",
        "freshness_h": 4,
        "key_fields": ["engine", "primary_framework", "primary_sleeve",
                       "all_framework_scores"],
    },
    {
        "n": 5, "lambda": "justhodl-ma-target-predictor",
        "s3_key": "data/ma-target-predictor.json", "freshness_h": 192,
        "key_fields": ["engine", "state", "n_high_conviction",
                       "all_evaluated"],
    },
    {
        "n": 6, "lambda": "justhodl-consensus-bottom",
        "s3_key": "data/consensus-bottom.json", "freshness_h": 192,
        "key_fields": ["engine", "state", "n_qualified", "qualified"],
    },
    {
        "n": 7, "lambda": "justhodl-correlation-break-trade-router",
        "s3_key": "data/correlation-break-trades.json",
        "freshness_h": 4,
        "key_fields": ["engine", "primary_regime", "primary_recipe",
                       "all_regime_scores"],
    },
    {
        "n": 8, "lambda": "justhodl-fed-pivot-factor-router",
        "s3_key": "data/fed-pivot-factor-trades.json",
        "freshness_h": 2,
        "key_fields": ["engine", "current_pivot_regime",
                       "current_recipe", "all_recipes"],
    },
    {
        # CORRECTED: parallel session used schema_version / method / as_of
        "n": 9, "lambda": "justhodl-earnings-tone-velocity",
        "s3_key": "data/earnings-tone-velocity.json",
        "freshness_h": 26,
        "key_fields": ["schema_version", "method", "as_of", "summary"],
    },
    {
        "n": 10, "lambda": "justhodl-earnings-cascade",
        "s3_key": "data/earnings-cascade.json", "freshness_h": 26,
        "key_fields": ["engine", "state", "n_titans", "n_strong",
                       "n_emerging"],
    },
]


def check_lambda_exists(name):
    try:
        meta = lam.get_function(FunctionName=name)
        cfg = meta.get("Configuration", {})
        return True, {"runtime": cfg.get("Runtime"),
                      "code_size": cfg.get("CodeSize"),
                      "last_modified": cfg.get("LastModified"),
                      "memory_mb": cfg.get("MemorySize")}
    except Exception as e:
        return False, {"error": str(e)[:200]}


def invoke_lambda(name):
    try:
        t0 = time.time()
        resp = lam.invoke(FunctionName=name,
                           InvocationType="RequestResponse",
                           Payload=b"{}")
        elapsed_s = round(time.time() - t0, 2)
        payload = json.loads(resp["Payload"].read().decode("utf-8"))
        function_error = resp.get("FunctionError")
        status_code = (payload.get("statusCode")
                        if isinstance(payload, dict) else None)
        return {"invoked": True, "status_code": status_code,
                "function_error": function_error,
                "elapsed_s": elapsed_s}
    except Exception as e:
        return {"invoked": False, "error": str(e)[:200]}


def check_s3(key, key_fields, freshness_h):
    try:
        meta = s3.head_object(Bucket=BUCKET, Key=key)
        last_mod = meta["LastModified"]
        age_h = (datetime.now(timezone.utc) -
                  last_mod).total_seconds() / 3600
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        d = json.loads(obj["Body"].read().decode("utf-8"))
        present = {f: f in d for f in key_fields}
        return {
            "exists": True,
            "age_h": round(age_h, 2),
            "is_fresh": age_h <= freshness_h,
            "size_bytes": meta.get("ContentLength"),
            "fields_check": present,
            "all_fields_present": all(present.values()),
        }
    except Exception as e:
        return {"exists": False, "error": str(e)[:200]}


def evaluate(eng):
    le, lm = check_lambda_exists(eng["lambda"])
    ir = invoke_lambda(eng["lambda"]) if le else None
    if ir and ir.get("status_code") == 200:
        time.sleep(2)
    s3c = check_s3(eng["s3_key"], eng["key_fields"], eng["freshness_h"])
    pl = le
    pi = ir and ir.get("status_code") == 200 and not ir.get(
        "function_error")
    ps = s3c.get("exists") and s3c.get("all_fields_present")
    return {
        "n": eng["n"], "lambda": eng["lambda"], "lambda_meta": lm,
        "invoke_result": ir, "s3_check": s3c,
        "pass_lambda": bool(pl), "pass_invoke": bool(pi),
        "pass_s3": bool(ps), "overall_pass": bool(pl and pi and ps),
    }


def main():
    started = datetime.now(timezone.utc)
    print(f"[ops 1028] start {started.isoformat()}")
    results = []
    for e in ENGINES:
        try:
            results.append(evaluate(e))
        except Exception as exc:
            print(f"[ops 1028] err #{e['n']}: {exc}")
            results.append({"n": e["n"], "lambda": e["lambda"],
                             "error": str(exc)[:300],
                             "overall_pass": False})
    n_pass = sum(1 for r in results if r.get("overall_pass"))
    all_pass = n_pass == len(results)
    report = {
        "ops_id": 1028,
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "scorecard": {
            "total_engines": len(results),
            "n_pass": n_pass,
            "n_fail": len(results) - n_pass,
            "all_pass": all_pass,
            "pass_by_engine": {f"#{r['n']}_{r['lambda']}":
                                r.get("overall_pass") for r in results},
        },
        "results": results,
    }
    out = REPO_ROOT / "aws" / "ops" / "reports" / "1028.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, default=str, indent=2))
    print(f"[ops 1028] {n_pass}/{len(results)} | ALL_PASS={all_pass}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
