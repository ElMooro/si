"""ops/745 — verify the 4 repaired Lambdas (ops 744 fixes) now run clean."""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

report = {"ops": 745, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "verify 4 repaired Lambdas"}

TARGETS = ["justhodl-morning-intelligence", "news-sentiment-agent",
           "justhodl-email-reports-v2", "justhodl-nobrainer-rationale"]

results = {}
for fn in TARGETS:
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
        fn_err = r.get("FunctionError")
        results[fn] = {
            "status_code": r.get("StatusCode"),
            "function_error": fn_err,
            "response": body[:400],
            "pass": r.get("StatusCode") == 200 and fn_err is None,
        }
    except Exception as e:
        results[fn] = {"status_code": "error", "err": str(e)[:240],
                       "pass": False}

report["results"] = results
report["n_pass"] = sum(1 for v in results.values() if v.get("pass"))
report["n_total"] = len(TARGETS)
report["all_pass"] = report["n_pass"] == report["n_total"]
report["verdict"] = (
    f"FIXED — all {report['n_total']} Lambdas now run clean (no FunctionError)"
    if report["all_pass"]
    else f"PARTIAL — {report['n_pass']}/{report['n_total']} clean; "
         "see results for any still failing")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/745_repaired_lambdas_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/745_repaired_lambdas_verify.json")
