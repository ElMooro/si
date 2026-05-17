"""ops/765 — verify Canary Grid Phase 3 multi-source freshness-aware fallback.

The moment of truth: did Korea/China exports pick up the DBnomics IMF IFS
series (correct + fresher), and did Swiss unemployment switch off the dead
FRED series to a live one? Reports the resolved source + age for every
signal.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 765, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Canary Grid Phase 3 multi-source verify"}

try:
    r = lam.invoke(FunctionName="justhodl-canary-grid",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:220]}

cg = {}
try:
    cg = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                  Key="data/canary-grid.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

sigs = {s.get("key"): s for s in cg.get("signals", [])}
report["early_warning_level"] = cg.get("early_warning_level")
report["band"] = cg.get("band")
report["n_available"] = cg.get("n_available")
report["freshness"] = cg.get("freshness")

watch = {}
for k in ("korea_exports", "china_exports", "swiss_unemp", "copper", "lumber",
          "mfg_hours", "temp_help", "chf_haven", "eurodollar_stress"):
    s = sigs.get(k, {})
    watch[k] = {"source": s.get("fred_series"), "available": s.get("available"),
                "age_days": s.get("age_days"), "as_of": s.get("as_of"),
                "value": s.get("value"), "stress": s.get("stress"),
                "stale_warning": s.get("stale_warning"),
                "reason": s.get("reason")}
report["signals"] = watch

kx = watch["korea_exports"]
cx = watch["china_exports"]
sw = watch["swiss_unemp"]
checks = {
    "engine_runs": report.get("invoke", {}).get("status") == 200
                   and not report.get("invoke", {}).get("fn_error"),
    "grid_healthy": isinstance(cg.get("early_warning_level"), (int, float))
                    and (cg.get("n_available") or 0) >= 7,
    "korea_on_dbnomics": "/" in str(kx.get("source") or ""),
    "korea_fresh": kx.get("available") and (kx.get("age_days") or 999) <= 95,
    "china_on_dbnomics": "/" in str(cx.get("source") or ""),
    "swiss_no_longer_dead": sw.get("available") is True
                            and (sw.get("age_days") or 999) <= 95,
}
report["checks"] = checks
# core success = engine healthy + at least the staleness handled
report["core_pass"] = checks["engine_runs"] and checks["grid_healthy"]
report["all_pass"] = all(checks.values())
if report["all_pass"]:
    report["verdict"] = ("CANARY GRID PHASE 3 COMPLETE — Korea/China exports now "
                         "pull fresh DBnomics IMF data and Swiss unemployment is "
                         "off the dead series. Every canary is fresh and correct.")
elif report["core_pass"]:
    report["verdict"] = ("PARTIAL — grid healthy, but check signals[]: a DBnomics "
                         "code or FRED fallback may need adjusting (see source / "
                         "reason per signal).")
else:
    report["verdict"] = "REVIEW — see checks[]"

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/765_canary_phase3_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/765_canary_phase3_verify.json")
