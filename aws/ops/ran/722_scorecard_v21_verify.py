"""ops/722 — verify the signal-scorecard schema-2.1 fix.

commit 56948a0 excludes legacy + unresolved outcomes and recomputes
correctness from ground truth. Expected after the fix:
  - schema_version == 2.1
  - n_outcomes_legacy + n_outcomes_unresolved are non-trivial (proves the
    exclusion is active)
  - avg_graded_wilson_lb is plausible (artifact kept it ~0.14)
  - deprecation count is sensible, not 21/28
  - macro_composite_z / screener_top_pick still PROMOTED
  - market_phase / momentum_tlt / cot_extreme are now INSUFFICIENT
    (legacy/unresolved) rather than DEPRECATED
  - data_quality_flags is populated
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")

report = {"ops": 722, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "signal-scorecard schema 2.1 — legacy/unresolved exclusion"}

try:
    r = lam.invoke(FunctionName="justhodl-signal-scorecard",
                   InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"), "response": body[:600]}
except Exception as e:
    report["invoke"] = {"status": "error", "err": str(e)[:250]}

time.sleep(4)

d = None
try:
    o = s3.get_object(Bucket=BUCKET, Key="data/signal-scorecard.json")
    d = json.loads(o["Body"].read())
    report["last_modified"] = o["LastModified"].isoformat()
except Exception as e:
    report["sidecar_error"] = str(e)[:200]

if d:
    sc = d.get("scorecard", [])
    by = {r["signal_type"]: r for r in sc}
    graded = [r for r in sc if r.get("n_scored", 0) >= 15]

    report["summary"] = {
        "schema_version": d.get("schema_version"),
        "method": d.get("method"),
        "n_outcomes_scanned": d.get("n_outcomes_scanned"),
        "n_outcomes_scored": d.get("n_outcomes_scored"),
        "n_outcomes_neutral": d.get("n_outcomes_neutral"),
        "n_outcomes_legacy": d.get("n_outcomes_legacy"),
        "n_outcomes_unresolved": d.get("n_outcomes_unresolved"),
        "n_signals_tracked": d.get("n_signals_tracked"),
        "n_signals_graded": d.get("n_signals_graded"),
        "n_promoted": d.get("n_promoted"),
        "n_deprecated": d.get("n_deprecated"),
        "n_insufficient": d.get("n_insufficient"),
        "avg_graded_wilson_lb": d.get("avg_graded_wilson_lb"),
    }

    def row(r):
        return {"signal": r.get("signal_type"), "grade": r.get("grade"),
                "status": r.get("status"), "n_scored": r.get("n_scored"),
                "n_legacy": r.get("n_legacy"), "n_unresolved": r.get("n_unresolved"),
                "n_neutral": r.get("n_neutral"), "hit_rate": r.get("hit_rate"),
                "wilson_lb": r.get("wilson_lb"),
                "stored_flag_agreement": r.get("stored_flag_agreement"),
                "mult": r.get("performance_multiplier")}

    report["graded_signals"] = [row(r) for r in graded]
    report["data_quality_flags"] = d.get("data_quality_flags", [])
    report["promoted_list"] = d.get("promoted_signals", [])
    report["deprecated_list"] = d.get("deprecated_signals", [])

    watch = ["macro_composite_z", "screener_top_pick", "market_phase",
             "momentum_tlt", "cot_extreme", "corr_break_top_pair",
             "crisis_broad_dollar_vs_spy", "edge_regime"]
    report["watch_signals"] = {s: row(by[s]) for s in watch if s in by}

    lb = d.get("avg_graded_wilson_lb")
    leg = d.get("n_outcomes_legacy", 0) or 0
    unr = d.get("n_outcomes_unresolved", 0) or 0
    agrees = [r.get("stored_flag_agreement") for r in graded
              if r.get("stored_flag_agreement") is not None]
    checks = {
        "schema_is_2_1": d.get("schema_version") == "2.1",
        "legacy_excluded": leg > 0,
        "unresolved_excluded": unr > 0,
        "avg_lb_plausible": (lb is not None and lb >= 0.40),
        "deprecation_sensible": (d.get("n_deprecated", 99) <= max(3, len(graded) // 3)),
        "good_signals_still_promoted": (
            by.get("macro_composite_z", {}).get("status") == "PROMOTED"
            and by.get("screener_top_pick", {}).get("status") == "PROMOTED"),
        "legacy_signals_not_deprecated": all(
            by.get(s, {}).get("status") in (None, "INSUFFICIENT")
            for s in ["market_phase", "momentum_tlt"]),
        "dq_flags_populated": len(d.get("data_quality_flags", [])) > 0,
        "stored_flag_agreement_high": (
            bool(agrees) and (sum(agrees) / len(agrees)) >= 0.9),
        "invoke_ok": report["invoke"].get("fn_error") is None
                     and report["invoke"].get("status") == 200,
    }
    report["checks"] = checks
    report["all_pass"] = all(checks.values())
    report["verdict"] = ("FIX VERIFIED — scorecard grades only valid scored "
                         "outcomes; legacy/unresolved surfaced separately"
                         if report["all_pass"]
                         else "REVIEW — one or more checks failed")
else:
    report["all_pass"] = False
    report["verdict"] = "REVIEW — could not read sidecar"

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/722_scorecard_v21_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/722_scorecard_v21_verify.json")
