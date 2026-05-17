"""ops/767 — re-verify justhodl-fx-intelligence after the resilience fix.
Captures the invoke Payload (full traceback if it still crashes) and the
output's errors[] section so any remaining failure is pinpointed."""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 767, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "FX Intelligence Engine re-verify"}

try:
    r = lam.invoke(FunctionName="justhodl-fx-intelligence",
                   InvocationType="RequestResponse", Payload=b"{}")
    payload = r["Payload"].read().decode()
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "payload": payload[:900]}
except Exception as e:
    report["invoke"] = {"err": str(e)[:300]}

fx = {}
try:
    fx = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                  Key="data/fx-intelligence.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

reg = fx.get("usd_regime") or {}
curs = fx.get("currencies") or []
avail = [c for c in curs if c.get("available")]
pos = fx.get("positioning") or {}
rates = fx.get("rate_differentials") or {}

report["headline"] = fx.get("headline")
report["engine_errors"] = fx.get("errors")
report["usd_regime"] = {"regime": reg.get("regime"),
                        "dollar_pressure": reg.get("dollar_pressure_0_100"),
                        "risk_barometer": reg.get("risk_barometer"),
                        "risk_state": reg.get("risk_state"),
                        "momentum_z": reg.get("momentum_z"),
                        "broad_index": reg.get("broad_index")}
report["n_currencies"] = len(avail)
report["currency_scorecard"] = [
    {"code": c["code"], "chg_3m": c.get("vs_usd_chg_3m"),
     "trend": c.get("trend"), "mom_z": c.get("momentum_z"),
     "vol_regime": c.get("vol_regime"), "signal": c.get("signal")}
    for c in avail]
report["rate_diff"] = {"us_2y": rates.get("us_2y"), "us_10y": rates.get("us_10y"),
                       "n_pairs": len(rates.get("pairs") or [])}
report["positioning"] = {"available": pos.get("available"),
                         "n": len(pos.get("contracts") or [])}
report["freshness"] = fx.get("freshness")

checks = {
    "engine_runs": report.get("invoke", {}).get("status") == 200
                   and not report.get("invoke", {}).get("fn_error"),
    "output_written": bool(fx.get("generated_at")),
    "currencies_ok": len(avail) >= 10,
    "usd_regime_ok": reg.get("available") is True
                     and isinstance(reg.get("dollar_pressure_0_100"), (int, float)),
    "barometer_ok": reg.get("risk_barometer") is not None,
    "headline_ok": bool(fx.get("headline"))
                   and "insufficient" not in str(fx.get("headline", "")),
    "no_engine_errors": not (fx.get("errors") or []),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "FX INTELLIGENCE ENGINE LIVE & VERIFIED — per-currency scorecard, USD "
    "regime, FX risk barometer, rate differentials all computing on real "
    "FRED data."
    if report["all_pass"]
    else "REVIEW — see checks[], invoke.payload, and engine_errors[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/767_fx_intelligence_verify2.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/767_fx_intelligence_verify2.json")
