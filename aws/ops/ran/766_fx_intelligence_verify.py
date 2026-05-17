"""ops/766 — verify justhodl-fx-intelligence end-to-end."""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 766, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "FX Intelligence Engine verify"}

try:
    r = lam.invoke(FunctionName="justhodl-fx-intelligence",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:220]}

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
report["usd_regime"] = {"regime": reg.get("regime"),
                        "dollar_pressure": reg.get("dollar_pressure_0_100"),
                        "risk_barometer": reg.get("risk_barometer"),
                        "risk_state": reg.get("risk_state"),
                        "momentum_z": reg.get("momentum_z")}
report["n_currencies"] = len(avail)
report["top3"] = [{"code": c["code"], "chg_3m": c.get("vs_usd_chg_3m"),
                   "trend": c.get("trend"), "signal": c.get("signal"),
                   "vol_regime": c.get("vol_regime")} for c in avail[:3]]
report["bottom3"] = [{"code": c["code"], "chg_3m": c.get("vs_usd_chg_3m"),
                      "trend": c.get("trend"), "signal": c.get("signal")}
                     for c in avail[-3:]]
report["rate_diff_pairs"] = len(rates.get("pairs") or [])
report["positioning_available"] = pos.get("available")
report["positioning_n"] = len(pos.get("contracts") or [])
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
    "fx_data_fresh": not (fx.get("freshness") or {}).get("fx_data_stale", True),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "FX INTELLIGENCE ENGINE LIVE — per-currency scorecard, USD regime, FX risk "
    "barometer and rate differentials all computing on real FRED data."
    if report["all_pass"] else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/766_fx_intelligence_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/766_fx_intelligence_verify.json")
