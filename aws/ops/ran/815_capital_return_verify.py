"""ops/815 - redeploy + verify justhodl-capital-return after the units fix.

The screener stores yield/margin fields already in percent; the engine had
been rescaling them, inflating a few names (BX FCF yield 123%, etc).
Fixed: raw percent values used directly, Financials/Real Estate excluded
(FCF is not a real funding metric for lenders), absurd yields bounded out.

Verifies the realism gate now passes: no shareholder yield >35%, no FCF
yield >80%, financials absent, cannibals found, targets present.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-capital-return"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"

report = {"ops": 815, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Verify capital-return units fix"}

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
try:
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    for _ in range(40):
        if lam.get_function_configuration(
                FunctionName=FN).get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    report["deploy"] = "updated"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

time.sleep(3)
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)
ob = {}
try:
    ob = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/capital-return.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:160]

cann = ob.get("cannibals") or []
insane_sy = [c["symbol"] for c in cann
             if (c.get("shareholder_yield_pct") or 0) > 35]
insane_fcf = [c["symbol"] for c in cann
              if (c.get("fcf_yield_pct") or 0) > 80]
fin = [c["symbol"] for c in cann
       if c.get("sector") in ("Financial Services", "Financials",
                              "Real Estate")]
with_target = sum(1 for c in cann[:30] if c.get("price_target") is not None)
report["capital_return"] = {
    "n_evaluated": ob.get("n_evaluated"), "n_cannibals": len(cann),
    "insane_yields": insane_sy, "insane_fcf": insane_fcf,
    "financials_present": fin, "top30_with_target": with_target,
    "top8": [{"sym": c.get("symbol"), "sector": c.get("sector"),
              "score": c.get("cannibal_score"),
              "buyback": c.get("buyback_yield_pct"),
              "total_yield": c.get("shareholder_yield_pct"),
              "fcf_yield": c.get("fcf_yield_pct"),
              "pe": c.get("pe_ratio"),
              "upside": c.get("upside_pct")} for c in cann[:8]],
}

checks = {
    "deploy_ok": report.get("deploy") == "updated",
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ob.get("ok") is True,
    "cannibals_found": len(cann) >= 5,
    "yields_sane": len(insane_sy) == 0,
    "fcf_sane": len(insane_fcf) == 0,
    "financials_excluded": len(fin) == 0,
    "targets_present": with_target >= 5,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CAPITAL-RETURN VERIFIED - {len(cann)} cannibals, all yields sane, "
    "Financials/Real Estate excluded, "
    f"{with_target}/30 top names carry a price target. Cannibal screen "
    "production-clean."
    if report["all_pass"] else "REVIEW - see checks[]/capital_return")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/815_capital_return_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/815_capital_return_verify.json")
