"""ops/783 — redeploy upgraded cb-injection + dedupe ecb-detail schedule.

cb-injection's ECB leg now consumes data/ecb-detail.json. Redeploy it,
verify the ECB stance reflects the excess-liquidity signal, and clean the
duplicate target the deploy race left on the ecb-detail-daily rule.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
acct = boto3.client("sts", region_name="us-east-1").get_caller_identity()["Account"]

FN = "justhodl-cb-injection"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
BUCKET = "justhodl-dashboard-live"
report = {"ops": 783, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Redeploy cb-injection (ecb-detail wiring) + dedupe schedule"}

# 1. redeploy cb-injection
try:
    code = open(SRC, encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    report["redeploy"] = "ok"
    report["ecb_detail_wired"] = "ecb-detail.json" in code
except Exception as e:
    report["redeploy"] = f"{type(e).__name__}: {str(e)[:200]}"

for _ in range(30):
    try:
        if lam.get_function_configuration(
                FunctionName=FN).get("LastUpdateStatus") == "Successful":
            break
    except Exception:
        pass
    time.sleep(3)

# 2. dedupe duplicate targets on the ecb-detail-daily rule
try:
    fn_arn = f"arn:aws:lambda:us-east-1:{acct}:function:justhodl-ecb-detail"
    tg = events.list_targets_by_rule(Rule="ecb-detail-daily").get("Targets", [])
    report["ecb_detail_targets_before"] = len(tg)
    if len(tg) > 1:
        # keep one, remove the rest
        keep = tg[0]["Id"]
        drop = [t["Id"] for t in tg if t["Id"] != keep]
        events.remove_targets(Rule="ecb-detail-daily", Ids=drop)
        events.put_targets(Rule="ecb-detail-daily",
                           Targets=[{"Id": keep, "Arn": fn_arn}])
        report["dedupe"] = f"removed {len(drop)} duplicate target(s)"
    else:
        report["dedupe"] = "no duplicates"
    tg2 = events.list_targets_by_rule(Rule="ecb-detail-daily").get("Targets", [])
    report["ecb_detail_targets_after"] = len(tg2)
except Exception as e:
    report["dedupe"] = f"ERROR {str(e)[:180]}"

# 3. invoke cb-injection and confirm the ECB leg reflects ecb-detail
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:200]}

time.sleep(3)
cbi = {}
try:
    cbi = json.loads(s3.get_object(Bucket=BUCKET,
                     Key="data/cb-injection.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

ecb_bank = next((b for b in (cbi.get("central_banks") or [])
                 if b.get("cb") == "ECB"), {})
report["ecb_leg"] = {
    "stance_label": ecb_bank.get("stance_label"),
    "injection_stance": ecb_bank.get("injection_stance"),
    "excess_liquidity_eur_bn": ecb_bank.get("excess_liquidity_eur_bn"),
    "read": ecb_bank.get("read"),
}
report["global_impulse"] = (cbi.get("global_injection_impulse") or {}).get("label")

checks = {
    "redeploy_ok": report.get("redeploy") == "ok",
    "ecb_detail_wired": report.get("ecb_detail_wired") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "cb_injection_ok": cbi.get("ok") is True,
    "ecb_leg_has_excess_liquidity":
        ecb_bank.get("excess_liquidity_eur_bn") is not None,
    "schedule_deduped": report.get("ecb_detail_targets_after") == 1,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "CHAIN COMPLETE — cb-injection's ECB leg now consumes ecb-detail's "
    f"excess-liquidity signal (ECB stance {ecb_bank.get('stance_label')}, "
    f"excess liquidity EUR {ecb_bank.get('excess_liquidity_eur_bn')}bn). "
    "ecb-detail schedule deduped to a single daily target."
    if report["all_pass"] else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/783_cb_injection_ecb_detail_wire.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/783_cb_injection_ecb_detail_wire.json")
