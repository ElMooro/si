"""ops/806 — redeploy + verify the opportunity-screener price-target fix.

ops 805 left two defects: micro-cap rockets had no price target, and the
board showed fantasy upside (+200%+) on low-P/E cyclicals because a growth-
justified P/E was applied to structurally cheap banks/airlines. This
redeploys the credible-target fix and confirms targets are now sane and
micro-caps carry a (multi-year) target.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-opportunity-screener"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"

report = {"ops": 806, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Verify opportunity-screener credible price targets"}

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
        Bucket=BUCKET,
        Key="screener/opportunity-screener.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

board = ob.get("boom_candidates") or []
micro = ob.get("microcap_rockets") or []
beaters = ob.get("serial_beaters") or []
hidden = ob.get("hidden_growth") or []

# every 12-month target should now imply <= 80% upside (the credible cap)
twelve_mo = [b for b in board if b.get("target_horizon") == "12-month"
             and b.get("upside_pct") is not None]
insane = [{"sym": b["symbol"], "upside": b["upside_pct"]}
          for b in twelve_mo if b["upside_pct"] > 81]
micro_with_tgt = sum(1 for b in micro[:20]
                     if b.get("price_target") is not None)

report["boom_board"] = {
    "headline": ob.get("headline"),
    "counts": ob.get("counts"),
    "n_12mo_targets": len(twelve_mo),
    "insane_12mo_targets": insane,
    "max_12mo_upside": round(max((b["upside_pct"] for b in twelve_mo),
                                 default=0), 1),
    "microcap_top20_with_target": f"{micro_with_tgt}/20",
    "microcap_sample": [{"sym": b.get("symbol"), "cap": b.get("cap_tier"),
                         "price": b.get("price"),
                         "target": b.get("price_target"),
                         "upside": b.get("upside_pct"),
                         "horizon": b.get("target_horizon")}
                        for b in micro[:5]],
    "board_top6": [{"sym": b.get("symbol"), "cap": b.get("cap_tier"),
                    "opp": b.get("opportunity_score"),
                    "upside": b.get("upside_pct"),
                    "horizon": b.get("target_horizon")}
                   for b in board[:6]],
    "hidden_growth_top3": [{"sym": b.get("symbol"),
                            "upside": b.get("upside_pct")}
                           for b in hidden[:3]],
}

checks = {
    "deploy_ok": report.get("deploy") == "updated",
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ob.get("ok") is True,
    "no_fantasy_12mo_targets": len(insane) == 0,
    "microcaps_have_targets": micro_with_tgt >= 10,
    "all_views_populated": (len(micro) >= 5 and len(beaters) >= 5
                            and len(hidden) >= 5),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "BOOM BOARD CREDIBLE — 12-month targets now bounded (max upside "
    f"{report['boom_board']['max_12mo_upside']}%, zero fantasy targets); "
    f"{micro_with_tgt}/20 micro-caps carry a multi-year bagger target. "
    "All views populated. The Boom Board is production-ready."
    if report["all_pass"]
    else "REVIEW — see checks[]/boom_board")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/806_opportunity_screener_targets.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/806_opportunity_screener_targets.json")
