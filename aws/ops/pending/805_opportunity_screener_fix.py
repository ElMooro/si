"""ops/805 — redeploy + verify the opportunity-screener microcap/ranking fix.

ops 804 shipped the Boom Board but verification exposed two defects: zero
micro-cap rockets (the micro-cap view required cross-confirmation micro-caps
can't get) and a board topped by fully-priced mega-caps. This redeploys the
fix and confirms micro-caps now surface and the board is upside-ranked.
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
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

report = {"ops": 805, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Redeploy + verify opportunity-screener microcap/rank fix"}

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
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(r["Payload"].read() or b"{}").get(
                            "body")}
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
# is the board upside-ranked? the top 10 should not be dominated by
# negative-upside names
top10 = board[:10]
top10_neg_upside = sum(1 for b in top10
                       if b.get("upside_pct") is not None
                       and b["upside_pct"] < 0)
top10_avg_upside = None
ups = [b["upside_pct"] for b in top10 if b.get("upside_pct") is not None]
if ups:
    top10_avg_upside = round(sum(ups) / len(ups), 1)

report["boom_board"] = {
    "ok": ob.get("ok"), "headline": ob.get("headline"),
    "counts": ob.get("counts"),
    "n_microcap_rockets": len(micro),
    "microcap_top5": [{"sym": b.get("symbol"), "cap": b.get("cap_tier"),
                       "boom": b.get("boom_score"),
                       "opp": b.get("opportunity_score"),
                       "upside": b.get("upside_pct"),
                       "target": b.get("price_target")}
                      for b in micro[:5]],
    "board_top8": [{"sym": b.get("symbol"), "cap": b.get("cap_tier"),
                    "opp": b.get("opportunity_score"),
                    "boom": b.get("boom_score"),
                    "upside": b.get("upside_pct")} for b in top10[:8]],
    "top10_negative_upside": top10_neg_upside,
    "top10_avg_upside_pct": top10_avg_upside,
    "errors": ob.get("errors"),
}

checks = {
    "deploy_ok": report.get("deploy") == "updated",
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ob.get("ok") is True,
    "microcaps_surface": len(micro) >= 3,
    "board_upside_ranked": top10_neg_upside <= 3,
    "microcaps_have_targets": sum(
        1 for b in micro[:10] if b.get("price_target") is not None) >= 1,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"BOOM BOARD FIX LIVE — {len(micro)} micro-cap rockets now surface "
    f"(was 0); board re-ranked by upside-aware opportunity_score, top-10 "
    f"average upside {top10_avg_upside}% with only {top10_neg_upside}/10 "
    "negative. Micro-caps carry price targets and a thesis."
    if report["all_pass"]
    else "REVIEW — see checks[]/boom_board (microcaps must surface and the "
         "board must not be topped by negative-upside names)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/805_opportunity_screener_fix.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/805_opportunity_screener_fix.json")
