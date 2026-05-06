"""Phase X7 — Deploy cross-asset-regime Lambda + smoke test."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-cross-asset-regime"
SCHEDULE_NAME = "justhodl-cross-asset-regime-daily"
SCHEDULE_EXPR = "cron(15 13 * * ? *)"  # 13:15 UTC — after most other ops
ROLE_ARN = "arn:aws:iam::" + ACCOUNT + ":role/lambda-execution-role"

L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=600, connect_timeout=10))
EB = boto3.client("events", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    src = open("aws/lambdas/justhodl-cross-asset-regime/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    section("1) Build zip + create/update Lambda")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log("  zip: " + str(len(zb)) + "b")

    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "TIMEOUT_BUDGET_S": "260",
    }
    exists = False
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        exists = True
        log("  exists — updating")
    except L.exceptions.ResourceNotFoundException:
        log("  creating new")

    if exists:
        L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
        for _ in range(30):
            c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        L.update_function_configuration(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=512, Timeout=300,
            Environment={"Variables": env},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=300, MemorySize=512,
            Environment={"Variables": env},
        )

    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Schedule daily 13:15 UTC")
    rule_arn = EB.put_rule(Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED")["RuleArn"]
    fn_arn = "arn:aws:lambda:" + REGION + ":" + ACCOUNT + ":function:" + LAMBDA_NAME
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(FunctionName=LAMBDA_NAME, StatementId=SCHEDULE_NAME + "-eb",
                          Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                          SourceArn=rule_arn)
        log("  ✓ permission added")
    except L.exceptions.ResourceConflictException:
        log("  ✓ permission exists")

    section("3) Smoke invoke")
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(dur) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + json.dumps(body)[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-12:]:
            log("    " + ln.rstrip())

    section("4) Inspect output — current macro regime")
    obj = S3.get_object(Bucket=BUCKET, Key="data/cross-asset-regime.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))

    log("")
    log("  ── REGIMES (multi-horizon) ──")
    for window in ["regime_5d", "regime_20d", "regime_60d"]:
        ri = d.get(window) or {}
        log("    " + window + ": " + ri.get("regime", "?") + " conf=" + str(ri.get("confidence")) +
             "  risk_score=" + str(ri.get("risk_score")) + " (" + ri.get("risk_label", "?") + ")")
        for r in (ri.get("rationale") or []):
            log("      → " + r)

    log("")
    log("  ── 20D ASSET RETURNS ──")
    rets = ((d.get("regime_20d") or {}).get("lookback_returns_pct") or {})
    for ticker in ["SPY", "TLT", "GLD", "UUP", "HYG", "USO", "BITO", "VIXY"]:
        r = rets.get(ticker)
        log("    {:<6}  {:>+7.2f}%".format(ticker, r if r is not None else 0))

    log("")
    log("  ── TOP 8 CORRELATION BREAKS (30d vs 90d baseline) ──")
    for b in d.get("correlation_breaks", [])[:8]:
        log("    {} <-> {}  c30d={:.2f}  c90d={:.2f}  Δ={:+.2f}  ({})".format(
            b["pair"][0], b["pair"][1],
            b["c30d"], b["c90d"], b["delta"], b["interpretation"]))

    log("")
    log("  ── 30D CORRELATION MATRIX (compact) ──")
    mat = d.get("correlation_matrix_30d", {})
    assets = ["SPY", "TLT", "GLD", "UUP", "HYG", "USO", "BITO", "VIXY"]
    header = "         " + "  ".join("{:<6}".format(a) for a in assets)
    log(header)
    for a in assets:
        row = mat.get(a, {})
        cells = []
        for b in assets:
            v = row.get(b)
            cells.append("{:<6}".format(("{:+.2f}".format(v) if v is not None else "n/a")))
        log("    " + a + "    " + "  ".join(cells))

    log("")
    log("  ── ALERTS ──")
    alerts = d.get("alerts", [])
    if not alerts:
        log("    (none — calm market state)")
    for a in alerts:
        log("    [" + a.get("severity", "?") + "] " + a.get("type", "?") + ": " + a.get("msg", ""))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
        for ln in traceback.format_exc().splitlines():
            log("    " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_x7_cross_asset_regime.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
