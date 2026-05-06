"""
PHASE B2 — Force-deploy the patched deep-value Lambda + smart-money schedule fix.

Why needed:
  Auto-commits add [skip-deploy] tag, so changes to source via ops scripts
  don't auto-deploy. Need to deploy via boto directly.

Plus: fix smart-money schedule (currently same time as deep-value at 9 UTC).
"""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
EB = boto3.client("events", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Fix smart-money schedule (currently 09:00 UTC, conflicts with deep-value)")
    SCHEDULE_NAME = "justhodl-smart-money-cluster-daily"
    SCHEDULE_EXPR = "cron(0 16 * * ? *)"  # 4 PM UTC, after market data settles
    rule_arn = EB.put_rule(
        Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED",
        Description="justhodl-smart-money-cluster — 16:00 UTC daily (post-close)"
    )["RuleArn"]
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:justhodl-smart-money-cluster"
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    log(f"  ✓ smart-money schedule updated to {SCHEDULE_EXPR}")

    section("2) Force-deploy deep-value Lambda from current source")
    src = open("aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py", "r").read()
    log(f"  source: {len(src)} chars")
    # Verify our patch markers
    markers = [
        ("MARGINAL", "MARGINAL flag"),
        ("fin_company_keywords", "company-name fin detection"),
        ("net_cash_pct < 0.15", "lowered threshold"),
    ]
    for m, desc in markers:
        ok = m in src
        log(f"    {'✓' if ok else '❌'} {desc}: '{m}'")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    L.update_function_code(FunctionName="justhodl-deep-value-screener", ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName="justhodl-deep-value-screener")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ deployed at {c['LastModified']}")

    section("3) Re-invoke deep-value")
    t0 = time.time()
    r = L.invoke(FunctionName="justhodl-deep-value-screener",
                  InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}, dur: {time.time()-t0:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {body.get('body','')[:200]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-10:]:
            log(f"    {ln.rstrip()}")

    section("4) Inspect new top_25 (should NOT have BAC/WFC, should be larger)")
    obj = S3.get_object(Bucket=BUCKET, Key="data/deep-value.json")
    d = json.loads(obj["Body"].read())
    top = d.get("summary", {}).get("top_25_overall", [])
    excluded = d.get("summary", {}).get("top_25_excluded_financials", [])
    log(f"  top_25_overall: {len(top)}")
    log(f"  top_25_excluded: {len(excluded)}")
    log("")
    log("  ── new top_25 ──")
    for c in top[:15]:
        log(f"    {c.get('symbol'):<6} {c.get('score'):>6.1f}  flag={c.get('flag','')[:24]:<24}  sector={c.get('sector','')[:25]}")
    log("")
    log("  ── excluded leaders ──")
    for c in excluded[:8]:
        log(f"    {c.get('symbol'):<6} {c.get('score'):>6.1f}  flag={c.get('flag','')[:26]:<26}  sector={c.get('sector','')[:25]}")

    section("5) Trigger compound-aggregator Lambda (will re-aggregate with new DV)")
    r = L.invoke(FunctionName="justhodl-compound-aggregator",
                  InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    body = json.loads(r["Payload"].read())
    log(f"  status: {r['StatusCode']}, body: {body.get('body','')[:200]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-8:]:
            log(f"    {ln.rstrip()}")

    section("6) Verify new compound output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")
    d = json.loads(obj["Body"].read())
    log(f"  feed_stats: {json.dumps(d.get('feed_stats', {}))}")
    log(f"  stats: {json.dumps(d.get('stats', {}))}")
    log("")
    log("  ── compound leaderboard (top 10) ──")
    for r in d.get("compound", [])[:10]:
        log(f"    {r['symbol']:<6} #{r['n_systems']}  comp={r['compound_score']:>7.1f}  ({','.join(r['systems'])})")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_b2_force_deploy_dv.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
