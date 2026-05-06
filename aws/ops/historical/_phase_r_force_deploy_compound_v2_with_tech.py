"""
PHASE R — Force-deploy compound aggregator v2 with momentum + pre_pump,
re-aggregate, and inspect cross-system intersections.
"""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S")
    print("- `" + ts + "`   " + m)
    REPORT.append("- `" + ts + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    section("1) Force-deploy compound aggregator with momentum + pre_pump feeds")
    src = open("aws/lambdas/justhodl-compound-aggregator/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    markers = [
        '"momentum":       ("data/momentum-breakout.json"',
        '"pre_pump":       ("data/pre-pump-signals.json"',
        'elif name == "momentum":',
        'elif name == "pre_pump":',
        '"momentum": "🚀"',
    ]
    for m in markers:
        log("    " + ("✓" if m in src else "❌") + " " + m[:60])

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    L.update_function_code(FunctionName="justhodl-compound-aggregator", ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName="justhodl-compound-aggregator")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Force-invoke compound — should now see 7 feeds + new intersections")
    r = L.invoke(FunctionName="justhodl-compound-aggregator",
                  InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    log("  status: " + str(r["StatusCode"]))
    body = json.loads(r["Payload"].read())
    log("  body: " + body.get("body", "")[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-12:]:
            log("    " + ln.rstrip())

    section("3) Updated compound state — full leaderboard")
    cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    log("  generated_at: " + str(cs.get("generated_at")))
    log("  feed_stats:   " + json.dumps(cs.get("feed_stats", {})))
    log("  stats:        " + json.dumps(cs.get("stats", {})))
    log("")
    log("  ── compound leaderboard (top 15) ──")
    for r in cs.get("compound", [])[:15]:
        sys_str = ",".join(r["systems"])
        log("    {:<6} #{}  comp={:>7.1f}  ({})".format(
            r["symbol"], r["n_systems"], r["compound_score"], sys_str))

    section("4) NEW: Cross-system convergence between FUNDAMENTALS and TECHNICALS")
    log("  Names appearing on BOTH a fundamental hunter (NB/insider/SM/DV/EPS)")
    log("  AND a technical hunter (momentum or pre_pump) are the highest-conviction")
    log("  setups — confirmed by data + price action both.")
    log("")
    fund = {"nobrainers", "insiders", "smart_money", "deep_value", "eps_velocity"}
    tech = {"momentum", "pre_pump"}
    convergent = []
    for r in cs.get("compound", []):
        sys_set = set(r["systems"])
        has_fund = bool(sys_set & fund)
        has_tech = bool(sys_set & tech)
        if has_fund and has_tech:
            convergent.append(r)
    log("  Found " + str(len(convergent)) + " cross-domain convergent setups:")
    for r in convergent[:12]:
        sys_str = ",".join(r["systems"])
        log("    {:<6} #{}  comp={:>7.1f}  ({})".format(
            r["symbol"], r["n_systems"], r["compound_score"], sys_str))

    section("5) Pure technical setups (momentum + pre_pump only — early signals)")
    pure_tech = []
    for r in cs.get("compound", []):
        sys_set = set(r["systems"])
        if sys_set <= tech and len(sys_set) >= 2:
            pure_tech.append(r)
    if pure_tech:
        log("  " + str(len(pure_tech)) + " names appear on BOTH momentum + pre_pump")
        for r in pure_tech[:8]:
            log("    " + r["symbol"] + "  comp=" + "{:.1f}".format(r["compound_score"]))
    else:
        log("  no pure-technical convergence yet")


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
    with open(os.path.join(out, "phase_r_compound_v2_tech.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
