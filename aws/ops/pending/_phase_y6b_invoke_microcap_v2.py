"""Phase Y6b — Re-invoke microcap-float-squeeze after the v2 patches applied."""
import io, json, os, time, base64
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-microcap-float-squeeze"

L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=900, connect_timeout=10))
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    section("1) Wait for v2 deploy to complete")
    for _ in range(45):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    log("  ✓ ready, last modified " + str(c["LastModified"]))

    section("2) Smoke invoke")
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(dur) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + json.dumps(body)[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-15:]:
            log("    " + ln.rstrip())

    section("3) Inspect output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/microcap-float-squeeze.json")
    d = json.loads(obj["Body"].read())
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TIER_S PARABOLIC ──")
    for sym in d.get("summary", {}).get("tier_s", []):
        log("    " + sym)
    log("")
    log("  ── TOP 20 OVERALL ──")
    for c in d.get("summary", {}).get("top_25_overall", [])[:20]:
        mc = "${:.0f}M".format(c["market_cap"] / 1_000_000)
        ft = "{:.1f}%".format(c.get("float_turnover") or 0)
        sp = "{:.0f}%".format(c["short_pct"]) if c.get("short_pct") is not None else "?"
        d2c = "{:.1f}d".format(c["days_to_cover"]) if c.get("days_to_cover") is not None else "?"
        sch = "{:+.1f}".format(c["short_change"]) if c.get("short_change") is not None else "?"
        log("    {:<6} score={:>5.1f}  mcap={}  float_turn={}  short={}  d2c={}  Δshort={}".format(
            c["symbol"], c["score"], mc, ft, sp, d2c, sch))
        log("      flags: " + ",".join(c.get("flags") or []))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_y6b_microcap_invoke.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
