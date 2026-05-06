"""
PHASE U — Redeploy theme-rotation v2 (with breadth fallback) +
build the THEME→STOCK cross-reference that surfaces names INSIDE rotating themes.

This is the "institutional convergence" piece:
  - Step 1: get top 10 rotating themes (rotation IN signal)
  - Step 2: for each theme, get its top constituents
  - Step 3: cross-reference against our 7-feed compound
  - Step 4: find names where THEME is rotating IN + STOCK is on compound = highest conviction
"""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-theme-rotation-engine"

L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=600, connect_timeout=10))
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    # Wait for any pending deploy
    log("  Waiting for any in-progress updates...")
    for i in range(60):
        try:
            c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
            if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
                break
        except Exception:
            pass
        time.sleep(2)
    log("  Lambda ready")

    section("1) Force-deploy theme-rotation v2 (with curated holdings fallback)")
    src = open("aws/lambdas/justhodl-theme-rotation-engine/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    markers = [
        "fetch_etf_holdings(ticker, fallback_top=fallback)",
        "curated_lookup",
        "Try newer stable endpoint first",
    ]
    for m in markers:
        log("    " + ("✓" if m in src else "❌") + " " + m[:70])

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Force-invoke")
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

    section("3) Read theme rotation + 7-feed compound")
    tr = json.loads(S3.get_object(Bucket=BUCKET, Key="data/theme-rotation.json")["Body"].read())
    cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    log("  theme-rotation: " + str(len(tr.get("all_themes", []))) + " themes, " +
         str(tr.get("stats", {}).get("n_with_breadth", 0)) + " with breadth")
    log("  compound: " + str(len(cs.get("compound", []))) + " multi-signal names")

    # Build compound lookup
    compound_by_ticker = {r["symbol"]: r for r in cs.get("compound", [])}
    log("  compound symbols: " + ", ".join(list(compound_by_ticker.keys())[:15]))

    section("4) Top 10 themes — show their breadth and constituents")
    breadth_details = tr.get("breadth_details", {})
    top10 = tr.get("summary", {}).get("top_10_momentum", [])
    for t in top10:
        ticker = t["ticker"]
        bd = breadth_details.get(ticker, {})
        breadth = bd.get("breadth", {}) if bd else {}
        constituents = bd.get("constituents_perf", []) if bd else []
        breadth_pct = breadth.get("breadth_outperform_pct", "?") if breadth else "N/A"
        log("")
        log("  ── " + ticker + " (" + t["name"] + ") — momentum=" + str(t["momentum_score"]) +
             "  RS_20d=" + "{:+.1f}%".format(t["rs_20d"]) + "  breadth=" + str(breadth_pct) + "% ──")
        if constituents:
            for c in constituents[:6]:
                sym = c["symbol"]
                ret = c["ret_20d"]
                marker = "🎯" if sym in compound_by_ticker else "  "
                if sym in compound_by_ticker:
                    cinfo = compound_by_ticker[sym]
                    sys_str = ",".join(cinfo.get("systems", []))
                    log("    {} {:<6} ret_20d={:>+5.1f}%  COMPOUND #{} ({}) score={:.0f}".format(
                        marker, sym, ret, cinfo.get("n_systems"), sys_str, cinfo.get("compound_score")))
                else:
                    log("    {} {:<6} ret_20d={:>+5.1f}%".format(marker, sym, ret))

    section("5) THE INSTITUTIONAL CONVERGENCE — names inside rotating themes WITH compound signal")
    log("  This is the highest-conviction setup the system can produce:")
    log("  Theme is rotating IN (institutions are buying the basket)")
    log("  AND the specific name appears on 2+ of our hunter systems")
    log("")

    convergent_picks = []
    for t in tr.get("summary", {}).get("top_10_momentum", []):
        ticker = t["ticker"]
        bd = breadth_details.get(ticker, {})
        constituents = bd.get("constituents_perf", []) if bd else []
        for c in constituents:
            sym = c["symbol"]
            if sym in compound_by_ticker:
                cinfo = compound_by_ticker[sym]
                convergent_picks.append({
                    "symbol": sym,
                    "theme_etf": ticker,
                    "theme_name": t["name"],
                    "theme_category": t["category"],
                    "theme_momentum": t["momentum_score"],
                    "theme_rs_20d": t["rs_20d"],
                    "ret_20d_in_theme": c["ret_20d"],
                    "compound_n_systems": cinfo.get("n_systems"),
                    "compound_score": cinfo.get("compound_score"),
                    "compound_systems": cinfo.get("systems"),
                })

    # Dedupe by symbol — keep highest theme momentum
    by_sym = {}
    for p in convergent_picks:
        if p["symbol"] not in by_sym or by_sym[p["symbol"]]["theme_momentum"] < p["theme_momentum"]:
            by_sym[p["symbol"]] = p
    final_picks = sorted(by_sym.values(),
                          key=lambda x: -(x["compound_score"] + x["theme_momentum"]))

    log("  Found " + str(len(final_picks)) + " institutional-convergence picks:")
    log("")
    for p in final_picks[:15]:
        sys_str = ",".join(p["compound_systems"])
        log("  {:<6} theme={:<6} momentum={}  RS={:+.1f}%  ret_in_theme={:+.1f}%".format(
            p["symbol"], p["theme_etf"], p["theme_momentum"],
            p["theme_rs_20d"], p["ret_20d_in_theme"]))
        log("         compound: #{}  score={:.0f}  ({})".format(
            p["compound_n_systems"], p["compound_score"], sys_str))

    section("6) Save institutional-convergence to S3")
    out = {
        "schema_version": 1,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "n_convergent": len(final_picks),
        "convergence": final_picks,
        "method": "theme_rotation × 7_feed_compound v1",
    }
    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key="data/institutional-convergence.json",
                   Body=body, ContentType="application/json")
    log("  ✓ wrote " + str(len(body)) + "b to data/institutional-convergence.json")


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
    with open(os.path.join(out, "phase_u_theme_x_compound.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
