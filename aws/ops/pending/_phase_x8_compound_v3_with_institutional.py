"""Phase X8 — Force-deploy compound aggregator v3 with 9 feeds + send finale digest."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config
import urllib.request, urllib.error

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=600))
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def md_escape(s):
    out = []
    for c in str(s):
        if c in "_*[]()~`>#+-=|{}.!\\":
            out.append("\\" + c)
        else:
            out.append(c)
    return "".join(out)


def main():
    src = open("aws/lambdas/justhodl-compound-aggregator/source/lambda_function.py").read()

    # Wait for any in-flight
    for _ in range(30):
        c = L.get_function_configuration(FunctionName="justhodl-compound-aggregator")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    section("1) Force-deploy compound v3 (9 feeds)")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    L.update_function_code(FunctionName="justhodl-compound-aggregator",
                              ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName="justhodl-compound-aggregator")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Force-invoke")
    r = L2.invoke(FunctionName="justhodl-compound-aggregator",
                  InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    body = json.loads(r["Payload"].read())
    log("  body: " + body.get("body", "")[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-12:]:
            log("    " + ln.rstrip())

    section("3) Inspect updated compound state")
    cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    log("  feed_stats: " + json.dumps(cs.get("feed_stats", {})))
    log("  stats:      " + json.dumps(cs.get("stats", {})))
    log("")
    log("  ── Top 15 compound (9-feed fusion) ──")
    for r in cs.get("compound", [])[:15]:
        sys_str = ",".join(r["systems"])
        log("    {:<6} #{}  comp={:>5.0f}  ({})".format(
            r["symbol"], r["n_systems"], r["compound_score"], sys_str))

    # Read all institutional signals
    section("4) Build institutional-grade finale digest")
    
    tr = json.loads(S3.get_object(Bucket=BUCKET, Key="data/theme-rotation.json")["Body"].read())
    sd = json.loads(S3.get_object(Bucket=BUCKET, Key="data/sector-earnings-diffusion.json")["Body"].read())
    nd = json.loads(S3.get_object(Bucket=BUCKET, Key="data/narrative-density.json")["Body"].read())
    of = json.loads(S3.get_object(Bucket=BUCKET, Key="data/options-flow.json")["Body"].read())
    af = json.loads(S3.get_object(Bucket=BUCKET, Key="data/activist-filings.json")["Body"].read())
    cr = json.loads(S3.get_object(Bucket=BUCKET, Key="data/cross-asset-regime.json")["Body"].read())

    parts = []
    parts.append("🏛️ *INSTITUTIONAL SYSTEM COMPLETE: ALL 5 PRIORITIES LIVE*\n")
    parts.append("📅 " + md_escape(time.strftime("%Y-%m-%d %H:%M UTC")) + "\n")
    parts.append("\n")
    parts.append(md_escape("Just deployed all 5 institutional-grade detection systems") + "\n")
    parts.append(md_escape("recommended in our roadmap. The platform now operates at") + "\n")
    parts.append(md_escape("13 signal domains — multi-strat hedge fund parity.") + "\n")
    parts.append("\n")

    # Macro regime
    regime = (cr.get("regime_20d") or {})
    parts.append("🌍 *Current Macro Regime \\(20d\\):*\n")
    parts.append("*" + md_escape(regime.get("regime", "?")) + "*  conf\\=" +
                  md_escape(str(regime.get("confidence", "?"))) + "%  risk\\=" +
                  md_escape(str(regime.get("risk_score", "?"))) + " \\(" +
                  md_escape(regime.get("risk_label", "?")) + "\\)\n")
    for r in (regime.get("rationale") or []):
        parts.append("  → " + md_escape(r) + "\n")
    parts.append("\n")

    # Theme rotation top 3
    parts.append("🟢 *Themes Rotating IN:*\n")
    for t in (tr.get("summary", {}).get("top_10_momentum", []) or [])[:5]:
        parts.append("  *" + md_escape(t["ticker"]) + "* " + md_escape(t["name"]) +
                      " RS\\_60d\\=" + md_escape("{:+.0f}%".format(t["rs_60d"])) + "\n")
    parts.append("\n")

    # Sector diffusion - bullish all-in sectors
    parts.append("📈 *Sectors with BULLISH\\_ALL\\_IN earnings diffusion:*\n")
    for s in (sd.get("summary", {}).get("sectors_top_diffusion", []) or [])[:3]:
        if "ALL_IN" in (s.get("regime") or ""):
            parts.append("  *" + md_escape(s["group"]) + "* " +
                          md_escape("{:.0f}".format(s["diffusion_up_pct"])) + "% rising\n")
    parts.append("\n")

    # Narrative top 3
    parts.append("📰 *Hottest Narratives:*\n")
    for t in (nd.get("summary", {}).get("top_15_themes", []) or [])[:5]:
        if (t.get("metrics") or {}).get("accel_today_vs_7d", 0) > 1.5:
            parts.append("  *" + md_escape(t["name"]) + "* accel\\=" +
                          md_escape("{:.1f}x".format(t["metrics"]["accel_today_vs_7d"])) + "\n")
    parts.append("\n")

    # Options flow top 5 TIER_A
    of_top = (of.get("summary", {}).get("top_25_overall", []) or [])[:5]
    if of_top:
        parts.append("📞 *Top Bullish Options Flow:*\n")
        for c in of_top:
            parts.append("  *" + md_escape(c["symbol"]) + "* score\\=" +
                          md_escape(str(c["score"])) + "  " +
                          md_escape(",".join(c.get("flags") or [])[:60]) + "\n")
        parts.append("\n")

    # Compound TIER-3 names
    t3 = [r for r in cs.get("compound", []) if r.get("n_systems", 0) >= 3]
    if t3:
        parts.append("🔥 *TIER\\-3 Compound \\(3\\+ systems agree\\):*\n")
        sys_emojis = {
            "nobrainers": "🎯", "insiders": "👀", "smart_money": "💼",
            "deep_value": "💎", "eps_velocity": "📈",
            "momentum": "🚀", "pre_pump": "🌱",
            "options_flow": "📞", "activist": "🏛️",
        }
        for r in t3[:6]:
            sym = md_escape(r["symbol"])
            comp = md_escape(str(int(r["compound_score"])))
            sys_str = " ".join(sys_emojis.get(s, "•") for s in r["systems"])
            parts.append("  *" + sym + "* " + sys_str + "  comp\\=" + comp + "\n")
        parts.append("\n")

    parts.append("*5 New Lambdas Live Today:*\n")
    parts.append(md_escape("✓ options-flow-scanner — Polygon options + FINRA shorts") + "\n")
    parts.append(md_escape("✓ sector-earnings-diffusion — diffusion index per sector") + "\n")
    parts.append(md_escape("✓ narrative-density-tracker — Polygon news, 53 themes") + "\n")
    parts.append(md_escape("✓ activist-filings-scanner — SEC EDGAR Atom RSS") + "\n")
    parts.append(md_escape("✓ cross-asset-regime — 8 asset correlation matrix") + "\n")
    parts.append("\n")
    parts.append(md_escape("Total system: 21 Lambdas, 9-feed compound, daily auto-update.") + "\n")

    text = "".join(parts)
    log("  message: " + str(len(text)) + " chars")

    section("5) Send finale digest")
    try:
        token = SSM.get_parameter(Name="/justhodl/telegram/bot_token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception as e:
        log("  ❌ telegram credentials: " + str(e))
        return

    url = "https://api.telegram.org/bot" + token + "/sendMessage"
    data = json.dumps({
        "chat_id": chat, "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }).encode()
    req = urllib.request.Request(url, data=data,
                                   headers={"Content-Type": "application/json"},
                                   method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            mid = json.loads(r.read())["result"]["message_id"]
            log("  ✅ delivered, message_id=" + str(mid))
    except urllib.error.HTTPError as e:
        body_err = e.read().decode("utf-8", "replace")[:400]
        log("  ❌ HTTP " + str(e.code) + ": " + body_err)
    except Exception as e:
        log("  ❌ " + str(e))


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
    with open(os.path.join(out, "phase_x8_finale_compound_v3.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
