"""Phase Y8 — Force-deploy compound v4 with 13 feeds + final celebratory digest."""
import io, json, os, time, base64, zipfile, urllib.request, urllib.error
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-compound-aggregator"

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
    log("source: " + str(len(src)) + " chars")

    section("1) Wait + force-deploy compound v4")
    for _ in range(45):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    
    deployed = False
    for attempt in range(8):
        try:
            L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
            deployed = True
            break
        except L.exceptions.ResourceConflictException:
            time.sleep(15)
    
    if not deployed:
        log("  ❌ deploy failed")
        return
    
    for _ in range(45):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Force-invoke")
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                  LogType="Tail", Payload=b"{}")
    body = json.loads(r["Payload"].read())
    log("  body: " + str(body.get("body", ""))[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-12:]:
            log("    " + ln.rstrip())

    section("3) Inspect compound v4 (13 feeds)")
    cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    log("  feed_stats: " + json.dumps(cs.get("feed_stats", {})))
    log("  stats:      " + json.dumps(cs.get("stats", {})))
    log("")
    log("  ── TOP 20 COMPOUND (13-feed fusion) ──")
    for r in cs.get("compound", [])[:20]:
        sys_str = ",".join(r["systems"])
        log("    {:<6} #{}  comp={:>5.0f}  ({})".format(
            r["symbol"], r["n_systems"], r["compound_score"], sys_str))

    section("4) Build finale digest")
    
    parts = []
    parts.append("🚀 *PUMP\\-DETECTION ARSENAL EXPANDED \\— 4 NEW HUNTERS LIVE*\n")
    parts.append("📅 " + md_escape(time.strftime("%Y-%m-%d %H:%M UTC")) + "\n\n")
    parts.append(md_escape("System now spans 25 Lambdas, 13-feed compound, all caps:") + "\n")
    parts.append(md_escape("nano (248) / micro (313) / small (353) / mid (421) / large (419) / mega (55)") + "\n")
    parts.append(md_escape("Total universe: 1,809 stocks (5.4x larger than before)") + "\n\n")

    parts.append("*🔋 Volatility Squeeze Hunter \\(coiled springs\\):*\n")
    try:
        vs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/volatility-squeeze.json")["Body"].read())
        for r in (vs.get("summary", {}).get("top_25_overall", []) or [])[:5]:
            if r.get("n_signals", 0) >= 4:
                parts.append("  *" + md_escape(r["symbol"]) + "* score\\=" +
                              md_escape(str(int(r["score"]))) + "  " +
                              md_escape(str(r["n_signals"]) + "/6 signals  base=" +
                                        str(r["base_days"]) + "d") + "\n")
    except Exception:
        pass
    parts.append("\n")

    parts.append("*📊 Revenue Acceleration \\(fundamental inflection\\):*\n")
    try:
        ra = json.loads(S3.get_object(Bucket=BUCKET, Key="data/revenue-acceleration.json")["Body"].read())
        for r in (ra.get("summary", {}).get("top_25_overall", []) or [])[:6]:
            if r["score"] >= 65:
                parts.append("  *" + md_escape(r["symbol"]) + "* score\\=" +
                              md_escape(str(int(r["score"]))) +
                              "  growth\\=" + md_escape("{:+.0f}%".format(r["growth"] or 0)) +
                              "  Δ\\=" + md_escape("{:+.1f}pp".format(r["acceleration"] or 0)) + "\n")
    except Exception:
        pass
    parts.append("\n")

    parts.append("*🔥 Microcap Float\\-Squeeze \\(parabolic setups\\):*\n")
    try:
        mc = json.loads(S3.get_object(Bucket=BUCKET, Key="data/microcap-float-squeeze.json")["Body"].read())
        for r in (mc.get("summary", {}).get("top_25_overall", []) or [])[:6]:
            if r["score"] >= 60:
                short_str = "?"
                if r.get("short_pct") is not None:
                    short_str = "{:.0f}%".format(r["short_pct"])
                parts.append("  *" + md_escape(r["symbol"]) + "* score\\=" +
                              md_escape(str(int(r["score"]))) +
                              "  short\\=" + md_escape(short_str) +
                              "  float\\_turn\\=" + md_escape("{:.1f}%".format(r.get("float_turnover") or 0)) + "\n")
    except Exception:
        pass
    parts.append("\n")

    parts.append("*📅 PEAD Detector \\(post\\-earnings drift\\):*\n")
    try:
        pd = json.loads(S3.get_object(Bucket=BUCKET, Key="data/pead-signals.json")["Body"].read())
        for r in (pd.get("summary", {}).get("top_30_overall", []) or [])[:6]:
            if r["score"] >= 88:
                parts.append("  *" + md_escape(r["symbol"]) + "* score\\=" +
                              md_escape(str(int(r["score"]))) +
                              "  streak\\=" + md_escape(str(r["streak"]) + "Q") +
                              "  beat\\=" + md_escape("{:+.0f}%".format(r["avg_beat_pct"] or 0)) + "\n")
    except Exception:
        pass
    parts.append("\n")

    # Top 13-feed compound
    t3plus = [r for r in cs.get("compound", []) if r.get("n_systems", 0) >= 3]
    if t3plus:
        sys_emojis = {
            "nobrainers": "🎯", "insiders": "👀", "smart_money": "💼",
            "deep_value": "💎", "eps_velocity": "📈",
            "momentum": "🚀", "pre_pump": "🌱",
            "options_flow": "📞", "activist": "🏛️",
            "vol_squeeze": "🔋", "rev_accel": "📊",
            "microcap_sq": "🔥", "pead": "📅",
        }
        parts.append("*🏆 13\\-FEED COMPOUND \\(3\\+ systems agree\\):*\n")
        for r in t3plus[:8]:
            sys_str = " ".join(sys_emojis.get(s, "•") for s in r["systems"])
            parts.append("  *" + md_escape(r["symbol"]) + "* " + sys_str +
                          "  comp\\=" + md_escape(str(int(r["compound_score"]))) + "\n")
        parts.append("\n")

    parts.append(md_escape("Coverage: ALL caps from $5M nano to $4.7T mega.") + "\n")
    parts.append(md_escape("Daily auto-update across institutional signal stack.") + "\n")

    text = "".join(parts)
    log("  message: " + str(len(text)) + " chars")

    section("5) Send finale digest")
    try:
        token = SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
        chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
        url = "https://api.telegram.org/bot" + token + "/sendMessage"
        data = json.dumps({
            "chat_id": chat, "text": text,
            "parse_mode": "MarkdownV2", "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=data,
                                       headers={"Content-Type": "application/json"},
                                       method="POST")
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
            log("  " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_y8_compound_v4.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
