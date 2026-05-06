"""
PHASE V — Wire theme-rotation as the 8th signal feed into compound aggregator,
plus build an institutional-convergence Telegram alert.

Why: a name showing up on 2+ hunter systems AND inside a top-rotating theme
is the highest-conviction pattern. The compound aggregator should now also
track "is this name's parent theme rotating IN" as its own signal.
"""
import io, json, os, time, base64, zipfile
import boto3
import urllib.request, urllib.error

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
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
    section("1) Read theme-rotation + institutional-convergence")
    tr = json.loads(S3.get_object(Bucket=BUCKET, Key="data/theme-rotation.json")["Body"].read())
    cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    ic = json.loads(S3.get_object(Bucket=BUCKET, Key="data/institutional-convergence.json")["Body"].read())

    log("  themes ranked: " + str(len(tr.get("all_themes", []))))
    log("  compound signals: " + str(len(cs.get("compound", []))))
    log("  institutional convergence: " + str(ic.get("n_convergent")))

    section("2) Compose institutional-convergence Telegram digest")
    sys_emojis = {
        "nobrainers": "🎯", "insiders": "👀", "smart_money": "💼",
        "deep_value": "💎", "eps_velocity": "📈",
        "momentum": "🚀", "pre_pump": "🌱",
    }

    parts = []
    parts.append("🏛️ *INSTITUTIONAL MONEY FLOW \\+ CONVERGENT BUY SIGNALS*\n")
    parts.append("📅 " + md_escape(time.strftime("%Y-%m-%d %H:%M UTC")) + "\n")
    parts.append("\n")
    parts.append(md_escape("New: theme-rotation engine tracks 118 ETFs across all sectors,") + "\n")
    parts.append(md_escape("calculates relative strength + breadth of constituents, identifies") + "\n")
    parts.append(md_escape("themes where institutional money is flowing.") + "\n")
    parts.append("\n")

    # SECTION 1: top rotating themes
    top = tr.get("summary", {}).get("top_10_momentum", [])[:5]
    parts.append("🟢 *THEMES WHERE MONEY IS FLOWING IN:*\n")
    for t in top:
        ticker = md_escape(t["ticker"])
        name = md_escape(t["name"])
        cat = md_escape(t.get("category", ""))
        rs60 = md_escape("{:+.0f}".format(t["rs_60d"]))
        rs20 = md_escape("{:+.0f}".format(t["rs_20d"]))
        breadth = t.get("breadth_pct")
        breadth_str = ""
        if breadth is not None:
            breadth_str = "  breadth\\=" + md_escape("{:.0f}".format(breadth)) + "%"
        parts.append("*" + ticker + "* " + md_escape("(" + t["name"] + ")") + "\n")
        parts.append(md_escape("  RS_60d=") + rs60 + md_escape("%, RS_20d=") + rs20 + "%" + breadth_str + "\n")
    parts.append("\n")

    # SECTION 2: bottom rotating themes
    bottom = tr.get("summary", {}).get("bottom_10_momentum", [])[-5:]
    parts.append("🔴 *THEMES WHERE MONEY IS FLOWING OUT:*\n")
    for t in bottom:
        ticker = md_escape(t["ticker"])
        rs60 = md_escape("{:+.0f}".format(t["rs_60d"]))
        rs20 = md_escape("{:+.0f}".format(t["rs_20d"]))
        cat = md_escape(t.get("category", ""))
        parts.append("*" + ticker + "* " + md_escape("(" + cat + ")") + "  ")
        parts.append("RS_60d\\=" + rs60 + "%, RS_20d\\=" + rs20 + "%\n")
    parts.append("\n")

    # SECTION 3: institutional convergence — names inside rotating themes
    convergent = ic.get("convergence", [])
    if convergent:
        parts.append("⭐ *INSTITUTIONAL CONVERGENCE \\(theme \\+ stock\\):*\n")
        parts.append(md_escape("Names where the parent theme is rotating IN") + "\n")
        parts.append(md_escape("AND the stock appears on 2+ of our hunter systems") + "\n")
        parts.append("\n")
        for p in convergent[:6]:
            sym = md_escape(p["symbol"])
            theme_etf = md_escape(p["theme_etf"])
            theme_mom = md_escape(str(int(p["theme_momentum"])))
            comp_score = md_escape(str(int(p["compound_score"])))
            sys_str = " ".join(sys_emojis.get(s, "•") for s in p.get("compound_systems", []))
            sys_names = md_escape(",".join(p.get("compound_systems", [])))
            parts.append("*" + sym + "* in " + theme_etf + " " + sys_str + "\n")
            parts.append(md_escape("  theme_momentum=") + theme_mom + md_escape(", compound=") + comp_score + "\n")
            parts.append("  _" + sys_names + "_\n")
        parts.append("\n")

    # SECTION 4: TIER-3 names from compound for completeness
    t3 = [r for r in cs.get("compound", []) if r.get("n_systems", 0) >= 3]
    if t3:
        parts.append("🔥 *TIER\\-3 COMPOUND \\(3\\+ hunter systems agree\\):*\n")
        for r in t3:
            sym = md_escape(r.get("symbol", "?"))
            comp = md_escape(str(int(r.get("compound_score", 0))))
            sys_str = " ".join(sys_emojis.get(s, "•") for s in r.get("systems", []))
            parts.append("*" + sym + "* " + sys_str + "  comp\\=" + comp + "\n")
        parts.append("\n")

    parts.append(md_escape("Today's exponential institutional-grade additions:") + "\n")
    parts.append(md_escape("✓ theme-rotation-engine Lambda — 118 ETFs tracked daily") + "\n")
    parts.append(md_escape("✓ Breadth calc — % of constituents beating SPY per theme") + "\n")
    parts.append(md_escape("✓ RS rotation deltas — alerts on rank shifts") + "\n")
    parts.append(md_escape("✓ Theme×compound cross-reference") + "\n")
    parts.append(md_escape("✓ Convergent breadth alerts (RS up + breadth > 60%)") + "\n")
    parts.append("\n")
    parts.append("[Compound page](https://justhodl.ai/compound-signals.html)\n")

    text = "".join(parts)
    log("  message: " + str(len(text)) + " chars")
    log("  preview:")
    for ln in text.splitlines()[:12]:
        log("    " + ln[:120])

    section("3) Send")
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
        body = e.read().decode("utf-8", "replace")[:400]
        log("  ❌ HTTP " + str(e.code) + ": " + body)
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
    with open(os.path.join(out, "phase_v_institutional_alert.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
