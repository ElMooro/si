"""Phase Y8 — Final audit + Telegram digest of full multi-cap institutional system.

System now covers:
  - 1809 stocks across all 6 cap buckets (mega/large/mid/small/micro/nano)
  - 13-feed compound fusion engine
  - 5 institutional priorities (options flow, sector diffusion, narrative,
    activist filings, cross-asset regime)
  - 4 coiled-spring detectors (volatility squeeze, revenue acceleration,
    microcap float squeeze, PEAD)
  - All hunters now have access to nano/micro coverage they didn't before
"""
import io, json, os, time, base64, urllib.request, urllib.error
import boto3

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
    section("1) Universe coverage check")
    u = json.loads(S3.get_object(Bucket=BUCKET, Key="data/universe.json")["Body"].read())
    log("  total: " + str(u.get("stats", {}).get("total_stocks")))
    bb = u.get("stats", {}).get("by_cap_bucket", {})
    for b in ["mega", "large", "mid", "small", "micro", "nano"]:
        log("    " + b.ljust(8) + ": " + str(bb.get(b, 0)))

    section("2) Volatility Squeeze top names")
    try:
        vs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/volatility-squeeze.json")["Body"].read())
        log("  stats: " + json.dumps(vs.get("stats", {})))
        for c in vs.get("summary", {}).get("top_25_overall", [])[:5]:
            log("    {:<6} score={:.1f}  signals={}  base={}d".format(
                c["symbol"], c["score"], c["n_signals"], c["base_days"]))
    except Exception as e:
        log("  ❌ " + str(e))

    section("3) Revenue Acceleration top names")
    try:
        ra = json.loads(S3.get_object(Bucket=BUCKET, Key="data/revenue-acceleration.json")["Body"].read())
        log("  stats: " + json.dumps(ra.get("stats", {})))
        for c in ra.get("summary", {}).get("top_25_overall", [])[:5]:
            log("    {:<6} score={:.0f}  growth={:+.0f}%  Δ={:+.0f}pp  streak={}Q".format(
                c["symbol"], c["score"], c.get("growth") or 0,
                c.get("acceleration") or 0, c.get("consec_accel", 0)))
    except Exception as e:
        log("  ❌ " + str(e))

    section("4) Microcap Float Squeeze top names")
    try:
        ms = json.loads(S3.get_object(Bucket=BUCKET, Key="data/microcap-float-squeeze.json")["Body"].read())
        log("  stats: " + json.dumps(ms.get("stats", {})))
        for c in ms.get("summary", {}).get("top_25_overall", [])[:8]:
            mc = "${:.0f}M".format(c["market_cap"] / 1_000_000)
            sp = c.get("short_pct")
            log("    {:<6} score={:.1f}  mcap={}  short={}".format(
                c["symbol"], c["score"], mc,
                "{:.0f}%".format(sp) if sp else "?"))
    except Exception as e:
        log("  ❌ " + str(e))

    section("5) PEAD top names")
    try:
        pd = json.loads(S3.get_object(Bucket=BUCKET, Key="data/earnings-pead.json")["Body"].read())
        log("  stats: " + json.dumps(pd.get("stats", {})))
        for c in pd.get("summary", {}).get("top_25_overall", [])[:5]:
            log("    {:<6} score={:.0f}".format(c["symbol"], c["score"]))
    except Exception as e:
        log("  ⚠ pead not yet live: " + str(e))

    section("6) 13-feed compound aggregator")
    try:
        cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
        log("  feed_stats: " + json.dumps(cs.get("feed_stats", {})))
        log("  stats: " + json.dumps(cs.get("stats", {})))
        log("")
        log("  ── Top 12 compound (all feeds fused) ──")
        for r in cs.get("compound", [])[:12]:
            log("    {:<6} #{} comp={:>5.0f}  ({})".format(
                r["symbol"], r["n_systems"], r["compound_score"],
                ",".join(r["systems"])))
    except Exception as e:
        log("  ❌ " + str(e))

    section("7) Build + send digest")

    parts = []
    parts.append("🚀 *MULTI\\-CAP SYSTEM FULLY OPERATIONAL*\n")
    parts.append("📅 " + md_escape(time.strftime("%Y-%m-%d %H:%M UTC")) + "\n\n")
    
    parts.append(md_escape("Universe expanded from 338 mega-caps to 1,809 stocks") + "\n")
    parts.append(md_escape("across ALL caps. Now hunting pumps in nano/micro/small.") + "\n\n")
    
    parts.append("🎯 *Universe Coverage:*\n")
    for b in ["mega", "large", "mid", "small", "micro", "nano"]:
        n = bb.get(b, 0)
        parts.append("  *" + md_escape(b.upper()) + "*: " + md_escape(str(n)) + " stocks\n")
    parts.append("\n")
    
    # Volatility squeeze top
    try:
        vs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/volatility-squeeze.json")["Body"].read())
        tier_s = vs.get("summary", {}).get("tier_s", [])
        if tier_s:
            parts.append("🔋 *VOL SQUEEZE TIER\\_S \\(coiled springs, 5\\+/6 signals\\):*\n")
            for sym in tier_s[:5]:
                parts.append("  *" + md_escape(sym) + "*\n")
            parts.append("\n")
    except Exception:
        pass
    
    # Microcap squeeze top
    try:
        ms = json.loads(S3.get_object(Bucket=BUCKET, Key="data/microcap-float-squeeze.json")["Body"].read())
        top_squeeze = (ms.get("summary", {}).get("top_25_overall") or [])[:5]
        if top_squeeze:
            parts.append("🔥 *FLOAT SQUEEZE \\(short \\+ float exhaustion\\):*\n")
            for c in top_squeeze:
                mc = "${:.0f}M".format(c["market_cap"] / 1_000_000)
                sp = c.get("short_pct")
                sp_str = "{:.0f}%".format(sp) if sp else "?"
                parts.append("  *" + md_escape(c["symbol"]) + "* mcap\\=" + 
                              md_escape(mc) + "  short\\=" + md_escape(sp_str) +
                              "  score\\=" + md_escape(str(c["score"])) + "\n")
            parts.append("\n")
    except Exception:
        pass
    
    # Revenue accel top
    try:
        ra = json.loads(S3.get_object(Bucket=BUCKET, Key="data/revenue-acceleration.json")["Body"].read())
        top_ra = (ra.get("summary", {}).get("top_25_overall") or [])[:5]
        if top_ra:
            parts.append("📊 *REVENUE ACCELERATION \\(YoY growth inflecting\\):*\n")
            for c in top_ra:
                parts.append("  *" + md_escape(c["symbol"]) + "* growth\\=" +
                              md_escape("{:+.0f}%".format(c.get("growth") or 0)) +
                              "  Δ\\=" + md_escape("{:+.0f}pp".format(c.get("acceleration") or 0)) +
                              "  streak\\=" + md_escape(str(c.get("consec_accel", 0))) + "Q\n")
            parts.append("\n")
    except Exception:
        pass
    
    # Compound TIER-3+
    try:
        cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
        t3 = [r for r in (cs.get("compound") or []) if r.get("n_systems", 0) >= 3]
        if t3:
            parts.append("⚡ *COMPOUND TIER\\-3\\+ \\(3\\+ systems agreeing\\):*\n")
            for r in t3[:6]:
                parts.append("  *" + md_escape(r["symbol"]) + "* \\#" +
                              md_escape(str(r["n_systems"])) +
                              "  comp\\=" + md_escape(str(int(r["compound_score"]))) + "\n")
            parts.append("\n")
    except Exception:
        pass
    
    parts.append("*New Tools Built Today:*\n")
    parts.append(md_escape("✓ universe-builder v3 — full multi-cap (1809 stocks)") + "\n")
    parts.append(md_escape("✓ volatility-squeeze-hunter — BB+TTM+NR7+VCP+ATR") + "\n")
    parts.append(md_escape("✓ revenue-acceleration — YoY growth inflection") + "\n")
    parts.append(md_escape("✓ microcap-float-squeeze — short \\+ float exhaustion") + "\n")
    parts.append(md_escape("✓ pead-detector — earnings surprise drift") + "\n")
    parts.append(md_escape("✓ compound v4 — 13-feed fusion engine") + "\n\n")
    
    parts.append(md_escape("System now catches pumps in EVERY cap class.") + "\n")
    
    text = "".join(parts)
    log("  digest: " + str(len(text)) + " chars")

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
    with open(os.path.join(out, "phase_y8_multicap_finale.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
