"""
Send insider-cluster top-5 digest to Telegram.
Reads data/insider-clusters.json and posts a structured MarkdownV2 message.
"""
import json, os, time, urllib.request, urllib.error
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def get_telegram():
    token = SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
    chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    return token, chat


def md_escape(s):
    out = []
    for c in str(s):
        if c in "_*[]()~`>#+-=|{}.!\\":
            out.append("\\" + c)
        else:
            out.append(c)
    return "".join(out)


def send(token, chat, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({
        "chat_id": chat,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }).encode()
    req = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"},
                                  method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            r = json.loads(resp.read())
            return True, r.get("result", {}).get("message_id")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        return False, f"HTTP {e.code}: {body[:300]}"
    except Exception as e:
        return False, str(e)


def signal_emoji(sig):
    return {
        "executive_cluster": "👥",
        "smart_money_dual": "🎯",
        "ceo_conviction": "🦅",
        "cluster_buy": "📊",
        "lone_buy": "🟢",
    }.get(sig, "•")


def main():
    section("1) Load top clusters")
    obj = S3.get_object(Bucket=BUCKET, Key="data/insider-clusters.json")
    data = json.loads(obj["Body"].read())
    clusters = data.get("clusters", [])
    stats = data.get("stats", {})
    log(f"  loaded {len(clusters)} clusters, generated_at={data.get('generated_at')}")
    log(f"  stats: {json.dumps(stats)}")

    section("2) Build digest")
    top = clusters[:8]
    lines = []
    lines.append("👀 *INSIDER CLUSTER DIGEST*")
    lines.append(f"📅 {time.strftime('%Y\\-%m\\-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"📊 {stats.get('n_form4_filings_scanned',0)} filings scanned \\| {stats.get('n_buy_transactions',0)} real buys \\| {stats.get('n_clusters',0)} clusters \\| *{stats.get('n_strong_signals',0)} strong signals*")
    lines.append("")
    lines.append("*Top setups* \\(score ≥ 70\\):")
    lines.append("")

    for c in top:
        score = c.get("score", 0) or 0
        if score < 50:
            continue
        f = c.get("fundamentals") or {}
        sym = c.get("ticker", "?")
        company = (c.get("company") or "")[:30]
        sig = c.get("signal_type", "")
        emoji = signal_emoji(sig)
        n_ins = c.get("n_insiders", 0)
        v = c.get("total_value", 0) or 0
        ph = f.get("pct_from_52w_high")
        mcap = f.get("market_cap") or 0
        ms = f"${mcap/1e9:.1f}B" if mcap >= 1e9 else f"${mcap/1e6:.0f}M" if mcap else "?"
        sec = (f.get("sector") or "")[:18]
        ceo = "🦅 " if c.get("has_ceo") else ""
        cfo = "💰 " if c.get("has_cfo") else ""

        lines.append(f"{emoji} *{md_escape(sym)}* — score *{md_escape(f'{score:.1f}')}*")
        lines.append(f"  {ceo}{cfo}{md_escape(company)} \\({md_escape(sec)}\\)")
        lines.append(f"  {n_ins} insiders bought ${md_escape(f'{v/1e6:.2f}')}M")
        if ph is not None:
            ph_str = md_escape(f"{ph:+.0f}%")
            lines.append(f"  Mcap {md_escape(ms)} \\| {ph_str} from 52W high")
        rat = c.get("rationale", "")
        if rat:
            rat = rat[:140]
            lines.append(f"  → _{md_escape(rat)}_")
        lines.append("")

    lines.append("📊 [Insider Clusters Dashboard](https://justhodl.ai/insider-clusters.html)")
    lines.append("📈 [Nobrainers](https://justhodl.ai/nobrainers.html) \\| 🎯 [Themes](https://justhodl.ai/themes.html)")
    lines.append("")
    lines.append("_SEC Form 4 cluster scanner \\| daily 14:30 UTC \\| 30\\-day rolling window_")

    text = "\n".join(lines)
    log(f"  message length: {len(text)} chars")
    log("  preview (first 800 chars):")
    for ln in text.splitlines()[:25]:
        log(f"    {ln}")

    section("3) Send via Telegram")
    token, chat = get_telegram()
    log(f"  bot token: ...{token[-8:]}")
    log(f"  chat_id: {chat}")
    ok, info = send(token, chat, text)
    if ok:
        log(f"  ✅ delivered, message_id={info}")
    else:
        log(f"  ❌ {info}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "send_insider_clusters_telegram.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
