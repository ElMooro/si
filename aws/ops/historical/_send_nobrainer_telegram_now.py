"""
Send the nobrainer top-5 thesis digest to Telegram immediately.
Independent of L5 — reads data/nobrainers-rationale.json and sends directly.
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
    """Escape Telegram MarkdownV2 reserved chars."""
    out = []
    for c in s:
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


def main():
    section("1) Load top-5 nobrainers")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
    data = json.loads(obj["Body"].read())
    summary = data.get("summary", {})
    top = summary.get("top_25_overall", [])[:5]
    log(f"  loaded {len(top)} nobrainers")

    section("2) Load matching theses")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
    rdata = json.loads(obj["Body"].read())
    theses_list = rdata.get("theses", []) or rdata.get("rationales", [])
    by_sym = {}
    for t in theses_list:
        sym = t.get("symbol") or t.get("ticker")
        if sym:
            by_sym[sym] = t
    log(f"  loaded {len(by_sym)} theses indexed by symbol")

    section("3) Build digest message")
    lines = []
    lines.append("🎯 *NOBRAINER DIGEST*")
    lines.append(f"📅 {time.strftime('%Y\\-%m\\-%d %H:%M UTC')}")
    lines.append("")
    lines.append("Tier\\-A asymmetric trades \\(score ≥ 80\\):")
    lines.append("")
    for c in top:
        sym = c.get("ticker", "?")
        theme = c.get("theme_etf", "?")
        score = c.get("asymmetric_score", "?")
        flag = c.get("flag", "")
        factors = c.get("factors", {})
        sup = factors.get("supply_inflection", 0)
        val = factors.get("valuation_asym", 0)
        cat = factors.get("catalyst_prox", 0)
        lines.append(f"*{md_escape(sym)}* \\({md_escape(theme)}\\) — score *{md_escape(str(score))}*")
        lines.append(f"  supply\\={md_escape(f'{sup:.0f}')} val\\={md_escape(f'{val:.0f}')} cat\\={md_escape(f'{cat:.0f}')}")
        # Add 1-line conclusion from thesis if available
        t = by_sym.get(sym)
        if t:
            txt = t.get("rationale") or t.get("thesis") or ""
            # Find DECISIVE CALL line and extract action
            for line in txt.splitlines():
                if "LONG" in line.upper() and "%" in line:
                    snippet = line.strip().lstrip("*").strip()[:120]
                    lines.append(f"  → {md_escape(snippet)}")
                    break
        lines.append("")
    lines.append("📊 [Nobrainers Dashboard](https://justhodl.ai/nobrainers.html)")
    lines.append("📈 [Themes Detector](https://justhodl.ai/themes.html)")
    lines.append("")
    lines.append("_Picks\\-and\\-shovels detection \\| L1\\-L6 pipeline \\| auto\\-updated daily 13:30 UTC_")

    text = "\n".join(lines)
    log(f"  message length: {len(text)} chars")
    log("  preview (first 600 chars):")
    for ln in text.splitlines()[:15]:
        log(f"    {ln}")

    section("4) Send via Telegram")
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
    with open(os.path.join(out, "send_nobrainer_telegram.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
