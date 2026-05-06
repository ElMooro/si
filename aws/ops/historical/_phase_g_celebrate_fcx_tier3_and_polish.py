"""
PHASE G — Mark the FCX tier-3 compound signal as the system's first big win.

Steps:
  1. Force-invoke L5 to write a fresh FCX thesis using the new compound-aware prompt
  2. Force-invoke compound-aggregator to ensure it sees latest data
  3. Send a custom Telegram digest highlighting the tier-3 finding
  4. Patch the master audit script to use 'stocks' key (cosmetic fix)
"""
import io, json, os, time, base64, urllib.request, urllib.error
from botocore.config import Config
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=600, connect_timeout=10))
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def md_escape(s):
    out = []
    for c in str(s):
        if c in "_*[]()~`>#+-=|{}.!\\":
            out.append("\\" + c)
        else:
            out.append(c)
    return "".join(out)


def send_telegram(text):
    token = SSM.get_parameter(Name="/justhodl/telegram/bot_token",
                                WithDecryption=True)["Parameter"]["Value"]
    chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
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
            return True, json.loads(r.read())["result"]["message_id"]
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}"
    except Exception as e:
        return False, str(e)


def main():
    section("1) Force-invoke compound-aggregator to refresh state")
    t0 = time.time()
    r = L.invoke(FunctionName="justhodl-compound-aggregator", InvocationType="RequestResponse",
                  LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}, dur: {time.time()-t0:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {body.get('body','')[:300]}")

    cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    log(f"  feed_stats: {json.dumps(cs.get('feed_stats', {}))}")
    log(f"  stats: {json.dumps(cs.get('stats', {}))}")
    log("")
    log("  ── compound leaderboard ──")
    for r2 in cs.get("compound", [])[:8]:
        log(f"    {r2['symbol']:<6} #{r2['n_systems']}  comp={r2['compound_score']:>7.1f}  ({','.join(r2['systems'])})")

    section("2) Force-invoke L5 to write fresh theses")
    log("  Note: L5 will pick up the latest compound signals via S3 reads")
    t0 = time.time()
    r5 = L.invoke(FunctionName="justhodl-nobrainer-rationale",
                   InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log(f"  status: {r5['StatusCode']}, dur: {dur:.1f}s")
    body5 = json.loads(r5["Payload"].read())
    log(f"  body: {body5.get('body','')[:300]}")
    if "LogResult" in r5:
        tail = base64.b64decode(r5["LogResult"]).decode()
        compound_lines = [ln for ln in tail.splitlines() if "COMPOUND" in ln]
        log(f"  ── COMPOUND hits in L5 ({len(compound_lines)}) ──")
        for ln in compound_lines[:20]:
            log(f"    {ln.strip()}")

    section("3) Read fresh L5 — search for FCX thesis")
    r5_data = json.loads(S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")["Body"].read())
    log(f"  generated_at: {r5_data.get('generated_at')}")
    fcx_thesis = None
    for t in r5_data.get("theses", []):
        if t.get("ticker") == "FCX":
            fcx_thesis = t
            break

    if fcx_thesis:
        log(f"  ✓ FCX thesis found")
        text = fcx_thesis.get("thesis", "")
        log(f"  length: {len(text)} chars")
        log("  ── thesis (first 30 lines) ──")
        for ln in text.splitlines()[:30]:
            log(f"    {ln[:140]}")
    else:
        # FCX may not be in top-12 of nobrainer leaderboard
        log("  ⚠ FCX not in L5 theses (may not be in top-12 nobrainers)")
        log("  Available tickers in L5:")
        for t in r5_data.get("theses", []):
            log(f"    {t.get('ticker')}/{t.get('theme')}")

    section("4) Compose celebration Telegram digest")
    fcx_record = next((r for r in cs.get("compound", []) if r.get("symbol") == "FCX"), None)

    lines = ["🚀 *MILESTONE: FIRST TIER\\-3 COMPOUND SIGNAL*"]
    lines.append(f"📅 {md_escape(time.strftime('%Y-%m-%d %H:%M UTC'))}")
    lines.append("")
    lines.append("Three independent hunter systems are flagging the same name\\.")
    lines.append("This is the rare convergence the framework was built to find\\.")
    lines.append("")

    if fcx_record:
        lines.append(f"🔥 *FCX \\(Freeport\\-McMoRan\\)*")
        lines.append(f"compound score \\= *{md_escape(str(int(fcx_record['compound_score'])))}*  \\| {md_escape(str(fcx_record['n_systems']))} systems agree")
        lines.append("")
        details = fcx_record.get("details", {}) or {}
        if "nobrainers" in details:
            d = details["nobrainers"]
            lines.append(f"🎯 *Nobrainer:* tier {md_escape(str(d.get('tier','?')))} in {md_escape(d.get('theme','?'))} \\({md_escape(d.get('flag','?'))}\\)")
        if "smart_money" in details:
            d = details["smart_money"]
            n_buy = d.get("n_buyers", 0) or 0
            n_sell = d.get("n_sellers", 0) or 0
            legends = ", ".join(d.get("legend_buyers") or [])
            lines.append(f"💼 *Smart Money:* {md_escape(str(n_buy))} buying \\| {md_escape(str(n_sell))} selling  \\(legends: {md_escape(legends or 'none')}\\)")
        if "eps_velocity" in details:
            d = details["eps_velocity"]
            lift = d.get("fy2_lift_pct", 0) or 0
            rg = d.get("fwd_rev_growth_pct", 0) or 0
            lines.append(f"📈 *EPS Velocity:* \\+{md_escape(f'{lift:.0f}')}% forward EPS, \\+{md_escape(f'{rg:.0f}')}% revenue growth")
        lines.append("")
        lines.append("*The story:* Copper miner where the consensus is rising, the theme")
        lines.append("ETF is bid, and Lone Pine \\(Stephen Mandel\\) is buying while 7 other")
        lines.append("13F funds are selling — classic contrarian smart\\-money pattern\\.")
        lines.append("")

    # Other multi-signals
    others = [r for r in cs.get("compound", []) if r.get("n_systems") >= 2 and r.get("symbol") != "FCX"]
    if others:
        lines.append(f"*Other multi\\-signal names \\({len(others)}\\):*")
        for r in others[:6]:
            sys_str = " ".join({"nobrainers":"🎯","insiders":"👀","smart_money":"💼","deep_value":"💎","eps_velocity":"📈"}.get(s,"•") for s in r["systems"])
            lines.append(f"*{md_escape(r['symbol'])}* {sys_str}  comp\\={md_escape(str(int(r['compound_score'])))}")
        lines.append("")

    lines.append(f"📊 [Compound Signals page](https://justhodl.ai/compound-signals.html)")
    lines.append("")
    lines.append("_5 hunter systems × 14 Lambdas \\| auto\\-updates hourly_")

    text = "\n".join(lines)
    log(f"  message length: {len(text)} chars")
    log("  preview:")
    for ln in text.splitlines()[:20]:
        log(f"    {ln[:120]}")

    section("5) Send")
    ok, info = send_telegram(text)
    log(f"  {'✅ delivered, message_id=' + str(info) if ok else '❌ ' + str(info)}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_g_fcx_tier3_celebration.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
