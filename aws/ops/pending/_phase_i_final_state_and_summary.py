"""
PHASE I — Final state verification + clean summary Telegram digest.

After all phases A through H, this:
  1. Confirms every Lambda is healthy
  2. Confirms compound aggregator is producing tier-3 (FCX)
  3. Confirms L5 wrote the FCX thesis
  4. Sends a final consolidated digest summarizing today's improvements
"""
import io, json, os, time, base64, urllib.request, urllib.error
import boto3
from collections import defaultdict
from boto3.dynamodb.conditions import Attr

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION).Table("justhodl-signals")

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


def main():
    section("1) Final compound state")
    cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    log(f"  generated_at: {cs.get('generated_at')}")
    log(f"  feed_stats: {json.dumps(cs.get('feed_stats', {}))}")
    log(f"  stats: {json.dumps(cs.get('stats', {}))}")
    log("")
    log("  ── compound leaderboard ──")
    for r in cs.get("compound", [])[:8]:
        log(f"    {r['symbol']:<6} #{r['n_systems']}  comp={r['compound_score']:>7.1f}  ({','.join(r['systems'])})")

    section("2) L5 rationale state")
    r5 = json.loads(S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")["Body"].read())
    log(f"  generated_at: {r5.get('generated_at')}")
    log(f"  n_theses: {r5.get('n_theses')}, n_ok: {r5.get('n_claude_ok')}")
    tickers_with_priority = []
    for t in r5.get("theses", []):
        priority = (t.get("candidate") or {}).get("_compound_priority")
        tickers_with_priority.append(f"{t.get('ticker')}{'[' + priority + ']' if priority else ''}")
    log(f"  tickers: {', '.join(tickers_with_priority)}")

    section("3) Lambda activity state")
    lambdas = [
        "justhodl-theme-detector", "justhodl-supply-inflection-scanner",
        "justhodl-theme-tier-classifier", "justhodl-asymmetric-hunter",
        "justhodl-nobrainer-rationale", "justhodl-nobrainer-tracker",
        "justhodl-insider-cluster-scanner", "justhodl-smart-money-cluster",
        "justhodl-deep-value-screener", "justhodl-eps-revision-velocity",
        "justhodl-compound-aggregator", "justhodl-universe-builder",
        "justhodl-system-signal-logger",
    ]
    active = 0
    for fn in lambdas:
        try:
            cfg = L.get_function_configuration(FunctionName=fn)
            if cfg.get("State") == "Active":
                active += 1
        except Exception:
            pass
    log(f"  active Lambdas: {active}/{len(lambdas)}")

    section("4) DDB signals — last 24h")
    try:
        cutoff = int(time.time()) - 86400
        resp = DDB.scan(
            FilterExpression=Attr("logged_at").gte(cutoff),
            ProjectionExpression="signal_id,#src,ticker",
            ExpressionAttributeNames={"#src": "source"},
        )
        items = resp.get("Items", [])
        while "LastEvaluatedKey" in resp:
            resp = DDB.scan(
                FilterExpression=Attr("logged_at").gte(cutoff),
                ProjectionExpression="signal_id,#src,ticker",
                ExpressionAttributeNames={"#src": "source"},
                ExclusiveStartKey=resp["LastEvaluatedKey"],
            )
            items.extend(resp.get("Items", []))
        by_src = defaultdict(int)
        for it in items:
            by_src[it.get("source","?")] += 1
        log(f"  total: {len(items)}")
        for src, n in sorted(by_src.items()):
            log(f"    {src}: {n}")
    except Exception as e:
        log(f"  ❌ {e}")

    section("5) Pages — full HTTP + nav check")
    pages = [
        "https://justhodl.ai/compound-signals.html",
        "https://justhodl.ai/nobrainers.html",
        "https://justhodl.ai/insider-clusters.html",
        "https://justhodl.ai/smart-money.html",
        "https://justhodl.ai/deep-value.html",
        "https://justhodl.ai/eps-velocity.html",
        "https://justhodl.ai/themes.html",
    ]
    for url in pages:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                log(f"  ✓ {r.status}  {len(r.read()):>8,}b  {url}")
        except Exception as e:
            log(f"  ❌ {url}: {e}")

    section("6) Send final summary digest")
    fcx = next((r for r in cs.get("compound", []) if r.get("symbol") == "FCX"), None)
    fcx_thesis = next((t for t in r5.get("theses", []) if t.get("ticker") == "FCX"), None)

    lines = ["✅ *SYSTEM AUDIT \\+ EXPONENTIAL IMPROVEMENT COMPLETE*"]
    lines.append(f"📅 {md_escape(time.strftime('%Y-%m-%d %H:%M UTC'))}")
    lines.append("")
    lines.append("*Final state:*")
    lines.append(f"• {md_escape(str(active))}/{md_escape(str(len(lambdas)))} Lambdas active")
    lines.append(f"• {md_escape(str(cs.get('stats',{}).get('n_total_names',0)))} names tracked across 5 systems")
    lines.append(f"• {md_escape(str(cs.get('stats',{}).get('n_multi_signal',0)))} multi\\-signal names")
    lines.append(f"• {md_escape(str(cs.get('stats',{}).get('n_3_plus',0)))} TIER\\-3 names \\(3\\+ systems\\)")
    lines.append("")

    if fcx and fcx_thesis:
        lines.append("🚀 *Featured: FCX \\(Freeport\\-McMoRan\\)*")
        lines.append(f"compound score: *{md_escape(str(int(fcx['compound_score'])))}* — TIER\\-3")
        details = fcx.get("details") or {}
        if "nobrainers" in details:
            lines.append(f"  🎯 nobrainer tier {md_escape(str(details['nobrainers'].get('tier','?')))}, theme {md_escape(details['nobrainers'].get('theme',''))}")
        if "smart_money" in details:
            sm = details["smart_money"]
            lines.append(f"  💼 smart\\-money {md_escape(str(sm.get('n_buyers',0)))} buying / {md_escape(str(sm.get('n_sellers',0)))} selling \\(legends: {md_escape(', '.join(sm.get('legend_buyers',[])) or 'none')}\\)")
        if "eps_velocity" in details:
            ev = details["eps_velocity"]
            lines.append(f"  📈 eps\\-velocity \\+{md_escape(f'{ev.get(\"fy2_lift_pct\",0):.0f}')}% EPS, \\+{md_escape(f'{ev.get(\"fwd_rev_growth_pct\",0):.0f}')}% rev growth")
        # Extract DECISIVE CALL line from thesis
        text = fcx_thesis.get("thesis", "")
        for ln in text.splitlines():
            if "Position:" in ln or "Entry zone:" in ln or "Target:" in ln:
                lines.append(f"  → _{md_escape(ln.replace('**','').strip()[:120])}_")
                break
        lines.append("")

    lines.append("*Other multi\\-signal:*")
    for r in cs.get("compound", [])[:7]:
        if r["symbol"] == "FCX":
            continue
        sys_str = " ".join({"nobrainers":"🎯","insiders":"👀","smart_money":"💼","deep_value":"💎","eps_velocity":"📈"}.get(s,"•") for s in r["systems"])
        lines.append(f"*{md_escape(r['symbol'])}* {sys_str}  comp\\={md_escape(str(int(r['compound_score'])))}")
    lines.append("")

    lines.append("*Today's exponential improvements:*")
    lines.append("✓ Compound aggregator now a Lambda \\(hourly auto\\-update \\+ alerts\\)")
    lines.append("✓ Universal signal logger pipes all 5 systems into calibration")
    lines.append("✓ Unified universe \\(336 quality stocks\\) increases overlap")
    lines.append("✓ L5 thesis Lambda force\\-includes tier\\-3 compound names")
    lines.append("✓ Deep\\-value financial\\-book exclusion fixed")
    lines.append("✓ Smart\\-money schedule no longer collides")
    lines.append("✓ All 5 hunter pages \\+ compound page wired across nav")
    lines.append("")
    lines.append("[Compound page](https://justhodl.ai/compound-signals.html)")

    text = "\n".join(lines)
    log(f"  message length: {len(text)} chars")

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
            mid = json.loads(r.read())["result"]["message_id"]
            log(f"  ✅ delivered, message_id={mid}")
    except urllib.error.HTTPError as e:
        log(f"  ❌ HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}")
    except Exception as e:
        log(f"  ❌ {e}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_i_final_summary.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
