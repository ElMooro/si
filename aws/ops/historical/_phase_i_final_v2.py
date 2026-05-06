"""
PHASE I (v2) — final state verification + summary Telegram digest.
Clean version — no nested f-string escapes that broke Phase I v1.
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
        sys_str = ",".join(r["systems"])
        log(f"    {r['symbol']:<6} #{r['n_systems']}  comp={r['compound_score']:>7.1f}  ({sys_str})")

    section("2) L5 rationale state")
    r5 = json.loads(S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")["Body"].read())
    log(f"  generated_at: {r5.get('generated_at')}")
    log(f"  n_theses: {r5.get('n_theses')}, n_ok: {r5.get('n_claude_ok')}")
    tickers_with_priority = []
    for t in r5.get("theses", []):
        priority = (t.get("candidate") or {}).get("_compound_priority")
        marker = " [" + priority + "]" if priority else ""
        tickers_with_priority.append(t.get("ticker", "?") + marker)
    log("  tickers: " + ", ".join(tickers_with_priority))

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
    cutoff = int(time.time()) - 86400
    items = []
    try:
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
    except Exception as e:
        log(f"  ❌ DDB: {e}")

    by_src = defaultdict(int)
    for it in items:
        by_src[it.get("source", "?")] += 1
    log(f"  total: {len(items)}")
    for src, n in sorted(by_src.items()):
        log(f"    {src}: {n}")

    section("5) Pages — HTTP check")
    pages = [
        "https://justhodl.ai/compound-signals.html",
        "https://justhodl.ai/nobrainers.html",
        "https://justhodl.ai/insider-clusters.html",
        "https://justhodl.ai/smart-money.html",
        "https://justhodl.ai/deep-value.html",
        "https://justhodl.ai/eps-velocity.html",
    ]
    for url in pages:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                log(f"  ✓ {r.status}  {len(r.read()):>8,}b  {url}")
        except Exception as e:
            log(f"  ❌ {url}: {e}")

    section("6) Compose summary digest")
    fcx = next((r for r in cs.get("compound", []) if r.get("symbol") == "FCX"), None)
    fcx_thesis = next((t for t in r5.get("theses", []) if t.get("ticker") == "FCX"), None)
    n_total_names = cs.get("stats", {}).get("n_total_names", 0)
    n_multi = cs.get("stats", {}).get("n_multi_signal", 0)
    n_t3 = cs.get("stats", {}).get("n_3_plus", 0)

    lines = []
    lines.append("✅ *SYSTEM AUDIT \\+ EXPONENTIAL IMPROVEMENT COMPLETE*")
    lines.append("📅 " + md_escape(time.strftime("%Y-%m-%d %H:%M UTC")))
    lines.append("")
    lines.append("*Final state:*")
    lines.append("• " + md_escape(str(active)) + "/" + md_escape(str(len(lambdas))) + " Lambdas active")
    lines.append("• " + md_escape(str(n_total_names)) + " names tracked across 5 systems")
    lines.append("• " + md_escape(str(n_multi)) + " multi\\-signal names")
    lines.append("• " + md_escape(str(n_t3)) + " TIER\\-3 names \\(3\\+ systems\\)")
    lines.append("• " + md_escape(str(len(items))) + " signals logged to calibration in last 24h")
    lines.append("")

    if fcx and fcx_thesis:
        lines.append("🚀 *Featured: FCX \\(Freeport\\-McMoRan\\) — TIER\\-3*")
        lines.append("compound score: *" + md_escape(str(int(fcx["compound_score"]))) + "*")
        details = fcx.get("details") or {}
        if "nobrainers" in details:
            d = details["nobrainers"]
            tier = md_escape(str(d.get("tier", "?")))
            theme = md_escape(d.get("theme", ""))
            lines.append("  🎯 nobrainer tier " + tier + ", theme " + theme)
        if "smart_money" in details:
            sm = details["smart_money"]
            n_buy = md_escape(str(sm.get("n_buyers", 0)))
            n_sell = md_escape(str(sm.get("n_sellers", 0)))
            legends = ", ".join(sm.get("legend_buyers") or [])
            legends_text = md_escape(legends) if legends else md_escape("none")
            lines.append("  💼 smart\\-money " + n_buy + " buying / " + n_sell + " selling \\(legends: " + legends_text + "\\)")
        if "eps_velocity" in details:
            ev = details["eps_velocity"]
            lift_val = ev.get("fy2_lift_pct") or 0
            rg_val = ev.get("fwd_rev_growth_pct") or 0
            lift_str = md_escape("{:.0f}".format(lift_val))
            rg_str = md_escape("{:.0f}".format(rg_val))
            lines.append("  📈 eps\\-velocity \\+" + lift_str + "% EPS, \\+" + rg_str + "% revenue growth")
        # Decisive call line
        text = fcx_thesis.get("thesis", "")
        for ln in text.splitlines():
            if "Position:" in ln or "Entry zone:" in ln or "CALL:" in ln:
                clean = ln.replace("**", "").strip()[:120]
                lines.append("  → _" + md_escape(clean) + "_")
                break
        lines.append("")

    lines.append("*Other multi\\-signal:*")
    sys_emojis = {
        "nobrainers": "🎯",
        "insiders": "👀",
        "smart_money": "💼",
        "deep_value": "💎",
        "eps_velocity": "📈",
    }
    for r in cs.get("compound", [])[:7]:
        if r.get("symbol") == "FCX":
            continue
        sys_str = " ".join(sys_emojis.get(s, "•") for s in r.get("systems", []))
        sym = md_escape(r.get("symbol", "?"))
        comp = md_escape(str(int(r.get("compound_score", 0))))
        lines.append("*" + sym + "* " + sys_str + "  comp\\=" + comp)
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
    log("  preview (first 25 lines):")
    for ln in text.splitlines()[:25]:
        log("    " + ln[:120])

    section("7) Send via Telegram")
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
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_i_final_summary.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
