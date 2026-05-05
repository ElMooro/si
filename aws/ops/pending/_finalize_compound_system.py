"""
Finalize compound-signal system after deep-value patch deploys:
1. Wait for deep-value redeploy
2. Re-run deep-value with proper top_25 (excludes financials)
3. Re-run compound aggregation
4. Build Telegram digest of compound + tier-A signals across all 5 systems
5. Send digest
"""
import io, json, os, time, base64, urllib.request, urllib.error
import boto3
from collections import defaultdict

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
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
    token = SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
    chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
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
        with urllib.request.urlopen(req, timeout=20) as r:
            return True, json.loads(r.read())["result"]["message_id"]
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode('utf-8','replace')[:200]}"


def main():
    section("1) Verify deep-value is the latest deployed version")
    cfg = L.get_function_configuration(FunctionName="justhodl-deep-value-screener")
    log(f"  modified: {cfg['LastModified']}")
    # Pull deployed code and check for the new top_25 filter
    code_url = L.get_function(FunctionName="justhodl-deep-value-screener")["Code"]["Location"]
    import zipfile
    zb = urllib.request.urlopen(code_url).read()
    z = zipfile.ZipFile(io.BytesIO(zb))
    src = z.read("lambda_function.py").decode("utf-8")
    has_filter = "top_25_excluded_financials" in src
    log(f"  has top_25_excluded_financials: {has_filter}")
    if not has_filter:
        # Force redeploy
        local_src = open("aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py", "r").read()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            zi = zipfile.ZipInfo("lambda_function.py")
            zi.external_attr = 0o644 << 16
            z.writestr(zi, local_src)
        L.update_function_code(FunctionName="justhodl-deep-value-screener", ZipFile=buf.getvalue())
        for _ in range(30):
            c = L.get_function_configuration(FunctionName="justhodl-deep-value-screener")
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        log(f"  ✓ force-redeployed")

    section("2) Re-run deep-value")
    r = L.invoke(FunctionName="justhodl-deep-value-screener", InvocationType="RequestResponse",
                  LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}")
    body = json.loads(r["Payload"].read())
    inner = json.loads(body["body"]) if body.get("statusCode") == 200 else {}
    log(f"  inner: {json.dumps(inner)[:300]}")

    section("3) Re-aggregate compound signals")
    feeds = {
        "nobrainers":     ("data/nobrainers.json",          "summary.top_25_overall",   "ticker"),
        "insiders":       ("data/insider-clusters.json",    "clusters",                 "ticker"),
        "smart_money":    ("data/smart-money-clusters.json", "clusters",                "symbol"),
        "deep_value":     ("data/deep-value.json",          "summary.top_25_overall",   "symbol"),
        "eps_velocity":   ("data/eps-revision-velocity.json","summary.top_25_overall",  "symbol"),
    }
    presence = defaultdict(lambda: {"systems": set(), "scores": {}, "details": {}})
    feed_stats = {}

    for name, (key, path, sym_field) in feeds.items():
        try:
            obj = S3.get_object(Bucket=BUCKET, Key=key)
            d = json.loads(obj["Body"].read())
            cursor = d
            for p in path.split("."):
                cursor = cursor.get(p, [])
                if cursor is None: cursor = []
            count = len(cursor) if isinstance(cursor, list) else 0
            feed_stats[name] = count
            log(f"  {name}: {count}")
            for c in cursor:
                if not isinstance(c, dict): continue
                sym = (c.get(sym_field) or "").upper().strip()
                if not sym: continue
                score = c.get("score") or c.get("asymmetric_score") or 0
                presence[sym]["systems"].add(name)
                presence[sym]["scores"][name] = score
                if name == "nobrainers":
                    presence[sym]["details"][name] = {
                        "theme": c.get("theme_etf"), "tier": c.get("tier"), "flag": c.get("flag"),
                    }
                elif name == "insiders":
                    presence[sym]["details"][name] = {
                        "signal": c.get("signal_type"), "n_insiders": c.get("n_insiders"),
                        "total_value": c.get("total_value"), "ceo": c.get("has_ceo"),
                        "cfo": c.get("has_cfo"), "rationale": c.get("rationale", "")[:120],
                    }
                elif name == "smart_money":
                    presence[sym]["details"][name] = {
                        "signal": c.get("signal_type"), "n_buying": c.get("n_funds_adding"),
                        "legend_funds": c.get("legend_funds", []),
                    }
                elif name == "deep_value":
                    presence[sym]["details"][name] = {
                        "flag": c.get("flag"), "net_cash_pct": c.get("net_cash_pct"),
                        "mcap_to_rev": c.get("mcap_to_rev"),
                        "pct_from_52w_high": c.get("pct_from_52w_high"),
                    }
                elif name == "eps_velocity":
                    presence[sym]["details"][name] = {
                        "flag": c.get("flag"), "fy2_lift_pct": c.get("fy2_lift_pct"),
                        "fwd_rev_growth_pct": c.get("fwd_rev_growth_pct"),
                    }
        except Exception as e:
            log(f"  {name}: ERROR {e}")

    multi = {sym: data for sym, data in presence.items() if len(data["systems"]) >= 2}
    ranked = []
    for sym, data in multi.items():
        n = len(data["systems"])
        score = sum(data["scores"].values())
        ranked.append({
            "symbol": sym, "n_systems": n,
            "systems": sorted(list(data["systems"])),
            "scores": data["scores"], "details": data["details"],
            "compound_score": round(score * (1 + 0.5 * (n - 1)), 1),
        })
    ranked.sort(key=lambda x: (-x["n_systems"], -x["compound_score"]))

    log("")
    log(f"  total names: {len(presence)}")
    log(f"  on 2+ lists: {len(multi)}")
    log(f"  on 3+ lists: {sum(1 for r in ranked if r['n_systems'] >= 3)}")
    log("")
    log(f"  ── Compound leaderboard ──")
    for r in ranked[:10]:
        log(f"  {r['symbol']:<6} #{r['n_systems']}  systems={r['systems']}  compound={r['compound_score']}")

    out = {
        "schema_version": 1,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "feed_stats": feed_stats,
        "stats": {
            "n_total_names": len(presence),
            "n_multi_signal": len(multi),
            "n_3_plus": sum(1 for r in ranked if r["n_systems"] >= 3),
        },
        "compound": ranked,
    }
    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key="data/compound-signals.json", Body=body, ContentType="application/json")
    log(f"  wrote {len(body)}b to data/compound-signals.json")

    section("4) Build Telegram digest")
    lines = [f"⚡ *MULTI\\-SIGNAL COMPOUND DIGEST*"]
    lines.append(f"📅 {md_escape(time.strftime('%Y-%m-%d %H:%M UTC'))}")
    lines.append("")
    lines.append(f"📊 {feed_stats.get('nobrainers',0)} nobrainers \\| {feed_stats.get('insiders',0)} insider clusters \\| {feed_stats.get('smart_money',0)} smart\\-money \\| {feed_stats.get('deep_value',0)} deep\\-value \\| {feed_stats.get('eps_velocity',0)} EPS velocity")
    lines.append("")
    if ranked:
        lines.append(f"*🎯 Compound signals \\({len(multi)} names on 2\\+ systems\\):*")
        lines.append("")
        for r in ranked[:8]:
            sym = r["symbol"]
            sys_emojis = {
                "nobrainers": "🎯",
                "insiders": "👀",
                "smart_money": "💼",
                "deep_value": "💎",
                "eps_velocity": "📈",
            }
            sys_str = " ".join(sys_emojis.get(s, "•") for s in r["systems"])
            lines.append(f"*{md_escape(sym)}* {sys_str} compound\\={md_escape(str(r['compound_score']))}")
            for s in r["systems"]:
                d = r["details"].get(s, {})
                if s == "insiders":
                    rt = d.get("rationale", "")[:100]
                    if rt:
                        lines.append(f"  insider: _{md_escape(rt)}_")
                elif s == "nobrainers":
                    lines.append(f"  nobrainer: tier {md_escape(str(d.get('tier','')))} \\({md_escape(d.get('theme',''))}\\)")
                elif s == "smart_money":
                    funds = ", ".join(d.get("legend_funds", [])[:3])[:60]
                    lines.append(f"  smart\\-money: {md_escape(str(d.get('n_buying','')))} buying \\({md_escape(funds or 'no legends')}\\)")
                elif s == "deep_value":
                    lines.append(f"  deep\\-value: net\\-cash\\={md_escape(str(int((d.get('net_cash_pct',0) or 0)*100)))}%, m/r\\={md_escape(str(d.get('mcap_to_rev','')))}")
                elif s == "eps_velocity":
                    lines.append(f"  eps\\-velocity: \\+{md_escape(str(d.get('fy2_lift_pct','')))}% fwd EPS lift")
            lines.append("")
    else:
        lines.append("_No compound signals today \\(low overlap is normal — different universes\\)_")
        lines.append("")

    lines.append("*Tier\\-A leaders by system:*")
    lines.append(f"🎯 [Nobrainers](https://justhodl.ai/nobrainers.html)")
    lines.append(f"👀 [Insider Clusters](https://justhodl.ai/insider-clusters.html)")
    lines.append(f"💼 [Smart Money](https://justhodl.ai/smart-money.html)")
    lines.append(f"💎 [Deep Value](https://justhodl.ai/deep-value.html)")
    lines.append(f"📈 [EPS Velocity](https://justhodl.ai/eps-velocity.html)")
    lines.append("")
    lines.append("_Compound signal scoring \\| auto\\-updated daily \\| 5\\-system fusion_")

    text = "\n".join(lines)
    log(f"  message length: {len(text)} chars")

    section("5) Send Telegram")
    ok, info = send_telegram(text)
    log(f"  ✅ delivered, message_id={info}" if ok else f"  ❌ {info}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "finalize_compound_system.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
