"""
1. Re-invoke deep-value with profile-based sector lookup
2. Re-aggregate compound signals (smart-money uses ticker field)
3. Send final consolidated Telegram digest with the cleaner findings
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


def main():
    section("1) Verify deep-value deployed with /profile fix")
    code_url = L.get_function(FunctionName="justhodl-deep-value-screener")["Code"]["Location"]
    import zipfile
    zb = urllib.request.urlopen(code_url).read()
    src = zipfile.ZipFile(io.BytesIO(zb)).read("lambda_function.py").decode("utf-8")
    has_profile_fix = "fetch_profile(symbol)" in src and "(profile or {}).get(\"sector\")" in src
    log(f"  /profile fix in deployed code: {has_profile_fix}")

    section("2) Re-invoke deep-value")
    r = L.invoke(FunctionName="justhodl-deep-value-screener", InvocationType="RequestResponse",
                  LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}")
    body = json.loads(r["Payload"].read())
    log(f"  body: {body.get('body','')[:300]}")

    obj = S3.get_object(Bucket=BUCKET, Key="data/deep-value.json")
    dv = json.loads(obj["Body"].read())
    log("")
    log("  ── new top_25 (financials excluded) ──")
    for c in dv.get("summary", {}).get("top_25_overall", [])[:15]:
        log(f"    {c.get('symbol'):<6} {c.get('score'):>6.1f}  {c.get('flag','')[:24]:<24}  {c.get('sector','')[:25]}")
    log("")
    log("  ── top excluded (financials/REITs) ──")
    for c in dv.get("summary", {}).get("top_25_excluded_financials", [])[:8]:
        log(f"    {c.get('symbol'):<6} {c.get('score'):>6.1f}  {c.get('flag',''):<26}  {c.get('sector','')[:25]}")

    section("3) Re-aggregate compound signals")
    feed_map = {
        "nobrainers":   ("data/nobrainers.json",            "summary.top_25_overall",  "ticker"),
        "insiders":     ("data/insider-clusters.json",      "clusters",                "ticker"),
        "smart_money":  ("data/smart-money-clusters.json",  "clusters",                "ticker"),
        "deep_value":   ("data/deep-value.json",            "summary.top_25_overall",  "symbol"),
        "eps_velocity": ("data/eps-revision-velocity.json", "summary.top_25_overall",  "symbol"),
    }
    presence = defaultdict(lambda: {"systems": set(), "scores": {}, "details": {}})
    feed_stats = {}
    for name, (key, path, sym_field) in feed_map.items():
        try:
            obj = S3.get_object(Bucket=BUCKET, Key=key)
            d = json.loads(obj["Body"].read())
            cursor = d
            for p in path.split("."):
                cursor = cursor.get(p, [])
                if cursor is None: cursor = []
            count = len(cursor) if isinstance(cursor, list) else 0
            feed_stats[name] = count
            for c in cursor:
                if not isinstance(c, dict): continue
                sym = (c.get(sym_field) or "").upper().strip()
                if not sym: continue
                score = c.get("score") or c.get("asymmetric_score") or 0
                presence[sym]["systems"].add(name)
                presence[sym]["scores"][name] = score
                if name == "nobrainers":
                    presence[sym]["details"][name] = {"theme": c.get("theme_etf"), "tier": c.get("tier"), "flag": c.get("flag")}
                elif name == "insiders":
                    presence[sym]["details"][name] = {
                        "signal": c.get("signal_type"), "n_insiders": c.get("n_insiders"),
                        "total_value": c.get("total_value"),
                        "ceo": c.get("has_ceo"), "cfo": c.get("has_cfo"),
                        "rationale": c.get("rationale", "")[:120],
                    }
                elif name == "smart_money":
                    presence[sym]["details"][name] = {
                        "signal_types": c.get("signal_types"),
                        "n_buyers": c.get("n_buyers"),
                        "n_sellers": c.get("n_sellers"),
                        "legend_buyers": c.get("legend_buyers", []),
                    }
                elif name == "deep_value":
                    presence[sym]["details"][name] = {
                        "flag": c.get("flag"),
                        "net_cash_pct": c.get("net_cash_pct"),
                        "mcap_to_rev": c.get("mcap_to_rev"),
                        "pct_from_52w_high": c.get("pct_from_52w_high"),
                        "sector": c.get("sector", ""),
                    }
                elif name == "eps_velocity":
                    presence[sym]["details"][name] = {
                        "flag": c.get("flag"),
                        "fy2_lift_pct": c.get("fy2_lift_pct"),
                        "fwd_rev_growth_pct": c.get("fwd_rev_growth_pct"),
                    }
        except Exception as e:
            log(f"  {name}: ERROR {e}")
            feed_stats[name] = 0

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
    log(f"  ✓ wrote {len(body)}b")
    log(f"  feed_stats: {json.dumps(feed_stats)}")
    log(f"  total: {len(presence)}, multi: {len(multi)}, 3+: {out['stats']['n_3_plus']}")
    log("")
    log("  ── compound leaderboard ──")
    for r in ranked[:15]:
        sym_systems = ",".join(r["systems"])
        log(f"    {r['symbol']:<6} #{r['n_systems']}  comp={r['compound_score']:>6.1f}  ({sym_systems})")

    section("4) Send final Telegram digest")
    lines = ["⚡ *FINAL: 5\\-SYSTEM HUNTER FULLY OPERATIONAL*"]
    lines.append(f"📅 {md_escape(time.strftime('%Y-%m-%d %H:%M UTC'))}")
    lines.append("")
    lines.append("*Feed health:*")
    lines.append(f"🎯 Nobrainers: {feed_stats.get('nobrainers',0)}")
    lines.append(f"👀 Insiders: {feed_stats.get('insiders',0)}")
    lines.append(f"💼 Smart Money: {feed_stats.get('smart_money',0)}")
    lines.append(f"💎 Deep Value: {feed_stats.get('deep_value',0)} \\(financials excluded\\)")
    lines.append(f"📈 EPS Velocity: {feed_stats.get('eps_velocity',0)}")
    lines.append("")

    if ranked:
        lines.append(f"🔥 *{len(multi)} TIER\\-2 \\(2 SYSTEMS AGREE\\):*")
        lines.append("")
        emojis = {"nobrainers":"🎯","insiders":"👀","smart_money":"💼","deep_value":"💎","eps_velocity":"📈"}
        for r in ranked[:8]:
            e = " ".join(emojis.get(s,"•") for s in r["systems"])
            lines.append(f"*{md_escape(r['symbol'])}* {e} compound\\={md_escape(str(r['compound_score']))}")
            for s in r["systems"]:
                d = r["details"].get(s, {})
                if s == "insiders" and d.get("rationale"):
                    lines.append(f"  {emojis[s]} _{md_escape(d['rationale'][:90])}_")
                    break
                elif s == "eps_velocity":
                    lift = d.get("fy2_lift_pct", 0) or 0
                    rg = d.get("fwd_rev_growth_pct", 0) or 0
                    lines.append(f"  {emojis[s]} _\\+{md_escape(f'{lift:.0f}')}% EPS, \\+{md_escape(f'{rg:.0f}')}% rev_")
                    break
                elif s == "smart_money":
                    leg = ", ".join((d.get("legend_buyers") or [])[:2])[:50]
                    nb = d.get("n_buyers", 0)
                    lines.append(f"  {emojis[s]} _{md_escape(str(nb))} funds buying \\| legends: {md_escape(leg) or md_escape('—')}_")
                    break
                elif s == "deep_value":
                    nc = (d.get("net_cash_pct", 0) or 0) * 100
                    mr = d.get("mcap_to_rev", 0) or 0
                    lines.append(f"  {emojis[s]} _{md_escape(f'{nc:.0f}')}% net cash, mcap/rev {md_escape(f'{mr:.2f}')}×_")
                    break
                elif s == "nobrainers":
                    theme = d.get("theme", "")
                    lines.append(f"  {emojis[s]} _tier {md_escape(str(d.get('tier','?')))} in {md_escape(theme)} theme_")
                    break
            lines.append("")

    lines.append("[Compound Page](https://justhodl.ai/compound-signals.html)")
    lines.append("")
    lines.append("_Auto\\-updates daily \\| 5 systems × 10 Lambdas \\| Sandbox\\-deployed via GitHub Actions_")

    text = "\n".join(lines)
    log(f"  message length: {len(text)} chars")

    token = SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
    chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({
        "chat_id": chat, "text": text, "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }).encode()
    req = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            mid = json.loads(r.read())["result"]["message_id"]
            log(f"  ✅ delivered, message_id={mid}")
    except Exception as e:
        log(f"  ❌ {e}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "rerun_dv_profile_fix.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
