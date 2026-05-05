"""
PHASE A — Build justhodl-compound-aggregator as a real Lambda.

This is a missing piece from today's session. The compound aggregation logic
has been running only in ops scripts on-demand. To make the system fully
auto-updating, we need:

1. A new Lambda justhodl-compound-aggregator that runs every hour
2. It reads all 5 system feeds, computes compound scores
3. Writes data/compound-signals.json
4. Triggers an alert via Telegram when:
   - A new TIER-3 (3+ systems) emerges
   - A TIER-2 entry's compound score crosses 200

This makes the compound page auto-refresh hourly without manual ops invocation.
"""
import io, json, os, time, zipfile, base64
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-compound-aggregator"
SCHEDULE_NAME = "justhodl-compound-aggregator-hourly"
SCHEDULE_EXPR = "rate(1 hour)"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


# ──────────────────────────────────────────────────────────────────────
# THE LAMBDA SOURCE
# ──────────────────────────────────────────────────────────────────────
LAMBDA_SOURCE = '''"""
justhodl-compound-aggregator — fuses signals across 5 hunter systems.

Reads:
  data/nobrainers.json          (theme-supply-tier asymmetric hunter)
  data/insider-clusters.json    (SEC Form 4 insider cluster scanner)
  data/smart-money-clusters.json (13F smart-money cluster scanner)
  data/deep-value.json          (Ben Graham net-cash screener)
  data/eps-revision-velocity.json (MU-pattern accelerating consensus)

Writes:
  data/compound-signals.json with structure:
    feed_stats:    counts per feed
    stats:         total_names / multi_signal / 3_plus
    compound:      ranked list of names appearing on 2+ systems
    new_alerts:    alerts emitted this run (for Telegram)
    history:       last N daily snapshots (rolling)

Compound score = sum(per_system_scores) * (1 + 0.5 * (n_systems - 1))

Alerts on:
  - new TIER-3 (>=3 systems agree) — never seen on yesterday's snapshot
  - existing entries crossing compound_score = 200
  - new TIER-2 with compound >= 250

Schedule: hourly. State persistence via S3 'data/compound-signals-state.json'
for delta detection between runs.
"""
import json
import os
import time
import urllib.request
import urllib.error
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/compound-signals.json")
STATE_KEY = os.environ.get("STATE_KEY", "data/compound-signals-state.json")
TELEGRAM_ENABLED = os.environ.get("TELEGRAM_ENABLED", "1") == "1"

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


FEEDS = {
    "nobrainers":   ("data/nobrainers.json",            "summary.top_25_overall",  "ticker"),
    "insiders":     ("data/insider-clusters.json",      "clusters",                "ticker"),
    "smart_money":  ("data/smart-money-clusters.json",  "clusters",                "ticker"),
    "deep_value":   ("data/deep-value.json",            "summary.top_25_overall",  "symbol"),
    "eps_velocity": ("data/eps-revision-velocity.json", "summary.top_25_overall",  "symbol"),
}


def load_feed(key, path, sym_field):
    """Load a feed and return list of records with normalized keys."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        d = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[compound] WARN — feed {key} failed: {e}")
        return []
    cursor = d
    for p in path.split("."):
        if not isinstance(cursor, dict):
            return []
        cursor = cursor.get(p)
        if cursor is None:
            return []
    if not isinstance(cursor, list):
        return []
    out = []
    for c in cursor:
        if not isinstance(c, dict):
            continue
        sym = (c.get(sym_field) or "").upper().strip()
        if not sym:
            continue
        c["_normalized_symbol"] = sym
        out.append(c)
    return out


def aggregate():
    presence = defaultdict(lambda: {"systems": set(), "scores": {}, "details": {}})
    feed_stats = {}

    for name, (key, path, sym_field) in FEEDS.items():
        records = load_feed(key, path, sym_field)
        feed_stats[name] = len(records)
        print(f"[compound] {name}: {len(records)} entries")
        for c in records:
            sym = c["_normalized_symbol"]
            score = c.get("score") or c.get("asymmetric_score") or 0
            presence[sym]["systems"].add(name)
            presence[sym]["scores"][name] = score
            d = {}
            if name == "nobrainers":
                d = {
                    "theme": c.get("theme_etf"),
                    "tier": c.get("tier"),
                    "flag": c.get("flag"),
                    "name": c.get("name", ""),
                }
            elif name == "insiders":
                d = {
                    "signal": c.get("signal_type"),
                    "n_insiders": c.get("n_insiders"),
                    "total_value": c.get("total_value"),
                    "ceo": c.get("has_ceo"),
                    "cfo": c.get("has_cfo"),
                    "rationale": (c.get("rationale", "") or "")[:160],
                    "company": c.get("company", ""),
                }
            elif name == "smart_money":
                d = {
                    "signal_types": c.get("signal_types"),
                    "n_buyers": c.get("n_buyers"),
                    "n_sellers": c.get("n_sellers"),
                    "legend_buyers": c.get("legend_buyers", []),
                    "name": c.get("name", ""),
                }
            elif name == "deep_value":
                d = {
                    "flag": c.get("flag"),
                    "net_cash_pct": c.get("net_cash_pct"),
                    "mcap_to_rev": c.get("mcap_to_rev"),
                    "pct_from_52w_high": c.get("pct_from_52w_high"),
                    "sector": c.get("sector", ""),
                    "company": c.get("company", ""),
                }
            elif name == "eps_velocity":
                d = {
                    "flag": c.get("flag"),
                    "fy2_lift_pct": c.get("fy2_lift_pct"),
                    "fwd_rev_growth_pct": c.get("fwd_rev_growth_pct"),
                    "company": c.get("company", ""),
                }
            presence[sym]["details"][name] = d

    multi = {sym: data for sym, data in presence.items() if len(data["systems"]) >= 2}
    ranked = []
    for sym, data in multi.items():
        n = len(data["systems"])
        score = sum(data["scores"].values())
        compound = score * (1 + 0.5 * (n - 1))
        ranked.append({
            "symbol": sym,
            "n_systems": n,
            "systems": sorted(list(data["systems"])),
            "scores": data["scores"],
            "details": data["details"],
            "compound_score": round(compound, 1),
        })
    ranked.sort(key=lambda x: (-x["n_systems"], -x["compound_score"]))

    return {
        "feed_stats": feed_stats,
        "presence": presence,
        "multi": multi,
        "ranked": ranked,
    }


def load_prior_state():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"alerted_keys": [], "last_run": None}


def detect_new_alerts(ranked, prior_state):
    """Return list of new alerts (symbol, type, reason) since last run."""
    prior_alerted = set(prior_state.get("alerted_keys", []))
    new_alerts = []
    new_alerted = list(prior_alerted)

    for r in ranked:
        sym = r["symbol"]
        n = r["n_systems"]
        score = r["compound_score"]

        # TIER-3 emergence
        key_t3 = f"TIER3_{sym}"
        if n >= 3 and key_t3 not in prior_alerted:
            new_alerts.append({
                "symbol": sym, "type": "TIER_3_EMERGED",
                "n_systems": n, "score": score,
                "systems": r["systems"],
                "reason": f"{sym} now flagged by {n} independent systems: {', '.join(r['systems'])}",
            })
            new_alerted.append(key_t3)

        # Compound score crossing 200
        key_200 = f"OVER200_{sym}"
        if score >= 200 and key_200 not in prior_alerted:
            new_alerts.append({
                "symbol": sym, "type": "COMPOUND_OVER_200",
                "score": score, "systems": r["systems"],
                "reason": f"{sym} compound score reached {score:.0f} ({', '.join(r['systems'])})",
            })
            new_alerted.append(key_200)

        # Compound score crossing 300
        key_300 = f"OVER300_{sym}"
        if score >= 300 and key_300 not in prior_alerted:
            new_alerts.append({
                "symbol": sym, "type": "COMPOUND_OVER_300",
                "score": score, "systems": r["systems"],
                "reason": f"{sym} compound score reached {score:.0f} (very high) ({', '.join(r['systems'])})",
            })
            new_alerted.append(key_300)

    return new_alerts, new_alerted


def md_escape(s):
    out = []
    for c in str(s):
        if c in "_*[]()~`>#+-=|{}.!\\\\":
            out.append("\\\\" + c)
        else:
            out.append(c)
    return "".join(out)


def send_telegram(text):
    try:
        token = SSM.get_parameter(Name="/justhodl/telegram/bot_token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception as e:
        print(f"[compound] WARN — Telegram credentials: {e}")
        return False, str(e)
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
        return False, f"HTTP {e.code}: {e.read().decode('utf-8','replace')[:200]}"
    except Exception as e:
        return False, str(e)


def emit_alerts(new_alerts, agg):
    if not new_alerts:
        return
    if not TELEGRAM_ENABLED:
        return
    lines = ["⚡ *COMPOUND SIGNAL ALERT*"]
    lines.append(f"📅 {md_escape(time.strftime('%Y-%m-%d %H:%M UTC'))}")
    lines.append("")
    emojis = {
        "nobrainers": "🎯", "insiders": "👀", "smart_money": "💼",
        "deep_value": "💎", "eps_velocity": "📈",
    }
    for a in new_alerts[:8]:
        sym = a["symbol"]
        e = " ".join(emojis.get(s, "•") for s in a.get("systems", []))
        if a["type"] == "TIER_3_EMERGED":
            lines.append(f"🔥 *TIER\\\\-3 EMERGED: {md_escape(sym)}* {e}")
            lines.append(f"  {md_escape(str(a['n_systems']))} independent systems agree, compound\\\\={md_escape(str(int(a['score'])))}")
        elif a["type"] == "COMPOUND_OVER_300":
            lines.append(f"🚀 *EXCEPTIONAL: {md_escape(sym)}* {e}")
            lines.append(f"  Compound score crossed 300: {md_escape(str(int(a['score'])))}")
        elif a["type"] == "COMPOUND_OVER_200":
            lines.append(f"⚡ *HIGH CONVICTION: {md_escape(sym)}* {e}")
            lines.append(f"  Compound score crossed 200: {md_escape(str(int(a['score'])))}")
        # context from per-system details
        for r in agg["ranked"]:
            if r["symbol"] == sym:
                d = r.get("details", {})
                if "insiders" in d:
                    rt = (d["insiders"].get("rationale") or "")[:90]
                    if rt:
                        lines.append(f"    {emojis['insiders']} _{md_escape(rt)}_")
                if "eps_velocity" in d:
                    lift = d["eps_velocity"].get("fy2_lift_pct", 0)
                    rg = d["eps_velocity"].get("fwd_rev_growth_pct", 0)
                    lines.append(f"    {emojis['eps_velocity']} _\\\\+{md_escape(f'{lift:.0f}')}% EPS, \\\\+{md_escape(f'{rg:.0f}')}% rev_")
                break
        lines.append("")
    lines.append("[Compound page](https://justhodl.ai/compound-signals.html)")

    text = "\\n".join(lines)
    ok, info = send_telegram(text)
    print(f"[compound] alert send: ok={ok} info={info}")


def lambda_handler(event=None, context=None):
    started = time.time()
    print("[compound] starting compound aggregator v1.0")

    agg = aggregate()
    ranked = agg["ranked"]
    feed_stats = agg["feed_stats"]
    print(f"[compound] aggregated: {len(agg['presence'])} names, {len(agg['multi'])} multi-signal")

    # Delta detection
    prior_state = load_prior_state()
    new_alerts, new_alerted = detect_new_alerts(ranked, prior_state)
    print(f"[compound] new alerts this run: {len(new_alerts)}")

    out = {
        "schema_version": 2,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 2),
        "feed_stats": feed_stats,
        "stats": {
            "n_total_names": len(agg["presence"]),
            "n_multi_signal": len(agg["multi"]),
            "n_3_plus": sum(1 for r in ranked if r["n_systems"] >= 3),
            "n_compound_over_200": sum(1 for r in ranked if r["compound_score"] >= 200),
            "n_compound_over_300": sum(1 for r in ranked if r["compound_score"] >= 300),
        },
        "compound": ranked,
        "new_alerts": new_alerts,
    }
    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print(f"[compound] wrote {len(body)}b to {S3_KEY}")

    # Persist alert state (cap at last 100 to keep file small)
    new_alerted = new_alerted[-100:]
    new_state = {
        "alerted_keys": new_alerted,
        "last_run": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "last_compound_count": len(agg["multi"]),
        "last_3plus_count": sum(1 for r in ranked if r["n_systems"] >= 3),
    }
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                   Body=json.dumps(new_state).encode(),
                   ContentType="application/json")
    print(f"[compound] wrote state: {len(new_alerted)} alerted_keys tracked")

    if new_alerts:
        emit_alerts(new_alerts, agg)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_compound": len(agg["multi"]),
            "n_3_plus": out["stats"]["n_3_plus"],
            "n_alerts": len(new_alerts),
            "duration_s": out["duration_s"],
        }),
    }
'''


def main():
    section("0) Build Lambda directory + write source")
    src_dir = "aws/lambdas/justhodl-compound-aggregator/source"
    os.makedirs(src_dir, exist_ok=True)
    src_path = f"{src_dir}/lambda_function.py"
    with open(src_path, "w", encoding="utf-8") as f:
        f.write(LAMBDA_SOURCE)
    log(f"  wrote {src_path}: {len(LAMBDA_SOURCE)} chars")

    section("1) Validate syntax")
    import ast
    try:
        ast.parse(LAMBDA_SOURCE)
        log("  ✓ valid python")
    except SyntaxError as e:
        log(f"  ❌ syntax: {e}")
        return

    section("2) Build deployment zip")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, LAMBDA_SOURCE)
    zb = buf.getvalue()
    log(f"  zip: {len(zb):,}b")

    L = boto3.client("lambda", region_name=REGION)
    EB = boto3.client("events", region_name=REGION)
    S3_ = boto3.client("s3", region_name=REGION)

    section("3) Create or update Lambda")
    exists = False
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        exists = True
        log("  exists — updating")
    except L.exceptions.ResourceNotFoundException:
        log("  creating new")

    if exists:
        L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
        for _ in range(30):
            c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        L.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=512,
            Timeout=120,
            Environment={"Variables": {
                "S3_BUCKET": "justhodl-dashboard-live",
                "S3_KEY": "data/compound-signals.json",
                "STATE_KEY": "data/compound-signals-state.json",
                "TELEGRAM_ENABLED": "1",
            }},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN,
            Code={"ZipFile": zb},
            Timeout=120,
            MemorySize=512,
            Environment={"Variables": {
                "S3_BUCKET": "justhodl-dashboard-live",
                "S3_KEY": "data/compound-signals.json",
                "STATE_KEY": "data/compound-signals-state.json",
                "TELEGRAM_ENABLED": "1",
            }},
        )
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ ready, mem={c['MemorySize']}MB to={c['Timeout']}s")

    section("4) Schedule hourly")
    rule_arn = EB.put_rule(Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED")["RuleArn"]
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(FunctionName=LAMBDA_NAME, StatementId=f"{SCHEDULE_NAME}-eb",
                          Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                          SourceArn=rule_arn)
        log("  ✓ permission added")
    except L.exceptions.ResourceConflictException:
        log("  ✓ permission already exists")
    log(f"  rule: {SCHEDULE_NAME} expr={SCHEDULE_EXPR}")

    section("5) Smoke invoke")
    t0 = time.time()
    r = L.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                  LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}  duration: {time.time()-t0:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {json.dumps(body)[:400]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail ──")
        for ln in tail.splitlines()[-12:]:
            log(f"    {ln.rstrip()}")

    section("6) Verify output")
    obj = S3_.get_object(Bucket=BUCKET, Key="data/compound-signals.json")
    d = json.loads(obj["Body"].read())
    log(f"  schema: {d.get('schema_version')}")
    log(f"  feed_stats: {json.dumps(d.get('feed_stats', {}))}")
    log(f"  stats: {json.dumps(d.get('stats', {}))}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_a_compound_aggregator.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
