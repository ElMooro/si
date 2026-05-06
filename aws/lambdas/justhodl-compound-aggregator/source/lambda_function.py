"""
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
    # 5 fundamental hunters
    "nobrainers":     ("data/nobrainers.json",             "summary.top_25_overall",  "ticker"),
    "insiders":       ("data/insider-clusters.json",       "clusters",                "ticker"),
    "smart_money":    ("data/smart-money-clusters.json",   "clusters",                "ticker"),
    "deep_value":     ("data/deep-value.json",             "summary.top_25_overall",  "symbol"),
    "eps_velocity":   ("data/eps-revision-velocity.json",  "summary.top_25_overall",  "symbol"),
    # 2 technical hunters
    "momentum":       ("data/momentum-breakout.json",      "summary.top_25_overall",  "symbol"),
    "pre_pump":       ("data/pre-pump-signals.json",       "summary.top_25_overall",  "symbol"),
    # NEW: institutional signals
    "options_flow":   ("data/options-flow.json",           "summary.top_25_overall",  "symbol"),
    "activist":       ("data/activist-filings.json",       "summary.top_25_overall",  "subject_ticker"),
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
            elif name == "momentum":
                d = {
                    "tier": c.get("tier"),
                    "flags": c.get("flags") or [],
                    "ret_60d": c.get("ret_60d"),
                    "ret_20d": c.get("ret_20d"),
                    "vol_ratio": c.get("vol_ratio"),
                    "rs_vs_spy_20d": c.get("rs_vs_spy_20d"),
                }
            elif name == "pre_pump":
                d = {
                    "tier": c.get("tier"),
                    "flags": c.get("flags") or [],
                    "obv_slope": c.get("obv_slope"),
                    "vol_comp": c.get("vol_comp"),
                    "liq_expand": c.get("liq_expand"),
                    "ret_60d": c.get("ret_60d"),
                    "ret_30d": c.get("ret_30d"),
                }
            elif name == "options_flow":
                d = {
                    "tier": c.get("tier"),
                    "flags": c.get("flags") or [],
                    "cpr_recent": c.get("cpr_recent"),
                    "cpr_change_pct": c.get("cpr_change_pct"),
                    "call_vol_surge": c.get("call_vol_surge"),
                    "short_pct_change": c.get("short_pct_change"),
                }
            elif name == "activist":
                d = {
                    "level": c.get("level"),
                    "form_type": c.get("form_type"),
                    "filer_tier": c.get("filer_tier"),
                    "filer_name": c.get("filer_name"),
                    "flags": c.get("flags") or [],
                    "filing_date": c.get("filing_date"),
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
        if c in "_*[]()~`>#+-=|{}.!\\":
            out.append("\\" + c)
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
        "momentum": "🚀", "pre_pump": "🌱",
        "options_flow": "📞", "activist": "🏛️",
    }
    for a in new_alerts[:8]:
        sym = a["symbol"]
        e = " ".join(emojis.get(s, "•") for s in a.get("systems", []))
        if a["type"] == "TIER_3_EMERGED":
            lines.append(f"🔥 *TIER\\-3 EMERGED: {md_escape(sym)}* {e}")
            lines.append(f"  {md_escape(str(a['n_systems']))} independent systems agree, compound\\={md_escape(str(int(a['score'])))}")
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
                    lines.append(f"    {emojis['eps_velocity']} _\\+{md_escape(f'{lift:.0f}')}% EPS, \\+{md_escape(f'{rg:.0f}')}% rev_")
                break
        lines.append("")
    lines.append("[Compound page](https://justhodl.ai/compound-signals.html)")

    text = "\n".join(lines)
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
