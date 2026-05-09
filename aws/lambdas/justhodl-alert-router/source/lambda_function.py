"""
justhodl-alert-router — Real-time threshold alerter, runs every 30 min.

Scans 12 high-leverage data sources for extreme readings:
  1. correlation-surface     | regime breaks (|Δ corr 30d vs 90d| ≥ 0.30)
  2. macro-surprise          | composite z-score |z| ≥ 2.0
  3. yield-curve             | rapid bear-flatten/bull-steepen detection
  4. eurodollar-stress       | composite stress > 70/100
  5. auction-crisis          | crisis score > 60/100
  6. vix-curve               | VIX1M/VIX3M inversion (term structure)
  7. divergence              | new divergences with |z| ≥ 2.5
  8. cot-extremes            | new percentile-rank extremes ≤ 5 or ≥ 95
  9. earnings-tracker        | new STRONG_POSITIVE_DRIFT or STRONG_NEGATIVE_DRIFT
 10. short-interest          | new SQUEEZE_RISK signals
 11. etf-flows               | new HEAVY_INFLOW or HEAVY_OUTFLOW
 12. sector-rotation         | regime change to/from BROAD_LEADERSHIP

Deduplication: keeps a state file in S3 (alerts-state.json) that records when
each alert type was last fired. Same alert is suppressed for 6 hours by default.

Sends to Telegram via existing bot. Schedule: rate(30 minutes).

Output:
  - data/alert-history.json  (last 100 alerts fired, for audit)
  - alerts-state.json        (dedup state)
"""

import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from _sentry_lite import track_errors


S3 = boto3.client("s3", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
DEDUP_HOURS = 6
HISTORY_KEY = "data/alert-history.json"
STATE_KEY = "alerts-state.json"

# WebSocket broadcast endpoint — fans alerts out to all subscribed clients
# in real time (parallel to the existing Telegram path).
WSS_BROADCAST_URL = os.environ.get(
    "WSS_BROADCAST_URL",
    "https://p6kvtojb2y6r4orgbtxh7ld3nu0pfktz.lambda-url.us-east-1.on.aws/",
)
_WSS_TOKEN_CACHE = {"token": None}


def _get_wss_admin_token():
    if _WSS_TOKEN_CACHE["token"]:
        return _WSS_TOKEN_CACHE["token"]
    try:
        t = SSM.get_parameter(Name="/justhodl/push/admin-token", WithDecryption=True)["Parameter"]["Value"]
        _WSS_TOKEN_CACHE["token"] = t
        return t
    except Exception as e:
        print(f"[wss] could not load admin token: {e}")
        return None


def broadcast_alert_to_wss(alert: dict):
    """Push alert to /alerts WSS channel. Best-effort, never raises."""
    token = _get_wss_admin_token()
    if not token:
        return False, "no_token"
    try:
        # Compact payload — full detail is in the alert; clients can fetch
        # data/alert-history.json if they need older alerts.
        payload = {
            "channel": "alerts",
            "id": alert.get("id"),
            "severity": alert.get("severity"),
            "category": alert.get("category"),
            "title": alert.get("title"),
            "detail": alert.get("detail"),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            WSS_BROADCAST_URL, data=body, method="POST",
            headers={"Content-Type": "application/json",
                     "X-Justhodl-Admin-Token": token},
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            resp_body = r.read().decode("utf-8")
            try:
                j = json.loads(resp_body)
                return True, f"sent={j.get('sent', 0)} scanned={j.get('scanned', 0)}"
            except Exception:
                return r.status == 200, resp_body[:120]
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:120]}"


def get_chat_id():
    try:
        return SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception as e:
        print(f"[ssm] chat_id: {e}")
        return None


def get_token():
    if TG_BOT_TOKEN:
        return TG_BOT_TOKEN
    try:
        return SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def send_telegram(text, chat_id):
    token = get_token()
    if not token or not chat_id:
        print("[tg] missing token/chat_id")
        return False, "missing creds"
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        body = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return True, resp.read().decode()[:200]
    except Exception as e:
        return False, str(e)


# ─── WEBHOOK DELIVERY ─────────────────────────────────────────────────
# In addition to Telegram, alerts can be POSTed to any number of webhook
# URLs (Slack, Discord, custom HTTP receivers, PagerDuty Events API).
# Webhook URLs are stored as a JSON list in SSM SecureString at
# /justhodl/alerts/webhook_urls — each item is either a plain URL string
# or {url, type, min_severity}. Slack/Discord get format-specific
# payloads; everything else gets the generic JSON envelope.
#
# Schema example (SSM value):
#   ["https://hooks.slack.com/services/T0/B0/abc",
#    {"url": "https://discord.com/api/webhooks/123/abc", "type": "discord"},
#    {"url": "https://example.com/alert", "type": "generic", "min_severity": "HIGH"}]

SEVERITY_RANK = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}


def get_webhook_urls():
    """Read /justhodl/alerts/webhook_urls — a JSON list. Returns [] if missing."""
    try:
        v = SSM.get_parameter(Name="/justhodl/alerts/webhook_urls", WithDecryption=True)["Parameter"]["Value"]
        parsed = json.loads(v) if v else []
        if not isinstance(parsed, list):
            return []
        out = []
        for item in parsed:
            if isinstance(item, str):
                out.append({"url": item, "type": _detect_type(item), "min_severity": "LOW"})
            elif isinstance(item, dict) and item.get("url"):
                out.append({
                    "url": item["url"],
                    "type": item.get("type") or _detect_type(item["url"]),
                    "min_severity": item.get("min_severity", "LOW"),
                })
        return out
    except Exception as e:
        # Param not set yet is the normal case — silent
        if "ParameterNotFound" not in str(e):
            print(f"[webhooks] read err: {e}")
        return []


def _detect_type(url: str) -> str:
    u = (url or "").lower()
    if "slack.com" in u:
        return "slack"
    if "discord.com" in u or "discordapp.com" in u:
        return "discord"
    return "generic"


def _passes_severity(alert_sev: str, min_sev: str) -> bool:
    return SEVERITY_RANK.get(alert_sev, 0) >= SEVERITY_RANK.get(min_sev or "LOW", 1)


def format_slack_payload(alert):
    """Slack incoming-webhook block format."""
    sev_color = {"HIGH": "#ff174a", "MEDIUM": "#ffc400", "LOW": "#00d4ff"}.get(alert.get("severity"), "#6f7b91")
    sev_icon = {"HIGH": ":red_circle:", "MEDIUM": ":large_yellow_circle:",
                "LOW": ":large_blue_circle:"}.get(alert.get("severity"), ":white_circle:")
    title = f"{sev_icon} {alert.get('title') or alert.get('check') or alert.get('id', 'Alert')}"
    body = alert.get("message") or alert.get("body") or ""
    fields = []
    for k in ("source", "metric", "value", "threshold", "check", "ts"):
        if k in alert and alert[k] is not None:
            fields.append({"title": k, "value": str(alert[k])[:200], "short": True})
    return {
        "attachments": [{
            "color": sev_color,
            "title": title,
            "text": body[:1500],
            "fields": fields,
            "footer": "JustHodl alert-router",
            "ts": int(time.time()),
        }],
    }


def format_discord_payload(alert):
    """Discord webhook embed format."""
    color_int = {"HIGH": 16718410, "MEDIUM": 16762880, "LOW": 54271}.get(alert.get("severity"), 7301513)
    sev_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🔵"}.get(alert.get("severity"), "⚪")
    title = f"{sev_icon} {alert.get('title') or alert.get('check') or alert.get('id', 'Alert')}"
    body = alert.get("message") or alert.get("body") or ""
    fields = []
    for k in ("source", "metric", "value", "threshold", "check"):
        if k in alert and alert[k] is not None:
            fields.append({"name": k, "value": str(alert[k])[:200], "inline": True})
    return {
        "username": "JustHodl",
        "embeds": [{
            "title": title[:240],
            "description": body[:2000],
            "color": color_int,
            "fields": fields[:10],
            "footer": {"text": "alert-router · justhodl.ai"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }],
    }


def format_generic_payload(alert):
    """Plain JSON envelope — for custom HTTP receivers, Zapier, n8n, etc."""
    return {
        "service": "justhodl-alert-router",
        "version": "1.1",
        "alert": alert,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def send_webhook(target: dict, alert: dict):
    """POST a single alert to one webhook target. Returns (ok, info)."""
    url = target["url"]
    typ = target.get("type", "generic")
    if typ == "slack":
        payload = format_slack_payload(alert)
    elif typ == "discord":
        payload = format_discord_payload(alert)
    else:
        payload = format_generic_payload(alert)
    try:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": "application/json", "User-Agent": "justhodl-alert-router/1.1"},
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            return True, f"{resp.status} {resp.read()[:160].decode(errors='replace')}"
    except Exception as e:
        return False, str(e)[:200]


def dispatch_to_webhooks(alert: dict, targets: list):
    """Send alert to all webhook targets that pass severity filter.
    Returns list of {url_host, type, ok, info} for the alert record.
    """
    results = []
    for t in targets:
        if not _passes_severity(alert.get("severity"), t.get("min_severity")):
            continue
        ok, info = send_webhook(t, alert)
        # Mask the URL — keep only the host so secrets don't leak in S3 history
        try:
            from urllib.parse import urlparse
            host = urlparse(t["url"]).netloc
        except Exception:
            host = "unknown"
        results.append({"host": host, "type": t.get("type"), "ok": ok, "info": info})
    return results


def load_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default if default is not None else {}


def write_json(key, data):
    body = json.dumps(data, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json", CacheControl="public, max-age=60")


def is_recent_dup(state, alert_id, hours=DEDUP_HOURS):
    last = state.get(alert_id)
    if not last:
        return False
    try:
        last_ts = datetime.fromisoformat(last.replace("Z", "+00:00"))
    except Exception:
        return False
    return (datetime.now(timezone.utc) - last_ts).total_seconds() < hours * 3600


def check_correlation_surface(alerts):
    d = load_json("data/correlation-surface.json")
    breaks = d.get("regime_breaks", [])
    for b in breaks[:5]:
        ta, tb = b.get("ticker_a"), b.get("ticker_b")
        delta = b.get("delta_30d_vs_90d")
        if delta is None or abs(delta) < 0.30:
            continue
        alerts.append({
            "id": f"corr_break_{ta}_{tb}",
            "category": "CORRELATION",
            "severity": "HIGH",
            "title": f"🔁 Correlation regime break: {ta}↔{tb}",
            "detail": f"30d vs 90d Δ = {delta:+.2f} ({b.get('name_a')} vs {b.get('name_b')}). 30d corr = {b.get('corr_30d')}, 90d = {b.get('corr_90d')}.",
        })


def check_macro_surprise(alerts):
    d = load_json("data/macro-surprise.json")
    composite = d.get("composite") or d.get("composite_z")
    regime = d.get("regime")
    if composite is None:
        return
    if abs(composite) >= 2.0:
        sign = "+" if composite > 0 else ""
        alerts.append({
            "id": f"macro_extreme_{regime}",
            "category": "MACRO",
            "severity": "HIGH" if abs(composite) >= 3 else "MEDIUM",
            "title": f"📊 Macro composite extreme: {sign}{composite:.2f}σ",
            "detail": f"Regime: {regime}. {d.get('regime_description', '')}",
        })


def check_yield_curve(alerts):
    d = load_json("data/yield-curve.json")
    slope = d.get("slope_2s10s")
    butterfly = d.get("butterfly_2s5s10s")
    regime = d.get("regime")
    if slope is None:
        return
    # Inversion threshold
    if slope < 0 and slope > -50:
        alerts.append({
            "id": f"yc_inverted_{round(slope)}",
            "category": "RATES",
            "severity": "MEDIUM",
            "title": "📉 2s10s curve inverted",
            "detail": f"2s10s = {slope:+.1f} bps. Regime: {regime}. Historical recession pre-signal.",
        })
    elif slope > 100:
        alerts.append({
            "id": "yc_steep",
            "category": "RATES",
            "severity": "LOW",
            "title": "📈 2s10s curve steepening",
            "detail": f"2s10s = {slope:+.1f} bps. Regime: {regime}.",
        })


def check_eurodollar_stress(alerts):
    d = load_json("data/eurodollar-stress.json")
    composite = d.get("composite_stress_score") or d.get("composite_score")
    regime = d.get("regime")
    if composite is None:
        return
    if composite >= 70:
        alerts.append({
            "id": f"ed_stress_high",
            "category": "DOLLAR",
            "severity": "HIGH",
            "title": f"💵 Eurodollar stress elevated: {composite}/100",
            "detail": f"Regime: {regime}. {d.get('regime_description', '')}",
        })


def check_auction_crisis(alerts):
    d = load_json("data/auction-crisis.json")
    score = d.get("composite_score") or d.get("crisis_score")
    regime = d.get("regime")
    if score is not None and score >= 60:
        alerts.append({
            "id": "auction_crisis",
            "category": "TREASURY",
            "severity": "HIGH",
            "title": f"🏛️ Treasury auction stress: {score}/100",
            "detail": f"Regime: {regime}. {d.get('regime_description', '')}",
        })


def check_tenor_signals(alerts):
    """Tenor-specific Treasury signals: 2y (Fed path), 1m/3m (eurodollar), 30y (QE).
       Fires on any state transition WATCH/FIRING/EXTREME."""
    d = load_json("data/auction-tenor-signals.json")
    if not d:
        return
    transitions = d.get("transitions", []) or []
    sev_map = {"WATCH": "MEDIUM", "FIRING": "HIGH", "EXTREME": "HIGH", "OFF": "LOW"}
    icon_map = {"fed_path": "📊", "eurodollar": "💵", "qe_imminence": "🏦"}

    for t in transitions:
        new_state = t.get("new_state")
        if new_state in ("OFF",):
            # Healing transitions get a low-severity informational alert
            alerts.append({
                "id": f"tenor_{t['channel']}_clear",
                "category": "TREASURY",
                "severity": "LOW",
                "title": f"{icon_map.get(t['channel'], '📡')} {t.get('label')} → CLEARED",
                "detail": f"State: {t['prior_state']} → {new_state}. {t.get('interpretation','')}",
            })
            continue
        alerts.append({
            "id": f"tenor_{t['channel']}_{new_state.lower()}",
            "category": "TREASURY",
            "severity": sev_map.get(new_state, "MEDIUM"),
            "title": f"{icon_map.get(t['channel'], '📡')} {t.get('label')} → {new_state}",
            "detail": f"State: {t['prior_state']} → {new_state}. {t.get('interpretation','')}",
        })

    # Also fire a sticky alert if any channel currently EXTREME (even without transition)
    signals = d.get("signals", {}) or {}
    for ch_key, sig in signals.items():
        if sig.get("state") == "EXTREME":
            alerts.append({
                "id": f"tenor_{ch_key}_extreme_state",
                "category": "TREASURY",
                "severity": "HIGH",
                "title": f"🚨 {sig.get('label')} — EXTREME",
                "detail": sig.get("interpretation", "")[:280],
            })


def check_vix_curve(alerts):
    d = load_json("data/vix-curve.json")
    structure = d.get("term_structure") or d.get("vix_curve_state")
    if structure == "INVERTED" or structure == "BACKWARDATION":
        ratio = d.get("vix1m_vix3m_ratio") or d.get("vix_3m_1m_ratio")
        alerts.append({
            "id": "vix_inverted",
            "category": "VOLATILITY",
            "severity": "HIGH",
            "title": "📊 VIX term structure inverted",
            "detail": f"VIX1M/VIX3M = {ratio}. Historic stress signal.",
        })


def check_earnings(alerts):
    d = load_json("data/earnings-tracker.json")
    pead_signals = d.get("pead_signals", [])
    for s in pead_signals[:3]:
        signal_label = s.get("signal")
        if signal_label not in ("STRONG_POSITIVE_DRIFT", "STRONG_NEGATIVE_DRIFT"):
            continue
        ticker = s.get("ticker")
        ret_1d = s.get("price_return_1d_pct")
        eps_surp = s.get("eps_surprise_pct")
        emoji = "🚀" if signal_label == "STRONG_POSITIVE_DRIFT" else "💥"
        alerts.append({
            "id": f"pead_{ticker}_{signal_label}",
            "category": "EARNINGS",
            "severity": "MEDIUM",
            "title": f"{emoji} PEAD: {ticker} {signal_label}",
            "detail": f"EPS surprise {eps_surp:+.1f}%, 1d return {ret_1d:+.2f}%. Score {s.get('drift_score')}.",
        })


def check_short_interest(alerts):
    d = load_json("data/short-interest.json")
    squeeze = d.get("top_squeeze_risk", [])
    for s in squeeze[:3]:
        ticker = s.get("ticker")
        dtc = s.get("days_to_cover") or s.get("polygon_days_to_cover")
        if dtc is None or dtc < 8:
            continue
        alerts.append({
            "id": f"squeeze_{ticker}",
            "category": "SHORT_INTEREST",
            "severity": "MEDIUM",
            "title": f"🚨 Squeeze risk: {ticker}",
            "detail": f"Days-to-cover {dtc:.1f}, signal {s.get('signal_label') or s.get('label') or 'SQUEEZE_RISK'}.",
        })


def check_etf_flows(alerts):
    d = load_json("data/etf-flows.json")
    for cat, etfs in (d.get("by_category") or {}).items():
        for e in etfs:
            sig = e.get("signal")
            z = e.get("dollar_volume_z_60d")
            if sig in ("HEAVY_INFLOW", "HEAVY_OUTFLOW") and z is not None and abs(z) >= 2.5:
                ticker = e.get("ticker")
                emoji = "💚" if sig == "HEAVY_INFLOW" else "🔻"
                alerts.append({
                    "id": f"flow_{ticker}_{sig}",
                    "category": "ETF_FLOW",
                    "severity": "MEDIUM",
                    "title": f"{emoji} {ticker} {sig}: z={z:+.2f}",
                    "detail": f"{cat} category. ${e.get('dollar_volume_today',0):,.0f} today vs 60d distribution.",
                })


def check_sector_rotation(alerts):
    d = load_json("data/sector-rotation.json")
    breadth = d.get("market_breadth")
    if breadth in ("BROAD_LEADERSHIP", "NARROW_LEADERSHIP"):
        n_lead = len(d.get("leaders", []))
        n_lag = len(d.get("laggards", []))
        alerts.append({
            "id": f"sector_breadth_{breadth}",
            "category": "SECTOR",
            "severity": "LOW",
            "title": f"📊 Market breadth: {breadth}",
            "detail": f"Leaders: {n_lead}, Laggards: {n_lag}. {d.get('market_breadth_description', '')}",
        })


def check_divergences(alerts):
    d = load_json("data/divergence-current.json") or load_json("divergence/current.json")
    if not d:
        return
    divergences = d.get("divergences", []) or d.get("active_divergences", [])
    for div in divergences[:3]:
        z = div.get("residual_z") or div.get("z_score")
        if z is None or abs(z) < 2.5:
            continue
        pair = div.get("pair") or f"{div.get('asset_y')} vs {div.get('asset_x')}"
        alerts.append({
            "id": f"divergence_{pair}_{round(z,1)}",
            "category": "DIVERGENCE",
            "severity": "MEDIUM",
            "title": f"📈 Divergence: {pair}",
            "detail": f"Residual z = {z:+.2f}. Mean-reversion candidate.",
        })


def check_cot_extremes(alerts):
    d = load_json("data/cot-extremes.json") or load_json("cot/extremes.json")
    extremes = (d.get("extremes") or []) if d else []
    for e in extremes[:3]:
        pct = e.get("percentile_rank") or e.get("pct_rank")
        if pct is None:
            continue
        contract = e.get("contract") or e.get("name")
        if pct >= 95:
            alerts.append({
                "id": f"cot_long_{contract}",
                "category": "COT",
                "severity": "MEDIUM",
                "title": f"📍 COT extreme LONG: {contract}",
                "detail": f"Percentile rank {pct}% (5y window). Net spec position is at extreme high.",
            })
        elif pct <= 5:
            alerts.append({
                "id": f"cot_short_{contract}",
                "category": "COT",
                "severity": "MEDIUM",
                "title": f"📍 COT extreme SHORT: {contract}",
                "detail": f"Percentile rank {pct}% (5y window). Net spec position is at extreme low.",
            })


def format_telegram_msg(alert):
    sev_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🔵"}.get(alert["severity"], "⚪")
    return (
        f"{sev_icon} *JustHodl Alert — {alert['category']}*\n\n"
        f"*{alert['title']}*\n\n"
        f"{alert['detail']}\n\n"
        f"_{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}_"
    )


@track_errors
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[alert-router] start")

    state = load_json(STATE_KEY)
    history = load_json(HISTORY_KEY, default={"version": "1.0", "alerts": []})
    if not isinstance(history, dict):
        history = {"version": "1.0", "alerts": []}

    chat_id = get_chat_id()

    # Run every check (each appends to alerts list)
    alerts = []
    checks = [
        ("correlation_surface", check_correlation_surface),
        ("macro_surprise", check_macro_surprise),
        ("yield_curve", check_yield_curve),
        ("eurodollar_stress", check_eurodollar_stress),
        ("auction_crisis", check_auction_crisis),
        ("tenor_signals", check_tenor_signals),
        ("vix_curve", check_vix_curve),
        ("earnings", check_earnings),
        ("short_interest", check_short_interest),
        ("etf_flows", check_etf_flows),
        ("sector_rotation", check_sector_rotation),
        ("divergences", check_divergences),
        ("cot_extremes", check_cot_extremes),
    ]
    for name, fn in checks:
        try:
            fn(alerts)
        except Exception as e:
            print(f"[check.{name}] error: {e}")

    print(f"[alert-router] {len(alerts)} candidate alerts before dedup")

    # Load webhook targets once (may be empty list = no webhooks configured)
    webhook_targets = get_webhook_urls()
    if webhook_targets:
        print(f"[alert-router] {len(webhook_targets)} webhook target(s) configured")

    # Dedup + send
    sent = []
    suppressed = []
    n_webhook_attempts = 0
    n_webhook_ok = 0
    n_wss_attempts = 0
    n_wss_ok = 0
    for a in alerts:
        aid = a["id"]
        if is_recent_dup(state, aid):
            suppressed.append(a)
            continue
        # Telegram (preserved)
        msg = format_telegram_msg(a)
        ok, info = send_telegram(msg, chat_id) if chat_id else (False, "no_chat_id")
        a["sent_at"] = datetime.now(timezone.utc).isoformat()
        a["telegram_sent"] = ok
        a["telegram_info"] = info[:200] if info else None

        # Webhooks (in addition to Telegram, severity-filtered)
        if webhook_targets:
            wh_results = dispatch_to_webhooks(a, webhook_targets)
            a["webhook_results"] = wh_results
            n_webhook_attempts += len(wh_results)
            n_webhook_ok += sum(1 for r in wh_results if r["ok"])

        # WebSocket broadcast — fan out to all subscribers of 'alerts' channel
        wss_ok, wss_info = broadcast_alert_to_wss(a)
        a["wss_broadcast_sent"] = wss_ok
        a["wss_broadcast_info"] = wss_info
        n_wss_attempts += 1
        if wss_ok:
            n_wss_ok += 1

        sent.append(a)
        state[aid] = a["sent_at"]
        time.sleep(0.5)  # rate limit

    # Update history
    history["alerts"] = (history.get("alerts", []) + sent)[-100:]
    history["last_run"] = datetime.now(timezone.utc).isoformat()
    history["last_run_summary"] = {
        "candidates": len(alerts),
        "sent": len(sent),
        "suppressed": len(suppressed),
        "webhooks_configured": len(webhook_targets),
        "webhook_attempts": n_webhook_attempts,
        "webhook_ok": n_webhook_ok,
        "wss_attempts": n_wss_attempts,
        "wss_ok": n_wss_ok,
    }

    write_json(HISTORY_KEY, history)
    write_json(STATE_KEY, state)

    print(f"[alert-router] sent={len(sent)} suppressed={len(suppressed)} "
          f"webhooks={n_webhook_ok}/{n_webhook_attempts} "
          f"duration={time.time()-started:.1f}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "candidates": len(alerts),
            "sent": len(sent),
            "suppressed": len(suppressed),
            "webhooks_configured": len(webhook_targets),
            "webhook_attempts": n_webhook_attempts,
            "webhook_ok": n_webhook_ok,
            "duration_s": round(time.time() - started, 2),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
