"""
justhodl-smart-wake v1.0 — volatility-gated scheduling.

Cost hibernation (2026-06) disabled ~62 EventBridge rules to cut invocations
~70%. That trade permanently darkened intraday alpha feeds (options-flow,
short-interest, ETF/exchange flows, tape, vol stack). This engine makes the
trade dynamic:

  WAKE  when stress composites breach → enable the curated alpha rules
  SLEEP when calm has persisted ≥ CALM_HOURS → disable ONLY rules this
        engine woke (never touches anything else)

Stress inputs (all already produced by the platform):
  • ESI (eurodollar stress index)  data/ecb-derived.json
  • ECB n_flashing                  data/ecb-derived.json
  • VIX spot / regime               data/vix-curve.json (defensive parse)
  • Auction crisis composite        data/auction-crisis.json (defensive)

State: data/smart-wake.json   Telegram on every transition.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3")
EV = boto3.client("events")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
STATE_KEY = "data/smart-wake.json"

# rules this engine is allowed to manage (prefix match, DISABLED-only)
WAKE_PREFIXES = [
    "justhodl-options-flow-30m",
    "justhodl-short-interest-6h",
    "justhodl-etf-flows-6h",
    "justhodl-exchange-flows-6h",
    "justhodl-narrative-vs-tape-4h",
    "event-flow-monitor-hourly",
    "justhodl-vix-backwardation",
    "justhodl-vol-target-unwind",
    "justhodl-vol-surface",
    "justhodl-opex-calendar",
    "justhodl-tape-reader",
    "justhodl-dealer-gex",
]

WAKE_ESI = 50          # eurodollar stress 0-100
WAKE_FLASHING = 3      # ECB dump signals active
WAKE_VIX = 25.0        # spot VIX
CALM_HOURS = 24        # sustained calm before re-hibernating

TG_TOKEN = os.environ.get("TG_TOKEN", "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs")
TG_CHAT = os.environ.get("TG_CHAT", "8678089260")


def _rd(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _tg(msg):
    try:
        data = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def _stress():
    s = {"esi": None, "n_flashing": None, "vix": None, "auction": None}
    ecb = _rd("data/ecb-derived.json")
    esi = ((ecb.get("indicators") or {}).get("eurodollar_stress_index") or {})
    s["esi"] = esi.get("esi_0_100")
    s["n_flashing"] = ecb.get("n_flashing")
    vx = _rd("data/vix-curve.json")
    cur = vx.get("current") if isinstance(vx.get("current"), dict) else {}
    for k in ("vix_spot", "spot", "vix", "vix_level"):
        v = cur.get(k) or vx.get(k)
        if isinstance(v, (int, float)):
            s["vix"] = round(float(v), 1); break
    ac = _rd("data/auction-crisis.json")
    for k in ("composite_score", "score", "composite"):
        v = ac.get(k)
        if isinstance(v, (int, float)):
            s["auction"] = round(float(v), 1); break
    return s


def _breach(s):
    reasons = []
    if (s["esi"] or 0) >= WAKE_ESI:
        reasons.append(f"ESI {s['esi']}≥{WAKE_ESI}")
    if (s["n_flashing"] or 0) >= WAKE_FLASHING:
        reasons.append(f"{s['n_flashing']} ECB signals flashing")
    if (s["vix"] or 0) >= WAKE_VIX:
        reasons.append(f"VIX {s['vix']}≥{WAKE_VIX}")
    if (s["auction"] or 0) >= 60:
        reasons.append(f"auction-crisis {s['auction']}≥60")
    return reasons


def _managed_rules():
    """All rules matching WAKE_PREFIXES with current state."""
    rules = []
    tok = None
    while True:
        kw = {"Limit": 100}
        if tok:
            kw["NextToken"] = tok
        r = EV.list_rules(**kw)
        rules += r.get("Rules", [])
        tok = r.get("NextToken")
        if not tok:
            break
    return [r for r in rules if any(r["Name"].startswith(p) for p in WAKE_PREFIXES)]


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    st = _rd(STATE_KEY) or {}
    mode = st.get("mode", "SLEEPING")
    woken = st.get("woken_rules", [])
    calm_since = st.get("calm_since")

    s = _stress()
    reasons = _breach(s)
    actions, errs = [], []

    if reasons and mode != "AWAKE":
        # WAKE: enable disabled managed rules
        for r in _managed_rules():
            if r["State"] == "DISABLED":
                try:
                    EV.enable_rule(Name=r["Name"])
                    woken.append(r["Name"]); actions.append(f"enabled {r['Name']}")
                except Exception as e:
                    errs.append(f"{r['Name']}: {str(e)[:60]}")
        mode, calm_since = "AWAKE", None
        _tg(f"⚡ <b>SMART-WAKE</b> → intraday alpha feeds ON\n{'; '.join(reasons)}\n{len(actions)} rules enabled")
    elif reasons and mode == "AWAKE":
        calm_since = None  # still stressed; reset calm timer
    elif not reasons and mode == "AWAKE":
        if not calm_since:
            calm_since = now.isoformat()
        else:
            hrs = (now - datetime.fromisoformat(calm_since)).total_seconds() / 3600
            if hrs >= CALM_HOURS:
                for name in sorted(set(woken)):
                    try:
                        EV.disable_rule(Name=name); actions.append(f"disabled {name}")
                    except Exception as e:
                        errs.append(f"{name}: {str(e)[:60]}")
                _tg(f"😴 <b>SMART-WAKE</b> → calm {int(hrs)}h, re-hibernating {len(set(woken))} rules")
                woken, mode, calm_since = [], "SLEEPING", None

    out = {"engine": "smart-wake", "version": "1.0", "ts": now.isoformat(),
           "mode": mode, "stress": s, "breach_reasons": reasons,
           "woken_rules": sorted(set(woken)), "calm_since": calm_since,
           "actions": actions, "errors": errs,
           "thresholds": {"esi": WAKE_ESI, "n_flashing": WAKE_FLASHING, "vix": WAKE_VIX, "calm_hours": CALM_HOURS},
           "read": (f"Mode {mode}. " + ("; ".join(reasons) if reasons else "All stress gauges calm.")
                    + (f" {len(set(woken))} intraday rules awake." if woken else ""))}
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=300")
    print(f"[smart-wake] mode={mode} reasons={reasons} actions={len(actions)} errs={len(errs)}")
    return {"statusCode": 200, "body": json.dumps({"mode": mode, "actions": len(actions), "errors": errs[:3]})}
