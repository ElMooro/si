"""
justhodl-alert-sentinel v1.0 — Finviz Module 6 (Alerts Engine)
==============================================================
Stateful diff watcher. Each run snapshots the desk's actionable signals,
compares to the prior snapshot, and pushes a Telegram message ONLY when
something changed. First run seeds state and sends a boot line.

Watched (each extracted defensively; misses land in DIAG, never crash):
  • upside breakouts            (data/upside-radar.json)
  • insider decline clusters    (data/insider-radar.json)
  • rotation thrust flips        (data/rotation-radar.json equity.ratios.*.live.thrust_live)
  • rotation live extremes       (semis off-low, smallcap vs 365d-high)
  • S&P breadth regime day       (data/market-map.json — |cap-wtd| >= 1.5%)
  • altseason phase change       (data/altseason.json)
  • crisis-canary red flips     (data/crisis-canaries.json v3)
  • sizing top recommendation    (data/sizing.json)
  • 200DMA reclaim/break          (data/ma-reversion.json if present)
"""
import json, os, time, urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
STATE_KEY = "data/_alerts/last.json"
OUT_KEY = "data/alert-sentinel.json"
TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT", "")
VERSION = "1.3.0"
DIAG = []


def gj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        DIAG.append(f"{key}: {str(e)[:50]}")
        return {}


def tickers_from(obj, want_key_substr=("breakout",)):
    """Probe common shapes for a set of tickers under keys matching substrings."""
    found = set()
    def grab(arr):
        for it in arr:
            if isinstance(it, dict):
                t = it.get("ticker") or it.get("symbol") or it.get("t")
                if t:
                    found.add(str(t).upper())
            elif isinstance(it, str):
                found.add(it.upper())
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = k.lower()
            if any(s in kl for s in want_key_substr):
                if isinstance(v, list):
                    grab(v)
                elif isinstance(v, dict):
                    for vv in v.values():
                        if isinstance(vv, list):
                            grab(vv)
        # nested 'scans'/'tiers' container
        for cont in ("scans", "tiers", "signals", "lists"):
            c = obj.get(cont)
            if isinstance(c, dict):
                for k, v in c.items():
                    if any(s in k.lower() for s in want_key_substr) and isinstance(v, list):
                        grab(v)
    return sorted(found)


def snapshot():
    s = {}
    up = gj("data/upside-radar.json")
    s["breakouts"] = tickers_from(up, ("breakout",))[:60]
    DIAG.append(f"breakouts: {len(s['breakouts'])}")

    ins = gj("data/insider-radar.json")
    s["insider_decline"] = sorted({c.get("ticker") for c in (ins.get("decline_clusters") or [])
                                     if c.get("ticker")})
    s["insider_src"] = ins.get("source_used")

    rot = gj("data/rotation-radar.json")
    ratios = (rot.get("equity") or {}).get("ratios") or {}
    s["thrusts"] = {k: bool((v.get("live") or {}).get("thrust_live"))
                     for k, v in ratios.items()}
    sm = (ratios.get("semis_mkt") or {}).get("live") or {}
    scl = (ratios.get("smallcap_large") or {}).get("live") or {}
    s["semis_off_low"] = sm.get("off_180d_low_pct")
    s["smallcap_vs_high"] = scl.get("vs_365d_high_pct")

    mp = gj("data/market-map.json")
    cap = (mp.get("breadth") or {}).get("mcap_weighted_chg")
    s["breadth_capwtd"] = cap
    s["breadth_regime_day"] = bool(cap is not None and abs(cap) >= 1.5)

    alt = gj("data/altseason.json")
    s["altseason_phase"] = ((alt.get("composite") or {}).get("phase")
                             or alt.get("phase") or alt.get("verdict"))

    mp2 = gj("data/market-map.json")
    cands = (mp2.get("cheap_candidates") or [])[:10]
    s["value_pump"] = [c.get("t") for c in cands if c.get("t")]
    s["_vp_detail"] = {c["t"]: (f"P/S {c.get('ps')} vs sector {c.get('sector_ps_median')} "
                                  f"({c.get('discount_pct')}% disc), 3M {c.get('m3')}%")
                        for c in cands if c.get("t")}

    sv = gj("data/stock-valuations.json")
    ser = [x for x in (sv.get("hp") or []) if (x.get("score") or 0) >= 75 and not x.get("flags")]
    s["hp_serious"] = sorted(x.get("t") for x in ser if x.get("t"))[:12]
    s["_hp_detail"] = {x["t"]: f"HP {x.get('score')}/100" for x in ser if x.get("t")}

    cc = gj("data/crisis-canaries.json")
    s["canary_reds"] = sorted(k for k, v in (cc.get("canaries") or {}).items()
                                if isinstance(v, dict) and v.get("family")
                                and v.get("status") == "RED")
    s["canary_level_v3"] = cc.get("level_v3") or cc.get("level")
    s["canary_v3"] = cc.get("composite_v3")
    s["_canary_names"] = {k: (v.get("name"), (v.get("detail") or "")[:90])
                           for k, v in (cc.get("canaries") or {}).items()
                           if isinstance(v, dict) and v.get("family")
                           and v.get("status") == "RED"}

    sz = gj("data/sizing.json")
    recs = sz.get("recommendations") or sz.get("sizes") or sz.get("positions") or []
    top = recs[0] if isinstance(recs, list) and recs else {}
    s["sizing_top"] = (top.get("ticker") or top.get("symbol")) if isinstance(top, dict) else None
    s["sizing_gross"] = (sz.get("gross_recommended_w_pct") or sz.get("gross_pct")
                          or sz.get("gross"))

    ma = gj("data/ma-reversion.json")
    if ma:
        s["ma_reclaim_200"] = tickers_from(ma, ("reclaim", "cross_up", "above"))[:40]
        s["ma_break_200"] = tickers_from(ma, ("break", "cross_down", "below"))[:40]
    return s


def diff(old, new):
    msgs = []
    def newset(k, label, emoji):
        o, n = set(old.get(k) or []), set(new.get(k) or [])
        add = sorted(n - o)
        if add:
            msgs.append(f"{emoji} {label}: {', '.join(add[:12])}" + (" …" if len(add) > 12 else ""))
    # 🏆 HP-Score: new >=75 clean names ("worth serious research")
    oh, nh = set(old.get("hp_serious") or []), set(new.get("hp_serious") or [])
    for k in sorted(nh - oh):
        msgs.append(f"🏆 HP-Score serious — {k}: {(new.get('_hp_detail') or {}).get(k, '')}")
    # 💎 value-pump: new sector-cheap P/S + momentum candidates
    ov, nv = set(old.get("value_pump") or []), set(new.get("value_pump") or [])
    for k in sorted(nv - ov):
        msgs.append(f"💎 New value-momentum candidate — {k}: "
                     f"{(new.get('_vp_detail') or {}).get(k, '')}")
    # crisis canaries: name each newly-red canary with its detail
    oc, nc = set(old.get("canary_reds") or []), set(new.get("canary_reds") or [])
    for k in sorted(nc - oc):
        nm, det = (new.get("_canary_names") or {}).get(k, (k, ""))
        msgs.append(f"🐤 NEW RED canary — {nm}: {det}")
    if (new.get("canary_level_v3") and old.get("canary_level_v3")
            and new["canary_level_v3"] != old["canary_level_v3"]):
        msgs.append(f"🚨 Crisis composite → {new['canary_level_v3']} "
                     f"({new.get('canary_v3')}) from {old['canary_level_v3']}")
    newset("insider_decline", "NEW insider cluster after decline", "🕵")
    newset("breakouts", "New breakouts", "🚀")
    newset("ma_reclaim_200", "Reclaimed 200DMA", "📈")
    newset("ma_break_200", "Lost 200DMA", "📉")
    # thrust flips false→true
    ot, nt = old.get("thrusts") or {}, new.get("thrusts") or {}
    flips = [k for k, v in nt.items() if v and not ot.get(k)]
    if flips:
        msgs.append("⚡ Rotation thrust FIRED: " + ", ".join(flips))
    offs = [k for k, v in nt.items() if (not v) and ot.get(k)]
    if offs:
        msgs.append("• Rotation thrust faded: " + ", ".join(offs))
    # breadth regime day
    if new.get("breadth_regime_day") and not old.get("breadth_regime_day"):
        msgs.append(f"🌡 Breadth regime day: S&P cap-wtd {new.get('breadth_capwtd')}%")
    # altseason phase change
    if new.get("altseason_phase") and new.get("altseason_phase") != old.get("altseason_phase"):
        msgs.append(f"🪙 Altseason phase → {new.get('altseason_phase')} (was {old.get('altseason_phase')})")
    # sizing top change
    if new.get("sizing_top") and new.get("sizing_top") != old.get("sizing_top"):
        msgs.append(f"📐 Sizing top rec → {new.get('sizing_top')} (gross {new.get('sizing_gross')})")
    return msgs


def telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        DIAG.append("telegram: creds missing")
        return False
    try:
        data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": text,
                                        "disable_web_page_preview": "true"}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                                      data=data)
        j = json.loads(urllib.request.urlopen(req, timeout=20).read())
        return bool(j.get("ok"))
    except Exception as e:
        DIAG.append(f"telegram: {str(e)[:60]}")
        return False


def lambda_handler(event=None, context=None):
    DIAG.clear()
    new = snapshot()
    try:
        old = json.loads(S3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
        first = False
    except Exception:
        old, first = {}, True
    sent = False
    if first:
        n_watch = (len(new.get("breakouts") or []) + len(new.get("thrusts") or {})
                    + (1 if new.get("breadth_capwtd") is not None else 0))
        sent = telegram(f"🛰 JustHodl Alert Sentinel online — watching breakouts, insider "
                         f"clusters, rotation thrusts, breadth, altseason, sizing. "
                         f"Seed: {len(new.get('breakouts') or [])} breakouts, "
                         f"{sum(new.get('thrusts',{}).values())} live thrusts, "
                         f"breadth {new.get('breadth_capwtd')}%.")
        changes = ["(seed)"]
    else:
        changes = diff(old, new)
        if changes:
            sent = telegram("🛰 JustHodl Alerts — " +
                             datetime.now(timezone.utc).strftime("%b %d %H:%M UTC") + "\n\n"
                             + "\n".join(changes))
    # queue-until-delivered: if real changes failed to send, DON'T advance state —
    # the same diff (plus anything new) retries next run and delivers as one message.
    if first or sent or not changes:
        S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                      Body=json.dumps(new, default=str).encode(),
                      ContentType="application/json")
        state_saved = True
    else:
        state_saved = False
        DIAG.append("state NOT advanced — alert queued for retry")
    out = {"engine": "alert-sentinel", "version": VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "first_run": first, "message_sent": sent, "state_saved": state_saved,
            "n_changes": len(changes),
            "changes": changes, "snapshot": new, "diagnostics": list(DIAG)}
    clean = json.loads(json.dumps(out, default=str), parse_constant=lambda c: None)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(clean).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print(f"[sentinel] first={first} sent={sent} changes={len(changes)}")
    return {"statusCode": 200, "body": json.dumps({"sent": sent, "changes": len(changes)})}
