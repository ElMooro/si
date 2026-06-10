"""
justhodl-apex-fusion v1.0 — learned cross-engine pump conviction.

Audit 1528/1529: five pump-relevant engines run fresh but in silos
(pump-positioning, momentum-leaders, microcap-float-squeeze, options-flow,
insider-clusters), the scorecard already computes per-signal
performance_multiplier, and cascade validation shows ALERT_TIER inverted
(0% hit). This engine is the fusion layer:

  1. Union tickers across the five live briefs
  2. Component scores 0-100 per source
  3. Weights = base × scorecard performance_multiplier (learned, capped)
  4. Confluence kicker for multi-source agreement
  5. Tier-inversion guard: if cascade ALERT_TIER validates <30%, its
     tickers get a FADE_RISK flag instead of a boost
  6. LIFTOFF / IGNITION picks logged to DDB justhodl-signals →
     the existing closed loop grades apex-fusion itself

Output: data/apex-fusion.json · Telegram top picks · DDB predictions.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone, timedelta

import boto3

S3 = boto3.client("s3")
DDB = boto3.client("dynamodb")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
TABLE = os.environ.get("SIGNALS_TABLE", "justhodl-signals")
TG_TOKEN = os.environ.get("TG_TOKEN", "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs")
TG_CHAT = os.environ.get("TG_CHAT", "8678089260")

BASE_W = {"pump": 1.2, "momentum": 1.0, "squeeze": 1.0, "flow": 1.0, "insider": 0.9}
SCORECARD_TOKENS = {"pump": ("pump", "convergence"), "momentum": ("momentum",),
                    "squeeze": ("squeeze", "short"), "flow": ("options", "flow"),
                    "insider": ("insider",)}


def _rd(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _tg(msg):
    try:
        data = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data,
            headers={"Content-Type": "application/json"}), timeout=10)
    except Exception:
        pass


def learned_weights():
    """base × scorecard performance_multiplier (PROMOTED, n≥20), capped 0.5–1.5."""
    sc = _rd("data/signal-scorecard.json")
    rows = sc.get("scorecard") or []
    w, src = dict(BASE_W), {}
    for source, toks in SCORECARD_TOKENS.items():
        best = None
        for r in rows:
            st = str(r.get("signal_type", "")).lower()
            if any(t in st for t in toks) and r.get("status") == "PROMOTED" and (r.get("n_scored") or 0) >= 20:
                m = r.get("performance_multiplier")
                if isinstance(m, (int, float)) and (best is None or r["n_scored"] > best[1]):
                    best = (m, r["n_scored"], r.get("signal_type"))
        if best:
            w[source] = round(BASE_W[source] * max(0.5, min(1.5, best[0])), 3)
            src[source] = {"signal_type": best[2], "multiplier": best[0], "n": best[1]}
    return w, src


def tier_inversion():
    """If cascade ALERT_TIER validates <30% (n≥10) → return its fade set."""
    vl = _rd("data/cascade-validation-log.json")
    st = (vl.get("by_tier_stats") or {}).get("ALERT_TIER") or {}
    hr = st.get("hit_rate_pct")
    if (st.get("n") or 0) >= 10 and hr is not None and hr < 30:
        tc = _rd("data/theme-cascade-calibrated.json")
        fades = {str(c.get("ticker", "")).upper() for c in (tc.get("alert_tier") or []) if c.get("ticker")}
        return {"active": True, "alert_tier_hit_pct": st.get("hit_rate_pct"),
                "n_validated": st.get("n"), "fade_set": sorted(fades)[:40]}
    return {"active": False, "alert_tier_hit_pct": st.get("hit_rate_pct"), "n_validated": st.get("n")}


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    W, w_src = learned_weights()
    inv = tier_inversion()
    book = {}   # ticker → {components:{}, price, evidence:{}}

    def slot(tk, price=None):
        tk = str(tk).upper().strip()
        if not tk or len(tk) > 6:
            return None
        e = book.setdefault(tk, {"components": {}, "price": None, "evidence": {}})
        if price and not e["price"]:
            e["price"] = price
        return e

    # 1) pump-positioning
    pp = _rd("data/pump-positioning.json")
    for c in (pp.get("candidates") or []):
        e = slot(c.get("ticker"))
        if e is None:
            continue
        s = c.get("directional_score") or c.get("convergence_score")
        if isinstance(s, (int, float)):
            e["components"]["pump"] = min(100, max(0, s))
            e["evidence"]["pump"] = f"{c.get('n_engines', 0)} engines · conv {c.get('convergence_score')}"

    # 2) momentum-leaders
    ml = _rd("data/momentum-leaders.json")
    for r in (ml.get("all_scored") or []):
        e = slot(r.get("ticker"), r.get("current_price"))
        if e is None:
            continue
        s = r.get("momentum_score")
        if isinstance(s, (int, float)):
            e["components"]["momentum"] = min(100, max(0, s))
            tg = ",".join((r.get("tags") or [])[:3])
            e["evidence"]["momentum"] = f"20d {r.get('perf_20d_pct')}% · vsurge {r.get('volume_surge')} · {tg}"

    # 3) squeeze
    sq = _rd("data/microcap-float-squeeze.json")
    for r in (sq.get("all_qualifying") or []):
        m = r.get("metrics") or {}
        e = slot(r.get("symbol"), m.get("price"))
        if e is None:
            continue
        s = r.get("score")
        if isinstance(s, (int, float)):
            e["components"]["squeeze"] = min(100, max(0, s))
            e["evidence"]["squeeze"] = f"short {m.get('short_pct_recent')}% · {r.get('tier', '')[:22]}"

    # 4) options flow
    of = _rd("data/options-flow.json")
    for r in (of.get("all_qualifying") or []):
        m = r.get("metrics") or {}
        e = slot(r.get("symbol"), m.get("spot"))
        if e is None:
            continue
        s = r.get("score")
        if isinstance(s, (int, float)):
            e["components"]["flow"] = min(100, max(0, s))
            e["evidence"]["flow"] = f"CPR +{m.get('cpr_change_pct')}% · callsurge {m.get('call_vol_surge')}"

    # 5) insider clusters
    ic = _rd("data/insider-clusters.json")
    for r in (ic.get("clusters") or []):
        e = slot(r.get("ticker"), r.get("avg_price"))
        if e is None:
            continue
        s = 30 + (r.get("n_insiders") or 0) * 6 + (15 if r.get("has_ceo") else 0) \
            + (10 if r.get("has_cfo") else 0) + min(25, (r.get("total_value") or 0) / 1e6)
        e["components"]["insider"] = min(100, round(s, 1))
        e["evidence"]["insider"] = f"{r.get('n_insiders')} insiders ${round((r.get('total_value') or 0)/1e6,1)}M · {r.get('highest_role','')[:18]}"

    # ── fuse ──
    rows = []
    for tk, e in book.items():
        comp = e["components"]
        if not comp:
            continue
        num = sum(W[k] * v for k, v in comp.items())
        den = sum(W[k] for k in comp)
        base = num / den
        n_src = len(comp)
        score = min(100, round(base * (1 + 0.08 * (n_src - 1)), 1))
        if n_src == 1:
            score = round(score * 0.78, 1)   # single-source haircut — confluence is the edge
        fade = inv["active"] and tk in set(inv.get("fade_set", []))
        tier = ("LIFTOFF" if score >= 78 and n_src >= 3 else
                "IGNITION" if score >= 62 and n_src >= 2 else
                "SIMMER" if score >= 48 else "NOISE")
        if fade and tier in ("LIFTOFF", "IGNITION"):
            tier, score = "FADE_RISK", round(score * 0.8, 1)
        rows.append({"ticker": tk, "apex_score": score, "tier": tier, "n_sources": n_src,
                     "sources": sorted(comp), "components": {k: round(v, 1) for k, v in comp.items()},
                     "evidence": e["evidence"], "price": e["price"],
                     "fade_flag": fade})
    rows.sort(key=lambda r: -r["apex_score"])
    by_tier = {}
    for r in rows:
        by_tier[r["tier"]] = by_tier.get(r["tier"], 0) + 1

    # ── DDB prediction logging — CANONICAL schema (v1.2) so outcome-checker
    # (status='pending' + check_timestamps + predicted_direction) and
    # signal-scorecard (signal_type) grade apex-fusion like every other engine ──
    regime_now = None
    try:
        rep = _rd("data/report.json")
        regime_now = rep.get("regime") or (rep.get("khalid_index") or {}).get("regime")
    except Exception:
        pass
    logged, log_errors = [], []
    windows = [3, 7, 14, 30]
    for r in rows[:12]:
        if r["tier"] not in ("LIFTOFF", "IGNITION") or not r["price"]:
            continue
        sid = f"apex-fusion#{r['ticker']}#{today}"
        ts = {f"day_{d}": (now + timedelta(days=d)).isoformat() for d in windows}
        try:
            item = {
                "signal_id": {"S": sid},
                "signal_type": {"S": "apex_fusion"},
                "signal_value": {"S": str(r["apex_score"])},
                "predicted_direction": {"S": "UP"},
                "confidence": {"N": str(round(r["apex_score"] / 100, 3))},
                "measure_against": {"S": r["ticker"]},
                "ticker": {"S": r["ticker"]},
                "source": {"S": "apex-fusion"},
                "baseline_price": {"N": str(r["price"])},
                "benchmark": {"NULL": True},
                "check_windows": {"L": [{"S": str(d)} for d in windows]},
                "check_timestamps": {"M": {k: {"S": v} for k, v in ts.items()}},
                "outcomes": {"M": {}},
                "accuracy_scores": {"M": {}},
                "logged_at": {"S": now.isoformat()},
                "logged_epoch": {"N": str(int(now.timestamp()))},
                "status": {"S": "pending"},
                "schema_version": {"S": "2"},
                "horizon_days_primary": {"N": "30"},
                "regime_at_log": ({"S": str(regime_now)} if regime_now else {"NULL": True}),
                "ttl": {"N": str(int(now.timestamp()) + 365 * 86400)},
                "metadata": {"S": json.dumps({"tier": r["tier"], "sources": r["sources"],
                                              "components": r["components"]})[:900]},
                "rationale": {"S": (f"Apex fusion {r['apex_score']} via {r['n_sources']} engines: "
                                    + ", ".join(r["sources"]))[:300]},
            }
            DDB.put_item(TableName=TABLE, Item=item,
                         ConditionExpression="attribute_not_exists(signal_id)")
            logged.append(sid)
        except Exception as ex:
            if "ConditionalCheckFailed" not in type(ex).__name__ + str(ex):
                log_errors.append(f"{r['ticker']}: {type(ex).__name__} {str(ex)[:70]}")

    top = rows[:25]
    lift = [r for r in top if r["tier"] == "LIFTOFF"][:5] or [r for r in top if r["tier"] == "IGNITION"][:3]
    if lift and event is not None and not (isinstance(event, dict) and event.get("no_tg")):
        lines = [f"• <b>{r['ticker']}</b> {r['apex_score']} ({r['n_sources']}src: {','.join(r['sources'])})" for r in lift]
        _tg("🚀 <b>APEX FUSION</b> — top conviction\n" + "\n".join(lines) +
            (f"\n⚠ tier-inversion active (ALERT_TIER {inv['alert_tier_hit_pct']}% hit)" if inv["active"] else ""))

    out = {"engine": "apex-fusion", "version": "1.2", "generated_at": now.isoformat(),
           "weights_used": W, "weight_sources": w_src, "tier_inversion": inv,
           "n_universe": len(book), "n_scored": len(rows), "by_tier": by_tier,
           "n_logged_to_ddb": len(logged), "log_errors": log_errors[:5], "top": top,
           "read": (f"{len(rows)} names fused across 5 engines; "
                    f"{by_tier.get('LIFTOFF', 0)} LIFTOFF / {by_tier.get('IGNITION', 0)} IGNITION. "
                    + (f"Tier-inversion guard ACTIVE — cascade alert tier validating at "
                       f"{inv['alert_tier_hit_pct']}%; its names are fade-flagged, not boosted. " if inv["active"] else "")
                    + f"Weights learned from scorecard: {W}."),
           "duration_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key="data/apex-fusion.json", Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print(f"[apex-fusion] n={len(rows)} liftoff={by_tier.get('LIFTOFF', 0)} logged={len(logged)} in {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"n": len(rows), "by_tier": by_tier, "logged": len(logged)})}
