"""
justhodl-sizing-engine v1.0 — Signal → Size (fractional Kelly, calibrated)
==========================================================================
Completes the loop: every live directional signal arrives with a position
size built from MEASURED edge, not vibes.

Chain per recommendation:
  1. ENGINE KELLY — from the closed loop's graded record (outcomes.day_N
     .correct + return_pct): p, median win W, median loss L → Kelly
     f* = p − (1−p)/(W/L); quarter-Kelly institutional floor/cap.
  2. CALIBRATED CONFIDENCE — claimed confidence × the calibrator's
     confidence_scale override (engines that overclaim get deflated by
     their own audit).
  3. VOL TARGETING — position shrinks toward a 30% annualized-vol budget.
  4. CLUSTER HAIRCUT — greedy correlation penalty vs higher-ranked accepted
     recs + SPY beta context (book-aware the day holdings exist; the book
     is read honestly either way).
Gates: n≥15 resolved or the engine sizes at starter-only; negative Kelly
publishes as NO-EDGE (fade list), never sized long.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Attr
from equity_donor_inputs import load_inputs, constrain_sizes, SIZING_SPECS
from capital_contract import capital_book_view
from public_brain_projection import sanitize_public
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
# riskgate-wire-v1 — brain-constitutional Master Risk Gate (Khalid 2026-07-26:
# macro gates SIZING before selection). data/risk-gate.json, 48h stale guard.
def _risk_gate_doc():
    # No warm-container permission cache. Zero and missing data must remain binding.
    try:
        d=json.loads(S3.get_object(Bucket="justhodl-dashboard-live",Key="data/risk-gate.json")["Body"].read())
        age=(datetime.now(timezone.utc)-datetime.fromisoformat(d.get("generated_at", "").replace("Z","+00:00"))).total_seconds()/3600
        if not 0<=age<=48:raise ValueError("expired gate")
        return d
    except Exception:return {"posture":"DATA_HOLD","sizing_multiplier":0.0}

_RG_RANK_CLAMP = {"RISK_ON": 1.05, "NEUTRAL": 1.0, "RISK_OFF": 0.88, "SEVERE": 0.80}

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/sizing.json"
POLY_KEY = managed_secret(('POLYGON_KEY', 'POLYGON_API_KEY', 'POLY_KEY'), ("/justhodl/polygon/api-key",))
VERSION = "1.0.1"
DIRMAP = {"UP": 1, "LONG": 1, "OUTPERFORM": 1, "BULLISH": 1,
           "DOWN": -1, "SHORT": -1, "UNDERPERFORM": -1, "BEARISH": -1}
VOL_TARGET = 0.30      # annualized
CAP_W = 5.0            # % of book per name
FLOOR_W = 0.25
STARTER_W = 0.5
LOSS_FLOOR_PCT = 0.75    # min assumed loss magnitude (cost+slippage reality)
PAYOFF_CAP = 6.0
KELLY_CAP = 0.30         # pre-quarter cap → max base 7.5%


def f(x):
    try:
        return float(x)
    except Exception:
        return None


def s3json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def poly_closes(t, days=140):
    from datetime import timedelta
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc) - __import__("datetime").timedelta(days=days)).date().isoformat()
    u = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=40).read())
        return [float(r["c"]) for r in (j.get("results") or [])]
    except Exception as e:
        print(f"[poly] {t}: {str(e)[:40]}")
        return []


def rets(c):
    return [c[i] / c[i - 1] - 1 for i in range(1, len(c))]


def corr(a, b):
    n = min(len(a), len(b))
    if n < 30:
        return None
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    va = sum((x - ma) ** 2 for x in a) ** 0.5
    vb = sum((x - mb) ** 2 for x in b) ** 0.5
    if not va or not vb:
        return None
    return sum((a[i] - ma) * (b[i] - mb) for i in range(n)) / (va * vb)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    donor_docs,donor_receipts=load_inputs(S3,BUCKET,SIZING_SPECS)
    cal = (s3json("data/_skill/calibration-config.json") or {}).get("engine_overrides", {})

    # ── A) full graded scan → per-engine Kelly table ──
    t = DDB.Table("justhodl-signals")
    agg = {}
    lek = None
    scanned = 0
    while True:
        kw = {"ProjectionExpression": "#s, signal_type, predicted_direction, "
                                        "horizon_days_primary, outcomes, confidence",
              "ExpressionAttributeNames": {"#s": "status"},
              "FilterExpression": Attr("status").is_in(["complete", "partial"])}
        if lek:
            kw["ExclusiveStartKey"] = lek
        r = t.scan(**kw)
        for it in r.get("Items", []):
            scanned += 1
            sgn = DIRMAP.get(str(it.get("predicted_direction", "")).upper())
            if sgn is None:
                continue
            ty = it.get("signal_type", "?")
            oc = it.get("outcomes") or {}
            hz = it.get("horizon_days_primary")
            key = f"day_{int(hz)}" if hz is not None and f"day_{int(hz)}" in oc else None
            if key is None and oc:
                try:
                    key = max(oc, key=lambda k: int(k.split("_")[1]))
                except Exception:
                    key = None
            if not key:
                continue
            o = oc.get(key) or {}
            ret = f(o.get("return_pct"))
            if ret is None or "correct" not in o:
                continue
            d = agg.setdefault(ty, {"wins": [], "losses": [], "confs": []})
            cf = f(it.get("confidence"))
            if cf:
                d["confs"].append(cf)
            mag = abs(ret)
            (d["wins"] if o["correct"] else d["losses"]).append(mag)
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break

    table = []
    for ty, d in agg.items():
        nw, nl = len(d["wins"]), len(d["losses"])
        n = nw + nl
        if n < 5:
            continue
        p = nw / n
        W = sorted(d["wins"])[nw // 2] if nw else 0.0
        L = sorted(d["losses"])[nl // 2] if nl else 0.0
        L_eff = max(L, LOSS_FLOOR_PCT)
        kelly = None
        if W > 0:
            b = min(W / L_eff, PAYOFF_CAP)
            kelly = min(p - (1 - p) / b, KELLY_CAP)
        qk = round(max(0.0, (kelly or 0)) / 4 * 100, 2)
        sc = f((cal.get(ty) or {}).get("confidence_scale")) or 1.0
        table.append({"signal_type": ty, "n": n, "win_rate": round(p * 100, 1),
                       "median_win_pct": round(W, 2), "median_loss_pct": round(L, 2), "loss_floored": bool(L < LOSS_FLOOR_PCT),
                       "payoff_eff": round(min(W / max(L, LOSS_FLOOR_PCT), PAYOFF_CAP), 2) if W else None,
                       "kelly_pct": round((kelly or 0) * 100, 1),
                       "quarter_kelly_w_pct": qk,
                       "avg_claimed_conf": round(sum(d["confs"]) / len(d["confs"]), 2)
                                            if d["confs"] else None,
                       "calibrator_scale": sc,
                       "gate": ("OK" if n >= 15 and qk > 0 else
                                 "NO-EDGE" if (kelly or 0) <= 0 else "THIN")})
    table.sort(key=lambda x: -(x["quarter_kelly_w_pct"] or 0))
    tmap = {x["signal_type"]: x for x in table}

    # ── B) pending per-ticker candidates (last 21d, dedup latest) ──
    cutoff = int(time.time()) - 21 * 86400
    cands, lek = {}, None
    while True:
        kw = {"FilterExpression": Attr("status").eq("pending")
                                    & Attr("logged_epoch").gte(cutoff),
              "ProjectionExpression": "signal_id, signal_type, predicted_direction, "
                                        "confidence, baseline_price, measure_against, "
                                        "logged_epoch, benchmark, rationale"}
        if lek:
            kw["ExclusiveStartKey"] = lek
        r = t.scan(**kw)
        for it in r.get("Items", []):
            sgn = DIRMAP.get(str(it.get("predicted_direction", "")).upper())
            if sgn is None:
                continue
            ma = str(it.get("measure_against", ""))
            tick = None
            if ma and ma.upper() == ma and ma.isalpha() and 1 <= len(ma) <= 5 and ma != "SPY":
                tick = ma
            elif ma == "ticker":
                parts = str(it.get("signal_id", "")).split("#")
                if len(parts) >= 2 and parts[1].isalpha():
                    tick = parts[1]
            if not tick:
                continue
            k = (it.get("signal_type"), tick)
            ep = int(it.get("logged_epoch") or 0)
            if k not in cands or ep > cands[k]["ep"]:
                cands[k] = {"ep": ep, "ticker": tick, "dir": sgn,
                             "type": it.get("signal_type"),
                             "conf": f(it.get("confidence")) or 0.5,
                             "px": f(it.get("baseline_price")),
                             "sid": str(it.get("signal_id"))[:60]}
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
    # rank by engine edge, keep top 30 longs + shorts
    cl = sorted(cands.values(),
                 key=lambda c: -((tmap.get(c["type"]) or {}).get("quarter_kelly_w_pct") or 0))
    cl = [c for c in cl if (tmap.get(c["type"]) or {}).get("gate") != "NO-EDGE"][:30]

    # Reconciled snapshot only; no unscoped DynamoDB portfolio scan or public holdings.
    account_book=capital_book_view(donor_docs.get('portfolio/snapshot.json',{}))
    holdings=[{'ticker':symbol} for symbol in account_book['gross_weights']] if account_book['status']=='READY' else []
    book_status=account_book['status']

    # ── D) vols + correlations + the sizing chain ──
    series = {}
    for c in cl + [{"ticker": h["ticker"]} for h in holdings]:
        tk = c["ticker"]
        if tk not in series:
            series[tk] = rets(poly_closes(tk))
            time.sleep(0.05)
    spy = rets(poly_closes("SPY"))
    recs, accepted = [], []
    for c in cl:
        eng = tmap.get(c["type"]) or {}
        qk = eng.get("quarter_kelly_w_pct") or 0
        gate = eng.get("gate")
        base = STARTER_W if gate == "THIN" else qk
        sc = eng.get("calibrator_scale") or 1.0
        avgc = eng.get("avg_claimed_conf") or c["conf"] or 0.5
        conf_adj = max(0.6, min(1.4, (c["conf"] * sc) / max(avgc, 0.2)))
        rs = series.get(c["ticker"]) or []
        vol_ann = (sum(x * x for x in rs[-63:]) / max(len(rs[-63:]), 1)) ** 0.5 * (252 ** 0.5) \
                   if len(rs) >= 30 else None
        vol_scalar = min(1.0, VOL_TARGET / vol_ann) if vol_ann else 0.7
        mx = 0.0
        overlaps = []
        for a in accepted:
            r_ = corr(rs, series.get(a["ticker"]) or [])
            if r_ is not None and r_ > mx and a["dir"] == c["dir"]:
                mx = r_
            if r_ is not None and r_ >= 0.8:
                overlaps.append(f"{a['ticker']} ρ{round(r_,2)}")
        for h in holdings:
            r_ = corr(rs, series.get(h["ticker"]) or [])
            if r_ is not None and r_ >= 0.8:
                overlaps.append("correlated existing account exposure")
                mx = max(mx, r_)
        hcut = 1 - 0.5 * max(0.0, mx)
        beta = corr(rs, spy)
        w = base * conf_adj * vol_scalar * hcut
        w = min(CAP_W, w)
        if w < FLOOR_W:
            continue
        rec = {"ticker": c["ticker"], "direction": "LONG" if c["dir"] > 0 else "SHORT",
                "engine": c["type"], "engine_gate": gate,
                "claimed_conf": round(c["conf"], 2), "calibrator_scale": sc,
                "chain": {"quarter_kelly_w": round(base, 2),
                           "conf_adj_x": round(conf_adj, 2),
                           "vol_ann_pct": round(vol_ann * 100, 1) if vol_ann else None,
                           "vol_scalar_x": round(vol_scalar, 2),
                           "cluster_corr_max": round(mx, 2),
                           "haircut_x": round(hcut, 2)},
                "spy_corr": round(beta, 2) if beta is not None else None,
                "final_w_pct": round(w, 2),
                "risk_gate_posture": donor_docs.get("data/risk-gate.json",{}).get("posture"),
                "pre_gate_w_pct": round(w, 2),
                "dollars_per_100k": int(round(w * 1000)),
                "overlap_flags": overlaps[:3],
                "baseline_px": c["px"], "signal_id": c["sid"]}
        recs.append(rec)
        accepted.append({"ticker": c["ticker"], "dir": c["dir"]})
    donor_constraints=constrain_sizes(recs,donor_docs)
    recs.sort(key=lambda x: -x["final_w_pct"])
    gross = round(sum(r_["final_w_pct"] for r_ in recs), 1)

    out = {"engine": "sizing-engine", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "donor_inputs":donor_receipts,"donor_constraint_summary":donor_constraints,"execution_eligible":False,
           "params": {"kelly_fraction": "1/4", "vol_target_ann": VOL_TARGET,
                       "cap_w_pct": CAP_W, "floor_w_pct": FLOOR_W,
                       "starter_w_pct": STARTER_W, "min_n_full": 15},
           "scanned_graded": scanned,
           "engine_table": table,
           "fade_list": [x["signal_type"] for x in table if x["gate"] == "NO-EDGE"][:15],
           "recommendations": recs, "gross_recommended_w_pct": gross,
           "book_status": book_status, "holdings": None,"holdings_publication":"REDACTED_ACCOUNT_PRIVATE",
           "methodology": (
             "Quarter-Kelly per engine from the closed loop's own graded record "
             "(outcomes.correct + return_pct at each signal's primary horizon; median "
             "win/loss magnitudes), confidence multiplied by the calibrator's "
             "per-engine scale (self-audited deflation), 30% annualized vol target, "
             "greedy cluster-correlation haircut, 5% per-name cap / 0.25% floor; prudence: 0.75% loss floor, 6x payoff cap, 30% Kelly cap. Engines with "
             "negative measured Kelly publish on the FADE list and are never sized. "
             "Sizes are research outputs per $100k of book, not advice.")}
    out["duration_s"] = round(time.time() - t0, 1)
    clean = json.loads(json.dumps(out, default=str), parse_constant=lambda c: None)
    clean=sanitize_public(OUT_KEY,clean)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(clean,allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[sizing] graded={scanned} engines={len(table)} recs={len(recs)} "
          f"gross={gross}% {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"recs": len(recs), "gross": gross})}
