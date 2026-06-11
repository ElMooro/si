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

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/sizing.json"
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "1.0.0"
DIRMAP = {"UP": 1, "LONG": 1, "OUTPERFORM": 1, "BULLISH": 1,
           "DOWN": -1, "SHORT": -1, "UNDERPERFORM": -1, "BEARISH": -1}
VOL_TARGET = 0.30      # annualized
CAP_W = 5.0            # % of book per name
FLOOR_W = 0.25
STARTER_W = 0.5


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
        kelly = None
        if W > 0 and L > 0:
            b = W / L
            kelly = p - (1 - p) / b
        elif nw and not nl:
            kelly = p
        qk = round(max(0.0, (kelly or 0)) / 4 * 100, 2)   # % of book
        sc = f((cal.get(ty) or {}).get("confidence_scale")) or 1.0
        table.append({"signal_type": ty, "n": n, "win_rate": round(p * 100, 1),
                       "median_win_pct": round(W, 2), "median_loss_pct": round(L, 2),
                       "payoff": round(W / L, 2) if W and L else None,
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

    # ── C) book (honest read) ──
    holdings = []
    try:
        pr = DDB.Table("justhodl-portfolio").scan(Limit=200)
        for it in pr.get("Items", []):
            tk = it.get("ticker") or it.get("symbol")
            if tk and (it.get("qty") or it.get("shares") or it.get("weight")):
                holdings.append({"ticker": str(tk),
                                  "qty": f(it.get("qty") or it.get("shares")),
                                  "weight": f(it.get("weight"))})
    except Exception as e:
        print(f"[book] {str(e)[:50]}")
    book_status = (f"{len(holdings)} holdings" if holdings
                    else "EMPTY — cluster haircut runs vs recommendation set + SPY; "
                          "book-aware path arms automatically when positions exist")

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
                overlaps.append(f"book:{h['ticker']} ρ{round(r_,2)}")
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
                "dollars_per_100k": int(round(w * 1000)),
                "overlap_flags": overlaps[:3],
                "baseline_px": c["px"], "signal_id": c["sid"]}
        recs.append(rec)
        accepted.append({"ticker": c["ticker"], "dir": c["dir"]})
    recs.sort(key=lambda x: -x["final_w_pct"])
    gross = round(sum(r_["final_w_pct"] for r_ in recs), 1)

    out = {"engine": "sizing-engine", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "params": {"kelly_fraction": "1/4", "vol_target_ann": VOL_TARGET,
                       "cap_w_pct": CAP_W, "floor_w_pct": FLOOR_W,
                       "starter_w_pct": STARTER_W, "min_n_full": 15},
           "scanned_graded": scanned,
           "engine_table": table,
           "fade_list": [x["signal_type"] for x in table if x["gate"] == "NO-EDGE"][:15],
           "recommendations": recs, "gross_recommended_w_pct": gross,
           "book_status": book_status, "holdings": holdings,
           "methodology": (
             "Quarter-Kelly per engine from the closed loop's own graded record "
             "(outcomes.correct + return_pct at each signal's primary horizon; median "
             "win/loss magnitudes), confidence multiplied by the calibrator's "
             "per-engine scale (self-audited deflation), 30% annualized vol target, "
             "greedy cluster-correlation haircut, 5% cap / 0.25% floor. Engines with "
             "negative measured Kelly publish on the FADE list and are never sized. "
             "Sizes are research outputs per $100k of book, not advice.")}
    clean = json.loads(json.dumps(out, default=str), parse_constant=lambda c: None)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(clean).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    out["duration_s"] = round(time.time() - t0, 1)
    print(f"[sizing] graded={scanned} engines={len(table)} recs={len(recs)} "
          f"gross={gross}% {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"recs": len(recs), "gross": gross})}
