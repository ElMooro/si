"""justhodl-capital-flow — where institutions + capital are flowing (stocks & ETFs)

Fuses three independent flow lenses into one ranked "capital is accumulating
here" signal — the smart-money footprint:

  1. 13F INSTITUTIONAL POSITIONS (data/13f-positions.json) — quarterly fund
     holdings + position changes (NEW / ADD / TRIM / EXIT) + #funds holding.
  2. ETF FLOWS (data/etf-flows.json + etf-fund-flows.json) — net creations/
     redemptions (real $ in/out of sector & thematic ETFs).
  3. INSTITUTIONAL OWNERSHIP CHANGE (screener/data.json) — instSharesChangePct,
     instQoQChgPct, instInvestorsChange — quarter-over-quarter accumulation.

CAPITAL-FLOW SCORE (per ticker, -100..+100):
    13f_signal   (new+added funds, $ delta, #funds growth)
  + inst_change  (QoQ shares % change, investor count change)
  + etf_pull     (is it held by ETFs seeing strong inflows)
  Positive = capital accumulating; negative = distribution/outflow.

OUTPUT: data/capital-flow.json — {accumulating[], distributing[], etf_flows[],
  by_ticker{}}. Daily 16:30 UTC.
"""
import json, os, time, statistics
from datetime import datetime, timezone
from collections import defaultdict
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/capital-flow.json"
s3 = boto3.client("s3", region_name=REGION)


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def sf(v):
    try:
        f = float(v); return f if f == f else None
    except Exception: return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    f13 = read_json("data/13f-positions.json") or {}
    etf = read_json("data/etf-flows.json") or {}
    etf_true = read_json("data/etf-true-flows.json") or {}
    etf2 = read_json("data/etf-fund-flows.json") or {}
    screener = read_json("screener/data.json") or {}
    rows = screener.get("stocks") or (screener if isinstance(screener, list) else [])

    by_ticker = defaultdict(lambda: {"ticker": "", "name": "", "flow_score": 0.0,
                                     "lenses": [], "detail": {}})

    # ── 1) 13F institutional positions ──
    agg = f13.get("aggregate_by_ticker") or {}
    if isinstance(agg, dict):
        items = agg.values()
    else:
        items = agg
    f13_scores = {}
    for p in items:
        tk = (p.get("ticker") or "").upper()
        if not tk:
            continue
        nf = sf(p.get("n_funds_holding")) or 0
        chg = (p.get("change") or "").upper()  # may be per-position; use changes_summary if present
        cs = p.get("changes_summary") or {}
        new_f = sf(cs.get("new")) or 0
        add_f = sf(cs.get("added") or cs.get("add")) or 0
        trim_f = sf(cs.get("trimmed") or cs.get("trim")) or 0
        exit_f = sf(cs.get("exited") or cs.get("exit")) or 0
        val_delta = sf(p.get("value_delta_pct"))
        score = 0.0
        score += min(20, new_f * 4)        # new positions = strong
        score += min(15, add_f * 1.5)
        score -= min(15, trim_f * 1.5)
        score -= min(20, exit_f * 3)
        if val_delta is not None:
            score += max(-15, min(15, val_delta * 0.5))
        if nf >= 10: score += 5            # broad institutional ownership
        f13_scores[tk] = {"score": round(score, 1), "n_funds": nf, "new": new_f,
                          "added": add_f, "trimmed": trim_f, "exited": exit_f, "val_delta_pct": val_delta}

    # ── 2) Institutional ownership change (screener QoQ) ──
    inst_scores = {}
    for r in rows:
        tk = (r.get("symbol") or r.get("ticker") or "").upper()
        if not tk:
            continue
        qoq = sf(r.get("instQoQChgPct"))
        sh = sf(r.get("instSharesChangePct"))
        inv = sf(r.get("instInvestorsChange"))
        sig = (r.get("instSignal") or "").upper()
        score = 0.0
        for v, w in ((qoq, 0.6), (sh, 0.4)):
            if v is not None:
                pv = v * 100 if abs(v) < 3 else v
                score += max(-20, min(20, pv * w))
        if inv is not None:
            score += max(-10, min(10, inv * 0.5))
        if "ACCUM" in sig or "BUY" in sig: score += 5
        elif "DISTRIB" in sig or "SELL" in sig: score -= 5
        inst_scores[tk] = {"score": round(score, 1), "qoq_chg": qoq, "shares_chg": sh,
                           "investor_chg": inv, "signal": sig,
                           "name": r.get("name"), "sector": r.get("sector")}

    # ── 3) ETF flows — prefer TRUE net flows (Δshares × price); fall back to
    # the dollar-volume proxy in etf-flows.json. ──
    etf_flows = []
    if etf_true.get("by_etf"):
        tin = etf_true.get("inflows") or []
        tout = etf_true.get("outflows") or []
        for e in tin + tout:
            etf_flows.append({"ticker": e.get("ticker"), "name": e.get("category"),
                              "category": e.get("category"),
                              "net_flow_5d_usd": e.get("net_flow_5d_usd"),
                              "net_flow_20d_usd": e.get("net_flow_20d_usd"),
                              "shares_chg_5d_pct": e.get("shares_chg_5d_pct"),
                              "aum_est_b": e.get("aum_est_b"), "true_flow": True})
        cat_rotation = [{"category": c.get("category"), "net_flow_5d_usd": c.get("net_flow_5d_usd"),
                         "n_etfs": c.get("n_etfs"), "signal": ("INFLOW" if (c.get("net_flow_5d_usd") or 0) > 0 else "OUTFLOW")}
                        for c in (etf_true.get("category_rotation") or [])]
    else:
        heavy_in = etf.get("heavy_inflow") or etf.get("rotation_in") or []
        heavy_out = etf.get("heavy_outflow") or etf.get("rotation_out") or []
        by_etf = etf.get("by_etf") or {}
        src = list(by_etf.values()) if isinstance(by_etf, dict) else (by_etf if isinstance(by_etf, list) else [])
        for e in src:
            tk = (e.get("ticker") or "").upper()
            if not tk:
                continue
            zsc = sf(e.get("dvol_z_score")); r5 = sf(e.get("return_5d_pct")); r20 = sf(e.get("return_20d_pct"))
            sig = (e.get("flow_signal") or "").upper()
            direction = 1 if (r5 is not None and r5 >= 0) else -1
            flow_proxy = round((zsc or 0) * direction, 2)
            etf_flows.append({"ticker": tk, "name": e.get("name"), "category": e.get("category"),
                              "aum_b": sf(e.get("aum_b")), "dvol_z": zsc, "return_5d_pct": r5,
                              "return_20d_pct": r20, "flow_signal": sig, "flow_proxy": flow_proxy})
        by_cat = etf.get("by_category") or {}
        cat_src = list(by_cat.values()) if isinstance(by_cat, dict) else (by_cat if isinstance(by_cat, list) else [])
        cat_rotation = sorted([{"category": c.get("category"), "signal": c.get("category_signal"),
                                "avg_dvol_z": sf(c.get("avg_dvol_z")), "avg_return_1d_pct": sf(c.get("avg_return_1d_pct")),
                                "total_aum_b": sf(c.get("total_aum_b"))}
                               for c in cat_src if c.get("category")],
                              key=lambda x: -(x.get("avg_dvol_z") or 0))
    # sort by whichever flow metric we have
    etf_flows.sort(key=lambda x: -(x.get("net_flow_5d_usd") if x.get("true_flow") else (x.get("flow_proxy") or 0)) or 0)

    # ── Fuse per ticker ──
    all_tks = set(f13_scores) | set(inst_scores)
    for tk in all_tks:
        rec = by_ticker[tk]; rec["ticker"] = tk
        total = 0.0; lenses = []
        f = f13_scores.get(tk)
        if f:
            total += f["score"]; rec["detail"]["13f"] = f
            if f["score"] > 5: lenses.append("13F accumulation")
            elif f["score"] < -5: lenses.append("13F distribution")
        ic = inst_scores.get(tk)
        if ic:
            total += ic["score"]; rec["detail"]["inst_change"] = ic
            rec["name"] = ic.get("name") or rec["name"]
            rec["sector"] = ic.get("sector")
            if ic["score"] > 5: lenses.append("inst QoQ accumulation")
            elif ic["score"] < -5: lenses.append("inst QoQ distribution")
        rec["flow_score"] = round(max(-100, min(100, total)), 1)
        rec["lenses"] = lenses

    results = [r for r in by_ticker.values() if r["lenses"]]
    accumulating = sorted([r for r in results if r["flow_score"] > 8], key=lambda r: -r["flow_score"])[:40]
    distributing = sorted([r for r in results if r["flow_score"] < -8], key=lambda r: r["flow_score"])[:25]

    output = {
        "engine": "capital-flow", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "sources": {"13f": bool(agg), "etf_flows": len(etf_flows), "inst_change": len(inst_scores), "categories": len(cat_rotation)},
        "methodology": ("Fuses 13F position changes (new/add/trim/exit + $ delta + "
                        "#funds), institutional QoQ ownership change (shares %, "
                        "investor count), and ETF net flows into one capital-flow "
                        "score (-100..+100). Positive = capital accumulating."),
        "accumulating": accumulating,
        "distributing": distributing,
        "etf_flows_in": etf_flows[:25],
        "etf_flows_out": [e for e in etf_flows if ((e.get("net_flow_5d_usd") if e.get("true_flow") else e.get("flow_proxy")) or 0) < 0][:15],
        "category_rotation": cat_rotation[:15],
        "by_ticker": {r["ticker"]: r for r in results[:300]},
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[capital-flow] DONE {round(time.time()-t0,1)}s — {len(accumulating)} accumulating, "
          f"{len(distributing)} distributing, {len(etf_flows)} ETF flows")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "accumulating": len(accumulating),
                                                     "distributing": len(distributing),
                                                     "etf_flows": len(etf_flows)})}
