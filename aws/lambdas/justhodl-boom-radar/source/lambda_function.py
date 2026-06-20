"""justhodl-boom-radar — catalyst-convergence boom detector.

The largest price booms happen when INDEPENDENT confirmations stack on the same
name. This engine fuses six independent dimensions and ranks names by how many
agree (convergence) — convergence is far more predictive than any single signal:

  BEAT     earnings-tracker  — recent EPS+revenue surprise (Benzinga PEAD)
  ANALYST  analyst-actions   — guidance raise / PT raise / upgrade (Benzinga)
  ESTIMATE estimate-revisions— forward-EPS growth + upward revision (FMP+Benzinga)
  FLOW     flow-lookthrough  — actual ETF mechanical accumulation (Constituents)
  SQUEEZE  squeeze-pretrigger— short-squeeze pressure (squeeze_risk = proven alpha)
  BREAKOUT 52wk-quality-breakout — technical breakout confirmation

boom_score = sum(dimension sub-scores) x convergence multiplier. Names with >=3
independent confirmations are BOOM CANDIDATES; >=4 -> top_picks to the harvester
(MEASURE-BEFORE-TRUST). Display/discovery engine — NOT wired into decision engines.
"""
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/boom-radar.json"

DIM_WEIGHT = {"BEAT": 1.0, "ANALYST": 1.1, "ESTIMATE": 1.0, "FLOW": 1.0,
              "SQUEEZE": 1.2, "BREAKOUT": 0.8}  # squeeze slightly up (proven alpha)


def getj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _tk(item):
    if isinstance(item, str):
        return item.strip().upper()
    if isinstance(item, dict):
        for k in ("ticker", "symbol", "sym", "t"):
            v = item.get(k)
            if v:
                return str(v).strip().upper()
    return None


def _clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


def lambda_handler(event=None, context=None):
    t0 = time.time()
    # dim -> {ticker: (subscore, reason)}
    dims = defaultdict(dict)

    # BEAT
    et = getj("data/earnings-tracker.json") or {}
    for s in et.get("pead_signals", []) or []:
        tk = _tk(s)
        sc = s.get("pead_score")
        if tk and isinstance(sc, (int, float)) and sc >= 60:
            dims["BEAT"][tk] = (_clamp((sc - 50) / 40), f"{s.get('pead_label','beat')} ({sc})")

    # ANALYST
    aa = getj("data/analyst-actions.json") or {}
    for g in aa.get("guidance_raises", []) or []:
        tk = _tk(g)
        if tk:
            dims["ANALYST"][tk] = (1.0, f"guidance raised {g.get('fiscal_period','')}{g.get('fiscal_year','')}")
    for r in aa.get("pt_raises", []) or []:
        tk = _tk(r); pct = r.get("pt_pct")
        if tk and tk not in dims["ANALYST"]:
            dims["ANALYST"][tk] = (_clamp((pct or 0) / 20, 0.3, 1.0), f"PT raised +{round(pct or 0,0)}% ({r.get('firm','')})")
    for a in aa.get("most_bullish", []) or []:
        tk = _tk(a); ns = a.get("net_score")
        if tk and tk not in dims["ANALYST"] and isinstance(ns, (int, float)) and ns > 0:
            dims["ANALYST"][tk] = (_clamp(ns / 15, 0.3, 1.0), f"net analyst +{ns}")

    # ESTIMATE
    er = getj("data/estimate-revisions.json") or {}
    for s in er.get("estimate_strength_leaders", []) or []:
        tk = _tk(s); st = s.get("estimate_strength")
        if tk and isinstance(st, (int, float)) and st >= 60:
            g = s.get("fwd_eps_growth_pct")
            dims["ESTIMATE"][tk] = (_clamp((st - 50) / 40), f"est strength {st}" + (f", +{g}% fwd EPS" if g is not None else ""))
    for s in er.get("upward_revisions", []) or []:
        tk = _tk(s); rv = s.get("eps_rev_pct")
        if tk and tk not in dims["ESTIMATE"] and isinstance(rv, (int, float)):
            dims["ESTIMATE"][tk] = (_clamp(0.5 + rv / 20, 0.3, 1.0), f"EPS est revised +{rv}%")

    # FLOW
    fl = getj("data/flow-lookthrough.json") or {}
    for s in fl.get("actual_accumulation", []) or []:
        tk = _tk(s); bps = s.get("delta_bps_mcap")
        if tk and isinstance(bps, (int, float)) and bps > 0:
            dims["FLOW"][tk] = (_clamp(bps / 300, 0.3, 1.0), f"ETF accumulation {round(bps,0)}bps")
    for s in fl.get("top_picks", []) or []:
        tk = _tk(s)
        if tk and tk not in dims["FLOW"]:
            dims["FLOW"][tk] = (0.6, "ETF flow pick")

    # SQUEEZE (proven-alpha anchor)
    sq = getj("data/squeeze-pretrigger.json") or {}
    for lk in ("top_squeeze_risk", "top_squeeze_tickers", "top_covering"):
        for s in sq.get(lk, []) or []:
            tk = _tk(s)
            if tk and tk not in dims["SQUEEZE"]:
                sc = s.get("score") or s.get("squeeze_score") if isinstance(s, dict) else None
                dims["SQUEEZE"][tk] = (_clamp((sc or 70) / 100, 0.4, 1.0), "squeeze pressure")

    # BREAKOUT
    bo = getj("data/52wk-quality-breakout.json") or {}
    bo_list = bo if isinstance(bo, list) else (bo.get("breakouts") or bo.get("top_picks")
              or bo.get("results") or bo.get("candidates") or [])
    for s in bo_list or []:
        tk = _tk(s)
        if tk and tk not in dims["BREAKOUT"]:
            dims["BREAKOUT"][tk] = (0.6, "52wk quality breakout")

    dims_present = {d: len(v) for d, v in dims.items() if v}
    # aggregate per ticker
    agg = defaultdict(lambda: {"ticker": None, "dims": [], "raw": 0.0, "reasons": []})
    for d, m in dims.items():
        w = DIM_WEIGHT.get(d, 1.0)
        for tk, (sub, reason) in m.items():
            a = agg[tk]
            a["ticker"] = tk
            a["dims"].append(d)
            a["raw"] += sub * w
            a["reasons"].append(f"{d}: {reason}")

    cands = []
    for tk, a in agg.items():
        n = len(a["dims"])
        boom = round(a["raw"] * (1 + 0.4 * (n - 1)), 2)  # convergence multiplier
        cands.append({"ticker": tk, "boom_score": boom, "convergence": n,
                      "dimensions": sorted(a["dims"]), "reasons": a["reasons"]})
    cands.sort(key=lambda c: (c["convergence"], c["boom_score"]), reverse=True)

    boom_candidates = [c for c in cands if c["convergence"] >= 2][:60]
    high_conviction = [c for c in cands if c["convergence"] >= 3]
    top_picks = [{"ticker": c["ticker"], "score": c["boom_score"],
                  "convergence": c["convergence"], "dimensions": c["dimensions"]}
                 for c in cands if c["convergence"] >= 4][:20]
    if len(top_picks) < 8:  # fall back to 3-way convergence if few 4-way
        for c in high_conviction:
            if len(top_picks) >= 12:
                break
            if not any(p["ticker"] == c["ticker"] for p in top_picks):
                top_picks.append({"ticker": c["ticker"], "score": c["boom_score"],
                                  "convergence": c["convergence"], "dimensions": c["dimensions"]})

    out = {
        "engine": "justhodl-boom-radar", "version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Booms come from CONVERGENCE — independent bullish signals stacking on "
                  "one name. Fuses earnings beats, analyst guidance/PT, estimate strength, "
                  "ETF accumulation, squeeze pressure (proven alpha), and breakouts.",
        "dimensions_loaded": dims_present,
        "n_scanned": len(agg),
        "n_2way": sum(1 for c in cands if c["convergence"] >= 2),
        "n_3way": len(high_conviction),
        "n_4way_plus": sum(1 for c in cands if c["convergence"] >= 4),
        "boom_candidates": boom_candidates,
        "high_conviction": high_conviction[:30],
        "top_picks": top_picks,
        "methodology": {
            "convergence": "count of independent dimensions flagging a name bullish",
            "boom_score": "sum(weighted dimension sub-scores) x (1 + 0.4*(convergence-1))",
            "dimensions": list(DIM_WEIGHT.keys()),
        },
        "caveats": [
            "Discovery/convergence engine — measure-before-trust; NOT wired into decision engines.",
            "Several inputs (analyst-actions, estimate-revisions, flow-lookthrough) are themselves "
            "still being graded vs SPY; convergence raises the bar but does not bypass measurement.",
            "Squeeze + breakout dimensions can favour high-volatility small-caps.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    return {"statusCode": 200, "body": json.dumps({
        "dimensions_loaded": dims_present, "n_scanned": len(agg),
        "n_2way": out["n_2way"], "n_3way": out["n_3way"], "n_4way_plus": out["n_4way_plus"],
        "n_picks": len(top_picks),
        "top": [(c["ticker"], c["convergence"], c["boom_score"]) for c in cands[:8]],
        "elapsed_s": out["elapsed_s"]})}
