"""justhodl-industry-boom v1.0 — WHICH INDUSTRIES BOOM NEXT: the full-breadth
league table (~140 FMP industries via the exhaustive universe) that fuses the
platform's OWN per-ticker alpha feeds up to industry level — the fusion FinViz
groups can't have and the 22-ETF rotation ladder can't reach.

Audit (2026-07-20, extend-don't-duplicate): rotation = 22 industry ETFs ·
bottleneck-boom = ~10 M3/G.17 pressure groups · finviz-groups = external
perf/valuation aggregates · per-ticker feeds carry industry tags but were
NEVER aggregated. This engine closes exactly that gap.

PER INDUSTRY (n>=5 listed names):
  DEMAND   revision-velocity mean + positive breadth · deal-wins 30d (real
           wins only — capital_structure/M&A-target/promo excluded) ·
           backlog/RPO accelerating share
  FLOWS    13F institutional net $ (bps of industry mcap) · insider buys 30d
  QUALITY  census conviction mean (S&P subset, honest coverage)
  TROUBLE  dilution share (sh_yoy>=3%) · census high-risk share
BOOM SCORE = cross-sectional percentile composite (weights below), 0-100.
History ledger self-builds -> 20d rank deltas activate automatically.
Every source degrades independently; coverage is reported, never faked."""
import json, os, time
from datetime import datetime, timedelta, timezone
import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/industry-boom.json"
HIST_KEY = "data/industry-boom-history.json"
s3 = boto3.client("s3", region_name="us-east-1")


def _g(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print("[ind-boom] load fail", key, str(e)[:70])
        return None


def _rows_with(obj, need=("ticker",), alt=("symbol",)):
    """Generic extractor: find lists of dicts carrying a ticker field anywhere
    one level deep — survives sibling engines' shape drift."""
    out = []
    def scan(x):
        if isinstance(x, list) and x and isinstance(x[0], dict):
            k = next((f for f in need + alt if f in x[0]), None)
            if k:
                out.extend(x)
    if isinstance(obj, dict):
        for v in obj.values():
            scan(v)
    scan(obj)
    return out


def _pct(vals):
    xs = sorted(v for v in vals.values() if isinstance(v, (int, float)))
    if not xs:
        return {k: None for k in vals}
    def p(v):
        if not isinstance(v, (int, float)):
            return None
        i = sum(1 for x in xs if x <= v)
        return round(100.0 * i / len(xs), 1)
    return {k: p(v) for k, v in vals.items()}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    src = {}

    uni = _g("data/universe.json") or {}
    ind_of, meta = {}, {}
    for s0 in uni.get("stocks") or []:
        t, ind = s0.get("symbol"), (s0.get("industry") or "").strip()
        if t and ind and ind.lower() != "unknown":
            ind_of[t] = ind
            m = meta.setdefault(ind, {"sector": s0.get("sector"), "n": 0, "mcap": 0.0, "names": []})
            m["n"] += 1
            m["mcap"] += (s0.get("market_cap") or 0)
            if len(m["names"]) < 3:
                m["names"].append(t)
    src["universe"] = bool(ind_of)

    # DEMAND — revision velocity
    rev_sum, rev_pos, rev_cov = {}, {}, {}
    rv = _g("data/eps-revision-velocity.json") or _g("data/estimate-revisions.json")
    for r in _rows_with(rv):
        t = (r.get("ticker") or r.get("symbol") or "").upper()
        ind = ind_of.get(t)
        sc = r.get("score") if isinstance(r.get("score"), (int, float)) else r.get("velocity")
        if not ind or not isinstance(sc, (int, float)):
            continue
        rev_sum[ind] = rev_sum.get(ind, 0.0) + sc
        rev_cov[ind] = rev_cov.get(ind, 0) + 1
        if sc > 0:
            rev_pos[ind] = rev_pos.get(ind, 0) + 1
    src["revisions"] = bool(rev_cov)

    # DEMAND — real deal wins 30d (ledger; guards applied)
    deals30 = {}
    dh = _g("data/deal-history.json") or {}
    cut = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    for v in (dh.get("entries") or {}).values():
        if (v.get("announce") or "") < cut or v.get("promo") or v.get("listed") is False:
            continue
        if v.get("event_type") in ("capital_structure", "ma_target"):
            continue
        ind = ind_of.get(v.get("sym"))
        if ind:
            deals30[ind] = deals30.get(ind, 0) + 1
    src["deal_ledger"] = bool(dh.get("entries"))

    # DEMAND — backlog/RPO accelerating share
    bk_acc = {}
    bk = _g("data/backlog.json") or {}
    for r in (bk.get("accelerating") or []):
        t = (r.get("ticker") if isinstance(r, dict) else r) or ""
        ind = ind_of.get(str(t).upper())
        if ind:
            bk_acc[ind] = bk_acc.get(ind, 0) + 1
    src["backlog"] = bool(bk)

    # FLOWS — 13F net $ per industry (bps of mcap)
    f13_net = {}
    tf = (_g("data/13f-flows-by-ticker.json") or {}).get("t") or {}
    for t, v in tf.items():
        ind = ind_of.get(t)
        n = v.get("n")
        if ind and isinstance(n, (int, float)):
            f13_net[ind] = f13_net.get(ind, 0.0) + n
    src["flows_13f"] = bool(tf)

    # FLOWS — insider buys 30d
    ins30 = {}
    ir = _g("data/insider-radar.json")
    for r in _rows_with(ir):
        d = str(r.get("date") or "")
        if d and d < cut:
            continue
        ind = ind_of.get(str(r.get("ticker") or r.get("symbol") or "").upper())
        if ind:
            ins30[ind] = ins30.get(ind, 0) + 1
    src["insider"] = ir is not None

    # QUALITY / TROUBLE — census + dilution
    conv_sum, conv_cov, risk_hi = {}, {}, {}
    mx = _g("data/fundamental-census-matrix.json") or {}
    C = mx.get("cols") or {}
    tk = mx.get("tickers") or []
    conv = C.get("conviction_score") or []
    rk = C.get("risk_score") or []
    rks = sorted(v for v in rk if isinstance(v, (int, float)))
    rhi = rks[2 * len(rks) // 3] if len(rks) >= 3 else None
    for i, t in enumerate(tk):
        ind = ind_of.get(t)
        if not ind:
            continue
        cv = conv[i] if i < len(conv) else None
        if isinstance(cv, (int, float)):
            conv_sum[ind] = conv_sum.get(ind, 0.0) + cv
            conv_cov[ind] = conv_cov.get(ind, 0) + 1
        rv0 = rk[i] if i < len(rk) else None
        if rhi is not None and isinstance(rv0, (int, float)) and rv0 >= rhi:
            risk_hi[ind] = risk_hi.get(ind, 0) + 1
    src["census"] = bool(tk)
    dil = {}
    sf = (_g("data/share-flows.json") or {}).get("tickers") or {}
    dil_cov = {}
    for t, v in sf.items():
        ind = ind_of.get(t)
        if not ind or v.get("data_suspect"):
            continue
        dil_cov[ind] = dil_cov.get(ind, 0) + 1
        if (v.get("sh_yoy_pct") or 0) >= 3:
            dil[ind] = dil.get(ind, 0) + 1
    src["share_flows"] = bool(sf)

    inds = [k for k, m in meta.items() if m["n"] >= 5]
    raw = {}
    for k in inds:
        m = meta[k]
        raw[k] = {
            "rev_mean": (rev_sum.get(k, 0.0) / rev_cov[k]) if rev_cov.get(k) else None,
            "rev_breadth": (100.0 * rev_pos.get(k, 0) / rev_cov[k]) if rev_cov.get(k) else None,
            "deal_wins_30d": deals30.get(k, 0),
            "backlog_accel_share": round(100.0 * bk_acc.get(k, 0) / m["n"], 1),
            "inst_net_bps": (round(1e4 * f13_net.get(k, 0.0) / m["mcap"], 1) if m["mcap"] else None),
            "insider_buys_30d": ins30.get(k, 0),
            "census_conviction": (round(conv_sum.get(k, 0.0) / conv_cov[k], 1) if conv_cov.get(k) else None),
            "dilution_share": (round(100.0 * dil.get(k, 0) / dil_cov[k], 1) if dil_cov.get(k) else None),
            "risk_high_share": (round(100.0 * risk_hi.get(k, 0) / conv_cov[k], 1) if conv_cov.get(k) else None),
        }
    W = {"rev_mean": 20, "rev_breadth": 10, "deal_wins_30d": 15, "backlog_accel_share": 15,
         "inst_net_bps": 15, "insider_buys_30d": 10, "census_conviction": 15}
    NEG = {"dilution_share": 10, "risk_high_share": 5}
    pcts = {f: _pct({k: raw[k][f] for k in inds}) for f in list(W) + list(NEG)}
    league = []
    for k in inds:
        num, den = 0.0, 0.0
        for f, w in W.items():
            p = pcts[f][k]
            if p is not None:
                num += w * p; den += w
        for f, w in NEG.items():
            p = pcts[f][k]
            if p is not None:
                num += w * (100 - p); den += w
        if den < 40:
            continue
        m = meta[k]
        league.append({"industry": k, "sector": m["sector"], "n": m["n"],
                       "mcap_b": round(m["mcap"] / 1e9, 1),
                       "boom_score": round(num / den, 1),
                       "coverage_w": round(den, 0),
                       "comp": raw[k], "top_names": m["names"]})
    league.sort(key=lambda x: x["boom_score"], reverse=True)

    # history ledger → 20d deltas self-activate
    hist = _g(HIST_KEY) or {"days": {}}
    hist["days"][now.strftime("%Y-%m-%d")] = {r["industry"]: r["boom_score"] for r in league}
    hist["days"] = dict(sorted(hist["days"].items())[-60:])
    days = sorted(hist["days"])
    base = hist["days"][days[max(0, len(days) - 21)]] if days else {}
    for r in league:
        b = base.get(r["industry"])
        r["score_delta_20d"] = round(r["boom_score"] - b, 1) if isinstance(b, (int, float)) else None
    s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist).encode(),
                  ContentType="application/json")

    trouble = sorted([r for r in league if r["comp"].get("dilution_share") is not None],
                     key=lambda r: ((r["comp"].get("dilution_share") or 0)
                                    + (r["comp"].get("risk_high_share") or 0)), reverse=True)[:10]
    out = {"engine": "industry-boom", "version": "1.0.0", "generated_at": now.isoformat(),
           "n_industries": len(league), "n_universe": len(ind_of),
           "league": league, "trouble": trouble,
           "coverage": {"sources_ok": src,
                        "note": "each source degrades independently; percentile composite renormalizes on available weight (coverage_w)"},
           "siblings": {"industry_etf_momentum": "data/industry-rotation.json (22-ETF ladder)",
                        "supply_pressure": "data/bottleneck-boom.json (M3 + G.17 groups)",
                        "external_group_stats": "data/finviz-groups.json"},
           "methodology": {"score": "percentile composite — DEMAND(rev velocity 20 + breadth 10 + deal-wins 15 + backlog-accel 15) FLOWS(13F bps 15 + insider 10) QUALITY(census 15) − TROUBLE(dilution 10 + high-risk 5); industries n>=5; renormalized on covered weight",
                           "reads": "top decile + positive 20d delta = boom forming; high trouble composite = avoid/short-book candidates"},
           "disclaimer": "Real internal + primary data only. Research, not advice.",
           "elapsed_s": round(time.time() - t0, 2)}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[ind-boom] industries={len(league)} sources={sum(src.values())}/{len(src)} "
          f"top={[(r['industry'], r['boom_score']) for r in league[:3]]} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n": len(league),
            "sources_ok": sum(src.values()), "top": league[0]["industry"] if league else None})}
