"""justhodl-distribution-composite v1.0 — the mirror the long book never had (wo4580).

accum-composite ranks quiet BUYING evidence; the fleet had no ranked view of
quiet SELLING. Same constitution, mirrored legs:

  insider_sell_cluster  tier_1  Form-4 open-market SELL clusters (real tape,
                                data/insider-trades.json sell side)
  svr_high              tier_1  HIGH short-volume-ratio quintile (Wang/Yan/
                                Zheng short leg — the documented alpha side)
  dark_distribution     tier_3  dark-pool DISTRIBUTION states (price-
                                conditioned off-exchange supply)
  radar_distributing    tier_4  technical distribution (confirmation only)

Rules inherited: every component residualized on liquidity rank; names
require tier-1 participation to rank at all; weights are priors Wilson-
shrunk by the calibration ledger (honest prior_only basis until each
family accrues n>=30 graded outcomes); per-run decay by feed age.

OUTPUT data/distribution-composite.json. Daily 22:00 UTC.
"""
import json
import time
from datetime import datetime, timezone

import boto3

from impact_mapper import (build as impact_build, load_graph,
                           measured_row, structural_row)
from evidence_weights import blend as ew_blend
from signals_emit import log_signal, yprice

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/distribution-composite.json"
s3 = boto3.client("s3", region_name=REGION)

FEEDS = {
    "insider_trades": "data/insider-trades.json",
    "finra_short": "data/finra-short.json",
    "dark_pool": "data/dark-pool.json",
    "radar": "data/accumulation-radar.json",
}
WEIGHTS = {
    "insider_sell_cluster": 0.35,
    "svr_high": 0.25,
    "dark_distribution": 0.20,
    "radar_distributing": 0.20,
}
TIER = {
    "insider_sell_cluster": "tier_1_regulatory_validated",
    "svr_high": "tier_1_regulatory_validated",
    "dark_distribution": "tier_3_microstructure_estimate",
    "radar_distributing": "tier_4_unvalidated_technical",
}
SIGNAL_TYPE_MAP = {
    "insider_sell_cluster": "dist_insider_sell",
    "svr_high": "dist_svr_high",
    "dark_distribution": "dist_dark",
    "radar_distributing": "dist_radar",
}
HALF_LIFE_D = {"insider_sell_cluster": 21, "svr_high": 10,
               "dark_distribution": 14, "radar_distributing": 7}
SELL_WORDS = ("sell", "s", "sale", "d", "disposition", "s-sale")


def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _rank(d):
    if not d:
        return {}
    items = sorted(d.items(), key=lambda kv: kv[1])
    n = max(len(items) - 1, 1)
    return {k: i / n for i, (k, _) in enumerate(items)}


def _residualize(sig, liq):
    common = [t for t in sig if t in liq]
    if len(common) < 8:
        return dict(sig), False
    rs, rl = _rank({t: sig[t] for t in common}), _rank({t: liq[t] for t in common})
    n = len(common)
    mx = sum(rl[t] for t in common) / n
    my = sum(rs[t] for t in common) / n
    cov = sum((rl[t] - mx) * (rs[t] - my) for t in common)
    var = sum((rl[t] - mx) ** 2 for t in common) or 1e-9
    b = cov / var
    return {t: rs[t] - (my + b * (rl[t] - mx)) for t in common}, True


def feed_age_days(j):
    try:
        ts = (j or {}).get("generated_at") or (j or {}).get("as_of")
        t2 = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return max(0.0, (datetime.now(timezone.utc) - t2).total_seconds() / 86400)
    except Exception:
        return None


def extract_insider_sells(j):
    """→ {ticker: {value, n_sellers}} from the Form-4 tape's sell side.
    Excludes rows flagged as 10b5-1/plan sales — scheduled selling is not
    information (mirror of the buy-side routine exclusion)."""
    out = {}
    if not isinstance(j, dict):
        return out
    agg, who = {}, {}
    for x in ((j.get("transactions") or []) + (j.get("sell_transactions") or [])):
        if not isinstance(x, dict):
            continue
        tk = (x.get("ticker") or "").upper()
        if not tk:
            continue
        side = str(x.get("side") or x.get("type") or x.get("transactionType")
                   or x.get("acquisitionOrDisposition") or "").strip().lower()
        if side not in SELL_WORDS:
            continue
        if x.get("rule_10b5_1") or x.get("is_plan") or \
                str(x.get("plan") or "").lower() == "10b5-1":
            continue
        v = x.get("value") or 0
        try:
            v = float(v)
        except Exception:
            v = 0.0
        if v <= 0:
            continue
        agg[tk] = agg.get(tk, 0.0) + v
        who.setdefault(tk, set()).add(x.get("insider") or x.get("name") or "")
    for tk, v in agg.items():
        n = len(who.get(tk) or ())
        if n >= 2 and v >= 500_000:   # cluster gate: 2+ sellers, real size
            out[tk] = {"value": v, "n_sellers": n}
    return out


def extract_svr_high(j):
    svr, liq = {}, {}
    if not isinstance(j, dict):
        return svr, liq
    cand = j.get("tickers") or {}
    if not isinstance(cand, dict):
        cand = {}
    for tk, r in cand.items():
        if not isinstance(r, dict):
            continue
        s2 = r.get("svr")
        tv = r.get("total_volume") or r.get("total_vol") or 0
        if s2 is None:
            continue
        try:
            svr[tk.upper()] = float(s2)   # HIGH svr = the short leg
            liq[tk.upper()] = float(tv or 0)
        except Exception:
            continue
    return svr, liq


def extract_dark_dist(j):
    out = {}
    if not isinstance(j, dict):
        return out
    for r in (j.get("top_distribution") or []):
        if isinstance(r, dict) and r.get("ticker"):
            out[r["ticker"].upper()] = float(r.get("score") or 50)
    for r in (j.get("distribution_into_strength") or []):
        tk = (r.get("ticker") or r.get("symbol") or "").upper() \
            if isinstance(r, dict) else str(r).upper()
        if tk:
            out[tk] = max(out.get(tk, 0.0),
                          float((r.get("score") if isinstance(r, dict) else 0) or 60))
    return out


def extract_radar_dist(j):
    out = {}
    if not isinstance(j, dict):
        return out
    rows = j.get("distributing") or []
    if isinstance(rows, dict):
        rows = list(rows.values())
    for c in rows if isinstance(rows, list) else []:
        if not isinstance(c, dict):
            continue
        tk = (c.get("ticker") or c.get("symbol") or "").upper()
        v = c.get("score") or c.get("distribution_score") or c.get("composite")
        if tk and v is not None:
            try:
                out[tk] = float(v)
            except Exception:
                continue
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    gaps = []
    raw = {k: read_json(p) for k, p in FEEDS.items()}
    feeds_missing = [{"feed": k, "key": p, "reason": "s3_read_failed_or_absent"}
                     for k, p in FEEDS.items() if raw[k] is None]

    sells = extract_insider_sells(raw["insider_trades"])
    svr, liq = extract_svr_high(raw["finra_short"])
    darkd = extract_dark_dist(raw["dark_pool"])
    radard = extract_radar_dist(raw["radar"])
    for name, m in (("insider_trades", sells), ("finra_short", svr),
                    ("dark_pool", darkd), ("radar", radard)):
        if raw.get(name) is not None and not m:
            feeds_missing.append({"feed": name, "reason": "read_ok_but_zero_rows"})

    learned_w, weights_meta = ew_blend(WEIGHTS, SIGNAL_TYPE_MAP)
    FEED_OF = {"insider_sell_cluster": "insider_trades", "svr_high": "finra_short",
               "dark_distribution": "dark_pool", "radar_distributing": "radar"}
    decay = {}
    for comp, feed in FEED_OF.items():
        age = feed_age_days(raw.get(feed))
        decay[comp] = (round(max(0.25, 0.5 ** (age / HALF_LIFE_D[comp])), 3)
                       if age is not None else 1.0)
    eff_w = {k: round(learned_w[k] * decay[k], 4) for k in learned_w}

    comp_raw = {
        "insider_sell_cluster": {t: v["value"] for t, v in sells.items()},
        "svr_high": svr,                      # high svr = distribution leg
        "dark_distribution": darkd,
        "radar_distributing": radard,
    }
    comp, resid_applied = {}, {}
    for k, m in comp_raw.items():
        if not m:
            comp[k] = {}
            continue
        if liq:
            comp[k], resid_applied[k] = _residualize(m, liq)
        else:
            comp[k], resid_applied[k] = _rank(m), False
    if not liq:
        gaps.append("no liquidity map (finra total_volume) — residualization "
                    "skipped, plain ranks used")
    norm = {k: _rank(v) if v else {} for k, v in comp.items()}
    tickers = set()
    for m in norm.values():
        tickers |= set(m)

    n_live_t1 = sum(1 for k in ("insider_sell_cluster", "svr_high") if norm.get(k))
    state = "OK" if n_live_t1 >= 1 and len(norm.get("svr_high", {})) + \
        len(norm.get("insider_sell_cluster", {})) > 0 else "INSUFFICIENT_DATA"

    names = []
    for tk in tickers:
        parts, wsum, score = [], 0.0, 0.0
        for k, w in eff_w.items():
            if tk in norm.get(k, {}):
                v = norm[k][tk]
                parts.append({"component": k, "evidence_tier": TIER[k],
                              "rank": round(v, 3), "weight": w,
                              "residualized": bool(resid_applied.get(k))})
                score += w * v
                wsum += w
        if wsum < 0.40:
            continue
        if not [p for p in parts if p["evidence_tier"].startswith("tier_1")]:
            continue
        row = {"ticker": tk, "score": round(score / wsum * 100, 1),
               "n_components": len(parts), "weight_covered": round(wsum, 2),
               "components": parts}
        if tk in sells:
            row["insider_sell_usd"] = round(sells[tk]["value"])
            row["n_sellers"] = sells[tk]["n_sellers"]
        names.append(row)
    names.sort(key=lambda x: -x["score"])
    for i, n in enumerate(names, 1):
        n["rank"] = i

    # impact_map — this engine's flagged names are the SUFFERING side
    graph = load_graph() or {}
    gtk = graph.get("tickers") or {}
    gind = graph.get("industries") or {}
    ind_flag = {}
    for n2 in names[:60]:
        info = gtk.get(n2["ticker"]) or {}
        ind, mc = info.get("industry"), info.get("mcap")
        if ind and mc and n2["n_components"] >= 2:
            d2 = ind_flag.setdefault(ind, {"mc": 0.0, "names": []})
            d2["mc"] += float(mc)
            d2["names"].append(n2["ticker"])
    ind_rows = []
    for ind, d2 in ind_flag.items():
        tot = float((gind.get(ind) or {}).get("mcap") or 0)
        if tot > 0 and len(d2["names"]) >= 2:
            r2 = measured_row(ind, "industry", d2["mc"] / tot * 100,
                              "pct_industry_mcap_under_evidence",
                              "sum(mcap of names with >=2 distribution "
                              "components) / industry mcap — %s"
                              % ",".join(sorted(d2["names"])[:6]))
            r2["n_members"] = len(d2["names"])
            ind_rows.append(r2)
    impact = impact_build(
        "distribution-composite", "distribution_evidence_composite",
        [],
        [structural_row(n2["ticker"], "company",
                        "%d components, score %.0f"
                        % (n2["n_components"], n2["score"]), -1)
         for n2 in names[:15]] + ind_rows,
        "Mirror of accum-composite: quiet SELLING evidence, tier-gated. "
        "Company rows structural; industry rows measured % of industry "
        "mcap under >=2-component distribution evidence.",
        basis_note="weights basis: %s" % weights_meta["overall_basis"])

    signals_logged = 0
    try:
        tbl = boto3.resource("dynamodb", region_name=REGION).Table("justhodl-signals")
        for n2 in names[:10]:
            bp = yprice(n2["ticker"])
            if not bp:
                continue
            if log_signal(tbl, "dist_composite", n2["ticker"], "bearish",
                          [21, 63], bp,
                          confidence=min(0.72, 0.45 + 0.004 * n2["score"]),
                          rationale="distribution composite rank %d (%d comps)"
                          % (n2["rank"], n2["n_components"]),
                          metadata={"score": n2["score"]}):
                signals_logged += 1
    except Exception as e:
        print("signal emit: %s" % e)

    out = {
        "engine": "distribution-composite", "version": "1.0",
        "engine_class": "distribution_evidence_composite",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "state": state,
        "method": ("Mirror of accum-composite (wo4580): insider SELL clusters "
                   "(2+ sellers, >=\\$500k, 10b5-1 excluded) 0.35 + HIGH-SVR "
                   "quintile (the documented short leg) 0.25 as tier-1; dark "
                   "distribution 0.20 tier-3; radar distributing 0.20 tier-4 "
                   "confirmation. Residualized on liquidity; tier-1 required "
                   "to rank; weights Wilson-shrunk by the ledger and decayed "
                   "by feed age."),
        "weights": {"priors": WEIGHTS, "learned": learned_w,
                    "effective_after_decay": eff_w, "decay": decay,
                    "meta": weights_meta},
        "component_coverage": {k: len(v) for k, v in norm.items()},
        "feeds_missing": feeds_missing, "gaps": gaps,
        "n_names": len(names), "names": names[:60],
        "impact_map": impact,
        "signals_logged": signals_logged,
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    print("[dist-composite] DONE %.1fs state=%s names=%d coverage=%s"
          % (time.time() - t0, state, len(names), out["component_coverage"]))
    return {"statusCode": 200, "body": json.dumps({
        "ok": state == "OK", "state": state, "n_names": len(names),
        "signals_logged": signals_logged})}
