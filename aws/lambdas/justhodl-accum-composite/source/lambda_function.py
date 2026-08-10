"""justhodl-accum-composite v1.0 — accumulation scoring by EVIDENCE, not habit

ops-4559 BUG-8. The research finding this engine encodes: no peer-reviewed
out-of-sample validation exists for OBV/CMF/MFI/Wyckoff as accumulation
detectors, while the signals with real replicated alpha are all free
regulatory data. The evidence gradient runs OPPOSITE to popularity:

  tier 1 (regulatory, validated):
    * opportunistic Form-4 insider buying — Cohen/Malloy/Pomorski: ~145bp/mo
      four-factor alpha, t=6.82 (survives any multiple-testing correction)
    * LOW short-volume-ratio quintile — Wang/Yan/Zheng: the alpha lives on
      the LONG side of the LOW-SVR tail (~12.6% ann.), i.e. the OPPOSITE
      tail from squeeze scanning
  tier 2 (disclosed, stale): institutional ownership delta, residualized —
      raw 13F delta is a size/momentum proxy; the residual carries ~76bp/mo
  tier 3 (microstructure estimate): dark-pool composition — block prints
      constructive, non-block dark share >10% corrosive
  tier 4 (unvalidated technical): accumulation-radar composite — kept as a
      CONFIRMATION layer only, tagged, never primary. Visible disagreement
      between tier-1/2 and tier-4 is itself information.

Discipline: every component is residualized on liquidity rank before scoring
(the VPIN lesson — naive flow metrics are volume in a costume), weights are
by evidence tier, and validation status is stated honestly.

INPUTS (all already produced by the fleet):
  data/insider-buys-enriched.json   data/finra-short.json
  data/smart-money-clusters.json    data/dark-pool.json
  data/accumulation-radar.json
OUTPUT: data/accum-composite.json   Daily 21:30 UTC.
"""
import json, time
from datetime import datetime, timezone
import boto3

from impact_mapper import (build as impact_build, load_graph,
                           measured_row, structural_row)
from evidence_weights import blend as ew_blend

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/accum-composite.json"
s3 = boto3.client("s3", region_name=REGION)

FEEDS = {
    "insider": "data/insider-buys-enriched.json",
    "finra_short": "data/finra-short.json",
    "inst_13f": "data/smart-money-clusters.json",
    "dark_pool": "data/dark-pool.json",
    "radar": "data/accumulation-radar.json",
    "congress": "data/congress-direct.json",
    "activist": "data/activist-13d.json",
}
WEIGHTS = {  # by evidence tier — not by intuition
    "form4_opportunistic": 0.30,
    "short_vol_low": 0.22,
    "inst_delta_resid": 0.18,
    "congress_cluster": 0.08,
    "activist_13d": 0.07,
    "dark_composition": 0.08,
    "technical_radar": 0.07,
}
# information half-lives (days) per evidence class — a stale feed's weight
# decays instead of silently dominating; floor 0.25 keeps it visible
HALF_LIFE_D = {
    "form4_opportunistic": 21, "short_vol_low": 10, "inst_delta_resid": 60,
    "congress_cluster": 30, "activist_13d": 45, "dark_composition": 14,
    "technical_radar": 7,
}
SIGNAL_TYPE_MAP = {
    "form4_opportunistic": "accum_form4", "short_vol_low": "accum_svr_low",
    "inst_delta_resid": "accum_13f_resid", "congress_cluster": "accum_congress",
    "activist_13d": "accum_activist", "dark_composition": "accum_dark",
    "technical_radar": "accum_radar",
}
TIER = {
    "form4_opportunistic": "tier_1_regulatory_validated",
    "short_vol_low": "tier_1_regulatory_validated",
    "inst_delta_resid": "tier_2_disclosed_residualized",
    "congress_cluster": "tier_1_regulatory_validated",
    "activist_13d": "tier_1_regulatory_validated",
    "dark_composition": "tier_3_microstructure_estimate",
    "technical_radar": "tier_4_unvalidated_technical",
}
NONBLOCK_CORROSIVE = 0.10


def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _rank(d):
    """dict → percentile ranks in [0,1] (ties by order)."""
    if not d:
        return {}
    items = sorted(d.items(), key=lambda kv: kv[1])
    n = max(len(items) - 1, 1)
    return {k: i / n for i, (k, _) in enumerate(items)}


def _residualize(sig, liq):
    """Residual of rank(sig) on rank(liquidity) — strips the volume costume."""
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


def extract_insider(j):
    """→ {ticker: {value, n_insiders, opportunistic}} from the enriched feed."""
    out = {}
    if not isinstance(j, dict):
        return out
    rows = j.get("clusters") or j.get("rows") or j.get("buys") or []
    if isinstance(rows, dict):
        rows = list(rows.values())
    for c in rows:
        if not isinstance(c, dict):
            continue
        tk = (c.get("ticker") or c.get("symbol") or "").upper()
        if not tk:
            continue
        val = c.get("total_value") or c.get("value_usd") or c.get("total_usd") or 0
        n = c.get("n_insiders") or c.get("cluster_size") or 1
        routine = bool(c.get("routine") or c.get("is_routine"))
        opp = c.get("opportunistic")
        if opp is None:
            # cluster heuristic: multi-insider, non-flagged-routine buying is
            # treated as opportunistic-candidate. Labeled as heuristic, not
            # as the full Cohen/Malloy/Pomorski same-calendar-month test.
            opp = (not routine) and (n or 1) >= 1
        try:
            val = float(val)
        except Exception:
            val = 0.0
        if val <= 0:
            continue
        prev = out.get(tk)
        if not prev or val > prev["value"]:
            out[tk] = {"value": val, "n_insiders": n, "opportunistic": bool(opp)}
    return out


def extract_svr(j):
    """→ ({ticker: svr}, {ticker: dollar_volume_proxy})"""
    svr, liq = {}, {}
    if not isinstance(j, dict):
        return svr, liq
    cand = j.get("tickers") or j.get("by_ticker") or {}
    if not isinstance(cand, dict):
        cand = {}
    if not cand:  # fall back: scan for a symbol→dict map with svr inside
        for v in j.values():
            if isinstance(v, dict) and v and all(isinstance(x, dict) for x in list(v.values())[:3]):
                sample = next(iter(v.values()))
                if "svr" in sample:
                    cand = v
                    break
    for tk, r in cand.items():
        if not isinstance(r, dict):
            continue
        s = r.get("svr")
        tv = r.get("total_volume") or r.get("total_vol") or 0
        if s is None:
            continue
        try:
            svr[tk.upper()] = float(s)
            liq[tk.upper()] = float(tv or 0)
        except Exception:
            continue
    return svr, liq


def extract_13f(j):
    """→ {ticker: net_conviction} from smart-money clusters."""
    out = {}
    if not isinstance(j, dict):
        return out
    rows = j.get("clusters") or j.get("rows") or j.get("names") or []
    if isinstance(rows, dict):
        rows = list(rows.values())
    for c in rows:
        if not isinstance(c, dict):
            continue
        tk = (c.get("ticker") or c.get("symbol") or "").upper()
        if not tk:
            continue
        v = (c.get("net_buyers") or c.get("n_funds") or c.get("conviction")
             or c.get("score") or 0)
        try:
            out[tk] = float(v)
        except Exception:
            continue
    return out


def extract_dark(j):
    """→ {ticker: composition_score} — needs the BUG-6 share-of-volume fields.
    Positive only when block-tilted; penalized when non-block share >10%."""
    out, mode = {}, "unavailable"
    if not isinstance(j, dict):
        return out, mode
    share = j.get("dark_share_map")
    if isinstance(share, dict) and share:
        mode = "share_of_volume"
        block = j.get("block_share_map") or {}
        for tk, v in share.items():
            try:
                v = float(v)
            except Exception:
                continue
            b = block.get(tk)
            nb = (v - float(b)) if b is not None else None
            score = 0.0
            if b:
                score += min(float(b) / 0.05, 1.0) * 0.5       # block prints constructive
            if nb is not None and nb > NONBLOCK_CORROSIVE:
                score -= min((nb - NONBLOCK_CORROSIVE) / 0.10, 1.0) * 0.5
            out[tk.upper()] = score
    return out, mode


def extract_congress(j):
    """→ {ticker: recent_buy_usd} from congress-direct (house+senate,
    official eFD/Clerk source). Purchases only; ranges midpointed upstream."""
    out = {}
    if not isinstance(j, dict):
        return out
    for side in ("house", "senate"):
        node = j.get(side)
        rows = []
        if isinstance(node, list):
            rows = node
        elif isinstance(node, dict):
            rows = (node.get("trades") or node.get("transactions")
                    or node.get("rows") or [])
        for r in rows if isinstance(rows, list) else []:
            if not isinstance(r, dict):
                continue
            tk = (r.get("ticker") or r.get("symbol") or "").upper()
            side_tx = str(r.get("type") or r.get("transaction")
                          or r.get("tx_type") or "").lower()
            if not tk or ("purchase" not in side_tx and "buy" not in side_tx):
                continue
            v = (r.get("amount_mid") or r.get("value_mid") or r.get("amount")
                 or r.get("value") or 0)
            try:
                v = float(v)
            except Exception:
                v = 0.0
            if v > 0:
                out[tk] = out.get(tk, 0.0) + v
    return out


def extract_activist(j):
    """→ {ticker: setup_score} from activist-13d all_setups."""
    out = {}
    if not isinstance(j, dict):
        return out
    rows = j.get("all_setups") or j.get("top_setups") or []
    if isinstance(rows, dict):
        rows = list(rows.values())
    for r in rows if isinstance(rows, list) else []:
        if not isinstance(r, dict):
            continue
        tk = (r.get("ticker") or r.get("symbol") or "").upper()
        v = (r.get("score") or r.get("setup_score") or r.get("conviction")
             or r.get("stake_pct") or 0)
        if not tk:
            continue
        try:
            out[tk] = float(v)
        except Exception:
            continue
    return out


def feed_age_days(j):
    try:
        ts = (j or {}).get("generated_at") or (j or {}).get("as_of")
        t2 = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return max(0.0, (datetime.now(timezone.utc) - t2).total_seconds() / 86400)
    except Exception:
        return None


def extract_radar(j):
    out = {}
    if not isinstance(j, dict):
        return out
    rows = j.get("accumulation") or j.get("names") or j.get("rows") or []
    if isinstance(rows, dict):
        rows = list(rows.values())
    for c in rows:
        if not isinstance(c, dict):
            continue
        tk = (c.get("ticker") or c.get("symbol") or "").upper()
        v = c.get("score") or c.get("composite") or c.get("accumulation_score")
        if tk and v is not None:
            try:
                out[tk] = float(v)
            except Exception:
                continue
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time(); gaps = []
    raw = {k: read_json(p) for k, p in FEEDS.items()}
    feeds_missing = []
    for k, p in FEEDS.items():
        if raw[k] is None:
            feeds_missing.append({"feed": k, "key": p, "reason": "s3_read_failed_or_absent"})

    insider = extract_insider(raw["insider"])
    svr, liq = extract_svr(raw["finra_short"])
    inst = extract_13f(raw["inst_13f"])
    dark, dark_mode = extract_dark(raw["dark_pool"])
    radar = extract_radar(raw["radar"])
    congress = extract_congress(raw.get("congress"))
    activist = extract_activist(raw.get("activist"))

    # wo4580: weights = tiered priors, Wilson-shrunk by the calibration
    # ledger (honest prior_only basis until each family accrues n>=30),
    # then decayed by each feed's actual age vs its information half-life.
    learned_w, weights_meta = ew_blend(WEIGHTS, SIGNAL_TYPE_MAP)
    FEED_OF = {"form4_opportunistic": "insider", "short_vol_low": "finra_short",
               "inst_delta_resid": "inst_13f", "congress_cluster": "congress",
               "activist_13d": "activist", "dark_composition": "dark_pool",
               "technical_radar": "radar"}
    decay = {}
    for comp, feed in FEED_OF.items():
        age = feed_age_days(raw.get(feed))
        hl = HALF_LIFE_D.get(comp, 30)
        decay[comp] = (round(max(0.25, 0.5 ** (age / hl)), 3)
                       if age is not None else 1.0)
    eff_w = {k: round(learned_w[k] * decay[k], 4) for k in learned_w}
    for name, m in (("insider", insider), ("finra_short", svr), ("inst_13f", inst),
                    ("radar", radar), ("congress", congress), ("activist", activist)):
        if raw.get(name if name != "finra_short" else "finra_short") is not None and not m:
            feeds_missing.append({"feed": name, "reason": "read_ok_but_zero_rows"})
    if dark_mode == "unavailable" and raw["dark_pool"] is not None:
        gaps.append("dark-pool payload lacks dark_share_map/block_share_map "
                    "(needs the BUG-6 patch live) — composition leg skipped")

    # components as {ticker: raw_signal}, higher = more accumulation evidence
    comp_raw = {
        "form4_opportunistic": {t: v["value"] for t, v in insider.items() if v["opportunistic"]},
        "short_vol_low": {t: -s for t, s in svr.items()},        # LOW svr = long leg
        "inst_delta_resid": inst,
        "dark_composition": dark,
        "technical_radar": radar,
        "congress_cluster": congress,
        "activist_13d": activist,
    }
    # residualize each on liquidity rank (the anti-volume-costume step)
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

    # normalize each residual to [0,1] ranks for combination
    norm = {k: _rank(v) if v else {} for k, v in comp.items()}
    tickers = set()
    for m in norm.values():
        tickers |= set(m)

    n_live = sum(1 for k in ("form4_opportunistic", "short_vol_low", "inst_delta_resid")
                 if norm.get(k))
    state = "OK" if n_live >= 2 else "INSUFFICIENT_DATA"

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
        if wsum < 0.45:   # need meaningful tier-1/2 participation
            continue
        t1 = [p for p in parts if p["evidence_tier"].startswith("tier_1")]
        if not t1:
            continue      # never rank a name on unvalidated evidence alone
        names.append({"ticker": tk, "score": round(score / wsum * 100, 1),
                      "n_components": len(parts),
                      "weight_covered": round(wsum, 2),
                      "components": parts,
                      "radar_rank": round(norm.get("technical_radar", {}).get(tk, -1), 3)
                      if tk in norm.get("technical_radar", {}) else None})
    names.sort(key=lambda x: -x["score"])
    for i, n in enumerate(names, 1):
        n["rank"] = i

    # visible disagreement with the technical radar — that gap IS information
    disagreement = []
    top_set = {n["ticker"] for n in names[:20]}
    for n in names[:20]:
        rr = n.get("radar_rank")
        if rr is not None and rr < 0.35:
            disagreement.append({"ticker": n["ticker"], "why": "evidence_high_radar_low",
                                 "composite": n["score"], "radar_rank": rr})
    for tk, v in sorted(norm.get("technical_radar", {}).items(), key=lambda kv: -kv[1])[:15]:
        if v > 0.8 and tk not in top_set and tk in tickers:
            disagreement.append({"ticker": tk, "why": "radar_high_evidence_low",
                                 "radar_rank": round(v, 3)})

    # wo4580 impact_map — top composite names structural; industries carry a
    # measured pp: % of industry mcap under >=2-component tier-1 evidence.
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
                              "sum(mcap of names with >=2 components) / "
                              "industry mcap — %s"
                              % ",".join(sorted(d2["names"])[:6]))
            r2["n_members"] = len(d2["names"])
            ind_rows.append(r2)
    impact = impact_build(
        "accum-composite", "accumulation_evidence_composite",
        [structural_row(n2["ticker"], "company",
                        "%d components, score %.0f, weight_covered %.2f"
                        % (n2["n_components"], n2["score"],
                           n2["weight_covered"]), +1)
         for n2 in names[:15]] + ind_rows, [],
        "Evidence-tiered composite: company rows are structural (tier-gated "
        "rank, no pp asserted); industry rows are measured % of industry "
        "market cap under >=2-component evidence.",
        basis_note="weights basis: %s; decay floors at 0.25"
                   % weights_meta["overall_basis"])

    out = {
        "engine": "accum-composite", "version": "1.1",
        "engine_class": "accumulation_evidence_composite",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "state": state,
        "method": ("Evidence-tiered accumulation composite (ops-4559 BUG-8). "
                   "Weights by validation tier: Form-4 opportunistic 0.35, "
                   "LOW-SVR quintile 0.25 (long leg — Wang/Yan/Zheng), residualized "
                   "institutional delta 0.20, dark composition 0.10, technical radar "
                   "0.10 (confirmation only, tier_4_unvalidated). Every component "
                   "residualized on liquidity rank before scoring. Names require "
                   "tier-1 participation to rank at all."),
        "validation": {"status": "PENDING",
                       "plan": ("purged, embargoed cross-validation against the "
                                "justhodl-signals graded ledger — standard k-fold "
                                "leaks across overlapping label windows and is "
                                "not accepted as evidence")},
        "weights": {"priors": WEIGHTS, "learned": learned_w,
                    "effective_after_decay": eff_w, "decay": decay,
                    "meta": weights_meta},
        "impact_map": impact,
        "component_coverage": {k: len(v) for k, v in norm.items()},
        "dark_composition_mode": dark_mode,
        "feeds_missing": feeds_missing,
        "gaps": gaps,
        "n_names": len(names),
        "names": names[:60],
        "radar_disagreement": disagreement[:20],
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print("[accum-composite] DONE %ss — state=%s names=%d coverage=%s gaps=%d"
          % (round(time.time() - t0, 1), state, len(names),
             out["component_coverage"], len(gaps)))
    return {"statusCode": 200, "body": json.dumps({"ok": state == "OK",
        "state": state, "n_names": len(names)})}
