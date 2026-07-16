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
        # current 13f-positions schema = flat n_funds_* fields; legacy changes_summary kept as fallback
        new_f = sf(p.get("n_funds_new_position")) or sf(cs.get("new")) or 0
        add_f = sf(p.get("n_funds_adding")) or sf(cs.get("added") or cs.get("add")) or 0
        trim_f = sf(p.get("n_funds_trimming")) or sf(cs.get("trimmed") or cs.get("trim")) or 0
        exit_f = sf(p.get("n_funds_exiting")) or sf(cs.get("exited") or cs.get("exit")) or 0
        val_delta = sf(p.get("value_delta_pct")) or sf(p.get("value_change_pct"))
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

    results = sorted([r for r in by_ticker.values() if r["lenses"]],
                     key=lambda r: -abs(r.get("flow_score") or 0))
    accumulating = sorted([r for r in results if r["flow_score"] > 8], key=lambda r: -r["flow_score"])[:40]
    distributing = sorted([r for r in results if r["flow_score"] < -8], key=lambda r: r["flow_score"])[:25]

    # ── intel layers (ops 3348, additive) ──
    # score distribution + smart-money sector footprint
    def _seccounts(rows):
        agg = defaultdict(lambda: {"n": 0, "sum": 0.0})
        for r in rows:
            sec = r.get("sector")
            if not sec:
                continue
            agg[sec]["n"] += 1
            agg[sec]["sum"] += r.get("flow_score") or 0
        return sorted([{"sector": k, "n": v["n"], "avg_score": round(v["sum"] / v["n"], 1)}
                       for k, v in agg.items()], key=lambda x: -x["n"])[:6]
    scores_all = [r.get("flow_score") or 0 for r in results]
    summary = {"n_scored": len(results),
               "n_strong_acc": sum(1 for v in scores_all if v > 25),
               "n_acc": sum(1 for v in scores_all if 8 < v <= 25),
               "n_neutral": sum(1 for v in scores_all if -8 <= v <= 8),
               "n_dis": sum(1 for v in scores_all if -25 <= v < -8),
               "n_strong_dis": sum(1 for v in scores_all if v < -25),
               "top_acc_sectors": _seccounts(accumulating),
               "top_dis_sectors": _seccounts(distributing)}

    # lens conflicts: quarterly 13F footprint vs fresher inst-QoQ disagree in sign
    lens_conflicts = []
    for r in results:
        f = (r.get("detail") or {}).get("13f") or {}
        ic = (r.get("detail") or {}).get("inst_change") or {}
        fs_, is_ = f.get("score"), ic.get("score")
        if fs_ is None or is_ is None:
            continue
        if abs(fs_) > 5 and abs(is_) > 5 and (fs_ > 0) != (is_ > 0):
            lens_conflicts.append({"ticker": r["ticker"], "name": r.get("name"),
                                   "sector": r.get("sector"), "flow_score": r.get("flow_score"),
                                   "f13_score": fs_, "inst_score": is_,
                                   "gap": round(abs(fs_ - is_), 1),
                                   "read": ("QoQ turning UP vs stale 13F" if is_ > 0
                                            else "QoQ turning DOWN vs stale 13F")})
    lens_conflicts.sort(key=lambda x: -x["gap"])
    lens_conflicts = lens_conflicts[:15]

    # new-money board: pure 13F initiations
    top_new_positions = sorted(
        [{"ticker": r["ticker"], "name": r.get("name"), "sector": r.get("sector"),
          "new": ((r.get("detail") or {}).get("13f") or {}).get("new") or 0,
          "added": ((r.get("detail") or {}).get("13f") or {}).get("added") or 0,
          "n_funds": ((r.get("detail") or {}).get("13f") or {}).get("n_funds") or 0,
          "val_delta_pct": ((r.get("detail") or {}).get("13f") or {}).get("val_delta_pct"),
          "flow_score": r.get("flow_score")}
         for r in results if ((r.get("detail") or {}).get("13f") or {}).get("new")],
        key=lambda x: (-x["new"], -x["added"]))[:15]

    # ── v2.0 DOLLAR & INSTITUTION-COUNT LAYER (ops 3360, additive) ──
    # FMP /stable institutional-ownership/symbol-positions-summary = the
    # ALL-institutions view per ticker (every 13F filer, not just tracked
    # funds): total investors holding, how many increased / reduced /
    # initiated / closed, and total invested DOLLARS with QoQ change.
    # Enriches only flagged names (~95/day) — real data, tiny API budget.
    import urllib.request
    from concurrent.futures import ThreadPoolExecutor, as_completed
    FMP_KEY = (os.environ.get("FMP_API_KEY") or os.environ.get("FMP_KEY")
               or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
    FMP_BASE = "https://financialmodelingprep.com/stable"

    def _fmp(path, params, timeout=20):
        url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
        for att in range(3):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/2.0"})
                return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
            except Exception:
                if att == 2:
                    return None
                time.sleep(0.8 * (att + 1))

    def _latest_13f_quarter():
        # Only FULLY-FILED quarters: 13F deadline = 45d after quarter-end, so
        # require >=60d past q-end (screener doctrine), THEN confirm
        # completeness empirically: AAPL always has thousands of holders in a
        # fully-filed quarter (ops 3360: mid-filing Q2-2026 showed only 946 and
        # a garbage -$2.3T "change").
        from datetime import date as _date
        now = datetime.now(timezone.utc)
        qends = []
        for yy in (now.year, now.year - 1):
            for q in (4, 3, 2, 1):
                m = q * 3
                qends.append((yy, q, _date(yy, m, 30 if m in (6, 9) else 31)))
        eligible = [(yy, qq, qd) for yy, qq, qd in qends
                    if (now.date() - qd).days >= 60]
        eligible.sort(key=lambda x: -x[2].toordinal())
        for yy, qq, _qd in eligible[:4]:
            js = _fmp("institutional-ownership/symbol-positions-summary",
                      f"&symbol=AAPL&year={yy}&quarter={qq}")
            if isinstance(js, list) and js:
                holders = sf(js[0].get("investorsHolding")) or 0
                if holders > 3000:
                    return yy, qq
                print(f"[capital-flow] Q{qq} {yy} incomplete "
                      f"({int(holders)} AAPL holders) — skipping")
        return None, None

    def _g(rec, *names):
        for n in names:
            v = sf(rec.get(n))
            if v is not None:
                return v
        return None

    flagged, _seen = [], set()
    for coll in (accumulating, distributing, lens_conflicts, top_new_positions):
        for r in coll:
            tk = r.get("ticker")
            if tk and tk not in _seen:
                _seen.add(tk)
                flagged.append(tk)

    inst_deep = {}
    q_year = q_q = None
    try:
        q_year, q_q = _latest_13f_quarter()
    except Exception as e:
        print(f"[capital-flow] quarter probe failed: {e!r}")
    if q_year:
        def _pull(tk):
            js = _fmp("institutional-ownership/symbol-positions-summary",
                      f"&symbol={tk}&year={q_year}&quarter={q_q}")
            if not (isinstance(js, list) and js):
                return tk, None
            rec = js[0]
            return tk, {
                "investors": _g(rec, "investorsHolding"),
                "investors_chg": _g(rec, "investorsHoldingChange"),
                "new_pos": _g(rec, "newPositions"),
                "closed_pos": _g(rec, "closedPositions"),
                "increased_pos": _g(rec, "increasedPositions"),
                "reduced_pos": _g(rec, "reducedPositions"),
                "invested_usd": _g(rec, "totalInvested"),
                "invested_chg_usd": _g(rec, "totalInvestedChange"),
                "ownership_pct": _g(rec, "ownershipPercent"),
                "put_call_ratio": _g(rec, "putCallRatio"),
                "quarter": f"Q{q_q} {q_year}",
            }
        with ThreadPoolExecutor(max_workers=8) as ex:
            for fut in as_completed([ex.submit(_pull, tk) for tk in flagged]):
                try:
                    tk, d = fut.result()
                    if d:
                        inst_deep[tk] = d
                except Exception:
                    pass
    for tk, d in inst_deep.items():
        if tk in by_ticker:
            by_ticker[tk]["detail"]["inst_deep"] = d

    # famous-funds dollar join — data/13f-flows-by-ticker.json is the in-house
    # dv ledger over the tracked-fund universe (whale = clone-alpha skill>=55)
    fj = (read_json("data/13f-flows-by-ticker.json") or {}).get("t") or {}
    n_f13join = 0
    for tk in flagged:
        w = fj.get(tk)
        if w and tk in by_ticker:
            by_ticker[tk]["detail"]["funds13f"] = {
                "bought_usd": w.get("b"), "sold_usd": w.get("s"),
                "net_usd": w.get("n"), "whale_net_usd": w.get("wn"),
                "n_funds": w.get("nf"), "held_usd": w.get("tv"),
                "buying": (w.get("fb") or [])[:3], "selling": (w.get("fs") or [])[:3]}
            n_f13join += 1

    # dollar boards + all-institution breadth
    def _dr(tk):
        r = by_ticker.get(tk) or {}
        d = (r.get("detail") or {}).get("inst_deep") or {}
        f = (r.get("detail") or {}).get("funds13f") or {}
        return {"ticker": tk, "name": r.get("name"), "sector": r.get("sector"),
                "flow_score": r.get("flow_score"),
                "invested_usd": d.get("invested_usd"),
                "invested_chg_usd": d.get("invested_chg_usd"),
                "investors": d.get("investors"), "investors_chg": d.get("investors_chg"),
                "increased_pos": d.get("increased_pos"), "reduced_pos": d.get("reduced_pos"),
                "new_pos": d.get("new_pos"), "closed_pos": d.get("closed_pos"),
                "ownership_pct": d.get("ownership_pct"),
                "funds_net_usd": f.get("net_usd"), "whale_net_usd": f.get("whale_net_usd"),
                "top_buyers": (f.get("buying") or [])[:2],
                "top_sellers": (f.get("selling") or [])[:2]}
    with_usd = [_dr(tk) for tk in flagged
                if (inst_deep.get(tk) or {}).get("invested_chg_usd") is not None]
    dollar_flow_in = sorted([r for r in with_usd if (r["invested_chg_usd"] or 0) > 0],
                            key=lambda x: -(x["invested_chg_usd"] or 0))[:20]
    dollar_flow_out = sorted([r for r in with_usd if (r["invested_chg_usd"] or 0) < 0],
                             key=lambda x: (x["invested_chg_usd"] or 0))[:20]

    acc_set = {r["ticker"] for r in accumulating}
    dis_set = {r["ticker"] for r in distributing}

    def _sumf(keys, field):
        return sum((inst_deep.get(t) or {}).get(field) or 0 for t in keys)
    inst_breadth = {
        "quarter": (f"Q{q_q} {q_year}" if q_year else None),
        "n_enriched": len(inst_deep),
        "usd_chg_acc": round(_sumf(acc_set & set(inst_deep), "invested_chg_usd")),
        "usd_chg_dis": round(_sumf(dis_set & set(inst_deep), "invested_chg_usd")),
        "investors_acc": round(_sumf(acc_set & set(inst_deep), "investors")),
        "investors_dis": round(_sumf(dis_set & set(inst_deep), "investors")),
        "increased_sum": round(_sumf(inst_deep, "increased_pos")),
        "reduced_sum": round(_sumf(inst_deep, "reduced_pos")),
        "new_sum": round(_sumf(inst_deep, "new_pos")),
        "closed_sum": round(_sumf(inst_deep, "closed_pos")),
    }
    inst_breadth["usd_chg_net"] = round((inst_breadth["usd_chg_acc"] or 0)
                                        + (inst_breadth["usd_chg_dis"] or 0))

    sec_usd = defaultdict(lambda: {"usd": 0.0, "n": 0, "inv": 0})
    for tk, d in inst_deep.items():
        sec = (by_ticker.get(tk) or {}).get("sector")
        if not sec or d.get("invested_chg_usd") is None:
            continue
        sec_usd[sec]["usd"] += d["invested_chg_usd"]
        sec_usd[sec]["n"] += 1
        sec_usd[sec]["inv"] += d.get("investors_chg") or 0
    sector_dollar_flows = sorted(
        [{"sector": k, "usd_chg": round(v["usd"]), "n": v["n"],
          "investors_chg": round(v["inv"])} for k, v in sec_usd.items()],
        key=lambda x: -x["usd_chg"])

    # history ledger → per-name score momentum + NEW-today flags
    hist = read_json("data/capital-flow-history.json") or {"entries": []}
    entries = [e for e in (hist.get("entries") or []) if isinstance(e, dict)]
    today = datetime.now(timezone.utc).date().isoformat()
    prev = None
    for e in reversed(entries):
        if e.get("date") and e["date"] < today:
            prev = e
            break
    prev_scores = (prev or {}).get("flagged_scores") or {}
    for tk in flagged:
        r = by_ticker.get(tk)
        if not r:
            continue
        ps = sf(prev_scores.get(tk))
        r["detail"]["momentum"] = {
            "prev_score": ps,
            "score_delta": (round((r.get("flow_score") or 0) - ps, 1)
                            if ps is not None else None),
            "new_today": (prev is not None and ps is None)}
    entries = ([e for e in entries if e.get("date") != today]
               + [{"date": today, "n_scored": len(results),
                   "n_strong_acc": summary["n_strong_acc"],
                   "n_strong_dis": summary["n_strong_dis"],
                   "usd_chg_net": inst_breadth.get("usd_chg_net"),
                   "flagged_scores": {tk: (by_ticker.get(tk) or {}).get("flow_score")
                                      for tk in flagged}}])[-120:]
    try:
        s3.put_object(Bucket=BUCKET, Key="data/capital-flow-history.json",
                      Body=json.dumps({"as_of": datetime.now(timezone.utc).isoformat(),
                                       "entries": entries}, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="public, max-age=3600")
    except Exception as e:
        print(f"[capital-flow] history write failed: {e!r}")

    output = {
        "engine": "capital-flow", "version": "2.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "sources": {"13f": bool(agg), "etf_flows": len(etf_flows), "inst_change": len(inst_scores), "categories": len(cat_rotation),
                    "inst_deep": len(inst_deep), "funds13f_join": n_f13join},
        "quarter_13f": inst_breadth.get("quarter"),
        "inst_breadth": inst_breadth,
        "dollar_flow_in": dollar_flow_in,
        "dollar_flow_out": dollar_flow_out,
        "sector_dollar_flows": sector_dollar_flows,
        "methodology": ("Fuses 13F position changes (new/add/trim/exit + $ delta + "
                        "#funds), institutional QoQ ownership change (shares %, "
                        "investor count), and ETF net flows into one capital-flow "
                        "score (-100..+100). Positive = capital accumulating."),
        "accumulating": accumulating,
        "distributing": distributing,
        "etf_flows_in": etf_flows[:25],
        "etf_flows_out": [e for e in etf_flows if ((e.get("net_flow_5d_usd") if e.get("true_flow") else e.get("flow_proxy")) or 0) < 0][:15],
        "category_rotation": cat_rotation[:15],
        "summary": summary,
        "lens_conflicts": lens_conflicts,
        "top_new_positions": top_new_positions,
        "by_ticker": {r["ticker"]: r for r in results[:300]},
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[capital-flow] DONE {round(time.time()-t0,1)}s — {len(accumulating)} accumulating, "
          f"{len(distributing)} distributing, {len(etf_flows)} ETF flows, "
          f"{len(inst_deep)} $-enriched (Q{q_q} {q_year}), {n_f13join} funds13f joins, "
          f"net inst $ {inst_breadth.get('usd_chg_net')}")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "accumulating": len(accumulating),
                                                     "distributing": len(distributing),
                                                     "etf_flows": len(etf_flows)})}
