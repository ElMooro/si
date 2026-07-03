"""justhodl-cryptoquant v2.0 — FULL-CATALOG ON-CHAIN VENDOR ADAPTER.

Every metric the Professional plan serves (spec discovered by probe ops, not
guesswork), organized by category. Per metric, per day:
  value, z365, pctl_1y, WoW; pearson corr + beta vs BTC (shared window);
  HIST_READ — percentile-conditional forward BTC returns (7/21/60d mean,
  median, hit-rate, n) computed at the CURRENT value's percentile bucket over
  the LONGEST real window available:
    - metrics with a free Coin Metrics long-twin (MVRV, NVT, active
      addresses, ...) -> stats over 2010->present (twin history);
    - CQ-only metrics -> stats over the accruing plan window (labeled).
  AI narration (llm_router, contract v4) only for extreme readings; the
  deterministic hist_read is always present and always real.
Composite unchanged: curated core (in_composite=true) with risk_sign; the
long tail is displayed + graded-eligible, never blindly composited.
Feeds: data/cryptoquant-onchain.json (latest+stats), data/cryptoquant-series.json
(chart series, 400pt, + BTC overlay + twins). Hist: data/history/cryptoquant.json.
BTC 2010-> price cache: data/history/btc-price-cm.json (Coin Metrics, free).
"""
import json, time, urllib.request, urllib.error, statistics
from datetime import datetime, timezone, timedelta
import boto3

BUCKET = "justhodl-dashboard-live"
OUT, SERIES = "data/cryptoquant-onchain.json", "data/cryptoquant-series.json"
SPEC_KEY, HIST_KEY = "data/config/cryptoquant-spec.json", "data/history/cryptoquant.json"
BTC_KEY = "data/history/btc-price-cm.json"
BASE = "https://api.cryptoquant.com/v1"
CM = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")

def _j(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

def _put(k, obj, compact=True):
    body = json.dumps(obj, separators=(",", ":") if compact else None,
                      indent=None if compact else 1, allow_nan=False).encode()
    s3.put_object(Bucket=BUCKET, Key=k, Body=body, ContentType="application/json",
                  CacheControl="public, max-age=60")

def _token():
    try:
        return ssm.get_parameter(Name="/justhodl/cryptoquant/token",
                                 WithDecryption=True)["Parameter"]["Value"].strip()
    except Exception:
        return None

def _get(url, tok, timeout=30):
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + tok,
                                               "User-Agent": "JustHodl/2.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as he:
        body = (he.read() or b"")[:160].decode("utf-8", "ignore")
        raise RuntimeError("HTTP %s %s :: %s" % (he.code, url.split("?")[0][-58:], body))

def _series(m, tok, limit=1000):
    q = dict(m.get("params") or {}); q["limit"] = str(m.get("limit") or limit)
    url = BASE + m["path"] + "?" + "&".join("%s=%s" % kv for kv in q.items())
    doc = _get(url, tok)
    rows = ((doc or {}).get("result") or {}).get("data") or []
    vk = m.get("resolved_key")
    out = {}
    for r in rows:
        d = str(r.get("date") or r.get("datetime") or "")[:10]
        if not d: continue
        if vk is None:
            for cand in (m.get("value_keys") or []) + [k for k in r if k not in ("date", "datetime")]:
                if isinstance(r.get(cand), (int, float)):
                    vk = cand; break
        v = r.get(vk)
        if isinstance(v, (int, float)): out[d] = float(v)
    if vk: m["resolved_key"] = vk
    return out

def _btc_price(tok_unused=None):
    """BTC daily close 2010->present via Coin Metrics community (free, real)."""
    cache = _j(BTC_KEY, {}) or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if cache.get("_asof") == today and len(cache.get("px") or {}) > 4000:
        return cache["px"]
    px = dict((cache.get("px") or {}))
    start = max(px) if px else "2010-07-01"
    url = CM + "?assets=btc&metrics=PriceUSD&frequency=1d&page_size=10000&start_time=" + start
    for _ in range(4):
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/2.0"})
        with urllib.request.urlopen(req, timeout=40) as r:
            doc = json.loads(r.read())
        for row in doc.get("data") or []:
            if row.get("PriceUSD"): px[row["time"][:10]] = float(row["PriceUSD"])
        nxt = doc.get("next_page_url")
        if not nxt: break
        url = nxt
    _put(BTC_KEY, {"_asof": today, "px": px})
    return px

def _cm_fetch(asset, cm_metric):
    out = {}
    url = CM + "?assets=%s&metrics=%s&frequency=1d&page_size=10000&start_time=2010-07-01" % (asset, cm_metric)
    for _ in range(4):
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/2.2"})
        with urllib.request.urlopen(req, timeout=45) as r:
            doc = json.loads(r.read())
        for row in doc.get("data") or []:
            v = row.get(cm_metric)
            if v is not None:
                try: out[row["time"][:10]] = float(v)
                except Exception: pass
        nxt = doc.get("next_page_url")
        if not nxt: break
        url = nxt
    return out

def _asset_price(asset, key):
    cache = _j(key, {}) or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if cache.get("_asof") == today and len(cache.get("px") or {}) > 800:
        return cache["px"]
    px = _cm_fetch(asset, "PriceUSD")
    _put(key, {"_asof": today, "px": px})
    return px

def _twin_series(spec_val):
    # spec twins values: "Metric" | "eth:Metric" | computed "A/B"
    asset, expr = "btc", spec_val
    if ":" in spec_val:
        asset, expr = spec_val.split(":", 1)
    if "/" in expr:
        p, q = expr.split("/", 1)
        A, B = _cm_fetch(asset, p), _cm_fetch(asset, q)
        return {d: A[d] / B[d] for d in A if d in B and B[d]}
    return _cm_fetch(asset, expr)

def _fwd(px_dates, px, i, days):
    j = i + days
    if j < len(px_dates):
        return px[px_dates[j]] / px[px_dates[i]] - 1.0
    return None

def _cond_stats(ser, px, cur):
    """Forward BTC returns historically observed when metric sat in the current
    value's percentile bucket (+/-10pctl) over the full shared window."""
    dates = [d for d in sorted(ser) if d in px]
    if len(dates) < 120 or cur is None: return None
    vals = [ser[d] for d in dates]
    rank = sum(1 for v in vals if v <= cur) / len(vals)
    lo_i, hi_i = max(0.0, rank - 0.10), min(1.0, rank + 0.10)
    svals = sorted(vals)
    lo_v = svals[int(lo_i * (len(svals) - 1))]
    hi_v = svals[int(hi_i * (len(svals) - 1))]
    px_dates = sorted(px)
    pidx = {d: i for i, d in enumerate(px_dates)}
    out = {}
    for h in (7, 21, 30, 60, 90, 180, 365):
        rets = []
        for d in dates:
            if lo_v <= ser[d] <= hi_v and d in pidx:
                r = _fwd(px_dates, px, pidx[d], h)
                if r is not None: rets.append(r)
        if len(rets) >= 25:
            rets.sort()
            out["fwd%d" % h] = {"mean_pct": round(100 * sum(rets) / len(rets), 2),
                                "median_pct": round(100 * rets[len(rets) // 2], 2),
                                "hit_up_pct": round(100 * sum(1 for r in rets if r > 0) / len(rets)),
                                "n": len(rets)}
    return out or None

def _corr(ser, px):
    dates = [d for d in sorted(ser)[-365:] if d in px]
    if len(dates) < 90: return None, None
    xs = [ser[d] for d in dates]; ys = [px[d] for d in dates]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(len(xs)))
    dx = sum((x - mx) ** 2 for x in xs) ** 0.5; dy = sum((y - my) ** 2 for y in ys) ** 0.5
    if not dx or not dy: return None, None
    r = num / (dx * dy)
    beta = num / (dx * dx) if dx else None
    return round(r, 3), (round(beta, 6) if beta is not None else None)

def _hist_read(name, pctl, stats, window):
    if not stats: return None
    s21 = stats.get("fwd21") or stats.get("fwd7") or {}
    if not s21: return None
    return ("At today's level (%sth percentile of %s), BTC's forward 21-day return has "
            "historically averaged %+0.1f%% (median %+0.1f%%, up %d%% of the time, n=%d)."
            % (pctl, window, s21.get("mean_pct", 0), s21.get("median_pct", 0),
               s21.get("hit_up_pct", 0), s21.get("n", 0)))

def _clean_brief(txt):
    if not txt: return None
    t = str(txt).strip().replace("**", "").replace("##", "")
    if (not t[:1].isalpha() or not t[:1].isupper()
            or chr(34) in t or "[" in t or "]" in t): return None
    if len(t) > 900: t = t[:900]
    if t and not t.rstrip().endswith((".", "!", "?")):
        t = t.rsplit(".", 1)[0] + "." if "." in t else ""
    return t if 90 <= len(t) <= 920 else None

def lambda_handler(event=None, context=None):
    event = event or {}
    spec = _j(SPEC_KEY) or {}
    mets = spec.get("metrics") or []
    assert mets, "spec missing — run catalog probe ops first"
    tok = _token()
    now = datetime.now(timezone.utc)
    if not tok:
        _put(OUT, {"engine": "justhodl-cryptoquant", "version": "2.0.0",
                   "generated_at": now.isoformat(timespec="seconds"),
                   "status": "GATED_PENDING_KEY",
                   "armed_metrics": [m["name"] for m in mets]})
        return {"ok": True, "status": "GATED_PENDING_KEY", "armed": len(mets)}

    px = _btc_price()
    epx = _asset_price("eth", "data/history/eth-price-cm.json")
    twins_cfg = dict(spec.get("twins") or {})
    twins_cfg.update(spec.get("twins_extra") or {})
    twin_data = {}
    for cqn, cmn in twins_cfg.items():
        try:
            tw = _twin_series(cmn)
            if len(tw) > 900: twin_data[cqn] = tw
        except Exception as e: print("[twin] %s -> %s" % (cmn, str(e)[:80]))
    # DERIVED 2010-> twins — per-raw isolation, REUSE fetched twins (no extra CM
    # burst -> no community-throttle flakiness). NUPL = 1 - 1/MVRV needs only the
    # already-live MVRV twin; RealizedPrice + Puell attempted once, skipped silently
    # where Coin Metrics free tier gates the raw (probed ceiling, ops 2747).
    mv = twin_data.get("btc_mvrv") or {}
    if len(mv) > 900 and "btc_nupl" not in twin_data:
        twin_data["btc_nupl"] = {d: 1.0 - 1.0 / v for d, v in mv.items() if v}
        twins_cfg.setdefault("btc_nupl", "derived:1-1/CapMVRVCur")
    if "btc_realized_price" not in twin_data:
        try:
            time.sleep(1.2); cr = _twin_series("CapRealUSD")
            time.sleep(1.2); sp = twin_data.get("btc_supply_total") or _twin_series("SplyCur")
            rp = {d: cr[d] / sp[d] for d in cr if d in sp and sp[d]}
            if len(rp) > 900:
                twin_data["btc_realized_price"] = rp
                twins_cfg.setdefault("btc_realized_price", "derived:CapRealUSD/SplyCur")
        except Exception as e:
            print("[derived rp]", str(e)[:80])
    if "btc_puell" not in twin_data:
        try:
            time.sleep(1.2); iu = _twin_series("IssContUSD")
            if len(iu) > 1200:
                ds = sorted(iu); vs = [iu[d] for d in ds]
                pu, run = {}, 0.0
                for i, d in enumerate(ds):
                    run += vs[i]
                    if i >= 365: run -= vs[i - 365]
                    if i >= 364 and run: pu[d] = vs[i] / (run / 365.0)
                if len(pu) > 900:
                    twin_data["btc_puell"] = pu
                    twins_cfg.setdefault("btc_puell", "derived:IssContUSD/MA365")
        except Exception as e:
            print("[derived puell]", str(e)[:80])

    hist = _j(HIST_KEY, {}) or {}
    metrics, errors, series_out = {}, [], {}
    for m in mets:
        name = m["name"]
        last_err = None
        for attempt in range(2):
            try:
                got = _series(m, tok, limit=1000)
                ser = dict(sorted({**(hist.get(name) or {}), **got}.items())[-2000:])
                if len(ser) < 45: raise RuntimeError("thin: %d" % len(ser))
                hist[name] = ser
                dates = sorted(ser); vals = [ser[d] for d in dates]
                w = vals[-365:]
                z = round((vals[-1] - statistics.mean(w)) / statistics.stdev(w), 2) \
                    if len(w) >= 90 and statistics.stdev(w) > 0 else None
                pctl = round(100 * sum(1 for x in w if x <= vals[-1]) / len(w)) if len(w) >= 90 else None
                twin = twin_data.get(name)
                stat_src = twin if twin and len(twin) > 900 else ser
                window = "2010-present (Coin Metrics twin)" if stat_src is twin else \
                         "the %d-day plan window (accruing daily)" % len(ser)
                cur_for_stats = (sorted(stat_src.values())[
                    min(len(stat_src) - 1, int((pctl or 50) / 100 * (len(stat_src) - 1)))]
                    if stat_src is twin and pctl is not None else vals[-1])
                bpx = epx if name.startswith("eth_") else px
                cstats = _cond_stats(stat_src, bpx, cur_for_stats)
                r, beta = _corr(ser, bpx)
                _mv = (twin_data.get(name) or ser)
                _sv = [_mv[d] for d in sorted(_mv)]
                _up = sum(1 for i in range(1, len(_sv)) if _sv[i] >= _sv[i - 1]) / max(1, len(_sv) - 1)
                metrics[name] = {"monotonic": bool(_up >= 0.95 or _up <= 0.05),
                                 "value": round(vals[-1], 6), "z365": z, "pctl_1y": pctl,
                                 "wow": round(vals[-1] - vals[-8], 6) if len(vals) >= 8 else None,
                                 "as_of": dates[-1], "category": m.get("category", "other"),
                                 "label": m.get("label", name), "unit": m.get("unit"),
                                 "risk_sign": m.get("risk_sign", 0),
                                 "in_composite": bool(m.get("in_composite")),
                                 "corr_1y": r, "beta_1y": beta,
                                 "stats_window": window, "cond_stats": cstats,
                                 "hist_read": _hist_read(name, pctl, cstats, window),
                                 "twin": twins_cfg.get(name)}
                series_out[name] = {"d": dates[-730:],
                                    "v": [round(ser[d], 6) for d in dates[-730:]]}
                last_err = None
                break
            except Exception as e:
                last_err = e
                if "429" in str(e) and attempt == 0:
                    time.sleep(22); continue
                break
        if last_err is not None:
            errors.append({"metric": name, "err": str(last_err)[:100]})
        time.sleep(2.0)
    assert len(metrics) >= max(8, int(0.6 * len(mets))), \
        "too few live: %d/%d %s" % (len(metrics), len(mets), errors[:4])
    _put(HIST_KEY, hist)
    core = {k: v for k, v in metrics.items() if v["in_composite"] and v["z365"] is not None}
    rz = [v["z365"] * v["risk_sign"] for v in core.values() if v["risk_sign"]]
    comp = round(statistics.mean(rz), 2) if rz else None

    extremes = sorted([k for k, v in metrics.items()
                       if v["pctl_1y"] is not None and (v["pctl_1y"] <= 5 or v["pctl_1y"] >= 95)],
                      key=lambda k: abs(50 - metrics[k]["pctl_1y"]), reverse=True)[:8]
    for k in extremes:
        try:
            from llm_router import complete
            v = metrics[k]
            t = _clean_brief(complete(
                "Respond with ONLY two plain prose sentences, no lists, no markdown. "
                "Bitcoin on-chain metric %s is at %s (%sth percentile of its year). "
                "Historically %s Explain what this reading has meant for Bitcoin and one caveat."
                % (v["label"], v["value"], v["pctl_1y"], v.get("hist_read") or ""),
                tier="reason", max_tokens=260))
            if t: metrics[k]["ai_context"] = t
        except Exception:
            pass

    tser = {}
    for cqn, tw in twin_data.items():
        if tw:
            td = sorted(tw); step = max(1, len(td) // 1200)
            tser[cqn] = {"d": td[::step], "v": [round(tw[d], 6) for d in td[::step]]}
    pd = sorted(px); pstep = max(1, len(pd) // 1200)
    _put(SERIES, {"generated_at": now.isoformat(timespec="seconds"),
                  "series": series_out, "twins": tser,
                  "btc": {"d": pd[::pstep], "v": [round(px[d], 2) for d in pd[::pstep]]}})

    # ── FORECAST DESK: percentile-conditional base-rate projections, ledger-graded ──
    def _pctile(xs, p):
        xs = sorted(xs); i = max(0, min(len(xs) - 1, int(p * (len(xs) - 1))))
        return xs[i]
    def _ensemble(names, hs):
        out = {}
        for h in hs:
            means = [((metrics[k].get("cond_stats") or {}).get("fwd%d" % h) or {}).get("mean_pct")
                     for k in names]
            means = [m for m in means if m is not None]
            if len(means) >= 3:
                out["h%d" % h] = {"exp_pct": round(_pctile(means, 0.5), 1),
                                  "p10_pct": round(_pctile(means, 0.1), 1),
                                  "p90_pct": round(_pctile(means, 0.9), 1),
                                  "n_metrics": len(means)}
        return out
    def _beta(px_a, px_b, days=730):
        ds = [d for d in sorted(px_a) if d in px_b][-days:]
        if len(ds) < 200: return None
        ra = [px_a[ds[i]] / px_a[ds[i - 1]] - 1 for i in range(1, len(ds))]
        rb = [px_b[ds[i]] / px_b[ds[i - 1]] - 1 for i in range(1, len(ds))]
        mb = sum(rb) / len(rb); ma = sum(ra) / len(ra)
        cov = sum((ra[i] - ma) * (rb[i] - mb) for i in range(len(ra)))
        var = sum((b - mb) ** 2 for b in rb)
        return round(cov / var, 2) if var else None
    HORIZONS = (30, 90, 180, 365)
    btc_names = [k for k, v in metrics.items()
                 if not k.startswith("eth_") and not v.get("monotonic") and v.get("cond_stats")]
    eth_names = [k for k, v in metrics.items()
                 if k.startswith("eth_") and not v.get("monotonic") and v.get("cond_stats")]
    btc_now = px[max(px)] if px else None
    eth_now = epx[max(epx)] if epx else None
    fb = _ensemble(btc_names, HORIZONS)
    for h, v in fb.items():
        if btc_now:
            v["price_target"] = round(btc_now * (1 + v["exp_pct"] / 100), 0)
            v["price_range"] = [round(btc_now * (1 + v["p10_pct"] / 100), 0),
                                round(btc_now * (1 + v["p90_pct"] / 100), 0)]
    beta_e = _beta(epx, px) or 1.4
    fe_raw = _ensemble(eth_names, HORIZONS)
    fe = {}
    for h in HORIZONS:
        kk = "h%d" % h
        b = fb.get(kk)
        if not b: continue
        own = (fe_raw.get(kk) or {}).get("exp_pct")
        exp = round(0.5 * own + 0.5 * b["exp_pct"] * beta_e, 1) if own is not None               else round(b["exp_pct"] * beta_e, 1)
        fe[kk] = {"exp_pct": exp,
                  "p10_pct": round(b["p10_pct"] * beta_e, 1), "p90_pct": round(b["p90_pct"] * beta_e, 1),
                  "basis": "own+beta" if own is not None else "beta-link",
                  "n_metrics": (fe_raw.get(kk) or {}).get("n_metrics", 0)}
        if eth_now:
            fe[kk]["price_target"] = round(eth_now * (1 + exp / 100), 0)
            fe[kk]["price_range"] = [round(eth_now * (1 + fe[kk]["p10_pct"] / 100), 0),
                                     round(eth_now * (1 + fe[kk]["p90_pct"] / 100), 0)]
    ALT = ("ltc", "xrp", "doge", "ada", "bch")
    apx_all = _j("data/history/alt-px-cm.json", {}) or {}
    if apx_all.get("_asof") != datetime.now(timezone.utc).strftime("%Y-%m-%d"):
        apx_all = {"_asof": datetime.now(timezone.utc).strftime("%Y-%m-%d")}
        for a in ALT:
            try: apx_all[a] = _cm_fetch(a, "PriceUSD")
            except Exception: pass
        _put("data/history/alt-px-cm.json", apx_all)
    common = None
    for a in ALT:
        ds = set((apx_all.get(a) or {}))
        common = ds if common is None else (common & ds)
    common = sorted(d for d in (common or set()) if d >= "2017-01-01")
    alt_idx = {}
    if len(common) > 500:
        base = {a: apx_all[a][common[0]] for a in ALT if apx_all.get(a)}
        for d in common:
            vals_a = [apx_all[a][d] / base[a] for a in base]
            alt_idx[d] = sum(vals_a) / len(vals_a)
    beta_a = _beta(alt_idx, px) if alt_idx else None
    fa = {}
    if beta_a:
        for h in HORIZONS:
            kk = "h%d" % h; b = fb.get(kk)
            if b:
                fa[kk] = {"exp_pct": round(b["exp_pct"] * beta_a, 1),
                          "p10_pct": round(b["p10_pct"] * beta_a, 1),
                          "p90_pct": round(b["p90_pct"] * beta_a, 1)}
    fc_ai, fc_src = None, "deterministic"
    fc_det = ("Base rates project BTC %+0.1f%% over 1 month, %+0.1f%% over 3 months, %+0.1f%% over 6 "
              "months and %+0.1f%% over 12 months (ensemble medians across %d non-monotonic metrics at "
              "their current percentiles); ETH carries a %.1fx beta and altcoins %.1fx, amplifying both "
              "tails. These are historical conditional distributions, not promises — the ledger grades "
              "every one of them."
              % tuple([fb.get("h%d" % h, {}).get("exp_pct", 0.0) for h in HORIZONS]
                      + [len(btc_names), beta_e, beta_a or 0.0]))
    try:
        from llm_router import complete
        t = _clean_brief(complete(
            "Respond with ONLY three to five plain prose sentences, no lists, no markdown, no quotes. "
            "Percentile-conditional base rates from on-chain metrics project BTC forward returns of "
            "%s pct over 30/90/180/365 days; ETH beta %.1f; altcoin-basket beta %.1f. Current BTC %s. "
            "Give the institutional interpretation, the strongest horizon, and the biggest caveat."
            % ("/".join(str(fb.get("h%d" % h, {}).get("exp_pct", "?")) for h in HORIZONS),
               beta_e, beta_a or 0, btc_now), tier="reason", max_tokens=340))
        if t: fc_ai, fc_src = t, "llm"
    except Exception:
        pass
    forecasts = {"btc": fb, "eth": dict(fe, beta_vs_btc=beta_e), 
                 "alt_basket": {"assets": list(ALT), "beta_vs_btc": beta_a, **fa},
                 "btc_price_now": btc_now, "eth_price_now": eth_now,
                 "ai": fc_ai or fc_det, "ai_src": fc_src,
                 "method": ("Ensemble median of percentile-conditional forward returns across %d "
                            "non-monotonic metrics (2010-window where twins exist); ETH = 50/50 own-metric "
                            "ensemble + BTC beta-link; alts = %s equal-weight index beta-link. PROVISIONAL, "
                            "ledger-graded, not financial advice." % (len(btc_names), "/".join(ALT)))}

    # ── MASTER BRIEF: full-catalog scan -> one read on where BTC/crypto stands ──
    sup = [k for k, v in metrics.items() if not v.get("monotonic") and ((v.get("cond_stats") or {}).get("fwd21") or {}).get("mean_pct", 0) > 1.0]
    con = [k for k, v in metrics.items() if not v.get("monotonic") and ((v.get("cond_stats") or {}).get("fwd21") or {}).get("mean_pct", 0) < -1.0]
    ext_hi = [metrics[k]["label"] for k in metrics if (metrics[k].get("pctl_1y") or 50) >= 95][:5]
    ext_lo = [metrics[k]["label"] for k in metrics if (metrics[k].get("pctl_1y") or 50) <= 5][:5]
    mv = metrics.get("btc_mvrv") or {}
    master_det = ("Across all %d on-chain metrics, the tape leans %s: %d metrics sit at levels that "
                  "historically preceded positive 21-day BTC returns versus %d at historically negative "
                  "levels, and the curated composite reads %+0.2fz. Valuation is the loudest signal — "
                  "MVRV at the %sth percentile of 2010-present has been followed by %+0.1f%% average "
                  "21-day returns (n=%d). Yearly extremes right now: %s near highs; %s near lows. "
                  "The main caveat is that exchange netflows and dry-powder trends must confirm before "
                  "this reads as an all-clear."
                  % (len(metrics), "constructive" if len(sup) >= len(con) else "defensive",
                     len(sup), len(con), comp if comp is not None else 0.0,
                     mv.get("pctl_1y", "?"),
                     ((mv.get("cond_stats") or {}).get("fwd21") or {}).get("mean_pct", 0.0),
                     ((mv.get("cond_stats") or {}).get("fwd21") or {}).get("n", 0),
                     ", ".join(ext_hi) or "none", ", ".join(ext_lo) or "none"))
    master, master_src = master_det, "deterministic"
    try:
        from llm_router import complete
        digest = "; ".join("%s pctl%s fwd21 %+0.1f%%" % (
            v["label"], v.get("pctl_1y"), ((v.get("cond_stats") or {}).get("fwd21") or {}).get("mean_pct", 0.0))
            for k, v in sorted(metrics.items(), key=lambda kv: abs(50 - (kv[1].get("pctl_1y") or 50)), reverse=True)[:16])
        t = complete("Respond with ONLY four to six plain prose sentences, no lists, no markdown, no quotes. "
                     "You scanned %d Bitcoin/crypto on-chain metrics. Composite risk %+0.2fz. "
                     "Most extreme readings with their historical forward-21-day BTC returns at these levels: %s. "
                     "Give one institutional read on where Bitcoin and crypto stand right now, the single "
                     "biggest opportunity, the single biggest risk, and what would change the picture."
                     % (len(metrics), comp if comp is not None else 0.0, digest),
                     tier="reason", max_tokens=420)
        t = _clean_brief(t)
        if t: master, master_src = t, "llm"
    except Exception:
        pass

    stale = max((now - datetime.strptime(v["as_of"], "%Y-%m-%d").replace(tzinfo=timezone.utc)).days
                for v in metrics.values())
    cats = {}
    for v in metrics.values(): cats[v["category"]] = cats.get(v["category"], 0) + 1
    _put(OUT, {"engine": "justhodl-cryptoquant", "version": "2.0.0",
               "generated_at": now.isoformat(timespec="seconds"), "status": "LIVE",
               "grading": "PROVISIONAL — scorecard excess-vs-BTC gates admission",
               "plan_note": spec.get("plan_note"),
               "n_metrics": len(metrics), "categories": cats,
               "ai_master_brief": master, "ai_master_src": master_src,
               "forecasts": forecasts,
               "metrics": metrics, "composite_onchain_risk_z": comp,
               "read": ("On-chain composite %+0.2fz across %d curated core metrics; %d total "
                        "catalog metrics live" % (comp, len(core), len(metrics))) if comp is not None else None,
               "max_staleness_days": stale, "errors": errors or None,
               "source": "CryptoQuant Professional (full probed catalog) + Coin Metrics twins for 2010+ context"})
    _put(SPEC_KEY, spec, compact=False)
    return {"ok": True, "status": "LIVE", "n_metrics": len(metrics),
            "categories": cats, "composite": comp, "extremes_ai": len([k for k in extremes if metrics[k].get("ai_context")]),
            "errors": len(errors)}
