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
    q = dict(m.get("params") or {}); q["limit"] = str(limit)
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

def _twin_series(cm_metric):
    out = {}
    url = CM + "?assets=btc&metrics=%s&frequency=1d&page_size=10000&start_time=2010-07-01" % cm_metric
    for _ in range(4):
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/2.0"})
        with urllib.request.urlopen(req, timeout=40) as r:
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
    for h in (7, 21, 60):
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
    if len(t) > 700: t = t[:700]
    if t and not t.rstrip().endswith((".", "!", "?")):
        t = t.rsplit(".", 1)[0] + "." if "." in t else ""
    return t if 90 <= len(t) <= 720 else None

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
    twins_cfg = spec.get("twins") or {}
    twin_data = {}
    for cqn, cmn in twins_cfg.items():
        try: twin_data[cqn] = _twin_series(cmn)
        except Exception as e: print("[twin] %s -> %s" % (cmn, str(e)[:80]))

    hist = _j(HIST_KEY, {}) or {}
    metrics, errors, series_out = {}, [], {}
    for m in mets:
        name = m["name"]
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
            cstats = _cond_stats(stat_src, px, cur_for_stats)
            r, beta = _corr(ser, px)
            metrics[name] = {"value": round(vals[-1], 6), "z365": z, "pctl_1y": pctl,
                             "wow": round(vals[-1] - vals[-8], 6) if len(vals) >= 8 else None,
                             "as_of": dates[-1], "category": m.get("category", "other"),
                             "label": m.get("label", name), "unit": m.get("unit"),
                             "risk_sign": m.get("risk_sign", 0),
                             "in_composite": bool(m.get("in_composite")),
                             "corr_1y": r, "beta_1y": beta,
                             "stats_window": window, "cond_stats": cstats,
                             "hist_read": _hist_read(name, pctl, cstats, window),
                             "twin": twins_cfg.get(name)}
            step = max(1, len(dates) // 400)
            series_out[name] = {"d": dates[::step][-400:],
                                "v": [round(ser[d], 6) for d in dates[::step][-400:]]}
            time.sleep(1.15)
        except Exception as e:
            if "429" in str(e):
                time.sleep(16)
                try:
                    got = _series(m, tok, limit=1000)
                    if got:
                        hist[name] = dict(sorted({**(hist.get(name) or {}), **got}.items())[-2000:])
                        continue
                except Exception as e2: e = e2
            errors.append({"metric": name, "err": str(e)[:100]})
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
            td = sorted(tw); step = max(1, len(td) // 500)
            tser[cqn] = {"d": td[::step], "v": [round(tw[d], 6) for d in td[::step]]}
    pd = sorted(px); pstep = max(1, len(pd) // 500)
    _put(SERIES, {"generated_at": now.isoformat(timespec="seconds"),
                  "series": series_out, "twins": tser,
                  "btc": {"d": pd[::pstep], "v": [round(px[d], 2) for d in pd[::pstep]]}})

    stale = max((now - datetime.strptime(v["as_of"], "%Y-%m-%d").replace(tzinfo=timezone.utc)).days
                for v in metrics.values())
    cats = {}
    for v in metrics.values(): cats[v["category"]] = cats.get(v["category"], 0) + 1
    _put(OUT, {"engine": "justhodl-cryptoquant", "version": "2.0.0",
               "generated_at": now.isoformat(timespec="seconds"), "status": "LIVE",
               "grading": "PROVISIONAL — scorecard excess-vs-BTC gates admission",
               "plan_note": spec.get("plan_note"),
               "n_metrics": len(metrics), "categories": cats,
               "metrics": metrics, "composite_onchain_risk_z": comp,
               "read": ("On-chain composite %+0.2fz across %d curated core metrics; %d total "
                        "catalog metrics live" % (comp, len(core), len(metrics))) if comp is not None else None,
               "max_staleness_days": stale, "errors": errors or None,
               "source": "CryptoQuant Professional (full probed catalog) + Coin Metrics twins for 2010+ context"})
    _put(SPEC_KEY, spec, compact=False)
    return {"ok": True, "status": "LIVE", "n_metrics": len(metrics),
            "categories": cats, "composite": comp, "extremes_ai": len([k for k in extremes if metrics[k].get("ai_context")]),
            "errors": len(errors)}
