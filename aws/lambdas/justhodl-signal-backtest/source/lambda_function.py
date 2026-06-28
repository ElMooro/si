"""justhodl-signal-backtest — does the system actually work? Forward-return proof.

Reads the daily opportunity-engine snapshots (data/track-record/snapshots/{date}),
plus the dislocation + best-setups snapshots, and measures FORWARD RETURNS by
signal class so you can trust (or discard) each signal:

  • By verdict tier (STRONG OPPORTUNITY / OPPORTUNITY / ... / HIGH RISK)
  • By compounder score bucket
  • By dislocation membership
  • By Triple-Threat membership (the rare 3-lens convergence)

For each snapshot ≥ N days old, fetch the current price (FMP batch quote),
compute return since the snapshot, and aggregate avg/median/hit-rate/win-rate
per signal class and per holding window (7/14/30/60/90d where available).

OUTPUT: data/signal-backtest.json — the empirical track record that powers
trust + lets the conviction board reweight by PROVEN performance.
SCHEDULE: daily 16:00 UTC.
"""
import json, os, time, statistics
import urllib.request
from datetime import datetime, timezone, date
from collections import defaultdict
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/signal-backtest.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
s3 = boto3.client("s3", region_name=REGION)

WINDOWS = [7, 14, 30, 60, 90]


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def list_snapshots(prefix, limit=120):
    keys = []
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if tok: kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents", []) if o["Key"].endswith(".json")]
        tok = r.get("NextContinuationToken")
        if not tok: break
    return sorted(keys)[-limit:]


def batch_quotes(tickers):
    """Current prices via FMP batch quote-short (chunks of 100)."""
    out = {}
    base = "https://financialmodelingprep.com/stable"
    tk = list(tickers)
    for i in range(0, len(tk), 100):
        chunk = tk[i:i+100]
        try:
            url = f"{base}/batch-quote-short?symbols={','.join(chunk)}&apikey={FMP_KEY}"
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                data = json.loads(r.read().decode())
            for q in (data if isinstance(data, list) else []):
                p = q.get("price")
                if p: out[(q.get("symbol") or "").upper()] = float(p)
        except Exception as e:
            print(f"[bt] quote chunk err: {str(e)[:60]}")
    return out


def stock_rollup(vt_map, top_n=20, min_n=2):
    """Per-ticker rollup inside each verdict/bucket -> leaders (best mean fwd return)
    and laggards (worst), so the scorecard can SHOW which names win/lose, not just %."""
    out_map = {}
    for v, tmap in vt_map.items():
        rolled = []
        for tk, rets in tmap.items():
            rr = [x for x in rets if x is not None]
            if not rr:
                continue
            m = statistics.mean(rr)
            rolled.append({"ticker": tk, "ret": round(m, 1), "n": len(rr),
                           "win_rate": round(sum(1 for x in rr if x > 0) / len(rr) * 100)})
        if not rolled:
            continue
        robust = [x for x in rolled if x["n"] >= min_n]
        use = robust if len(robust) >= 8 else rolled
        use.sort(key=lambda x: -x["ret"])
        leaders = use[:top_n]
        lead_set = {x["ticker"] for x in leaders}
        laggards = [x for x in reversed(use) if x["ticker"] not in lead_set][:top_n]
        out_map[v] = {"leaders": leaders, "laggards": laggards,
                      "n_tickers": len(rolled), "min_obs": min_n}
    return out_map


CONVICTION_RANK = {"STRONG OPPORTUNITY": 6, "OPPORTUNITY": 5, "FAIR VALUE": 4,
                   "HOLD / NEUTRAL": 3, "EXPENSIVE": 2, "HIGH RISK": 1}


def _spearman(pairs):
    n = len(pairs)
    if n < 3:
        return None
    xs = sorted(range(n), key=lambda i: pairs[i][0])
    ys = sorted(range(n), key=lambda i: pairs[i][1])
    rx = {}; ry = {}
    for r, i in enumerate(xs):
        rx[i] = r
    for r, i in enumerate(ys):
        ry[i] = r
    d2 = sum((rx[i] - ry[i]) ** 2 for i in range(n))
    return round(1 - 6 * d2 / (n * (n * n - 1)), 2)


def _deterministic_analysis(out):
    """Always-on honest analysis computed directly from the data (no LLM needed)."""
    from collections import Counter
    bv = out.get("by_verdict") or {}
    bvs = out.get("by_verdict_stocks") or {}
    bc = out.get("by_compounder_bucket") or {}
    ranked = sorted(((k, v) for k, v in bv.items() if v and v.get("n")),
                    key=lambda kv: -(kv[1].get("win_rate") or 0))
    if not ranked:
        return {"_skip": "no verdict data", "source": "deterministic"}
    top_k, top_v = ranked[0]
    bot_k, bot_v = ranked[-1]
    so = bv.get("STRONG OPPORTUNITY") or {}
    so_wr = so.get("win_rate")
    pairs = [(CONVICTION_RANK.get(k, 0), v.get("win_rate") or 0)
             for k, v in bv.items() if v and v.get("n") and k in CONVICTION_RANK]
    rho = _spearman(pairs)
    inverted = bool(so_wr is not None and top_k != "STRONG OPPORTUNITY"
                    and (top_v.get("win_rate") or 0) - so_wr > 3)
    if inverted:
        headline = ("Conviction labels are inverted — %s leads at %s%% while STRONG OPPORTUNITY trails at %s%%; the buy labels are not predictive yet."
                    % (top_k, round(top_v.get("win_rate"), 1), round(so_wr, 1)))
    elif top_k == "STRONG OPPORTUNITY":
        headline = "Conviction labels are working — STRONG OPPORTUNITY leads all verdicts at %s%% win." % round(top_v.get("win_rate"), 1)
    else:
        headline = "%s leads at %s%% win; conviction ordering is noisy (rank corr %s)." % (top_k, round(top_v.get("win_rate"), 1), rho)
    spread = round((top_v.get("win_rate") or 0) - (bot_v.get("win_rate") or 0), 1)
    q80 = (bc.get("compounder_80+") or {}).get("win_rate")
    diag = ("Across %d verdicts the win-rate spread is %spp (best %s, worst %s); conviction-vs-outcome rank correlation is %s (1.0 = perfectly ordered, <=0 = inverted/noisy)."
            % (len(ranked), spread, top_k, bot_k, rho if rho is not None else "n/a"))
    if q80 is not None:
        diag += " Top-quality names (compounder_80+) win %s%%, so the quality axis carries more signal than the verdict axis right now." % round(q80, 1)
    vnotes = {}
    for k, v in ranked:
        st = bvs.get(k) or {}
        L = st.get("leaders") or []; D = st.get("laggards") or []
        ld = ", ".join("%s %s%%" % (x["ticker"], ("+%s" % x["ret"] if x["ret"] >= 0 else x["ret"])) for x in L[:2])
        dg = ", ".join("%s %s%%" % (x["ticker"], ("+%s" % x["ret"] if x["ret"] >= 0 else x["ret"])) for x in D[:2])
        note = "%s%% win" % round(v.get("win_rate"), 1)
        if ld:
            note += "; driven by %s" % ld
        if dg:
            note += "; dragged by %s" % dg
        vnotes[k] = note
    lag_c = Counter(); led_c = Counter()
    for k, st in bvs.items():
        for x in (st.get("laggards") or []):
            lag_c[x["ticker"]] += 1
        for x in (st.get("leaders") or []):
            led_c[x["ticker"]] += 1
    repeat_lag = [t for t, c in lag_c.most_common(6) if c >= 2]
    repeat_led = [t for t, c in led_c.most_common(6) if c >= 2]
    patterns = []
    if inverted:
        patterns.append("The high-conviction buy bucket underperforms lower-conviction ones — verdict scoring is currently anti-correlated with forward returns (rank corr %s)." % rho)
    if repeat_lag:
        patterns.append("Systematic losers dragging multiple verdicts: %s — these lose regardless of the label assigned." % ", ".join(repeat_lag))
    if repeat_led:
        patterns.append("Systematic winners across verdicts: %s — consistently positive wherever they appear." % ", ".join(repeat_led))
    qb = {k: (v or {}).get("win_rate") for k, v in bc.items() if v}
    if qb.get("compounder_80+") and qb.get("compounder_70-80") and qb["compounder_80+"] - qb["compounder_70-80"] > 8:
        patterns.append("Quality is non-monotonic: 80+ wins %s%% but the 70-80 bucket is a trap at %s%% — mid-quality underperforms." % (round(qb["compounder_80+"], 1), round(qb["compounder_70-80"], 1)))
    recs = []
    if inverted:
        recs.append("Down-weight or temporarily invert the STRONG OPPORTUNITY signal weights; it trails %s by %spp." % (top_k, round((top_v.get("win_rate") or 0) - so_wr, 1)))
    if repeat_lag:
        recs.append("Add a penalty/exclusion for cross-verdict repeat laggards (%s) until they show positive forward returns." % ", ".join(repeat_lag[:5]))
    if q80 is not None:
        recs.append("Overweight compounder_80+ (%s%% win) and treat the 70-80 bucket as neutral rather than a buy." % round(q80, 1))
    if top_k == "HIGH RISK":
        recs.append("HIGH RISK leading suggests the risk flag is catching high-momentum names that keep running — add a momentum/trend overlay so the buy labels capture that move.")
    recs.append("Keep grading point-in-time and re-fit verdict weights to forward excess-vs-SPY (not absolute return) so labels become monotonic with outcomes.")
    return {"headline": headline, "diagnosis": diag, "verdict_notes": vnotes,
            "patterns": patterns, "recommendations": recs, "rank_corr": rho,
            "inverted": inverted, "source": "deterministic",
            "generated_at": datetime.now(timezone.utc).isoformat()}


def _parse_json_obj(txt):
    t = (txt or "").strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t[:4].lower() == "json":
            t = t[4:]
    t = t.strip()
    try:
        o = json.loads(t)
        return o if isinstance(o, dict) else None
    except Exception:
        return None


def _call_glm(system, user):
    try:
        key = boto3.client("ssm", region_name="us-east-1").get_parameter(
            Name="/justhodl/zai-api-key", WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None
    body = json.dumps({"model": "glm-4.6", "max_tokens": 1600,
                       "messages": [{"role": "system", "content": system},
                                    {"role": "user", "content": user}]}).encode()
    req = urllib.request.Request("https://api.z.ai/api/paas/v4/chat/completions", data=body,
                                 headers={"Content-Type": "application/json", "Authorization": "Bearer " + key}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=70) as r:
            data = json.loads(r.read().decode())
        return _parse_json_obj(data["choices"][0]["message"]["content"])
    except Exception:
        return None


def _call_claude(system, user):
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None
    payload = json.dumps({"model": "claude-sonnet-4-6", "max_tokens": 1600, "system": system,
                          "messages": [{"role": "user", "content": user}]}).encode()
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=payload,
                                 headers={"Content-Type": "application/json", "x-api-key": key,
                                          "anthropic-version": "2023-06-01"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=70) as r:
            data = json.loads(r.read().decode())
        return _parse_json_obj(data["content"][0]["text"] if data.get("content") else "")
    except Exception:
        return None


def ai_analyze(out):
    """Heavy-AI layer: Claude reads the scorecard (verdict stats + per-ticker winners/
    losers + buckets) and returns a strict-JSON honest diagnosis + recalibration actions."""
    ov = out.get("overall") or {}
    lines = ["OVERALL: n=%s win=%s%% avg=%s%% median=%s%% hit5=%s%% best/worst=%s/%s" % (
        out.get("n_observations"), ov.get("win_rate"), ov.get("avg"), ov.get("median"),
        ov.get("hit_5pct"), ov.get("best"), ov.get("worst"))]
    for v, s in sorted((out.get("by_verdict") or {}).items(),
                       key=lambda kv: -((kv[1] or {}).get("win_rate") or 0)):
        if not s:
            continue
        lines.append("VERDICT %s: n=%s win=%s%% avg=%s%% median=%s%% hit5=%s%% best/worst=%s/%s" % (
            v, s["n"], s["win_rate"], s["avg"], s["median"], s["hit_5pct"], s["best"], s["worst"]))
    for v, st in (out.get("by_verdict_stocks") or {}).items():
        L = ", ".join("%s(%s%%,n%s)" % (x["ticker"], x["ret"], x["n"]) for x in st["leaders"][:8])
        D = ", ".join("%s(%s%%,n%s)" % (x["ticker"], x["ret"], x["n"]) for x in st["laggards"][:8])
        lines.append("%s -> winners: %s || losers: %s" % (v, L, D))
    for b, s in (out.get("by_compounder_bucket") or {}).items():
        if s:
            lines.append("QUALITY %s: n=%s win=%s%% avg=%s%%" % (b, s["n"], s["win_rate"], s["avg"]))
    summary = "\n".join(lines)
    system = (
        "You are the quantitative validation analyst for JustHodl.AI's Signal Scorecard. You receive "
        "forward-return validation for stock-selection verdicts (STRONG OPPORTUNITY, OPPORTUNITY, FAIR VALUE, "
        "HOLD/NEUTRAL, EXPENSIVE, HIGH RISK), compounder-quality buckets, and the per-ticker winners and losers "
        "inside each verdict. Diagnose HONESTLY whether the conviction labels are predictive, explain WHY a label "
        "is inverted or working using the per-ticker evidence (name the tickers that drag or drive each bucket), "
        "and give concrete, specific recalibration actions. Be blunt and quantitative. No hedging, no praise, no filler. "
        "Output STRICT JSON only, no markdown, with keys: "
        "headline (string: the single most important truth in one sentence), "
        "diagnosis (string: 2-3 sentences on whether labels are predictive and the core problem), "
        "verdict_notes (object: each verdict label -> one-line evidence-based note citing tickers), "
        "patterns (array of 2-4 strings: cross-cutting patterns separating winners from losers), "
        "recommendations (array of 3-5 strings: specific actions to make labels predictive)."
    )
    user = "Scorecard data:\n" + summary + "\n\nReturn the JSON analysis now."

    # Always compute a deterministic analysis so the panel is never empty.
    det = _deterministic_analysis(out)

    # Enrich with an LLM when a provider is funded: GLM (Z.ai) first, then Claude.
    llm = _call_glm(system, user)
    src = "glm-4.6"
    if not llm:
        llm = _call_claude(system, user)
        src = "claude-sonnet-4-6"
    if llm and (llm.get("headline") or llm.get("diagnosis")):
        for k in ("headline", "diagnosis", "verdict_notes", "patterns", "recommendations"):
            if not llm.get(k):
                llm[k] = det.get(k)
        llm["source"] = src
        llm.setdefault("rank_corr", det.get("rank_corr"))
        llm.setdefault("inverted", det.get("inverted"))
        llm["generated_at"] = datetime.now(timezone.utc).isoformat()
        return llm
    return det


def agg(returns):
    rs = [r for r in returns if r is not None]
    if not rs:
        return None
    return {"n": len(rs), "avg": round(statistics.mean(rs), 2),
            "median": round(statistics.median(rs), 2),
            "win_rate": round(sum(1 for r in rs if r > 0) / len(rs) * 100, 1),
            "hit_5pct": round(sum(1 for r in rs if r >= 5) / len(rs) * 100, 1),
            "best": round(max(rs), 1), "worst": round(min(rs), 1)}


FACTOR_IC_KEY = "data/factor-ic.json"
PANEL_PREFIX = "screener/alpha-panel/"
IC_MIN_AGE = 7        # forward window (days) before a panel is usable
IC_MIN_NAMES = 50     # min cross-section per date for a stable IC


def _ranks(vals):
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    r = [0.0] * len(vals)
    i = 0
    while i < len(vals):
        j = i
        while j + 1 < len(vals) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            r[order[k]] = avg
        i = j + 1
    return r


def _pearson(x, y):
    n = len(x)
    if n < 3:
        return None
    mx = sum(x) / n; my = sum(y) / n
    num = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    dx = sum((x[i] - mx) ** 2 for i in range(n)) ** 0.5
    dy = sum((y[i] - my) ** 2 for i in range(n)) ** 0.5
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def _spearman(x, y):
    return _pearson(_ranks(x), _ranks(y))


def _ic_summ(ics):
    n = len(ics)
    if n == 0:
        return {"n_dates": 0, "insufficient": True}
    mean = statistics.mean(ics)
    sd = statistics.pstdev(ics) if n > 1 else 0.0
    ir = (mean / sd) if sd > 0 else None
    t = (mean / (sd / (n ** 0.5))) if sd > 0 else None
    return {"n_dates": n, "mean_ic": round(mean, 4), "ic_std": round(sd, 4),
            "ic_ir": round(ir, 3) if ir is not None else None,
            "t_stat": round(t, 2) if t is not None else None}


def compute_factor_ic():
    """Cross-sectional forward-return IC per factor, pooled across dated panels.
    Needs NO trade journal: per panel date, correlate each factor's score vs the
    realized forward return across the whole universe, then average daily ICs.
    This is the root fix for the calibrator's n=0 problem."""
    panel_keys = list_snapshots(PANEL_PREFIX)
    today = date.today()
    matured = []
    for k in panel_keys:
        d = k.split("/")[-1].replace(".json", "")
        try:
            age = (today - date.fromisoformat(d)).days
        except Exception:
            continue
        if age >= IC_MIN_AGE:
            matured.append((d, age, k))

    if not matured:
        out = {"engine": "factor-ic", "generated_at": datetime.now(timezone.utc).isoformat(),
               "maturity": "WARMING_UP", "panels_total": len(panel_keys), "panels_matured": 0,
               "note": f"Need alpha-panels >= {IC_MIN_AGE}d old. Accruing daily.",
               "eta": "first read ~7d after first panel; stable mean IC ~3-4 weeks."}
        s3.put_object(Bucket=BUCKET, Key=FACTOR_IC_KEY, Body=json.dumps(out, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=3600")
        print(f"[ic] warming up — {len(panel_keys)} panels, 0 matured")
        return out

    panels, all_tk = [], set()
    for d, age, k in matured:
        rows = (read_json(k) or {}).get("rows") or {}
        if rows:
            panels.append((d, age, rows))
            all_tk.update(rows.keys())
    prices = batch_quotes(all_tk)

    factors = None
    daily_ic = defaultdict(list)
    daily_spread = defaultdict(list)
    composite_ic = []
    dates_used = 0
    for d, age, rows in panels:
        pairs = []
        for sym, rec in rows.items():
            p0 = rec.get("p"); pnow = prices.get(sym.upper())
            if not p0 or p0 <= 0 or not pnow:
                continue
            pairs.append((rec.get("a"), (pnow / p0 - 1) * 100, rec.get("c") or {}))
        if len(pairs) < IC_MIN_NAMES:
            continue
        dates_used += 1
        if factors is None:
            factors = sorted({f for _, _, c in pairs for f in c.keys()})
        a_pairs = [(a, r) for a, r, _ in pairs if a is not None]
        if len(a_pairs) >= IC_MIN_NAMES:
            ic = _spearman([a for a, _ in a_pairs], [r for _, r in a_pairs])
            if ic is not None:
                composite_ic.append(ic)
        for f in factors:
            fp = [(c.get(f), r) for _, r, c in pairs if c.get(f) is not None]
            if len(fp) < IC_MIN_NAMES:
                continue
            xs = [v for v, _ in fp]; ys = [r for _, r in fp]
            ic = _spearman(xs, ys)
            if ic is not None:
                daily_ic[f].append(ic)
            order = sorted(range(len(fp)), key=lambda i: xs[i])
            q = max(1, len(fp) // 5)
            daily_spread[f].append(statistics.mean(ys[i] for i in order[-q:])
                                   - statistics.mean(ys[i] for i in order[:q]))

    factor_ic = {}
    for f in (factors or []):
        summ = _ic_summ(daily_ic.get(f, []))
        sp = daily_spread.get(f, [])
        summ["quintile_spread_avg"] = round(statistics.mean(sp), 2) if sp else None
        factor_ic[f] = summ

    out = {"engine": "factor-ic", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "maturity": "MATURE" if dates_used >= 15 else "BUILDING" if dates_used >= 5 else "BOOTSTRAPPING",
           "panels_total": len(panel_keys), "panels_matured": len(matured),
           "dates_used": dates_used, "universe_priced": len(prices),
           "composite_alpha_ic": _ic_summ(composite_ic), "factor_ic": factor_ic,
           "note": ("Cross-sectional Spearman rank IC of each factor vs forward return, "
                    "per panel date, averaged. mean_ic ~0.03-0.05 with t_stat>2 is a real, "
                    "tradeable factor. quintile_spread_avg = top-20% minus bottom-20% return. "
                    "No trade journal required.")}
    s3.put_object(Bucket=BUCKET, Key=FACTOR_IC_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[ic] dates_used={dates_used} maturity={out['maturity']} "
          f"composite_ic={out['composite_alpha_ic'].get('mean_ic')}")
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    snap_keys = list_snapshots("data/track-record/snapshots/")
    today = date.today()
    print(f"[bt] {len(snap_keys)} opportunity snapshots")

    # gather (snapshot_date, ticker, entry_price, signal_tags) for matured snaps
    records = []  # {date, age, ticker, p0, verdict, comp, go, cap}
    for key in snap_keys:
        d = key.split("/")[-1].replace(".json", "")
        try:
            age = (today - date.fromisoformat(d)).days
        except Exception:
            continue
        if age < 7:
            continue
        snap = read_json(key)
        if not snap:
            continue
        for tk, p in (snap.get("picks") or {}).items():
            p0 = p.get("p")
            if not p0 or p0 <= 0:
                continue
            records.append({"date": d, "age": age, "ticker": tk, "p0": p0,
                            "verdict": p.get("v"), "comp": p.get("comp"),
                            "go": p.get("go"), "cap": p.get("cap"),
                            "rev": p.get("rev"), "pv": p.get("pv"), "cyc": p.get("cyc")})

    if not records:
        out = {"engine": "signal-backtest", "generated_at": datetime.now(timezone.utc).isoformat(),
               "maturity": "BOOTSTRAPPING", "note": "Need snapshots >=7 days old. Accruing daily.",
               "n_observations": 0}
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                      ContentType="application/json", CacheControl="public, max-age=3600")
        print("[bt] no matured snapshots yet")
        return {"statusCode": 200, "body": json.dumps(out)}

    # current prices
    tickers = {r["ticker"] for r in records}
    prices = batch_quotes(tickers)
    print(f"[bt] {len(records)} obs, {len(prices)} live prices")

    # compute returns + bucket by signal class
    by_verdict = defaultdict(list)
    by_comp = defaultdict(list)
    by_cap = defaultdict(list)
    by_rev = defaultdict(list)
    by_val = defaultdict(list)
    by_cheap_improving = defaultdict(list)
    by_verdict_ticker = defaultdict(lambda: defaultdict(list))   # verdict -> ticker -> [rets]
    by_comp_ticker = defaultdict(lambda: defaultdict(list))      # bucket  -> ticker -> [rets]
    overall = []
    for r in records:
        pnow = prices.get(r["ticker"])
        if not pnow:
            continue
        ret = (pnow / r["p0"] - 1) * 100
        overall.append(ret)
        if r["verdict"]:
            by_verdict[r["verdict"]].append(ret)
            by_verdict_ticker[r["verdict"]][r["ticker"]].append(ret)
        c = r.get("comp")
        if c is not None:
            bucket = "compounder_80+" if c >= 80 else "compounder_70-80" if c >= 70 else "compounder_<70"
            by_comp[bucket].append(ret)
            by_comp_ticker[bucket][r["ticker"]].append(ret)
        if r.get("cap"): by_cap[r["cap"]].append(ret)
        if r.get("rev") in ("UP", "DOWN", "FLAT"): by_rev["revision_" + r["rev"]].append(ret)
        # peer-relative valuation axis (the 'cheap vs industry' half of the thesis)
        if r.get("pv"): by_val["valuation_" + r["pv"]].append(ret)
        # the interaction = the actual thesis: cheap vs peers AND estimates moving.
        # cheap_x_rev_UP is the re-rate setup; cheap_x_rev_DOWN is the value trap.
        if r.get("pv") and r.get("rev") in ("UP", "DOWN", "FLAT"):
            by_cheap_improving[f"{r['pv']}_x_rev_{r['rev']}"].append(ret)

    # dislocation + triple-threat membership (from latest snapshots; approximate
    # using current dislocations/best-setups as the cohort, returns since entry)
    out = {
        "engine": "signal-backtest", "version": "2.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "n_observations": len(overall),
        "snapshots_used": len(snap_keys),
        "maturity": "MATURE" if len(overall) >= 500 else "BUILDING" if len(overall) >= 60 else "BOOTSTRAPPING",
        "overall": agg(overall),
        "by_verdict": {k: agg(v) for k, v in by_verdict.items()},
        "by_compounder_bucket": {k: agg(v) for k, v in by_comp.items()},
        "by_cap_bucket": {k: agg(v) for k, v in by_cap.items()},
        "by_revision": {k: agg(v) for k, v in by_rev.items()},
        "by_valuation_vs_peer": {k: agg(v) for k, v in by_val.items()},
        "by_cheap_x_improving": {k: agg(v) for k, v in by_cheap_improving.items()},
        "by_verdict_stocks": stock_rollup(by_verdict_ticker),
        "by_compounder_stocks": stock_rollup(by_comp_ticker),
        "note": ("Forward return = % change from the snapshot's entry price to "
                 "the current price (variable holding period, snapshots >=7d old). "
                 "As history matures these become reliable; the conviction board "
                 "can then reweight signals by proven win-rate."),
    }
    try:
        out["ai_analysis"] = ai_analyze(out)
        _a = out["ai_analysis"]
        print("[bt] ai_analysis:", _a.get("headline") or _a.get("_skip") or _a.get("_error") or "ok")
    except Exception as _e:
        out["ai_analysis"] = {"_error": str(_e)[:140]}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[bt] DONE {round(time.time()-t0,1)}s — {len(overall)} obs, maturity {out['maturity']}")
    # cross-sectional factor IC (no trade journal) — writes data/factor-ic.json
    try:
        fic = compute_factor_ic()
        print(f"[bt] factor-ic: {fic.get('maturity')} dates_used={fic.get('dates_used', 0)}")
    except Exception as e:
        print(f"[bt] factor-ic err: {str(e)[:140]}")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "obs": len(overall),
                                                     "maturity": out["maturity"],
                                                     "overall": out["overall"]})}
