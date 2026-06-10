"""
justhodl-regime-engine v1.0 — The Conductor
===========================================
Canonical macro-regime state for the whole platform. Growth × Inflation
quadrants classified monthly back to 1971, liquidity overlay (NFCI), and —
the institutional part — MEASURED per-regime playbooks: real forward 1m/3m
distributions for SPX, 10y yields, USD, WTI and HY spreads inside each
quadrant, with n. Plus the historical 3-month transition matrix, so "where
are we" comes with "where do regimes like this usually go".

Quadrants (6m momentum of growth & inflation composites):
  GOLDILOCKS  G↑ I↓   ·  REFLATION  G↑ I↑
  DEFLATION-BUST G↓ I↓  ·  STAGFLATION G↓ I↑

Every engine that logs to the closed loop can stamp this regime; AI briefs,
hedge-planner and the opportunity ranker condition on data/regime.json.
Regime TRANSITIONS log to the closed loop with playbook-derived direction.
"""
import json, os, time, urllib.request, urllib.parse, bisect
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/regime.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
VERSION = "1.0.0"
QUADS = ("GOLDILOCKS", "REFLATION", "STAGFLATION", "DEFLATION-BUST")


def fred(sid, start="1962-01-01"):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY,
                                   "file_type": "json", "observation_start": start,
                                   "limit": 100000}))
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=40).read())
        return [(o["date"], float(o["value"])) for o in j.get("observations", [])
                if o.get("value") not in (".", "", None)]
    except Exception as e:
        print(f"[fred] {sid}: {str(e)[:50]}")
        return []


def monthly(pts):
    """last value per month → [(YYYY-MM, v)]"""
    out = {}
    for d, v in pts:
        out[d[:7]] = v
    return sorted(out.items())


def yoy(mpts):
    return [(mpts[i][0], (mpts[i][1] / mpts[i - 12][1] - 1) * 100)
            for i in range(12, len(mpts)) if mpts[i - 12][1]]


def slope6(series):
    """6m change of a [(m,v)] series → dict m→Δ"""
    return {series[i][0]: series[i][1] - series[i - 6][1]
            for i in range(6, len(series))}


def lambda_handler(event=None, context=None):
    t0 = time.time()

    # ── growth & inflation composites (monthly, 1962+) ──
    ip = yoy(monthly(fred("INDPRO")))
    pay = yoy(monthly(fred("PAYEMS")))
    cpi = yoy(monthly(fred("CPIAUCSL")))
    ipd, payd = dict(ip), dict(pay)
    growth = [(m, mean([x for x in (ipd.get(m), payd.get(m)) if x is not None]))
              for m, _ in ip if ipd.get(m) is not None or payd.get(m) is not None]
    g6, i6 = slope6(growth), slope6(cpi)
    months = sorted(set(g6) & set(i6))
    regime = []
    for m in months:
        gq, iq = g6[m] > 0, i6[m] > 0
        q = ("REFLATION" if gq and iq else "GOLDILOCKS" if gq else
             "STAGFLATION" if iq else "DEFLATION-BUST")
        regime.append((m, q))
    rmap = dict(regime)

    # liquidity overlay: NFCI (weekly 1971+) monthly avg; <0 easy
    nf = monthly(fred("NFCI", "1971-01-01"))
    nfd = dict(nf)

    # ── asset histories for playbooks ──
    spx_doc = json.loads(S3.get_object(Bucket=BUCKET,
                                        Key="data/spx-history-deep.json")["Body"].read())
    spx_m = monthly([(d, float(v)) for d, v in spx_doc.get("points", []) if v])
    y10 = monthly(fred("DGS10", "1962-01-01"))
    usd_old = monthly(fred("DTWEXB", "1973-01-01"))
    usd_new = monthly(fred("DTWEXBGS", "2006-01-01"))
    usd = usd_old[: next((i for i, (m, _) in enumerate(usd_old)
                           if m >= "2006-01"), len(usd_old))] + usd_new
    wti = monthly(fred("DCOILWTICO", "1986-01-01"))
    hy = monthly(fred("BAA10Y", "1971-01-01"))  # Baa−10y spread (HY analogue pre-1997)

    def fwd_table(series, w, mode="ret"):
        """per-quadrant forward stats; mode ret=%; mode chg=Δ level"""
        d = dict(series)
        ms = [m for m, _ in series]
        out = {q: [] for q in QUADS}
        for i, m in enumerate(ms):
            if i + w >= len(ms) or m not in rmap:
                continue
            a, b = d[m], d[ms[i + w]]
            if a is None or b is None or (mode == "ret" and not a):
                continue
            x = (b / a - 1) * 100 if mode == "ret" else (b - a)
            out[rmap[m]].append(x)
        res = {}
        for q, xs in out.items():
            if len(xs) >= 8:
                xs.sort()
                res[q] = {"n": len(xs), "median": round(xs[len(xs) // 2], 2),
                          "pos_pct": round(100 * sum(1 for x in xs if x > 0) / len(xs), 1)}
        return res

    playbook = {
        "spx": {"1m": fwd_table(spx_m, 1), "3m": fwd_table(spx_m, 3), "unit": "%"},
        "ust10y_yield": {"3m": fwd_table(y10, 3, "chg"), "unit": "Δpct-pts"},
        "usd": {"3m": fwd_table(usd, 3), "unit": "%"},
        "wti": {"3m": fwd_table(wti, 3), "unit": "%"},
        "credit_spread": {"3m": fwd_table(hy, 3, "chg"), "unit": "Δpct-pts"}}

    # transition matrix: P(quadrant in 3m | quadrant now)
    trans = {q: {q2: 0 for q2 in QUADS} for q in QUADS}
    for i in range(len(regime) - 3):
        trans[regime[i][1]][regime[i + 3][1]] += 1
    tmat = {}
    for q, row in trans.items():
        tot = sum(row.values())
        if tot:
            tmat[q] = {q2: round(100 * c / tot, 1) for q2, c in row.items()}

    # ── current state ──
    cur_m, cur_q = regime[-1]
    run = 1
    for m, q in reversed(regime[:-1]):
        if q == cur_q:
            run += 1
        else:
            break
    prev_q = next((q for m, q in reversed(regime[:-1]) if q != cur_q), None)
    liq_now = nfd.get(cur_m) or nfd.get(months[-2] if len(months) > 1 else cur_m)
    counts = {q: sum(1 for _, x in regime if x == q) for q in QUADS}
    current = {
        "quadrant": cur_q, "as_of_month": cur_m, "months_in_regime": run,
        "previous_quadrant": prev_q,
        "growth_6m_momentum": round(g6[cur_m], 2),
        "inflation_6m_momentum": round(i6[cur_m], 2),
        "liquidity_nfci": round(liq_now, 2) if liq_now is not None else None,
        "liquidity_state": ("EASY" if (liq_now or 0) < -0.3 else
                             "TIGHT" if (liq_now or 0) > 0.3 else "NEUTRAL"),
        "next_3m_probabilities": tmat.get(cur_q),
        "history_share_pct": {q: round(100 * c / len(regime), 1)
                               for q, c in counts.items()}}

    # regime strip for the page (last 30y)
    strip = [(m, q) for m, q in regime if m >= "1996-01"]

    # transition logging: fresh quadrant change → playbook-direction SPY signal
    n_logged = 0
    if run == 1 and prev_q:
        pb = (playbook["spx"]["3m"] or {}).get(cur_q) or {}
        if pb.get("n", 0) >= 20:
            direction = "UP" if pb["pos_pct"] >= 55 else "DOWN" if pb["pos_pct"] <= 45 else None
            if direction:
                try:
                    nowt = datetime.now(timezone.utc)
                    spy = fred("SP500", (nowt - timedelta(days=10)).date().isoformat())
                    px0 = spy[-1][1] if spy else None
                    if px0:
                        conf = round(min(0.68, max(0.52,
                                     0.30 + abs(pb["pos_pct"] - 50) / 100 * 0.9)), 2)
                        DDB.Table("justhodl-signals").put_item(Item={
                            "signal_id": f"regime-shift-{cur_q}#SPY#{cur_m}",
                            "signal_type": "regime_shift", "signal_value": cur_q,
                            "predicted_direction": direction,
                            "confidence": Decimal(str(conf)),
                            "measure_against": "ticker", "baseline_price": str(px0),
                            "benchmark": "SPY",
                            "check_windows": ["day_21", "day_63"],
                            "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat()
                                                  for w in (21, 63)},
                            "outcomes": {}, "accuracy_scores": {},
                            "logged_at": nowt.isoformat(),
                            "logged_epoch": int(nowt.timestamp()), "status": "pending",
                            "schema_version": "2", "horizon_days_primary": 63,
                            "regime_at_log": cur_q,
                            "ttl": int(nowt.timestamp()) + 150 * 86400,
                            "metadata": {"engine": "regime-engine", "v": VERSION,
                                         "from": prev_q or "", "to": cur_q},
                            "rationale": (f"Regime shift {prev_q}→{cur_q}. Measured playbook: "
                                           f"SPX +3m median {pb['median']}%, {pb['pos_pct']}% "
                                           f"positive over n={pb['n']} months since 1971.")})
                        n_logged = 1
                except Exception as e:
                    print(f"[loop] {str(e)[:70]}")

    out = {"engine": "regime-engine", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "current": current, "playbook": playbook,
           "transition_matrix_3m": tmat,
           "regime_strip": strip, "n_months_classified": len(regime),
           "first_month": regime[0][0],
           "signals_logged": n_logged,
           "methodology": ("Growth (INDPRO+payrolls YoY composite) × Inflation (CPI YoY), "
                           "each by 6m momentum → 4 quadrants monthly since the 1960s; "
                           "NFCI liquidity overlay. Playbooks are measured forward "
                           "distributions per quadrant (real n) for SPX, 10y yields, USD, "
                           "WTI, credit spreads — plus the historical 3m transition matrix. "
                           "The conductor: every engine, brief and hedge decision can "
                           "condition on this one state.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[regime] {cur_q} ({run}m) classified={len(regime)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"quadrant": cur_q, "months": run})}
