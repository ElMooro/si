"""
justhodl-pnl-attribution -- the firm Performance & P&L Attribution desk.

This is the performance-measurement layer that sits on top of the desk
stack. It answers the question every multi-strategy CIO asks each morning:
"Did we make money, where did it come from, and was it skill or beta?"

PIPELINE POSITION
  desk-returns (23:30) -> desk-allocator (00:30) -> firm-book (01:00)
  -> risk-monitor (01:30) -> liquidity-capacity (02:00)
  -> factor-risk (02:30) -> pnl-attribution (03:00)  <-- this engine

WHAT IT DOES
  1. APPEND-ONLY FIRM LEDGER. Each run reads every desk's realized return
     series from data/desk-returns.json and the live capital weights from
     data/desk-allocator.json. For every desk-return date newer than the
     last ledger row it writes ONE row: the firm return for that day is the
     weight-blended sum of the desks that marked, re-normalised over the
     desks present. The row also stores the raw per-desk returns and the
     weights live that day, so history is never re-weighted -- the books
     are marked once, exactly as a real fund's P&L is struck.
  2. BENCHMARK. SPY daily returns are pulled from Polygon and aligned to
     the ledger dates, so every firm number has a market reference.
  3. ANALYTICS off the full ledger:
       - equity curve, period returns (1D / 1W / MTD / QTD / YTD / ITD)
       - drawdown (current, max, days underwater)
       - risk-adjusted: annualised return + vol, Sharpe, Sortino, Calmar,
         hit rate, gain/loss ratio, best / worst day
       - vs benchmark: cumulative excess, beta, annualised alpha,
         tracking error, information ratio, up / down capture
  4. ATTRIBUTION on three institutional axes:
       - BY DESK: each desk's cumulative contribution and share of P&L
       - ALLOCATION EFFECT: firm curve vs a naive equal-weight-of-desks
         curve -- did the inverse-vol / regime allocator add value over
         doing nothing clever
       - FACTOR: firm return split into market-beta-driven P&L and
         residual alpha, using the net market beta from the factor model

OUTPUT    data/pnl-attribution.json   (+ append-only data/pnl-ledger.json)
SCHEDULE  daily 03:00 UTC
HONESTY   Built on desk-returns' hypothetical equal-/signal-weight marks --
          no transaction costs, slippage or borrow. Research only, not a
          record of actual trading or investment advice.
"""

import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta, date

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/pnl-attribution.json"
LEDGER_KEY = "data/pnl-ledger.json"
RETURNS_KEY = "data/desk-returns.json"
ALLOC_KEY = "data/desk-allocator.json"
FACTOR_KEY = "data/factor-risk.json"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
SCHEMA = "1.0"

DESK_ORDER = ["best-ideas", "pairs-arb", "trend-engine", "merger-arb",
              "spinoff-desk", "index-recon", "risk-radar"]
DESK_NAME = {
    "best-ideas": "Best Ideas",
    "pairs-arb": "Pairs Arb",
    "trend-engine": "Trend Engine",
    "merger-arb": "Merger Arb",
    "spinoff-desk": "Spinoff Desk",
    "index-recon": "Index Recon",
    "risk-radar": "Risk Radar",
}

TRADING_DAYS = 252
ANNUAL_RF = 0.043               # T-bill ~4.3%, the cash hurdle for Sharpe
DAILY_RF = ANNUAL_RF / TRADING_DAYS
WARM_OBS = 5                    # ledger rows needed before analytics are LIVE
LEDGER_CAP = 1500               # ~6 trading years; keeps the file bounded
HISTORY_DAYS = 250              # SPY look-back window

s3 = boto3.client("s3", region_name="us-east-1")


# ---- io --------------------------------------------------------------------
def get_json(key):
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(raw)
    except Exception:
        return None


def put_json(key, obj):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode("utf-8"),
                  ContentType="application/json", CacheControl="no-cache")


def poly_daily_closes(ticker, retries=1):
    """Return {date_iso: adjusted_close} for ~HISTORY_DAYS, or {} on miss."""
    to_d = datetime.now(timezone.utc).date()
    from_d = to_d - timedelta(days=HISTORY_DAYS + 60)
    url = ("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s"
           "?adjusted=true&sort=asc&limit=500&apiKey=%s"
           % (ticker, from_d, to_d, POLYGON_KEY))
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-pnl-attribution"})
            with urllib.request.urlopen(req, timeout=25) as r:
                body = json.loads(r.read().decode("utf-8"))
            results = body.get("results") or []
            if results:
                out = {}
                for bar in results:
                    d = datetime.fromtimestamp(
                        bar["t"] / 1000, timezone.utc).date().isoformat()
                    out[d] = bar.get("c")
                return out
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                time.sleep(2.0)
                continue
            return {}
        except Exception:
            if attempt < retries:
                time.sleep(1.0)
                continue
            return {}
        time.sleep(0.5)
    return {}


# ---- statistics (pure python) ----------------------------------------------
def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def stdev(xs):
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def downside_dev(xs, target=0.0):
    lo = [min(x - target, 0.0) for x in xs]
    if len(lo) < 2:
        return 0.0
    return (sum(v * v for v in lo) / (len(lo) - 1)) ** 0.5


def covariance(xs, ys):
    n = min(len(xs), len(ys))
    if n < 2:
        return 0.0
    mx, my = mean(xs[:n]), mean(ys[:n])
    return sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / (n - 1)


def compound(rets):
    """Cumulative simple return from a list of daily simple returns."""
    eq = 1.0
    for r in rets:
        eq *= (1.0 + r)
    return eq - 1.0


def equity_path(rets):
    """Running cumulative return after each day (list, same length as rets)."""
    out, eq = [], 1.0
    for r in rets:
        eq *= (1.0 + r)
        out.append(eq - 1.0)
    return out


def drawdown_series(rets):
    """Per-day drawdown from the running peak of the equity path."""
    out, eq, peak = [], 1.0, 1.0
    for r in rets:
        eq *= (1.0 + r)
        peak = max(peak, eq)
        out.append(eq / peak - 1.0)
    return out


def pct(x, nd=2):
    return round(x * 100.0, nd)


# ---- ledger maintenance ----------------------------------------------------
def build_desk_series(desk_returns):
    """key -> {date_iso: ret} from the desk-returns realized history."""
    out = {}
    desks = (desk_returns or {}).get("desks") or {}
    for key in DESK_ORDER:
        series = {}
        recs = ((desks.get(key) or {}).get("returns")) or []
        for rec in recs:
            d, r = rec.get("date"), rec.get("ret")
            if isinstance(d, str) and isinstance(r, (int, float)):
                series[d] = float(r)
        out[key] = series
    return out


def current_weights(alloc):
    """key -> capital weight as a fraction; falls back to equal weight."""
    w = {}
    for row in (alloc or {}).get("desks", []) or []:
        k = row.get("key")
        cw = row.get("capital_weight_pct")
        if k in DESK_ORDER and isinstance(cw, (int, float)):
            w[k] = cw / 100.0
    if not w or sum(w.values()) <= 0:
        return {k: 1.0 / len(DESK_ORDER) for k in DESK_ORDER}
    return w


def extend_ledger(ledger, desk_series, weights):
    """Append one row per new desk-return date. Returns (ledger, n_added)."""
    rows = ledger.get("rows", [])
    have = set(r["date"] for r in rows if isinstance(r.get("date"), str))

    all_dates = set()
    for s in desk_series.values():
        all_dates.update(s.keys())
    new_dates = sorted(d for d in all_dates if d not in have)

    added = 0
    for d in new_dates:
        marked = {k: desk_series[k][d] for k in DESK_ORDER
                  if d in desk_series[k]}
        if not marked:
            continue
        wsum = sum(weights.get(k, 0.0) for k in marked)
        if wsum <= 0:
            wsum = float(len(marked))
            day_w = {k: 1.0 / len(marked) for k in marked}
        else:
            day_w = {k: weights.get(k, 0.0) / wsum for k in marked}
        firm_ret = sum(day_w[k] * marked[k] for k in marked)
        rows.append({
            "date": d,
            "firm_return": round(firm_ret, 6),
            "desk_returns": {k: round(v, 6) for k, v in marked.items()},
            "weights": {k: round(day_w[k], 4) for k in marked},
            "n_marked": len(marked),
        })
        added += 1

    rows.sort(key=lambda r: r["date"])
    ledger["rows"] = rows[-LEDGER_CAP:]
    ledger["schema"] = SCHEMA
    return ledger, added


def attach_benchmark(ledger, spy_rets):
    """Stamp each ledger row with the aligned SPY return when available."""
    for r in ledger.get("rows", []):
        if r.get("spy_return") is None and r["date"] in spy_rets:
            r["spy_return"] = round(spy_rets[r["date"]], 6)
    return ledger


# ---- analytics -------------------------------------------------------------
def period_return(rows, kind, today):
    """Cumulative firm return over a calendar window ending at the latest row."""
    try:
        t = date.fromisoformat(today)
    except Exception:
        return None
    if kind == "d1":
        sel = rows[-1:]
    elif kind == "d5":
        sel = rows[-5:]
    elif kind == "mtd":
        sel = [r for r in rows if r["date"] >= t.replace(day=1).isoformat()]
    elif kind == "qtd":
        q0 = date(t.year, 3 * ((t.month - 1) // 3) + 1, 1)
        sel = [r for r in rows if r["date"] >= q0.isoformat()]
    elif kind == "ytd":
        sel = [r for r in rows if r["date"] >= date(t.year, 1, 1).isoformat()]
    else:
        sel = rows
    return compound([r["firm_return"] for r in sel]) if sel else None


def analytics(rows):
    """Full risk / performance / benchmark stat block off the ledger."""
    firm = [r["firm_return"] for r in rows]
    n = len(firm)
    today = rows[-1]["date"]

    cum = compound(firm)
    ann_ret = (1.0 + cum) ** (TRADING_DAYS / n) - 1.0 if n else 0.0
    vol_d = stdev(firm)
    ann_vol = vol_d * (TRADING_DAYS ** 0.5)

    excess_d = [r - DAILY_RF for r in firm]
    sharpe = (mean(excess_d) / vol_d * (TRADING_DAYS ** 0.5)) if vol_d else 0.0
    dd = downside_dev(firm, DAILY_RF)
    sortino = (mean(excess_d) / dd * (TRADING_DAYS ** 0.5)) if dd else 0.0

    dds = drawdown_series(firm)
    max_dd = min(dds) if dds else 0.0
    cur_dd = dds[-1] if dds else 0.0
    underwater = 0
    for x in reversed(dds):
        if x < -1e-9:
            underwater += 1
        else:
            break
    calmar = (ann_ret / abs(max_dd)) if max_dd < -1e-9 else None

    wins = [x for x in firm if x > 0]
    losses = [x for x in firm if x < 0]
    hit = len(wins) / n if n else 0.0
    gl = (mean(wins) / abs(mean(losses))) if losses and wins else None

    block = {
        "observations": n,
        "cumulative_return_pct": pct(cum),
        "annualized_return_pct": pct(ann_ret),
        "annualized_vol_pct": pct(ann_vol),
        "sharpe": round(sharpe, 2),
        "sortino": round(sortino, 2),
        "calmar": round(calmar, 2) if calmar is not None else None,
        "max_drawdown_pct": pct(max_dd),
        "current_drawdown_pct": pct(cur_dd),
        "days_underwater": underwater,
        "hit_rate_pct": pct(hit, 1),
        "gain_loss_ratio": round(gl, 2) if gl is not None else None,
        "best_day_pct": pct(max(firm)) if firm else 0.0,
        "worst_day_pct": pct(min(firm)) if firm else 0.0,
        "period_returns": {
            k: (pct(v) if v is not None else None)
            for k, v in (
                ("d1", period_return(rows, "d1", today)),
                ("w1", period_return(rows, "d5", today)),
                ("mtd", period_return(rows, "mtd", today)),
                ("qtd", period_return(rows, "qtd", today)),
                ("ytd", period_return(rows, "ytd", today)),
                ("itd", period_return(rows, "itd", today)),
            )
        },
    }

    # ---- benchmark block (only over the SPY-aligned sub-sample) -----------
    paired = [(r["firm_return"], r["spy_return"]) for r in rows
              if isinstance(r.get("spy_return"), (int, float))]
    if len(paired) >= 2:
        f = [p[0] for p in paired]
        b = [p[1] for p in paired]
        var_b = covariance(b, b)
        beta = covariance(f, b) / var_b if var_b > 1e-12 else 0.0
        alpha_d = mean(f) - beta * mean(b)
        diff = [f[i] - b[i] for i in range(len(f))]
        te = stdev(diff) * (TRADING_DAYS ** 0.5)
        ir = (mean(diff) / stdev(diff) * (TRADING_DAYS ** 0.5)
              ) if stdev(diff) else 0.0
        up = [(f[i], b[i]) for i in range(len(b)) if b[i] > 0]
        dn = [(f[i], b[i]) for i in range(len(b)) if b[i] < 0]
        up_cap = (mean([x[0] for x in up]) / mean([x[1] for x in up])
                  ) if up and mean([x[1] for x in up]) else None
        dn_cap = (mean([x[0] for x in dn]) / mean([x[1] for x in dn])
                  ) if dn and mean([x[1] for x in dn]) else None
        block["benchmark"] = {
            "name": "SPY",
            "aligned_days": len(paired),
            "spy_cumulative_pct": pct(compound(b)),
            "excess_cumulative_pct": pct(compound(f) - compound(b)),
            "beta_to_spy": round(beta, 3),
            "alpha_annualized_pct": pct(alpha_d * TRADING_DAYS),
            "tracking_error_pct": pct(te),
            "information_ratio": round(ir, 2),
            "up_capture_pct": pct(up_cap) if up_cap is not None else None,
            "down_capture_pct": pct(dn_cap) if dn_cap is not None else None,
        }
    else:
        block["benchmark"] = {"name": "SPY", "aligned_days": len(paired),
                              "note": "awaiting aligned benchmark history"}
    return block


def attribution_by_desk(rows):
    """Each desk's cumulative additive contribution to firm P&L."""
    contrib = {k: 0.0 for k in DESK_ORDER}
    wsum = {k: 0.0 for k in DESK_ORDER}
    days = {k: 0 for k in DESK_ORDER}
    for r in rows:
        for k, ret in r.get("desk_returns", {}).items():
            w = r.get("weights", {}).get(k, 0.0)
            contrib[k] += w * ret
            wsum[k] += w
            days[k] += 1
    total = sum(contrib.values())
    out = []
    for k in DESK_ORDER:
        out.append({
            "key": k,
            "name": DESK_NAME[k],
            "contribution_pct": pct(contrib[k], 3),
            "share_of_pnl_pct": (pct(contrib[k] / total, 1)
                                 if abs(total) > 1e-9 else None),
            "avg_weight_pct": (pct(wsum[k] / days[k], 1) if days[k] else None),
            "days_marked": days[k],
        })
    out.sort(key=lambda x: -(x["contribution_pct"] or 0.0))
    return out, total


def allocation_effect(rows):
    """Firm (allocator-weighted) curve vs a naive equal-weight-of-desks curve."""
    firm_r, eqw_r = [], []
    for r in rows:
        dr = r.get("desk_returns", {})
        if not dr:
            continue
        firm_r.append(r["firm_return"])
        eqw_r.append(sum(dr.values()) / len(dr))
    firm_cum = compound(firm_r)
    eqw_cum = compound(eqw_r)
    add = firm_cum - eqw_cum
    if add > 0.0005:
        verdict = ("The inverse-vol / regime allocator is beating a naive "
                   "equal-weight of the seven desks -- capital tilting is "
                   "adding value.")
    elif add < -0.0005:
        verdict = ("A naive equal-weight of the seven desks is currently "
                   "ahead of the allocator -- the tilts have cost value so "
                   "far.")
    else:
        verdict = ("The allocator and a naive equal-weight are running "
                   "neck-and-neck; tilting has been roughly P&L-neutral.")
    return {
        "firm_cumulative_pct": pct(firm_cum),
        "equalweight_cumulative_pct": pct(eqw_cum),
        "allocator_value_add_pct": pct(add, 3),
        "verdict": verdict,
    }


def factor_attribution(rows, factor):
    """Split firm return into market-beta P&L and residual alpha P&L."""
    beta = None
    if isinstance(factor, dict):
        for path in (("firm", "net_market_beta"),
                     ("firm", "net_beta"),
                     ("portfolio", "net_market_beta"),
                     ("net_market_beta",)):
            node = factor
            ok = True
            for p in path:
                if isinstance(node, dict) and p in node:
                    node = node[p]
                else:
                    ok = False
                    break
            if ok and isinstance(node, (int, float)):
                beta = float(node)
                break
    paired = [(r["firm_return"], r["spy_return"]) for r in rows
              if isinstance(r.get("spy_return"), (int, float))]
    if beta is None or len(paired) < 2:
        return {"available": False,
                "note": "needs factor-model net beta and aligned SPY days"}
    mkt = [beta * p[1] for p in paired]
    res = [paired[i][0] - mkt[i] for i in range(len(paired))]
    mkt_cum = compound(mkt)
    res_cum = compound(res)
    firm_cum = compound([p[0] for p in paired])
    denom = abs(mkt_cum) + abs(res_cum)
    return {
        "available": True,
        "net_market_beta": round(beta, 3),
        "aligned_days": len(paired),
        "firm_cumulative_pct": pct(firm_cum),
        "market_component_pct": pct(mkt_cum, 3),
        "alpha_component_pct": pct(res_cum, 3),
        "alpha_share_pct": (pct(abs(res_cum) / denom, 1)
                            if denom > 1e-9 else None),
    }


def equity_curve(rows):
    firm = [r["firm_return"] for r in rows]
    fc = equity_path(firm)
    dd = drawdown_series(firm)
    eqw, bc = [], []
    bench = []
    eqeq = 1.0
    beq = 1.0
    for i, r in enumerate(rows):
        dr = r.get("desk_returns", {})
        eqr = sum(dr.values()) / len(dr) if dr else 0.0
        eqeq *= (1.0 + eqr)
        eqw.append(eqeq - 1.0)
        sr = r.get("spy_return")
        if isinstance(sr, (int, float)):
            beq *= (1.0 + sr)
        bench.append(beq - 1.0)
    out = []
    for i, r in enumerate(rows):
        out.append({
            "date": r["date"],
            "firm_cum_pct": pct(fc[i]),
            "equalweight_cum_pct": pct(eqw[i]),
            "spy_cum_pct": pct(bench[i]),
            "drawdown_pct": pct(dd[i]),
        })
    return out


def recent_days(rows, by_desk_lookup):
    out = []
    for r in rows[-10:]:
        dr = r.get("desk_returns", {})
        wts = r.get("weights", {})
        contribs = {k: wts.get(k, 0.0) * dr.get(k, 0.0) for k in dr}
        top = max(contribs, key=lambda k: contribs[k]) if contribs else None
        out.append({
            "date": r["date"],
            "firm_return_pct": pct(r["firm_return"], 3),
            "spy_return_pct": (pct(r["spy_return"], 3)
                               if isinstance(r.get("spy_return"),
                                             (int, float)) else None),
            "n_desks_marked": r.get("n_marked"),
            "top_desk": DESK_NAME.get(top) if top else None,
            "top_desk_contribution_pct": (pct(contribs[top], 3)
                                          if top else None),
        })
    out.reverse()
    return out


# ---- handler ---------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    desk_returns = get_json(RETURNS_KEY) or {}
    alloc = get_json(ALLOC_KEY) or {}
    factor = get_json(FACTOR_KEY) or {}
    ledger = get_json(LEDGER_KEY) or {"schema": SCHEMA, "rows": []}
    if not isinstance(ledger.get("rows"), list):
        ledger["rows"] = []

    desk_series = build_desk_series(desk_returns)
    weights = current_weights(alloc)

    ledger, n_added = extend_ledger(ledger, desk_series, weights)

    spy_closes = poly_daily_closes("SPY", retries=1)
    spy_rets = {}
    if spy_closes:
        ds = sorted(spy_closes.keys())
        for i in range(1, len(ds)):
            p0, p1 = spy_closes[ds[i - 1]], spy_closes[ds[i]]
            if p0 and p1 and p0 > 0:
                spy_rets[ds[i]] = p1 / p0 - 1.0
    ledger = attach_benchmark(ledger, spy_rets)

    ledger["engine"] = "justhodl-pnl-attribution"
    ledger["generated_at"] = now.isoformat()
    put_json(LEDGER_KEY, ledger)

    rows = ledger["rows"]
    n = len(rows)

    payload = {
        "schema": SCHEMA,
        "engine": "justhodl-pnl-attribution",
        "generated_at": now.isoformat(),
        "trading_date": rows[-1]["date"] if rows else None,
        "build_seconds": round(time.time() - t0, 2),
        "ledger_observations": n,
        "rows_added_this_run": n_added,
        "spy_history_ok": bool(spy_rets),
    }

    if n < WARM_OBS:
        payload["posture"] = "WARMING"
        payload["headline"] = (
            "P&L attribution is warming up: %d of %d daily marks logged. "
            "The desk-return feed appends one observation per trading day; "
            "full performance analytics unlock once %d rows are in the "
            "firm ledger." % (n, WARM_OBS, WARM_OBS))
        payload["firm"] = {"observations": n}
        payload["attribution_by_desk"] = []
        payload["equity_curve"] = [
            {"date": r["date"],
             "firm_return_pct": pct(r["firm_return"], 3)} for r in rows]
        payload["disclaimer"] = (
            "Built on hypothetical desk marks - no costs, slippage or "
            "borrow. Research and education only, not investment advice.")
        put_json(OUT_KEY, payload)
        return {"statusCode": 200, "body": json.dumps(
            {"posture": "WARMING", "ledger_observations": n,
             "rows_added": n_added})}

    firm = analytics(rows)
    by_desk, total = attribution_by_desk(rows)
    alloc_eff = allocation_effect(rows)
    fac_attr = factor_attribution(rows, factor)

    cum = firm["cumulative_return_pct"]
    sharpe = firm["sharpe"]
    posture = "LIVE"
    lead = by_desk[0] if by_desk else None
    bench = firm.get("benchmark", {})
    excess = bench.get("excess_cumulative_pct")

    parts = ["Firm P&L %+.2f%% over %d marked days (Sharpe %.2f, max "
             "drawdown %.2f%%)." % (cum, n, sharpe,
                                    firm["max_drawdown_pct"])]
    if lead:
        parts.append("%s is the top contributor at %+.3f%%." % (
            lead["name"], lead["contribution_pct"]))
    if isinstance(excess, (int, float)):
        parts.append("Excess vs SPY %+.2f%%." % excess)
    if fac_attr.get("available"):
        parts.append("Alpha is %.0f%% of gross P&L by factor split." % (
            fac_attr.get("alpha_share_pct") or 0.0))
    headline = " ".join(parts)

    payload["posture"] = posture
    payload["headline"] = headline
    payload["firm"] = firm
    payload["attribution_by_desk"] = by_desk
    payload["pnl_total_pct"] = pct(total, 3)
    payload["allocation_effect"] = alloc_eff
    payload["factor_attribution"] = fac_attr
    payload["equity_curve"] = equity_curve(rows)
    payload["recent_days"] = recent_days(rows, by_desk)
    payload["methodology"] = (
        "Each trading day the firm return is the capital-weight-blended "
        "sum of the desks that marked, re-normalised over the desks "
        "present, and appended once to an immutable ledger - history is "
        "never re-weighted. Risk-adjusted statistics annualise daily "
        "moments with 252 trading days and a %.1f%% cash hurdle. Desk "
        "attribution sums each desk's weight x return contribution. The "
        "allocation effect compares the allocator-weighted curve with a "
        "naive equal-weight of the seven desks. Factor attribution splits "
        "the return into market-beta P&L (net beta x SPY) and residual "
        "alpha." % (ANNUAL_RF * 100))
    payload["disclaimer"] = (
        "Built on desk-returns' hypothetical equal-/signal-weight marks - "
        "no transaction costs, slippage or borrow cost. Research and "
        "education only, not investment advice or a record of actual "
        "trading.")
    put_json(OUT_KEY, payload)

    return {"statusCode": 200, "body": json.dumps({
        "posture": posture,
        "ledger_observations": n,
        "rows_added": n_added,
        "cumulative_return_pct": cum,
        "sharpe": sharpe,
        "top_desk": lead["name"] if lead else None,
    })}
