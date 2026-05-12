"""
justhodl-portfolio-risk — Roadmap #10 risk engine

═══════════════════════════════════════════════════════════════════════
INSTITUTIONAL-GRADE PORTFOLIO RISK
─────────────────────────────────────
Reads portfolio snapshot, fetches 90 days of daily returns for each
holding, then computes the full risk dashboard a hedge fund desk runs:

  • Portfolio annualized volatility (correlation-aware)
  • Beta-weighted exposure to SPY
  • Pairwise correlation matrix
  • Sector concentration with HHI (Herfindahl-Hirschman Index)
  • Parametric VAR 99% and 95% (1-day)
  • Historical scenario projections:
      - COVID-2020 (Feb-Mar 2020)
      - 2022 Inflation Bear (Jan-Oct 2022)
      - 2018 Q4 Vol Crisis
      - 2008 Global Financial Crisis
      - Dot-Com Bust 2000
      - Custom: 1-sigma daily move
  • Drawdown velocity per position (5d/30d max drawdown)
  • Correlation cluster detection (4+ holdings with avg corr >0.8)
  • Stop-loss alerts (Telegram)
  • VAR breach alerts (1-day VAR > 5% portfolio value)
  • Concentration alerts (any sector > 40%)

═══════════════════════════════════════════════════════════════════════
"""
import json
import math
import os
import statistics
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

S3_BUCKET = "justhodl-dashboard-live"
SNAPSHOT_KEY = "portfolio/snapshot.json"
RISK_KEY = "portfolio/risk.json"
ALERT_HISTORY_KEY = "portfolio/risk-alert-history.json"

POLY_KEY = os.environ.get("POLY_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Alert thresholds
VAR_99_PCT_ALERT = 5.0     # 1-day VAR > 5% of portfolio = alert
SECTOR_CONCENTRATION_ALERT = 40.0   # sector > 40% = alert
CORRELATION_CLUSTER_THRESHOLD = 0.8  # 4+ positions w/ avg corr >0.8 = alert
CORRELATION_CLUSTER_MIN = 4
DEDUPE_HOURS = 12

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════
# HISTORICAL CRISIS SCENARIOS
# ═══════════════════════════════════════════════════════════════════════
# Sector-level returns during major drawdowns. Sources: SPDR sector ETFs
# peak-to-trough during each event. Used for stress testing.

SCENARIOS = {
    "covid_2020": {
        "name": "COVID-2020 (Feb 19 → Mar 23, 2020)",
        "duration_days": 33,
        "spy_return": -0.339,
        "sector_returns": {
            "Technology":            -0.265,
            "Healthcare":            -0.235,
            "Consumer Defensive":    -0.176,
            "Utilities":             -0.339,
            "Consumer Cyclical":     -0.367,
            "Communication Services": -0.249,
            "Industrials":           -0.402,
            "Basic Materials":       -0.348,
            "Real Estate":           -0.422,
            "Financial Services":    -0.434,
            "Energy":                 -0.564,
        },
    },
    "inflation_2022": {
        "name": "2022 Inflation Bear (Jan 4 → Oct 12, 2022)",
        "duration_days": 281,
        "spy_return": -0.252,
        "sector_returns": {
            "Technology":            -0.342,
            "Communication Services": -0.401,
            "Consumer Cyclical":     -0.385,
            "Real Estate":           -0.305,
            "Healthcare":            -0.057,
            "Financial Services":    -0.180,
            "Industrials":           -0.160,
            "Consumer Defensive":     -0.061,
            "Utilities":              0.020,
            "Basic Materials":       -0.187,
            "Energy":                  0.426,
        },
    },
    "q4_2018_volcrisis": {
        "name": "Q4 2018 Vol Crisis (Oct 3 → Dec 24, 2018)",
        "duration_days": 82,
        "spy_return": -0.195,
        "sector_returns": {
            "Technology":            -0.236,
            "Energy":                 -0.276,
            "Industrials":           -0.225,
            "Consumer Cyclical":     -0.234,
            "Financial Services":    -0.189,
            "Healthcare":            -0.108,
            "Communication Services": -0.169,
            "Utilities":              0.019,
            "Consumer Defensive":     -0.041,
            "Real Estate":           -0.064,
            "Basic Materials":       -0.193,
        },
    },
    "gfc_2008": {
        "name": "GFC 2008 (Oct 9, 2007 → Mar 9, 2009)",
        "duration_days": 517,
        "spy_return": -0.566,
        "sector_returns": {
            "Technology":            -0.530,
            "Financial Services":    -0.823,
            "Real Estate":           -0.710,
            "Industrials":           -0.626,
            "Consumer Cyclical":     -0.555,
            "Energy":                 -0.524,
            "Basic Materials":       -0.594,
            "Communication Services": -0.487,
            "Healthcare":            -0.379,
            "Consumer Defensive":     -0.288,
            "Utilities":             -0.452,
        },
    },
    "dotcom_2000": {
        "name": "Dot-Com Bust (Mar 24, 2000 → Oct 9, 2002)",
        "duration_days": 929,
        "spy_return": -0.491,
        "sector_returns": {
            "Technology":            -0.778,
            "Communication Services": -0.658,
            "Consumer Cyclical":     -0.376,
            "Healthcare":            -0.158,
            "Financial Services":    -0.301,
            "Industrials":           -0.297,
            "Energy":                 -0.149,
            "Basic Materials":       -0.157,
            "Real Estate":            0.149,
            "Consumer Defensive":      0.099,
            "Utilities":             -0.279,
        },
    },
}


# ═══════════════════════════════════════════════════════════════════════
# DATA FETCHERS
# ═══════════════════════════════════════════════════════════════════════

def fetch_polygon_bars(symbol, lookback_days=120):
    if not POLY_KEY: return []
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=lookback_days)
    url = (f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
           f"{start}/{end}?adjusted=true&sort=asc&limit=500&apiKey={POLY_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-PR/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("results") or []
    except Exception as e:
        print(f"  [poly:{symbol}] {str(e)[:80]}")
        return []


def batch_fetch_bars(symbols, lookback_days=120, max_workers=10):
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_polygon_bars, s, lookback_days): s for s in symbols}
        for f in as_completed(futures):
            try: out[futures[f]] = f.result() or []
            except Exception: out[futures[f]] = []
    return out


# ═══════════════════════════════════════════════════════════════════════
# RISK MATH
# ═══════════════════════════════════════════════════════════════════════

def compute_returns(bars):
    closes = [b["c"] for b in bars]
    if len(closes) < 2: return []
    return [(closes[i] / closes[i-1] - 1) for i in range(1, len(closes))]


def annualized_vol(returns):
    if len(returns) < 5: return None
    return statistics.stdev(returns) * math.sqrt(252)


def safe_correlation(r1, r2):
    n = min(len(r1), len(r2))
    if n < 20: return None
    a, b = r1[-n:], r2[-n:]
    try: return statistics.correlation(a, b)
    except Exception: return None


def safe_beta(r_sym, r_spy):
    n = min(len(r_sym), len(r_spy))
    if n < 20: return None
    a, b = r_sym[-n:], r_spy[-n:]
    try:
        cov = statistics.covariance(a, b)
        var_spy = statistics.variance(b)
        return cov / var_spy if var_spy else None
    except Exception:
        return None


def max_drawdown(closes):
    """Return max peak-to-trough drawdown as negative percentage."""
    if len(closes) < 2: return None
    peak = closes[0]
    max_dd = 0.0
    for c in closes:
        if c > peak: peak = c
        dd = (c / peak - 1)
        if dd < max_dd: max_dd = dd
    return max_dd * 100  # percentage


def herfindahl(weights_pct):
    """HHI = sum of squared weight percentages. Range 0-10000. >2500 = concentrated."""
    return sum(w * w for w in weights_pct)


# ═══════════════════════════════════════════════════════════════════════
# SCENARIO PROJECTIONS
# ═══════════════════════════════════════════════════════════════════════

def project_scenarios(positions, total_value):
    """For each historical scenario, project the portfolio's $ P&L."""
    out = {}
    for sid, scenario in SCENARIOS.items():
        total_pnl = 0.0
        per_position = []
        for p in positions:
            sec = p.get("sector") or "Unknown"
            sec_ret = scenario["sector_returns"].get(sec, scenario["spy_return"])
            mv = p.get("market_value", 0)
            pos_pnl = mv * sec_ret
            total_pnl += pos_pnl
            per_position.append({
                "symbol": p["symbol"], "sector": sec,
                "scenario_return": round(sec_ret * 100, 1),
                "scenario_pnl": round(pos_pnl, 2),
            })
        out[sid] = {
            "name": scenario["name"],
            "duration_days": scenario["duration_days"],
            "spy_return_pct": round(scenario["spy_return"] * 100, 1),
            "projected_pnl_dollars": round(total_pnl, 2),
            "projected_pnl_pct": round((total_pnl / total_value) * 100, 2) if total_value else None,
            "per_position": per_position,
        }
    return out


# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None


def send_telegram(text, chat_id):
    if not TELEGRAM_TOKEN or not chat_id: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text[:4000],
        "parse_mode": "Markdown", "disable_web_page_preview": "true",
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("ok", False)
    except Exception as e:
        print(f"  telegram err: {str(e)[:200]}")
        return False


def load_alert_history():
    try: return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY)["Body"].read())
    except Exception: return {}


def save_alert_history(h):
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY,
            Body=json.dumps(h, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json")
    except Exception as e: print(f"  hist err: {e}")


def should_alert(history, key):
    last = history.get(key)
    if not last: return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - last_dt) >= timedelta(hours=DEDUPE_HOURS)
    except Exception: return True


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== PORTFOLIO RISK ENGINE · {datetime.now(timezone.utc).isoformat()} ===")

    # 1. Load snapshot
    try:
        snapshot = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SNAPSHOT_KEY)["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"snapshot read: {e}"})}

    positions = snapshot.get("positions") or []
    if not positions:
        # Write minimal empty risk report
        s3.put_object(Bucket=S3_BUCKET, Key=RISK_KEY,
            Body=json.dumps({
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "no_positions",
                "message": "No positions yet. Use justhodl-portfolio-admin Lambda to add positions.",
            }).encode("utf-8"),
            ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({
            "success": True, "status": "no_positions",
            "elapsed_seconds": round(time.time() - started, 2),
        })}

    total_value = snapshot.get("portfolio_summary", {}).get("total_market_value") or sum(
        p.get("market_value", 0) for p in positions)

    # 2. Fetch 90 days of daily data for every position + SPY (for beta)
    symbols = list({p["symbol"] for p in positions} | {"SPY"})
    print(f"  fetching {len(symbols)} symbols × 90d...")
    bars = batch_fetch_bars(symbols, 120)
    n_ok = sum(1 for s in symbols if len(bars.get(s, [])) >= 30)
    print(f"  {n_ok}/{len(symbols)} symbols have ≥30 bars")

    # 3. Compute returns per symbol
    returns_by = {s: compute_returns(bars.get(s, [])) for s in symbols}
    spy_returns = returns_by.get("SPY", [])

    # 4. Per-position metrics
    pos_metrics = {}
    for p in positions:
        sym = p["symbol"]
        rets = returns_by.get(sym, [])
        closes = [b["c"] for b in bars.get(sym, [])]
        pos_metrics[sym] = {
            "annual_vol_pct": round(annualized_vol(rets) * 100, 2) if annualized_vol(rets) else None,
            "beta_spy": round(safe_beta(rets, spy_returns), 3) if safe_beta(rets, spy_returns) else None,
            "30d_max_drawdown_pct": round(max_drawdown(closes[-30:]) if len(closes) >= 30 else 0, 2),
            "90d_max_drawdown_pct": round(max_drawdown(closes) if closes else 0, 2),
            "n_bars": len(closes),
        }

    # 5. Pairwise correlation matrix between positions only
    pos_symbols = [p["symbol"] for p in positions]
    correlation_matrix = {}
    for s1 in pos_symbols:
        correlation_matrix[s1] = {}
        for s2 in pos_symbols:
            if s1 == s2:
                correlation_matrix[s1][s2] = 1.0
                continue
            c = safe_correlation(returns_by.get(s1, []), returns_by.get(s2, []))
            correlation_matrix[s1][s2] = round(c, 3) if c is not None else None

    # 6. Compute portfolio variance: w'Σw
    #    where Σ_ij = vol_i * vol_j * corr_ij (annualized)
    weights = {}
    for p in positions:
        mv = p.get("market_value", 0)
        weights[p["symbol"]] = mv / total_value if total_value else 0

    port_var_annual = 0.0
    for s1 in pos_symbols:
        for s2 in pos_symbols:
            w_i = weights.get(s1, 0)
            w_j = weights.get(s2, 0)
            v_i = (pos_metrics[s1].get("annual_vol_pct") or 25) / 100  # default 25%
            v_j = (pos_metrics[s2].get("annual_vol_pct") or 25) / 100
            c_ij = correlation_matrix[s1].get(s2)
            if c_ij is None: c_ij = 0.4 if s1 != s2 else 1.0
            port_var_annual += w_i * w_j * v_i * v_j * c_ij
    port_vol_annual = math.sqrt(port_var_annual) * 100  # percent
    port_vol_daily_pct = port_vol_annual / math.sqrt(252)

    # 7. VAR (parametric, 1-day, 99% / 95%)
    var_1d_99_pct = port_vol_daily_pct * 2.326
    var_1d_95_pct = port_vol_daily_pct * 1.645
    var_1d_99_dollars = total_value * (var_1d_99_pct / 100)
    var_1d_95_dollars = total_value * (var_1d_95_pct / 100)

    # 8. Portfolio beta
    portfolio_beta = sum(weights.get(s, 0) * (pos_metrics[s].get("beta_spy") or 1.0)
                           for s in pos_symbols)

    # 9. Sector concentration + HHI
    sector_weights = {}
    for p in positions:
        sec = p.get("sector") or "Unknown"
        mv = p.get("market_value", 0)
        sector_weights[sec] = sector_weights.get(sec, 0) + mv
    sector_pcts = sorted(
        [(sec, (val / total_value) * 100 if total_value else 0) for sec, val in sector_weights.items()],
        key=lambda x: -x[1])
    hhi = herfindahl([p for _, p in sector_pcts])
    # HHI interpretation:
    #   < 1500: unconcentrated · 1500-2500: moderate · > 2500: concentrated
    if hhi < 1500: hhi_label = "Diversified"
    elif hhi < 2500: hhi_label = "Moderate concentration"
    else: hhi_label = "Highly concentrated"

    # 10. Correlation cluster detection
    correlation_clusters = []
    # For each position, find others highly correlated with it
    seen_pairs = set()
    for i, s1 in enumerate(pos_symbols):
        cluster = [s1]
        for j, s2 in enumerate(pos_symbols):
            if i == j: continue
            c = correlation_matrix[s1].get(s2)
            if c is not None and c >= CORRELATION_CLUSTER_THRESHOLD:
                cluster.append(s2)
        if len(cluster) >= CORRELATION_CLUSTER_MIN:
            key = tuple(sorted(cluster))
            if key in seen_pairs: continue
            seen_pairs.add(key)
            # Compute avg pairwise correlation within cluster
            pair_corrs = []
            for a in cluster:
                for b in cluster:
                    if a != b:
                        c = correlation_matrix[a].get(b)
                        if c is not None: pair_corrs.append(c)
            avg_corr = sum(pair_corrs) / len(pair_corrs) if pair_corrs else None
            total_cluster_weight = sum(weights.get(s, 0) for s in cluster) * 100
            correlation_clusters.append({
                "symbols": cluster,
                "avg_pairwise_correlation": round(avg_corr, 3) if avg_corr else None,
                "total_weight_pct": round(total_cluster_weight, 2),
            })

    # 11. Stops hit (from snapshot, already computed)
    stops_hit = [p for p in positions if p.get("stop_hit")]

    elapsed = time.time() - started
    print(f"  portfolio_vol={port_vol_annual:.1f}% · VAR99={var_1d_99_pct:.2f}% · β={portfolio_beta:.2f} · "
          f"HHI={hhi:.0f} ({hhi_label})")

    # ─── Build payload ───
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "elapsed_seconds": round(elapsed, 2),

        # Top-line
        "total_market_value": round(total_value, 2),
        "n_positions": len(positions),

        # Volatility & beta
        "portfolio_vol_annual_pct": round(port_vol_annual, 2),
        "portfolio_vol_daily_pct": round(port_vol_daily_pct, 3),
        "portfolio_beta_spy": round(portfolio_beta, 3),

        # VAR
        "var_1d_99_dollars": round(var_1d_99_dollars, 2),
        "var_1d_99_pct": round(var_1d_99_pct, 2),
        "var_1d_95_dollars": round(var_1d_95_dollars, 2),
        "var_1d_95_pct": round(var_1d_95_pct, 2),

        # Concentration
        "sector_concentration": [{"sector": s, "weight_pct": round(p, 2)} for s, p in sector_pcts],
        "concentration_hhi": round(hhi, 0),
        "concentration_label": hhi_label,
        "max_sector_concentration_pct": round(sector_pcts[0][1], 2) if sector_pcts else 0,

        # Per-position
        "position_metrics": pos_metrics,
        "correlation_matrix": correlation_matrix,
        "correlation_clusters": correlation_clusters,

        # Scenario projections
        "historical_scenarios": project_scenarios(positions, total_value),

        # Alerts
        "stops_hit": stops_hit,
        "alerts_summary": {
            "var_breach": var_1d_99_pct > VAR_99_PCT_ALERT,
            "stops_hit_count": len(stops_hit),
            "sector_concentration_breach": (sector_pcts[0][1] > SECTOR_CONCENTRATION_ALERT
                                              if sector_pcts else False),
            "correlation_cluster_count": len(correlation_clusters),
        },
    }

    # ─── Fire Telegram alerts ───
    chat_id = get_chat_id()
    history = load_alert_history()
    now_iso = datetime.now(timezone.utc).isoformat()
    alerts_sent = 0

    if chat_id and TELEGRAM_TOKEN:
        # Stops hit (one alert per position)
        for stop in stops_hit:
            key = f"stop:{stop['symbol']}"
            if should_alert(history, key):
                msg = (f"📉 *STOP LOSS HIT — {stop['symbol']}*\n"
                       f"Current: `${stop.get('current_price')}` · "
                       f"Stop: `${stop.get('stop_loss')}`\n"
                       f"Position type: {stop.get('position_type', 'LONG')}\n"
                       f"P&L on stop: `${stop.get('pnl_dollars', 0)}` "
                       f"({stop.get('pnl_pct', 0)}%)\n\n"
                       f"*Action required.* Review position in dashboard.\n"
                       f"[Portfolio](https://justhodl.ai/portfolio/) · "
                       f"[Stock detail](https://justhodl.ai/stock/?symbol={stop['symbol']})")
                if send_telegram(msg, chat_id):
                    history[key] = now_iso
                    alerts_sent += 1
                time.sleep(0.5)

        # VAR breach
        if payload["alerts_summary"]["var_breach"]:
            key = "var_breach"
            if should_alert(history, key):
                msg = (f"⚠️ *PORTFOLIO RISK ALERT*\n"
                       f"1-day 99% VAR: `${var_1d_99_dollars:,.0f}` "
                       f"({var_1d_99_pct:.2f}% of book) — exceeds {VAR_99_PCT_ALERT}% threshold\n\n"
                       f"`Portfolio vol: {port_vol_annual:.1f}% annual`\n"
                       f"`β to SPY: {portfolio_beta:.2f}`\n"
                       f"Reduce gross exposure or hedge.\n\n"
                       f"[Portfolio](https://justhodl.ai/portfolio/)")
                if send_telegram(msg, chat_id):
                    history[key] = now_iso
                    alerts_sent += 1

        # Sector concentration
        if payload["alerts_summary"]["sector_concentration_breach"]:
            top_sec = sector_pcts[0]
            key = f"concentration:{top_sec[0]}"
            if should_alert(history, key):
                msg = (f"📊 *SECTOR CONCENTRATION ALERT*\n"
                       f"{top_sec[0]}: `{top_sec[1]:.1f}%` of portfolio — exceeds {SECTOR_CONCENTRATION_ALERT}%\n\n"
                       f"`HHI: {hhi:.0f} ({hhi_label})`\n"
                       f"Consider rebalancing.")
                if send_telegram(msg, chat_id):
                    history[key] = now_iso
                    alerts_sent += 1

        # Correlation clusters
        for cluster in correlation_clusters[:2]:  # max 2 cluster alerts per run
            syms = "+".join(cluster["symbols"])
            key = f"cluster:{syms}"
            if should_alert(history, key):
                symstr = ", ".join(cluster["symbols"])
                msg = (f"🔗 *CORRELATION CLUSTER ALERT*\n"
                       f"`{symstr}` move together (avg corr {cluster['avg_pairwise_correlation']})\n"
                       f"Combined weight: `{cluster['total_weight_pct']}%`\n\n"
                       f"_Diversification is illusory — these positions act as one._")
                if send_telegram(msg, chat_id):
                    history[key] = now_iso
                    alerts_sent += 1

        save_alert_history(history)

    payload["alerts_sent"] = alerts_sent

    # ─── Write sidecar ───
    s3.put_object(Bucket=S3_BUCKET, Key=RISK_KEY,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=1800")

    print(f"  ✓ risk written · alerts sent={alerts_sent}")

    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "n_positions": len(positions),
        "total_market_value": round(total_value, 2),
        "portfolio_vol_annual_pct": round(port_vol_annual, 2),
        "portfolio_beta_spy": round(portfolio_beta, 3),
        "var_1d_99_pct": round(var_1d_99_pct, 2),
        "var_1d_99_dollars": round(var_1d_99_dollars, 2),
        "hhi": round(hhi, 0),
        "stops_hit_count": len(stops_hit),
        "alerts_sent": alerts_sent,
        "elapsed_seconds": round(elapsed, 2),
    })}
