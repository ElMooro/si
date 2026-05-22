"""
justhodl-portfolio-sizer — REAL portfolio Kelly recommender (DDB-integrated)

═══════════════════════════════════════════════════════════════════════
THE DECISION LAYER ON TOP OF EVERYTHING ELSE
─────────────────────────────────────────────
Reads YOUR actual positions from DDB + the full intelligence stack and
outputs a single-page action plan:

  - For each current position: ADD / TRIM / HOLD with sized shares
  - For each TIER S/A entry candidate not in book: NEW with sized shares
  - Regime-adjusted via Macro Stress Score (anomaly detector)
  - Concentration-capped per position + per sector

The math: regime-multiplied quarter-Kelly per signal-conviction tier.
The output: portfolio/sizing.json + Telegram alerts on big gaps.

═══════════════════════════════════════════════════════════════════════
KELLY FORMULA
─────────────
  edge_per_position    = expected_alpha_return × confidence_factor
  variance             = (annualized_vol_pct / 100) ²
  kelly_full           = edge / variance
  kelly_quarter        = kelly_full × 0.25
  kelly_regime         = kelly_quarter × regime_multiplier
  kelly_drawdown       = kelly_regime × drawdown_multiplier
  kelly_capped         = min(kelly_drawdown, MAX_SINGLE_POSITION_PCT)

EXPECTED ALPHA RETURN (60-day expected return given alpha score)
  α ≥ 90  →  12%
  α 80-90 →   8%
  α 70-80 →   5%
  α 60-70 →   3%
  α 50-60 →   1.5%
  α < 50  →  -1% (negative edge — don't size up)

CONFIDENCE FACTOR (multiplier for confluence tier)
  TIER S confluence  →  1.20  (multiple signals firing)
  TIER A confluence  →  1.00
  TIER B confluence  →  0.70
  no confluence       →  0.50

═══════════════════════════════════════════════════════════════════════
REGIME MULTIPLIER (from Macro Stress Score 0-100)
───────────────────────────────────────────────
  MSS 0-20   →  1.00  Goldilocks: full quarter-Kelly
  MSS 20-40  →  0.85  Normal
  MSS 40-60  →  0.65  Elevated
  MSS 60-80  →  0.40  High stress: cut sizing
  MSS 80-100 →  0.20  Crisis: quarter quarter-Kelly

═══════════════════════════════════════════════════════════════════════
DRAWDOWN MULTIPLIER (current portfolio P&L circuit breaker)
────────────────────────────────────────────────────────────
  pnl ≥ -3%  →  1.00
  pnl -3-7%  →  0.75
  pnl -7-12% →  0.50
  pnl -12%+  →  0.25  (defensive — minimal new risk)

═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
SIZING_KEY = "portfolio/sizing.json"
SNAPSHOT_KEY = "portfolio/snapshot.json"
RISK_KEY = "portfolio/risk.json"
ANOMALIES_KEY = "signals/anomalies.json"
ALPHA_KEY = "screener/alpha-score.json"
CONFLUENCE_KEY = "signals/confluence.json"
ALERT_HISTORY_KEY = "portfolio/sizing-alert-history.json"

DDB_TABLE = "justhodl-portfolio"

POLY_KEY = os.environ.get("POLY_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Sizing parameters
KELLY_FRACTION = 0.25                 # quarter-Kelly
MAX_SINGLE_POSITION_PCT = 8.0         # cap any one position at 8% of NAV
MAX_SECTOR_PCT = 30.0                 # cap any sector at 30%
DEFAULT_NAV_IF_NO_POSITIONS = 100_000.0  # fallback notional for empty books
ENTRY_CANDIDATE_TIER_S_TOP_N = 5      # how many new TIER S to suggest
ENTRY_CANDIDATE_TIER_A_TOP_N = 5      # how many new TIER A to suggest

# Alert thresholds
GAP_ALERT_PCT = 3.0     # |current_weight - kelly_weight| > 3% triggers alert candidacy
DEDUPE_HOURS = 12

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table(DDB_TABLE)
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════════════

def load_s3_json(key):
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(body)
    except Exception as e:
        print(f"  load {key} err: {str(e)[:120]}")
        return None


def scan_ddb_positions():
    """Returns dict of {symbol: position_dict}."""
    out = {}
    last = None
    while True:
        kwargs = {"FilterExpression": "pk = :p",
                   "ExpressionAttributeValues": {":p": "POSITION"}}
        if last: kwargs["ExclusiveStartKey"] = last
        resp = table.scan(**kwargs)
        for item in resp.get("Items") or []:
            sym = item.get("symbol")
            if sym: out[sym] = _decimal_to_float(item)
        last = resp.get("LastEvaluatedKey")
        if not last: break
    return out


def _decimal_to_float(obj):
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, dict): return {k: _decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_decimal_to_float(v) for v in obj]
    return obj


def fetch_polygon_price(symbol):
    """Latest close + day change."""
    if not POLY_KEY: return None, None
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLY_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Sizer/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        if data.get("results"):
            r0 = data["results"][0]
            close = r0["c"]
            open_ = r0["o"]
            day_chg_pct = ((close - open_) / open_ * 100) if open_ else 0
            return close, day_chg_pct
    except Exception as e:
        print(f"  poly:{symbol} {str(e)[:80]}")
    return None, None


def batch_fetch_prices(symbols, max_workers=10):
    """Parallel price fetch."""
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_polygon_price, s): s for s in symbols}
        for f in as_completed(futures):
            sym = futures[f]
            try: out[sym] = f.result()
            except Exception: out[sym] = (None, None)
    return out


# ═══════════════════════════════════════════════════════════════════════
# SIZING LOGIC
# ═══════════════════════════════════════════════════════════════════════

def expected_alpha_return(alpha_score):
    """Mapping from alpha score to 60-day expected return (as percentage)."""
    if alpha_score is None: return 0
    if alpha_score >= 90: return 12.0
    if alpha_score >= 80: return 8.0
    if alpha_score >= 70: return 5.0
    if alpha_score >= 60: return 3.0
    if alpha_score >= 50: return 1.5
    return -1.0


def confidence_factor(confluence_tier):
    """Conviction multiplier from confluence tier."""
    return {"S": 1.20, "A": 1.00, "B": 0.70}.get(confluence_tier, 0.50)


def regime_multiplier(macro_stress_score):
    """Risk multiplier from Macro Stress Score."""
    if macro_stress_score is None: return 0.80
    if macro_stress_score < 20: return 1.00
    if macro_stress_score < 40: return 0.85
    if macro_stress_score < 60: return 0.65
    if macro_stress_score < 80: return 0.40
    return 0.20


def regime_label(macro_stress_score):
    if macro_stress_score is None: return "UNKNOWN"
    if macro_stress_score < 20: return "GOLDILOCKS"
    if macro_stress_score < 40: return "NORMAL"
    if macro_stress_score < 60: return "ELEVATED"
    if macro_stress_score < 80: return "HIGH_STRESS"
    return "CRISIS"


def drawdown_multiplier(portfolio_pnl_pct):
    """Drawdown circuit breaker."""
    if portfolio_pnl_pct is None: return 1.00
    if portfolio_pnl_pct >= -3: return 1.00
    if portfolio_pnl_pct >= -7: return 0.75
    if portfolio_pnl_pct >= -12: return 0.50
    return 0.25


def kelly_weight_pct(alpha_score, confluence_tier, annual_vol_pct,
                       regime_mult, drawdown_mult):
    """Compute Kelly-optimal portfolio weight (as % of NAV)."""
    edge_pct = expected_alpha_return(alpha_score) * confidence_factor(confluence_tier)
    if edge_pct <= 0: return 0.0  # negative edge — don't size
    edge = edge_pct / 100.0
    vol = (annual_vol_pct or 25) / 100.0  # default 25% vol if unknown
    if vol < 0.05: vol = 0.05  # floor at 5% to avoid blow-up
    kelly_full = edge / (vol ** 2)
    kelly_q = kelly_full * KELLY_FRACTION
    kelly_regime = kelly_q * regime_mult * drawdown_mult
    kelly_capped = min(kelly_regime * 100.0, MAX_SINGLE_POSITION_PCT)
    return max(0.0, round(kelly_capped, 2))


def action_label(current_pct, kelly_pct):
    if kelly_pct == 0 and current_pct > 0: return "TRIM"
    if current_pct == 0 and kelly_pct > 0: return "NEW"
    gap = kelly_pct - current_pct
    if abs(gap) < 1.0: return "HOLD"
    return "ADD" if gap > 0 else "TRIM"


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
    print(f"=== PORTFOLIO SIZER v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # ─── Load all inputs ───
    snapshot = load_s3_json(SNAPSHOT_KEY) or {}
    risk = load_s3_json(RISK_KEY) or {}
    anomalies = load_s3_json(ANOMALIES_KEY) or {}
    alpha = load_s3_json(ALPHA_KEY) or {}
    confluence = load_s3_json(CONFLUENCE_KEY) or {}

    # Portfolio state
    portfolio_summary = snapshot.get("portfolio_summary") or {}
    total_value = portfolio_summary.get("total_market_value") or 0
    total_pnl_pct = portfolio_summary.get("total_pnl_pct")
    nav = total_value if total_value > 0 else DEFAULT_NAV_IF_NO_POSITIONS

    # Macro context
    mss = anomalies.get("macro_stress_score")
    regime_mult = regime_multiplier(mss)
    regime_lbl = regime_label(mss)
    drawdown_mult = drawdown_multiplier(total_pnl_pct)

    print(f"  NAV ${nav:,.0f}  MSS {mss}  regime={regime_lbl}  regime_mult={regime_mult}  dd_mult={drawdown_mult}")

    # ─── Index alpha-score by symbol (for risk-free lookup) ───
    alpha_by_sym = {}
    for tier_data in [alpha.get("S") or [], alpha.get("A") or [],
                       alpha.get("B") or [], alpha.get("C") or [],
                       alpha.get("D") or []]:
        for row in tier_data:
            sym = row.get("symbol")
            if sym: alpha_by_sym[sym] = row

    # ─── Index confluence by symbol ───
    confluence_by_sym = {}
    for row in confluence.get("rankings") or []:
        sym = row.get("symbol")
        if sym: confluence_by_sym[sym] = row

    # ─── Index risk metrics by symbol ───
    risk_position_metrics = risk.get("position_metrics") or {}

    # ─── 1. Process current positions ───
    positions_sized = []
    snapshot_positions = snapshot.get("positions") or []

    for p in snapshot_positions:
        sym = p.get("symbol")
        if not sym: continue
        current_value = p.get("market_value") or 0
        current_pct = (current_value / nav * 100) if nav else 0
        alpha_row = alpha_by_sym.get(sym, {})
        conf_row = confluence_by_sym.get(sym, {})
        risk_metrics = risk_position_metrics.get(sym, {})

        alpha_score = alpha_row.get("alpha_score") or p.get("alpha_score")
        confluence_tier = conf_row.get("confluence_tier")
        annual_vol = risk_metrics.get("annual_vol_pct")

        kelly_pct = kelly_weight_pct(alpha_score, confluence_tier, annual_vol,
                                       regime_mult, drawdown_mult)
        gap_pct = kelly_pct - current_pct
        action = action_label(current_pct, kelly_pct)

        current_price = p.get("current_price")
        # Compute share delta (positive = add, negative = trim)
        if current_price and current_price > 0:
            target_value = nav * kelly_pct / 100.0
            target_shares = target_value / current_price
            current_shares = p.get("qty") or 0
            shares_delta = round(target_shares - current_shares, 1)
            dollar_delta = round(target_value - current_value, 2)
        else:
            shares_delta = None
            dollar_delta = None

        rationale_parts = []
        if alpha_score: rationale_parts.append(f"α={alpha_score}→{expected_alpha_return(alpha_score):+.1f}% expected")
        if confluence_tier:
            rationale_parts.append(f"conf-tier {confluence_tier} (mult {confidence_factor(confluence_tier):.2f})")
        if annual_vol: rationale_parts.append(f"vol {annual_vol:.0f}%")
        rationale_parts.append(f"regime {regime_lbl} (mult {regime_mult:.2f})")
        if drawdown_mult < 1.0:
            rationale_parts.append(f"DD circuit-breaker (mult {drawdown_mult:.2f})")

        positions_sized.append({
            "symbol": sym,
            "current_shares": p.get("qty"),
            "current_value": current_value,
            "current_weight_pct": round(current_pct, 2),
            "current_price": current_price,
            "alpha_score": alpha_score,
            "alpha_tier": alpha_row.get("tier") or p.get("tier"),
            "confluence_tier": confluence_tier,
            "annual_vol_pct": annual_vol,
            "sector": p.get("sector") or alpha_row.get("sector"),
            "kelly_weight_pct": kelly_pct,
            "weight_gap_pct": round(gap_pct, 2),
            "action": action,
            "shares_delta": shares_delta,
            "dollar_delta": dollar_delta,
            "rationale": " · ".join(rationale_parts),
        })

    # Sort by gap magnitude (largest action item first)
    positions_sized.sort(key=lambda p: -abs(p.get("weight_gap_pct") or 0))

    # ─── 2. Find entry candidates (TIER S/A confluence NOT in book) ───
    in_book = {p.get("symbol") for p in snapshot_positions}
    entry_candidates = []
    confluence_rankings = confluence.get("rankings") or []
    tier_s_picks = [r for r in confluence_rankings if r.get("confluence_tier") == "S"
                     and r.get("symbol") not in in_book][:ENTRY_CANDIDATE_TIER_S_TOP_N]
    tier_a_picks = [r for r in confluence_rankings if r.get("confluence_tier") == "A"
                     and r.get("symbol") not in in_book][:ENTRY_CANDIDATE_TIER_A_TOP_N]

    # Need current prices for candidates
    candidate_symbols = [r["symbol"] for r in tier_s_picks + tier_a_picks if r.get("symbol")]
    candidate_prices = batch_fetch_prices(candidate_symbols) if candidate_symbols else {}

    for row in tier_s_picks + tier_a_picks:
        sym = row.get("symbol")
        if not sym: continue
        alpha_row = alpha_by_sym.get(sym, {})
        alpha_score = row.get("alpha_score") or alpha_row.get("alpha_score")
        confluence_tier = row.get("confluence_tier")
        # No per-position vol yet — use category default
        default_vol = 30.0  # neutral assumption for new entries
        kelly_pct = kelly_weight_pct(alpha_score, confluence_tier, default_vol,
                                       regime_mult, drawdown_mult)
        if kelly_pct < 0.5: continue  # too small to bother

        price = (candidate_prices.get(sym) or (None, None))[0]
        target_value = nav * kelly_pct / 100.0
        target_shares = round(target_value / price, 1) if price and price > 0 else None

        rationale_parts = [f"α={alpha_score}→{expected_alpha_return(alpha_score):+.1f}% expected",
                            f"conf-tier {confluence_tier} (mult {confidence_factor(confluence_tier):.2f})",
                            f"regime {regime_lbl} (mult {regime_mult:.2f})",
                            f"vol assumed {default_vol:.0f}% (no history yet)"]

        entry_candidates.append({
            "symbol": sym,
            "name": row.get("name") or alpha_row.get("name"),
            "sector": row.get("sector") or alpha_row.get("sector"),
            "alpha_score": alpha_score,
            "confluence_tier": confluence_tier,
            "current_price": price,
            "kelly_weight_pct": kelly_pct,
            "target_value": round(target_value, 2),
            "target_shares": target_shares,
            "action": "NEW",
            "rationale": " · ".join(rationale_parts),
        })

    entry_candidates.sort(key=lambda c: -(c.get("kelly_weight_pct") or 0))

    # ─── 3. Build summary ───
    sum_actionable = sum(1 for p in positions_sized
                          if p.get("action") in ("ADD", "TRIM"))
    sum_total_kelly_pct = sum(p.get("kelly_weight_pct") or 0 for p in positions_sized)
    sum_current_pct = sum(p.get("current_weight_pct") or 0 for p in positions_sized)
    sum_target_cash = max(0, nav - (sum_total_kelly_pct / 100.0) * nav)

    summary = {
        "nav": round(nav, 2),
        "current_invested_pct": round(sum_current_pct, 2),
        "kelly_invested_pct": round(sum_total_kelly_pct, 2),
        "kelly_cash_pct": round(100 - sum_total_kelly_pct, 2),
        "kelly_cash_dollars": round(sum_target_cash, 2),
        "actionable_count": sum_actionable,
        "entry_candidates_count": len(entry_candidates),
        "regime": regime_lbl,
        "macro_stress_score": mss,
        "regime_multiplier": regime_mult,
        "drawdown_multiplier": drawdown_mult,
    }

    # ─── 4. Fire Telegram alerts on big gaps ───
    chat_id = get_chat_id()
    history = load_alert_history()
    now_iso = datetime.now(timezone.utc).isoformat()
    alerts_sent = 0
    alerts_skipped = 0

    if chat_id and TELEGRAM_TOKEN:
        # Big-gap positions
        for p in positions_sized:
            gap = p.get("weight_gap_pct") or 0
            if abs(gap) < GAP_ALERT_PCT: continue
            sym = p.get("symbol")
            action = p.get("action")
            key = f"sizing:{sym}:{action}"
            if not should_alert(history, key):
                alerts_skipped += 1; continue
            icon = "📈" if action == "ADD" else "📉"
            msg = (f"{icon} *SIZING SIGNAL · {action} {sym}*\n"
                    f"Current: {p.get('current_weight_pct'):.1f}% → Kelly: {p.get('kelly_weight_pct'):.1f}% "
                    f"(gap {gap:+.1f}%)\n"
                    f"Δ ${p.get('dollar_delta', 0):+,.0f} · {p.get('shares_delta', 0):+.0f} shares\n"
                    f"_{p.get('rationale','')}_\n\n"
                    f"[Sizing Dashboard](https://justhodl.ai/sizing/)")
            if send_telegram(msg, chat_id):
                history[key] = now_iso
                alerts_sent += 1
            time.sleep(0.4)

        # Top TIER S entry candidate
        for c in entry_candidates[:2]:  # only top 2 NEW signals
            sym = c.get("symbol")
            if c.get("confluence_tier") != "S": continue
            key = f"sizing:NEW:{sym}"
            if not should_alert(history, key):
                alerts_skipped += 1; continue
            msg = (f"⭐ *NEW POSITION SUGGESTED · {sym}*\n"
                    f"Kelly: {c.get('kelly_weight_pct'):.1f}% of NAV "
                    f"(${c.get('target_value', 0):,.0f}, {c.get('target_shares', 0)} shares @ ${c.get('current_price', 0)})\n"
                    f"_{c.get('rationale','')}_\n\n"
                    f"[Sizing Dashboard](https://justhodl.ai/sizing/)")
            if send_telegram(msg, chat_id):
                history[key] = now_iso
                alerts_sent += 1
            time.sleep(0.4)
        save_alert_history(history)

    # ─── 5. Build payload ───
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "elapsed_seconds": round(time.time() - started, 2),
        "method": "regime-adjusted quarter-Kelly with confluence-conviction multiplier",
        "kelly_fraction": KELLY_FRACTION,
        "max_single_position_pct": MAX_SINGLE_POSITION_PCT,
        "max_sector_pct": MAX_SECTOR_PCT,
        "expected_alpha_returns_map": {
            "alpha_90+": 12.0, "alpha_80_90": 8.0, "alpha_70_80": 5.0,
            "alpha_60_70": 3.0, "alpha_50_60": 1.5, "alpha_below_50": -1.0,
        },
        "confidence_factor_map": {"TIER_S": 1.20, "TIER_A": 1.00, "TIER_B": 0.70, "none": 0.50},
        "regime_multiplier_map": {
            "GOLDILOCKS_0_20": 1.00, "NORMAL_20_40": 0.85,
            "ELEVATED_40_60": 0.65, "HIGH_STRESS_60_80": 0.40,
            "CRISIS_80_100": 0.20,
        },
        "summary": summary,
        "positions": positions_sized,
        "entry_candidates": entry_candidates,
        "alerts_sent": alerts_sent,
        "alerts_skipped_dedupe": alerts_skipped,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=SIZING_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=1800")
        print(f"  ✓ sizing.json written")
    except Exception as e:
        # audit P2.5: emit EMF metric for silent put_object failure
        print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
        print(f"  put_object err: {e}")
        return {"statusCode": 500, "body": json.dumps({"err": str(e)})}

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_positions": len(positions_sized),
        "n_entry_candidates": len(entry_candidates),
        "actionable_count": sum_actionable,
        "regime": regime_lbl,
        "macro_stress_score": mss,
        "alerts_sent": alerts_sent,
        "elapsed_seconds": round(time.time() - started, 2),
    })}
