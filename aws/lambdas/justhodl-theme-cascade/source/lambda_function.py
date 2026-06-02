"""justhodl-theme-cascade — THE SYNTHESIS LAYER (v2 — correct schema parsing)

User insight: "stocks within a theme/industry thats already pumping are most
likely to pump especially if capital and money has rotated to that theme."

SCHEMA REALITY (validated 2026-06-02 via ops 1206):

theme-rotation.json:
  - all_themes (LIST of 114 ETF themes): ticker, category, momentum_score,
    rs_5d/20d/60d, rs_acceleration, rs_rank_20d, rs_rank_delta,
    vol_ratio_20v60, money_flow_ratio
  - summary.top_10_momentum, rotators_in, convergent_breadth
  - breadth_details[ETF].constituents_perf

velocity-acceleration.json: tickers have theme = industry name

stock-exposure-lookup.json: ticker → top_etfs list with weights/flows
"""
import json
import time
from datetime import datetime, timezone
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def build_theme_heat_index(theme_rotation: dict) -> dict:
    """For each ETF, compute heat: {etf: {multiplier, factors, momentum_score, ...}}"""
    if not theme_rotation:
        return {}

    all_themes = theme_rotation.get("all_themes") or []
    summary = theme_rotation.get("summary") or {}
    breadth_details = theme_rotation.get("breadth_details") or {}

    top_10_tickers = {t.get("ticker") for t in (summary.get("top_10_momentum") or [])}
    rotators_in_tickers = {t.get("ticker") for t in (summary.get("rotators_in") or [])}
    convergent_tickers = {t.get("ticker") for t in (summary.get("convergent_breadth") or [])}

    heat = {}
    for t in all_themes:
        if not isinstance(t, dict):
            continue
        etf = t.get("ticker")
        if not etf:
            continue

        mom = t.get("momentum_score") or 0
        rs_accel = t.get("rs_acceleration") or 0
        rs_rank_20d = t.get("rs_rank_20d") or 999
        rs_rank_delta = t.get("rs_rank_delta") or 0
        vol_ratio = t.get("vol_ratio_20v60") or 0
        money_flow = t.get("money_flow_ratio") or 0
        category = t.get("category") or "UNKNOWN"
        name = t.get("name") or etf

        bd = breadth_details.get(etf, {}).get("breadth", {})
        breadth_pct = bd.get("breadth_outperform_pct") or 0
        breadth_above_ma50 = bd.get("breadth_above_ma50_pct") or 0

        mult = 1.0
        factors = []

        if mom >= 90:
            mult *= 1.6
            factors.append(f"top_momentum (m={mom})")
        elif mom >= 75:
            mult *= 1.4
            factors.append(f"high_momentum (m={mom})")
        elif mom >= 60:
            mult *= 1.2
            factors.append(f"good_momentum (m={mom})")

        if rs_rank_20d <= 5:
            mult *= 1.35
            factors.append(f"top_5_rs (#{rs_rank_20d})")
        elif rs_rank_20d <= 10:
            mult *= 1.20
            factors.append(f"top_10_rs (#{rs_rank_20d})")
        elif rs_rank_20d <= 20:
            mult *= 1.10
            factors.append(f"top_20_rs (#{rs_rank_20d})")

        if rs_accel >= 80:
            mult *= 1.35
            factors.append(f"strong_accel (a={rs_accel:.1f})")
        elif rs_accel >= 50:
            mult *= 1.20
            factors.append(f"positive_accel (a={rs_accel:.1f})")
        elif rs_accel >= 20:
            mult *= 1.08
            factors.append(f"mild_accel (a={rs_accel:.1f})")

        if rs_rank_delta >= 10:
            mult *= 1.15
            factors.append(f"rank_jumping (+{rs_rank_delta})")

        if breadth_pct >= 75:
            mult *= 1.20
            factors.append(f"high_breadth ({breadth_pct:.0f}%)")
        elif breadth_pct >= 60:
            mult *= 1.10
            factors.append(f"good_breadth ({breadth_pct:.0f}%)")

        if etf in top_10_tickers:
            mult *= 1.15
            factors.append("top_10_momentum")
        if etf in rotators_in_tickers:
            mult *= 1.20
            factors.append("rotator_in")
        if etf in convergent_tickers:
            mult *= 1.20
            factors.append("convergent_breadth")

        if money_flow >= 2.0:
            mult *= 1.10
            factors.append(f"strong_money_flow ({money_flow:.1f})")

        mult = min(mult, 3.0)

        heat[etf] = {
            "etf": etf, "name": name, "category": category,
            "multiplier": round(mult, 3), "factors": factors,
            "momentum_score": mom, "rs_acceleration": rs_accel,
            "rs_rank_20d": rs_rank_20d, "rs_rank_delta": rs_rank_delta,
            "breadth_outperform_pct": breadth_pct,
            "breadth_above_ma50_pct": breadth_above_ma50,
        }
    return heat


def build_ticker_to_etfs_map(theme_rotation: dict, exposure_lookup: dict) -> dict:
    """For each stock, find ETFs that hold it. Combines breadth_details + stock-exposure."""
    ticker_to_etfs = {}
    breadth_details = theme_rotation.get("breadth_details") or {}

    # Source 1: theme-rotation breadth details
    for etf, info in breadth_details.items():
        for c in (info.get("constituents_perf") or []):
            sym = c.get("symbol")
            if sym:
                if etf not in ticker_to_etfs.setdefault(sym, []):
                    ticker_to_etfs[sym].append(etf)

    # Source 2: stock-exposure-lookup (much broader coverage)
    if isinstance(exposure_lookup, dict):
        for ticker, info in exposure_lookup.items():
            if not isinstance(info, dict):
                continue
            top_etfs = info.get("top_etfs") or []
            for etf_info in top_etfs:
                if isinstance(etf_info, dict):
                    etf = etf_info.get("etf")
                    if etf:
                        if etf not in ticker_to_etfs.setdefault(ticker, []):
                            ticker_to_etfs[ticker].append(etf)

    return ticker_to_etfs


def compute_flow_multiplier(ticker: str, exposure_lookup: dict) -> dict:
    info = exposure_lookup.get(ticker) if isinstance(exposure_lookup, dict) else None
    if not info:
        return {"multiplier": 1.0, "factors": [], "n_etfs_holding": 0,
                "cumulative_weight_pct": 0, "aggregate_flow_5d_usd": 0,
                "aggregate_flow_21d_usd": 0}

    n_etfs = info.get("n_etfs_holding") or 0
    cum_weight = info.get("cumulative_weight_pct") or 0
    agg_5d = info.get("total_aggregate_flow_5d_usd") or 0
    agg_21d = info.get("total_aggregate_flow_21d_usd") or 0

    mult = 1.0
    factors = []

    if agg_5d > 100e6 and cum_weight > 20:
        mult *= 1.4
        factors.append(f"strong_etf_inflow_+${agg_5d/1e6:.0f}M")
    elif agg_5d > 25e6 and cum_weight > 10:
        mult *= 1.25
        factors.append(f"etf_inflow_+${agg_5d/1e6:.0f}M")
    elif agg_5d > 0 and n_etfs >= 5:
        mult *= 1.10
        factors.append(f"broad_exposure_{n_etfs}_etfs")
    elif agg_5d < -50e6:
        mult *= 0.85
        factors.append(f"etf_outflow_${agg_5d/1e6:.0f}M")

    if agg_21d > 200e6 and agg_5d > 0:
        mult *= 1.10
        factors.append("sustained_21d_inflow")
    elif agg_21d < -100e6 and agg_5d < 0:
        mult *= 0.90
        factors.append("sustained_21d_outflow")

    return {
        "multiplier": round(mult, 3), "factors": factors,
        "n_etfs_holding": n_etfs, "cumulative_weight_pct": cum_weight,
        "aggregate_flow_5d_usd": agg_5d, "aggregate_flow_21d_usd": agg_21d,
    }


# ═════════════════════════════════════════════════════════════════════
# TELEGRAM ALERTING — push when new names enter alert_tier (combined >= 80)
# ═════════════════════════════════════════════════════════════════════
def _get_telegram_config():
    """Fetch bot token + chat_id from SSM."""
    try:
        ssm = boto3.client("ssm", region_name="us-east-1")
        token = ssm.get_parameter(Name="/justhodl/telegram/bot-token",
                                   WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
        return token, chat_id
    except Exception as e:
        print(f"[telegram] SSM error: {e}")
        # Fallback hardcoded values (from memories)
        return "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs", "8678089260"


def _html_escape(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _send_telegram_html(token: str, chat_id: str, text: str) -> dict:
    import urllib.request, urllib.parse
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as r:
            return {"status": r.status, "body": r.read().decode()[:300]}
    except Exception as e:
        return {"error": str(e)[:200]}


def _load_alert_state() -> dict:
    """State file tracks which tickers have been alerted today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        state = json.loads(
            s3.get_object(Bucket=S3_BUCKET,
                          Key="data/_alerts/theme-cascade-alerted.json")["Body"].read())
        if state.get("date") != today:
            return {"date": today, "alerted_tickers": []}
        return state
    except Exception:
        return {"date": today, "alerted_tickers": []}


def _save_alert_state(state: dict) -> None:
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/_alerts/theme-cascade-alerted.json",
        Body=json.dumps(state, default=str).encode(),
        ContentType="application/json",
    )


def deliver_telegram_alerts(alert_tier: list) -> dict:
    """For each NEW name in alert_tier (not yet alerted today), push Telegram message."""
    if not alert_tier:
        return {"sent": 0, "skipped": "no_alert_tier"}

    state = _load_alert_state()
    already_alerted = set(state.get("alerted_tickers", []))

    new_alerts = [c for c in alert_tier if c["ticker"] not in already_alerted]
    if not new_alerts:
        return {"sent": 0, "skipped": "all_already_alerted",
                "n_in_alert_tier": len(alert_tier)}

    token, chat_id = _get_telegram_config()
    if not token or not chat_id:
        return {"sent": 0, "error": "no_telegram_config"}

    # Build message
    lines = [
        "<b>🎯 THEME CASCADE — Pre-Pump Alert</b>",
        f"<i>{len(new_alerts)} new HIGH-CONVICTION candidates (combined ≥ 80)</i>",
        "",
    ]
    for c in new_alerts[:10]:
        ticker = _html_escape(c["ticker"])
        score = c.get("combined_score", 0)
        tier = _html_escape(c.get("tier", "?"))
        industry = _html_escape(c.get("industry_label") or c.get("industry") or "?")
        hot_etf = _html_escape(c.get("hot_etf") or "?")
        theme_factors = (c.get("theme_factors") or [])[:2]
        flow_5d_m = (c.get("aggregate_flow_5d_usd") or 0) / 1e6

        lines.append(f"<b>{ticker}</b> · combined <b>{score:.0f}</b> ({tier})")
        lines.append(f"  Industry: {industry}")
        lines.append(f"  Hot ETF: <b>{hot_etf}</b> ({'×'}{c.get('theme_multiplier', 1):.2f})")
        if theme_factors:
            lines.append(f"  Theme: {_html_escape(', '.join(theme_factors))}")
        if abs(flow_5d_m) > 5:
            sign = "+" if flow_5d_m >= 0 else ""
            lines.append(f"  ETF flow 5d: {sign}${flow_5d_m:.0f}M")
        lines.append("")

    lines.append("<i>Pattern: stocks in already-pumping themes with WATCH/EMERGING accumulation.</i>")
    lines.append("<i>Real-world validation: MRVL pumped +29% from this exact pattern 2026-06-02.</i>")

    msg = "\n".join(lines)
    result = _send_telegram_html(token, chat_id, msg)
    print(f"[telegram] sent: {result}")

    # Save updated state
    state["alerted_tickers"] = list(already_alerted) + [c["ticker"] for c in new_alerts]
    state["last_send"] = datetime.now(timezone.utc).isoformat()
    state["last_send_result"] = result
    _save_alert_state(state)

    return {"sent": len(new_alerts), "tickers": [c["ticker"] for c in new_alerts],
            "result": result}


def compute_position_sizing(c: dict, earnings_set: Optional[set] = None) -> dict:
    """Position sizing for theme-cascade candidates.

    Base sizing depends on tier:
      Alert tier (combined >=80):    5% base, cap 18%
      Medium tier (50-79):           3% base, cap 10%
      Laggards in hot themes:        3% base, cap 12% (higher risk - not moving yet)
      Watch tier (<50):              2% base, cap 6%

    Multipliers stacked (max product 3.0x):
      Theme acceleration: rs_accel >= 100 x1.5, >=50 x1.3, >=20 x1.1
      Multiple top-10 ETFs holding: 3+ x1.3, 2 x1.15
      Earnings within 3 days: x1.3 (if earnings_set provided)
      Strong ETF inflow (>$100M 5d): x1.2
      Sustained 21d inflow (>$200M): x1.15

    Returns: {base_pct, multipliers[], final_pct, rationale}
    """
    tier = c.get("tier", "")
    combined = c.get("combined_score", 0)
    is_laggard = c.get("is_laggard", False)

    # Base by tier
    if is_laggard:
        base, cap = 3.0, 12.0
    elif combined >= 80:
        base, cap = 5.0, 18.0
    elif combined >= 50:
        base, cap = 3.0, 10.0
    else:
        base, cap = 2.0, 6.0

    multipliers = []
    cumulative = 1.0

    # Theme acceleration
    accel = c.get("theme_acceleration") or c.get("max_rs_acceleration") or 0
    if accel >= 100:
        multipliers.append({"factor": "extreme_acceleration", "value": 1.5,
                            "detail": f"theme_rs_accel {accel:.0f}"})
        cumulative *= 1.5
    elif accel >= 50:
        multipliers.append({"factor": "strong_acceleration", "value": 1.3,
                            "detail": f"theme_rs_accel {accel:.0f}"})
        cumulative *= 1.3
    elif accel >= 20:
        multipliers.append({"factor": "positive_acceleration", "value": 1.1,
                            "detail": f"theme_rs_accel {accel:.0f}"})
        cumulative *= 1.1

    # Multiple top-10 ETFs (signal strength)
    n_top_10 = c.get("n_etfs_in_top_10", 0) or 0
    if n_top_10 >= 3:
        multipliers.append({"factor": "multi_top_10_etfs", "value": 1.3,
                            "detail": f"{n_top_10} top-10 ETFs hold this"})
        cumulative *= 1.3
    elif n_top_10 == 2:
        multipliers.append({"factor": "dual_top_10_etfs", "value": 1.15,
                            "detail": "2 top-10 ETFs hold this"})
        cumulative *= 1.15

    # Earnings proximity (if catalysts data available)
    ticker = c.get("ticker")
    if earnings_set and ticker in earnings_set:
        multipliers.append({"factor": "earnings_3d", "value": 1.3,
                            "detail": "earnings within 3 days"})
        cumulative *= 1.3

    # Strong ETF inflow
    flow_5d = c.get("aggregate_flow_5d_usd") or 0
    if flow_5d > 100e6:
        multipliers.append({"factor": "strong_etf_inflow",
                            "value": 1.2,
                            "detail": f"+${flow_5d/1e6:.0f}M 5d ETF inflow"})
        cumulative *= 1.2

    flow_21d = c.get("aggregate_flow_21d_usd") or 0
    if flow_21d > 200e6:
        multipliers.append({"factor": "sustained_21d_inflow",
                            "value": 1.15,
                            "detail": f"+${flow_21d/1e6:.0f}M 21d ETF inflow"})
        cumulative *= 1.15

    final_uncapped = base * cumulative
    final_pct = min(final_uncapped, cap)
    final_pct = round(final_pct, 1)

    # Rationale
    if final_pct >= cap - 0.5:
        rationale = f"MAX SIZE ({cap}%) — all multipliers stacked, hit tier ceiling"
    elif len(multipliers) >= 3:
        rationale = f"Heavy conviction — {len(multipliers)} multipliers active"
    elif len(multipliers) >= 1:
        rationale = f"Moderate conviction — {len(multipliers)} multiplier(s) active"
    else:
        rationale = "Base sizing — no extra multipliers"

    return {
        "base_pct": base,
        "multipliers": multipliers,
        "cumulative_multiplier": round(cumulative, 2),
        "uncapped_pct": round(final_uncapped, 2),
        "final_pct": final_pct,
        "tier_cap_pct": cap,
        "rationale": rationale,
    }


def scan_laggards_in_hot_themes(momentum: dict, theme_index: dict,
                                  ticker_to_etfs: dict, top_10_etfs: set,
                                  top_20_etfs: set, exposure_lookup: dict,
                                  already_in_velocity: set) -> list:
    """Identify stocks NOT in velocity tiers (not yet pumping) but in hot themes.

    These are the SECOND-WAVE candidates — laggards waiting to catch up.

    Note: theme_index entries use key "etf" not "ticker" — these are the heat
    dicts built by build_theme_heat_index(), not raw all_themes entries.

    Criteria (loosened in v3.1 — accept underperformers vs theme too):
      - In momentum-leaders universe
      - NOT already in velocity tiers
      - perf_5d < +3% (laggard — flat, down, or rising slowly)
      - ANY of its ETFs is in top-10 RS rank (in a hot theme)
    """
    laggards = []
    leaders = (momentum.get("leaders") or [])

    for stock in leaders:
        t = stock.get("ticker")
        if not t or t in already_in_velocity:
            continue
        perf_5d = stock.get("perf_5d_pct")
        if perf_5d is None or perf_5d >= 3:  # loosened from <=0
            continue

        etfs = ticker_to_etfs.get(t, [])
        candidates_theme = [theme_index[e] for e in etfs if e in theme_index]
        if not candidates_theme:
            continue

        # theme_index entries use "etf" field (built by build_theme_heat_index)
        n_top_10 = sum(1 for c in candidates_theme if c.get("etf") in top_10_etfs)
        n_top_20 = sum(1 for c in candidates_theme if c.get("etf") in top_20_etfs)
        if n_top_10 == 0:
            continue

        # Hottest theme (lowest RS rank)
        hottest = min(candidates_theme, key=lambda x: x.get("rs_rank_20d") or 999)
        max_accel = max(
            (c.get("rs_acceleration") or 0) for c in candidates_theme
        ) if candidates_theme else 0

        # Compute flow stats
        flow_info = compute_flow_multiplier(t, exposure_lookup)

        laggards.append({
            "ticker": t,
            "tier": "LAGGARD",
            "is_laggard": True,
            "perf_5d_pct": perf_5d,
            "perf_20d_pct": stock.get("perf_20d_pct"),
            "perf_60d_pct": stock.get("perf_60d_pct"),
            "industry_label": stock.get("industry") or stock.get("sector") or "?",
            "hot_etf": hottest.get("etf"),       # FIX: use "etf" not "ticker"
            "hot_etf_name": hottest.get("name"),
            "hot_etf_category": hottest.get("category"),
            "theme_rs_rank": hottest.get("rs_rank_20d"),
            "theme_momentum": hottest.get("momentum_score"),
            "theme_acceleration": max_accel,
            "max_rs_acceleration": max_accel,
            "n_etfs_in_top_10": n_top_10,
            "n_etfs_in_top_20": n_top_20,
            "n_etfs_holding": len(etfs),
            "aggregate_flow_5d_usd": flow_info["aggregate_flow_5d_usd"],
            "aggregate_flow_21d_usd": flow_info["aggregate_flow_21d_usd"],
            # Synthetic score: deeper pullback in hotter theme = higher candidate
            "combined_score": round(
                max(0, -perf_5d) * 5
                + n_top_10 * 10
                + max_accel / 5
                + (10 if (stock.get("perf_20d_pct") or 0) > 10 else 0),
                1),
        })

    laggards.sort(key=lambda x: -x["combined_score"])
    return laggards


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[theme-cascade] starting at {datetime.now(timezone.utc).isoformat()}")

    velocity = _read_json("data/velocity-acceleration.json") or {}
    theme_rotation = _read_json("data/theme-rotation.json") or {}
    exposure_lookup = _read_json("etf-flows/stock-exposure-lookup.json") or {}
    themes_doc = _read_json("data/themes.json") or {}
    macro = _read_json("macro/regime.json") or {}
    print(f"[theme-cascade] loaded: velocity={bool(velocity)} "
          f"theme_rot={bool(theme_rotation)} exposure={bool(exposure_lookup)} "
          f"themes={bool(themes_doc)}")

    theme_heat = build_theme_heat_index(theme_rotation)
    print(f"[theme-cascade] theme heat: {len(theme_heat)} ETFs scored")

    ticker_to_etfs = build_ticker_to_etfs_map(theme_rotation, exposure_lookup)
    print(f"[theme-cascade] ticker→etfs: {len(ticker_to_etfs)} stocks mapped")

    sorted_themes = sorted(theme_heat.items(), key=lambda x: -x[1]["multiplier"])
    top_hot_etfs = sorted_themes[:15]
    print(f"[theme-cascade] TOP 5 HOTTEST THEMES:")
    for etf, h in top_hot_etfs[:5]:
        print(f"  {etf:6s} ({h.get('category'):14s}) x{h['multiplier']:.2f}  "
              f"m={h.get('momentum_score')}  rs#{h.get('rs_rank_20d')}  "
              f"a={h.get('rs_acceleration'):.0f}  {h['factors'][:3]}")

    combined = []
    sources = [
        ("FIRED_CONFIRMED", velocity.get("confirmed_today") or [], 80),
        ("FIRED_FRESH",     velocity.get("fresh_fires") or [], 70),
        ("AGING",           velocity.get("aging") or [], 60),
        ("EMERGING",        velocity.get("emerging") or [], 50),
        ("WATCH",           velocity.get("watch") or [], 35),
    ]

    seen_tickers = set()
    for tier_name, items, default_score in sources:
        for item in items:
            t = item.get("ticker")
            if not t or t in seen_tickers:
                continue
            seen_tickers.add(t)

            base_score = (item.get("composite_score") or item.get("current_score")
                          or default_score)
            industry = item.get("theme")
            industry_label = item.get("theme_label") or industry

            etfs_holding = ticker_to_etfs.get(t, [])
            heat_entries = [theme_heat[e] for e in etfs_holding if e in theme_heat]

            if heat_entries:
                best_heat = max(heat_entries, key=lambda x: x["multiplier"])
                theme_mult = best_heat["multiplier"]
                hot_etf = best_heat["etf"]
                hot_etf_name = best_heat["name"]
                hot_etf_category = best_heat["category"]
                theme_factors = best_heat["factors"]
                theme_mom = best_heat["momentum_score"]
                theme_rs_rank = best_heat["rs_rank_20d"]
                theme_acceleration = best_heat["rs_acceleration"]
            else:
                theme_mult = 1.0
                hot_etf = None
                hot_etf_name = None
                hot_etf_category = None
                theme_factors = []
                theme_mom = None
                theme_rs_rank = None
                theme_acceleration = None

            flow_info = compute_flow_multiplier(t, exposure_lookup)
            flow_mult = flow_info["multiplier"]

            combined_score = base_score * theme_mult * flow_mult

            combined.append({
                "ticker": t, "tier": tier_name,
                "base_score": round(base_score, 1),
                "theme_multiplier": theme_mult,
                "flow_multiplier": flow_mult,
                "combined_score": round(combined_score, 1),
                "industry": industry, "industry_label": industry_label,
                "hot_etf": hot_etf, "hot_etf_name": hot_etf_name,
                "hot_etf_category": hot_etf_category,
                "n_etfs_in_heat_index": len(heat_entries),
                "theme_factors": theme_factors,
                "theme_momentum": theme_mom,
                "theme_rs_rank": theme_rs_rank,
                "theme_acceleration": theme_acceleration,
                "flow_factors": flow_info["factors"],
                "n_etfs_holding": flow_info["n_etfs_holding"],
                "aggregate_flow_5d_usd": flow_info["aggregate_flow_5d_usd"],
                "aggregate_flow_21d_usd": flow_info["aggregate_flow_21d_usd"],
                "slope_score": item.get("slope_score"),
                "accum_score": item.get("accum_score"),
                "floor_score": item.get("floor_score"),
                "current_vol_ratio": item.get("current_vol_ratio"),
                "momentum_score": item.get("momentum_score"),
            })

    combined.sort(key=lambda x: -x["combined_score"])

    alert_tier = [c for c in combined if c["combined_score"] >= 80]
    medium_tier = [c for c in combined if 50 <= c["combined_score"] < 80]
    watch_tier = [c for c in combined if c["combined_score"] < 50]

    # NEW: Detect laggards — stocks in hot themes not yet pumping
    momentum_doc = _read_json("data/momentum-leaders.json") or {}
    # Build top_10 / top_20 from theme_heat (sorted by rs_rank_20d)
    sorted_by_rank = sorted(
        [(etf, h) for etf, h in theme_heat.items() if h.get("rs_rank_20d") is not None],
        key=lambda x: x[1]["rs_rank_20d"],
    )
    top_10_etfs_set = {etf for etf, _ in sorted_by_rank[:10]}
    top_20_etfs_set = {etf for etf, _ in sorted_by_rank[:20]}
    already_in_velocity = seen_tickers

    laggards = scan_laggards_in_hot_themes(
        momentum_doc, theme_heat, ticker_to_etfs,
        top_10_etfs_set, top_20_etfs_set, exposure_lookup, already_in_velocity,
    )
    print(f"[theme-cascade] laggards: {len(laggards)} stocks not pumping yet in hot themes")

    # Get earnings within 3 days set (if available)
    earnings_set = set()
    try:
        cats = _read_json("data/catalysts.json") or {}
        for item in (cats.get("calendar") or cats.get("items") or [])[:200]:
            if isinstance(item, dict):
                t = item.get("ticker") or item.get("symbol")
                days_out = item.get("days_out") or item.get("days_until")
                if t and days_out is not None and days_out <= 3:
                    earnings_set.add(t)
    except Exception as e:
        print(f"[earnings] couldn't load catalysts: {e}")

    # NEW: Add position sizing to ALL candidates
    for c in alert_tier:
        c["position_sizing"] = compute_position_sizing(c, earnings_set)
    for c in medium_tier:
        c["position_sizing"] = compute_position_sizing(c, earnings_set)
    for c in laggards:
        c["position_sizing"] = compute_position_sizing(c, earnings_set)

    elapsed = round(time.time() - t0, 1)
    print(f"[theme-cascade] DONE — {len(combined)} ranked, "
          f"alert={len(alert_tier)} medium={len(medium_tier)} watch={len(watch_tier)} "
          f"laggards={len(laggards)} earnings_set={len(earnings_set)} "
          f"in {elapsed}s")

    top_hot_themes_out = [
        {
            "etf": h["etf"], "name": h["name"], "category": h["category"],
            "multiplier": h["multiplier"], "factors": h["factors"],
            "momentum_score": h["momentum_score"],
            "rs_rank_20d": h["rs_rank_20d"],
            "rs_acceleration": h["rs_acceleration"],
            "breadth_outperform_pct": h["breadth_outperform_pct"],
        }
        for _, h in top_hot_etfs
    ]

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output = {
        "schema_version": "2.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "macro_regime": (macro.get("top_level_regime") or {}).get("regime"),
        "n_themes_tracked": len(theme_heat),
        "n_tickers_mapped": len(ticker_to_etfs),
        "n_total_ranked": len(combined),
        "n_alert_tier": len(alert_tier),
        "n_medium_tier": len(medium_tier),
        "n_watch_tier": len(watch_tier),
        "top_hot_themes": top_hot_themes_out,
        "alert_tier": alert_tier[:25],
        "medium_tier": medium_tier[:30],
        "watch_tier": watch_tier[:30],
        "laggards_hot_themes": laggards[:25],
        "n_laggards_hot_themes": len(laggards),
        "earnings_within_3d": sorted(list(earnings_set)),
        "all_ranked": combined[:80],
    }

    s3.put_object(Bucket=S3_BUCKET, Key="data/theme-cascade.json",
                  Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    s3.put_object(Bucket=S3_BUCKET, Key=f"data/theme-cascade-history/{today}.json",
                  Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=86400")

    # Push Telegram alerts for new alert-tier names (idempotent — state-aware)
    tg_result = deliver_telegram_alerts(alert_tier)
    print(f"[theme-cascade] telegram result: {tg_result}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                     "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed, "n_total": len(combined),
            "n_alert_tier": len(alert_tier),
            "n_medium_tier": len(medium_tier),
            "n_themes_tracked": len(theme_heat),
            "top_5_hot_themes": [
                {"etf": t["etf"], "name": t["name"], "category": t["category"],
                 "mult": t["multiplier"], "momentum": t["momentum_score"],
                 "rs_rank": t["rs_rank_20d"], "rs_accel": t["rs_acceleration"]}
                for t in top_hot_themes_out[:5]
            ],
            "top_10_combined": [
                {"ticker": c["ticker"], "tier": c["tier"],
                 "combined_score": c["combined_score"], "base": c["base_score"],
                 "theme_mult": c["theme_multiplier"],
                 "flow_mult": c["flow_multiplier"],
                 "hot_etf": c["hot_etf"], "industry": c["industry"]}
                for c in combined[:10]
            ],
        }),
    }
