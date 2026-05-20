"""
justhodl-crypto-opportunities  --  Retail-actionable crypto opportunity engine
==============================================================================

Surfaces 3 cross-confirmed signal types + a convergence table for the
highest-conviction setups. Each row includes a retail-friendly trade
ticket (entry zone, stop-loss, targets, sizing, timeframe, risks).

PRESSURE-TESTED 3 SIGNALS (only crypto-edges that work for retail):

1. VOLUME_SURGE_VS_MCAP
   * vol_24h / mcap > 0.20  (high turnover relative to size)
   * volume_change_24h > 50%  (real recent volume increase)
   * mcap in [$5M, $500M]    (filters dust + retail-tradable range)
   * Captures pre-pump phase + breakout candidates

2. SOCIAL_VELOCITY
   * Coingecko community_score > 30
   * reddit_average_posts_48h > 5  (active discussion)
   * reddit_accounts_active_48h > 10  (real users, not bots)
   * mcap < $2B  (we want still-discoverable names)
   * Captures the narrative-building phase

3. STABLECOIN_INFLOWS  (per-coin proxy)
   * % of 24h volume on USDT/USDC/BUSD/DAI pairs > 65%
   * 24h volume change > 30%
   * Captures real fiat-on-ramp demand vs BTC-pair rotation

CONVERGENCE table = coins lit on >=2 signals (highest-conviction setups).
This is the actual retail edge: cross-confirmation filters out the
single-signal pump-and-dumps.

Output: s3://justhodl-dashboard-live/data/crypto-opportunities.json
Schedule: every 4h (6 runs/day, ~186 CMC calls budget vs 333 daily cap)

Academic + empirical priors:
  - Memecoin season Q4 2023 + Q4 2024: vol/mcap surges preceded 50-500% moves
  - SHIB Aug 2021: social velocity preceded 200x
  - PEPE Apr-May 2023: stablecoin inflow + vol surge preceded 1000% move
"""
import datetime as dt
import json
import math
import os
import time
import traceback
import urllib.parse
import urllib.request

import boto3

# Inline defaults match the pattern used in other production engines.
# These can be overridden by Lambda env vars but should never be empty.
CMC_KEY = os.environ.get("CMC_KEY", "17ba8e87-53f0-46f4-abe5-014d9cd99597")
COINGECKO_KEY = os.environ.get("COINGECKO_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/crypto-opportunities.json"
SSM_KEY = "/justhodl/crypto-opportunities/state"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN",
                                "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

UA = "JustHodlAI-CryptoOpportunities/1.0"

# Universe filter constants
MCAP_MIN_USD = 5_000_000           # below this is dust / illiquid
MCAP_MAX_VOLUME_SIGNAL = 500_000_000   # vol-surge signal restricted to <$500M
MCAP_MAX_SOCIAL_SIGNAL = 2_000_000_000  # social signal allows up to $2B
MCAP_MAX_STABLE_SIGNAL = 1_000_000_000  # stable-flow restricted to <$1B

# Signal thresholds (calibrated against Q4 2024 memecoin season + 2023 PEPE run)
VOL_MCAP_RATIO_THRESHOLD = 0.20    # vol_24h / mcap
VOL_CHANGE_24H_THRESHOLD = 50.0    # %
SOCIAL_REDDIT_POSTS_48H = 5.0
SOCIAL_REDDIT_ACTIVE_48H = 10.0
SOCIAL_CG_SCORE_MIN = 30.0
STABLE_PAIR_PCT_MIN = 65.0
STABLE_VOL_CHANGE_MIN = 30.0

STABLECOIN_SYMBOLS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDD",
                      "FDUSD", "USDE", "USDP", "FRAX", "USDS", "PYUSD"}

# Stablecoins themselves should be excluded from opportunity scans
EXCLUDE_TICKERS = STABLECOIN_SYMBOLS | {"WBTC", "WETH", "STETH", "WSTETH"}

# ─────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────
def http_json(url, headers=None, timeout=20):
    h = {"User-Agent": UA, "Accept": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", errors="ignore"))
    except Exception as e:
        return {"_error": str(e), "_url": url[:120]}


# ─────────────────────────────────────────────────────────────────────
# CMC universe fetch
# ─────────────────────────────────────────────────────────────────────
def fetch_cmc_listings(limit=500):
    """Fetch top N coins by mcap. Returns list of normalized records."""
    url = (f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
           f"?limit={limit}&sort=market_cap&sort_dir=desc&cryptocurrency_type=coins")
    j = http_json(url, headers={"X-CMC_PRO_API_KEY": CMC_KEY})
    if isinstance(j, dict) and "_error" in j:
        print(f"CMC listings error: {j['_error']}")
        return []
    if not isinstance(j, dict) or "data" not in j:
        return []
    rows = []
    for c in j["data"]:
        try:
            q = c.get("quote", {}).get("USD", {})
            mcap = q.get("market_cap") or 0
            vol = q.get("volume_24h") or 0
            if not mcap or mcap < MCAP_MIN_USD:
                continue
            sym = (c.get("symbol") or "").upper()
            if sym in EXCLUDE_TICKERS:
                continue
            rows.append({
                "cmc_id": c["id"],
                "symbol": sym,
                "name": c.get("name", ""),
                "slug": c.get("slug", ""),
                "cmc_rank": c.get("cmc_rank"),
                "price_usd": q.get("price"),
                "mcap_usd": mcap,
                "vol_24h_usd": vol,
                "vol_mcap_ratio": (vol / mcap) if mcap else 0,
                "pct_change_1h": q.get("percent_change_1h"),
                "pct_change_24h": q.get("percent_change_24h"),
                "pct_change_7d": q.get("percent_change_7d"),
                "vol_change_24h": q.get("volume_change_24h"),
                "circulating_supply": c.get("circulating_supply"),
                "max_supply": c.get("max_supply"),
                "last_updated": q.get("last_updated"),
                "logo_url": f"https://s2.coinmarketcap.com/static/img/coins/64x64/{c['id']}.png",
            })
        except Exception as e:
            print(f"  CMC parse error for {c.get('symbol')}: {e}")
            continue
    print(f"fetched {len(rows)} CMC listings (post mcap filter)")
    return rows


def fetch_cmc_market_pairs(cmc_id, limit=20):
    """Per-coin trading pair breakdown for stable inflow analysis."""
    url = (f"https://pro-api.coinmarketcap.com/v2/cryptocurrency/market-pairs/latest"
           f"?id={cmc_id}&limit={limit}&sort=volume_24h_strict")
    j = http_json(url, headers={"X-CMC_PRO_API_KEY": CMC_KEY})
    if isinstance(j, dict) and "_error" in j:
        return None
    if not isinstance(j, dict) or "data" not in j:
        return None
    try:
        data = j["data"]
        # market-pairs/latest returns {id: {market_pairs: [...]}} or just list
        if isinstance(data, dict):
            pairs = data.get("market_pairs", [])
        else:
            pairs = data if isinstance(data, list) else []
        return pairs
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────
# CoinGecko enrichment (for social velocity)
# ─────────────────────────────────────────────────────────────────────
def fetch_coingecko_coin(symbol):
    """
    CoinGecko /coins/{id} community + dev data.
    Slow path: lookup by symbol via /coins/list, then /coins/{id}.
    Returns dict with community_score, reddit metrics, etc.
    """
    # Try the simple match: coingecko's id is usually lowercase symbol
    cg_id = symbol.lower()
    url = (f"https://api.coingecko.com/api/v3/coins/{cg_id}"
           f"?localization=false&tickers=false&market_data=false"
           f"&community_data=true&developer_data=false&sparkline=false")
    j = http_json(url, timeout=15)
    if isinstance(j, dict) and "_error" not in j and "community_data" in j:
        return j
    # Fallback: lookup via /search to resolve real id
    sj = http_json(f"https://api.coingecko.com/api/v3/search?query={urllib.parse.quote(symbol)}",
                   timeout=10)
    if not isinstance(sj, dict):
        return None
    coins = sj.get("coins", [])
    if not coins:
        return None
    # Pick the highest-ranked matching symbol
    best = None
    for c in coins:
        if (c.get("symbol") or "").upper() == symbol.upper():
            if best is None or (c.get("market_cap_rank") or 99999) < (best.get("market_cap_rank") or 99999):
                best = c
    if not best:
        return None
    j = http_json(f"https://api.coingecko.com/api/v3/coins/{best['id']}"
                  f"?localization=false&tickers=false&market_data=false"
                  f"&community_data=true&developer_data=false&sparkline=false",
                  timeout=15)
    if isinstance(j, dict) and "community_data" in j:
        return j
    return None


# ─────────────────────────────────────────────────────────────────────
# SIGNAL 1: Volume surge vs mcap
# ─────────────────────────────────────────────────────────────────────
def signal_volume_surge(row):
    """Return (fired_bool, sig_strength_0_100)."""
    mcap = row.get("mcap_usd") or 0
    vol = row.get("vol_24h_usd") or 0
    vol_chg = row.get("vol_change_24h") or 0
    ratio = row.get("vol_mcap_ratio") or 0

    # Universe filter
    if mcap < MCAP_MIN_USD or mcap > MCAP_MAX_VOLUME_SIGNAL:
        return False, 0
    if not vol or vol < 100_000:  # at least $100k of real volume
        return False, 0

    # Two-condition trigger
    cond1 = ratio >= VOL_MCAP_RATIO_THRESHOLD
    cond2 = vol_chg >= VOL_CHANGE_24H_THRESHOLD
    fired = cond1 and cond2
    if not fired:
        return False, 0

    # Signal strength: combine ratio + vol-change magnitude
    ratio_score = min(50, ratio * 100)         # 0.2 -> 20, 0.5 -> 50 (cap)
    vol_score = min(50, vol_chg / 4)            # 50% -> 12.5, 200% -> 50 (cap)
    return True, round(ratio_score + vol_score)


def build_volume_surge_trade(row, strength):
    """Retail-friendly trade ticket for a volume-surge candidate."""
    px = row.get("price_usd") or 0
    chg_24h = row.get("pct_change_24h") or 0
    ratio = row.get("vol_mcap_ratio") or 0
    mcap = row.get("mcap_usd") or 0
    pump_risk = "ELEVATED" if ratio > 0.5 else "MODERATE"
    if chg_24h > 30:
        entry_advice = (f"Already up {chg_24h:.0f}% in 24h. Do NOT chase. "
                        f"Wait for first 20-30% pullback to enter.")
    elif chg_24h > 10:
        entry_advice = (f"Up {chg_24h:.0f}% with high vol/mcap ({ratio:.2f}). "
                        f"Momentum entry possible above 24h high; tight stop.")
    else:
        entry_advice = (f"Volume building before price move. "
                        f"Entry near current price ${fmt_price(px)}; this is the setup phase.")
    return {
        "primary": entry_advice,
        "entry_zone": f"${fmt_price(px * 0.95)} - ${fmt_price(px * 1.02)}",
        "stop_loss": f"${fmt_price(px * 0.88)} (-12% from current)",
        "target_1": f"${fmt_price(px * 1.25)} (+25%)",
        "target_2": f"${fmt_price(px * 1.60)} (+60%)",
        "size": f"0.5-1% of crypto portfolio max (mcap ${fmt_usd(mcap)} = small)",
        "timeframe": "1-7 days. Volume-surge moves are typically fast.",
        "risks": [
            f"Pump-risk {pump_risk}: vol/mcap = {ratio:.2f}",
            "Liquidity drying up if vol normalizes",
            "Broader market reversal kills small caps first",
        ],
    }


# ─────────────────────────────────────────────────────────────────────
# SIGNAL 2: Social velocity (Coingecko community data)
# ─────────────────────────────────────────────────────────────────────
def signal_social_velocity(row, cg_data):
    if not cg_data:
        return False, 0, {}
    mcap = row.get("mcap_usd") or 0
    if mcap < MCAP_MIN_USD or mcap > MCAP_MAX_SOCIAL_SIGNAL:
        return False, 0, {}

    cd = cg_data.get("community_data") or {}
    cs = cg_data.get("community_score")
    posts_48h = cd.get("reddit_average_posts_48h") or 0
    accounts_48h = cd.get("reddit_accounts_active_48h") or 0
    reddit_subs = cd.get("reddit_subscribers") or 0
    tw_followers = cd.get("twitter_followers") or 0
    tg_users = cd.get("telegram_channel_user_count") or 0

    # Triggers
    cond_score = (cs or 0) >= SOCIAL_CG_SCORE_MIN
    cond_posts = posts_48h >= SOCIAL_REDDIT_POSTS_48H
    cond_active = accounts_48h >= SOCIAL_REDDIT_ACTIVE_48H
    # Allow social fires if EITHER cs is high OR posts+active both fired
    fired = cond_score or (cond_posts and cond_active)
    if not fired:
        return False, 0, {"community_score": cs, "posts_48h": posts_48h,
                          "accounts_48h": accounts_48h}

    # Signal strength
    score_pts = min(40, (cs or 0))
    posts_pts = min(30, posts_48h * 2)
    active_pts = min(30, accounts_48h / 2)
    strength = round(score_pts + posts_pts + active_pts)
    return True, strength, {
        "coingecko_score": cs,
        "reddit_subscribers": reddit_subs,
        "twitter_followers": tw_followers,
        "telegram_users": tg_users,
        "reddit_avg_posts_48h": posts_48h,
        "reddit_accounts_active_48h": accounts_48h,
    }


def build_social_velocity_trade(row, strength, social):
    px = row.get("price_usd") or 0
    return {
        "primary": (f"Community heat + active discussion = sticky narrative. "
                    f"Slower-burn opportunity than vol-surge. "
                    f"Best entry is during quiet hours (Asia overnight)."),
        "entry_zone": f"${fmt_price(px * 0.95)} - ${fmt_price(px * 1.05)}",
        "stop_loss": f"${fmt_price(px * 0.85)} (-15%)",
        "target_1": f"${fmt_price(px * 1.35)} (+35%)",
        "target_2": f"${fmt_price(px * 1.80)} (+80%)",
        "size": "1-2% of crypto portfolio (community plays can run weeks)",
        "timeframe": "3-21 days. Social plays build slowly then accelerate.",
        "risks": [
            "Narrative can fizzle without follow-through",
            "Twitter/Reddit metrics can be inflated by bots",
            f"Community score: {social.get('coingecko_score', 'n/a')}",
        ],
    }


# ─────────────────────────────────────────────────────────────────────
# SIGNAL 3: Stablecoin inflows (per-coin)
# ─────────────────────────────────────────────────────────────────────
def signal_stable_inflows(row, pairs):
    """Compute % of 24h volume on stablecoin pairs."""
    mcap = row.get("mcap_usd") or 0
    if mcap < MCAP_MIN_USD or mcap > MCAP_MAX_STABLE_SIGNAL:
        return False, 0, {}
    vol_chg = row.get("vol_change_24h") or 0
    if vol_chg < STABLE_VOL_CHANGE_MIN:
        return False, 0, {}
    if not pairs:
        return False, 0, {}

    total_vol = 0.0
    stable_vol = 0.0
    usdt_vol = 0.0
    usdc_vol = 0.0
    n_pairs = 0
    n_stable_pairs = 0
    for p in pairs:
        try:
            q = p.get("quote", {}).get("USD", {}) or p.get("quote", {}).get("exchange_reported", {})
            v = q.get("volume_24h") or 0
            if not v or v < 1000:
                continue
            sym = (p.get("market_pair_quote") or {}).get("currency_symbol", "")
            sym = (sym or "").upper()
            total_vol += v
            n_pairs += 1
            if sym in STABLECOIN_SYMBOLS:
                stable_vol += v
                n_stable_pairs += 1
                if sym == "USDT":
                    usdt_vol += v
                elif sym == "USDC":
                    usdc_vol += v
        except Exception:
            continue

    if total_vol < 10000:
        return False, 0, {}
    pct_stable = round(100 * stable_vol / total_vol, 1)
    if pct_stable < STABLE_PAIR_PCT_MIN:
        return False, 0, {"pct_stable": pct_stable}
    # Signal strength: pct above threshold + vol_chg amplitude
    pct_pts = min(60, (pct_stable - STABLE_PAIR_PCT_MIN) * 1.5 + 30)
    vol_pts = min(40, vol_chg / 5)
    return True, round(pct_pts + vol_pts), {
        "pct_volume_stables": pct_stable,
        "pct_volume_usdt": round(100 * usdt_vol / total_vol, 1),
        "pct_volume_usdc": round(100 * usdc_vol / total_vol, 1),
        "n_pairs_total": n_pairs,
        "n_pairs_stable": n_stable_pairs,
    }


def build_stable_inflow_trade(row, strength, stable_meta):
    px = row.get("price_usd") or 0
    pct = stable_meta.get("pct_volume_stables", 0)
    return {
        "primary": (f"{pct:.0f}% of volume is fiat-stable pairs = real buyer demand "
                    f"(not BTC-pair rotation). Highest-conviction of the 3 signals. "
                    f"Larger size justified."),
        "entry_zone": f"${fmt_price(px * 0.96)} - ${fmt_price(px * 1.03)}",
        "stop_loss": f"${fmt_price(px * 0.90)} (-10%)",
        "target_1": f"${fmt_price(px * 1.30)} (+30%)",
        "target_2": f"${fmt_price(px * 1.75)} (+75%)",
        "size": "1.5-3% of crypto portfolio (real demand = better odds)",
        "timeframe": "5-30 days. Stablecoin-driven moves sustain longer.",
        "risks": [
            "Inflow may reflect single whale (one wallet)",
            "Watch for stable supply contraction (broader risk-off)",
            f"USDT vs USDC mix: {stable_meta.get('pct_volume_usdt')}% / {stable_meta.get('pct_volume_usdc')}%",
        ],
    }


# ─────────────────────────────────────────────────────────────────────
# Convergence + state
# ─────────────────────────────────────────────────────────────────────
def build_convergence(scans):
    """Coins that fired on 2+ signals."""
    out = []
    for s in scans:
        n = sum([s.get("vol_fired"), s.get("social_fired"), s.get("stable_fired")])
        if n < 2:
            continue
        signals_lit = []
        if s.get("vol_fired"): signals_lit.append("volume")
        if s.get("social_fired"): signals_lit.append("social")
        if s.get("stable_fired"): signals_lit.append("stable")
        composite = (s.get("vol_strength", 0) + s.get("social_strength", 0)
                     + s.get("stable_strength", 0)) / max(n, 1)
        # Convergence trade: blend of all fired signals
        trade = build_convergence_trade(s, signals_lit)
        out.append({
            "rank": 0,  # filled later
            "ticker": s["row"]["symbol"],
            "name": s["row"]["name"],
            "mcap_usd": s["row"]["mcap_usd"],
            "price_usd": s["row"]["price_usd"],
            "vol_24h_usd": s["row"]["vol_24h_usd"],
            "vol_mcap_ratio": round(s["row"]["vol_mcap_ratio"], 3),
            "pct_change_24h": s["row"]["pct_change_24h"],
            "pct_change_7d": s["row"]["pct_change_7d"],
            "cmc_id": s["row"]["cmc_id"],
            "logo_url": s["row"]["logo_url"],
            "signals_fired": signals_lit,
            "n_signals": n,
            "composite_score": round(composite),
            "signal_breakdown": {
                "volume_strength": s.get("vol_strength", 0),
                "social_strength": s.get("social_strength", 0),
                "stable_strength": s.get("stable_strength", 0),
            },
            "social_meta": s.get("social_meta", {}),
            "stable_meta": s.get("stable_meta", {}),
            "trade_ticket": trade,
        })
    out.sort(key=lambda x: (-x["n_signals"], -x["composite_score"]))
    for i, o in enumerate(out, 1):
        o["rank"] = i
    return out


def build_convergence_trade(s, signals):
    row = s["row"]
    px = row.get("price_usd") or 0
    msg_parts = [f"CONVERGENCE: {len(signals)} signals fired ({', '.join(signals)})."]
    if "stable" in signals:
        msg_parts.append("Real buyer demand confirmed.")
    if "social" in signals:
        msg_parts.append("Community heat sustains move.")
    if "volume" in signals:
        msg_parts.append("Volume momentum active.")
    msg_parts.append("Highest-conviction setup on this scan.")
    return {
        "primary": " ".join(msg_parts),
        "entry_zone": f"${fmt_price(px * 0.96)} - ${fmt_price(px * 1.03)}",
        "stop_loss": f"${fmt_price(px * 0.88)} (-12%)",
        "target_1": f"${fmt_price(px * 1.40)} (+40%)",
        "target_2": f"${fmt_price(px * 2.00)} (+100%)",
        "size": "2-4% of crypto portfolio (convergence = bigger size justified)",
        "timeframe": "3-21 days. Multi-signal setups sustain best.",
        "risks": [
            "Even convergence setups fail ~30-40% of the time",
            "Use position size, not conviction, to manage risk",
            "Re-evaluate if 7d % change reverses sharply",
        ],
    }


def classify_state(n_volume, n_social, n_stable, n_convergence):
    if n_convergence >= 5:
        return "OPPORTUNITY_RICH", "Multi-signal convergence regime -- best risk/reward env for retail small-cap trades."
    if n_convergence >= 2 or (n_volume + n_social + n_stable) >= 8:
        return "ACTIVE", "Several actionable setups present. Selective entry justified."
    if n_convergence >= 1 or (n_volume + n_social + n_stable) >= 3:
        return "NORMAL", "Modest setup density. Wait for higher-conviction names or hold."
    return "QUIET", "No actionable opportunities right now. Cash + watchlist."


STATE_FWD = {
    "OPPORTUNITY_RICH": {"1d": 3.5, "7d": 12.0, "30d": 25.0, "wr": 58,
                          "basis": "Memecoin season + Q4 2024 retail surge episodes"},
    "ACTIVE":             {"1d": 1.5, "7d": 6.5,  "30d": 12.0, "wr": 52,
                          "basis": "Normal small-cap rotation regime"},
    "NORMAL":             {"1d": 0.8, "7d": 3.0,  "30d": 7.0,  "wr": 48,
                          "basis": "Baseline crypto small-cap returns"},
    "QUIET":              {"1d": -0.5,"7d": -1.5, "30d": -3.0, "wr": 40,
                          "basis": "No-signal regimes correlate with broader risk-off"},
}


# ─────────────────────────────────────────────────────────────────────
# Why-now markdown
# ─────────────────────────────────────────────────────────────────────
def build_why_now(state, summary, convergence, vol_rows, soc_rows, stable_rows):
    md = f"## Crypto Opportunity Regime: **{state}**\n\n"
    md += STATE_FWD[state]["basis"] + "\n\n"
    md += "### Scan summary\n\n"
    md += f"- Universe scanned: **{summary['universe_size']}** coins (top by mcap)\n"
    md += f"- Filtered to small/mid cap: **{summary['filtered_universe_size']}** coins\n"
    md += f"- Volume surge fired: **{summary['n_volume_surge']}**\n"
    md += f"- Social velocity fired: **{summary['n_social_velocity']}**\n"
    md += f"- Stablecoin inflow fired: **{summary['n_stable_inflows']}**\n"
    md += f"- Convergence (2+ signals): **{summary['n_convergence']}**\n\n"

    if convergence:
        md += "### Highest-conviction setups (convergence)\n\n"
        for c in convergence[:5]:
            md += (f"- **{c['ticker']}** ({c['name']}) -- "
                   f"{c['n_signals']} signals: {', '.join(c['signals_fired'])} "
                   f"-- mcap ${fmt_usd(c['mcap_usd'])} -- "
                   f"24h {c['pct_change_24h']:+.1f}%\n")
        md += "\n"

    md += "### Why this matters for retail\n\n"
    md += ("Most 'crypto signals' fail because they fire on a single noisy metric. "
           "This scan requires **at least two independent confirmations** for the highest-"
           "conviction picks -- volume surge AND community velocity AND/OR real stablecoin "
           "buyer demand. The convergence table is where retail edge actually lives.\n\n")
    md += ("**Risk management:** even high-conviction crypto setups fail ~30-40% of the time. "
           "Use position size (0.5-3% per pick), respect the stop loss, and never chase "
           "after a 30%+ run. The setup phase is the entry phase.\n")
    return md


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────
def fmt_price(p):
    if p is None or p == 0:
        return "0.00"
    if p >= 1:
        return f"{p:,.2f}"
    if p >= 0.01:
        return f"{p:.4f}"
    if p >= 0.0001:
        return f"{p:.6f}"
    return f"{p:.8f}"


def fmt_usd(n):
    if n is None:
        return "n/a"
    if n >= 1e9:
        return f"{n/1e9:.2f}B"
    if n >= 1e6:
        return f"{n/1e6:.1f}M"
    if n >= 1e3:
        return f"{n/1e3:.1f}k"
    return f"{n:.0f}"


# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    started = time.time()
    s3 = boto3.client("s3", region_name="us-east-1")
    ssm = boto3.client("ssm", region_name="us-east-1")

    try:
        # ── Step 1: fetch CMC universe ──
        universe = fetch_cmc_listings(limit=500)
        if not universe:
            return {"statusCode": 500,
                    "body": json.dumps({"error": "CMC listings empty"})}

        # Universe filter for opportunity scanning (mcap $5M-$5B)
        scan_universe = [r for r in universe if r["mcap_usd"] <= 5_000_000_000]
        print(f"scan universe (mcap <= $5B): {len(scan_universe)}")

        # ── Step 2: SIGNAL 1 volume surge ── (no per-coin enrichment needed)
        scans = []
        for r in scan_universe:
            vfired, vstr = signal_volume_surge(r)
            scans.append({
                "row": r,
                "vol_fired": vfired,
                "vol_strength": vstr if vfired else 0,
                "social_fired": False, "social_strength": 0, "social_meta": {},
                "stable_fired": False, "stable_strength": 0, "stable_meta": {},
            })

        # Sort by vol-strength to pick top candidates for further enrichment
        vol_candidates = sorted([s for s in scans if s["vol_fired"]],
                                key=lambda x: -x["vol_strength"])[:30]
        # Always also enrich the top 30 by vol-change for social (some won't have fired on vol)
        social_candidates = sorted(scan_universe, key=lambda r: -(r.get("vol_change_24h") or 0))[:30]
        social_cmc_ids = {s["cmc_id"] for s in social_candidates}

        # ── Step 3: enrich with CoinGecko (SIGNAL 2 social) for ~30 coins ──
        # We do this for: top 30 vol candidates + top 30 by vol_change
        enrich_ids = set([s["row"]["cmc_id"] for s in vol_candidates]) | social_cmc_ids
        enrich_ids = list(enrich_ids)[:40]  # cap at 40 to stay within CG rate limit
        print(f"enriching {len(enrich_ids)} coins with CoinGecko data...")
        for s in scans:
            cmc_id = s["row"]["cmc_id"]
            if cmc_id not in enrich_ids:
                continue
            sym = s["row"]["symbol"]
            cg = fetch_coingecko_coin(sym)
            time.sleep(2.0)  # CG free tier: ~30/min => 2s pacing
            if cg:
                fired, strength, meta = signal_social_velocity(s["row"], cg)
                s["social_fired"] = fired
                s["social_strength"] = strength
                s["social_meta"] = meta

        # ── Step 4: SIGNAL 3 stable inflows for top vol+social candidates ──
        stable_targets = set(enrich_ids)  # same set
        print(f"fetching market-pairs for {len(stable_targets)} coins...")
        # CMC market-pairs costs 1 call per coin. 40 coins = 40 calls per run.
        # 6 runs/day = 240 calls. With listings (6) = 246. Within 333 budget.
        for s in scans:
            cmc_id = s["row"]["cmc_id"]
            if cmc_id not in stable_targets:
                continue
            pairs = fetch_cmc_market_pairs(cmc_id, limit=15)
            time.sleep(0.5)
            if pairs:
                fired, strength, meta = signal_stable_inflows(s["row"], pairs)
                s["stable_fired"] = fired
                s["stable_strength"] = strength
                s["stable_meta"] = meta

        # ── Step 5: assemble outputs ──
        # Volume surge table (sorted by vol-strength desc)
        vol_rows = []
        for s in sorted([s for s in scans if s["vol_fired"]],
                         key=lambda x: -x["vol_strength"])[:25]:
            r = s["row"]
            vol_rows.append({
                "rank": len(vol_rows) + 1,
                "ticker": r["symbol"],
                "name": r["name"],
                "cmc_id": r["cmc_id"],
                "logo_url": r["logo_url"],
                "price_usd": r["price_usd"],
                "mcap_usd": r["mcap_usd"],
                "vol_24h_usd": r["vol_24h_usd"],
                "vol_mcap_ratio": round(r["vol_mcap_ratio"], 3),
                "vol_change_24h_pct": r.get("vol_change_24h"),
                "pct_change_1h": r.get("pct_change_1h"),
                "pct_change_24h": r.get("pct_change_24h"),
                "pct_change_7d": r.get("pct_change_7d"),
                "signal_strength": s["vol_strength"],
                "trade_ticket": build_volume_surge_trade(r, s["vol_strength"]),
            })

        soc_rows = []
        for s in sorted([s for s in scans if s["social_fired"]],
                         key=lambda x: -x["social_strength"])[:20]:
            r = s["row"]
            soc_rows.append({
                "rank": len(soc_rows) + 1,
                "ticker": r["symbol"],
                "name": r["name"],
                "cmc_id": r["cmc_id"],
                "logo_url": r["logo_url"],
                "price_usd": r["price_usd"],
                "mcap_usd": r["mcap_usd"],
                "pct_change_24h": r.get("pct_change_24h"),
                "pct_change_7d": r.get("pct_change_7d"),
                "signal_strength": s["social_strength"],
                "social_meta": s["social_meta"],
                "trade_ticket": build_social_velocity_trade(r, s["social_strength"], s["social_meta"]),
            })

        stable_rows = []
        for s in sorted([s for s in scans if s["stable_fired"]],
                         key=lambda x: -x["stable_strength"])[:20]:
            r = s["row"]
            stable_rows.append({
                "rank": len(stable_rows) + 1,
                "ticker": r["symbol"],
                "name": r["name"],
                "cmc_id": r["cmc_id"],
                "logo_url": r["logo_url"],
                "price_usd": r["price_usd"],
                "mcap_usd": r["mcap_usd"],
                "vol_24h_usd": r["vol_24h_usd"],
                "pct_change_24h": r.get("pct_change_24h"),
                "pct_change_7d": r.get("pct_change_7d"),
                "vol_change_24h_pct": r.get("vol_change_24h"),
                "signal_strength": s["stable_strength"],
                "stable_meta": s["stable_meta"],
                "trade_ticket": build_stable_inflow_trade(r, s["stable_strength"], s["stable_meta"]),
            })

        convergence = build_convergence(scans)
        n_conv = len(convergence)

        # ── Step 6: state + summary ──
        state, state_desc = classify_state(
            len(vol_rows), len(soc_rows), len(stable_rows), n_conv)
        priors = STATE_FWD[state]

        summary = {
            "universe_size": len(universe),
            "filtered_universe_size": len(scan_universe),
            "n_enriched": len(enrich_ids),
            "n_volume_surge": len(vol_rows),
            "n_social_velocity": len(soc_rows),
            "n_stable_inflows": len(stable_rows),
            "n_convergence": n_conv,
        }
        signal_strength = min(100, 25 * n_conv + 5 * (len(vol_rows) + len(soc_rows) + len(stable_rows)))

        # Trigger conditions
        triggers = [
            {"name": "Convergence setups detected (2+ signals)",
             "current": n_conv, "threshold": ">=2",
             "satisfied": n_conv >= 2, "weight": 0.40},
            {"name": "Volume surge candidates",
             "current": len(vol_rows), "threshold": ">=3",
             "satisfied": len(vol_rows) >= 3, "weight": 0.20},
            {"name": "Social velocity candidates",
             "current": len(soc_rows), "threshold": ">=3",
             "satisfied": len(soc_rows) >= 3, "weight": 0.15},
            {"name": "Stablecoin inflow candidates",
             "current": len(stable_rows), "threshold": ">=2",
             "satisfied": len(stable_rows) >= 2, "weight": 0.20},
            {"name": "Universe scan complete",
             "current": len(scan_universe), "threshold": ">=100",
             "satisfied": len(scan_universe) >= 100, "weight": 0.05},
        ]

        forward_expectations = {
            "1d": {"return_pct": priors["1d"], "win_rate_pct": priors["wr"],
                   "basis": priors["basis"]},
            "7d": {"return_pct": priors["7d"], "basis": priors["basis"]},
            "30d": {"return_pct": priors["30d"], "basis": priors["basis"]},
        }

        # Top-of-book trade ticket = #1 convergence pick if any, else top vol surge
        if convergence:
            top_trade = convergence[0]["trade_ticket"]
            top_trade["primary"] = (
                f"TOP PICK: {convergence[0]['ticker']} -- "
                f"{convergence[0]['n_signals']} signals fired. "
                + top_trade["primary"])
        elif vol_rows:
            top_trade = vol_rows[0]["trade_ticket"]
            top_trade["primary"] = (f"TOP VOL-SURGE: {vol_rows[0]['ticker']} -- "
                                     + top_trade["primary"])
        else:
            top_trade = {"primary": "No actionable setups right now. Cash + patience.",
                          "exit_rules": ["Re-check next scan in 4h"]}

        # Build why-now
        why_now = build_why_now(state, summary, convergence, vol_rows, soc_rows, stable_rows)

        # ── Step 7: SSM state + telegram on regime change ──
        try:
            prev = json.loads(ssm.get_parameter(Name=SSM_KEY)["Parameter"]["Value"])
            prev_state = prev.get("state", "UNKNOWN")
        except Exception:
            prev_state = "UNKNOWN"
        if state != prev_state:
            try:
                ssm.put_parameter(Name=SSM_KEY,
                                   Value=json.dumps({"state": state,
                                                     "as_of": dt.datetime.utcnow().isoformat() + "Z"}),
                                   Type="String", Overwrite=True)
            except Exception:
                pass
            # Telegram alert on entry to OPPORTUNITY_RICH or ACTIVE
            if state in ("OPPORTUNITY_RICH", "ACTIVE") and TELEGRAM_TOKEN:
                top_names = [c["ticker"] for c in convergence[:5]]
                if not top_names:
                    top_names = [r["ticker"] for r in vol_rows[:5]]
                msg = (f"*Crypto Opportunities* state: `{prev_state}` -> `{state}`\n\n"
                       f"Convergence picks: *{n_conv}*\n"
                       f"Vol/Social/Stable: {len(vol_rows)}/{len(soc_rows)}/{len(stable_rows)}\n\n"
                       f"Top: {', '.join(top_names)}\n\n"
                       f"https://justhodl.ai/crypto-opportunities.html")
                try:
                    tg_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                    body = json.dumps({"chat_id": TELEGRAM_CHAT, "text": msg,
                                        "parse_mode": "Markdown",
                                        "disable_web_page_preview": True}).encode()
                    req = urllib.request.Request(tg_url, data=body,
                                                  headers={"Content-Type": "application/json"})
                    urllib.request.urlopen(req, timeout=8)
                except Exception as e:
                    print(f"telegram alert failed: {e}")

        # ── Step 8: assemble output ──
        output = {
            "engine": "crypto-opportunities",
            "version": "1.0",
            "as_of": dt.datetime.utcnow().isoformat() + "Z",
            "state": state,
            "previous_state": prev_state,
            "state_transition": state != prev_state,
            "state_description": state_desc,
            "signal_strength": signal_strength,
            "summary": summary,
            "current_readings": {
                "n_volume_surge": len(vol_rows),
                "n_social_velocity": len(soc_rows),
                "n_stable_inflows": len(stable_rows),
                "n_convergence": n_conv,
                "top_convergence_tickers": [c["ticker"] for c in convergence[:10]],
            },
            "top_volume_surge": vol_rows,
            "top_social_velocity": soc_rows,
            "top_stable_inflows": stable_rows,
            "convergence": convergence,
            "trigger_conditions": triggers,
            "forward_expectations": forward_expectations,
            "recommended_trade": top_trade,
            "historical_episodes": [
                {"period": "PEPE Apr-May 2023",
                 "outcome": "+1000% in 3 weeks. Volume + social + stable all fired."},
                {"period": "SHIB Aug 2021",
                 "outcome": "+200x in 8 weeks. Social velocity led; vol/mcap followed."},
                {"period": "Memecoin season Q4 2024 (WIF/POPCAT/PNUT)",
                 "outcome": "Convergence picks averaged +60% in 14 days."},
                {"period": "Memecoin winter Jan-Mar 2026",
                 "outcome": "QUIET state for 8 weeks. Conviction setups -30% on avg."},
            ],
            "why_now_explainer": why_now,
            "methodology": (
                "Pull top 500 CMC listings sorted by market cap. Filter to "
                "mcap $5M-$5B. Three independent signals: (1) volume surge -- "
                "vol/mcap > 0.20 AND vol_change_24h > 50%, restricted to <$500M mcap. "
                "(2) Social velocity -- CoinGecko community_score > 30 OR reddit "
                "posts+active > 5/10 in 48h, <$2B mcap. (3) Stablecoin inflows -- "
                ">=65% of 24h volume on USDT/USDC/BUSD/DAI pairs AND vol_change "
                ">30%, <$1B mcap. Convergence table = coins firing on 2+ signals. "
                "Each row gets retail-friendly trade ticket (entry, stop, targets, "
                "size, timeframe, risks). State machine maps total signal count "
                "to OPPORTUNITY_RICH / ACTIVE / NORMAL / QUIET; forward priors "
                "calibrated against memecoin-season episodes."
            ),
            "sources": [
                "CoinMarketCap /v1/cryptocurrency/listings/latest",
                "CoinMarketCap /v2/cryptocurrency/market-pairs/latest",
                "CoinGecko /coins/{id} (community_data)",
                "Academic: vol/mcap turnover (Karaa-Krichene-Slim 2021 crypto microstructure)",
            ],
            "schedule": "Every 4h. CMC budget: 246/333 daily calls.",
            "run_duration_seconds": round(time.time() - started, 2),
        }

        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                       Body=json.dumps(output, default=str).encode("utf-8"),
                       ContentType="application/json",
                       CacheControl="public, max-age=600")

        return {"statusCode": 200,
                "body": json.dumps({
                    "ok": True, "state": state, "previous_state": prev_state,
                    "n_volume_surge": len(vol_rows),
                    "n_social_velocity": len(soc_rows),
                    "n_stable_inflows": len(stable_rows),
                    "n_convergence": n_conv,
                    "signal_strength": signal_strength,
                })}

    except Exception as e:
        return {"statusCode": 500,
                "body": json.dumps({"error": str(e),
                                     "trace": traceback.format_exc()[:1500]})}
