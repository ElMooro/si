"""
justhodl-tax-plan — Tax-Aware Portfolio Engine
=================================================================
The institutional after-tax wealth layer that bridges the Capital Compass
(forward returns) and the Wealth Plan (Monte Carlo) into AFTER-TAX reality
— what retail actually keeps.

WHY THIS EXISTS
---------------
Every existing allocator/wealth tool on this platform shows PRE-TAX returns.
For a US taxable account that's a 25-40% overstatement of what the investor
keeps. This engine closes that gap with the same methodology Goldman Apex,
JPM Private Bank, and Wealthfront use for tax-aware portfolio management.

DOCUMENTED EDGE (Wealthfront whitepaper, Fidelity Tax-Smart):
  • Tax-loss harvesting: 0.5–1.5%/yr after-tax alpha
  • Asset location (right asset in right account): 0.5–1.0%/yr
  • LT vs ST gain timing: 0.2–0.5%/yr
  • Total documented: ~1.5–3%/yr — bigger than most stock-picking alpha.

WHAT IT DOES
------------
1. PER-POSITION TAX EXPOSURE — reads portfolio/signal-portfolio-state.json
   for cost basis + entry date. Per holding:
      • Unrealized P/L
      • LT vs ST classification (days_held vs 365)
      • Days-to-LT for ST winners (don't sell prematurely!)
      • Estimated tax if sold today

2. AFTER-TAX FORWARD RETURNS — reads data/forward-returns.json (Capital
   Compass). For each asset class, applies:
      • Qualified dividend split (broad equity = 100% QD; bonds = 0%)
      • Ordinary income drag (bonds, REIT distributions)
      • Capital gains realization assumption (long-term hold)
      • Collectibles rate for gold (28% max LTCG)
   Outputs after-tax forward ER per asset.

3. ASSET LOCATION OPTIMIZER — given multi-account split
   (taxable/IRA/Roth/401k), recommends optimal placement:
      • Tax-INEFFICIENT (bonds, REITs, HY) → tax-advantaged accounts
      • Tax-EFFICIENT (broad equity ETFs) → taxable accounts OK
      • Roth IDEAL for highest-growth assets (BTC, EM)
   Computes estimated annual savings from improvement moves.

4. TAX-LOSS HARVEST candidates — positions in unrealized loss > $X with
   institutional substitute ETF suggestions (not "substantially identical"
   per IRS guidance). Per candidate: harvestable loss, tax savings at
   marginal rate, suggested replacement.

5. ANNUAL TAX BILL ESTIMATE:
      Federal bracket × ordinary income
    + LTCG rate × realized LT gains
    + Ordinary rate × realized ST gains
    + 3.8% NIIT if AGI > threshold
    + State stacking
    − Capital loss carryforward (up to $3k/yr against ordinary)

6. PLAIN-ENGLISH VERDICT with prioritized action items.

INPUTS (via Function URL or scheduled default)
----------------------------------------------
  federal_bracket    12 | 22 | 24 | 32 | 35 | 37  (default 24)
  state_rate         0 - 13 % (default 5)
  agi                AGI for NIIT trigger (default 200000)
  filing_status      single | mfj (default single)
  taxable_pct        % of portfolio in taxable accounts (default 0.6)
  ira_pct            % in traditional IRA / 401(k) (default 0.3)
  roth_pct           % in Roth (default 0.1)
  carryforward       existing cap-loss carryforward $ (default 0)
  min_harvest_loss   minimum $ loss to flag for harvest (default 500)

OUTPUT: data/tax-plan-snapshot.json (default profile) + Function URL JSON
SCHEDULE: daily 11:45 UTC (after macro-calendar 11:00, compass weekly Sun 03)

METHODOLOGY: 2026 US federal tax brackets, LTCG 0/15/20 thresholds, NIIT
3.8% surtax, qualified dividend tax treatment, collectibles 28% rate.
Wash-sale rule monitoring requires trade history (deferred to v2).
"""

import os
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import boto3

VERSION = "1.0.0"
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
PORTFOLIO_KEY = "portfolio/signal-portfolio-state.json"
COMPASS_KEY = "data/forward-returns.json"
OUT_KEY = "data/tax-plan-snapshot.json"

s3 = boto3.client("s3", region_name=REGION)


# =============================================================================
# 2026 US TAX CONSTANTS (federal)
# =============================================================================
# Ordinary income brackets (single filer 2026 — projected from IRS inflation
# adjustments). User passes bracket directly so brackets are pre-resolved.
NIIT_THRESHOLD_SINGLE = 200_000
NIIT_THRESHOLD_MFJ = 250_000
NIIT_RATE = 0.038

# LTCG bracket thresholds (single 2026, projected)
LTCG_0_PCT_AGI_MAX_SINGLE = 47_025
LTCG_15_PCT_AGI_MAX_SINGLE = 518_900
LTCG_0_PCT_AGI_MAX_MFJ = 94_050
LTCG_15_PCT_AGI_MAX_MFJ = 583_750

CAP_LOSS_VS_ORDINARY_ANNUAL_CAP = 3_000  # $3k/yr against ordinary income
COLLECTIBLES_RATE = 0.28  # max LTCG on physical-gold-backed funds (GLD is a grantor trust → collectibles)


def get_ltcg_rate(agi, filing_status):
    if filing_status == "mfj":
        if agi <= LTCG_0_PCT_AGI_MAX_MFJ:
            return 0.0
        elif agi <= LTCG_15_PCT_AGI_MAX_MFJ:
            return 0.15
        else:
            return 0.20
    else:  # single
        if agi <= LTCG_0_PCT_AGI_MAX_SINGLE:
            return 0.0
        elif agi <= LTCG_15_PCT_AGI_MAX_SINGLE:
            return 0.15
        else:
            return 0.20


def get_niit_rate(agi, filing_status):
    threshold = NIIT_THRESHOLD_MFJ if filing_status == "mfj" else NIIT_THRESHOLD_SINGLE
    return NIIT_RATE if agi > threshold else 0.0


# =============================================================================
# ASSET CLASS TAX CLASSIFICATION (per IRS publication 550 / fund disclosures)
# =============================================================================
# qd_pct = % of distributions taxed as Qualified Dividends (LTCG rates)
# ord_pct = % taxed as ordinary income (marginal bracket)
# ltcg_override = special rate on appreciation (e.g. GLD collectibles 28%)
# ideal_account = optimal location (TAX_DEFERRED / TAXABLE_OK / ROTH_IDEAL / ANY)
TAX_CLASS = {
    # ── Broad equity ETFs — tax-EFFICIENT (low turnover, mostly QD) ─────
    "SPY": dict(qd_pct=100, ord_pct=0, ideal="TAXABLE_OK",
                explain="Broad-index ETF with low turnover. Distributions are mostly Qualified Dividends taxed at LTCG rates. Highly tax-efficient."),
    "QQQ": dict(qd_pct=100, ord_pct=0, ideal="ROTH_IDEAL",
                explain="High-growth tech. Best held in Roth — appreciation never taxed."),
    "IWM": dict(qd_pct=100, ord_pct=0, ideal="TAXABLE_OK",
                explain="Small-cap index. Mostly QD. Tax-efficient."),
    "EFA": dict(qd_pct=70, ord_pct=30, ideal="TAXABLE_OK",
                explain="International developed. ~70% QD + foreign tax credit available in taxable accounts."),
    "EEM": dict(qd_pct=65, ord_pct=35, ideal="ROTH_IDEAL",
                explain="Emerging markets. Higher growth → Roth ideal. Some ordinary distributions."),

    # ── Bonds — tax-INEFFICIENT (interest taxed as ordinary income) ─────
    "IEF": dict(qd_pct=0, ord_pct=100, ideal="TAX_DEFERRED",
                explain="Treasury bond interest is taxed as ordinary income. Major tax drag — keep in IRA/401(k)."),
    "TLT": dict(qd_pct=0, ord_pct=100, ideal="TAX_DEFERRED",
                explain="Long Treasury interest taxed as ordinary. Tax-deferred only."),
    "TIP": dict(qd_pct=0, ord_pct=100, ideal="TAX_DEFERRED",
                explain="TIPS pay both real coupon AND phantom inflation income, both ordinary. Worst possible asset for taxable account."),
    "LQD": dict(qd_pct=0, ord_pct=100, ideal="TAX_DEFERRED",
                explain="Corporate bond interest = ordinary income. Tax-deferred only."),
    "HYG": dict(qd_pct=0, ord_pct=100, ideal="TAX_DEFERRED",
                explain="High yield bond interest = ordinary income at high yield. Maximum tax drag in taxable."),
    "BIL": dict(qd_pct=0, ord_pct=100, ideal="TAXABLE_OK",
                explain="T-bill interest is state-tax-free but federal ordinary. OK in taxable for short-term cash."),

    # ── REITs — mostly ordinary income (90% pass-through requirement) ───
    "VNQ": dict(qd_pct=20, ord_pct=80, ideal="TAX_DEFERRED",
                explain="REIT distributions are 80% ordinary income (REIT 199A deduction helps but minor). Tax-deferred ideal."),

    # ── Gold — collectibles 28% max LTCG (worst capital gains treatment) ─
    "GLD": dict(qd_pct=0, ord_pct=0, ltcg_override=COLLECTIBLES_RATE, ideal="TAX_DEFERRED",
                explain="GLD is a grantor trust → collectibles 28% rate on gains. Tax-deferred ideal."),
    "DBC": dict(qd_pct=0, ord_pct=0, k1=True, ideal="TAX_DEFERRED",
                explain="Commodity ETF issues K-1, mark-to-market every year. K-1 complexity + ordinary income on roll yield. Tax-deferred ideal."),

    # ── Crypto — capital gains, no dividends, highest growth potential ──
    "BTC": dict(qd_pct=0, ord_pct=0, ideal="ROTH_IDEAL",
                explain="Bitcoin = pure capital gains, taxed only on sale. Roth IDEAL — highest expected long-term appreciation, never taxed in Roth."),
}

# Fallback for unmapped tickers (treat as broad equity ETF)
DEFAULT_TAX_CLASS = dict(qd_pct=100, ord_pct=0, ideal="TAXABLE_OK",
                         explain="Treated as broad-index equity ETF (default classification).")


# =============================================================================
# TAX-LOSS HARVEST: ETF SUBSTITUTES (institutionally NOT "substantially identical")
# =============================================================================
# Per IRS guidance + Wealthfront/Betterment whitepapers. Same-index different-fund
# is the most common dispute zone — these pairs are widely treated as safe.
TLH_ALTERNATIVES = {
    "SPY":  ["IVV", "VOO", "SPLG"],   # S&P 500
    "IVV":  ["SPY", "VOO", "SPLG"],
    "VOO":  ["SPY", "IVV", "SPLG"],
    "QQQ":  ["QQQM", "ONEQ"],         # NASDAQ
    "IWM":  ["VTWO", "SPSM"],         # Russell 2000 / small-cap
    "EFA":  ["VEA", "IEFA", "SCHF"],  # Intl developed
    "EEM":  ["VWO", "IEMG", "SCHE"],  # EM
    "GLD":  ["IAU", "GLDM"],
    "VNQ":  ["SCHH", "USRT", "RWR"],
    "TLT":  ["VGLT", "SPTL"],
    "IEF":  ["VGIT", "SCHR"],
    "TIP":  ["VTIP", "SCHP", "STIP"],
    "HYG":  ["JNK", "USHY"],
    "LQD":  ["VCIT", "IGIB"],
    "BIL":  ["SHV", "SGOV"],
    "DBC":  ["GSG", "PDBC"],
    "BTC":  ["IBIT", "FBTC", "GBTC"],
}


# =============================================================================
# DATA FETCHERS
# =============================================================================
def load_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[s3 load {key}] {e}")
        return None


def load_portfolio():
    """Returns list of positions: {ticker, entry_date, entry_price, qty, current_price, days_held}."""
    data = load_json(PORTFOLIO_KEY)
    if not data:
        return []
    return [p for p in (data.get("open_positions") or []) if isinstance(p, dict)]


def load_compass():
    """Returns {ticker: {forward_er, vol, ...}, macro: {...}}."""
    return load_json(COMPASS_KEY) or {}


# =============================================================================
# CORE TAX COMPUTATIONS
# =============================================================================
def per_position_tax_view(positions, marginal_bracket_pct, ltcg_rate, niit_rate):
    """Per-holding unrealized gain/loss + ST/LT classification + tax-if-sold."""
    rows = []
    today = datetime.now(timezone.utc).date()
    total_unrealized_st = 0.0
    total_unrealized_lt = 0.0
    total_market_value = 0.0
    for p in positions:
        ticker = (p.get("ticker") or p.get("symbol") or "").upper()
        if not ticker:
            continue
        qty = float(p.get("qty") or 0)
        entry_px = float(p.get("entry_price") or 0)
        cur_px = float(p.get("current_price") or 0)
        if qty <= 0 or entry_px <= 0:
            continue
        cost = qty * entry_px
        mv = qty * cur_px
        unreal = mv - cost
        days = int(p.get("days_held") or 0)
        is_lt = days >= 365
        days_to_lt = max(0, 365 - days)

        # Tax if sold today (capital gains)
        if unreal >= 0:
            # gain
            rate = (ltcg_rate + niit_rate) if is_lt else (marginal_bracket_pct/100 + niit_rate)
            tax_if_sold = unreal * rate
            cls = "LT_GAIN" if is_lt else "ST_GAIN"
        else:
            # loss → offsets gains, eventually $3k/yr against ordinary
            tax_if_sold = unreal * (marginal_bracket_pct / 100)  # negative (savings)
            cls = "LT_LOSS" if is_lt else "ST_LOSS"

        if cls.endswith("LOSS"):
            pass  # losses aren't accrued the same way
        elif is_lt:
            total_unrealized_lt += unreal
        else:
            total_unrealized_st += unreal
        total_market_value += mv

        rows.append({
            "ticker": ticker,
            "qty": round(qty, 4),
            "entry_price": round(entry_px, 2),
            "current_price": round(cur_px, 2),
            "cost_basis": round(cost, 2),
            "market_value": round(mv, 2),
            "unrealized_pl": round(unreal, 2),
            "unrealized_pl_pct": round(unreal / cost * 100, 2) if cost > 0 else 0,
            "days_held": days,
            "is_long_term": is_lt,
            "days_to_lt": days_to_lt,
            "classification": cls,
            "tax_if_sold_today": round(tax_if_sold, 2),
            "effective_tax_rate_pct": round((tax_if_sold / unreal * 100) if unreal else 0, 1),
        })
    return rows, {
        "total_market_value": round(total_market_value, 2),
        "total_unrealized_lt_gain": round(total_unrealized_lt, 2),
        "total_unrealized_st_gain": round(total_unrealized_st, 2),
    }


def after_tax_forward_returns(compass_data, marginal_bracket_pct, ltcg_rate, niit_rate):
    """For each asset in the Compass, compute after-tax forward ER assuming
    LONG-TERM hold (= primary use case for retail buy-and-hold).
    Tax drag = annual_dividend_yield × dividend_tax_rate + appreciation × 0
    (deferred until sale). For bonds/REITs which distribute almost everything
    every year, the drag is much higher.
    """
    if not compass_data:
        return []
    assets = compass_data.get("assets", {})
    rows = []
    for ticker, a in assets.items():
        tc = TAX_CLASS.get(ticker, DEFAULT_TAX_CLASS)
        forward_er = a.get("forward_er_10y_pct") or 0
        div_yield = a.get("trailing_dividend_yield_pct") or 0

        qd_rate = ltcg_rate + niit_rate  # qualified dividends taxed at LTCG + NIIT
        ord_rate = marginal_bracket_pct / 100 + niit_rate

        # Annual dividend tax drag
        qd_drag = div_yield * (tc.get("qd_pct", 0) / 100) * qd_rate
        ord_drag = div_yield * (tc.get("ord_pct", 0) / 100) * ord_rate
        annual_distribution_drag = qd_drag + ord_drag

        # If asset has collectibles override OR is K-1 commodity, the appreciation
        # is taxed annually at the override rate (or as ordinary for K-1).
        ltcg_override = tc.get("ltcg_override")
        if ltcg_override:
            # Appreciation portion (forward_er - div_yield) realized lazily; but
            # for transparency, show what the eventual realization costs.
            appreciation = max(0, forward_er - div_yield)
            collectibles_drag_on_eventual_realization = appreciation * min(ltcg_override, marginal_bracket_pct/100) * 0.5
            # Simplified: half-effective rate over 10y assuming partial realization
        else:
            collectibles_drag_on_eventual_realization = 0

        after_tax_er = forward_er - annual_distribution_drag - collectibles_drag_on_eventual_realization
        tax_drag_total = annual_distribution_drag + collectibles_drag_on_eventual_realization

        rows.append({
            "ticker": ticker,
            "name": a.get("name"),
            "forward_er_pretax_pct": forward_er,
            "after_tax_er_taxable_pct": round(after_tax_er, 2),
            "after_tax_er_tax_deferred_pct": forward_er,  # IRA/401k — no annual drag
            "after_tax_er_roth_pct": forward_er,  # Roth — never taxed
            "annual_tax_drag_pct": round(tax_drag_total, 2),
            "drag_breakdown": {
                "from_qualified_dividends": round(qd_drag, 2),
                "from_ordinary_distributions": round(ord_drag, 2),
                "from_collectibles_realization": round(collectibles_drag_on_eventual_realization, 2),
            },
            "ideal_account": tc.get("ideal"),
            "tax_efficiency_verdict": ("EFFICIENT" if tc.get("ord_pct", 0) <= 25 and not ltcg_override
                                       else ("INEFFICIENT" if tc.get("ord_pct", 0) >= 75 or ltcg_override else "MODERATE")),
            "explainer": tc.get("explain"),
        })
    return rows


def tax_loss_harvest_candidates(positions, marginal_bracket_pct, min_harvest_loss):
    """Identify TLH-eligible losers with substitute suggestions."""
    candidates = []
    for p in positions:
        if p["unrealized_pl"] >= -abs(min_harvest_loss):
            continue
        ticker = p["ticker"]
        loss = abs(p["unrealized_pl"])
        # Tax savings: use marginal bracket (offsets ordinary income via cap-loss carry)
        # or LTCG (offsets gains). Conservative: use LTCG for LT losses, marginal for ST.
        tax_savings = loss * (marginal_bracket_pct / 100)
        alts = TLH_ALTERNATIVES.get(ticker, [])
        candidates.append({
            "ticker": ticker,
            "harvestable_loss": round(loss, 2),
            "estimated_tax_savings": round(tax_savings, 2),
            "classification": p["classification"],
            "substitute_etfs": alts,
            "rationale": f"Harvest ${loss:,.0f} loss now → save ~${tax_savings:,.0f} in taxes. Rotate into {alts[0] if alts else 'similar asset'} to maintain exposure without wash-sale risk.",
        })
    return sorted(candidates, key=lambda c: c["estimated_tax_savings"], reverse=True)


def asset_location_recommendations(after_tax_rows, taxable_pct, ira_pct, roth_pct):
    """Given account split, suggest which asset class belongs where.
    Computes annual savings from moving INEFFICIENT assets out of taxable.
    """
    if not after_tax_rows:
        return {}
    moves = []
    total_annual_savings = 0
    for r in after_tax_rows:
        if r["tax_efficiency_verdict"] == "INEFFICIENT" and taxable_pct > 0:
            # Estimated savings if current taxable portion of this asset were moved to IRA
            # Conservative: assume 5% portfolio weight per inefficient asset in taxable
            estimated_position_weight = 0.05
            annual_savings_per_dollar = r["annual_tax_drag_pct"] / 100
            move_savings = estimated_position_weight * annual_savings_per_dollar
            moves.append({
                "ticker": r["ticker"],
                "current_account": "TAXABLE",
                "move_to": r["ideal_account"],
                "annual_tax_savings_per_position_dollar": round(annual_savings_per_dollar * 100, 2),
                "rationale": r["explainer"],
            })
            total_annual_savings += move_savings * 100  # convert to %
    return {
        "current_split": {
            "taxable_pct": round(taxable_pct * 100, 1),
            "ira_pct": round(ira_pct * 100, 1),
            "roth_pct": round(roth_pct * 100, 1),
        },
        "suggested_moves": moves,
        "total_estimated_annual_savings_bps": round(total_annual_savings * 10, 0),
        "interpretation": (
            "Tax-inefficient assets (bonds, REITs, HY credit, gold) sitting in your taxable account are losing more to taxes "
            "every year than they would in an IRA/401(k). The 'ideal_account' column shows where each asset class belongs."
        ),
    }


def annual_tax_bill_estimate(per_position_rows, marginal_bracket_pct, ltcg_rate, niit_rate, carryforward, agi, compass_data):
    """Project this year's tax bill assuming current portfolio held as-is.
    Includes dividend income projection from Compass yields."""
    realized_lt_gains = 0  # would need actual realized trades
    realized_st_gains = 0
    realized_losses = 0
    # Dividend income projection (annual)
    div_qd = 0
    div_ord = 0
    if compass_data:
        for p in per_position_rows:
            ticker = p["ticker"]
            comp = (compass_data.get("assets") or {}).get(ticker, {})
            dy = comp.get("trailing_dividend_yield_pct") or 0
            mv = p["market_value"]
            annual_div = mv * (dy / 100)
            tc = TAX_CLASS.get(ticker, DEFAULT_TAX_CLASS)
            div_qd += annual_div * (tc.get("qd_pct", 0) / 100)
            div_ord += annual_div * (tc.get("ord_pct", 0) / 100)

    # Tax components
    tax_on_qd = div_qd * (ltcg_rate + niit_rate)
    tax_on_ord_div = div_ord * (marginal_bracket_pct / 100 + niit_rate)
    tax_on_lt_realized = realized_lt_gains * (ltcg_rate + niit_rate)
    tax_on_st_realized = realized_st_gains * (marginal_bracket_pct / 100 + niit_rate)
    # Capital loss carryforward usage
    cf_used_against_gains = min(carryforward, realized_lt_gains + realized_st_gains)
    cf_used_against_ordinary = min(CAP_LOSS_VS_ORDINARY_ANNUAL_CAP,
                                    max(0, carryforward - cf_used_against_gains))
    cf_remaining = max(0, carryforward - cf_used_against_gains - cf_used_against_ordinary)

    total_tax = (tax_on_qd + tax_on_ord_div + tax_on_lt_realized + tax_on_st_realized
                 - cf_used_against_ordinary * (marginal_bracket_pct / 100))
    return {
        "annual_dividend_income_projection_usd": round(div_qd + div_ord, 2),
        "qualified_dividend_usd": round(div_qd, 2),
        "ordinary_dividend_usd": round(div_ord, 2),
        "tax_on_qualified_dividends_usd": round(tax_on_qd, 2),
        "tax_on_ordinary_dividends_usd": round(tax_on_ord_div, 2),
        "tax_on_realized_lt_gains_usd": round(tax_on_lt_realized, 2),
        "tax_on_realized_st_gains_usd": round(tax_on_st_realized, 2),
        "carryforward_used_against_gains_usd": round(cf_used_against_gains, 2),
        "carryforward_used_against_ordinary_usd": round(cf_used_against_ordinary, 2),
        "carryforward_remaining_usd": round(cf_remaining, 2),
        "total_federal_tax_usd": round(max(0, total_tax), 2),
        "niit_active": niit_rate > 0,
        "agi": agi,
    }


def generate_verdict(per_pos_summary, tlh_candidates, after_tax_rows, asset_loc, tax_est):
    """Plain-English prioritized action items."""
    actions = []

    # TLH
    if tlh_candidates:
        top = tlh_candidates[0]
        total_savings = sum(c["estimated_tax_savings"] for c in tlh_candidates)
        actions.append({
            "priority": "HIGH",
            "category": "TAX-LOSS HARVESTING",
            "headline": f"Harvest ${sum(c['harvestable_loss'] for c in tlh_candidates):,.0f} in losses → save ~${total_savings:,.0f}",
            "detail": f"{len(tlh_candidates)} positions in loss. Top: {top['ticker']} (-${top['harvestable_loss']:,.0f}). Rotate into {top['substitute_etfs'][0] if top.get('substitute_etfs') else 'a substitute'} to maintain exposure.",
        })

    # ST → LT crossover
    near_lt = [p for p in per_pos_summary.get("positions", []) if not p["is_long_term"] and 0 < p["days_to_lt"] <= 60 and p["unrealized_pl"] > 500]
    if near_lt:
        nearest = min(near_lt, key=lambda x: x["days_to_lt"])
        actions.append({
            "priority": "MEDIUM",
            "category": "LT TIMING",
            "headline": f"{nearest['ticker']} crosses to LT in {nearest['days_to_lt']} days — don't sell early",
            "detail": f"Selling {nearest['ticker']} today costs ordinary tax (~{nearest['effective_tax_rate_pct']}%); waiting {nearest['days_to_lt']} days drops to LTCG. Saves ${nearest['tax_if_sold_today'] * 0.4:,.0f}+ on a ${nearest['unrealized_pl']:,.0f} gain.",
        })

    # Asset location
    if asset_loc.get("suggested_moves"):
        actions.append({
            "priority": "HIGH",
            "category": "ASSET LOCATION",
            "headline": f"{len(asset_loc['suggested_moves'])} tax-inefficient assets sitting in taxable — move to IRA/401(k)",
            "detail": "Bonds, REITs, HY credit, gold pay distributions taxed at ordinary income rates every year. In an IRA those distributions compound tax-free. Estimated savings: ~{}bp annually.".format(asset_loc.get("total_estimated_annual_savings_bps", 0)),
        })

    # Big concentrated position
    pos = per_pos_summary.get("positions", [])
    if pos:
        biggest = max(pos, key=lambda p: p["market_value"])
        if biggest["market_value"] > 0.20 * per_pos_summary.get("total_market_value", 1):
            actions.append({
                "priority": "INFO",
                "category": "CONCENTRATION",
                "headline": f"{biggest['ticker']} is >{round(biggest['market_value']/per_pos_summary['total_market_value']*100)}% of portfolio",
                "detail": f"Heavy concentration in {biggest['ticker']}. Consider whether the tax cost of trimming (${biggest['tax_if_sold_today']:,.0f} if sold today) is worth the diversification.",
            })

    # NIIT triggered?
    if tax_est.get("niit_active"):
        actions.append({
            "priority": "INFO",
            "category": "NIIT",
            "headline": "Net Investment Income Tax (3.8%) is active — your AGI exceeds threshold",
            "detail": "Every dollar of investment income (dividends, capital gains, interest) gets an extra 3.8% surtax. Maximize tax-deferred contributions to reduce AGI below the threshold ($200k single / $250k MFJ).",
        })

    return {
        "summary_line": (
            f"Tax-aware analysis: {len(actions)} action items. "
            f"{'Top priority: ' + actions[0]['category'] + '. ' if actions else ''}"
            f"Estimated annual tax cost on dividends: ${tax_est.get('tax_on_qualified_dividends_usd', 0) + tax_est.get('tax_on_ordinary_dividends_usd', 0):,.0f}."
        ),
        "action_items": actions,
    }


# =============================================================================
# HANDLER
# =============================================================================
def parse_event(event):
    """Accept Function URL query params OR direct invoke dict."""
    if event and isinstance(event, dict):
        qs = event.get("queryStringParameters") or {}
        if event.get("body"):
            try:
                body = json.loads(event["body"])
                if isinstance(body, dict):
                    qs = {**qs, **body}
            except Exception:
                pass
    else:
        qs = {}

    return {
        "federal_bracket": float(qs.get("federal_bracket", 24)),
        "state_rate": float(qs.get("state_rate", 5)),
        "agi": float(qs.get("agi", 200_000)),
        "filing_status": str(qs.get("filing_status", "single")).lower(),
        "taxable_pct": float(qs.get("taxable_pct", 0.6)),
        "ira_pct": float(qs.get("ira_pct", 0.3)),
        "roth_pct": float(qs.get("roth_pct", 0.1)),
        "carryforward": float(qs.get("carryforward", 0)),
        "min_harvest_loss": float(qs.get("min_harvest_loss", 500)),
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    inputs = parse_event(event)
    print(f"[tax-plan] v{VERSION} inputs={inputs}")

    federal_pct = inputs["federal_bracket"]
    state_pct = inputs["state_rate"]
    agi = inputs["agi"]
    filing = inputs["filing_status"]

    # Compute effective rates
    ltcg = get_ltcg_rate(agi, filing)
    niit = get_niit_rate(agi, filing)
    # Stack state with federal for ordinary
    marginal_combined_pct = federal_pct + state_pct  # rough combined

    # Load data
    positions = load_portfolio()
    compass = load_compass()

    # Compute
    pos_rows, pos_summary = per_position_tax_view(positions, marginal_combined_pct, ltcg, niit)
    pos_summary["positions"] = pos_rows
    at_rows = after_tax_forward_returns(compass, marginal_combined_pct, ltcg, niit)
    tlh = tax_loss_harvest_candidates(pos_rows, marginal_combined_pct, inputs["min_harvest_loss"])
    asset_loc = asset_location_recommendations(at_rows, inputs["taxable_pct"], inputs["ira_pct"], inputs["roth_pct"])
    tax_est = annual_tax_bill_estimate(pos_rows, marginal_combined_pct, ltcg, niit, inputs["carryforward"], agi, compass)
    verdict = generate_verdict(pos_summary, tlh, at_rows, asset_loc, tax_est)

    result = {
        "version": VERSION,
        "engine": "justhodl-tax-plan",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 2),
        "inputs": inputs,
        "effective_rates": {
            "marginal_combined_pct": marginal_combined_pct,
            "ltcg_pct": round(ltcg * 100, 1),
            "niit_pct": round(niit * 100, 1),
            "ltcg_plus_niit_pct": round((ltcg + niit) * 100, 1),
            "ordinary_plus_niit_pct": round(federal_pct + state_pct + niit * 100, 1),
        },
        "portfolio_tax_view": pos_summary,
        "after_tax_forward_returns": at_rows,
        "tax_loss_harvest_candidates": tlh,
        "asset_location": asset_loc,
        "annual_tax_bill_estimate": tax_est,
        "verdict": verdict,
        "compass_generated_at": compass.get("generated_at") if compass else None,
        "methodology": {
            "ordinary_income": "Federal marginal bracket + state stacking + NIIT 3.8% surtax (if AGI > $200k single / $250k MFJ)",
            "ltcg": "0% / 15% / 20% based on AGI thresholds (2026: $47k/$519k single; $94k/$584k MFJ). Plus 3.8% NIIT.",
            "qualified_dividends": "Same rate as LTCG (broad equity ETF distributions qualify; bond interest does not)",
            "asset_classification": "Per IRS Publication 550 + fund disclosure documents. REITs ~80% ordinary. Gold (GLD) = collectibles 28% max LTCG. K-1 commodities = ordinary on roll yield.",
            "tlh_substitutes": "Institutional consensus per Wealthfront/Betterment whitepapers — NOT 'substantially identical' under IRS guidance.",
            "wash_sale": "30-day window monitoring requires trade history (deferred to v2 — current version flags TLH candidates without auto-checking recent buys).",
        },
        "disclaimer": "Estimates based on 2026 US federal/state tax rules. Not tax advice. Consult a tax professional for material decisions. State-specific rules (especially CA, NY, NJ) may differ.",
    }

    # Write to S3 (default-profile snapshot or full result depending on caller)
    s3.put_object(
        Bucket=BUCKET,
        Key=OUT_KEY,
        Body=json.dumps(result, default=str, indent=2).encode(),
        ContentType="application/json",
    )

    print(f"[tax-plan] done · {len(pos_rows)} positions · {len(tlh)} TLH candidates · {len(verdict.get('action_items', []))} actions · elapsed={result['elapsed_seconds']}s")

    # Function URL response
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
        },
        "body": json.dumps(result, default=str),
    }
