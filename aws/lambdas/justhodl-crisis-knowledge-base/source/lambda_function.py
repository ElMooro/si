"""
justhodl-crisis-knowledge-base — Crisis Pattern Knowledge Base builder

Generates a structured knowledge base of historical crisis patterns,
regime signatures, and current-state pattern matches. Output becomes
the RAG corpus for ai-chat to reference when answering "is this like
2018?" or "what should I do when bonds and gold both selloff?".

Output: data/crisis-knowledge-base.json
{
  "version": "1.0",
  "generated_at": "...",
  "patterns": [                # Historical crisis archetypes
    {
      "id": "dollar_shortage_2008",
      "name": "Dollar Shortage / Funding Stress",
      "trigger_signals": ["TED spread > 1%", "FRA-OIS > 50bp", "EFP > 30bp"],
      "historical_examples": ["2008-09 Lehman", "2020-03 COVID", "2023-03 SVB"],
      "characteristic_moves": {"DXY": "+5-15%", "Gold": "-5% then +20%", ...},
      "duration": "weeks to months",
      "policy_response": "Fed swap lines + RP facility",
      "playbook": "..."
    }, ...
  ],
  "current_state": {           # Today's signature
    "active_signals": [...],
    "best_pattern_match": {...},  # Closest historical analog
    "match_score": 0.73
  },
  "frameworks": {              # Methodology references
    "dollar_smile": "...",
    "btc_cycle_phases": [...],
    "yield_curve_shapes": [...]
  }
}

This is built as a Lambda that runs daily, snapshots current regime
state, and updates the historical-pattern database.
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/crisis-knowledge-base.json")
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")

s3 = boto3.client("s3")


# ═══ The Crisis Pattern Library — curated, time-tested archetypes ═══

CRISIS_PATTERNS = [
    {
        "id": "dollar_shortage",
        "name": "Dollar Shortage / Funding Stress",
        "category": "funding_crisis",
        "trigger_signals": [
            "DXY rallying >5% in 30d",
            "TED spread > 100bp",
            "FRA-OIS spread > 50bp",
            "Cross-currency basis swaps blow out",
            "Repo rates spike",
        ],
        "historical_examples": [
            {"date": "2008-09 Lehman", "duration_days": 45, "spy_drawdown": -25},
            {"date": "2020-03 COVID", "duration_days": 21, "spy_drawdown": -34},
            {"date": "2023-03 SVB", "duration_days": 14, "spy_drawdown": -8},
        ],
        "characteristic_moves": {
            "DXY": "rally +5 to +15%",
            "Gold": "initial drop -5% then rally +20% as Fed responds",
            "Treasuries": "long-end rally hard (flight to quality)",
            "Credit": "IG spreads wider 50-200bp; HY widens 200-500bp",
            "VIX": "spike to 35+, often 60+",
            "BTC": "correlates with risk assets, drops 15-50%",
        },
        "duration": "2-12 weeks",
        "policy_response": "Fed: emergency rate cut + swap lines + RP/RFP facility + QE restart",
        "playbook": (
            "1) Reduce equity exposure aggressively. "
            "2) Long DXY via UUP. "
            "3) Long long-end Treasuries (TLT). "
            "4) Sell credit (LQD short or buy HYG puts). "
            "5) Wait for Fed pivot signal then reverse all positions. "
            "6) Fed pivot is the high-conviction LONG entry — historically buys ~6-12 months of bull market."
        ),
        "watch_for_resolution": [
            "Fed announces emergency facility",
            "DXY rolls over (loses 1.5% in a week)",
            "Cross-currency basis normalizes",
        ],
    },
    {
        "id": "treasury_auction_failure",
        "name": "Treasury Auction Stress",
        "category": "fiscal_crisis",
        "trigger_signals": [
            "Bid-to-cover ratio < 2.0x for 10Y/30Y",
            "Tail > 2bp on long-end auctions",
            "Indirect bidder share collapsing",
            "Foreign holdings of UST declining",
            "30Y yield > 4.5% with rising real yield",
        ],
        "historical_examples": [
            {"date": "2008 Q4", "context": "post-Lehman, ironically demand surged"},
            {"date": "2023 Q4", "context": "30Y >5%, weak demand, Treasury rebalanced supply"},
            {"date": "2024 Aug", "context": "20Y auction tail"},
        ],
        "characteristic_moves": {
            "30Y yield": "rises 30-50bp in days",
            "Term premium": "rises sharply",
            "DXY": "weakens (fiscal concern > rate differential)",
            "Gold": "rallies hard",
            "Equities": "growth/long-duration sells off, value relatively OK",
        },
        "duration": "Days to weeks",
        "policy_response": "Treasury rebalances supply to short-end. Fed may slow QT.",
        "playbook": (
            "1) Short long-duration Treasuries (TBT). "
            "2) Long gold (GLD). "
            "3) Underweight long-duration equities (XLK, growth). "
            "4) Overweight commodity equities (XLE). "
            "5) Watch for Treasury Refunding Announcement — that's the resolution catalyst."
        ),
    },
    {
        "id": "credit_event",
        "name": "Credit Event / Default Wave",
        "category": "credit_crisis",
        "trigger_signals": [
            "HY-IG spread > 400bp",
            "CDX HY > 450bp",
            "Distressed debt ratio > 10%",
            "Loan/bond default rate rising",
            "VIX-VVIX divergence (VVIX outpacing VIX)",
        ],
        "historical_examples": [
            {"date": "2008-09", "context": "Lehman default cascading"},
            {"date": "2015-08", "context": "Energy HY blowup"},
            {"date": "2020-03", "context": "COVID lockdowns"},
            {"date": "2023-03", "context": "SVB regional banks"},
        ],
        "characteristic_moves": {
            "Credit spreads": "HY blows out 200-500bp",
            "VIX": "spikes",
            "Equities": "small-caps and financials lead lower",
            "Treasuries": "long-end rallies",
            "Gold": "rallies after initial wobble",
        },
        "duration": "Weeks to quarters",
        "policy_response": "Fed cuts + emergency facilities (TALF, PPPLF, etc.)",
        "playbook": (
            "1) Sell IWM (small caps). "
            "2) Sell XLF (financials, esp regionals via KRE). "
            "3) Buy HYG puts or short HYG. "
            "4) Long TLT. "
            "5) Look for Fed announcement of credit facility — that's the buy signal."
        ),
    },
    {
        "id": "growth_scare",
        "name": "Growth Scare / Recession Pricing",
        "category": "macro_slowdown",
        "trigger_signals": [
            "ISM PMI < 48 for 2+ months",
            "Initial claims rising rapidly (>250K and climbing)",
            "OECD CLI declining for 6+ months",
            "Yield curve disinverting (bull steepening = recession trigger)",
            "Earnings revisions sharply negative",
        ],
        "historical_examples": [
            {"date": "2001", "context": "tech recession"},
            {"date": "2008 H1", "context": "pre-Lehman housing"},
            {"date": "2019-2020 transition", "context": "COVID prelude"},
            {"date": "2022 H2", "context": "Fed-induced slowdown"},
        ],
        "characteristic_moves": {
            "Equities": "down 15-30% in cyclical names",
            "Defensives": "outperform (XLP, XLV, XLU)",
            "Treasuries": "rally significantly",
            "Gold": "rallies on Fed cut expectations",
            "USD": "weakens as Fed pivots dovish",
        },
        "duration": "6-18 months",
        "policy_response": "Fed cutting cycle 200-500bp",
        "playbook": (
            "1) Rotate from cyclicals to defensives (XLY→XLP, XLI→XLV). "
            "2) Long duration Treasuries (TLT). "
            "3) Long gold. "
            "4) Reduce credit exposure. "
            "5) Wait for ISM > 50 + claims rolling over to re-enter cyclicals."
        ),
    },
    {
        "id": "inflation_resurgence",
        "name": "Inflation Resurgence / Stagflation",
        "category": "inflation_regime",
        "trigger_signals": [
            "Core CPI accelerating",
            "Wage growth >4% YoY",
            "Commodity index rising",
            "Real yields negative",
            "Long-end yields rising with breakevens",
        ],
        "historical_examples": [
            {"date": "1973-1974", "context": "Oil shock + Vietnam fiscal"},
            {"date": "1978-1980", "context": "Volcker pre-cure"},
            {"date": "2021-2022", "context": "post-COVID stimulus + supply chain"},
        ],
        "characteristic_moves": {
            "Equities": "P/E compress; growth/long-duration hit hardest",
            "Commodities": "energy, metals, ag rally",
            "Gold": "underperforms TIPS initially, rallies later",
            "Treasuries": "sell off, especially long-end",
            "USD": "mixed — rate diff vs inflation differential",
        },
        "duration": "Years",
        "policy_response": "Fed hiking cycle 300-500bp",
        "playbook": (
            "1) Underweight long-duration equities. "
            "2) Overweight energy (XLE), materials (XLB). "
            "3) Long TIPS (real yield protection). "
            "4) Short long-duration Treasuries (TBT). "
            "5) Short USD vs commodity currencies (CAD, AUD)."
        ),
    },
    {
        "id": "fed_pivot",
        "name": "Fed Pivot — The Generational Buy",
        "category": "policy_inflection",
        "trigger_signals": [
            "Fed funds futures pricing cuts within 6 months",
            "Real yields collapsing",
            "Yield curve steepening hard",
            "Forward guidance shift in FOMC statement",
            "Powell speech tone change",
            "Net liquidity expansion (WALCL up, TGA spending)",
        ],
        "historical_examples": [
            {"date": "1995-Q3", "context": "Soft landing pivot, +30% next 12mo"},
            {"date": "1998-Q4", "context": "LTCM rescue, +40% next 12mo"},
            {"date": "2019-Q1", "context": "QT pause, +30% next 12mo"},
            {"date": "2020-Q2", "context": "COVID infinity QE, +75% next 12mo"},
            {"date": "2023-Q4", "context": "Pivot signaled, +25% next 6mo"},
        ],
        "characteristic_moves": {
            "Equities": "rally 20-40% over 6-12 months",
            "Long-duration": "outperforms by 10-20%",
            "Gold": "rallies to new highs",
            "USD": "weakens 5-15%",
            "Crypto": "rallies 50-200%",
        },
        "duration": "6-24 months",
        "policy_response": "RATE CUTS + QE/balance sheet expansion",
        "playbook": (
            "1) MAX LONG everything risk: SPY, QQQ, IWM, BTC, ETH, gold. "
            "2) Long TLT for the rate-cut amplifier. "
            "3) Avoid USD longs. "
            "4) This is the highest-conviction trade in the entire macro playbook. "
            "5) Hold for 6-12 months minimum."
        ),
        "warning": "Watch for inflation reacceleration — that ends the party prematurely.",
    },
    {
        "id": "btc_cycle_top",
        "name": "BTC Cycle Top",
        "category": "crypto_cycle",
        "trigger_signals": [
            "MVRV Z-score > 7",
            "RHODL ratio > 50K",
            "Funding rates persistently > 0.05% per 8h",
            "Retail Google search interest at peak",
            "BTC Mayer Multiple > 2.4",
            "Open interest at all-time-highs",
        ],
        "historical_examples": [
            {"date": "2017-12", "btc_price": 19500, "drawdown_to_low": -84},
            {"date": "2021-04", "btc_price": 64800, "drawdown_to_low": -55},
            {"date": "2021-11", "btc_price": 69000, "drawdown_to_low": -77},
        ],
        "characteristic_moves": {
            "BTC": "tops, declines 50-85% over 12 months",
            "Alts": "drop 80-95%",
            "USD": "may rally during crypto winter",
        },
        "duration": "12-18 months",
        "playbook": (
            "1) Gradually scale out of BTC at MVRV >5, full exit by MVRV >7. "
            "2) Avoid alts entirely. "
            "3) Short ETH/BTC as alts always underperform in crypto winter. "
            "4) Wait for MVRV <1 to re-enter."
        ),
    },
    {
        "id": "btc_cycle_bottom",
        "name": "BTC Cycle Bottom",
        "category": "crypto_cycle",
        "trigger_signals": [
            "MVRV Z-score < -1",
            "Realized price > spot price (capitulation)",
            "Hash rate making new highs (miners stayed)",
            "Retail Google interest at multi-year lows",
            "Funding rates negative for weeks",
            "Spot ETF outflows have stopped",
        ],
        "historical_examples": [
            {"date": "2018-12", "btc_price": 3200, "next_12mo_return": "+200%"},
            {"date": "2020-03", "btc_price": 4000, "next_12mo_return": "+500%"},
            {"date": "2022-11", "btc_price": 16000, "next_12mo_return": "+150%"},
        ],
        "characteristic_moves": {
            "BTC": "bottoms, rallies 200-500% over 12-18 months",
            "Alts": "outperform BTC after 6-12 month lag",
        },
        "duration": "Bottom phase 1-3 months, run-up 18-36 months",
        "playbook": (
            "1) DCA into BTC aggressively. "
            "2) Wait for BTC to clearly trend above 200d MA before adding alts. "
            "3) ETH/BTC ratio bottoming = alt season starts."
        ),
    },
    {
        "id": "yield_curve_inversion",
        "name": "Yield Curve Inversion",
        "category": "leading_indicator",
        "trigger_signals": [
            "10Y-2Y spread negative",
            "10Y-3M spread negative",
            "Curve inverted for 3+ months",
        ],
        "historical_examples": [
            {"date": "1989", "recession_lag_months": 16},
            {"date": "2000", "recession_lag_months": 13},
            {"date": "2006", "recession_lag_months": 22},
            {"date": "2019-08", "recession_lag_months": 7},
            {"date": "2022-07", "recession_lag_months": "unresolved"},
        ],
        "characteristic_moves": {
            "Equities": "often rally first 6-12 months after inversion (false comfort)",
            "Recession": "arrives 6-22 months later when curve dis-inverts (bull steepens)",
        },
        "duration": "6-22 months from inversion to recession",
        "playbook": (
            "1) Inversion is NOT an immediate sell signal. "
            "2) Watch for DIS-INVERSION (bull steepening = short-end falling faster than long-end). "
            "3) Dis-inversion = recession imminent within 3-6 months. "
            "4) Add defensive exposure when 10Y-2Y crosses back to positive."
        ),
    },
    {
        "id": "vix_term_structure_inversion",
        "name": "VIX Term Structure Inversion (Backwardation)",
        "category": "vol_regime",
        "trigger_signals": [
            "VIX spot > VIX 3M",
            "VIX9D > VIX (very near-term stress)",
        ],
        "historical_examples": [
            {"date": "2008-09", "context": "Lehman"},
            {"date": "2015-08", "context": "China devaluation"},
            {"date": "2018-02", "context": "Volmageddon"},
            {"date": "2020-03", "context": "COVID"},
            {"date": "2022-09", "context": "Fed hiking"},
        ],
        "characteristic_moves": {
            "Spot VIX": "spikes >25",
            "Equities": "selling off currently or imminently",
            "Resolution": "term structure normalizes within 1-4 weeks usually",
        },
        "duration": "Days to weeks",
        "playbook": (
            "1) Backwardation = stress NOW. "
            "2) Don't short vol while inverted (VXX/UVXY). "
            "3) Lean defensive. "
            "4) When term structure flips back to contango → vol crush trade (short VXX) becomes attractive."
        ),
    },
    {
        "id": "permanent_portfolio",
        "name": "Permanent Portfolio (Browne)",
        "category": "all_weather",
        "trigger_signals": ["Always — this is a passive allocation"],
        "historical_examples": [
            {"date": "1972-2024", "context": "Backtested ~6-7% real return with low drawdown"},
        ],
        "characteristic_moves": {
            "25% Equities (SPY)": "for prosperity",
            "25% Long Treasuries (TLT)": "for deflation",
            "25% Gold (GLD)": "for inflation/uncertainty",
            "25% Cash (BIL/SGOV)": "for tight money",
        },
        "duration": "Permanent",
        "playbook": (
            "Rebalance annually. "
            "Designed for households, not alpha-seeking. "
            "Floor for capital preservation."
        ),
    },
    {
        "id": "13f_consensus_buy",
        "name": "13F Institutional Consensus Build",
        "category": "smart_money",
        "trigger_signals": [
            "Same stock added by 8+ tracked funds in same quarter",
            "Net adds > 0 across all funds",
            "AUM-weighted addition > $1B",
        ],
        "historical_examples": [
            {"date": "2009-Q1", "stocks": ["BAC", "C"], "context": "Post-crisis financials"},
            {"date": "2020-Q2", "stocks": ["AAPL", "MSFT"], "context": "Stay-at-home"},
            {"date": "2023-Q4", "stocks": ["NVDA", "META"], "context": "AI / cost cuts"},
        ],
        "characteristic_moves": {
            "Stock": "tends to outperform SPY by 5-15% over 6-12 months when 5+ funds add",
            "Risk": "tends to underperform by 5-10% when 5+ funds exit",
        },
        "duration": "6-18 months",
        "playbook": (
            "1) Filter for stocks with N_funds_adding ≥ 5 AND value > $1B. "
            "2) Verify it's not pre-earnings (smart money may know something). "
            "3) Build position over 1-3 months. "
            "4) Hold for 6-12 months. "
            "5) Re-evaluate at next 13F filing."
        ),
    },
    {
        "id": "asymmetric_setup",
        "name": "Asymmetric Risk/Reward Setup (QARP)",
        "category": "single_stock",
        "trigger_signals": [
            "asymmetric_score > 70 from QARP scorer",
            "Multiple confirming signals: insider cluster + 8-K positive + AAII bearish (contrarian) + chart support",
            "Positive risk/reward: upside 30%+ vs downside 10%-",
        ],
        "historical_examples": [
            {"context": "Stocks scoring 70+ on asymmetric historically outperform SPY 15-25% over 12 months"},
        ],
        "playbook": (
            "1) These are the highest-conviction single-stock entries. "
            "2) Size at 2-5% of portfolio per name. "
            "3) Stop-loss at -10%. "
            "4) Take partial profits at +25%."
        ),
    },
]

FRAMEWORKS = {
    "dollar_smile": {
        "name": "Dollar Smile Theory (Stephen Jen)",
        "description": (
            "USD strengthens at BOTH economic extremes — strong global growth (US outperforming) "
            "OR risk-off / global recession (flight to USD). USD weakens in the middle (synchronized "
            "global recovery). Mid-cycle is the classic 'dollar bear' regime."
        ),
        "implication": (
            "Watch synchronized global PMIs: when ALL major economies are expanding together, "
            "USD typically weakens — short DXY, long EM and commodity FX."
        ),
    },
    "yield_curve_shapes": {
        "name": "Yield Curve Decomposition",
        "shapes": {
            "steepening_bull": {
                "description": "Short rates falling faster than long (Fed cutting). RECESSION imminent (3-6mo).",
                "signal": "Highest-conviction recession indicator when occurring after sustained inversion.",
            },
            "steepening_bear": {
                "description": "Long rates rising faster than short. INFLATION/GROWTH expectations rising.",
                "signal": "Risk-on for cyclicals, value > growth.",
            },
            "flattening_bull": {
                "description": "Long rates falling faster than short. Slowdown / disinflation.",
                "signal": "Mid-cycle slowdown, defensives outperform.",
            },
            "flattening_bear": {
                "description": "Short rates rising faster than long. Fed hiking aggressively.",
                "signal": "Late-cycle, growth at risk, watch for inversion.",
            },
        },
    },
    "btc_cycle_indicators": {
        "name": "BTC Cycle Phase Detection",
        "phases": {
            "deep_bear": {"mvrv_z": "< -1", "next_action": "DCA aggressively"},
            "accumulation": {"mvrv_z": "-1 to 1", "next_action": "DCA, scale in alts late"},
            "bull": {"mvrv_z": "1 to 5", "next_action": "Hold, ride trend"},
            "euphoria": {"mvrv_z": "> 5", "next_action": "Scale out"},
            "top": {"mvrv_z": "> 7", "next_action": "Full exit, await crypto winter"},
        },
    },
    "plant_and_harvest": {
        "name": "Plant-and-Harvest Cycle",
        "description": (
            "Macro investing cycles. PLANT during max-pessimism (high VIX, max-drawdown, capitulation, "
            "Fed pivot signaled). HARVEST during euphoria (low VIX, all-time-highs, retail FOMO, "
            "complacent options pricing)."
        ),
        "rules": [
            "PLANT: VIX > 35 + Khalid Index < 30 + Fed cutting + AAII bear extreme",
            "HARVEST: VIX < 15 + Khalid Index > 70 + Fed hiking + AAII bull extreme + GEX positive extreme",
        ],
    },
}


def fetch_fred(series_id: str, last_n: int = 30):
    """Pull a FRED series for current-state classification."""
    try:
        url = (f"https://api.stlouisfed.org/fred/series/observations"
               f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
               f"&sort_order=desc&limit={last_n}")
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        obs = data.get("observations", [])
        return [{"date": o["date"], "value": float(o["value"])} for o in obs if o.get("value") not in (".", "")]
    except Exception as e:
        print(f"  FRED {series_id} fail: {e}")
        return []


def get_current_state():
    """Sample current macro readings to identify which patterns are active."""
    state = {
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "indicators": {},
        "active_patterns": [],
    }

    # Pull live data from existing S3 files
    def gs3(key):
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            return json.loads(obj["Body"].read())
        except Exception:
            return None

    # 1. Khalid Index
    report = gs3("data/report.json")
    if report:
        ki = report.get("khalid_index", report.get("khalidIndex", {}))
        if isinstance(ki, dict):
            state["indicators"]["khalid_index"] = {"value": ki.get("score"), "regime": ki.get("regime")}

    # 2. VIX curve
    vix = gs3("data/vix-curve.json")
    if vix:
        state["indicators"]["vix"] = {
            "spot": vix.get("vix_spot") or vix.get("spot"),
            "3m": vix.get("vix_3m") or vix.get("3m"),
            "regime": vix.get("regime"),
        }

    # 3. Fed liquidity
    liq = gs3("data/liquidity-flow.json")
    if liq:
        state["indicators"]["liquidity"] = {
            "net_b": liq.get("net_liquidity_b") or liq.get("net_liquidity"),
            "regime": liq.get("regime"),
        }

    # 4. AAII sentiment
    aaii = gs3("data/aaii-sentiment.json")
    if aaii:
        state["indicators"]["aaii"] = {
            "bull_pct": aaii.get("bullish_pct"),
            "bear_pct": aaii.get("bearish_pct"),
            "regime": aaii.get("regime") or aaii.get("signal"),
        }

    # 5. Bond regime
    br = gs3("regime/current.json")
    if br:
        state["indicators"]["bond_regime"] = {
            "regime": br.get("regime"),
            "strength": br.get("regime_strength"),
        }

    # 6. Yield curve via FRED
    curve = fetch_fred("T10Y2Y", last_n=1)
    if curve:
        state["indicators"]["yield_curve_2s10s"] = curve[0]["value"]

    # 7. DXY
    dxy_obs = fetch_fred("DTWEXBGS", last_n=30)
    if dxy_obs:
        latest = dxy_obs[0]["value"]
        m1_ago = dxy_obs[-1]["value"] if len(dxy_obs) >= 30 else dxy_obs[-1]["value"]
        chg30 = round((latest / m1_ago - 1) * 100, 2)
        state["indicators"]["dxy"] = {"value": latest, "change_30d_pct": chg30}

    # ─── Pattern matching: which crisis patterns are most-active? ───
    # Score each pattern based on how many of its triggers are met
    pattern_scores = []
    for p in CRISIS_PATTERNS:
        score = 0
        matched = []

        # Heuristic matching against current state
        if p["id"] == "dollar_shortage":
            dxy_chg = (state["indicators"].get("dxy") or {}).get("change_30d_pct", 0) or 0
            vix_spot = (state["indicators"].get("vix") or {}).get("spot", 0) or 0
            if dxy_chg > 5:
                score += 1; matched.append("DXY +5% in 30d")
            if vix_spot > 25:
                score += 1; matched.append(f"VIX > 25 ({vix_spot})")

        elif p["id"] == "growth_scare":
            ki = (state["indicators"].get("khalid_index") or {}).get("value", 50) or 50
            if ki > 60:
                score += 1; matched.append(f"Khalid Index > 60 ({ki})")

        elif p["id"] == "btc_cycle_top":
            # Would need MVRV — not in S3 yet, skip for now
            pass

        elif p["id"] == "yield_curve_inversion":
            yc = state["indicators"].get("yield_curve_2s10s", 0) or 0
            if yc < 0:
                score += 1; matched.append(f"2s10s inverted ({yc})")

        elif p["id"] == "vix_term_structure_inversion":
            vix_d = state["indicators"].get("vix") or {}
            spot = vix_d.get("spot", 0) or 0
            v3m = vix_d.get("3m", 0) or 0
            if spot > v3m and v3m > 0:
                score += 1; matched.append(f"VIX backwardation (spot={spot} > 3M={v3m})")

        elif p["id"] == "fed_pivot":
            # Net liquidity expanding = pivot signal
            liq_regime = (state["indicators"].get("liquidity") or {}).get("regime", "")
            if liq_regime and "EXPAND" in liq_regime.upper():
                score += 1; matched.append(f"Net liquidity expanding ({liq_regime})")

        if score > 0:
            pattern_scores.append({
                "pattern_id": p["id"],
                "name": p["name"],
                "match_score": score,
                "matched_signals": matched,
                "playbook": p.get("playbook", ""),
            })

    pattern_scores.sort(key=lambda x: -x["match_score"])
    state["active_patterns"] = pattern_scores[:5]
    return state


def lambda_handler(event, context):
    print("[START] crisis-knowledge-base builder")
    started = time.time()

    try:
        current_state = get_current_state()
    except Exception as e:
        print(f"  current_state error: {e}")
        current_state = {"as_of": datetime.now(timezone.utc).isoformat(), "error": str(e)}

    output = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_patterns": len(CRISIS_PATTERNS),
        "n_frameworks": len(FRAMEWORKS),
        "patterns": CRISIS_PATTERNS,
        "frameworks": FRAMEWORKS,
        "current_state": current_state,
        "build_duration_s": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="no-cache",
    )
    print(f"[DONE] {len(CRISIS_PATTERNS)} patterns + {len(FRAMEWORKS)} frameworks → s3://{S3_BUCKET}/{S3_KEY}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "n_patterns": len(CRISIS_PATTERNS),
            "n_frameworks": len(FRAMEWORKS),
            "n_active_patterns": len(current_state.get("active_patterns", [])),
        }),
    }
