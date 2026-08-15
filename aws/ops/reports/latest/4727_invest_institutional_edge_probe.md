# ops 4727 — probe institutional-edge engine outputs

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-08-15T23:42:05+00:00  

## Log
- `23:42:04` ── insider_industry_cluster (guessed: data/insider-industry-cluster.json) ──
- `23:42:04` ✅   data/insider-industry-cluster.json: FOUND. Shape:
{
  "version": "str: '1.1.0'",
  "generated_at": "str: '2026-08-15T14:20:48.291987+00:00'",
  "source_feed": "str: 'all_ticker_buys'",
  "lookback_days": 30,
  "n_industries": 3,
  "n_clusters": 0,
  "n_diffuse": 2,
  "industries": [
    {
      "industry": "<str>",
      "sector": "<str>",
      "n_companies": "<int>",
      "n_listed": "<int>",
      "participation_pct": "<float>",
      "n_insiders": "<int>",
      "n_transactions": "<int>",
      "total_value_usd": "<float>",
      "companies": "<list>",
      "ceo_cfo_companies": "<list>",
      "has_exec_conviction": "<bool>",
      "dollar_hhi": "<float>",
      "top_company": "<str>",
      "top_company_share_pct": "<float>",
      "z_vs_own_history": "<float>",
      "hist_n": "<int>",
      "thin_universe": "<bool>",
      "tier": "<str>",
      "participation_floor_pct": "<float>",
      "awaiting_base_rate": "<bool>"
    },
    {
      "industry": "<str>",
      "sector": "<str>",
      "n_companies": "<int>",
      "n_listed": "<int>",
      "participation_pct": "<float>",
      "n_insiders": "<int>",
      "n_transactions": "<int>",
      "total_value_usd": "<float>",
      "companies": "<list>",
      "ceo_cfo_companies": "<list>",
      "has_exec_conviction": "<bool>",
      "dollar_hhi": "<float>",
      "top_company": "<str>",
      "top_company_share_pct": "<float>",
      "z_vs_own_history": "<float>",
      "hist_n": "<int>",
      "thin_universe": "<bool>",
      "tier": "<str>",
      "participation_floor_pct": "<float>",
      "awaiting_base_rate": "<bool>"
    },
    {
      "industry": "<str>",
      "sector": "<str>",
      "n_companies": "<int>",
      "n_listed": "<int>",
      "participation_pct": "<float>",
      "n_insiders": "<int>",
      "n_transactions": "<int>",
      "total_value_usd": "<float>",
      "companies": "<list>",
      "ceo_cfo_companies": "<list>",
      "has_exec_conviction": "<bool>",
      "dollar_hhi": "<float>",
      "top_company": "<str>",
      "top_company_share_pct": "<float>",
      "z_vs_own_history": "<NoneType>",
      "hist_n": "<int>",
      "thin_universe": "<bool>",
      "tier": "<str>",
      "participation_floor_pct": "<float>",
      "awaiting_base_rate": "<bool>"
    }
  ],
  "degraded": [],
  "coverage": {
    "insider_rows_in": 64,
    "tickers_unmapped": 14,
    "universe_industries": 149,
    "min_companies": 3,
    "strong_companies": 4,
    "min_listed_for_rate": 8,
    "min_participation_pct": 4.0
  },
  "method": "str: 'Breadth is DISTINCT COMPANIES, not transactions or dollars \u2014'",
  "attribution": "str: 'SEC EDGAR Form 4 via justhodl-insider-cluster-scanner; indus'"
}
- `23:42:04` ── credit_before_equity (guessed: data/credit-before-equity.json) ──
- `23:42:04` ✅   data/credit-before-equity.json: FOUND. Shape:
{
  "version": "str: '1.0.0'",
  "generated_at": "str: '2026-08-15T22:35:41.862666+00:00'",
  "n_names": 26,
  "n_leads": 1,
  "n_awaiting_history": 0,
  "names": [
    {
      "ticker": "<str>",
      "name": "<str>",
      "group": "<str>",
      "distance_to_default": "<float>",
      "synthetic_cds_bp": "<float>",
      "default_prob_5y_pct": "<float>",
      "regime": "<str>",
      "peer_rank": "<int>",
      "market_cap_usd_bn": "<float>",
      "d_distance_to_default": "<float>",
      "d_synthetic_cds_bp": "<float>",
      "d_price_pct": "<float>",
      "credit_direction": "<str>",
      "equity_flat": "<bool>",
      "signal": "<str>",
      "prior_obs_date": "<str>",
      "hist_n": "<int>"
    },
    {
      "ticker": "<str>",
      "name": "<str>",
      "group": "<str>",
      "distance_to_default": "<float>",
      "synthetic_cds_bp": "<float>",
      "default_prob_5y_pct": "<float>",
      "regime": "<str>",
      "peer_rank": "<int>",
      "market_cap_usd_bn": "<float>",
      "d_distance_to_default": "<float>",
      "d_synthetic_cds_bp": "<float>",
      "d_price_pct": "<float>",
      "credit_direction": "<NoneType>",
      "equity_flat": "<bool>",
      "signal": "<str>",
      "prior_obs_date": "<str>",
      "hist_n": "<int>"
    },
    {
      "ticker": "<str>",
      "name": "<str>",
      "group": "<str>",
      "distance_to_default": "<float>",
      "synthetic_cds_bp": "<float>",
      "default_prob_5y_pct": "<float>",
      "regime": "<str>",
      "peer_rank": "<int>",
      "market_cap_usd_bn": "<float>",
      "d_distance_to_default": "<float>",
      "d_synthetic_cds_bp": "<float>",
      "d_price_pct": "<float>",
      "credit_direction": "<NoneType>",
      "equity_flat": "<bool>",
      "signal": "<str>",
      "prior_obs_date": "<str>",
      "hist_n": "<int>"
    },
    {
      "ticker": "<str>",
      "name": "<str>",
      "group": "<str>",
      "distance_to_default": "<float>",
      "synthetic_cds_bp": "<float>",
      "default_prob_5y_pct": "<float>",
      "regime": "<str>",
      "peer_rank": "<int>",
      "market_cap_usd_bn": "<float>",
      "d_distance_to_default": "<float>",
      "d_synthetic_cds_bp": "<float>",
      "d_price_pct": "<float>",
      "credit_direction": "<NoneType>",
      "equity_flat": "<bool>",
      "signal": "<str>",
      "prior_obs_date": "<str>",
      "hist_n": "<int>"
    },
    {
      "ticker": "<str>",
      "name": "<str>",
      "group": "<str>",
      "distance_to_default": "<float>",
      "synthetic_cds_bp": "<float>",
      "default_prob_5y_pct": "<float>",
      "regime": "<str>",
      "peer_rank": "<int>",
      "market_cap_usd_bn": "<float>",
      "d_distance_to_default": "<float>",
      "d_synthetic_cds_bp": "<float>",
      "d_price_pct": "<float>",
      "credit_direction": "<NoneType>",
      "equity_flat": "<bool>",
      "signal": "<str>",
      "prior_obs_date": "<str>",
      "hist_n": "<int>"
    },
    "...+21 more"
  ],
  "leads": [
    {
      "ticker": "<str>",
      "name": "<str>",
      "group": "<str>",
      "distance_to_default": "<float>",
      "synthetic_cds_bp": "<float>",
      "default_prob_5y_pct": "<float>",
      "regime": "<str>",
      "peer_rank": "<int>",
      "market_cap_usd_bn": "<float>",
      "d_distance_to_default": "<float>",
      "d_synthetic_cds_bp": "<float>",
      "d_price_pct": "<float>",
      "credit_direction": "<str>",
      "equity_flat": "<bool>",
      "signal": "<str>",
      "prior_obs_da
- `23:42:04` ── sector_flow_state (guessed: data/sector-flow-state.json) ──
- `23:42:04` ✅   data/sector-flow-state.json: FOUND. Shape:
{
  "engine": "str: 'justhodl-sector-flow-state'",
  "version": "str: '1.1.0'",
  "generated_at": "str: '2026-08-15T23:20:43.436041+00:00'",
  "liquidity_regime": "str: 'draining'",
  "cycle_phase": "str: 'EXPANSION-MID'",
  "n_sectors": 11,
  "overweight": [
    "str: 'XLE'",
    "str: 'XLC'",
    "str: 'XLF'"
  ],
  "underweight": [
    "str: 'XLY'",
    "str: 'XLP'",
    "str: 'XLRE'",
    "str: 'XLU'"
  ],
  "sectors": [
    {
      "symbol": "<str>",
      "name": "<str>",
      "conviction": "<float>",
      "posture": "<str>",
      "quadrant": "<str>",
      "confluence": "<int>",
      "drivers": "<list>",
      "rotation_score": "<float>",
      "rs_rank_1y": "<float>",
      "rs_slope": "<float>",
      "flow_confirm": "<str>",
      "in_cycle": "<bool>",
      "dollar_flow_usd": "<int>",
      "dollar_confirms": "<bool>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "conviction": "<float>",
      "posture": "<str>",
      "quadrant": "<str>",
      "confluence": "<int>",
      "drivers": "<list>",
      "rotation_score": "<float>",
      "rs_rank_1y": "<float>",
      "rs_slope": "<float>",
      "flow_confirm": "<str>",
      "in_cycle": "<bool>",
      "dollar_flow_usd": "<int>",
      "dollar_confirms": "<bool>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "conviction": "<float>",
      "posture": "<str>",
      "quadrant": "<str>",
      "confluence": "<int>",
      "drivers": "<list>",
      "rotation_score": "<float>",
      "rs_rank_1y": "<float>",
      "rs_slope": "<float>",
      "flow_confirm": "<str>",
      "in_cycle": "<bool>",
      "dollar_flow_usd": "<int>",
      "dollar_confirms": "<bool>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "conviction": "<float>",
      "posture": "<str>",
      "quadrant": "<str>",
      "confluence": "<int>",
      "drivers": "<list>",
      "rotation_score": "<float>",
      "rs_rank_1y": "<float>",
      "rs_slope": "<float>",
      "flow_confirm": "<str>",
      "in_cycle": "<bool>",
      "dollar_flow_usd": "<int>",
      "dollar_confirms": "<bool>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "conviction": "<float>",
      "posture": "<str>",
      "quadrant": "<str>",
      "confluence": "<int>",
      "drivers": "<list>",
      "rotation_score": "<float>",
      "rs_rank_1y": "<float>",
      "rs_slope": "<float>",
      "flow_confirm": "<str>",
      "in_cycle": "<bool>",
      "dollar_flow_usd": "<int>",
      "dollar_confirms": "<bool>"
    },
    "...+6 more"
  ],
  "methodology": "str: 'Fused per-sector conviction = rotation_score + RRG-quadrant '",
  "consumers": "str: 'deal-scanner, master-ranker, best-setups, bottleneck-boom (m'"
}
- `23:42:04` ── cftc_deep_view (guessed: data/cftc-deep-view.json) ──
- `23:42:04` ✅   data/cftc-deep-view.json: FOUND. Shape:
{
  "engine": "str: 'cftc-deep-view'",
  "version": "str: '1.0.0'",
  "generated_at": "str: '2026-08-15T21:00:20.149523+00:00'",
  "state": "str: 'NORMAL_POSITIONING'",
  "n_contracts_analyzed": 0,
  "risk_appetite": null,
  "n_extremes": 0,
  "n_divergences": 0,
  "top_divergences": [],
  "top_extremes": [],
  "all_contract_analyses": [
    {
      "symbol": "<str>",
      "name": "<str>",
      "category": "<str>",
      "status": "<str>",
      "n_records": "<int>",
      "as_of": "<str>",
      "net_speculator": "<float>",
      "net_commercial": "<float>",
      "net_spec_wow": "<float>",
      "smart_money_side": "<str>",
      "smart_money_net": "<float>",
      "direction": "<str>",
      "note": "<str>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "category": "<str>",
      "status": "<str>",
      "n_records": "<int>",
      "as_of": "<str>",
      "net_speculator": "<float>",
      "net_commercial": "<float>",
      "net_spec_wow": "<float>",
      "smart_money_side": "<str>",
      "smart_money_net": "<float>",
      "direction": "<str>",
      "note": "<str>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "category": "<str>",
      "status": "<str>",
      "n_records": "<int>",
      "as_of": "<str>",
      "net_speculator": "<float>",
      "net_commercial": "<float>",
      "net_spec_wow": "<float>",
      "smart_money_side": "<str>",
      "smart_money_net": "<float>",
      "direction": "<str>",
      "note": "<str>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "category": "<str>",
      "status": "<str>",
      "n_records": "<int>",
      "as_of": "<str>",
      "net_speculator": "<float>",
      "net_commercial": "<float>",
      "net_spec_wow": "<float>",
      "smart_money_side": "<str>",
      "smart_money_net": "<float>",
      "direction": "<str>",
      "note": "<str>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "category": "<str>",
      "status": "<str>",
      "n_records": "<int>",
      "as_of": "<str>",
      "net_speculator": "<float>",
      "net_commercial": "<float>",
      "net_spec_wow": "<float>",
      "smart_money_side": "<str>",
      "smart_money_net": "<float>",
      "direction": "<str>",
      "note": "<str>"
    },
    "...+24 more"
  ],
  "smart_money_map": {
    "equity_index": "str: 'large_speculators'",
    "volatility": "str: 'commercials'",
    "treasury": "str: 'commercials'",
    "rates": "str: 'commercials'",
    "fx": "str: 'large_speculators'",
    "currency": "str: 'large_speculators'",
    "energy": "str: 'commercials'",
    "metals": "str: 'commercials'",
    "agricultural": "str: 'commercials'",
    "ags": "str: 'commercials'",
    "softs": "str: 'commercials'",
    "livestock": "str: 'commercials'"
  },
  "methodology": {
    "framework": "str: 'COT z-score + smart/dumb divergence framework'",
    "philosophy": "str: 'Bloomberg COT function has no z-score layer; Refinitiv COT i'",
    "z_score_framework": "str: 'Net positioning standardized vs 1y + 3y historical mean. z >'",
    "smart_dumb_divergence": "str: 'Triggered when smart-money z and dumb-money z both >= 1.5 ab'",
    "data_source": "str: 'Reads data/cftc-all-cache.json from upstream cftc-futures-po'"
  },
  "academic_basis": [
    "str: 'Wang (2003). The behavior and performance of major types of '",
    "str: 'de Roon, Nijman, Veld (2000). Hedging pressure effects in fu'",
    "str: 'Bessembinder & Chan (1992). Time-varying risk p
- `23:42:04` ── cot_extremes (guessed: data/cot-extremes.json) ──
- `23:42:04` ✅   data/cot-extremes.json: FOUND. Shape:
{
  "engine": "str: 'cot-extremes'",
  "version": "str: '1.0.0'",
  "generated_at": "str: '2026-08-15T21:00:20.149523+00:00'",
  "state": "str: 'NORMAL_POSITIONING'",
  "n_extremes": 0,
  "n_divergences": 0,
  "extremes": [],
  "divergences": []
}
- `23:42:04` ── etf_fund_flows (guessed: etf-flows/composite.json) ──
- `23:42:04` ✅   etf-flows/composite.json: FOUND. Shape:
{
  "generated_at": "str: '2026-08-15T22:00:25.726840+00:00'",
  "universe_size": 300,
  "n_ok": 300,
  "n_failed": 0,
  "elapsed_s": 2.7,
  "schema_version": "str: '1.0'",
  "composite": {
    "defensive_rotation": {
      "score": "<float>",
      "label": "<str>",
      "components": "<dict>"
    },
    "smart_vs_dumb": {
      "score": "<float>",
      "label": "<str>",
      "components": "<dict>"
    },
    "risk_on_off": {
      "score": "<float>",
      "label": "<str>",
      "components": "<dict>"
    },
    "domestic_vs_intl": {
      "score": "<float>",
      "label": "<str>"
    },
    "growth_vs_value": {
      "score": "<float>",
      "label": "<str>"
    },
    "credit_stress": {
      "score": "<float>",
      "label": "<str>"
    },
    "regime": "str: 'TRANSITION'",
    "leveraged_appetite": {
      "bull_5d_usd": "<int>",
      "bear_5d_usd": "<int>",
      "net_5d_usd": "<int>",
      "read": "<str>",
      "n_suspect_excluded": "<int>",
      "suspects": "<list>",
      "pairs": "<list>"
    }
  },
  "divergence_board": {
    "method": "str: 'z(flow,90d) vs 21d nav return; stealth = z>=+1 & ret<=-2%; d'",
    "n_scored": 287,
    "stealth_accumulation": [],
    "distribution_rally": [
      "<dict>",
      "<dict>",
      "<dict>"
    ],
    "trend_confirmed": 4,
    "capitulation": 4,
    "leveraged_extremes": [
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "...+3 more"
    ],
    "note": "str: 'leveraged/inverse products are listed separately: their flow'"
  },
  "divergence_signals_logged": 2
}
- `23:42:04` ── stealth_accumulation (guessed: data/stealth-accumulation.json) ──
- `23:42:04` ✅   data/stealth-accumulation.json: FOUND. Shape:
{
  "engine": "str: 'stealth-accumulation'",
  "version": "str: '1.2.0'",
  "as_of": "str: '2026-08-15T23:00:39.411128Z'",
  "state": "str: 'INSUFFICIENT_DATA'",
  "previous_state": "str: 'UNKNOWN'",
  "state_description": "str: 'Detector blind: required feed(s) insider returned no rows \u2014 '",
  "signal_strength": 0,
  "summary": {
    "n_insider_tickers": 0,
    "n_smart_money_tickers": 80,
    "n_short_covering_tickers": 4,
    "n_options_flow_tickers": 0,
    "n_convergence_2plus": 0,
    "n_convergence_3plus": 0,
    "n_convergence_4_all_signals": 0,
    "feeds_available": [
      "<str>",
      "<str>"
    ],
    "feeds_missing": [
      "<dict>",
      "<dict>"
    ]
  },
  "data_sufficiency": {
    "sufficient": false,
    "required_feeds": [
      "<str>",
      "<str>",
      "<str>"
    ],
    "required_live": 2,
    "feed_rows": {
      "insider": "<int>",
      "smart_money": "<int>",
      "short_covering": "<int>",
      "options_flow": "<int>"
    },
    "rule": "str: 'state can only be QUIET/NORMAL when every required feed yiel'"
  },
  "combo_weights": {
    "weights": {
      "insider": "<float>",
      "smart_money": "<float>",
      "short_covering": "<float>",
      "options_flow": "<float>"
    },
    "meta": {
      "components": "<dict>",
      "overall_basis": "<str>",
      "min_n": "<int>",
      "learning_rate": "<float>",
      "clamp": "<list>"
    }
  },
  "impact_map": {
    "schema": "str: 'impact-map/1.0'",
    "engine": "str: 'stealth-accumulation'",
    "factor": "str: 'stealth_accumulation_evidence'",
    "generated_at": "str: '2026-08-15T23:00:39.012566+00:00'",
    "benefiting": [],
    "suffering": [],
    "insufficient": [],
    "method": "str: 'Self-nominating detector: flagged names ARE the beneficiarie'",
    "basis_note": "str: 'weights basis: prior_only'"
  },
  "signals_logged": 0,
  "current_readings": {
    "top_convergence_tickers": [],
    "n_signals_distribution": {
      "2": "<int>",
      "3": "<int>",
      "4": "<int>"
    }
  },
  "convergence": [],
  "top_insider_only": [],
  "top_smart_money_only": [
    {
      "ticker": "<str>",
      "strength": "<int>",
      "pattern": "<str>",
      "n_funds_buying": "<int>",
      "score": "<float>",
      "evidence": "<str>"
    },
    {
      "ticker": "<str>",
      "strength": "<int>",
      "pattern": "<str>",
      "n_funds_buying": "<int>",
      "score": "<float>",
      "evidence": "<str>"
    },
    {
      "ticker": "<str>",
      "strength": "<int>",
      "pattern": "<str>",
      "n_funds_buying": "<int>",
      "score": "<float>",
      "evidence": "<str>"
    },
    {
      "ticker": "<str>",
      "strength": "<int>",
      "pattern": "<str>",
      "n_funds_buying": "<int>",
      "score": "<float>",
      "evidence": "<str>"
    },
    {
      "ticker": "<str>",
      "strength": "<int>",
      "pattern": "<str>",
      "n_funds_buying": "<int>",
      "score": "<float>",
      "evidence": "<str>"
    },
    "...+5 more"
  ],
  "top_short_covering_only": [
    {
      "ticker": "<str>",
      "strength": "<int>",
      "z_score": "<float>",
      "category": "<str>",
      "evidence": "<str>"
    },
    {
      "ticker": "<str>",
      "strength": "<int>",
      "z_score": "<float>",
      "category": "<str>",
      "evidence": "<str>"
    },
    {
      "ticker": "<str>",
      "strength": "<int>",
      "z_score": "<float>",
      "category": "<str>",
      "evidence": "<str>"
    },
    {
      "ticker": "<str>"
- `23:42:04` ── dealer_gex (guessed: data/dealer-gex.json) ──
- `23:42:04` ✅   data/dealer-gex.json: FOUND. Shape:
{
  "generated_at": "str: '2026-08-14T21:07:22.984329+00:00'",
  "generated_at_unix": 1786741642,
  "version": "str: '1.3.0'",
  "elapsed_seconds": 6.94,
  "calculation_config": {
    "risk_free_rate": 0.0425,
    "expiry_horizon_days": 60,
    "contract_multiplier": 100,
    "n_underlyings": 10
  },
  "market_composite": {
    "spy_regime": "str: 'STRONG_POSITIVE_GAMMA'",
    "spy_gex_billions": 13.708,
    "spy_flip_level": null,
    "spy_spot": 777.88,
    "spy_pct_to_flip": null,
    "spy_trading_bias": "str: 'Fade rallies \u00b7 buy dips \u00b7 sell volatility \u00b7 low realized vol'",
    "qqq_regime": "str: 'POSITIVE_GAMMA'",
    "iwm_regime": "str: 'POSITIVE_GAMMA'",
    "index_gex_signs": "str: '+++'",
    "composite_regime": "str: 'ALL_POSITIVE_GAMMA'",
    "composite_signal": "str: 'Strong positive gamma across SPY/QQQ/IWM \u2014 sell vol, fade ex'"
  },
  "squeeze_candidates": [
    {
      "symbol": "<str>",
      "score": "<int>",
      "gex_billions": "<float>",
      "pcr_oi": "<float>",
      "spot": "<float>",
      "regime": "<str>"
    },
    {
      "symbol": "<str>",
      "score": "<int>",
      "gex_billions": "<float>",
      "pcr_oi": "<float>",
      "spot": "<float>",
      "regime": "<str>"
    },
    {
      "symbol": "<str>",
      "score": "<int>",
      "gex_billions": "<float>",
      "pcr_oi": "<float>",
      "spot": "<float>",
      "regime": "<str>"
    },
    {
      "symbol": "<str>",
      "score": "<int>",
      "gex_billions": "<float>",
      "pcr_oi": "<float>",
      "spot": "<float>",
      "regime": "<str>"
    },
    {
      "symbol": "<str>",
      "score": "<int>",
      "gex_billions": "<float>",
      "pcr_oi": "<float>",
      "spot": "<float>",
      "regime": "<str>"
    }
  ],
  "underlyings": {
    "NVDA": {
      "symbol": "<str>",
      "spot": "<float>",
      "n_contracts_modeled": "<int>",
      "total_call_oi": "<int>",
      "total_put_oi": "<int>",
      "total_call_volume": "<int>",
      "total_put_volume": "<int>",
      "pcr_oi": "<float>",
      "pcr_volume": "<float>",
      "total_dealer_gex_dollars": "<float>",
      "total_dealer_gex_billions": "<float>",
      "zero_gamma_flip_level": "<NoneType>",
      "spot_pct_to_flip": "<NoneType>",
      "spot_above_flip": "<bool>",
      "regime": "<str>",
      "trading_bias": "<str>",
      "total_vanna_dollars": "<float>",
      "total_charm_dollars_per_day": "<float>",
      "total_delta_dollars": "<float>",
      "total_vega_dollars": "<float>"
    },
    "IWM": {
      "symbol": "<str>",
      "spot": "<float>",
      "n_contracts_modeled": "<int>",
      "total_call_oi": "<int>",
      "total_put_oi": "<int>",
      "total_call_volume": "<int>",
      "total_put_volume": "<int>",
      "pcr_oi": "<float>",
      "pcr_volume": "<float>",
      "total_dealer_gex_dollars": "<float>",
      "total_dealer_gex_billions": "<float>",
      "zero_gamma_flip_level": "<NoneType>",
      "spot_pct_to_flip": "<NoneType>",
      "spot_above_flip": "<bool>",
      "regime": "<str>",
      "trading_bias": "<str>",
      "total_vanna_dollars": "<float>",
      "total_charm_dollars_per_day": "<float>",
      "total_delta_dollars": "<float>",
      "total_vega_dollars": "<float>"
    },
    "SPY": {
      "symbol": "<str>",
      "spot": "<float>",
      "n_contracts_modeled": "<int>",
      "total_call_oi": "<int>",
      "total_put_oi": "<int>",
      "total_call_volume": "<int>",
      "total_put_volume": "<int>",
      "pcr_oi":
- `23:42:04` ── finra_short (guessed: data/finra-short.json) ──
- `23:42:04` ✅   data/finra-short.json: FOUND. Shape:
{
  "generated_at": "str: '2026-08-15T01:00:33.810763+00:00'",
  "generated_at_unix": 1786755633,
  "version": "str: '1.1.0'",
  "data_date": "str: '2026-08-14'",
  "elapsed_seconds": 3.97,
  "config": {
    "universe": "str: 'S&P 500'",
    "history_days": 90,
    "squeeze_thresholds": {
      "svr_hot": "<float>",
      "z_hot": "<float>",
      "z_extreme": "<float>",
      "dtc_hot": "<float>",
      "climax_svr": "<float>"
    }
  },
  "market_composite": {
    "regime": "str: 'NORMAL'",
    "volume_weighted_svr": 0.4743,
    "volume_weighted_svr_pct": 47.43,
    "median_svr_pct": 51.74,
    "p10_svr_pct": 34.47,
    "p90_svr_pct": 69.47,
    "p99_svr_pct": 82.24,
    "n_analyzed": 501,
    "n_universe_in_file": 501,
    "n_high_svr": 138,
    "n_extreme_z": 14
  },
  "squeeze_candidates": [
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "svr_pct": "<float>",
      "z_score": "<float>",
      "momentum_pct": "<float>",
      "days_to_cover": "<float>",
      "squeeze_score": "<int>",
      "squeeze_flags": "<list>",
      "price_strength": "<dict>",
      "short_volume": "<float>",
      "total_volume": "<float>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "svr_pct": "<float>",
      "z_score": "<float>",
      "momentum_pct": "<float>",
      "days_to_cover": "<float>",
      "squeeze_score": "<int>",
      "squeeze_flags": "<list>",
      "price_strength": "<dict>",
      "short_volume": "<float>",
      "total_volume": "<float>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "svr_pct": "<float>",
      "z_score": "<float>",
      "momentum_pct": "<float>",
      "days_to_cover": "<float>",
      "squeeze_score": "<int>",
      "squeeze_flags": "<list>",
      "price_strength": "<dict>",
      "short_volume": "<float>",
      "total_volume": "<float>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "svr_pct": "<float>",
      "z_score": "<float>",
      "momentum_pct": "<float>",
      "days_to_cover": "<float>",
      "squeeze_score": "<int>",
      "squeeze_flags": "<list>",
      "price_strength": "<dict>",
      "short_volume": "<float>",
      "total_volume": "<float>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "svr_pct": "<float>",
      "z_score": "<float>",
      "momentum_pct": "<float>",
      "days_to_cover": "<float>",
      "squeeze_score": "<int>",
      "squeeze_flags": "<list>",
      "price_strength": "<dict>",
      "short_volume": "<float>",
      "total_volume": "<float>"
    },
    "...+9 more"
  ],
  "top_svr": [
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "svr_pct": "<float>",
      "z_score": "<float>",
      "short_volume": "<float>",
      "total_volume": "<float>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "svr_pct": "<float>",
      "z_score": "<float>",
      "short_volume": "<float>",
      "total_volume": "<float>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "svr_pct": "<float>",
      "z_score": "<float>",
      "short_volume": "<float>",
      "total_volume": "<float>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "svr_pct": "<float>",
      "z_score": "<float>",
      "short_volume": "<float>",
 
- `23:42:04` ── squeeze_fuel (guessed: data/squeeze-fuel.json) ──
- `23:42:05` ✅   data/squeeze-fuel.json: FOUND. Shape:
{
  "engine": "str: 'justhodl-squeeze-fuel'",
  "version": "str: '1.0.0'",
  "ok": true,
  "generated_at": "str: '2026-08-15T13:30:42.685307+00:00'",
  "thesis": "str: 'Per-name short-squeeze FUEL gauge from 100% free authoritati'",
  "si_settlement_date": "str: '2026-07-31'",
  "ftd_file": "str: 'cnsfails202607a.zip'",
  "n_finra_universe": 2976,
  "n_scored": 328,
  "distribution": {
    "n_loaded": 5,
    "n_building": 114,
    "n_elevated": 112
  },
  "board": [
    {
      "score": "<float>",
      "state": "<str>",
      "price_confirm": "<bool>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "short_interest": "<int>",
      "si_change_pct": "<float>",
      "components": "<dict>",
      "reasons": "<list>",
      "ticker": "<str>",
      "name": "<str>"
    },
    {
      "score": "<float>",
      "state": "<str>",
      "price_confirm": "<bool>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "short_interest": "<int>",
      "si_change_pct": "<float>",
      "components": "<dict>",
      "reasons": "<list>",
      "ticker": "<str>",
      "name": "<str>"
    },
    {
      "score": "<float>",
      "state": "<str>",
      "price_confirm": "<bool>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "short_interest": "<int>",
      "si_change_pct": "<float>",
      "components": "<dict>",
      "reasons": "<list>",
      "ticker": "<str>",
      "name": "<str>"
    },
    {
      "score": "<float>",
      "state": "<str>",
      "price_confirm": "<bool>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "short_interest": "<int>",
      "si_change_pct": "<float>",
      "components": "<dict>",
      "reasons": "<list>",
      "ticker": "<str>",
      "name": "<str>"
    },
    {
      "score": "<float>",
      "state": "<str>",
      "price_confirm": "<bool>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "short_interest": "<int>",
      "si_change_pct": "<float>",
      "components": "<dict>",
      "reasons": "<list>",
      "ticker": "<str>",
      "name": "<str>"
    },
    "...+55 more"
  ],
  "top_picks": [
    {
      "ticker": "<str>",
      "score": "<float>",
      "direction": "<str>",
      "state": "<str>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "reasons": "<list>"
    },
    {
      "ticker": "<str>",
      "score": "<float>",
      "direction": "<str>",
      "state": "<str>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "reasons": "<list>"
    },
    {
      "ticker": "<str>",
      "score": "<float>",
      "direction": "<str>",
      "state": "<str>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "reasons": "<list>"
    },
    {
      "ticker": "<str>",
      "score": "<float>",
      "direction": "<str>",
      "state": "<str>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "reasons": "<list>"
    },
    {
      "ticker": "<str>",
      "score": "<float>",
      "direction": "<str>",
      "state": "<str>",
      "pct_of_float": "<float>",
      "days_to_cover": "<float>",
      "reasons": "<list>"
    },
    "...+15 more"
  ],
  "data_sources": {
    "short_interest": "str: 'FINRA Consolidated Short Interest API (official, bi-monthly)'",
    "fails_to_deliver": "str: 'SEC CNS fails-to-deliver (semi-monthly)'",
    "daily_short_volume": "str: 'justhodl-finra-short (FINRA Reg
- `23:42:05` ── congress_direct (guessed: data/congress-direct.json) ──
- `23:42:05` ✅   data/congress-direct.json: FOUND. Shape:
{
  "ok": true,
  "version": "str: '1.0.2'",
  "generated_at": "str: '2026-08-15T15:30:39.291836+00:00'",
  "elapsed_s": 18.15,
  "source": "str: 'OFFICIAL \u2014 Senate eFD + House Clerk (no vendor)'",
  "senate": {
    "n_reports": 23,
    "n_transactions": 256,
    "n_with_ticker": 198,
    "reports": [
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "...+18 more"
    ],
    "transactions": [
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "...+195 more"
    ],
    "error": null
  },
  "house": {
    "n_ptr_filings": 200,
    "filings": [
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "...+95 more"
    ],
    "error": null
  }
}
- `23:42:05` ── hiring_velocity (guessed: data/hiring-velocity.json) ──
- `23:42:05` ✅   data/hiring-velocity.json: FOUND. Shape:
{
  "schema_version": "str: '1.0'",
  "method": "str: 'hiring_velocity_v1'",
  "generated_at": "str: '2026-08-09T12:32:45.329572+00:00'",
  "elapsed_s": 137.4,
  "n_scanned": 4289,
  "n_scored": 3300,
  "n_errors": 0,
  "counts": {
    "expansion_inflection": 303,
    "aggressive_expansion": 213,
    "double_confirmed_with_bagger": 19
  },
  "top_50": [
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "cap_bucket": "<str>",
      "market_cap": "<int>",
      "expansion_score": "<float>",
      "state": "<str>",
      "headcount_latest": "<int>",
      "headcount_yoy_pct": "<float>",
      "headcount_accel_pp": "<float>",
      "headcount_multiyr_cagr_pct": "<float>",
      "revenue_per_employee": "<int>",
      "revenue_per_employee_trend_pct": "<float>",
      "inflection": "<bool>",
      "notes": "<list>",
      "history": "<list>",
      "bagger_score": "<NoneType>",
      "rank": "<int>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "cap_bucket": "<str>",
      "market_cap": "<int>",
      "expansion_score": "<float>",
      "state": "<str>",
      "headcount_latest": "<int>",
      "headcount_yoy_pct": "<float>",
      "headcount_accel_pp": "<float>",
      "headcount_multiyr_cagr_pct": "<float>",
      "revenue_per_employee": "<int>",
      "revenue_per_employee_trend_pct": "<float>",
      "inflection": "<bool>",
      "notes": "<list>",
      "history": "<list>",
      "bagger_score": "<NoneType>",
      "rank": "<int>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "cap_bucket": "<str>",
      "market_cap": "<int>",
      "expansion_score": "<float>",
      "state": "<str>",
      "headcount_latest": "<int>",
      "headcount_yoy_pct": "<float>",
      "headcount_accel_pp": "<float>",
      "headcount_multiyr_cagr_pct": "<float>",
      "revenue_per_employee": "<int>",
      "revenue_per_employee_trend_pct": "<NoneType>",
      "inflection": "<bool>",
      "notes": "<list>",
      "history": "<list>",
      "bagger_score": "<NoneType>",
      "rank": "<int>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "cap_bucket": "<str>",
      "market_cap": "<int>",
      "expansion_score": "<float>",
      "state": "<str>",
      "headcount_latest": "<int>",
      "headcount_yoy_pct": "<float>",
      "headcount_accel_pp": "<float>",
      "headcount_multiyr_cagr_pct": "<float>",
      "revenue_per_employee": "<int>",
      "revenue_per_employee_trend_pct": "<float>",
      "inflection": "<bool>",
      "notes": "<list>",
      "history": "<list>",
      "bagger_score": "<NoneType>",
      "rank": "<int>"
    },
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "cap_bucket": "<str>",
      "market_cap": "<int>",
      "expansion_score": "<float>",
      "state": "<str>",
      "headcount_latest": "<int>",
      "headcount_yoy_pct": "<float>",
      "headcount_accel_pp": "<float>",
      "headcount_multiyr_cagr_pct": "<float>",
      "revenue_per_employee": "<int>",
      "revenue_per_employee_trend_pct": "<float>",
      "inflection": "<bool>",
      "notes": "<list>",
      "history": "<list>",
      "bagger_score": "<NoneType>",
      "rank": "<int>"
    },
    "...+45 more"
  ],
  "expansion_inflections": [
    {
      "symbol": "<str>",
      "name": "<str>",
      "sector": "<str>",
      "cap_bucket": "<str>",
      "market_ca
- `23:42:05` ── estimate_revisions (guessed: data/estimate-revisions.json) ──
- `23:42:05` ✅   data/estimate-revisions.json: FOUND. Shape:
{
  "engine": "str: 'justhodl-estimate-revisions'",
  "version": "str: '2.1.0'",
  "generated_at": "str: '2026-08-15T12:00:37.248409+00:00'",
  "status": "str: 'LIVE'",
  "thesis": "str: 'FMP depth (forward-EPS growth, analyst coverage, dispersion '",
  "horizon_days": 75,
  "n_tracked": 877,
  "direction_map": {
    "GTLS": "str: 'FLAT'",
    "MCW": "str: 'FLAT'",
    "WOLF": "str: 'FLAT'",
    "TNK": "str: 'FLAT'",
    "TEL": "str: 'FLAT'",
    "OTLY": "str: 'FLAT'",
    "GSK": "str: 'FLAT'",
    "CSTM": "str: 'FLAT'",
    "CP": "str: 'FLAT'",
    "BNL": "str: 'FLAT'",
    "ARIS": "str: 'FLAT'",
    "VAL": "str: 'FLAT'",
    "USLM": "str: 'FLAT'",
    "SYBT": "str: 'FLAT'",
    "SKFRY": "str: 'FLAT'",
    "PLPC": "str: 'FLAT'",
    "OBT": "str: 'FLAT'",
    "MVBF": "str: 'FLAT'",
    "KMTUY": "str: 'FLAT'",
    "JMSB": "str: 'FLAT'"
  },
  "n_fmp_enriched": 279,
  "n_with_history": 597,
  "n_state_keys": 1200,
  "estimate_strength_leaders": [
    {
      "ticker": "<str>",
      "company": "<str>",
      "earnings_date": "<str>",
      "session": "<str>",
      "days_to_earnings": "<int>",
      "fiscal_period": "<str>",
      "fiscal_year": "<int>",
      "importance": "<int>",
      "current_eps_est": "<float>",
      "baseline_eps_est": "<float>",
      "eps_rev_pct": "<float>",
      "eps_rev_recent_pct": "<float>",
      "rev_rev_pct": "<float>",
      "revenue_confirms": "<bool>",
      "baseline_date": "<str>",
      "n_obs": "<int>",
      "fwd_eps_growth_pct": "<float>",
      "n_analysts": "<int>",
      "dispersion_pct": "<float>",
      "estimate_strength": "<float>"
    },
    {
      "ticker": "<str>",
      "company": "<str>",
      "earnings_date": "<str>",
      "session": "<str>",
      "days_to_earnings": "<int>",
      "fiscal_period": "<str>",
      "fiscal_year": "<int>",
      "importance": "<int>",
      "current_eps_est": "<float>",
      "baseline_eps_est": "<float>",
      "eps_rev_pct": "<float>",
      "eps_rev_recent_pct": "<float>",
      "rev_rev_pct": "<float>",
      "revenue_confirms": "<bool>",
      "baseline_date": "<str>",
      "n_obs": "<int>",
      "fwd_eps_growth_pct": "<float>",
      "n_analysts": "<int>",
      "dispersion_pct": "<float>",
      "estimate_strength": "<float>"
    },
    {
      "ticker": "<str>",
      "company": "<str>",
      "earnings_date": "<str>",
      "session": "<str>",
      "days_to_earnings": "<int>",
      "fiscal_period": "<str>",
      "fiscal_year": "<int>",
      "importance": "<int>",
      "current_eps_est": "<float>",
      "baseline_eps_est": "<float>",
      "eps_rev_pct": "<float>",
      "eps_rev_recent_pct": "<float>",
      "rev_rev_pct": "<float>",
      "revenue_confirms": "<bool>",
      "baseline_date": "<str>",
      "n_obs": "<int>",
      "fwd_eps_growth_pct": "<float>",
      "n_analysts": "<int>",
      "dispersion_pct": "<float>",
      "estimate_strength": "<float>"
    },
    {
      "ticker": "<str>",
      "company": "<str>",
      "earnings_date": "<str>",
      "session": "<str>",
      "days_to_earnings": "<int>",
      "fiscal_period": "<str>",
      "fiscal_year": "<int>",
      "importance": "<int>",
      "current_eps_est": "<float>",
      "baseline_eps_est": "<float>",
      "eps_rev_pct": "<float>",
      "eps_rev_recent_pct": "<float>",
      "rev_rev_pct": "<float>",
      "revenue_confirms": "<bool>",
      "baseline_date": "<str>",
      "n_obs": "<int>",
      "fwd_eps_growth_pct": "<float>",
      "n_analysts": "<int
- `23:42:05` ── smart_money_13f (guessed: data/smart-money-13f.json) ──
- `23:42:05` ✅   data/smart-money-13f.json: FOUND. Shape:
{
  "engine": "str: 'smart-money-13f'",
  "version": "str: '1.0.0'",
  "generated_at": "str: '2026-08-10T11:00:28.830078+00:00'",
  "thesis": "str: 'Track the AGI/AI-infra conviction funds that have demonstrat'",
  "funds": [
    {
      "fund": "<str>",
      "manager": "<str>",
      "cik": "<str>",
      "report_date": "<str>",
      "filing_date": "<str>",
      "n_longs": "<int>",
      "n_puts": "<int>",
      "top_longs": "<list>",
      "puts": "<list>"
    }
  ],
  "smart_money_long_by_layer": {
    "unmapped": [
      "<dict>",
      "<dict>"
    ],
    "memory": [
      "<dict>",
      "<dict>"
    ],
    "neocloud": [
      "<dict>"
    ],
    "miners_to_ai": [
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "...+1 more"
    ],
    "foundry": [
      "<dict>"
    ],
    "silicon": [
      "<dict>"
    ]
  },
  "shorting_signal": [
    "str: 'AMD'",
    "str: 'ASML'",
    "str: 'AVGO'",
    "str: 'GLW'",
    "str: 'INTC'",
    "...+5 more"
  ],
  "confluence_cheap_and_backed": [
    {
      "ticker": "<str>",
      "n_funds_long": "<int>",
      "layer": "<str>",
      "your_discount_pct": "<float>",
      "your_growth_pct": "<float>",
      "note": "<str>"
    },
    {
      "ticker": "<str>",
      "n_funds_long": "<int>",
      "layer": "<str>",
      "your_discount_pct": "<float>",
      "your_growth_pct": "<float>",
      "note": "<str>"
    },
    {
      "ticker": "<str>",
      "n_funds_long": "<int>",
      "layer": "<str>",
      "your_discount_pct": "<float>",
      "your_growth_pct": "<float>",
      "note": "<str>"
    },
    {
      "ticker": "<str>",
      "n_funds_long": "<int>",
      "layer": "<str>",
      "your_discount_pct": "<float>",
      "your_growth_pct": "<float>",
      "note": "<str>"
    },
    {
      "ticker": "<str>",
      "n_funds_long": "<int>",
      "layer": "<str>",
      "your_discount_pct": "<float>",
      "your_growth_pct": "<float>",
      "note": "<str>"
    }
  ],
  "interpretation": "str: 'LONGS by layer mirror the buildout bottlenecks; PUTS are the'",
  "caveats": "str: '13F is stale up to 45 days, discloses only long equity + lis'",
  "source": "str: 'SEC EDGAR Form 13F-HR (free, public)'",
  "elapsed_s": 0.78
}
## Done

- `23:42:05` ✅ probe complete -- wire causal_graph.py from the real shapes above, not guesses
