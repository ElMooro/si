# ops 4261 -- trend/asym + forward-returns shape probe

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-08-01T22:36:18+00:00  

## Log
- `22:36:18` ticker=IEF class=bonds price=92.95
- `22:36:18`   trend = {"label": "DOWNTREND", "ok": false, "px_vs_50dma_pct": -1.0, "px_vs_200dma_pct": -2.8, "sma50_rising": false}
- `22:36:18`   breakout = {"state": "NONE", "bb_width_pctile_1y": 27.8, "range_52w_pos_pct": 1.9, "dist_to_52w_high_pct": 5.4, "vol_20d_ratio": 0.55, "ext_vs_50dma_pct": -1.0}
- `22:36:18`   asym = {"upside_pct": 6.7, "downside_pct": 0.1, "ratio": 25.0, "dd_now_pct": -6.2, "dd_depth_pctile_hist": 4.5, "status": "WATCH", "score": 68.7}
- `22:36:18`   horizon = {"hold": "~8y", "basis": "hold to duration immunizes the rate path"}
- `22:36:18` ticker=HYG class=credit trend={"label": "DOWNTREND", "ok": false, "px_vs_50dma_pct": -0.3, "px_vs_200dma_pct": -1.0, "sma50_rising": false} asym={"upside_pct": 2.3, "downside_pct": -2.8, "ratio": 0.83, "dd_now_pct": -2.3, "dd_depth_pctile_hist": 10.5, "status": "NEUTRAL", "score": 33.5}
- `22:36:18` forward-returns top keys: ['version', 'engine', 'generated_at', 'horizon_years', 'macro_inputs', 'real_gdp_growth_assumption_pct', 'assets', 'rankings', 'benchmark_portfolios', 'headlines', 'methodology', 'disclaimer', 'elapsed_s']
- `22:36:18`   dict 'macro_inputs'['y10']: 4.68
- `22:36:18`   dict 'assets'['SPY']: {"ticker": "SPY", "name": "US Large-Cap Stocks (S&P 500)", "current_price": 747.03, "trailing_dividend_yield_pct": 1.01, "buyback_yield_assumption_pct": 2.0, "nominal_growth_assumption_pct": 4.28, "forward_er_10y_pct": 7.29, "history_30y": {"realized_cagr_pct": 10.2, "er_median_pct": 7.0, "er_p10_pct": 4.5, "er_p90_pct": 10.5}, "current_vs_history_percentile": 53, "verdict": "FAIR", "verdict_color
- `22:36:18`   dict 'rankings'['by_forward_er']: ["BTC", "QQQ", "EEM", "EFA", "SPY", "VNQ", "IWM", "TLT", "LQD", "TIP", "IEF", "HYG", "BIL", "GLD", "DBC"]
- `22:36:18`   dict 'benchmark_portfolios'['all_cash']: {"label": "100% Cash (T-Bills)", "weights": {"BIL": 1.0}, "forward_er_pct": 3.82, "ten_k_10yr": 14548, "description": "The risk-free baseline. You lose to inflation when real rates are negative."}
- `22:36:18`   dict 'methodology'['stocks_er']: "Bogle's Sources of Return: dividend_yield + buyback_yield + nominal_earnings_growth (real_growth + breakeven_inflation). Same framework as Vanguard CMAs and GMO 7-year forecasts. Trailing dividend yield from FMP /stable/profile (lastDividend / price); buyback yield from institutional consensus (SPY=2%, QQQ=2.5%, IWM=1%, EFA=0.8%, EEM=0.4%); real growth per region (SPY=2%, QQQ=3%, IWM=2.5%, EFA=1.
- `22:36:18` ✅ probe2 complete
