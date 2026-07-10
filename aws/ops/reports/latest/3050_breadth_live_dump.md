## 1. Full live payload

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-07-10T13:05:10+00:00  

## Data

| has_build | has_urlbase | main_script_len | main_script_sha | n_inline | n_script_tags | page_bytes | title_ok |
|---|---|---|---|---|---|---|---|
| True | True |  |  | 3 | 11 | 18100 | True |
|  |  | 8643 | 5660883f2b02ba25 |  |  |  |  |

## Log
- `13:05:10` PAYLOAD>>>{"engine": "breadth-thrust", "version": "1.0", "as_of": "2026-07-09T22:00:42.935887+00:00", "state": "NULL", "prev_state": "NULL", "state_since": "2026-06-10T01:00:11.328386+00:00", "state_transition": null, "signal_strength": 0, "cooldown_until": null, "current_readings": {"zweig_10d_ema": 0.4832, "zweig_window_min": 0.4657, "zweig_window_max": 0.5365, "zweig_thresholds": {"low": 0.4, "high": 0.615}, "n_breadth_days_cached": 45, "newly_fetched_this_run": 0, "whaley": {"state": "PENDING", "first_5d_return_pct": null, "note": "less than 5 January sessions so far"}, "coppock": {"state": "INSUFFICIENT_DATA"}}, "trigger_conditions": [{"name": "10d EMA recently <= 0.40 (oversold)", "current": "min 0.4657", "satisfied": false, "weight": 50}, {"name": "10d EMA now > 0.615 (thrust)", "current": "current 0.4832", "satisfied": false, "weight": 50}], "forward_expectations": {"1m": {"return_pct": null, "win_rate_pct": null, "n": 0, "basis": "SPY next 30 calendar days"}, "3m": {"return_pct": null, "win_rate_pct": null, "n": 0, "basis": "SPY next 90 calendar days"}, "6m": {"return_pct": null, "win_rate_pct": null, "n": 0, "basis": "SPY next 180 calendar days"}, "12m": {"return_pct": null, "win_rate_pct": null, "n": 0, "basis": "SPY next 365 calendar days"}}, "recommended_trade": {"primary": {"instrument": "No trade. Normal regime.", "thesis": "Breadth EMA in normal range. Zweig Thrust fires only after broad capitulation. This is a once-per-4-7-years event.", "size_guidance": "0%", "max_loss": "n/a", "expected_horizon": "indeterminate"}}, "why_now_explainer": "**NULL -- no setup.** Breadth EMA is in normal range. The Zweig Thrust requires broad capitulation first (10-day EMA <= 0.40), then violent reversal above 0.61 within 10 sessions.\n\nThis signal fires once every 4-7 years. It is extraordinarily rare and extraordinarily reliable. Until then, the dashboard waits.\n\n**Whaley:** PENDING (first 5 days of Jan None%). **Coppock:** INSUFFICIENT_DATA (None).", "historical_episodes": [], "supporting_signals": {"whaley_january_barometer": {"state": "PENDING", "first_5d_return_pct": null, "note": "less than 5 January sessions so far"}, "coppock_curve": {"state": "INSUFFICIENT_DATA"}}, "methodology": "Polygon grouped daily aggregates -> daily advancing/declining ratio (filter v>=100k) -> 10-period EMA. Zweig: EMA crosses from <0.40 to >0.615 within 10 trading sessions. Forward expectations computed live from 20y SPY history at curated historical trigger dates. Cache stored in data/breadth-history.json to respect Polygon rate limits (max 5 fresh days per run).", "sources": ["Polygon /v2/aggs/grouped/locale/us/market/stocks", "FMP /stable/historical-price-eod (SPY for forwards)", "academic: Martin Zweig 'Winning on Wall Street' (1986)"], "schedule": "daily 22:00 UTC (after US close)"}<<<END
## 2. Live page fingerprint

- `13:05:10` extra_script_1_head: document.addEventListener("DOMContentLoaded",function(){if(window.JHKit)JHKit.mountHistContext("jhk-hist",['VIXCLS','T10Y2Y','BAMLH0A0HYM2']);});
- `13:05:10` extra_script_2_head: window.__jhRail={"title":"Breadth Thrust (Zweig + Whaley + Coppock)","feeds":[{"label":"breadth-thrust","h":14.198796367579035}],"related":[{"title":"Kill Theses \u2014 Adversarial Pre-Mortem","href":
- `13:05:10` done
