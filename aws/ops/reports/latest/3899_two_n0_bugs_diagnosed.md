# ops 3899 — diagnose signal-backtest's snapshot bug + alpha-calibrator's trade-journal bug

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-07-26T03:15:49+00:00  

## Data

| failures | has_picks_key | item_count_approx | n_aged_7_plus_days | n_parseable_dates | n_picks | n_snapshot_files | picks_type | sample_has_p_field | sample_items_with_outcome_field | table_size_bytes | table_status |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 70 |  |  |  |  |  |
|  |  |  | 64 | 70 |  |  |  |  |  |  |  |
|  | True |  |  |  | 501 |  | dict |  |  |  |  |
|  |  |  |  |  |  |  |  | True |  |  |  |
|  |  | 1984 |  |  |  |  |  |  |  | 1172913 | ACTIVE |
|  |  |  |  |  |  |  |  |  | 0/5 |  |  |
| [] |  |  |  |  |  |  |  |  |  |  |  |

## Log
## 1. data/track-record/snapshots/ — does it actually have dated files

- `03:15:49`   earliest: data/track-record/snapshots/2026-05-17.json (2026-05-17 14:07:02+00:00)
- `03:15:49`   latest:   data/track-record/snapshots/2026-07-25.json (2026-07-25 14:01:05+00:00)
- `03:15:49`   age distribution (date, age_days, size_bytes): [('2026-05-17', 70, 51922), ('2026-05-18', 69, 52027), ('2026-05-19', 68, 52177), ('2026-05-20', 67, 52040), ('2026-05-21', 66, 52017), ('2026-05-22', 65, 52018), ('2026-05-23', 64, 52021), ('2026-05-24', 63, 52118), ('2026-05-25', 62, 52015), ('2026-05-26', 61, 52107)]
## 2. shape check — does the OLDEST aged-7+ snapshot actually have a real 'picks' dict

- `03:15:49` ✅   data/track-record/snapshots/2026-05-17.json readable, top-level keys: ['date', 'generated_at', 'n', 'picks', 'schema']
- `03:15:49`   sample pick [FICO]: {"v": "STRONG OPPORTUNITY", "p": 1098.59, "fv": 1609.24, "s": 85.9, "c": "high", "ss": [91, 100, 82, 32]}
## 3. data/trade-journal.json — does alpha-calibrator's OWN journal read have real data

- `03:15:49` ✅   top-level keys: ['best_calls_60d', 'generated_at', 'generated_at_unix', 'ledger', 'lookback_days', 'strategies', 'summary', 'version', 'worst_calls_60d']
- `03:15:49`   FULL DOC (first 1500 chars): {"generated_at": "2026-07-25T23:03:29.107211+00:00", "generated_at_unix": 1785020609, "version": "1.0.0", "lookback_days": 90, "summary": {"total_calls_90d": 1973, "total_evaluated_30d": 0, "overall_win_rate_30d_pct": null, "overall_avg_return_30d_pct": null, "n_strategies_tracked": 5, "n_open": 1973, "n_closed_or_hit": 0}, "strategies": [{"strategy": "OPTIONS_TIER_A", "total_calls_90d": 1462, "evaluated_30d": 0, "win_rate_30d_pct": null, "avg_return_30d_pct": null, "evaluated_90d": 0, "win_rate_90d_pct": null, "avg_return_90d_pct": null}, {"strategy": "REGIME_PICK", "total_calls_90d": 449, "evaluated_30d": 0, "win_rate_30d_pct": null, "avg_return_30d_pct": null, "evaluated_90d": 0, "win_rate_90d_pct": null, "avg_return_90d_pct": null}, {"strategy": "TIER_A_ALPHA", "total_calls_90d": 45, "evaluated_30d": 0, "win_rate_30d_pct": null, "avg_return_30d_pct": null, "evaluated_90d": 0, "win_rate_90d_pct": null, "avg_return_90d_pct": null}, {"strategy": "DEBATE_BUY", "total_calls_90d": 7, "evaluated_30d": 0, "win_rate_30d_pct": null, "avg_return_30d_pct": null, "evaluated_90d": 0, "win_rate_90d_pct": null, "avg_return_90d_pct": null}, {"strategy": "DEBATE_STRONG_BUY", "total_calls_90d": 10, "evaluated_30d": 0, "win_rate_30d_pct": null, "avg_return_30d_pct": null, "evaluated_90d": 0, "win_rate_90d_pct": null, "avg_return_90d_pct": null}], "best_calls_60d": [], "worst_calls_60d": [], "ledger": [{"call_date": "2026-07-25", "call_timestamp": "2026-07-25T22:50:42.020965+00:00", "symbol":
## 4. justhodl-trades DynamoDB table — the REAL source alpha-calibrator scans

- `03:15:49`   sample of 5 items: [{"strategy": "OPTIONS_TIER_A", "current_price_at_call": "84.42", "symbol": "ABT", "call_date": "2026-05-13", "signals_firing": ["CPR_SURGING", "CALL_VOL_3X", "SHORTS_COVERING"], "sector": "Healthcare", "outcome_status": "OPEN", "tier": "C", "options_flow_score_at_call": "75", "entry_price": "84.42", "call_timestamp": "2026-05-13T00:10:35.126154+00:00", "alpha_score": "57", "components_snapshot": {"sentiment": "58", "analysts": "75", "insiders": "92", "smart_money": "65", "growth": "41", "options_flow": "75", "quality": "67", "momentum": "9"}, "name": "Abbott Laboratories", "evaluated": false, "rationale": "Options flow TIER A \u00b7 score 75.0 \u00b7 CPR_SURGING, CALL_VOL_3X, SHORTS_COVERING", "macro_stress_at_call": "34", "sk": "2026-05-13#ABT#OPTIONS_TIER_A", "pk": "CALL", "regime_at_call": "NORMAL"}, {"strategy": "OPTIONS_TIER_A", "current_price_at_call": "432.17", "symbol": "ADI", "call_date": "2026-05-13", "signals_firing": ["CPR_SURGING", "CALL_VOL_3X", "ABS_CPR_3X", "SHORTS_COVERING"], "sector": "Technology", "outcome_status": "OPEN", "tier": "B", "options_flow_score_at_call": "85", "entry_price": "432.17", "call_timestamp": "2026-05-13T21:50:24.197105+00:00", "alpha_score"
## verdict

- `03:15:49` ✅ PROBE COMPLETE
