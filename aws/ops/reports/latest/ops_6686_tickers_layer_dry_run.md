# ops 6686 - short-interest tickers layer: the first failing check, on the runner (read-only)

**Status:** success  
**Duration:** 36.1s  
**Finished:** 2026-10-26T21:11:59+00:00  

## Log
## 1. the head and today's request journal

- `21:11:24` head data/short-interest.json: 5012293 bytes, contract short-interest-original-research.v1, generated_at 2026-10-26T21:01:07.590180+00:00, settlement_date 2026-10-15, counts {"change_pct_statuses": {"matches_rounded_formula": 88156, "undefined_previous_zero": 2126}, "dtc_statuses": {"matches_reconstructed_rounded_ratio": 42007, "provider_999_99_convention_unconfirmed": 34, replay {"manifest_key": "data/short-interest-research/runs/d763b4b41479c8e9ab97d8ad651aa99d470c7fa8b8af1308a99a3db0be8dca83.json", "output_sha256": "ca478c0dfe793ddbf3df66ed625db5efd256aa122f76d50c730bb8c6b63ed891"}
- `21:11:24` permissions: {'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False, 'forecast_qualified': False}
- `21:11:24` journal scheduled-short-interest:2026-10-26: status complete, started None, result.published True reason None, result.replay.output_sha256 ca478c0dfe793ddbf3df66ed625db5efd256aa122f76d50c730bb8c6b63ed891
- `21:11:24`    head digest (minus replay) ca478c0dfe793ddb | head.replay == journal.replay: True | digest == journal.replay.output_sha256: True
- `21:11:24` journal scheduled-short-interest:2026-10-25: NoSuchKey
## 2. the layer's checks, in order

- `21:11:24` record_shards: 256 prefixes
- `21:11:57` 256 shards read, 38.7 MB, 23970 issues in 32.8s
- `21:11:57` issue inventory agrees with the head (23970)
- `21:11:59` project_verified_tickers: 23678 tickers, settlement 2026-10-15, measurement_contract short-interest-ticker-projection.v1 in 1.7s; encoded 18932830 bytes
- `21:11:59` sample: {"AAPL": {"ticker": "AAPL", "issue_name": "Apple Inc. Common Stock", "exchange_code": "R", "market_class": "NNM", "short_interest": "131591865", "prev_short_interest": "120763539", "change_shares": "10828326", "change_pct": "8.966552396249", "change_pct_reported": "8.97", "change_pct_reconciliation": "matches_rounded_formula", "avg_daily_volume": "35498663", "days_to_cover": "3.71", "dtc_reconstructed": "3.706952709740", "dtc_effective": "3.71", "dtc_status": "matches_reconstructed_rounded_ratio", "settlement_date": "2026-10-15", "prev_settlement_date": null, "revision_flag": null, "split_flag
- `21:11:59` ✅ every layer check passes on the runner with the live head - the skip is in the binding to the producer result or the publisher's budgets/retention, not in the data
- `21:11:59` ✅ ops 6686 complete (read-only)
