# ops 6708 - ETF RESEARCH strip: why SPY's flow windows read 'Unavailable' (read-only)

**Status:** success  
**Duration:** 5.4s  
**Finished:** 2026-11-02T15:38:13+00:00  

## Log
## A. the desk packet data/etf-desk-research.json

- `15:38:08` written 2026-10-31T23:08:39 | generated_at 2026-10-31T23:06:40.137428+00:00 | query_date 2026-10-31 | version 2.0.0 | quality {"canonical_overlap": 100, "configured_funds": 116, "flows_current_eligible": 116, "funds_with_current_profiles_flows_holdings": 111, "holdings_complete": 111, "independent_investment_votes": 0, "profile_fee_scale_certified": false, "profiles_current_eligible": 116, "status": "partial", "weight_unit_certified": false}
- `15:38:08` canonical_sources.flows.generated_at 2026-10-31T22:01:03.657038+00:00 | holdings 2026-10-31T22:48:24.311914+00:00 | reference {"bytes": 1461, "end_date": "2026-10-30", "key": "data/provider-flow-research/histories/4fc330e965a89a509507165ea9ef5921b445ba77776049eccbda4164724a4de8.json", "observations": 79, "sha256": "4fc330e965a89a509507165ea9ef5921b445ba77776049eccbda4164724a4de8"}
- `15:38:08` SPY: basis canonical_original_replay | flow quality {"flow_current_eligible": true, "holdings_source_check_current": true, "independent_investment_votes": 0, "profile_current_eligible": true} | flows.status complete_acquired_history acquisition retained acquired 2026-10-31T22:00:52.453210+00:00 valid_until 2026-11-02T00:00:52.453210+00:00 latest_effective 2026-10-30 | profile 2026-10-30 | holdings ['2026-10-29']
- `15:38:08`      window 1: status matched_reporting_window | 2026-10-30 .. 2026-10-30 | flow_usd_decimal 5232643078.0 | observations None
- `15:38:08`      window 5: status matched_reporting_window | 2026-10-26 .. 2026-10-30 | flow_usd_decimal 2200505214.15 | observations None
- `15:38:08`      window 21: status matched_reporting_window | 2026-10-02 .. 2026-10-30 | flow_usd_decimal 1580235326.60 | observations None
- `15:38:08`      SPY flows keys: ['acquisition_status', 'aligned_windows', 'call', 'calls_eligible', 'classification', 'classification_scope', 'execution_eligible', 'forecast_qualified', 'history', 'latest_effective_date', 'latest_observation', 'latest_processed_date', 'latest_windows', 'measure', 'originals', 'portfolio_action', 'quality', 'revision_versions_selected', 'sizing_eligible', 'source_acquired_at', 'source_valid_until', 'ticker', 'unit'] | quality: {"exchange_calendar_verified": false, "first_historical_availability_verified": false, "invalid_rows": 0, "observed_dates": 79, "pagination_complete": true, "source_age_hours": 0.0031121744444444445, "status": "complete_acquired_history"}
- `15:38:08` QQQ: basis canonical_original_replay | flow quality {"flow_current_eligible": true, "holdings_source_check_current": true, "independent_investment_votes": 0, "profile_current_eligible": true} | flows.status complete_acquired_history acquisition retained acquired 2026-10-31T22:00:47.814662+00:00 valid_until 2026-11-02T00:00:47.814662+00:00 latest_effective 2026-10-29 | profile 2026-10-30 | holdings ['2026-10-29']
- `15:38:08`      window 1: status incomplete | 2026-10-30 .. 2026-10-30 | flow_usd_decimal None | observations None
- `15:38:08`      window 5: status incomplete | 2026-10-26 .. 2026-10-30 | flow_usd_decimal None | observations None
- `15:38:08`      window 21: status incomplete | 2026-10-02 .. 2026-10-30 | flow_usd_decimal None | observations None
- `15:38:08` IWM: basis canonical_original_replay | flow quality {"flow_current_eligible": true, "holdings_source_check_current": true, "independent_investment_votes": 0, "profile_current_eligible": true} | flows.status complete_acquired_history acquisition retained acquired 2026-10-31T22:00:41.282956+00:00 valid_until 2026-11-02T00:00:41.282956+00:00 latest_effective 2026-10-30 | profile 2026-10-30 | holdings ['2026-10-29']
- `15:38:08`      window 1: status matched_reporting_window | 2026-10-30 .. 2026-10-30 | flow_usd_decimal 0 | observations None
- `15:38:08`      window 5: status matched_reporting_window | 2026-10-26 .. 2026-10-30 | flow_usd_decimal 206907448.75 | observations None
- `15:38:08`      window 21: status matched_reporting_window | 2026-10-02 .. 2026-10-30 | flow_usd_decimal -2855782980.70 | observations None
- `15:38:08` TLT: basis canonical_original_replay | flow quality {"flow_current_eligible": true, "holdings_source_check_current": true, "independent_investment_votes": 0, "profile_current_eligible": true} | flows.status complete_acquired_history acquisition retained acquired 2026-10-31T22:00:54.166555+00:00 valid_until 2026-11-02T00:00:54.166555+00:00 latest_effective 2026-10-30 | profile 2026-10-30 | holdings ['2026-10-29']
- `15:38:08`      window 1: status matched_reporting_window | 2026-10-30 .. 2026-10-30 | flow_usd_decimal 407213840.0 | observations None
- `15:38:08`      window 5: status matched_reporting_window | 2026-10-26 .. 2026-10-30 | flow_usd_decimal 82759962.6 | observations None
- `15:38:08`      window 21: status matched_reporting_window | 2026-10-02 .. 2026-10-30 | flow_usd_decimal -1877553937.0 | observations None
- `15:38:08` GLD: basis canonical_original_replay | flow quality {"flow_current_eligible": true, "holdings_source_check_current": true, "independent_investment_votes": 0, "profile_current_eligible": true} | flows.status complete_acquired_history acquisition retained acquired 2026-10-31T22:00:38.399388+00:00 valid_until 2026-11-02T00:00:38.399388+00:00 latest_effective 2026-10-30 | profile 2026-10-30 | holdings ['2026-10-30']
- `15:38:08`      window 1: status matched_reporting_window | 2026-10-30 .. 2026-10-30 | flow_usd_decimal 73800800.0 | observations None
- `15:38:08`      window 5: status matched_reporting_window | 2026-10-26 .. 2026-10-30 | flow_usd_decimal 835200.0 | observations None
- `15:38:08`      window 21: status matched_reporting_window | 2026-10-02 .. 2026-10-30 | flow_usd_decimal -1901245200.0 | observations None
## B. the canonical flow packet data/provider-fund-flow-research.json

- `15:38:08` written 2026-10-31T22:02:02 | generated_at 2026-10-31T22:01:03.657038+00:00 | source_valid_until 2026-11-02T00:00:26.772298+00:00 | reference {"bytes": 1461, "end_date": "2026-10-30", "key": "data/provider-flow-research/histories/4fc330e965a89a509507165ea9ef5921b445ba77776049eccbda4164724a4de8.json", "observations": 79, "sha256": "4fc330e96 | funds 300 by status {'complete_acquired_history': 298, 'stale': 1, 'unavailable': 1}
- `15:38:08` SPY: status complete_acquired_history | acquisition retained | acquired 2026-10-31T22:00:52.453210+00:00 | valid_until 2026-11-02T00:00:52.453210+00:00 | latest_effective 2026-10-30 | quality {"exchange_calendar_verified": false, "first_historical_availability_verified": false, "invalid_rows": 0, "observed_dates": 79, "pagination_complete": true, "source_age_hours": 0.0031121744444444445, "status": "complete_acquired_history"}
- `15:38:08` QQQ: status complete_acquired_history | acquisition retained | acquired 2026-10-31T22:00:47.814662+00:00 | valid_until 2026-11-02T00:00:47.814662+00:00 | latest_effective 2026-10-29 | quality {"exchange_calendar_verified": false, "first_historical_availability_verified": false, "invalid_rows": 0, "observed_dates": 78, "pagination_complete": true, "source_age_hours": 0.00440066, "status": "complete_acquired_history"}
- `15:38:08` IWM: status complete_acquired_history | acquisition retained | acquired 2026-10-31T22:00:41.282956+00:00 | valid_until 2026-11-02T00:00:41.282956+00:00 | latest_effective 2026-10-30 | quality {"exchange_calendar_verified": false, "first_historical_availability_verified": false, "invalid_rows": 0, "observed_dates": 79, "pagination_complete": true, "source_age_hours": 0.0062150227777777785, "status": "complete_acquired_history"}
- `15:38:08` TLT: status complete_acquired_history | acquisition retained | acquired 2026-10-31T22:00:54.166555+00:00 | valid_until 2026-11-02T00:00:54.166555+00:00 | latest_effective 2026-10-30 | quality {"exchange_calendar_verified": false, "first_historical_availability_verified": false, "invalid_rows": 0, "observed_dates": 79, "pagination_complete": true, "source_age_hours": 0.0026362452777777775, "status": "complete_acquired_history"}
- `15:38:08` GLD: status complete_acquired_history | acquisition retained | acquired 2026-10-31T22:00:38.399388+00:00 | valid_until 2026-11-02T00:00:38.399388+00:00 | latest_effective 2026-10-30 | quality {"exchange_calendar_verified": false, "first_historical_availability_verified": false, "invalid_rows": 0, "observed_dates": 79, "pagination_complete": true, "source_age_hours": 0.007016013888888889, "status": "complete_acquired_history"}
## C. the two engines: schedules, last runs, errors (5 days)

- `15:38:09` justhodl-etf-fund-flows: 2048 MB / 300 s, LastModified 2026-09-21T10:08:29.000+0000, state Active
- `15:38:09`    reserved concurrency 0
- `15:38:11`    4 runs, 0 error lines
- `15:38:11`    2026-10-28T22:02 c-a2b8c64b059c Duration: 109272.07 ms Billed Duration: 109622 ms Memory Size: 2048 MB Max Memory Used: 329 MB Init Duration: 349.50 ms 
XRAY TraceId: 1-6ae27076-562f7dad5448a8795e05e2e2 SegmentId: 83d
- `15:38:11`    2026-10-29T22:02 1-f891268943ce Duration: 109381.74 ms Billed Duration: 109740 ms Memory Size: 2048 MB Max Memory Used: 332 MB Init Duration: 358.03 ms 
XRAY TraceId: 1-6ae3c1f6-574c43ee4d3b6ca268a41d3e SegmentId: 621
- `15:38:11`    2026-10-30T22:02 a-6b9075fd392f Duration: 106985.53 ms Billed Duration: 107278 ms Memory Size: 2048 MB Max Memory Used: 346 MB Init Duration: 292.33 ms 
XRAY TraceId: 1-6ae51376-6542ca7324a729911c9402d9 SegmentId: 061
- `15:38:11`    2026-10-31T22:02 a-28fdf4e958ec Duration: 101799.86 ms Billed Duration: 102198 ms Memory Size: 2048 MB Max Memory Used: 329 MB Init Duration: 397.67 ms 
XRAY TraceId: 1-6ae664f6-6bc00b6e1171e99531df17fd SegmentId: 588
- `15:38:11` justhodl-etf-global-desk: 4096 MB / 900 s, LastModified 2026-10-01T17:08:34.000+0000, state Active
- `15:38:11`    reserved concurrency 0
- `15:38:13`    4 runs, 0 error lines
- `15:38:13`    2026-10-28T23:08 c-f752c011b34f Duration: 198972.75 ms Billed Duration: 199365 ms Memory Size: 4096 MB Max Memory Used: 1009 MB Init Duration: 391.81 ms 
XRAY TraceId: 1-6ae27faf-7a45bb820fe656577eb4bc4d SegmentId: f4
- `15:38:13`    2026-10-29T23:08 c-a0f33d62ef93 Duration: 205848.05 ms Billed Duration: 206235 ms Memory Size: 4096 MB Max Memory Used: 1004 MB Init Duration: 386.64 ms 
XRAY TraceId: 1-6ae3d12f-36ea19d75e10d8486dbdf02e SegmentId: bb
- `15:38:13`    2026-10-30T23:08 5-e5ff1dd5486a Duration: 207499.42 ms Billed Duration: 207924 ms Memory Size: 4096 MB Max Memory Used: 1018 MB Init Duration: 424.28 ms 
XRAY TraceId: 1-6ae522af-780254026c8b105f5b270640 SegmentId: 95
- `15:38:13`    2026-10-31T23:08 8-5f8ce7de748c Duration: 200150.21 ms Billed Duration: 200566 ms Memory Size: 4096 MB Max Memory Used: 1045 MB Init Duration: 415.21 ms 
XRAY TraceId: 1-6ae6742f-503e852b71763e027943e5bd SegmentId: a5
- `15:38:13` ✅ ops 6708 complete (read-only)
