# Retire 4,377 legacy correct=None outcomes

**Status:** success  
**Duration:** 51.1s  
**Finished:** 2026-04-25T20:31:47+00:00  

## Data

| n_correct_false | n_correct_none | n_correct_true | n_outcomes_total | n_tagged | post_fix_legacy | pre_fix_legacy |
|---|---|---|---|---|---|---|
| 0 | 4410 | 0 | 4410 | 0 | 103 | 4307 |

## Log
## A. How does calibrator handle correct=None?

- `20:30:56`   Calibrator source mentions correct field handling
- `20:30:56`     for o in outcomes if o.get("correct
- `20:30:56`     for o in outcomes if o.get("correct
- `20:30:56`     for o in up_preds if o.get("correct
- `20:30:56`     for o in down_preds if o.get("correct
## B. Count legacy outcomes (correct is None)

- `20:30:56`   Scanned 2 pages of outcomes
- `20:30:56`   correct=True:  0
- `20:30:56`   correct=False: 0
- `20:30:56`   correct=None:  4410  ← legacy
- `20:30:56` 
  Legacy by signal_type:
- `20:30:56`     screener_top_pick              955
- `20:30:56`     crypto_fear_greed              444
- `20:30:56`     crypto_risk_score              444
- `20:30:56`     khalid_index                   336
- `20:30:56`     ml_risk                        336
- `20:30:56`     plumbing_stress                336
- `20:30:56`     momentum_uso                   303
- `20:30:56`     edge_composite                 275
- `20:30:56`     momentum_gld                   202
- `20:30:56`     market_phase                   183
- `20:30:56`     carry_risk                     183
- `20:30:56`     edge_regime                    183
- `20:30:56`     momentum_spy                   142
- `20:30:56`     momentum_tlt                   79
- `20:30:56`     momentum_uup                   9
## C. Verify legacy records are from before Apr 24

- `20:30:56`   Pre-fix (before 2026-04-24T23:25:16): 4307
- `20:30:56`   Post-fix (after):             103
- `20:30:56` ⚠   ⚠ 103 null outcomes scored AFTER the signal-logger fix
- `20:30:56` ⚠   Possible causes:
- `20:30:56` ⚠     1. Some signal types still don\'t set baseline (audit needed)
- `20:30:56` ⚠     2. Signals logged before fix but scored after (legacy)
- `20:30:56`       type=momentum_gld checked_at=2026-04-25T09:41:57.840179+00:00 sid=373ae5fb-f53c-4a75-a...
- `20:30:56`       type=screener_top_pick checked_at=2026-04-25T20:27:54.835714+00:00 sid=b4b1bdb1-d435-4b9e-8...
- `20:30:56`       type=screener_top_pick checked_at=2026-04-25T20:27:54.835714+00:00 sid=351eb33a-c4e7-4bb7-b...
## D. Tag legacy outcomes for cleanup (DRY-RUN preview only)

- `20:30:56`   Would tag 4410 outcomes with:
- `20:30:56`     is_legacy: true
- `20:30:56`     legacy_reason: 'pre_baseline_fix_2026_04_24'
- `20:30:56`     ttl: now + 30 days (auto-purge)
- `20:30:56`   Cost estimate: 4,377 × 1 WCU = ~4.4k WCU (~\$0.005)
- `20:30:56`   Time estimate: ~22 sec at 200 WCU/sec batched
## E. Actually tag the legacy records

- `20:30:56` ⚠     Failed fdfa64fe-acef-44f9-8: An error occurred (ValidationException) when calling the UpdateItem operation: Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: ttl
- `20:30:56` ⚠     Failed c2447ef6-3f05-43ee-9: An error occurred (ValidationException) when calling the UpdateItem operation: Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: ttl
- `20:30:56` ⚠     Failed 035715a5-ba9e-44ad-8: An error occurred (ValidationException) when calling the UpdateItem operation: Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: ttl
- `20:30:56` ⚠     Failed f652cc71-b463-47f8-b: An error occurred (ValidationException) when calling the UpdateItem operation: Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: ttl
- `20:31:47` 
  Tagged 0, failed 4410
- `20:31:47` Done
