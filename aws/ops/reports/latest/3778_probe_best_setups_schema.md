# ops 3778 — probe best-setups.json (no code written)

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-07-23T19:46:01+00:00  

## Data

| capture_names | expected_join | ticker_field | use_key |
|---|---|---|---|
| 1771 |  |  |  |
|  | 47 | ticker | top_setups |

## Log
## Top-level keys of the LIVE artifact

- `19:46:01`   schema_version                     str    3
- `19:46:01`   engine                             str    32
- `19:46:01`   generated_at                       str    25
- `19:46:01`   duration_s                         float  7.6
- `19:46:01`   weight_source                      str    10
- `19:46:01`   bond_vol_regime                    dict   4
- `19:46:01`   nowcast_regime                     dict   5
- `19:46:01`   methodology                        str    317
- `19:46:01`   playbook_context                   list   3
- `19:46:01`   stats                              dict   4
- `19:46:01`   top_setups                         list   50
- `19:46:01`   quad_threats                       list   0
- `19:46:01`   triple_threats                     list   1
- `19:46:01`   buildout_threats                   list   0
- `19:46:01`   brain_aligned                      list   0
- `19:46:01`   structural_chokepoints             list   30
- `19:46:01`   contested_picks                    list   0
- `19:46:01`   picks_with_kill_thesis             list   0
- `19:46:01`   lead_lag_tailwinds                 list   4
- `19:46:01`   meta_confluence_book               list   16
- `19:46:01`   resilient_setups                   list   3
- `19:46:01`   synth_aligned_bullish              list   20
- `19:46:01`   synth_conflicted                   list   17
- `19:46:01`   synthesizer_wiring                 dict   5
- `19:46:01`   orphan_meta_wiring                 dict   4
- `19:46:01`   alpha_trust_wiring                 dict   6
- `19:46:01`   meta_intelligence                  dict   6
- `19:46:01`   structural_at_trough               list   6
- `19:46:01`   by_verdict                         dict   4
- `19:46:01`   industry_context                   dict   3
## Which top-level keys are LISTS OF SETUP-LIKE DICTS?

- `19:46:01`   playbook_context               n=3     ticker_field=symbol   keys=['family', 'id', 'symbol', 'text']
- `19:46:01`   top_setups                     n=50    ticker_field=ticker   keys=['asia_flash_tailwind', 'brain_aligned', 'buildout_threat', 'confluence_mult', 'conviction', 'criticality', 'cycle_flag', 'cycle_phase', 'cycle_warning', 'earnings_date']
- `19:46:01`   triple_threats                 n=1     ticker_field=ticker   keys=['asia_flash_tailwind', 'brain_aligned', 'buildout_threat', 'confluence_mult', 'conviction', 'criticality', 'cycle_flag', 'cycle_phase', 'cycle_warning', 'earnings_date']
- `19:46:01`   structural_chokepoints         n=30    ticker_field=ticker   keys=['asia_flash_tailwind', 'brain_aligned', 'buildout_threat', 'confluence_mult', 'conviction', 'criticality', 'cycle_flag', 'cycle_phase', 'cycle_warning', 'earnings_date']
- `19:46:01`   lead_lag_tailwinds             n=4     ticker_field=ticker   keys=['conviction', 'lead_lag', 'ticker']
- `19:46:01`   meta_confluence_book           n=16    ticker_field=ticker   keys=['conviction', 'meta_confluence', 'ticker']
- `19:46:01`   resilient_setups               n=3     ticker_field=ticker   keys=['conviction', 'resilience', 'ticker']
- `19:46:01`   synth_aligned_bullish          n=20    ticker_field=ticker   keys=['conviction', 'flow', 'options', 'ticker']
- `19:46:01`   synth_conflicted               n=17    ticker_field=ticker   keys=['conviction', 'flow', 'options', 'ticker']
- `19:46:01`   structural_at_trough           n=6     ticker_field=ticker   keys=['asia_flash_tailwind', 'brain_aligned', 'buildout_threat', 'census_context', 'confluence_mult', 'conviction', 'criticality', 'cycle_flag', 'cycle_phase', 'cycle_warning']
- `19:46:01` ✅ PROBE.found_lists :: 10 list-of-dict keys
## True overlap vs capture_gap.all_rows

- `19:46:01`   playbook_context               n=3     overlap=0     ([])
- `19:46:01`   top_setups                     n=50    overlap=47    (['ABBV', 'ABNB', 'ADBE', 'AEG', 'AKAM', 'ALB'])
- `19:46:01`   triple_threats                 n=1     overlap=1     (['TSM'])
- `19:46:01`   structural_chokepoints         n=30    overlap=30    (['ACLS', 'ADBE', 'AMAT', 'AMD', 'ANET', 'APH'])
- `19:46:01`   lead_lag_tailwinds             n=4     overlap=4     (['BA', 'ETN', 'META', 'NRG'])
- `19:46:01`   meta_confluence_book           n=16    overlap=16    (['APA', 'BAC', 'BSX', 'CAT', 'EXE', 'INTC'])
- `19:46:01`   resilient_setups               n=3     overlap=3     (['LLY', 'ROIV', 'TRV'])
- `19:46:01`   synth_aligned_bullish          n=20    overlap=20    (['AAPL', 'ABT', 'ASML', 'AVGO', 'BAC', 'CVX'])
- `19:46:01`   synth_conflicted               n=17    overlap=16    (['AMZN', 'ARM', 'BA', 'BABA', 'COIN', 'F'])
- `19:46:01`   structural_at_trough           n=6     overlap=6     (['ACLS', 'AMAT', 'CRM', 'LRCX', 'STX', 'WDC'])
- `19:46:01` ✅ PROBE.overlap_found :: best key 'top_setups' (field 'ticker') overlaps 47 names
## VERDICT — what the wiring ops must use

- `19:46:01`   Wire against bs['top_setups'], ticker field 'ticker', expect 47 joins.
- `19:46:01`   NOTE: if the producer builds this list under a different in-code
- `19:46:01`   variable name, the splice must target THAT variable, not the
- `19:46:01`   output key — grep the producer before writing the overlay.
- `19:46:01` ✅ PASS_ALL — schema probed, wiring target identified
