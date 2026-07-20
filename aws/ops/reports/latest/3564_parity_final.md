- `03:11:06` FAIL  H1_matrix — {'raw_present': 0, 'stat_present': 12, 'metrics_total': 231, 'generated_at': '2026-07-20T01:49:15.276470+00:00', 'raw_missing': ['otherOpex', 'costAndExpenses', 'sellingMarketing', 'nonOpIncomeTotal', 'otherCurrentAssets', 'otherNonCurrentAssets', 'totalNonCurrentAssets', 'deferredTaxAssets'], 'stat_missing': ['price_to_book', 'price_to_cfo_ttm', 'price_to_tangible_book', 'book_value_per_share', 'tangible_bvps', 'fcf_per_share', 'roce_pct', 'rote_pct']}
- `03:11:06` PASS  H2_checklist — {'DIRECT_raw': 0, 'DIRECT_stats': 12, 'AGGREGATE_MAPPED': ['EBIT→operating income', 'impairment/restructuring→other expenses & non-op total', 'treasury stock→other equity', 'buyback yield≈net_buyback_yield (+gross added)'], 'NOT_IN_SOURCE_never_synthesized': ['PP&E by class (buildings/machinery/land/leases/…)', 'Accumulated depreciation by class', 'Inventory WIP / finished / raw splits', 'Receivables gross / bad-debt split', 'Income tax current-vs-deferred domestic/foreign', 'Interest capitalized', 'Notes payable', 'Accrued payroll', 'Dividends payable', 'Separate impairment/restructuring/legal lines', 'Free float', 'Preferred dividends paid (separate)']}
**Status:** success  
**Duration:** 1.0s  
**Finished:** 2026-07-20T03:11:06+00:00  

## Log
- `03:11:06` FAIL  H3_spots — {'AAPL_price_to_book': None, 'MSFT_roa_pct': 19.925, 'NVDA_pe_fwd': None, 'AAPL_netChangeInCash': None, 'JNJ_payout_ratio_pct': 59.52}
