# ops 6692 - universe-builder v4.1.0: a bank-built universe (dry run, force_bank) vs the live publication

**Status:** success  
**Duration:** 12.7s  
**Finished:** 2026-10-27T18:48:11+00:00  

## Log
- `18:47:58` deployed: 512 MB / 420 s, LastModified 2026-10-27T18:46:53.000+0000, description v4.1 all-US-listed universe: FMP screener with mcap bisection; a failed range is filled fr
## verdict

- `18:48:11` ✅ would_publish True; violations []
- `18:48:11` total 5078 | by bucket {"nano": 957, "micro": 860, "small": 1274, "mid": 1092, "large": 821, "mega": 74, "unknown": 0} | by source {"bank_profile_bulk": 5072, "curated_seed": 6} | 11.6s
- `18:48:11` bank: status read, as_of 2026-10-25T23:13:14+00:00, age 1.82 d, rows 6670, in the failed spans 6670, dropped as not common 1472, book {"status": "read", "key": "data/warm/factor-book/universe.json.gz", "generated_at": "2026-10-27T02:50:45.023172+00:00", "symbols": 27215}
## vs the live publication

- `18:48:11` live 5230 (generated 2026-10-27T18:06:31+00:00) vs bank-built 5078: both 4967 (94.97% of live), only live 263, only bank 111, same cap bucket 4876 of 4967
- `18:48:11` only live (sample): ['AAC', 'AEAQW', 'AETR', 'AFGB', 'AFGC', 'AFGD', 'AFGE', 'AGMB', 'AIZN', 'ALIS', 'ALOVW', 'AMAC', 'AMAN', 'ANNAW', 'AOMN', 'AQNB', 'ARCIW', 'ARTC', 'ASBA', 'ASPSW', 'ASPSZ', 'ASTS', 'ATHS', 'ATLQ', 'AXINR', 'BACCR', 'BCPC', 'BCTXL', 'BEPH', 'BEPI', 'BEPJ', 'BIPH', 'BIPI', 'BIVIW', 'BIXIW', 'BLRK', 'BMNP', 'BREZ', 'BRTMW', 'BUSEP']
- `18:48:11` only bank (sample): ['ACA', 'ACGLN', 'ADAMG', 'ADAMH', 'ADAMI', 'ADAMO', 'AGM-A', 'AGNCL', 'AGNCO', 'AKO-B', 'ASBP', 'ASTLW', 'ATLCL', 'ATLCZ', 'BELFA', 'BF-A', 'BH-A', 'BHFAL', 'BHFAM', 'BHFAN', 'BHFAO', 'BIO-B', 'BPYPN', 'BRK-B', 'BRVE', 'BWBBP', 'BZH', 'CENTA', 'CNOBP', 'CRD-B', 'DBGI', 'DGICB', 'DHCNI', 'DHCNL', 'DRK', 'EDVA', 'FBDT', 'FJDI', 'FRMEP', 'FULTP']
## the run

- `18:48:11`    [universe] v4.1.0 starting — exhaustive all-US-listed (bisection + ADRs) [dry_run] [force_bank]
- `18:48:11`    [universe] 6 screener range(s) failed - filling those market-cap spans from the FMP bank's profile-bulk
- `18:48:11`    [universe] bank fill: {"status": "read", "as_of": "2026-10-25T23:13:14+00:00", "age_days": 1.82, "rows": 6670, "in_failed_spans": 6670, "dropped_not_common": 1472, "error": null}
- `18:48:11`    [universe] combined: 5198 unique symbols (5198 from the bank)
- `18:48:11`    [universe] valid: 5072 (filtered industry: 122, symbol: 0, other: 4)
- `18:48:11`    [universe] curated seed missing from screener: 6 of 97
- `18:48:11`    [universe] curated seed added: 6
- `18:48:11`    [universe] dry run (nothing written): {"violations": [], "by_bucket": {"nano": 957, "micro": 860, "small": 1274, "mid": 1092, "large": 821, "mega": 74, "unknown": 0}, "by_source": {"bank_profile_bulk": 5072, "curated_seed": 6}, "compare": {"previous_total": 5230, "new_total": 5078, "both": 4967, "both_pct_of_previous": 94.97, "only_previous": 263, "only_new": 111, "same_bucket": 4876, "previous_ge
- `18:48:11`    REPORT RequestId: 6b89b6e6-cfcd-4621-a58e-f7bfe2c9f112 Duration: 11626.89 ms Billed Duration: 12129 ms Memory Size: 512 MB Max Memory Used: 145 MB Init Duration: 501.21 ms 
- `18:48:11` ✅ ops 6692 complete (nothing written by the builder)
