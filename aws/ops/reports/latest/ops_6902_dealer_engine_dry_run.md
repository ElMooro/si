# ops 6902 - the dealer engine's run from the checkout, writes kept in memory, 2026-11-22T04:19+00:00

**Status:** failure  
**Duration:** 12.2s  
**Finished:** 2026-11-22T04:19:42+00:00  

## Error

```
SystemExit: 1
```

## Data

| as_of_after | as_of_now | changed_values | published | treasury_repo_out_b | treasury_status | writes_to_s3 |
|---|---|---|---|---|---|---|
| 2026-11-11 | 2026-10-28 | 25 | True | 3398.38 | incomplete | 0 |

## Log
- `04:19:31` published now: version 4.0.0 | generated_at 2026-11-06T21:36:37.326892+00:00 | as_of 2026-10-28 | quality {"definition_unverified": 14, "fresh": 47, "incomplete": 9} | Treasury financing 2026-10-28: repo out 3339.623, reverse repo in 3001.363 (fresh)
- `04:19:31` request parts: {'observations_1': 14, 'observations_2': 14, 'observations_3': 14, 'observations_4': 14, 'observations_5': 14} | longest URL 256 characters
- `04:19:42` run(): {"published": true, "generated_at": "2026-11-22T04:19:40.259878+00:00", "replay": {"manifest_key": "data/dealer-research/runs/dbef25992abf4081e59bd2e01fae9ab50a2736efa99e4c46f97bab6e7991d8ef.json", "output_sha256": "4e6de03298834c3981cd137a5a1f33dcd5163d24c17e6702832d1a19bad72521"}, "quality": {"status": "degraded", "counts": {"definition_unverified": 14, "fresh": 49, "incomplete": 7}, "total_series": 70}, "original_series": 70, "retained_observations": 46381, "signals_emitted": 0, "paid_ai_calls": 0, "notifications_sent": 0, "private_account_reads": 0, "portfolio_writes": 0}
- `04:19:42` writes kept in memory: 13 ({'evidence/fr2004': 6, 'dealer-research/contexts': 1, 'dealer-research/inputs': 1, 'dealer-research/outputs': 1, 'dealer-research/compilers': 2, 'dealer-research/runs': 1, 'nyfed-primary-dealer.json/': 1}) | objects read from the bucket: 15
- `04:19:42` would publish: version 4.1.0 | generated_at 2026-11-22T04:19:40.259878+00:00 | source_generated_at 2026-11-22T04:19:35.854543+00:00 | as_of 2026-11-11 | quality {"counts": {"definition_unverified": 14, "fresh": 49, "incomplete": 7}, "status": "degraded", "total_series": 70}
- `04:19:42`   Treasury financing 2026-11-11: repo out 3398.38, reverse repo in None, two-sided None (incomplete; missing ['latest_report_suppressed_or_missing'])
- `04:19:42`   all-collateral financing: repo out None, reverse repo in None (definition_unverified; missing ['source_definition_unresolved', 'latest_report_suppressed_or_missing'])
- `04:19:42`   corporate bonds 2026-11-11: net 6.976 (fresh) | specific-issue net settled -7.404
- `04:19:42`   70 native series | newest report dates {'2026-11-11': 70} | observations 46381 (now published: 46241) | sources ['SBN2013', 'SBN2015', 'SBN2022', 'SBN2024', 'breaks', 'catalog', 'historical_release', 'observations_1', 'observations_2', 'observations_3', 'observations_4', 'observations_5', 'release_schedule']
- `04:19:42`   original responses named by the series: {'observations_1': 14, 'observations_2': 14, 'observations_3': 14, 'observations_4': 14, 'observations_5': 14}
- `04:19:42`   against the previous output: 25 changed values | first: [{"current": 8282, "date": "2026-10-21", "previous": 8252, "series_id": "PDPOSCS-TOT"}, {"current": 669, "date": "2026-10-21", "previous": 670, "series_id": "PDPOSCSBND-BELG10"}, {"current": 1023, "date": "2026-10-21", "previous": 1007, "series_id": "PDPOSCSBND-BELG13"}, {"current": -2084, "date": "2026-10-21", "previous": -2090, "series_id": "PDPOSCSBND-BELG5L10"}]
- `04:19:42`     PDSORA-UTSETTOT    2026-10-21 3068806 | 2026-10-28 3064193 | 2026-11-04 3166326 | 2026-11-11 3118983
- `04:19:42`     PDSORA-UTSTTOT     2026-10-21 261317 | 2026-10-28 275664 | 2026-11-04 285358 | 2026-11-11 279397
- `04:19:42`     PDSIRRA-UTSETTOT   2026-10-21 2818997 | 2026-10-28 2776340 | 2026-11-04 None | 2026-11-11 None
- `04:19:42`     PDSIRRA-UTSTTOT    2026-10-21 211880 | 2026-10-28 225842 | 2026-11-04 230199 | 2026-11-11 226721
- `04:19:42` ✗ the dry run does not produce a current Treasury financing figure
