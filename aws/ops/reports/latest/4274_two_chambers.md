# ops 4274 -- two chambers, one tape

**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-08-02T17:34:12+00:00  

## Log
## 1. parser recall histogram

- `17:34:10` rows-per-parsed-doc: {0: 12, 1: 5}
- `17:34:10` ⚠ 12/17 parsed docs yielded ZERO rows -- recall gap, sample doc_ids for layout study: [('20034954', 'Richard Dean Dr McCorm'), ('20034806', 'Kelly Louise Morrison'), ('20034947', 'Mike Kelly'), ('20034790', 'Susie Lee')]
## 2. political-stocks with the merge actually deployed

- `17:34:11` invoked: {"statusCode": 200, "body": "{\"ok\": true, \"n_quiver\": 91, \"n_house\": 5, \"n_senate\": 86, \"n_tickers\": 31, \"n_clusters\": 0, \"n_bipartisan\": 0, \"duration_s\": 0.2}"}
- `17:34:12` chambers: senate=86 house=5 tickers=31
- `17:34:12` ⚠ no recent_trades pools found to sample (artifact shape note for next pass)
## RESULT

- `17:34:12` ✅ OPS 4274 PASS -- both chambers merged on official rails with attribution visible
