# ops 4289 -- census v3 + attribution widened

**Status:** success  
**Duration:** 6.9s  
**Finished:** 2026-08-02T21:03:03+00:00  

## Log
## A. put-proximity writer map

- `21:02:57` ✅ v3 writer map: 675 artifacts with proven put-writers (engines: 772)
- `21:02:57` morning-intel true writer under v3: ORPHAN CONFIRMED (no put anywhere)
- `21:03:01` ✅ FINAL dormant ledger (n=60): {'ALIVE': 44, 'LAZY_EVENT': 1, 'ORPHAN': 12, 'STILL_DORMANT': 3}
- `21:03:01` orphans (no put-writer exists; reader-blamed before): ['analyst-consensus-history.json', 'commodity-curves-history.json', 'etf-flows/event-study.json', 'history/causality-discoveries-history.json', 'history/convexity-scores-history.json', 'history/meta-improver-history.json', 'history/pre-disaster-history.json', 'morning-intel.json']
- `21:03:01` still dormant with real writers: ['_telegram-chat.json', 'engine-robustness.json', 'transcripts-index.json']
- `21:03:02` ✅ both census docs on v3 writers (69 atlas rows corrected)
## B. attribution v1.3, measured

- `21:03:03` invoked: {"statusCode": 200, "body": "{\"ok\": true, \"n_quiver\": 422, \"n_house\": 336, \"n_senate\": 86, \"n_tickers\": 207, \"n_clusters\": 23, \"n_bipartisan\": 15,
- `21:03:03` ✅ attribution: 547/547 = 100% (was 64%)
## RESULT

- `21:03:03` ✅ OPS 4289 PASS -- census truthful, attribution >=80%%
