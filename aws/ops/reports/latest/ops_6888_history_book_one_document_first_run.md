# ops 6888 - history book 1.3.0: dry run, first run and read-back of the one-document block, 2026-11-21T19:23+00:00

**Status:** success  
**Duration:** 209.8s  
**Finished:** 2026-11-21T19:27:13+00:00  

## Data

| complete | documents | failed | families_compared | families_equal | kicks | one_family_blocks_left | second_pass_would_write | version | wall_s | written |
|---|---|---|---|---|---|---|---|---|---|---|
| True | 27127 | 0 | 333 | 333 | 1 | 0 | 0 | 1.3.0 | 210 | 27127 |

## Log
## 1. Dry run on live data (nothing is written)

- `19:23:44` justhodl-bank-histories: last updated 2026-11-21T19:22:52.000+0000, code u3vef4ksBzLgVCBA..., 3008 MB, timeout 900 s
- `19:23:44` book before: manifest version 1.2.0, phase STEADY, 27 blocks, one-family snapshot blocks 9, combined block absent
- `19:24:05` dry run of snapshots/latest: version 1.3.0, would write 27127, would leave alone 0, 19.9 s
- `19:24:05`   block: {"symbols": 27127, "written": 27127, "unchanged": 0, "changed": 0, "new": 27127, "migrated": 0, "seconds": 19.2, "checked": "read in full"} | notes []
- `19:24:05` ✅ version 1.3.0 answers; its dry run built 27127 documents (27127 new, 0 changed, 0 the same as stored) and left the state file as it was
## 2. The first run of the block

- `19:24:05` kick 1 -> 202
- `19:26:23` run 1: 129.1 s | documents 27127, written 27127 (new 27127, changed 0), left alone 0, failed 0, 62.6 MB | complete | notes ['retired 9 one-family snapshot blocks: snapshots/latest holds them']
- `19:26:23` after 1 kick(s): manifest version 1.3.0; combined block listed with 27127 symbols, families 9; one-family blocks still listed 0; retired record: {"at": "2026-11-21T19:24:06.023175+00:00", "by": "1.3.0", "blocks": 9, "superseded_by": "snapshots/latest"}
- `19:26:23` ✅ the combined block is complete and the nine one-family blocks are off the manifest
## 3. Read back: the new document against the nine it replaces

- `19:26:24` 27127 documents in the block's hash state; reading 60
- `19:27:07` families compared with their one-family document: 333, equal row for row: 333; documents with every family equal: 60 of 60
- `19:27:07` families present in the sampled documents: {"dcf-bulk": 27, "key-metrics-ttm-bulk": 38, "peers-bulk": 56, "price-target-summary-bulk": 19, "profile-bulk": 60, "rating-bulk": 32, "ratios-ttm-bulk": 38, "scores-bulk": 37, "upgrades-downgrades-consensus-bulk": 26}
- `19:27:07` AAPL: as_of 2026-11-20T12:02:55.733758+00:00, 9 families, n 9: {'profile-bulk': 1, 'ratios-ttm-bulk': 1, 'key-metrics-ttm-bulk': 1, 'scores-bulk': 1, 'rating-bulk': 1, 'dcf-bulk': 1, 'price-target-summary-bulk': 1, 'upgrades-downgrades-consensus-bulk': 1, 'peers-bulk': 1}
- `19:27:07` ✅ 333 of 333 families read back equal to the documents they replace
## 4. The reader the engines carry, against the live bucket

- `19:27:07` layout per family: {"dcf-bulk": ["combined"], "key-metrics-ttm-bulk": ["combined"], "peers-bulk": ["combined"], "price-target-summary-bulk": ["combined"], "profile-bulk": ["combined"], "rating-bulk": ["combined"], "ratios-ttm-bulk": ["combined"], "scores-bulk": ["combined"], "upgrades-downgrades-consensus-bulk": ["combined"]}
- `19:27:12` fmp_bank.snapshot_doc for 163 (symbol, family) pairs: 163 returned the combined document's rows; reader counters {"s3_gets": 26}
- `19:27:12` fmp_bank.snapshot('profile', 'AAPL'): 1 row(s), companyName Apple Inc., price 349.21
- `19:27:12` ✅ the reader takes every family from the combined document
## 5. A second pass with nothing new

- `19:27:13` dry run again: would write 0, left alone 27127, block checked as 'inputs unchanged', 0.9 s
- `19:27:13` ✅ with nothing new in the bank the block has nothing to write
