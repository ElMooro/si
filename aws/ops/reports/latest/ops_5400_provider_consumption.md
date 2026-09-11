# ops 5400 -- provider consumption / cancel map

**Status:** success  
**Duration:** 13.4s  
**Finished:** 2026-09-11T18:36:55+00:00  

## Data

| ds | hot | refs | slug | verdict |
|---|---|---|---|---|
| 8195 | 43 | 12 | eurostat | KEEP |
| 402987 | 7 | 12 | gdelt | KEEP |
| 1686 | 8 | 12 | bls | KEEP |
| 8271 | 5 | 12 | statcan | KEEP |
| 280692 | 282 | 12 | fred | KEEP |
| 51 | 0 | 9 | sec-midas | KEEP |
| 73 | 0 | 5 | sec-dera | KEEP |
| 759 | 76 | 12 | ecb | KEEP |
| 471 | 29 | 12 | imf | KEEP |
| 2 | 0 | 8 | sec-bulk | KEEP |
| 1184 | 5 | 12 | oecd | KEEP |
| 58968 | 26 | 12 | worldbank | KEEP |
| 513 | 0 | 6 | nyfed-research | KEEP |
| 10 | 0 | 12 | finra | KEEP |
| 1547 | 254 | 12 | polygon | KEEP |
| 6 | 4 | 9 | gleif | KEEP |
| 139 | 0 | 4 | sec-edgar | KEEP |
| 74 | 4 | 8 | eiopa | KEEP |
| 78 | 5 | 12 | dol | KEEP |
| 74 | 25 | 12 | boe | KEEP |
| 43 | 17 | 2 | fed-board | KEEP |
| 17076 | 24 | 12 | boj | KEEP |
| 84 | 41 | 12 | treasury | KEEP |
| 58 | 27 | 12 | bis | KEEP |
| 160 | 0 | 7 | te-mirror | CANCEL_CANDIDATE |
| 359 | 0 | 12 | census-us | KEEP |
| 66 | 66 | 12 | yahoo | CANCEL_CANDIDATE |
| 32 | 29 | 12 | cftc | KEEP |
| 24 | 24 | 12 | snb | KEEP |
| 22 | 22 | 12 | bcb | KEEP |
| 152 | 152 | 12 | other | KEEP |
| 1946 | 39 | 12 | nyfed | KEEP |
| 193 | 4 | 12 | bea | KEEP |
| 482 | 35 | 12 | ofr | KEEP |
| 15 | 0 | 1 | cl-datos | ORPHAN |
| 35 | 0 | 9 | official-yields | KEEP |
| 501 | 0 | 9 | ofr-bsrm | CANCEL_CANDIDATE |
| 498 | 0 | 9 | ofr-hfm | KEEP |
| 6 | 0 | 6 | ofr-site | KEEP |
| 1 | 0 | 12 | te-feed | CANCEL_CANDIDATE |
| 82 | 0 | 1 | hk-data | ORPHAN |
| 2 | 0 | 12 | ofr-fsi | KEEP |
| 11 | 11 | 12 | cboe | KEEP |
| 10 | 0 | 12 | tic | KEEP |
| 15 | 15 | 12 | dbnomics | CANCEL_CANDIDATE |
| 5 | 4 | 8 | banxico | KEEP |
| 11 | 11 | 12 | coinmetrics | KEEP |
| 1 | 0 | 4 | chicagofed | KEEP |
| 1 | 0 | 1 | clevelandfed | ORPHAN |
| 2 | 0 | 1 | atlantafed | ORPHAN |
| 5 | 4 | 9 | nasa | KEEP |
| 4 | 4 | 12 | occ | KEEP |
| 1 | 0 | 12 | taiwan-moea | KEEP |
| 1 | 0 | 12 | peru-copper | KEEP |
| 0 | 0 | 1 | kr-ecos | CANCEL_CANDIDATE |
| 18847 | 0 | 12 | indicator-bus | KEEP |
| 6517 | 0 | 0 | equity-research-tickers | ORPHAN |
| 5575 | 0 | 7 | tradingview-vault-live | KEEP |

## Log
- `18:36:42` catalog as_of=2026-09-11T17:48:53+00:00 providers=58
- `18:36:52` lambda functions listed=888
- `18:36:54` repo files scanned=3740
## verdict

- `18:36:55` KEEP=47 ORPHAN=5 CANCEL/DEAD=6
- `18:36:55` cancel/dead: te-mirror, yahoo, ofr-bsrm, te-feed, dbnomics, kr-ecos
- `18:36:55` orphans (first 20): cl-datos, hk-data, clevelandfed, atlantafed, equity-research-tickers
- `18:36:55` ✅ GREEN -- data/provider-consumption.json written and read back
