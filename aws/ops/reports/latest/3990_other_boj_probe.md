# ops 3990 — probe: OTHER decomposition + missing-BOJ forensics

**Status:** success  
**Duration:** 2.0s  
**Finished:** 2026-07-27T22:33:58+00:00  

## Data

| distinct_other_srcs | families | ledger_n | marker | n_total_indexed | note | vault_rows_in_ledger |
|---|---|---|---|---|---|---|
|  |  | 150000 |  | 250110 |  |  |
|  | {"FLEET-INTERNAL": 7084, "OTHER": 3139, "FMP": 1251, "POLYGON": 696, "FRED": 488, "CFTC": 95, "ECB": 37, "COINMETRICS": 28, "YAHOO": 20, "US-TREASURY": 7, "MOEA-TAIWAN": 4, "SEC-EDGAR": 3, "BOE": 2, "GNEWS": 1, "PBOC": 1} |  | data-census v1.8 ops3987 clean-values |  |  |  |
| 135 |  |  |  |  |  |  |
|  |  |  |  |  | ledger is named-first capped 150k — vault rows have no name sibling, so absence HERE does not mean absence from the in-memory by_source pool; section C is the truth for that | 167 |

## Log
## A. what is inside OTHER — top source strings

- `22:33:58`     345  Benzinga (via Massive) — actual results
- `22:33:58`     253  seed
- `22:33:58`     211  sec
- `22:33:58`     128  discovered
- `22:33:58`     110  earnings_pead
- `22:33:58`     110  short_squeeze
- `22:33:58`      77  live_breakout
- `22:33:58`      76  figi
- `22:33:58`      36  port throughput (physical)
- `22:33:58`      10  OFR
- `22:33:58`      10  HKMA
- `22:33:58`       9  magdist
- `22:33:58`       8  edge
- `22:33:58`       8  bank-of-japan
- `22:33:58`       8  coingecko
- `22:33:58`       7  cad_xlsx(edge)
- `22:33:58`       6  owned
- `22:33:58`       6  unresolved_futures
- `22:33:58`       6  w52_baseline
- `22:33:58`       6  opportunity
- `22:33:58`       5  risk-monitor
- `22:33:58`       5  factor-risk
- `22:33:58`       5  firm-stress
- `22:33:58`       5  firm-book + desk-allocator
- `22:33:58`       5  merger-arb-risk
- `22:33:58`       5  pnl-attribution
- `22:33:58`       4  liquidity-capacity
- `22:33:58`       4  unresolved_tv_only
- `22:33:58`       4  repo-market engine
- `22:33:58`       3  ICSA
- `22:33:58`       3  BAMLH0A3HYC−BAMLH0A1HYBB
- `22:33:58`       3  WLRRAFOIAL
- `22:33:58`       3  eurodollar-plumbing
- `22:33:58`       3  global-sovereign
- `22:33:58`       3  crisis-plumbing
- `22:33:58`       3  credit-stress
- `22:33:58`       3  regime-composite
- `22:33:58`       3  vol-surface
- `22:33:58`       3  global-liquidity
- `22:33:58`       3  leading-markets
## B. the vault's BOJ/MOF/NORGES/BCRP rows — what the walk saw

- `22:33:58`   JPLG: 8 ledger entries
- `22:33:58`       k=data/domain-barometers.json p=symbols[JPLG].polarity v=1.0 leaf=polarity src=bank-of-japan name=None
- `22:33:58`       k=data/domain-barometers.json p=symbols[JPLG].value v=7.07 leaf=value src=bank-of-japan name=None
- `22:33:58`       k=data/domain-barometers.json p=symbols[JPLG].chg_pct v=0.44 leaf=chg_pct src=bank-of-japan name=None
- `22:33:58`   JP02Y: 0 ledger entries
- `22:33:58`   NO03Y: 0 ledger entries
- `22:33:58`   PETOT: 0 ledger entries
- `22:33:58`   EUBUND: 0 ledger entries
- `22:33:58`   US10Y: 10 ledger entries
- `22:33:58`       k=data/data-census.json p=by_source.FRED.metrics[US10Y].value v=-1.0 leaf=value src=None name=US10Y
- `22:33:58`       k=data/market-tape.json p=items[US10Y].value v=4.69 leaf=value src=None name=US10Y
- `22:33:58`       k=data/domain-barometers.json p=symbols[US10Y].polarity v=-1.0 leaf=polarity src=fred_alias:DGS10 name=None
- `22:33:58`   SOFR: 12 ledger entries
- `22:33:58`       k=data/ka-config.json p=metrics[SOFR].weight v=9.0 leaf=weight src=fred name=SOFR Rate
- `22:33:58`       k=data/khalid-config.json p=metrics[SOFR].weight v=9.0 leaf=weight src=fred name=SOFR Rate
- `22:33:58`       k=data/data-census.json p=by_source.FRED.metrics[SOFR].value v=0.7419 leaf=value src=None name=SOFR
## C. are they in by_source anywhere?

- `22:33:58`   JPLG: [('OTHER', 7.07, 'bank-of-japan')]
- `22:33:58`   NO03Y: ABSENT from every family
- `22:33:58`   PETOT: ABSENT from every family
- `22:33:58`   JP02Y: ABSENT from every family
## D. ledger truncation check

- `22:33:58` ✅ PROBE DONE — evidence recorded for the FAMS patch
