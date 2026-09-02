# ops 5109 -- probe replacements for the dead FRED ids / NY Fed endpoint (read-only)

**Status:** success  
**Duration:** 34.5s  
**Finished:** 2026-09-02T02:25:56+00:00  

## Data

| dead | engine | live_2026 |
|---|---|---|
| WCBSL | plumbing-aggregator | SWPT |
| DRTSCLM | plumbing-aggregator | DRTSCILM,DRTSCLCC,SUBLPDRCSM |
| WGRESUS | plumbing-aggregator | WMTSECL1,WSHOMCB |
| OTHL1690 | repo-monitor | OTHL1690,WLCFLPCL,WORAL |
| SOFR25 | repo-monitor | NONE |
| SOFR75 | repo-monitor | NONE |
| USD3MTD156N | repo-monitor | NONE |
| WDTGAL | repo-monitor | NONE |
| WLCFLPCL | repo-monitor | NONE |
| SWPT | repo-monitor | NONE |
| DALLASFEDFAB | manufacturing-global-agent | NONE |
| KCLFEDFAB | manufacturing-global-agent | NONE |
| CHEFMNM156N | manufacturing-global-agent | NONE |
| GAFDIMSA | manufacturing-global-agent | NONE |
| GAPHDFBA | manufacturing-global-agent | NONE |
| RMTSPL | manufacturing-global-agent | NONE |
| NAPMEI | manufacturing-global-agent | NONE |
| NAPMII | manufacturing-global-agent | NONE |
| EA19PRMNTO01IXOBM | manufacturing-global-agent | NONE |
| JPNPRMNTO01IXOBM | manufacturing-global-agent | NONE |
| GBRPRMNTO01IXOBM | manufacturing-global-agent | NONE |
| DEUPRMNTO01IXOBM | manufacturing-global-agent | NONE |

## Log
## plumbing-aggregator

- `02:25:23` WCBSL: SWPT->200 ('2026-08-26', '121') Assets: Central Bank Liquidity Swaps: Central Bank | WCBSL->400 None 
- `02:25:28` DRTSCLM: DRTSCLM->400 None  | DRTSCILM->200 ('2026-07-01', '0.0000000000') Net Percentage of Domestic Banks Tightening Standa | DRTSCLCC->200 ('2026-07-01', '6.7000000000') Net Percentage of Domestic Banks Tightening Standa | SUBLPDRCSM->200 ('2026-07-01', '-5.7') Net Percentage of Domestic Banks Tightening Standa | DRTSPM->200 ('2014-10-01', '-11.1') Net Percentage of Domestic Banks Tightening Standa
- `02:25:31` WGRESUS: WGRESUS->400 None  | WMTSECL1->200 ('2026-08-26', '2615246') Memorandum Items: Custody Holdings: Marketable U.S | WSHOMCB->200 ('2026-08-26', '1913585') Assets: Securities Held Outright: Mortgage-Backed  | FDHBFIN->200 ('2025-10-01', '9270.9') Federal Debt Held by Foreign and International Inv
## repo-monitor

- `02:25:33` OTHL1690: OTHL1690->200 ('2026-08-26', '2568') Assets: Liquidity and Credit Facilities: Loans: Ma | WLCFLPCL->200 ('2026-08-26', '4890') Assets: Liquidity and Credit Facilities: Loans: Pr | WORAL->200 ('2026-08-26', '3') 
- `02:25:35` SOFR25: SOFR25->429 None  | SOFR1->429 None  | SOFR99->429 None 
- `02:25:37` SOFR75: SOFR75->429 None  | SOFR99->429 None 
- `02:25:38` USD3MTD156N: USD3MTD156N->429 None  | SOFR90DAYAVG->429 None  | TSFR3M->429 None 
- `02:25:40` WDTGAL: WDTGAL->429 None  | WTREGEN->429 None 
- `02:25:41` WLCFLPCL: WLCFLPCL->429 None 
- `02:25:41` SWPT: SWPT->429 None 
## manufacturing-global-agent

- `02:25:43` DALLASFEDFAB: DALLASFEDFAB->429 None  | BACTSAMFRBDAL->429 None  | PROSAMFRBDAL->429 None 
- `02:25:46` KCLFEDFAB: KCLFEDFAB->429 None  | KCFMCI->429 None  | COMPRMTSAMFRBKC->429 None 
- `02:25:47` CHEFMNM156N: CHEFMNM156N->429 None  | CHNPRMNTO01IXOBM->429 None 
- `02:25:48` GAFDIMSA: GAFDIMSA->429 None  | GACDISA066MSFRBNY->429 None 
- `02:25:49` GAPHDFBA: GAPHDFBA->429 None  | GACDFSA066MSFRBPHI->429 None 
- `02:25:50` RMTSPL: RMTSPL->429 None 
- `02:25:50` NAPMEI: NAPMEI->429 None 
- `02:25:51` NAPMII: NAPMII->429 None 
- `02:25:52` EA19PRMNTO01IXOBM: EA19PRMNTO01IXOBM->429 None  | EA20PRMNTO01IXOBM->429 None 
- `02:25:53` JPNPRMNTO01IXOBM: JPNPRMNTO01IXOBM->429 None 
- `02:25:53` GBRPRMNTO01IXOBM: GBRPRMNTO01IXOBM->429 None 
- `02:25:54` DEUPRMNTO01IXOBM: DEUPRMNTO01IXOBM->429 None 
## nyfed endpoints

- `02:25:54` https://markets.newyorkfed.org/api/rp/srf/results/latest.json: HTTP 400
- `02:25:55` https://markets.newyorkfed.org/api/rp/all/all/results/latest.json: 200 2282B { "repo": { "operations": [ { "operationId": "RP 090126 25", "auctionStatus": "Results", "operationDate": "2026-09-01", "settlementDate": "2026-09-01", "maturityDate": "2026-09-02", "operationType": "Repo", "operationMet
- `02:25:55` https://markets.newyorkfed.org/api/rp/repo/all/results/latest.json: 200 1628B { "repo": { "operations": [ { "operationId": "RP 090126 25", "auctionStatus": "Results", "operationDate": "2026-09-01", "settlementDate": "2026-09-01", "maturityDate": "2026-09-02", "operationType": "Repo", "operationMet
- `02:25:55` https://markets.newyorkfed.org/api/rp/repo/all/results/last/1.json: 200 830B { "repo": { "operations": [ { "operationId": "RP 090126 25", "auctionStatus": "Results", "operationDate": "2026-09-01", "settlementDate": "2026-09-01", "maturityDate": "2026-09-02", "operationType": "Repo", "operationMet
- `02:25:55` https://markets.newyorkfed.org/api/rp/all/all/results/last/3.json: 200 2282B { "repo": { "operations": [ { "operationId": "RP 090126 25", "auctionStatus": "Results", "operationDate": "2026-09-01", "settlementDate": "2026-09-01", "maturityDate": "2026-09-02", "operationType": "Repo", "operationMet
- `02:25:56` ✅ probe written to data/audit/fred-id-probe-5109.json
