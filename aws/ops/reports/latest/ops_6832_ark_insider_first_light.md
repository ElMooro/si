# ops 6832 - ARK tracker 2.1.0 + insider scanner 2.2.0, first light 2026-11-18T03:51+00:00

**Status:** success  
**Duration:** 21.2s  
**Finished:** 2026-11-18T03:52:04+00:00  

## Data

| ark_moves_listed | ark_new_day | ark_version | checks_failed | insider_lines | insider_set_aside | insider_usd_m | insider_version |
|---|---|---|---|---|---|---|---|
| 76 | True | 2.1.0 | 0 | 1195 | 2 | 1186.5 | 2.2.0 |

## Log
- `03:51:43` justhodl-ark-holdings: last modified 2026-11-18T03:47:01.000+0000, handler lambda_function.lambda_handler, 512 MB, timeout 180s, code ei0jpWi2ynMX
- `03:51:43` justhodl-insider-cluster-scanner: last modified 2026-11-18T03:50:47.000+0000, handler lambda_function.lambda_handler, 1536 MB, timeout 900s, code CSc2Trr7/46A
## ARK 1. the baseline the published diff was measured against

- `03:51:44` before: engine 2.0 (old), generated 2026-11-17T06:00:39+00:00, measured against the snapshot of 2026-11-16T06:00:38+00:00; published 72 added to, 1 cut, 0 new, 0 closed; highlights list 15 additions
- `03:51:44` stored newest snapshot: generated 2026-11-17T06:00:38+00:00, holdings dated 2026-11-16; retained baseline: none
- `03:51:44` seeded data/ark-holdings-base.json with the engine's own stored snapshot of 2026-11-16T06:00:38+00:00 (holdings dated 2026-11-13, 212 lines)
## ARK 2. one run

- `03:51:47` engine answered: {"ok": true, "n_funds": 6, "n_positions": 211, "n_unique_tickers": 117, "n_new_positions": 0, "n_adds": 68, "n_trims": 7, "n_closed": 1, "duration_s": 2.2, "version": "2.1.0", "holdings_date": "2026-11-17", "prev_holdings_date": "2026-11-16", "new_day": true, "baseline_kept": true}
- `03:51:48` holdings dated 2026-11-17, measured against 2026-11-16 (a new day at the source); the op expects the baseline dated 2026-11-16
- `03:51:48`   new_positions     engine   0, computed here   0, published count   0
- `03:51:48`   position_adds     engine  68, computed here  68, published count  68
- `03:51:48`   position_trims    engine   7, computed here   7, published count   7
- `03:51:48`   closed_positions  engine   1, computed here   1, published count   1
- `03:51:48` positions published 211 of 211; complete lists hold 76 moves where the highlights hold 23
- `03:51:48` holdings date by fund: {"ARKW": "2026-10-06", "ARKF": "2026-11-17", "ARKG": "2026-11-17", "ARKK": "2026-11-17", "ARKQ": "2026-11-17", "ARKX": "2026-11-17"}  STALE AT THE SOURCE: {"ARKW": "2026-10-06"}
- `03:51:48`   largest by dollars: ARKK SPCX +981,432 sh (+15.7%), about $134.8M
- `03:51:48`   largest by dollars: ARKK TSLA +339,720 sh (+10.3%), about $129.0M
- `03:51:48`   largest by dollars: ARKK TEM +914,264 sh (+10.3%), about $59.2M
- `03:51:48`   largest by dollars: ARKK CRCL +936,858 sh (+12.7%), about $54.0M
- `03:51:48`   largest by dollars: ARKK XYZ +585,437 sh (+16.6%), about $48.6M
## ARK 3. the same holdings again

- `03:51:50` engine answered: {"ok": true, "n_funds": 6, "n_positions": 211, "n_unique_tickers": 117, "n_new_positions": 0, "n_adds": 68, "n_trims": 7, "n_closed": 1, "duration_s": 2.1, "version": "2.1.0", "holdings_date": "2026-11-17", "prev_holdings_date": "2026-11-16", "new_day": false, "baseline_kept": true}
- `03:51:50` second run: 68 added to, 7 cut, 0 new, 1 closed - identical lists: True; baseline unchanged: True; carried: True
## INSIDER 4. one run, against the banked feed read here

- `03:51:50` before: engine 2.1.0, generated 2026-11-17T13:50:34.962494+00:00: 210 scored, 374 in the purchase list, largest LONA $9919M, NFRX $114M, CRBG $106M, ROL $100M
- `03:51:58` engine answered: {"n_clusters": 208, "n_strong": 49, "n_smart_money_dual": 17, "duration_s": 7.2, "top_5_tickers": ["UPBD", "AN", "FF", "DTI", "LRMR"]}
- `03:51:59` hygiene: 3584 feed rows -> 1195 filing lines; in range 1145, off the tape (counted) 32, no tape 16, set aside 2 ($209919161281 as filed); multi-ticker filings merged: [{"cik": "0001567892", "kept": "MNK", "also_filed_as": ["MNKKQ", "MNKPF"], "n_transactions": 4}]
- `03:51:59`   SET ASIDE LONA LeonaBio, Inc. | Portman David J. | 3,149,470 sh at 3149.47 = $9919161281 | tape 3.47 | priced at 907.6 times the tape and worth more than the whole company (72,478% of its market value)
- `03:51:59`   SET ASIDE AASP Agassi Sports Entertainment Corp. | AGASSI ANDRE K | 200,000 sh at 1000000.0 = $200000000000 | tape 5.1 | priced at 196,078.4 times the tape and worth more than the whole company (296,823% of its market value)
- `03:52:04` read here: 25 feed days, 3584 purchase rows -> 1195 lines after the merge, 1193 counted, 2 set aside (AASP, LONA); tickers on the tape 387
- `03:52:04` purchase list: 371 tickers, $1186.53M; computed here 371 tickers, $1186.53M; tickers that differ: 0 
- `03:52:04` largest company in the list: NFRX $114.2M (Harrison Street Infrastructure Active ETF); before the fix: LONA $9919.2M
- `03:52:04` issuers listed under more than one ticker: none; merged filings: MNK (also filed as MNKKQ, MNKPF)
- `03:52:04` scored companies 210 -> 208; no longer scored: ['MNKKQ', 'MNKPF']; newly scored: []
- `03:52:04` scores that changed among the 208 names scored both times: 35 [('MTDR', 87.5, 85.5), ('PSEC', 85.5, 83.5), ('FIP', 84.8, 82.8), ('FBRT', 81.8, 79.8), ('OCFC', 73.0, 71.0), ('WW', 70.5, 68.5), ('EGBN', 69.5, 67.5), ('PRLB', 68.5, 66.5)]
- `03:52:04` top five now: UPBD 91.8 smart_money_dual, AN 87.8 smart_money_dual, FF 87.5 smart_money_dual, DTI 87.5 smart_money_dual, LRMR 87.5 smart_money_dual
- `03:52:04` names for unscored companies: 362 of 371 rows carry a company name, 371 carry a CIK
- `03:52:04` ✅ FIRST LIGHT: ARK 2.1.0 lists all 76 moves against the 2026-11-16 holdings and carries them on a second run; insider 2.2.0 publishes 371 companies ($1186.5M), 2 impossible record(s) set aside, one row per filing
