# ops 6991 - Credit Before Equity, held by funds: first publication and live proof, 2026-11-27T02:36+00:00

**Status:** failure  
**Duration:** 1252.9s  
**Finished:** 2026-11-27T02:57:18+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Guard: the deployed function is this checkout's

- `02:36:26` justhodl-credit-before-equity updated 2026-11-27T02:30:54 | memory 3008 MB, timeout 900 s | files as in this checkout: {'lambda_function.py': True, 'cbe_core.py': True, 'cbe_names.py': True, 'cbe_themes.py': True, 'cbe_study.py': True, 'cbe_funds.py': True, 'leaders_tape.py': True}
- `02:36:27` before: data/credit-before-equity.json version 3.0.0, 2840263 bytes, generated 2026-11-26T22:35:42.373194+00:00, holdings of 2026-11-25, 4180 companies, 69 columns, fund layer None
- `02:36:27` the SEC bank's document: generated 2026-11-27T01:11:45+00:00, as of 2026-06-30, file 2026q3_nport.zip, set aside []
## 2. One dry run inside Lambda (nothing written)

- `02:36:48` HTTP 200, function error None, 21.3 s | {"statusCode": 200, "dry_run": true, "as_of": "2026-11-25", "companies": 4180, "listed": 1106, "names": 1112, "leads": 56, "cds_levels": 401, "windows": {"w1": "2026-11-18", "m1": "2026-10-30", "m3": "2026-08-31", "ytd": "2025-12-31"}, "degraded": [], "ledger_days": ["2026-11-25"], "bytes": 3214700, "version": "3.0.0", "stressed": 438, "themes": {"ai": 70}, "evidence": true, "study_due": null, "forecasts": 1083, "bonds_rows": 21084, "calls": 1083, "funds": {"as_of": "2026-06-30", "rows": 35569, "securities": 35569, "value": 2501086600929, "value_pct": 82.8, "companies": 3502, "listed": 1069, "how": {"issuer": [30904, 2088220407832], "short name": [2057, 230819212186], "name cut": [955, 89740330034], "co-issuer": [245, 40707586250], "cut name": [1106, 35837996010], "exact": [144, 4885105060], "subsidiary": [85, 4542079269], "alias": [47, 4341509631], "core": [26, 1992374657]}, "cds_names_
- `02:36:48`   lambda: REPORT RequestId: 88d22a5e-30e2-442d-84ad-0b36381e0a87	Duration: 20615.47 ms	Billed Duration: 20948 ms	Memory Size: 3008 MB	Max Memory Used: 752 MB	Init Duration: 332.23 ms
- `02:36:49` the document is what it was and no fund document exists yet: the dry run wrote nothing
## 3. One publication (a one-off run outside the 22:35 UTC schedule)

- `02:37:09` HTTP 200, function error None, 19.8 s | {"statusCode": 200, "body": "{\"names\": 1112, \"leads\": 56, \"companies\": 4180, \"as_of\": \"2026-11-25\", \"study\": false}"}
- `02:37:09`   lambda: REPORT RequestId: aaa5984b-a528-4c0a-a4ba-3c79c7dc808e	Duration: 19678.09 ms	Billed Duration: 19679 ms	Memory Size: 3008 MB	Max Memory Used: 754 MB
## 4. Read back

- `02:37:10` data/credit-before-equity.json: 3214700 bytes (743252 gzipped), generated 2026-11-27T02:36:49.794432+00:00, version 3.0.0, holdings of 2026-11-25 | data/credit-before-equity-funds.json: 6377937 bytes (1401083 gzipped), 35,569 rows, written 02:37:10
- `02:37:10` degraded: []
- `02:37:10` ✅ the board's 69 columns are where they were and the 13 fund columns follow them
- `02:37:10` ✅ the engine reports nothing wrong with the fund layer
- `02:37:10` against the document it replaces (holdings of 2026-11-25 then, 2026-11-25 now): 4180 companies then, 4180 now, 0 gone, 0 with another spread, rank, book, tie or signal
- `02:37:10` ✅ the board is what it was: the fund layer moved no company's spread, rank, book or signal (none)
- `02:37:10` ✅ the fund layer is of the quarter the SEC bank published whole: 2026q3_nport.zip, as of 2026-06-30
- `02:37:11` companies with fund holdings 3502 (1069 listed), 35,569 securities worth $2.50T | most widely held: OAK EAGLE ACQUIRECO  469 funds; MERIDIAN ARC HOLDCO  458 funds; SV RNO PROPERTY OWNE 448 funds; QUIKRETE HOLDINGS IN 428 funds; ASURION LLC 421 funds; WULF COMPUTE LLC 412 funds; 1261229 BC LTD 409 funds; PR RNO PROPERTY OWNE 403 funds
- `02:37:11` ✅ every company's nine holding columns are its own rows of the fund document added up here (3502 companies, 35,569 rows)
- `02:37:11` ✅ every row sits under a company on the board and is a bond or a loan
- `02:37:12` the SEC bank's table: 287,615 securities, 67,426 of them a company's bonds and loans worth $3.02T; tied to the board 35,569 worth $2.50T (82.8%; the block says 82.8%)
- `02:37:12` ✅ every row of the fund document is a row of the SEC bank's table, and the share tied is the block's (0 not found)
- `02:37:12`   T:ORCL                 the board:   58 securities    $26.1B, most funds  378, mark   91.46, gap  0.43 | the SEC table by the pattern '^ORACLE':   61 securities    $26.1B
- `02:37:12`   T:CRWV                 the board:   20 securities     $3.7B, most funds  333, mark  102.46, gap  0.37 | the SEC table by the pattern '^COREWEAVE INC':   12 securities     $3.6B
- `02:37:12`   T:F                    the board:   83 securities    $19.7B, most funds  167, mark   99.96, gap  0.29 | the SEC table by the pattern '^FORD MOTOR':   83 securities    $19.7B
- `02:37:13`   T:BA                   the board:   43 securities    $12.8B, most funds  245, mark  101.24, gap  0.81 | the SEC table by the pattern '^BOEING':   43 securities    $12.8B
- `02:37:13`   T:CHTR                 the board:   90 securities    $25.7B, most funds  294, mark    90.3, gap  0.45 | the SEC table by the pattern '^CHARTER COMM|^CCO H':   82 securities    $23.8B
- `02:37:13`   I:MERIDIAN ARC HOLDCO  the board:    1 securities     $2.2B, most funds  458, mark  100.22, gap  0.11 | the SEC table by the pattern 'MERIDIAN ARC':    1 securities     $2.2B
- `02:37:13` credit default swaps: 755 names in the SEC bank's table, 755 on the page, 323 tied to 274 companies; dealers' names 10, tied 0: GOLDMAN SACHS & CO; CITIBANK NA; MORGAN STANLEY & COMPANY LLC; JP MORGAN CHASE BANK; BARCLAYS; BANK OF AMERICA; MORGAN STANLEY; DEUTSCHE BANK AG; BNP PARIBAS SA; DEUTSCHE BANK AKTIENGESELLSC
- `02:37:13` funds are net sellers of protection on: ORCL $550M; VZ $461M; T $356M; BA $294M; C $196M; GOOGL $159M; WFC $139M; VG $137M | net buyers on: INTESA SANPAOLO SP $213M; INDUSTRIAL AND COM $104M; BCS $68M; AEP $55M; LUV $50M; WHR $36M; OXY $33M; AMX $33M
- `02:37:13` ✅ every company's swap figures are its reference names added up here, and every name of the SEC bank's table is listed
- `02:37:13` ✅ a reference name funds also write as a dealer enters no company (10 names)
- `02:37:13` ✅ the fund document carries the lists whole: 747 names tied by a rule, 452 listed companies not on the board, 16697 issuers left untied, 15,175 securities enough funds mark
- `02:37:13` ✗ nothing published is, or is shaped like, an identifier (89,337 names and titles read; 2 shaped like one)
- `02:37:15` the site: the board's document generated 2026-11-27T02:36:49.794432+00:00; the fund document 6377937 bytes, 35,569 rows, content-encoding None, cache-control public, max-age=60, s-maxage=300
- `02:37:15` ✅ the site serves both documents as they are in the bucket
## 5. The page in a real browser on the live site

- `02:57:18` the live page names the fund script of 20261127a: False (after 40 checks, 30 s apart)
- `02:57:18` ✗ the site has not deployed the page yet; the engine side is published and checked above. Run this op again once the site is out: it will not run the engine a second time
