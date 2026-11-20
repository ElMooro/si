# ops 6860 - the factor book's typing and the split rule, released 2026-11-20T16:01+00:00

**Status:** failure  
**Duration:** 631.3s  
**Finished:** 2026-11-20T16:12:06+00:00  

## Error

```
SystemExit: 1
```

## Data

| common_after | common_before | failures | liquid_moved | moved_out_of_common | refused_campaign | refused_volatility |
|---|---|---|---|---|---|---|
| 5333 | 5333 | 2 | 0 | 0 | 53 | 53 |

## Log
## G0. holds, stage, deployed versions

- `16:01:38` governor stage NORMAL | capped ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi'] | anomaly []
- `16:01:38` justhodl-bank-factors: deployed VERSION 1.0.9, other_issues.py present
- `16:01:39` justhodl-strength: deployed VERSION 1.0.13, at-the-open test in lambda_function.py: present
- `16:01:40` justhodl-leaders: deployed VERSION 1.0.1, at-the-open test in leaders_tape.py: present
- `16:01:41` justhodl-campaign-scan: deployed VERSION 1.0.2, at-the-open test in leaders_tape.py: present
- `16:01:42` justhodl-stock-volatility: deployed VERSION 1.0.1, at-the-open test in leaders_tape.py: present
- `16:01:45` before: data/strength.json 1.0.12 generated 2026-11-20T03:40:09+00:00 as of 2026-11-19, 857 tickers on it
- `16:01:45` before: data/leaders.json 1.0.0 generated 2026-11-20T03:50:07+00:00 as of 2026-11-19, 796 tickers on it
- `16:01:45` before: data/campaign-scan.json 1.0.1 generated 2026-11-20T04:02:22+00:00 as of 2026-11-19, 3995 tickers on it
- `16:01:45` before: data/stock-volatility.json 1.0.0 generated 2026-11-20T04:06:32+00:00 as of 2026-11-19, 7462 tickers on it
- `16:01:45` before: the book has 27123 rows, 5333 common stock
## 1. justhodl-bank-factors: one run, then the book read back

- `16:01:46` invoke status 202 (manifest before: generated 2026-11-20T02:50:44.798467+00:00, version 1.0.8)
- `16:08:19` the book: version 1.0.9, written 393s after the invoke; other_issues n None, by flag null, by evidence null, structure test None, reference names None
- `16:08:19` FAIL the manifest does not carry the 1.0.9 pass: version 1.0.9, other_issues {}
- `16:08:20` FAIL expected to move and did not: ['AGNCO', 'AGNCL', 'HBANL', 'HBANM', 'HBANZ', 'ATLCL', 'ATLCZ', 'BHFAL', 'MCHPP', 'ACGLN', 'XELLL', 'TPGXL', 'WAFDP', 'CTGG', 'EMP', 'JSM']
- `16:08:20` common stock 5333 -> 5333; 0 of the 0 moved rows were common stock before this run
- `16:08:20`   is_preferred (0): 
- `16:08:20`   is_debt_or_structured (0): 
- `16:08:20`   is_warrant_unit (0): 
- `16:08:20` controls still common stock: GOOGL, GOOG, FOXA, FOX, NWSA, NWS, BBD, CIB, BAK, ARM, AGNC, HBAN, PFBC, MCHP, ACGL, XEL, TPG, ATLC
- `16:08:20` moved rows that trade $250k a day or more: 0 - 
## 2. strength, leaders, campaign scan, stock volatility: one run each

- `16:08:20` justhodl-strength invoke status 202
- `16:08:21` justhodl-leaders invoke status 202
- `16:08:21` justhodl-campaign-scan invoke status 202
- `16:08:21` justhodl-stock-volatility invoke status 202
- `16:09:02` justhodl-leaders wrote data/leaders.json 42s after the invoke
- `16:09:03` justhodl-strength wrote data/strength.json 43s after the invoke
- `16:11:25` justhodl-stock-volatility wrote data/stock-volatility.json 185s after the invoke
- `16:12:06` justhodl-campaign-scan wrote data/campaign-scan.json 226s after the invoke
- `16:12:06` data/strength.json: version 1.0.13, generated 2026-11-20T16:08:21+00:00, as of 2026-11-19 | 857 tickers (before 857) | left the artifact: 0  | came on: 0 
- `16:12:06`   of those that left, moved by the book: none
- `16:12:06`   splits: inferred 0, refused 0 {}
- `16:12:06` data/leaders.json: version 1.0.1, generated 2026-11-20T16:08:21+00:00, as of 2026-11-19 | 791 tickers (before 796) | left the artifact: 16 ACGLN, AGNCL, AGNCO, APG, BR, BTU, DVN, EXK, KTB, LEU, MKC.V, NTRA, OKLO, QSR, ROIV, RUN | came on: 11 CDW, DRI, FANG, HAFN, HRL, INSM, J, PVH, TAC, WAT, WSO
- `16:12:06`   of those that left, moved by the book: none
- `16:12:06`   splits: inferred 0, refused 0 {}
- `16:12:06`   leaders changes kept: prev_as_of 2026-11-18, new 172, gone 178; leaders 667
- `16:12:06` data/campaign-scan.json: version 1.0.2, generated 2026-11-20T16:08:22+00:00, as of 2026-11-19 | 3995 tickers (before 3995) | left the artifact: 16 ACGLN, BHFAL, BHFAM, BHFAN, BHFAO, CGABL, DHCNI, DHCNL, FULTP, HBANM, HUBC, JSM, MCHPP, RILYZ, TPGXL, XELLL | came on: 16 ABLV, AIOK, CIFR, EMAT, ENGS, INLF, INV, IPST, JDZG, MWYN, NCI, OPTX, RPGL, STFS, WETO, YMAT
- `16:12:06`   of those that left, moved by the book: none
- `16:12:06`   splits refused 53: ABLV 2026-10-12 x0.5565; AIOK 2026-10-28 x2.0969; AIXI 2026-04-07 x2.426; AKAN 2026-04-30 x1.8838; AKBA 2022-03-30 x0.3398; BMGL 2026-11-10 x0.3574; BMNR 2025-07-03 x2.3077; BNAI 2024-05-31 x2.1085; CIFR 2022-05-10 x0.5325; CLIK 2025-04-16 x0.258; CLRO 2026-07-07 x1.9771; CSAI 2025-02-13 x2.1373; DGNX 2025-01-29 x3.3456; DKI 2026-10-09 x0.4592; EDSA 2026-03-06 x1.8061; EHGO 2026-06-30 x0.4869; ELPW 2026-05-12 x0.5335; EMAT 2026-11-10 x0.5596; ENGS 2025-05-05 x0.3359; FLYE 2025-10-31 x0.4715; FMFC 2025-10-30 x0.5526; GMM 2024-03-18 x0.33; HOLO 2024-02-20 x0.2473; IBIO 2025-04-07 x0.4682; INV 2026-10-07 x0.4744; IPDN 2023-02-28 x0.488; IPST 2026-08-18 x2.1326; JDZG 2026-11-06 x0.5324; JYD 2026-05-20 x0.4789; LGHL 2022-09-29 x0.5075; LICN 2026-05-20 x0.3852; LUNR 2023-02-23 x0.2476; MI 2024-11-15 x0.3333; MWYN 2025-06-06 x0.2896; NCI 2026-09-28 x0.1625; ONEG 2025-11-14 x0.501; OPTX 2024-12-20 x1.8041; PASW 2026-11-05 x0.5427; PLRX 2025-03-03 x0.4012; RBNE 2025-06-17 x1.9185; RETO 2025-02-25 x1.8305; RPGL 2026-10-16 x0.5169; SDOT 2026-06-17 x1.9815; SOUN 2022-06-06 x0.5164; STFS 2025-08-20 x0.2101; SXTC 2026-10-09 x0.4329; TWG 2024-10-25 x0.04; VSA 2026-03-27 x0.5549; WCT 2024-11-14 x0.5253; WETO 2026-08-17 x2.9915; WFF 2026-10-13 x0.1227; YMAT 2026-05-19 x0.4894; YMT 2026-05-20 x0.5572
- `16:12:06`   universe left_out: {"otc": 10206, "thin (under the dollar-volume floor)": 2791, "not a common stock": 5888, "geared or inverse fund": 808}
- `16:12:06`   counts: {"accumulation": 245, "bottom": 2994, "distribution": 1359} | changes prev_as_of 2026-11-18
- `16:12:06` data/stock-volatility.json: version 1.0.1, generated 2026-11-20T16:08:22+00:00, as of 2026-11-19 | 7429 tickers (before 7462) | left the artifact: 34 ACGLN, ADAMG, ADAMH, ADAMK, ADAMO, AGNCL, AGNCO, ATLCL, ATLCZ, BHFAL, BHFAM, BHFAN, BHFAO, BPYPM, CGABL, CTGG, CTHH, DHCNI, DHCNL, EMP, FULTP, HBANL, HBANM, HBANZ, JSM, LANDO, MBINL, MCHPP, RILYG, RILYT, RILYZ, TPGXL, WAFDP, XELLL | came on: 1 PCPC
- `16:12:06`   of those that left, moved by the book: none
- `16:12:06`   splits refused 53: ABLV 2026-10-12 x0.5565; AIOK 2026-10-28 x2.0969; AIXI 2026-04-07 x2.426; AKAN 2026-04-30 x1.8838; AKBA 2022-03-30 x0.3398; BMGL 2026-11-10 x0.3574; BMNR 2025-07-03 x2.3077; BNAI 2024-05-31 x2.1085; CIFR 2022-05-10 x0.5325; CLIK 2025-04-16 x0.258; CLRO 2026-07-07 x1.9771; CSAI 2025-02-13 x2.1373; DGNX 2025-01-29 x3.3456; DKI 2026-10-09 x0.4592; EDSA 2026-03-06 x1.8061; EHGO 2026-06-30 x0.4869; ELPW 2026-05-12 x0.5335; EMAT 2026-11-10 x0.5596; ENGS 2025-05-05 x0.3359; FLYE 2025-10-31 x0.4715; FMFC 2025-10-30 x0.5526; GMM 2024-03-18 x0.33; HOLO 2024-02-20 x0.2473; IBIO 2025-04-07 x0.4682; INV 2026-10-07 x0.4744; IPDN 2023-02-28 x0.488; IPST 2026-08-18 x2.1326; JDZG 2026-11-06 x0.5324; JYD 2026-05-20 x0.4789; LGHL 2022-09-29 x0.5075; LICN 2026-05-20 x0.3852; LUNR 2023-02-23 x0.2476; MI 2024-11-15 x0.3333; MWYN 2025-06-06 x0.2896; NCI 2026-09-28 x0.1625; ONEG 2025-11-14 x0.501; OPTX 2024-12-20 x1.8041; PASW 2026-11-05 x0.5427; PLRX 2025-03-03 x0.4012; RBNE 2025-06-17 x1.9185; RETO 2025-02-25 x1.8305; RPGL 2026-10-16 x0.5169; SDOT 2026-06-17 x1.9815; SOUN 2022-06-06 x0.5164; STFS 2025-08-20 x0.2101; SXTC 2026-10-09 x0.4329; TWG 2024-10-25 x0.04; VSA 2026-03-27 x0.5549; WCT 2024-11-14 x0.5253; WETO 2026-08-17 x2.9915; WFF 2026-10-13 x0.1227; YMAT 2026-05-19 x0.4894; YMT 2026-05-20 x0.5572
- `16:12:06`   universe left_out: {"otc": 10206, "thin (under the dollar-volume floor)": 2791, "not a common stock": 5888, "geared or inverse fund": 808}
- `16:12:06`   universe: stocks 4277, ETFs 3152, set aside {"stock": 112, "etf": 0}, own guard found 0
- `16:12:06` ✗ 2 checks failed: the manifest does not carry the 1.0.9 pass: version 1.0.9, other_issues {} | expected to move and did not: ['AGNCO', 'AGNCL', 'HBANL', 'HBANM', 'HBANZ', 'ATLCL', 'ATLCZ', 'BHFAL', 'MCHPP', 'ACGLN', 'XELLL', 'TPGXL', 'WAFDP', 'CTGG', 'EMP', 'JSM']
