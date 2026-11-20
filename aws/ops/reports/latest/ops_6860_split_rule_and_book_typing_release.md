# ops 6860 - the factor book's typing and the split rule, released 2026-11-20T16:15+00:00

**Status:** success  
**Duration:** 8.6s  
**Finished:** 2026-11-20T16:15:44+00:00  

## Data

| common_after | common_before | failures | liquid_moved | moved_out_of_common | refused_campaign | refused_volatility |
|---|---|---|---|---|---|---|
| 5256 | 5256 | 0 | 34 | 65 | 53 | 53 |

## Log
## G0. holds, stage, deployed versions

- `16:15:37` governor stage NORMAL | capped ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi'] | anomaly []
- `16:15:37` justhodl-bank-factors: deployed VERSION 1.0.9, other_issues.py present
- `16:15:38` justhodl-strength: deployed VERSION 1.0.13, at-the-open test in lambda_function.py: present
- `16:15:38` justhodl-leaders: deployed VERSION 1.0.1, at-the-open test in leaders_tape.py: present
- `16:15:39` justhodl-campaign-scan: deployed VERSION 1.0.2, at-the-open test in leaders_tape.py: present
- `16:15:39` justhodl-stock-volatility: deployed VERSION 1.0.1, at-the-open test in leaders_tape.py: present
- `16:15:41` before: data/strength.json 1.0.13 generated 2026-11-20T16:08:21+00:00 as of 2026-11-19, 857 tickers on it
- `16:15:41` before: data/leaders.json 1.0.1 generated 2026-11-20T16:08:21+00:00 as of 2026-11-19, 791 tickers on it
- `16:15:41` before: data/campaign-scan.json 1.0.2 generated 2026-11-20T16:08:22+00:00 as of 2026-11-19, 3995 tickers on it
- `16:15:42` before: data/stock-volatility.json 1.0.1 generated 2026-11-20T16:08:22+00:00 as of 2026-11-19, 7429 tickers on it
- `16:15:42` before: the book has 27127 rows, 5256 common stock
## 1. justhodl-bank-factors: one run, then the book read back

- `16:15:42` already released: the book is 1.0.9 (generated 2026-11-20T16:01:46.805824+00:00) with its pass recorded, and the four artifacts are at the new versions - nothing is invoked, what stands is verified
- `16:15:42` the book: version 1.0.9, written 0s after the invoke; other_issues n 65, by flag {"is_debt_or_structured": 35, "is_preferred": 27, "is_warrant_unit": 3}, by evidence {"book name": 20, "reference name": 42, "structure": 3}, structure test True, reference names 13263
- `16:15:43` common stock now 5256 (5256 when this op started; 5,333 before the release); 0 of the 65 moved rows were common stock when this op started
- `16:15:43`   is_preferred (27): ACGLN, AGNCL, AGNCO, BHFAM, BHFAN, BHFAO, BPYPM, BPYPN, BPYPO, BWBBP, CNOBP, FRMEP, FULTP, HBANL, HBANM, HBANZ, IMPPP, LANDO, MBINL, MBINM, MBINN, MCHPP, MNSBP, ONBPO, OXLCO, WAFDP, WSBCO
- `16:15:43`   is_debt_or_structured (35): ADAMG, ADAMH, ADAMI, ADAMK, ADAMO, ATLCL, ATLCZ, BHFAL, CGABL, CTGG, CTHH, DHCNI, DHCNL, EMP, GAINI, GECCH, HWCPZ, INBKZ, JSM, METCI, METCZ, NEWTG, NEWTH, NEWTI, NSLRL, OXLCI, OXSQG, OXSQH, PRHIZ, RILYG, RILYT, RILYZ, RWAYL, TPGXL, XELLL
- `16:15:43`   is_warrant_unit (3): BEATW, BTBDW, SWAGW
- `16:15:43`     ACGLN   a preferred share by the reference list's name for it
- `16:15:43`     ADAMG   a note or bond by its own name
- `16:15:43`     ADAMH   a note or bond by the reference list's name for it
- `16:15:43`     ADAMI   a note or bond by its own name
- `16:15:43`     ADAMK   a note or bond by its own name
- `16:15:43`     ADAMO   a note or bond by its own name
- `16:15:43`     AGNCL   a preferred share by the reference list's name for it
- `16:15:43`     AGNCO   a preferred share by the reference list's name for it
- `16:15:43`     ATLCL   a note or bond by its own name
- `16:15:43`     ATLCZ   a note or bond by its own name
- `16:15:43`     BEATW   a warrant of BEAT (NASDAQ fifth letter W, under a tenth of its dollar volume)
- `16:15:43`     BHFAL   a note or bond by the reference list's name for it
- `16:15:43`     BHFAM   a preferred share by the reference list's name for it
- `16:15:43`     BHFAN   a preferred share by the reference list's name for it
- `16:15:43`     BHFAO   a preferred share by the reference list's name for it
- `16:15:43`     BPYPM   a preferred share by its own name
- `16:15:43`     BPYPN   a preferred share by the reference list's name for it
- `16:15:43`     BPYPO   a preferred share by the reference list's name for it
- `16:15:43`     BTBDW   a warrant of BTBD (NASDAQ fifth letter W, under a tenth of its dollar volume)
- `16:15:43`     BWBBP   a preferred share by the reference list's name for it
- `16:15:43`     CGABL   a note or bond by its own name
- `16:15:43`     CNOBP   a preferred share by the reference list's name for it
- `16:15:43`     CTGG    a note or bond by its own name
- `16:15:43`     CTHH    a note or bond by its own name
- `16:15:43`     DHCNI   a note or bond by its own name
- `16:15:43`     DHCNL   a note or bond by the reference list's name for it
- `16:15:43`     EMP     a note or bond by its own name
- `16:15:43`     FRMEP   a preferred share by the reference list's name for it
- `16:15:43`     FULTP   a preferred share by the reference list's name for it
- `16:15:43`     GAINI   a note or bond by the reference list's name for it
- `16:15:43`     GECCH   a note or bond by the reference list's name for it
- `16:15:43`     HBANL   a preferred share by the reference list's name for it
- `16:15:43`     HBANM   a preferred share by the reference list's name for it
- `16:15:43`     HBANZ   a preferred share by the reference list's name for it
- `16:15:43`     HWCPZ   a note or bond by the reference list's name for it
- `16:15:43`     IMPPP   a preferred share by the reference list's name for it
- `16:15:43`     INBKZ   a note or bond by the reference list's name for it
- `16:15:43`     JSM     a note or bond by its own name
- `16:15:43`     LANDO   a preferred share by the reference list's name for it
- `16:15:43`     MBINL   a preferred share by the reference list's name for it
- `16:15:43`     MBINM   a preferred share by the reference list's name for it
- `16:15:43`     MBINN   a preferred share by the reference list's name for it
- `16:15:43`     MCHPP   a preferred share by the reference list's name for it
- `16:15:43`     METCI   a note or bond by the reference list's name for it
- `16:15:43`     METCZ   a note or bond by the reference list's name for it
- `16:15:43`     MNSBP   a preferred share by the reference list's name for it
- `16:15:43`     NEWTG   a note or bond by its own name
- `16:15:43`     NEWTH   a note or bond by the reference list's name for it
- `16:15:43`     NEWTI   a note or bond by its own name
- `16:15:43`     NSLRL   a note or bond by the reference list's name for it
- `16:15:43`     ONBPO   a preferred share by the reference list's name for it
- `16:15:43`     OXLCI   a note or bond by the reference list's name for it
- `16:15:43`     OXLCO   a preferred share by the reference list's name for it
- `16:15:43`     OXSQG   a note or bond by the reference list's name for it
- `16:15:43`     OXSQH   a note or bond by the reference list's name for it
- `16:15:43`     PRHIZ   a note or bond by its own name
- `16:15:43`     RILYG   a note or bond by its own name
- `16:15:43`     RILYT   a note or bond by its own name
- `16:15:43`     RILYZ   a note or bond by its own name
- `16:15:43`     RWAYL   a note or bond by the reference list's name for it
- `16:15:43`     SWAGW   a warrant of SWAG (NASDAQ fifth letter W, under a tenth of its dollar volume)
- `16:15:43`     TPGXL   a note or bond by the reference list's name for it
- `16:15:43`     WAFDP   a preferred share by the reference list's name for it
- `16:15:43`     WSBCO   a preferred share by the reference list's name for it
- `16:15:43`     XELLL   a note or bond by its own name
- `16:15:43` controls still common stock: GOOGL, GOOG, FOXA, FOX, NWSA, NWS, BBD, CIB, BAK, ARM, AGNC, HBAN, PFBC, MCHP, ACGL, XEL, TPG, ATLC
- `16:15:43` moved rows that trade $250k a day or more: 34 - ACGLN, ADAMG, ADAMH, ADAMK, ADAMO, AGNCL, AGNCO, ATLCL, ATLCZ, BHFAL, BHFAM, BHFAN, BHFAO, BPYPM, CGABL, CTGG, CTHH, DHCNI, DHCNL, EMP, FULTP, HBANL, HBANM, HBANZ, JSM, LANDO, MBINL, MCHPP, RILYG, RILYT, RILYZ, TPGXL, WAFDP, XELLL
## 2. strength, leaders, campaign scan, stock volatility: one run each

- `16:15:43` data/strength.json: version 1.0.13, generated 2026-11-20T16:08:21+00:00, as of 2026-11-19 | 857 tickers (before 857) | left the artifact: 0  | came on: 0 
- `16:15:43`   of those that left, moved by the book: none
- `16:15:43`   splits: inferred 0, refused 0 {}
- `16:15:43` data/leaders.json: version 1.0.1, generated 2026-11-20T16:08:21+00:00, as of 2026-11-19 | 791 tickers (before 791) | left the artifact: 0  | came on: 0 
- `16:15:43`   of those that left, moved by the book: none
- `16:15:43`   splits: inferred 0, refused 0 {}
- `16:15:43`   leaders changes kept: prev_as_of 2026-11-18, new 172, gone 178; leaders 667
- `16:15:43`   named only in the day-over-day record as having left: MBINL, RILYT, WAFDP
- `16:15:43` data/campaign-scan.json: version 1.0.2, generated 2026-11-20T16:08:22+00:00, as of 2026-11-19 | 3995 tickers (before 3995) | left the artifact: 0  | came on: 0 
- `16:15:43`   of those that left, moved by the book: none
- `16:15:43`   splits refused 53: ABLV 2026-10-12 x0.5565; AIOK 2026-10-28 x2.0969; AIXI 2026-04-07 x2.426; AKAN 2026-04-30 x1.8838; AKBA 2022-03-30 x0.3398; BMGL 2026-11-10 x0.3574; BMNR 2025-07-03 x2.3077; BNAI 2024-05-31 x2.1085; CIFR 2022-05-10 x0.5325; CLIK 2025-04-16 x0.258; CLRO 2026-07-07 x1.9771; CSAI 2025-02-13 x2.1373; DGNX 2025-01-29 x3.3456; DKI 2026-10-09 x0.4592; EDSA 2026-03-06 x1.8061; EHGO 2026-06-30 x0.4869; ELPW 2026-05-12 x0.5335; EMAT 2026-11-10 x0.5596; ENGS 2025-05-05 x0.3359; FLYE 2025-10-31 x0.4715; FMFC 2025-10-30 x0.5526; GMM 2024-03-18 x0.33; HOLO 2024-02-20 x0.2473; IBIO 2025-04-07 x0.4682; INV 2026-10-07 x0.4744; IPDN 2023-02-28 x0.488; IPST 2026-08-18 x2.1326; JDZG 2026-11-06 x0.5324; JYD 2026-05-20 x0.4789; LGHL 2022-09-29 x0.5075; LICN 2026-05-20 x0.3852; LUNR 2023-02-23 x0.2476; MI 2024-11-15 x0.3333; MWYN 2025-06-06 x0.2896; NCI 2026-09-28 x0.1625; ONEG 2025-11-14 x0.501; OPTX 2024-12-20 x1.8041; PASW 2026-11-05 x0.5427; PLRX 2025-03-03 x0.4012; RBNE 2025-06-17 x1.9185; RETO 2025-02-25 x1.8305; RPGL 2026-10-16 x0.5169; SDOT 2026-06-17 x1.9815; SOUN 2022-06-06 x0.5164; STFS 2025-08-20 x0.2101; SXTC 2026-10-09 x0.4329; TWG 2024-10-25 x0.04; VSA 2026-03-27 x0.5549; WCT 2024-11-14 x0.5253; WETO 2026-08-17 x2.9915; WFF 2026-10-13 x0.1227; YMAT 2026-05-19 x0.4894; YMT 2026-05-20 x0.5572
- `16:15:43`   universe left_out: {"otc": 10206, "thin (under the dollar-volume floor)": 2791, "not a common stock": 5888, "geared or inverse fund": 808}
- `16:15:43`   counts: {"accumulation": 245, "bottom": 2994, "distribution": 1359} | changes prev_as_of 2026-11-18
- `16:15:44` data/stock-volatility.json: version 1.0.1, generated 2026-11-20T16:08:22+00:00, as of 2026-11-19 | 7429 tickers (before 7429) | left the artifact: 0  | came on: 0 
- `16:15:44`   of those that left, moved by the book: none
- `16:15:44`   splits refused 53: ABLV 2026-10-12 x0.5565; AIOK 2026-10-28 x2.0969; AIXI 2026-04-07 x2.426; AKAN 2026-04-30 x1.8838; AKBA 2022-03-30 x0.3398; BMGL 2026-11-10 x0.3574; BMNR 2025-07-03 x2.3077; BNAI 2024-05-31 x2.1085; CIFR 2022-05-10 x0.5325; CLIK 2025-04-16 x0.258; CLRO 2026-07-07 x1.9771; CSAI 2025-02-13 x2.1373; DGNX 2025-01-29 x3.3456; DKI 2026-10-09 x0.4592; EDSA 2026-03-06 x1.8061; EHGO 2026-06-30 x0.4869; ELPW 2026-05-12 x0.5335; EMAT 2026-11-10 x0.5596; ENGS 2025-05-05 x0.3359; FLYE 2025-10-31 x0.4715; FMFC 2025-10-30 x0.5526; GMM 2024-03-18 x0.33; HOLO 2024-02-20 x0.2473; IBIO 2025-04-07 x0.4682; INV 2026-10-07 x0.4744; IPDN 2023-02-28 x0.488; IPST 2026-08-18 x2.1326; JDZG 2026-11-06 x0.5324; JYD 2026-05-20 x0.4789; LGHL 2022-09-29 x0.5075; LICN 2026-05-20 x0.3852; LUNR 2023-02-23 x0.2476; MI 2024-11-15 x0.3333; MWYN 2025-06-06 x0.2896; NCI 2026-09-28 x0.1625; ONEG 2025-11-14 x0.501; OPTX 2024-12-20 x1.8041; PASW 2026-11-05 x0.5427; PLRX 2025-03-03 x0.4012; RBNE 2025-06-17 x1.9185; RETO 2025-02-25 x1.8305; RPGL 2026-10-16 x0.5169; SDOT 2026-06-17 x1.9815; SOUN 2022-06-06 x0.5164; STFS 2025-08-20 x0.2101; SXTC 2026-10-09 x0.4329; TWG 2024-10-25 x0.04; VSA 2026-03-27 x0.5549; WCT 2024-11-14 x0.5253; WETO 2026-08-17 x2.9915; WFF 2026-10-13 x0.1227; YMAT 2026-05-19 x0.4894; YMT 2026-05-20 x0.5572
- `16:15:44`   universe left_out: {"otc": 10206, "thin (under the dollar-volume floor)": 2791, "not a common stock": 5888, "geared or inverse fund": 808}
- `16:15:44`   universe: stocks 4277, ETFs 3152, set aside {"stock": 112, "etf": 0}, own guard found 0
- `16:15:44` ✅ RELEASED: the book moved 65 second issues out of common stock (controls unchanged); strength 1.0.13, leaders 1.0.1, campaign scan 1.0.2 and stock volatility 1.0.1 ran on it with the at-the-open split test, none carries a moved row
