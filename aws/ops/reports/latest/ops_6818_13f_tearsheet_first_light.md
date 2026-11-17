# ops 6818 - 13F book 1.4.0 first light 2026-11-17T20:38+00:00

**Status:** success  
**Duration:** 21.8s  
**Finished:** 2026-11-17T20:38:43+00:00  

## Data

| book_bytes | invariant_failures | run_seconds | tearsheets | version |
|---|---|---|---|---|
| 2215128 | 0 | 19.5 | 18 | 1.4.0 |

## Log
## 1. deployed code + one run

- `20:38:21` justhodl-13f-book: modified 2026-11-17T20:32:50, memory 2048, timeout 300, state Active, update Successful, sha v4nOjmtoZ+Yh
- `20:38:42` invoke 20.7s: {"statusCode": 200, "key": "data/13f-book.json", "bytes": 2215128, "managers": 18, "cohort": 15, "master_resolved": 9110, "master_identities": 9259, "run_seconds": 19.5, "errors": 0}
- `20:38:43` book: version 1.4.0 contract 13f-book.v2 generated 2026-11-17T20:38:23+00:00 trigger {"kind": "ops_6818_first_light", "source_generated_at": null} run 19.5s | 2215128 bytes (was 1784236) cache max-age=300 | period 2026-09-30 cohort 15/18 errors 0
## 2. the tearsheet on every manager

- `20:38:43` CITADEL      2026-09-30 vs 2026-06-30 CURRENT | 13F value $899.60B (prior $875.01B, 2.8%) lines 13843 (prior 13572) top-10 22.3% | new 2429 inc 5940 red 5289 absent 2158 flat 47 reclass 138 n/c 0 | options 79.7% | cover ok True | add META calls $7.16B | cut SMH puts $7.31B | top SPY calls 3.8%, SPY puts 3.28%, QQQ puts 3.22%, QQQ calls 2.18%
- `20:38:43` AQR          2026-09-30 vs 2026-06-30 CURRENT | 13F value $337.41B (prior $286.85B, 17.6%) lines 4057 (prior 3900) top-10 13.0% | new 279 inc 2239 red 1385 absent 122 flat 115 reclass 39 n/c 0 | options 0.0% | cover ok True | add NVDA $1.53B | cut EIX $458.7M | top NVDA 3.01%, AAPL 2.06%, MSFT 1.92%, AMZN 1.23%
- `20:38:43` BERKSHIRE    2026-09-30 vs 2026-06-30 CURRENT | 13F value $304.20B (prior $299.25B, 1.7%) lines 30 (prior 29) top-10 89.4% | new 1 inc 4 red 6 absent 0 flat 19 reclass 0 n/c 0 | options 0.0% | cover ok True | add LEN $1.05B | cut BAC $1.50B | top AAPL 24.95%, AXP 15.16%, KO 11.32%, GOOGL 8.91%
- `20:38:43` MILLENNIUM   2026-09-30 vs 2026-06-30 CURRENT | 13F value $302.25B (prior $276.29B, 9.4%) lines 6103 (prior 5796) top-10 19.1% | new 941 inc 2459 red 2163 absent 634 flat 502 reclass 38 n/c 0 | options 54.6% | cover ok True | add QQQ puts $3.64B | cut IVV $2.50B | top IWM puts 3.45%, IVV 2.85%, QQQ puts 2.53%, NVDA puts 2.35%
- `20:38:43` DURATION     2026-09-30 vs 2026-06-30 CURRENT | 13F value $188.79B (prior $191.39B, -1.4%) lines 2300 (prior 2301) top-10 36.5% | new 50 inc 1031 red 792 absent 51 flat 418 reclass 9 n/c 0 | options 0.0% | cover ok True | add CRWD $534.6M | cut NVDA $509.1M | top NVDA 7.61%, AAPL 7.09%, MSFT 5.24%, AMZN 3.49%
- `20:38:43` TWO_SIGMA    2026-09-30 vs 2026-06-30 CURRENT | 13F value $141.91B (prior $138.12B, 2.7%) lines 4830 (prior 5017) top-10 8.8% | new 797 inc 1918 red 1849 absent 984 flat 238 reclass 28 n/c 0 | options 3.0% | cover ok True | add NVDA $958.5M | cut WDC 3 11/15/28 $1.50B | top NVDA 1.69%, MSFT 1.25%, AAPL 0.95%, JPM 0.78%
- `20:38:43` POINT72      2026-09-30 vs 2026-06-30 CURRENT | 13F value $90.04B (prior $90.68B, -0.7%) lines 3856 (prior 3923) top-10 9.6% | new 838 inc 1414 red 1416 absent 905 flat 167 reclass 21 n/c 0 | options 29.5% | cover ok True | add MSFT $1.04B | cut AMD $804.6M | top SPY puts 1.37%, AMZN 1.21%, SPY calls 1.18%, MSFT 1.17%
- `20:38:43` RENAISSANCE  2026-09-30 vs 2026-06-30 CURRENT | 13F value $73.42B (prior $72.62B, 1.1%) lines 3151 (prior 3140) top-10 12.0% | new 577 inc 1133 red 1406 absent 566 flat 10 reclass 25 n/c 0 | options 0.0% | cover ok True | add MU $1.39B | cut NVDA $1.63B | top MU 2.21%, AMD 1.64%, UTHR 1.24%, INTC 1.14%
- `20:38:43` COATUE       2026-09-30 vs 2026-06-30 CURRENT | 13F value $54.50B (prior $48.63B, 12.1%) lines 73 (prior 66) top-10 55.9% | new 14 inc 14 red 11 absent 7 flat 34 reclass 0 n/c 0 | options 0.0% | cover ok True | add CRM $1.84B | cut FPS $1.15B | top TSM 7.9%, MU 6.4%, MSFT 6.08%, META 6.01%
- `20:38:43` ELLIOTT      2026-09-30 vs 2026-06-30 CURRENT | 13F value $28.64B (prior $22.67B, 26.3%) lines 39 (prior 29) top-10 75.8% | new 11 inc 8 red 7 absent 1 flat 13 reclass 0 n/c 0 | options 43.9% | cover ok True | add QQQ puts $1.47B | cut IGV puts $803.9M | top PSX 17.16%, TFPM 14.33%, QQQ puts 14.11%, SU 6.98%
- `20:38:43` BRIDGEWATER  2026-09-30 vs 2026-06-30 CURRENT | 13F value $22.99B (prior $24.38B, -5.7%) lines 974 (prior 997) top-10 28.3% | new 163 inc 479 red 324 absent 186 flat 3 reclass 5 n/c 0 | options 0.0% | cover ok True | add APP $155.8M | cut SPY $1.46B | top SPY 11.29%, IVV 5.02%, NVDA 2.67%, AVGO 2.01%
- `20:38:43` TIGER_GLOBAL 2026-09-30 vs 2026-06-30 CURRENT | 13F value $20.17B (prior $23.98B, -15.9%) lines 45 (prior 46) top-10 66.7% | new 3 inc 5 red 16 absent 4 flat 20 reclass 1 n/c 0 | options 0.0% | cover ok True | add CBRS $257.1M | cut GOOGL $845.2M | top NVDA 13.59%, AMZN 11.46%, META 8.08%, TSM 7.09%
- `20:38:43` LONE_PINE    2026-09-30 vs 2026-06-30 CURRENT | 13F value $15.33B (prior $16.36B, -6.3%) lines 31 (prior 34) top-10 55.7% | new 9 inc 9 red 12 absent 12 flat 1 reclass 0 n/c 0 | options 0.0% | cover ok True | add AMZN $1.39B | cut STX $965.1M | top AMZN 9.1%, DHR 7.14%, META 5.93%, V 5.23%
- `20:38:43` PERSHING     2026-03-31 vs 2025-12-31 NOTICE  | 13F value $13.71B (prior $15.53B, -11.7%) lines 11 (prior 11) top-10 99.9% | new 1 inc 1 red 6 absent 1 flat 3 reclass 0 n/c 0 | options 0.0% | cover ok True | add MSFT $2.09B | cut GOOG $1.68B | top BN 17.62%, AMZN 17.39%, UBER 15.71%, MSFT 15.26%
- `20:38:43` SOROS        2026-09-30 vs 2026-06-30 CURRENT | 13F value $8.51B (prior $8.14B, 4.5%) lines 263 (prior 266) top-10 34.4% | new 98 inc 46 red 84 absent 101 flat 35 reclass 0 n/c 0 | options 23.8% | cover ok True | add XLE puts $547.2M | cut EA $199.1M | top XLE puts 7.88%, INTC 4.0%, XOP puts 3.85%, AMZN 3.09%
- `20:38:43` GREENLIGHT   2023-12-31 vs 2023-09-30 DATED   | 13F value $2.05B (prior $1.76B, 16.3%) lines 40 (prior 39) top-10 77.0% | new 7 inc 20 red 4 absent 6 flat 9 reclass 0 n/c 0 | options 0.0% | cover ok True | add ALIGHT INC $78.8M | cut SWN $65.2M | top GRBK 28.92%, CEIX 10.34%, BHF 7.86%, KD 6.45%
- `20:38:43` SCION        2025-09-30 vs 2025-06-30 DATED   | 13F value $1.38B (prior $578.3M, 138.8%) lines 8 (prior 15) top-10 100.0% | new 7 inc 1 red 0 absent 14 flat 0 reclass 0 n/c 0 | options 95.1% | cover ok True | add PLTR puts $912.1M | cut UNH calls $109.2M | top PLTR puts 66.04%, NVDA puts 13.51%, PFE calls 11.07%, HAL calls 4.45%
- `20:38:43` BAUPOST      2026-09-30 vs 2026-06-30 CURRENT | 13F value $5.0M (prior $5.4M, -6.8%) lines 21 (prior 23) top-10 77.0% | new 2 inc 6 red 4 absent 4 flat 9 reclass 0 n/c 0 | options 0.0% | cover ok True | add HONA $175K | cut TFX $179K | top AMZN 18.49%, ELV 9.83%, QSR 9.51%, GPC 7.72%
- `20:38:43` tearsheets: 18 of 18 managers, 429981 bytes of the book
## 3. the cohort side: re-registered issuers

- `20:38:43` ticker table: version 1.4.0, 7398 tickers, generated 2026-11-17T20:38:23+00:00 | XOM row: null | XPRO row: null
- `20:38:43` contenders cohort: rising SKHY, MFP, LIME, REAX, KLRA, JMKE, FRNM, ACCV, TGLS, ETRA, STUB, ALGM, OTF, UEC, AGEN | fading APGE, TMHC, NUVL, CPRX, STEL, LEGT, OLPXGBP, EA, WBS, AVB, LBRDK, NSA, DBRG, SKYT, GTLS
- `20:38:43` sector cohort / Energy: inflow $9.97B outflow $11.02B net -$1.05B stance BALANCED money BALANCED
- `20:38:43` contenders concentrated: rising MELI, BSP | fading AGX
- `20:38:43` sector concentrated / Energy: inflow $0K outflow $48.0M net -$48.0M stance DISTRIBUTING money BALANCED
- `20:38:43` ✅ FIRST LIGHT: book 1.4.0 live with 18 tearsheets, every invariant holds, 2215128 bytes, run 19.5s
