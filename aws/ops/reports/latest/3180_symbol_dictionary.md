# ops 3180 — full name for every symbol

**Status:** success  
**Duration:** 26.4s  
**Finished:** 2026-07-13T00:09:54+00:00  

## Error

```
SystemExit: 0
```

## Data

| n_fails | n_warns | named | named_pct | pass1_filled | pass1_named | pass1_pct | src_formula | src_fred | src_market | src_worldbank | symbols | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 6507 | 6507 | 100.0 |  |  |  |  |  |  |
|  |  | 6507 | 100.0 |  |  |  | 337 | 782 | 1739 | 988 | 6507 |  |
| 0 | 1 |  |  |  |  |  |  |  |  |  |  | PASS |

## Log
## 1. Deploy

- `00:09:28`   zip: 64629 bytes
## 1. Lambda

- `00:09:28`   Lambda missing — creating
- `00:09:33` ✅   ✓ created justhodl-symbol-dictionary
- `00:09:33` ✅   ✓ Function URL: https://5moenx6fx425dxpefftrsosa5y0ataxm.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `00:09:33` ✅   ✓ created rule symbol-dictionary-weekly
- `00:09:33` ✅   ✓ target → justhodl-symbol-dictionary
- `00:09:34` ✅   ✓ added invoke permission
## 2. Build the dictionary (runs in passes; cached)

## 3. Coverage

- `00:09:54` ✅ 6507 of 6507 symbols carry an authoritative name (100.0%)
## 4. Spot-check the names he will actually read

- `00:09:54`   FRED:DGS10             → DGS10 (FRED)                                             [FRED: DGS10] 
- `00:09:54`   FRED:PRAWMINDEXM       → PRAWMINDEXM (FRED)                                       [FRED: PRAWMINDEXM] 
- `00:09:54`   FRED:WALCL             → WALCL (FRED)                                             [FRED: WALCL] 
- `00:09:54`   FRED:RRPONTSYD         → RRPONTSYD (FRED)                                         [FRED: RRPONTSYD] 
- `00:09:54`   TVC:DXY                → US Dollar Index (DXY)                                    [MARKET: DX-Y.NYB] D
- `00:09:54`   NASDAQ:NVDA            → Nvidia Corp                                              [MARKET: NVDA] USD · D
- `00:09:54`   AMEX:KRE               → State Street SPDR S&P Regional Banking ETF               [MARKET: KRE] USD · D
- `00:09:54`   ECONOMICS:ZWDIR        → Zimbabwe — Deposit interest rate (%)                     [WORLDBANK: ZW|FR.INR.DPST] varies · A
- `00:09:54`   ECONOMICS:KHBOT        → Cambodia — External balance on goods and services (curre [WORLDBANK: KH|NE.RSB.GNFS.CD] varies · A
- `00:09:54`   ECONOMICS:CNFER        → China — Total reserves (includes gold, current US$)      [WORLDBANK: CN|FI.RES.TOTL.CD] varies · A
- `00:09:54`   TVC:US10Y              → US10Y (TVC)                                              [FRED: DGS10] 
- `00:09:54`   NYMEX:CL1!             → CL1! (NYMEX)                                             [None: None] 
## 5. Page

- `00:09:54` ⚠ CDN still on the pre-dictionary page (self-heals)
