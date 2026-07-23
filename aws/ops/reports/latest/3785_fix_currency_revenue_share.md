# ops 3785 — revenue share: USD-only denominator

**Status:** success  
**Duration:** 38.1s  
**Finished:** 2026-07-23T23:35:52+00:00  

## Data

| invoke_seconds | invoke_status | semis | semis_published | semis_suppressed |
|---|---|---|---|---|
| 17.0 | 200 |  |  |  |
|  |  | 86 | 72 | 14 |

## Log
## G0

- `23:35:13` ✅ G0.v41 :: engine at v4.1
- `23:35:13` ✅ G0.rev_field :: revenue persisted
- `23:35:13` ✅ G0.share_block :: share computation present to patch
- `23:35:13` ✅ G0.no_currency_yet :: currency never read — the confirmed defect
## [1] Read reportedCurrency in evaluate()

- `23:35:13` ✅ P1.anchor :: revenue pick anchor unique
## [2] Carry currency through cap_rows

- `23:35:13` ✅ P2.anchor :: cap_rows revenue carry unique
## [3] USD-only denominator with a coverage floor

- `23:35:13` ✅ P3.anchor :: denominator build anchor unique
- `23:35:13` ✅ P4.anchor :: share assignment anchor unique
- `23:35:13` ✅ USD-only denominator + coverage floor spliced (v4.1.1)
## Deploy

- `23:35:14`   zip: 101119 bytes
## 1. Lambda

- `23:35:14`   Lambda exists — updating
- `23:35:18` ✅   ✓ updated justhodl-chokepoint
- `23:35:33` ✅ settled attempt 1
- `23:35:33` ✅ DEPLOY.settled :: v4.1.1 live
## Invoke + MAGNITUDE gate (the check that caught this)

- `23:35:52` ✅ LIVE.v411 :: version=4.1.1
## Semis after the fix

- `23:35:52`   NVDA   share=32.25%  rev=215,938,000,000  ccy=USD
- `23:35:52`   AVGO   share=9.54%   rev=63,887,000,000   ccy=USD
- `23:35:52`   INTC   share=7.89%   rev=52,853,000,000   ccy=USD
- `23:35:52`   QCOM   share=6.61%   rev=44,284,000,000   ccy=USD
- `23:35:52`   MU     share=5.58%   rev=37,378,000,000   ccy=USD
- `23:35:52`   AMD    share=5.17%   rev=34,639,000,000   ccy=USD
- `23:35:52`   AMAT   share=4.24%   rev=28,368,000,000   ccy=USD
- `23:35:52`   LRCX   share=2.75%   rev=18,435,591,000   ccy=USD
- `23:35:52`   TXN    share=2.64%   rev=17,682,000,000   ccy=USD
- `23:35:52`   TEL    share=2.55%   rev=17,089,000,000   ccy=USD
- `23:35:52`   TSM    SUPPRESSED :: filer reports in TWD — not summable with USD peers
- `23:35:52`   ASX    SUPPRESSED :: filer reports in unknown — not summable with USD peers
- `23:35:52`   SKHY   SUPPRESSED :: filer reports in KRW — not summable with USD peers
- `23:35:52`   UMC    SUPPRESSED :: filer reports in TWD — not summable with USD peers
- `23:35:52` ✅ MAGNITUDE.nvda :: NVDA now 32.2% of USD-filing semis (was 0.21% with KRW in the denominator)
- `23:35:52` ✅ PURITY.usd_only :: 0 non-USD filers still publishing a share (must be 0)
## Additive

- `23:35:52` ✅ ADDITIVE.capture_gap :: preserved
- `23:35:52` ✅ ADDITIVE.catchup_pct :: preserved
- `23:35:52` ✅ ADDITIVE.global_capture_gap :: preserved
- `23:35:52` ✅ ADDITIVE.criticality_pctile :: preserved
## VERDICT

- `23:35:52` ✅ PASS_ALL — mixed-currency denominator fixed; shares are USD-only or blank
