# ops 3769 — sample-confidence tiers + v3.1/v3.2 page coverage

**Status:** success  
**Duration:** 34.7s  
**Finished:** 2026-07-23T18:17:46+00:00  

## Data

| backlog_joined | backlog_overlap | industries_high_conf | invoke_seconds | invoke_status | scored |
|---|---|---|---|---|---|
|  |  |  | 15.5 | 200 |  |
| 49 | 61 | 8 |  |  | 877 |

## Log
## G0_KEY_CONTRACT

- `18:17:11` ✅ G0.bygi :: _bygi industry->gaps map in scope
- `18:17:11` ✅ G0.tots :: _tots industry totals in scope
- `18:17:11` ✅ G0.v32 :: engine at v3.2
- `18:17:11` ✅ G0.page_missing_v31 :: confirms page renders NO v3.1/v3.2 fields (the gap being closed)
## [A] Engine — sample-confidence tiers on industry medians

- `18:17:11` ✅ A.anchor :: industry_underweight block unique
- `18:17:11` ✅ confidence tiers spliced (n>=5 floor, HIGH/MEDIUM/LOW, IQR) + v3.3
## [B] Page — add cross-industry + coverage sections

- `18:17:11` ✅ B.sec_anchor :: ledger section anchor unique
- `18:17:11` ✅ B.hero_anchor :: hero backlog tile anchor unique
- `18:17:11` ✅ B.method_anchor :: method anchor unique
- `18:17:11` ✅ page rewritten with cross-industry + confidence-tiered industry board
- `18:17:11` ✅ PAGE.renders_global_capture_gap :: now rendered
- `18:17:11` ✅ PAGE.renders_gap_divergence :: now rendered
- `18:17:11` ✅ PAGE.renders_widest_global_gaps :: now rendered
- `18:17:11` ✅ PAGE.renders_industry_underweight :: now rendered
- `18:17:11` ✅ PAGE.renders_sample_confidence :: now rendered
- `18:17:11` ✅ PAGE.renders_iqr_global_gap :: now rendered
- `18:17:11` ✅ PAGE.renders_legs_available :: now rendered
- `18:17:11` ✅ PAGE.renders_backlog_overlap :: now rendered
- `18:17:11` ✅ PAGE.renders_backlog_ledger_size :: now rendered
- `18:17:11` ✅ PAGE.renders_global_criticality_pctile :: now rendered
- `18:17:11` ✅ PAGE.renders_global_mcap_pctile :: now rendered
- `18:17:11` ✅ PAGE.renders_rpo_yoy :: now rendered
- `18:17:11` ✅ PAGE.no_dead_field :: dead 3766 field removed from page
## Deploy

- `18:17:11`   zip: 96206 bytes
## 1. Lambda

- `18:17:12`   Lambda exists — updating
- `18:17:15` ✅   ✓ updated justhodl-chokepoint
- `18:17:30` ✅ settled attempt 1
- `18:17:30` ✅ DEPLOY.settled :: v3.3 live
## Live verification

- `18:17:46` ✅ LIVE.v33 :: version=3.3
- `18:17:46` ✅ LIVE.iu_present :: industry_underweight n=25
- `18:17:46` ✅ LIVE.conf_field :: every row carries sample_confidence
- `18:17:46` ✅ LIVE.floor :: n>=5 floor enforced (min n=8)
- `18:17:46` ✅ LIVE.conf_sorted :: top row is HIGH confidence (Software - Application, n=80) — thin curiosities demoted
## Confidence-tiered industry board (was: 3-name medians on top)

- `18:17:46`   Software - Application             n=80  HIGH   median  +44.4pp  IQR 49.1
- `18:17:46`   Biotechnology                      n=62  HIGH   median  +32.4pp  IQR 55.4
- `18:17:46`   Software - Infrastructure          n=52  HIGH   median  +25.4pp  IQR 50.1
- `18:17:46`   Semiconductors                     n=71  HIGH   median  +13.6pp  IQR 39.5
- `18:17:46`   Medical - Diagnostics & Research   n=21  HIGH   median   +2.3pp  IQR 41.4
- `18:17:46`   Financial - Capital Markets        n=29  HIGH   median   -6.3pp  IQR 44.0
- `18:17:46`   Information Technology Services    n=28  HIGH   median   -9.8pp  IQR 50.5
- `18:17:46`   Aerospace & Defense                n=37  HIGH   median  -13.9pp  IQR 38.9
- `18:17:46`   Medical - Devices                  n=17  MEDIUM median  +31.6pp  IQR 17.3
- `18:17:46`   Drug Manufacturers - Specialty & G n=9   MEDIUM median  +25.4pp  IQR 45.4
- `18:17:46`   Communication Equipment            n=19  MEDIUM median  +21.4pp  IQR 43.8
- `18:17:46`   Medical - Instruments & Supplies   n=13  MEDIUM median  +16.0pp  IQR 47.1
- `18:17:46`   --- demoted as too thin (n<5) ---
- `18:17:46`   Medical - Pharmaceuticals          n=4   median +51.5pp
- `18:17:46`   Medical - Equipment & Services     n=3   median +48.0pp
- `18:17:46`   REIT - Diversified                 n=1   median +46.7pp
- `18:17:46`   REIT - Mortgage                    n=1   median +39.8pp
- `18:17:46`   Apparel - Footwear & Accessories   n=2   median +31.7pp
- `18:17:46`   REIT - Office                      n=2   median +30.5pp
## Additive contract

- `18:17:46` ✅ ADDITIVE.structural_names :: present
- `18:17:46` ✅ ADDITIVE.industry_leaders :: present
- `18:17:46` ✅ ADDITIVE.all_chokepoints :: present
- `18:17:46` ✅ ADDITIVE.hidden_chokepoint_book :: present
- `18:17:46` ✅ ADDITIVE.cheap_chokepoint_book :: present
- `18:17:46` ✅ ADDITIVE.within_gap :: within-industry gap preserved
- `18:17:46` ✅ ADDITIVE.global_gap :: cross-industry gap preserved
## VERDICT

- `18:17:46` ✅ PASS_ALL — thin-sample flaw closed, page now renders every published field
