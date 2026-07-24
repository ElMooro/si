# ops 3812 — mispricing verdict must populate for real

**Status:** success  
**Duration:** 18.5s  
**Finished:** 2026-07-24T18:03:46+00:00  

## Data

| estimates_falling | gap_stale | industry_decaying | invoke_seconds | invoke_status |
|---|---|---|---|---|
|  |  |  | 17.4 | 200 |
| 0 | 0 | 524 |  |  |

## Log
## Settle v5.0.1

- `18:03:28` ✅ v5.0.1 artifact live (attempt 1)
- `18:03:28` ✅ DEPLOY.settled :: type-guard patch in deployed zip
## Invoke

- `18:03:46` ✅ LIVE.v501 :: version=5.0.1
- `18:03:46` ✅ LIVE.no_err :: err=None
## Every leg must join

- `18:03:46` ✅ JOIN.revisions :: 318 rows
- `18:03:46` ✅ JOIN.dark_pool :: 59 rows
- `18:03:46` ✅ JOIN.short :: 494 rows
- `18:03:46` ✅ JOIN.pead :: 241 rows
- `18:03:46` ✅ JOIN.industry_boom :: 2939 rows
## Verdict distribution

- `18:03:46`   NO_GAP            2746  (85.2%)
- `18:03:46`   UNPROVEN           373  (11.6%)
- `18:03:46`   VALUE_TRAP          97  (3.0%)
- `18:03:46`   MISPRICED            4  (0.1%)
- `18:03:46`   CROWDED_SHORT        3  (0.1%)
- `18:03:46` ✅ VERDICT.populated :: 3223 rows classified
- `18:03:46` ✅ VERDICT.discriminates :: 5 classes present: ['UNPROVEN', 'VALUE_TRAP', 'CROWDED_SHORT', 'MISPRICED', 'NO_GAP']
- `18:03:46` ✅ VERDICT.mispriced_minority :: MISPRICED=4 of 3223
- `18:03:46` ✅ VERDICT.rejects_something :: VALUE_TRAP=97 — a classifier that never rejects is not a classifier
## Disqualifier legs must actually fire

- `18:03:46` ✅ DISQ.any_fires :: 524 disqualifier hits total
## MISPRICED book

- `18:03:46`   GOTU   Education & Training Ser gap=+43.2 SI=48.8 evid=2 :: industry inflecting up; estimates stable or rising
- `18:03:46`   AMBA   Semiconductors           gap=+36.7 SI=40.1 evid=2 :: industry inflecting up; estimates stable or rising
- `18:03:46`   CHYM   Banks - Regional         gap=+20.9 SI=47.0 evid=2 :: institutional accumulation off-exchange; industry in
- `18:03:46`   AVAV   Aerospace & Defense      gap=+26.0 SI=43.1 evid=2 :: industry inflecting up; estimates stable or rising
## VALUE_TRAP book — cheap for a reason

- `18:03:46`   CDRO   gap=+70.6 :: industry in structural decline
- `18:03:46`   NWE    gap=+68.5 :: industry in structural decline
- `18:03:46`   TGS    gap=+64.5 :: industry in structural decline
- `18:03:46`   GNE    gap=+61.5 :: industry in structural decline
- `18:03:46`   AZUL   gap=+61.0 :: industry in structural decline
- `18:03:46`   CEPU   gap=+60.1 :: industry in structural decline
- `18:03:46`   JBI    gap=+59.5 :: industry in structural decline
- `18:03:46`   ELPC   gap=+58.9 :: industry in structural decline
- `18:03:46`   TXNM   gap=+57.7 :: industry in structural decline
- `18:03:46`   TGLS   gap=+54.9 :: industry in structural decline
## Additive

- `18:03:46` ✅ ADDITIVE.capture_gap :: preserved
- `18:03:46` ✅ ADDITIVE.structural_importance :: preserved
- `18:03:46` ✅ ADDITIVE.catchup_pct :: preserved
- `18:03:46` ✅ ADDITIVE.revenue_share_pct :: preserved
- `18:03:46` ✅ ADDITIVE.growth_tier :: preserved
## VERDICT

- `18:03:46` ✅ PASS_ALL — mispricings separated from value traps, with evidence
