# ops 3786 — revenue share served on capture-gap + why.html

**Status:** success  
**Duration:** 301.4s  
**Finished:** 2026-07-24T00:07:37+00:00  

## Data

| publishing_share | scored | suppressed | with_dependency |
|---|---|---|---|
| 1063 | 2811 | 1748 | 154 |

## Log
## Feed precondition (v4.1.1)

- `00:02:36` ✅ FEED.version :: engine v4.1.1
- `00:02:36` ✅ FEED.share_published :: 1063 names publish a share
- `00:02:36` ✅ FEED.suppression_reasons :: 1748 names carry an explicit suppression reason
- `00:02:36` ✅ FEED.usd_purity :: 0 non-USD filers publishing (must be 0)
## capture-gap.html v6

- `00:02:37` capture-gap attempt 1: HTTP 200 · 30840 bytes · 0/7
- `00:03:02` capture-gap attempt 2: HTTP 200 · 30840 bytes · 0/7
- `00:03:27` capture-gap attempt 3: HTTP 200 · 30840 bytes · 0/7
- `00:03:52` capture-gap attempt 4: HTTP 200 · 30840 bytes · 0/7
- `00:04:17` capture-gap attempt 5: HTTP 200 · 32303 bytes · 7/7
- `00:04:17` ✅ CG.stamp :: present
- `00:04:17` ✅ CG.rsh_fn :: present
- `00:04:17` ✅ CG.dep_fn :: present
- `00:04:17` ✅ CG.col_rev :: present
- `00:04:17` ✅ CG.col_dep :: present
- `00:04:17` ✅ CG.note :: present
- `00:04:17` ✅ CG.member_rev :: present
- `00:04:17` ✅ CG.KEPT.Most_Undervalued :: intact
- `00:04:17` ✅ CG.KEPT.By_Industry :: intact
- `00:04:17` ✅ CG.KEPT.Full_Ledger :: intact
- `00:04:17` ✅ CG.KEPT.catchup_pct :: intact
- `00:04:17` ✅ CG.KEPT.Default_rank_is_blen :: intact
- `00:04:17` ✅ CG.KEPT.data-lk :: intact
- `00:04:17` ✅ CG.KEPT.data-bk :: intact
## why.html

- `00:04:17` why attempt 1: HTTP 200 · 273413 bytes · 1/5
- `00:04:42` why attempt 2: HTTP 200 · 273413 bytes · 1/5
- `00:05:07` why attempt 3: HTTP 200 · 273413 bytes · 1/5
- `00:05:32` why attempt 4: HTTP 200 · 273413 bytes · 1/5
- `00:05:57` why attempt 5: HTTP 200 · 273413 bytes · 1/5
- `00:06:22` why attempt 6: HTTP 200 · 273413 bytes · 1/5
- `00:06:47` why attempt 7: HTTP 200 · 273413 bytes · 1/5
- `00:07:12` why attempt 8: HTTP 200 · 273413 bytes · 1/5
- `00:07:37` why attempt 9: HTTP 200 · 274341 bytes · 5/5
- `00:07:37` ✅ WHY.rev_tile :: present
- `00:07:37` ✅ WHY.crit_tile :: present
- `00:07:37` ✅ WHY.not_a_share :: present
- `00:07:37` ✅ WHY.usd_label :: present
- `00:07:37` ✅ WHY.capture_fn :: present
- `00:07:37` ✅ WHY.KEPT.P_E_TTM :: intact
- `00:07:37` ✅ WHY.KEPT.PEG :: intact
- `00:07:37` ✅ WHY.KEPT.EV_EBITDA :: intact
- `00:07:37` ✅ WHY.KEPT.SHARES_OUT :: intact
- `00:07:37` ✅ WHY.KEPT.DILUTION :: intact
- `00:07:37` ✅ WHY.KEPT.CAPTURE_GAP :: intact
- `00:07:37` ✅ WHY.KEPT.CATCH-UP :: intact
- `00:07:37` ✅ WHY.KEPT.fillJHVitals :: intact
## Sample — what a reader will actually see

- `00:07:37`   NVDA   rev_share=32.25%   ccy=USD   crit_pctile=98.9   dep=14.9%   
- `00:07:37`   AVGO   rev_share=9.54%    ccy=USD   crit_pctile=82.8   dep=5.0%    
- `00:07:37`   AMD    rev_share=5.17%    ccy=USD   crit_pctile=67.8   dep=5.9%    
- `00:07:37`   TSM    rev_share=—        ccy=TWD   crit_pctile=89.7   dep=5.9%    [filer reports in TWD — not summable with USD p]
- `00:07:37`   SKHY   rev_share=—        ccy=KRW   crit_pctile=31.0   dep=—       [filer reports in KRW — not summable with USD p]
- `00:07:37`   MSFT   rev_share=55.79%   ccy=USD   crit_pctile=89.6   dep=—       
## VERDICT

- `00:07:37` ✅ PASS_ALL — revenue share live on both surfaces, suppression reasons visible
