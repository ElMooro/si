# ops 5184 -- auction desk v1.1.0 (explanations)

**Status:** success  
**Duration:** 199.6s  
**Finished:** 2026-09-04T03:10:57+00:00  

## Log
- `03:07:38`   Lambda exists — updating
- `03:07:41` ✅   ✓ updated justhodl-auction-desk
- `03:07:58`    run -> {"ok": true, "elapsed_s": 10.6, "newest_auction": "2026-09-03", "newest_buyback": "2026-09-03", "today": "2026-09-03", "headline": "$12.5B buyback at max \u00b7 $185.5B bills -> risk-on supportive", "notes": ["buybacks 218 operations", "ai note: HTTP 400 {\"type\":\"error\",\"error\":{\"type\":\"inv
- `03:07:58`    version 1.1.0 today 2026-09-03: $12.5B buyback at max · $185.5B bills -> risk-on supportive
- `03:07:58`    8-Week explain lines=9 score_parts=4
- `03:07:58`      Bid-to-cover 3.02 -> Investors asked for $3.02 of bills for every $1 Treasury sold (usual for this tenor lately: 2.77). Higher means more demand.
- `03:07:58`      Indirect bidders 70% -> Share bought by foreign central banks and large funds bidding through dealers -- the 'real money' crowd (usual: 61%). More is stronger.
- `03:07:58`      Dealers 25% -> Share the primary dealers had to take onto their own books because nobody else bid for it (usual: 35%). Less is better -- it means investors, not deal
- `03:07:58`    buyback securities 24, first: {'cusip': '91282CKR1', 'ttm_label': '8m', 'orig_term': '3-Year Note', 'maturity': '2027-05-15', 'par_accepted': 249000000.0}
## page

- `03:10:45`    page carries the explainer: True
- `03:10:57`    facts={'plain': 18, 'gradeBtns': 2, 'tapeBtns': 80, 'secTenor': '8m'} card-click={'open': True, 'title': 'B 8-Week Bill · 2026-09-03', 'rows': 4, 'plain': 9} tape-click={'open': True, 'title': 'B 8-Week Bill · 2026-09-03'} errors=[]
- `03:10:57` ✅    GREEN: explanations live, grade explainer works from cards and tape, tenors on securities
