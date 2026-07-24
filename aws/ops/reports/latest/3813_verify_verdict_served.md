# ops 3813 — verdict visible on the page

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-07-24T18:16:09+00:00  

## Log
## Feed precondition

- `18:16:09` ✅ FEED.v5 :: v5.0.1
- `18:16:09` ✅ FEED.classified :: verdict counts: {"UNPROVEN": 373, "VALUE_TRAP": 97, "CROWDED_SHORT": 3, "MISPRICED": 4, "NO_GAP": 2746}
- `18:16:09` ✅ FEED.books :: mispriced=4 traps=25
## Served page v12

- `18:16:09` attempt 1: 53166 bytes · 10/10
- `18:16:09` ✅ SERVED.stamp :: present
- `18:16:09` ✅ SERVED.vdt_fn :: present
- `18:16:09` ✅ SERVED.mispriced_div :: present
- `18:16:09` ✅ SERVED.traps_div :: present
- `18:16:09` ✅ SERVED.verdict_filter :: present
- `18:16:09` ✅ SERVED.verdict_col :: present
- `18:16:09` ✅ SERVED.gloss :: present
- `18:16:09` ✅ SERVED.book_key :: present
- `18:16:09` ✅ SERVED.trap_key :: present
- `18:16:09` ✅ SERVED.regime :: present
## Honesty copy must survive

- `18:16:09` ✅ COPY.does :: present
- `18:16:09` ✅ COPY.the :: present
- `18:16:09` ✅ COPY.research :: present
## Provenance names the new feeds

- `18:16:09` ✅ PROV.estimate_revisions :: cited
- `18:16:09` ✅ PROV.dark_pool :: cited
- `18:16:09` ✅ PROV.finra_short :: cited
- `18:16:09` ✅ PROV.earnings_pead :: cited
- `18:16:09` ✅ PROV.industry_boom :: cited
## Additive — v11 surfaces intact

- `18:16:09` ✅ KEPT.Most_Undervalued :: intact
- `18:16:09` ✅ KEPT.By_Industry :: intact
- `18:16:09` ✅ KEPT.Full_Ledger :: intact
- `18:16:09` ✅ KEPT.How_crucial :: intact
- `18:16:09` ✅ KEPT.function_si( :: intact
- `18:16:09` ✅ KEPT.Default_rank_is_bl :: intact
## What a reader now sees

- `18:16:09`   MISPRICED  GOTU   gap=+43.2% SI=48.8 :: industry inflecting up; estimates stable or ri
- `18:16:09`   MISPRICED  AMBA   gap=+36.7% SI=40.1 :: industry inflecting up; estimates stable or ri
- `18:16:09`   MISPRICED  CHYM   gap=+20.9% SI=47.0 :: institutional accumulation off-exchange; indus
- `18:16:09`   MISPRICED  AVAV   gap=+26.0% SI=43.1 :: industry inflecting up; estimates stable or ri
- `18:16:09`   TRAP       CDRO   gap=+70.6% :: industry in structural decline
- `18:16:09`   TRAP       NWE    gap=+68.5% :: industry in structural decline
- `18:16:09`   TRAP       TGS    gap=+64.5% :: industry in structural decline
- `18:16:09`   TRAP       GNE    gap=+61.5% :: industry in structural decline
- `18:16:09`   TRAP       AZUL   gap=+61.0% :: industry in structural decline
## VERDICT

- `18:16:09` ✅ PASS_ALL — the engine's judgement is now visible and explained
