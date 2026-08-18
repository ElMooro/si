# 0. env keys

**Status:** success  
**Duration:** 183.7s  
**Finished:** 2026-08-18T01:20:31+00:00  

## Log
- `01:17:37` ✅ env keys set (FMP=True, ANTHROPIC=True)
# 1. settle + invoke

- `01:17:44` ✅ marker settled (attempt 1)
- `01:18:00` ✅ fresh in 15s status=LIVE
# 2. field-level G0

- `01:18:00` ✅ beat_league[0]: DUOT score +100.0 (n=528 reporters)
# 3. truths

- `01:18:00` ✅   row0 surprise+score == recompute (eps +107.6%)
- `01:18:00` ✅   rank monotonic over 250 rows
- `01:18:00` ✅   scanned 42 transcripts -> 24 picks
- `01:18:00`   top pick DUOT pick=100.0 growth=100 mode=rules_only (llm error HTTP Error 400: Bad Request -- self-heals)
- `01:18:01` ✅   transcript spot-check: 'backlog' x18 == independent recount
- `01:18:01` ✅   ai_mode honest: rules_only (llm error HTTP Error 400: Bad Request -- self-he
- `01:18:01` ✅   history born runs=1
# 4. page

- `01:18:01` ✅   committed
- `01:20:31` ✅   SERVED (150s)
# 5. verdict

- `01:20:31` ✅ earnings desk born: beat league + transcript growth calls, every number recomputed independently
