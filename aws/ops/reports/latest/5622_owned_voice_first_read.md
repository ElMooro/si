# ops 5622 -- the market read through the owned model

**Status:** success  
**Duration:** 189.6s  
**Finished:** 2026-09-17T19:21:34+00:00  

## Data

| ai_generated_at | blockers | calls | calls_logged | calls_this_read | decision_status | deterministic | elapsed_s | latency_s | llm_path | opportunities | origin | owned_error | owned_pending | owned_state | public_stances | public_voice | read_id | read_path | scoreboard_voice | settled_at | stances | voice |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | True | 36.4 |  | governed-router | glm: glm failed: HTTP Error 429: Too Many Requests |  | owned:qwen2-5-coder-7b-instruct@c03e6d358207 | None | req-5270bb75bc38f3937c6c47ae | queued |  |  | 20260917T191901Z |  |  |  |  |  |
|  | 4 | 2 | 2 |  | ADVISORY_ONLY |  |  | 137 |  | 3 |  |  |  |  |  |  |  |  |  |  | {'stocks': 'SELECTIVE', 'bonds': 'NEUTRAL', 'metals': 'HOLD', 'crypto': 'HOLD'} | owned |
| 2026-09-17T19:21:31.621661+00:00 |  |  |  | 2 |  |  |  |  |  |  |  |  |  |  | {'stocks': 'SELECTIVE', 'bonds': 'NEUTRAL', 'metals': 'HOLD', 'crypto': 'HOLD'} | owned |  | owned | online (owned model owned:qwen2-5-coder-7b-instruct@c03e6d358207, in this account, with your notes) | 2026-09-17T19:21:18.934136+00:00 |  |  |

## Log
## 1. Force a governed read

## 2. Tick until the owned answer settles (endpoint may be waking from zero)

- `19:20:16` t+ 1 min  owned=running  endpoint=Updating instances=0  tick=done
- `19:21:33` t+ 2 min  owned=done  endpoint=InService instances=1  tick=done
## 3. What the owned model said

- `19:21:33` overall: The market is in a mildly supportive regime with a risk-off posture, indicating caution. Breadth is selective, and the crisis composite is neutral, suggesting a cautious approach to stocks. Bonds are in a calm regime, and metals are in a neutral stance. Crypto is in a neutral stance, with Bitcoin showing a slight bullish signal.
- `19:21:33` macro: Mildly supportive regime with a risk-off posture, neutral crisis composite, calm bonds, and neutral metals.
- `19:21:33` stocks [SELECTIVE]: Breadth is selective, and the crisis composite is neutral, suggesting a cautious approach to stocks.
- `19:21:33` bonds [NEUTRAL]: Bonds are in a calm regime, indicating stable conditions.
- `19:21:33` metals [HOLD]: Metals are in a neutral stance, suggesting no strong action is needed.
- `19:21:33` crypto [HOLD]: Crypto is in a neutral stance, with Bitcoin showing a slight bullish signal.
- `19:21:33` opportunity: NCMI LONG 21d -- Triggered in the bottom process, showing strong potential for recovery.
- `19:21:33` opportunity: OSIS LONG 21d -- Triggered in the bottom process, showing strong potential for recovery.
- `19:21:33` opportunity: CCB LONG 21d -- Triggered in the bottom process, showing strong potential for recovery.
- `19:21:33` call: TSM UP 21d conf=0.7 -- TSM has a strong bullish signal from the fusion engine and is a key component of the US equity index.
- `19:21:33` call: AMZN UP 21d conf=0.6 -- AMZN has a slight bullish signal from the fusion engine, but it is in a reduced capital allocation.
## 4. The public page projection (data/ai.json)

- `19:21:34` ✅ owned-voice read live in 190 s end to end
