# ops 5822 -- owned endpoint context 8k -> 16k

**Status:** success  
**Duration:** 21.1s  
**Finished:** 2026-09-18T20:15:15+00:00  

## Data

| approx_tokens | config | endpoint_status | image | instance | instances_now | max_model_len | model | prompt_chars |
|---|---|---|---|---|---|---|---|---|
|  | jh-owned-coder-cfg-16k-20260918200057 | InService | 6f6c37b935f62cf05e574a292653f18d0d4b3b95 | ml.g5.xlarge | 1 | 16384 | jh-owned-coder-16k-20260918200057 |  |
| 12684 |  |  |  |  |  |  |  | 50737 |

## Log
## 1. The live recipe

- `20:14:54` ✅ already at 16384 tokens
## 4. Proof: a 12k-token prompt round trip

- `20:15:15` ✅ answered in ~20s: {"generated_text": "OK<|im_end|>"}
