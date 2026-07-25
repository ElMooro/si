# ops 3887 — which claimed secrets source is actually real, and did news-wire even redeploy

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-07-25T21:58:48+00:00  

## Data

| confluence_meta_key_len | confluence_meta_key_present | confluence_meta_placeholder | equity_research_key_len | equity_research_key_present | equity_research_placeholder | flows_ai_analysis_key_len | flows_ai_analysis_key_present | news_wire_all_env_keys | news_wire_current_anthropic_key | news_wire_last_modified |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | False | False |  |  |  |  |  |  |  |  |
|  |  |  | 108 | True | False |  |  |  |  |  |
|  |  |  |  |  |  | 108 | True |  |  |  |
|  |  |  |  |  |  |  |  | ['ANTHROPIC_API_KEY', 'FMP_KEY', 'MAX_NEW_TO_SCORE', 'NEWSAPI_KEY', 'S3_BUCKET', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID'] | PLACEHOLDER_REPLACE_VIA_AWS_CONSOLE_OR_OPS | 2026-07-25T21:38:23.000+0000 |

## Log
## 1. does justhodl-confluence-meta actually have a real ANTHROPIC_API_KEY

- `21:58:48`   all env keys on confluence-meta: ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY']
## 2. does justhodl-equity-research (flows-ai-analysis's PROVEN-working source) have it

## 3. sanity check: does flows-ai-analysis ITSELF currently have a real key (the proof this works)

## 4. did news-wire's Lambda actually get touched by the last deploy at all (config-only change - check LastModified / CodeSha256 timestamp)

## 5. verdict

- `21:58:48` ✅ PROBE COMPLETE
