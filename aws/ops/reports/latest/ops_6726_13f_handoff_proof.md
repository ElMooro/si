# ops 6726 - 13F hand-off chain proof 2026-11-04T02:51+00:00

**Status:** success  
**Duration:** 316.0s  
**Finished:** 2026-11-04T02:57:10+00:00  

## Data

| book_version | desk_match | lag_s | trigger_kind |
|---|---|---|---|
| 1.0.2 | True | 6 | holdings_research_published |

## Log
## 1. deployed code

- `02:51:54` justhodl-13f-positions: modified 2026-11-04T02:50:35, memory 1024, timeout 600, state Active
- `02:51:55` justhodl-13f-book: modified 2026-11-04T02:50:03, memory 2048, timeout 300, state Active
## 2. detector-shaped invoke of the research

- `02:51:55` before: desk 2026-11-03T15:45:47+00:00, book 2026-11-04T00:18:06+00:00
- `02:51:55` async invoke 202 at 02:51:55
## 3. the chain

- `02:57:00` desk refreshed at 2026-11-04T02:56:52+00:00 (+40265 s)
- `02:57:10` book refreshed at 2026-11-04T02:56:58+00:00 (+9532 s)
## 4. what the book says about its trigger

- `02:57:10` book version 1.0.2 generated_at 2026-11-04T02:56:52+00:00 trigger {"kind": "holdings_research_published", "source_generated_at": "2026-11-04T02:52:58.439107+00:00"}
- `02:57:10` desk generated_at 2026-11-04T02:52:58.439107+00:00 source_generated_at 2026-11-04T02:52:55.795077+00:00 | book.source.desk_generated_at 2026-11-04T02:52:58.439107+00:00
- `02:57:10` book managers 18 cohort 15 | desk funds 18/18
- `02:57:10` research log: ['[13f] book hand-off fired (justhodl-13f-book, async)']
- `02:57:10` ✅ PROVEN: a detector-shaped research run published and the book followed it in 6 s, carrying the research's clock
