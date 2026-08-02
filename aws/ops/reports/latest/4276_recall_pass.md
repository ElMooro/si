# ops 4276 -- glued-date recall pass

**Status:** success  
**Duration:** 15.8s  
**Finished:** 2026-08-02T17:41:02+00:00  

## Data

| amount | date | ticker | tx | who |
|---|---|---|---|---|
| $1,001 - $15,000 | 2026-07-30 | TSM | Purchase | Cleo Fields |
| $15,001 - $50,000 | 2026-07-27 | NVDA | Sale | Sam T. Liccardo |
| $1,001 - $15,000 | 2026-07-24 | ARCC | Sale | Pete Sessions |
| $1,001 - $15,000 | 2026-07-20 | FMAO | Purchase | Robert E. Latta |
| $1,001 - $15,000 | 2026-07-20 | BAC | Sale | James A. Himes |
| $15,001 - $50,000 | 2026-07-20 | XOM | Sale | James A. Himes |

## Log
## 1. reparse the zero-row dozen on v1.1

- `17:41:02` invoked: {"ok": true, "new_docs": 20, "parsed": 20, "no_text": 0, "errors": 0, "elapsed_s": 5.8, "trades_total": 132}
- `17:41:02` histogram now: {0: 7, 1: 9, 2: 3, 3: 1, 8: 1, 11: 1, 13: 1, 19: 1, 63: 1} -- trades_total=132
## 2. both chambers, attribution visible

## RESULT

- `17:41:02` ✗   zero-bucket still 7 after v1.1
