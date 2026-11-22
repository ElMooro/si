# ops 6908 - FRED's refusal at 06:10 UTC and the bank behind the group (read-only), 2026-11-22T06:23+00:00

**Status:** success  
**Duration:** 14.3s  
**Finished:** 2026-11-22T06:23:41+00:00  

## Data

| banked_documents |
|---|
| 1054 |

## Log
## 1. The ICE BofA group

- `06:23:28` ICE BofA option-adjusted spreads (FRED): 192 series | older than 21 days (asked of FRED on every run): 0 | newest dates {'2026-11-16': 67, '2026-11-17': 55, '2026-11-19': 36, '2026-11-13': 28, '2026-11-18': 6}
## 2. FRED now, with the engine's key

- `06:23:28` the function has a FRED key: True (32 characters)
- `06:23:29`   one dollar series, TWEXEMEGSMTH            200 | 5 rows, count 5
- `06:23:29`   a dollar series that answered, DTWEXBGS    200 | 10 rows, count 10
- `06:23:30`   tag inventory, first page                  200 | 500 rows, count 2012
- `06:23:31`   tag inventory, second page                 200 | 500 rows, count 2012
- `06:23:32`   SOFR search                                200 | 9 rows, count 9
- `06:23:33`   one title, IORB                            200 | 1 rows, count None
- `06:23:34`   one ICE series, BAMLH0A0HYM2               200 | 14 rows, count 14
- `06:23:41`   40 title requests without a pause: 6.5 s (369 a minute) -> {200: 40}
## 3. The bank behind the group

- `06:23:41` banked documents: 1054, 9.1 MB | first ['BOGZ1FA102051003A.json', 'BOGZ1FA102051003Q.json', 'BOGZ1FA102051063A.json', 'BOGZ1FA102051063Q.json'] | IORB document keys ['banked_at', 'id', 'observations', 'source', 'title'], title 'Interest Rate on Reserve Balances (IORB Rate)', banked_at 2026-11-21T06:10:17+00:00, 1944 observations to 2026-11-23
- `06:23:41` ✅ read-only
