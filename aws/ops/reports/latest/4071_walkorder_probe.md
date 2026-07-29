# ops 4071 — PROBE: walk order + source-map producer

**Status:** success  
**Duration:** 9.4s  
**Finished:** 2026-07-29T02:32:42+00:00  

## Data

| agency_first_500 | agency_in_queue | agency_median_idx | already_sourced | page_bytes | page_marker | source_map_armed | source_map_producer | unique_symbols | walk_queue |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  | 188 |  |  |  |  | 10319 | 10131 |
| 117 | 4598 | 4985 |  |  |  |  |  |  |  |
|  |  |  |  |  |  | False | NONE |  |  |
|  |  |  |  | 5971 | v2-ops4067 |  |  |  |  |

## Log
## H1 — where does the payoff sit in the queue?

## Walk queue by prefix (top 18)

- `02:32:34`     3317  ECONOMICS  ← AGENCY PAYOFF
- `02:32:34`     1488  NASDAQ  (venue: low info)
- `02:32:34`      765  FRED  ← AGENCY PAYOFF
- `02:32:34`      593  AMEX  (venue: low info)
- `02:32:34`      454  FTSE
- `02:32:34`      353  TVC  ← AGENCY PAYOFF
- `02:32:34`      330  INDEX
- `02:32:34`      269  NYSE  (venue: low info)
- `02:32:34`      229  COT3
- `02:32:34`      162  CBOE  ← AGENCY PAYOFF
- `02:32:34`      151  FX_IDC
- `02:32:34`      150  INTOTHEBLOCK
- `02:32:34`      143  USI
- `02:32:34`      111  COT
- `02:32:34`      107  SGX
- `02:32:34`       86  DJ
- `02:32:34`       64  LSE  (venue: low info)
- `02:32:34`       62  GLASSNODE
## H1 verdict — position of the payoff

- `02:32:34`   agency-bearing symbols in queue : 4598
- `02:32:34`   first agency symbol at index    : 0
- `02:32:34`   median agency index             : 4985
- `02:32:34`   agency symbols in first 500     : 117
- `02:32:34`   venue symbols in first 500      : 278
- `02:32:34`   → at the observed 340ms/symbol, the MEDIAN agency row is 0.5h into the walk
- `02:32:34`   → under priority ordering the first 4598 symbols walked would ALL be agency-bearing (26 min to full payoff)
## Attribution banked so far, by family

- `02:32:34`      60  source/AMEX
- `02:32:34`      47  source/NASDAQ
- `02:32:34`      18  provider/tvc
- `02:32:34`      16  source/CRYPTOCAP
- `02:32:34`       8  source/NYSE
- `02:32:34`       7  source/CBOE
- `02:32:34`       5  source/EURONEXT
- `02:32:34`       2  country/US
- `02:32:34`       2  source/TSX
- `02:32:34`       2  source/OMXCOP
- `02:32:34`       2  source/ICEUS
- `02:32:34`       1  source/SIX
## H2 — does the monitor's artifact have a producer?

- `02:32:34`   source-map.json generated_at : 2026-07-29T02:21:13.730861+00:00
- `02:32:34`   marker                       : source-map v1.3 ops4070 normalized
- `02:32:41`   fleet size                   : 756 functions
- `02:32:41`   functions named *source-map* : NONE
- `02:32:42`   schedules targeting it       : NONE
## H2 verdict

- `02:32:42`   ✓ CONFIRMED — source-map.json has NO Lambda producer.
- `02:32:42`     It exists only because ops_4070 wrote it by hand.
- `02:32:42`     harvest-monitor.html freezes the moment this session ends. Must be promoted to a scheduled engine.
## Served page check (runner CAN reach the edge)

- `02:32:42`   served bytes  : 5971
- `02:32:42`   served marker : ['v2-ops4067']
- `02:32:42` 
- `02:32:42` PROBE COMPLETE — no code written. Wire op reads these numbers.
