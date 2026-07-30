# ops 4135 — intl price discovery

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-07-30T15:46:55+00:00  

## Data

| bytes | chart_status | distinct_prefixed | has_price | intl_candidates | us_equities | v7_status |
|---|---|---|---|---|---|---|
| 157 |  |  |  |  |  | 401 |
|  | 200 |  | True |  |  |  |
|  |  | 10319 |  | 2423 | 2648 |  |

## Log
## A. yahoo v7/quote BULK — crumb wall or open?

- `15:46:55`   body: {"finance":{"result":null,"error":{"code":"Unauthorized","description":"User is unable to access this feature - https://bit.ly/yahoo-finance-api-feedback"}}}
## B. chart endpoint fallback (per-symbol, no crumb)

- `15:46:55`   regularMarketPrice":2205.0,"fiftyTwoWeekHigh":2535.0,"fiftyT
## C. HIS intl universe — exchange census from watchlists

- `15:46:55`     455  FTSE
- `15:46:55`     229  COT3
- `15:46:55`     150  INTOTHEBLOCK
- `15:46:55`     143  USI
- `15:46:55`     111  COT
- `15:46:55`     107  SGX
- `15:46:55`      87  DJ
- `15:46:55`      65  LSE
- `15:46:55`      62  GLASSNODE
- `15:46:55`      61  SSE
- `15:46:55`      59  EURONEXT
- `15:46:55`      58  SWB
- `15:46:55`      52  HSI
- `15:46:55`      40  CBOEEU
- `15:46:55`      39  SIX
- `15:46:55`      33  TRADEGATE
- `15:46:55`      30  CSEMA
- `15:46:55`      27  XETR
- `15:46:55`      25  MIL
- `15:46:55`      21  TSX
- `15:46:55`      20  1/FRED
- `15:46:55`      20  FWB
- `15:46:55` ✅ DISCOVERY — v7=401 chart=OK intl=2423 across 145 exchanges
