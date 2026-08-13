# ops 4643 — DXY predict-the-future engine

**Status:** success  
**Duration:** 48.3s  
**Finished:** 2026-08-13T02:22:38+00:00  

## Data

| candidates | family_prefixes | fn_error | list | members | polarity_rows | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|---|---|---|---|
| [["Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 437], ["Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 327], ["Dollar Shortage Indicators", 17], ["DXY predict Future Moves : Currencies best seen in \"3M\": FED hiking rates strengthen the Dollar", 85], ["DXY: Currencies best seen in \"3M\" : DXY pumping means tightening and liquidity drying up in the Eurodollar system.", 90], ["Emerging Market Sovereign Crisis - Eurodollar crisis", 20], ["Euro Dollar Shortage & Liquidity squeeze", 14], ["EuroDollar - Interest rates", 26]] |  |  |  |  |  |  |  |  |  |  |
|  |  | None |  |  |  |  |  |  |  |  |
|  |  |  | TOP FAMILY (6 lists) | 1132 | 0 | 626 | 0.0 | NONE | MIXED | -2.3 |
|  | [["FRED", 157], ["INTOTHEBLOCK", 147], ["AMEX", 144], ["NASDAQ", 137], ["ECONOMICS", 85], ["TVC", 53], ["GLASSNODE", 48], ["CBOE", 23], ["INDEX", 19], ["CRYPTOCAP", 16], ["USI", 16], ["NYSE", 13]] |  |  |  |  |  |  |  |  |  |

## Log
## pre-dump: top list candidates

- `02:21:50` ✅   [list-exists] top list present: [('Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.', 437), ('Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.', 327), ('Dollar Shortage Indicators', 17)]
## deploy (ops-side) + settle + schedule

- `02:21:52` ✅   [deploy] v1.0.1 live (created=False)
## run + dxy truth

- `02:22:32` ✅   [list-found] list 'TOP FAMILY (6 lists)' (1132 members)
- `02:22:32` ✅   [resolution] 626/1132 resolved (shared cache pool)
- `02:22:32` unsigned-by-design: 0 rows carry mechanical polarity
- `02:22:32` crypto z-based: 1 (e.g. ['COINBASE:LINKUSD'])
- `02:22:32` ✅   [crypto-route] 1 crypto-class rows z-based (route production test)
- `02:22:32` ✅   [dials] TOP TREND -2.3 (MIXED) · REVERSAL 0.0 (NONE)
## edge (with CF purge)

- `02:22:32` CF purge: True
- `02:22:38` CONTROL dxy: len=6609 has_json=True
- `02:22:38` LIQ body: len=6608 count(liq)=0 fetch@3137 liq@-1
- `02:22:38` slice@fetch: :'dim')); fetch('data/top-signals.json?cb='+Date.now()).then(r=>r.json()) .then(d=>{ document.getElementById('asof').tex
- `02:22:38` ⚠ CLOUDFLARE_API_TOKEN absent — probing without purge
- `02:22:38` ✅   [edge] page + payload at the edge
## verdict

- `02:22:38` ✅ TOP SIGNALS LIVE — list 'TOP FAMILY (6 lists)': 626/1132 resolved, 0 signed rows · TREND -2.3 (MIXED) · REVERSAL 0.0 (NONE) · https://justhodl.ai/top-signals.html
