# ops 4643 — DXY predict-the-future engine

**Status:** success  
**Duration:** 97.0s  
**Finished:** 2026-08-13T15:46:07+00:00  

## Data

| candidates | family_prefixes | fn_error | list | members | polarity_rows | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|---|---|---|---|
| [["Bitcoin - Global Liquidity: GOLD ALWAYS BOTTOM BEFORE BITCOIN", 248], ["Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 437], ["Bottom Indicators", 500], ["Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 327], ["Dollar Shortage Indicators", 17], ["DXY predict Future Moves : Currencies best seen in \"3M\": FED hiking rates strengthen the Dollar", 85], ["DXY: Currencies best seen in \"3M\" : DXY pumping means tightening and liquidity drying up in the Eurodollar system.", 90], ["Emerging Market Sovereign Crisis - Eurodollar crisis", 20]] |  |  |  |  |  |  |  |  |  |  |
|  |  | None |  |  |  |  |  |  |  |  |
|  |  |  | BOTTOM FAMILY (6 lists) | 1063 | 0 | 593 | 0.0 | NONE | MIXED | -1.0 |
|  | [["AMEX", 153], ["NASDAQ", 150], ["INTOTHEBLOCK", 147], ["FRED", 112], ["ECONOMICS", 59], ["TVC", 49], ["GLASSNODE", 46], ["INDEX", 24], ["NYSE", 20], ["CBOE", 20], ["FTSE", 15], ["CRYPTOCAP", 15]] |  |  |  |  |  |  |  |  |  |

## Log
## pre-dump: bottom list candidates

- `15:44:31` ✅   [list-exists] bottom list present: [('Bitcoin - Global Liquidity: GOLD ALWAYS BOTTOM BEFORE BITCOIN', 248), ('Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.', 437), ('Bottom Indicators', 500)]
## deploy (ops-side) + settle + schedule

- `15:44:32` ✅   [deploy] v1.0.0 live (created=False)
- `15:44:33` hourly schedule created
## run + dxy truth

- `15:46:01` ✅   [list-found] list 'BOTTOM FAMILY (6 lists)' (1063 members)
- `15:46:01` ✅   [resolution] 593/1063 resolved (shared cache pool)
- `15:46:01` unsigned-by-design: 0 rows carry mechanical polarity
- `15:46:01` crypto z-based: 1 (e.g. ['COINBASE:LINKUSD'])
- `15:46:01` ✅   [crypto-route] 1 crypto-class rows z-based (route production test)
- `15:46:01` ✅   [dials] BOTTOM TREND -1.0 (MIXED) · REVERSAL 0.0 (NONE)
## edge (with CF purge)

- `15:46:01` CF purge: True
- `15:46:07` CONTROL dxy: len=6609 has_json=True
- `15:46:07` LIQ body: len=6565 count(liq)=0 fetch@3169 liq@-1
- `15:46:07` slice@fetch: :'dim')); fetch('data/bottom-signals.json?cb='+Date.now()).then(r=>r.json()) .then(d=>{ document.getElementById('asof').
- `15:46:07` ⚠ CLOUDFLARE_API_TOKEN absent — probing without purge
- `15:46:07` ✅   [edge] page + payload at the edge
## verdict

- `15:46:07` ✅ BOTTOM SIGNALS LIVE — list 'BOTTOM FAMILY (6 lists)': 593/1063 resolved, 0 signed rows · TREND -1.0 (MIXED) · REVERSAL 0.0 (NONE) · https://justhodl.ai/bottom-signals.html
