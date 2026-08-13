# ops 4643 — DXY predict-the-future engine

**Status:** success  
**Duration:** 11.4s  
**Finished:** 2026-08-13T01:50:30+00:00  

## Data

| candidates | fn_error | list | members | polarity_rows | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|---|---|---|
| [["Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 437], ["Bonds - Sovereign : Bonds dumping especially sovereign bonds dumping is a major Warning signal that macro is deteriorati", 11], ["BONDS : Fixed income tends to lean itself toward macro more than equities thats more micro", 103], ["Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 327], ["Dollar Shortage Indicators", 17], ["DXY predict Future Moves : Currencies best seen in \"3M\": FED hiking rates strengthen the Dollar", 85], ["DXY: Currencies best seen in \"3M\" : DXY pumping means tightening and liquidity drying up in the Eurodollar system.", 90], ["Emerging Market Sovereign Crisis - Eurodollar crisis", 20]] |  |  |  |  |  |  |  |  |  |
|  | None |  |  |  |  |  |  |  |  |
|  |  | MACRO PREDICT (1 lists) | 65 | 0 | 47 | 0.0 | NONE | MIXED | 6.2 |

## Log
## pre-dump: macro-predict list candidates

- `01:50:19` ✅   [list-exists] macro-predict list present: [('Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.', 437), ('Bonds - Sovereign : Bonds dumping especially sovereign bonds dumping is a major Warning signal that macro is deteriorati', 11), ('BONDS : Fixed income tends to lean itself toward macro more than equities thats more micro', 103)]
## deploy (ops-side) + settle + schedule

- `01:50:20` ✅   [deploy] v1.0.1 live (created=False)
## run + dxy truth

- `01:50:25` ✅   [list-found] list 'MACRO PREDICT (1 lists)' (65 members)
- `01:50:25` ✅   [resolution] 47/65 resolved (shared cache pool)
- `01:50:25` unsigned-by-design: 0 rows carry mechanical polarity
- `01:50:25` ✅   [dials] MACRO TREND 6.2 (MIXED) · REVERSAL 0.0 (NONE)
## edge (with CF purge)

- `01:50:25` CF purge: True
- `01:50:30` CONTROL dxy: len=6610 has_json=True
- `01:50:30` LIQ body: len=6646 count(liq)=0 fetch@3154 liq@-1
- `01:50:30` slice@fetch: :'dim')); fetch('data/macro-predict.json?cb='+Date.now()).then(r=>r.json()) .then(d=>{ document.getElementById('asof').t
- `01:50:30` ⚠ CLOUDFLARE_API_TOKEN absent — probing without purge
- `01:50:30` ✅   [edge] page + payload at the edge
## verdict

- `01:50:30` ✅ MACRO PREDICT LIVE — list 'MACRO PREDICT (1 lists)': 47/65 resolved, 0 signed rows · TREND 6.2 (MIXED) · REVERSAL 0.0 (NONE) · https://justhodl.ai/macro-predict.html
