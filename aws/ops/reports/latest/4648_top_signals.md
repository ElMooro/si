# ops 4643 — DXY predict-the-future engine

**Status:** failure  
**Duration:** 45.9s  
**Finished:** 2026-08-13T02:31:43+00:00  

## Error

```
SystemExit: 1
```

## Data

| candidates | family_prefixes | fn_error | list | members | polarity_rows | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|---|---|---|---|
| [["Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 437], ["Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 327], ["Dollar Shortage Indicators", 17], ["DXY predict Future Moves : Currencies best seen in \"3M\": FED hiking rates strengthen the Dollar", 85], ["DXY: Currencies best seen in \"3M\" : DXY pumping means tightening and liquidity drying up in the Eurodollar system.", 90], ["Emerging Market Sovereign Crisis - Eurodollar crisis", 20], ["Euro Dollar Shortage & Liquidity squeeze", 14], ["EuroDollar - Interest rates", 26]] |  |  |  |  |  |  |  |  |  |  |
|  |  | None |  |  |  |  |  |  |  |  |
|  |  |  | TOP FAMILY (6 lists) | 1132 | 0 | 630 | 0.0 | NONE | MIXED | -2.6 |
|  | [["FRED", 157], ["INTOTHEBLOCK", 147], ["AMEX", 144], ["NASDAQ", 137], ["ECONOMICS", 85], ["TVC", 53], ["GLASSNODE", 48], ["CBOE", 23], ["INDEX", 19], ["CRYPTOCAP", 16], ["USI", 16], ["NYSE", 13]] |  |  |  |  |  |  |  |  |  |

## Log
## pre-dump: top list candidates

- `02:30:58` ✅   [list-exists] top list present: [('Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.', 437), ('Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.', 327), ('Dollar Shortage Indicators', 17)]
## deploy (ops-side) + settle + schedule

- `02:31:00` ✅   [deploy] v1.0.2 live (created=False)
## run + dxy truth

- `02:31:37` ✅   [list-found] list 'TOP FAMILY (6 lists)' (1132 members)
- `02:31:37` ✅   [resolution] 630/1132 resolved (shared cache pool)
- `02:31:37` unsigned-by-design: 0 rows carry mechanical polarity
- `02:31:37` GLASSNODE samples: ['GLASSNODE:USDT_UNISWAPLIQUIDITYUSD', 'GLASSNODE:USDC_UNISWAPLIQUIDITYUSD', 'GLASSNODE:USDT_UNISWAPLIQUIDITY', 'GLASSNODE:MATIC_SUPPLY', 'GLASSNODE:CRV_RECEIVINGADDRESSES', 'GLASSNODE:CRV_ATHDRAWDOWN', 'GLASSNODE:CRV_NEWADDRESSES', 'GLASSNODE:CRV_ADDRESSES', 'GLASSNODE:CRV_MEANVOLUME', 'GLASSNODE:CRV_MEDIANVOLUME', 'GLASSNODE:CRV_TOTALVOLUME', 'GLASSNODE:CRV_MARKETCAP']
- `02:31:37` INTOTHEBLOCK samples: ['INTOTHEBLOCK:BTC_HASHRATE', 'INTOTHEBLOCK:BTC_MINERTOTALFLOWSUSD', 'INTOTHEBLOCK:ETH_MINERTOTALFLOWSUSD', 'INTOTHEBLOCK:BTC_INFLOWTXCOUNT', 'INTOTHEBLOCK:BTC_MINEROUTFLOWSUSD', 'INTOTHEBLOCK:CUSDC_AVGTX', 'INTOTHEBLOCK:CAG_WHALESPERCENTAGE', 'INTOTHEBLOCK:CAG_RETAILPERCENTAGE', 'INTOTHEBLOCK:REAL_RETAIL', 'INTOTHEBLOCK:REAL_RETAILPERCENTAGE', 'INTOTHEBLOCK:WHALE_WHALES', 'INTOTHEBLOCK:WHALE_WHALESPERCENTAGE']
- `02:31:37` CRYPTOCAP samples: ['CRYPTOCAP:USDC.D+CRYPTOCAP:USDT.D', 'CRYPTOCAP:USDT.D', 'CRYPTOCAP:USDC.D', 'CRYPTOCAP:BTC.D', 'CRYPTOCAP:TOTAL3', 'CRYPTOCAP:OTHERS.D', 'CRYPTOCAP:USDT.D+CRYPTOCAP:USDC.D', 'CRYPTOCAP:TOTAL3-CRYPTOCAP:BTC', 'CRYPTOCAP:OTHERS', 'CRYPTOCAP:TOTAL3ES', 'CRYPTOCAP:BTC/TVC:BTPBUND', 'CRYPTOCAP:USDC+CRYPTOCAP:USDT']
- `02:31:37` USI samples: ['USI:YRLO.NQ', 'USI:YRHI.NQ', 'USI:YRLO.US', 'USI:YRHI.US', 'USI:YRLO.DJ', 'USI:YRHI.NY', 'USI:VOL.DNTK.US', 'USI:VAL.ASK.US', 'USI:TVOL.NY', 'USI:VOLD', 'USI:VOL.UPTK.NY', 'USI:ACTV.NY']
- `02:31:37` crypto z-based: 1 (e.g. ['COINBASE:LINKUSD'])
- `02:31:37` cryptocap resolved: 7 · onchain z-based: ['INTOTHEBLOCK:BTC_HASHRATE']
- `02:31:37` ✗   [cryptocap-fam] CONTRACT MISS — 7 CRYPTOCAP rows resolved (TOTAL3/OTHERS.D/mcaps seeding)
- `02:31:37` ✅   [crypto-route] 1 crypto-class rows z-based (route production test)
- `02:31:37` ✅   [dials] TOP TREND -2.6 (MIXED) · REVERSAL 0.0 (NONE)
## edge (with CF purge)

- `02:31:37` CF purge: True
- `02:31:42` CONTROL dxy: len=6609 has_json=True
- `02:31:42` LIQ body: len=6608 count(liq)=0 fetch@3137 liq@-1
- `02:31:42` slice@fetch: :'dim')); fetch('data/top-signals.json?cb='+Date.now()).then(r=>r.json()) .then(d=>{ document.getElementById('asof').tex
- `02:31:42` ⚠ CLOUDFLARE_API_TOKEN absent — probing without purge
- `02:31:43` ✅   [edge] page + payload at the edge
## verdict

- `02:31:43` ✗ liq-indicators: 1 red
