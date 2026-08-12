# ops 4643 — DXY predict-the-future engine

**Status:** failure  
**Duration:** 20.0s  
**Finished:** 2026-08-12T23:58:19+00:00  

## Error

```
SystemExit: 1
```

## Data

| candidates | fn_error | list | members | polarity_rows | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|---|---|---|
| [["Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 437], ["Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 327], ["Dollar Shortage Indicators", 17], ["DXY predict Future Moves : Currencies best seen in \"3M\": FED hiking rates strengthen the Dollar", 85], ["DXY: Currencies best seen in \"3M\" : DXY pumping means tightening and liquidity drying up in the Eurodollar system.", 90], ["DXY: DIFFERENT TYPE OF DXY: IN CURRENCY MARKETS THE UPSIDE AND DOWNSIDE ARE SYMMETRICAL.", 18], ["Emerging Market Sovereign Crisis - Eurodollar crisis", 20], ["Euro Dollar Shortage & Liquidity squeeze", 14]] |  |  |  |  |  |  |  |  |  |
|  | None |  |  |  |  |  |  |  |  |
|  |  | DXY FAMILY (21 lists) | 408 | 22 | 229 | -54.5 | CONFIRMED TURN TO USD-DOWN | USD_UP | 36.4 |

## Log
## pre-dump: dxy/dollar list candidates

- `23:58:00` ✅   [list-exists] dxy/dollar list present: [('Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.', 437), ('Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.', 327), ('Dollar Shortage Indicators', 17)]
## deploy (ops-side) + settle + schedule

- `23:58:01` ✅   [deploy] v1.1.0 live (created=False)
## crypto-closes shape (pre-dump)

- `23:58:02` dates=235 series_keys(30)=['BTC', 'NEAR', 'ADA', 'XLM', 'SOL', 'ETH', 'XRP', 'DOGE', 'SUI', 'LTC', 'XMR', 'LINK', 'ZEC', 'UST', 'HYPE', 'ONDO', 'AVAX', 'ENA', 'TAO', 'BICO', 'CRV', 'UNI', 'DOT', 'FET']
## run + dxy truth

- `23:58:18` crypto z-based: 0 (e.g. [])
- `23:58:18` ✗   [crypto-route] CONTRACT MISS — 0 crypto-class rows on z-basis
- `23:58:18` FX_IDC:KYDUSD              pol=-1 trend=DOWN  rev=REVERSAL_DOWN  z=2.76
- `23:58:18` FX_IDC:USDCNY              pol=+1 trend=DOWN  rev=REVERSAL_DOWN  z=1.67
- `23:58:18` FX_IDC:CNYUSD              pol=-1 trend=UP    rev=REVERSAL_UP    z=1.67
- `23:58:18` FRED:DTWEXAFEGS            pol=+1 trend=UP    rev=REVERSAL_DOWN  z=1.24
- `23:58:18` FRED:DTWEXBGS              pol=+1 trend=UP    rev=REVERSAL_DOWN  z=1.11
- `23:58:18` FX:USDCHF                  pol=+1 trend=UP    rev=REVERSAL_DOWN  z=1.02
- `23:58:18` FX_IDC:CHFUSD              pol=-1 trend=DOWN  rev=REVERSAL_UP    z=1.01
- `23:58:18` FRED:DTWEXEMEGS            pol=+1 trend=DOWN  rev=REVERSAL_DOWN  z=0.86
- `23:58:18` FX:NZDUSD                  pol=-1 trend=UP    rev=NONE           z=0.69
- `23:58:18` FX_IDC:KRWUSD              pol=-1 trend=UP    rev=NONE           z=0.59
- `23:58:18` FX_IDC:USDKRW              pol=+1 trend=DOWN  rev=NONE           z=0.58
- `23:58:18` TVC:US10Y-TVC:DE10Y        pol=+1 trend=UP    rev=NONE           z=0.51
- `23:58:18` TVC:DXY                    pol=+1 trend=UP    rev=REVERSAL_DOWN  z=0.49
- `23:58:18` FX:USDJPY                  pol=+1 trend=UP    rev=REVERSAL_DOWN  z=0.47
- `23:58:18` ✅   [list-found] list 'DXY FAMILY (21 lists)' (408 members)
- `23:58:18` ✗   [resolution] CONTRACT MISS — 229/408 resolved (shared cache pool)
- `23:58:18` ✅   [polarity] 22 mechanically-signed rows
- `23:58:18` ✅   [dials] DXY TREND 36.4 (USD_UP) · REVERSAL -54.5 (CONFIRMED TURN TO USD-DOWN)
## edge

- `23:58:19` ✅   [edge] page + payload at the edge
## verdict

- `23:58:19` ✗ dxy-predict: 2 red
