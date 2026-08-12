# ops 4630 — barometer + TE join

**Status:** success  
**Duration:** 32.1s  
**Finished:** 2026-08-12T03:21:42+00:00  

## Data

| barometer | by_prefix | extreme | label | n_unresolved | resolved | shock | stretched | top_extremes |
|---|---|---|---|---|---|---|---|---|
| 13.9 |  | 27.2 | QUIET |  | 388 | 1.5 | 15.4 | [{"symbol": "FRED:PCOPPUSDM", "range_pos_pct": 100.0}, {"symbol": "NASDAQ:TLT", "range_pos_pct": 0.0}, {"symbol": "FRED:HOANBS", "range_pos_pct": 100.0}, {"symbol": "FRED:RRSFS", "range_pos_pct": 100. |
|  | [["ECONOMICS", 29], ["NASDAQ", 29], ["ICEEUR", 8], ["FTSE", 7], ["EUREX", 6], ["TVC", 5], ["MULTPL", 4], ["ICEUS", 3], ["AMEX", 3], ["NYSE", 2], ["NYMEX", 1], ["SGX", 1]] |  |  | 112 |  |  |  |  |

## Log
## deploy-settle

- `03:21:11` ✅   [deploy] blackswan v1.6.1 + signal v2.1.3
## purge poisoned negative caches

- `03:21:17` purged 47 poisoned miss stubs
## run + barometer truth

- `03:21:36` ✅   [barometer] barometer 13.9 (QUIET)
- `03:21:36` TE-joined: ['ECONOMICS:CLGDPYY', 'ECONOMICS:CLUR', 'ECONOMICS:CHUR', 'ECONOMICS:HKUR', 'ECONOMICS:USRSYY', 'ECONOMICS:EURSYY', 'ECONOMICS:NLCU', 'ECONOMICS:USBCOI', 'ECONOMICS:FIGDPYY', 'ECONOMICS:CHBCOI', 'ECONOMICS:FIBCOI', 'ECONOMICS:KYGDPYY']
- `03:21:36` ✅   [te-join] 42 ECONOMICS rows via Trading Economics
## UNRESOLVED CENSUS (the 125, named)

- `03:21:36` NYMEX:CL1!-BLACKBULL:WTI · EUREX:FMOG1! · ECONOMICS:WWOPI · ECONOMICS:WWMPMI · NYSE:C+NYSE:BAC+NYSE:WFC+NYSE:JPM · ECONOMICS:DEIFOCC
- `03:21:36` ECONOMICS:USGDPMAN · ECONOMICS:USFYGDPG · ECONOMICS:USGDPT · ECONOMICS:USINBR-TVC:US01MY · ECONOMICS:USINBR-TVC:US03MY · SGX:SGP2!
- `03:21:36` ECONOMICS:EUIS · FTSE:SS03 · TVC:TRJEFFCRB · EUREX:FMAA1! · ICEEUR:ME32! · FTSE:4GL1
- `03:21:36` FTSE:4EU5 · ICEEUR:MPE2! · NSE:CNXSMALLCAP · ECONOMICS:WWCOMPPMI · ECONOMICS:DENO · ECONOMICS:USDFSRI
- `03:21:36` FTSE:AWHDY01.TR · ECONOMICS:EUEMC · ICEEUR:USW1! · ECONOMICS:USCPMI · ICEUS:AWN1! · ECONOMICS:USBOI
- `03:21:36` ECONOMICS:FIMGDPYY · ECONOMICS:CNNO · TVC:DE10Y-TVC:IT10Y · ECONOMICS:JPMTO · ECONOMICS:EUMPMI · ECONOMICS:EUCOMPPMI
- `03:21:36` ECONOMICS:USKFCOMPI · ECONOMICS:JPCIND · ECONOMICS:USNMEMP · ECONOMICS:USNMBA · NASDAQ:NQUSS50206015N · NASDAQ:NQDXUSMLTCG
- `03:21:36` ECONOMICS:JPJAR · FTSE:SD10 · NASDAQ:NQG50 · NASDAQ:B3010PI · NASDAQ:NQEMASIA3010T · NASDAQ:NQEM3010
- `03:21:36` NASDAQ:NQHKBANK · NASDAQ:NQASPA35 · NASDAQ:NQEMASIA35 · NASDAQ:NQEM35 · NASDAQ:NQEM30 · NASDAQ:NQEM5010
- `03:21:36` NASDAQ:NQEM5020 · NASDAQ:NQEM50 · AMEX:RXI/AMEX:KXI · USI:YRLO.US · ECONOMICS:JPSBSI · TSE:TPXLVG
- `03:21:36` NASDAQ:NQJP35T · NASDAQ:NQUSB50101010 · NASDAQ:NQTW30 · EUREX:FMMG1! · ICEEUR:MWG2! · NYSE:ADR
- `03:21:36` 1-TVC:IN03Y · TVC:CN01Y · TVC:US10Y-MULTPL:SP500_EARNINGS_MONTH · MULTPL:SP500_EARNINGS_YEAR · MULTPL:SP500_EARNINGS_MONTH · MULTPL:SP500_EARNINGS_YIELD_MONTH
- `03:21:36` MULTPL:SHILLER_PE_RATIO_MONTH · ICEUS:MWL1! · EUREX:FMEA2! · AMEX:XLP/AMEX:XLY · OANDA:SG30SGD · NASDAQ:NQUSB302010N
- `03:21:36` EUREX:FMXU2! · AMEX:AGG/AMEX:HYG · NASDAQ:TLT/AMEX:SPY · HOSE:VNINDEX · TVC:VN03Y · ICEUS:DX1!-TVC:DXY
- `03:21:36` CBOE:VIX1Y · ICEEUR:US31!-TVC:US30Y · NASDAQ:NQMAFIT · ICEEUR:I2! · FX:SONIA3M · ICEEUR:SA32!
- `03:21:36` TMX:CRA2! · ICEEUR:EON2! · NASDAQ:NQASIAN · NASDAQ:NQBRICT · NASDAQ:NOMXNIN · NASDAQ:NQGRMIT
- `03:21:36` NASDAQ:NQRSKEM · LUXSE:LU0310511422 · SWB:JSGW · FTSE:FEMCRF · CSEMA:MASID · ECONOMICS:USMCEC
- `03:21:36` ECONOMICS:DEZCC · NASDAQ:NQCN55T · NASDAQ:B55PI · NASDAQ:NQEMEA4050T · FTSE:JAPAN.TR · NASDAQ:NQHKBANKN
- `03:21:36` CBOT:UB2! · EUREX:FVS2! · ASX24:AP1! · ECONOMICS:USRCR
- `03:21:36` ✅   [census-shrunk] 112 unresolved (125->112 this arc; ~85+ fetch-slots/hour keep compounding)
- `03:21:36` KRX:KOSPI200       CALM      z=1.05  n=227  
- `03:21:36` INDEX:FTSEMIB      CALM      z=0.11  n=253  
- `03:21:36` EURONEXT:N100      CALM      z=0.09  n=256  
- `03:21:36` CBOT:ZB2!          CALM      z=0.5   n=252  
- `03:21:36` CME:SR32!          NO_HISTORY z=None  n=1    
- `03:21:36` FX_IDC:INRUSD      CALM      z=0.0   n=260  
- `03:21:36` FX_IDC:MXNJPY      CALM      z=0.53  n=260  
- `03:21:36` ECONOMICS:USJO     CALM      z=0.43  n=300  
- `03:21:36` ✅   [class-routes] 6/7 per-class spot-checks z-based
- `03:21:36` CBOT:ZB1!        CALM      z=0.5   n=252  
- `03:21:36` FX_IDC:KRWUSD    CALM      z=0.01  n=260  
- `03:21:36` ECONOMICS:CHUR   NO_HISTORY z=None  n=1    TE latest
- `03:21:36` NASDAQ:VXUS      CALM      z=0.46  n=235  
- `03:21:36` CBOE:VXEEM       NO_HISTORY z=None  n=1    
- `03:21:36` AMEX:VEA         CALM      z=0.52  n=235  
- `03:21:36` ✅   [alias-z] 4/5 alias spot-checks on z-basis
- `03:21:36` ✅   [history-depth] 293 rows on statistical basis (steady-state ~330 as the hourly cache compounds)
- `03:21:36` ✅   [ffill-composite] SOFR-FEDFUNDS z-based: z=0.42 +0.01 Δ (DoD)
- `03:21:36` ✅   [resolution] 388/500 — census-attacked; residue enumerated below
## board + edge

- `03:21:42` ✅   [canary-barometer] board carries barometer 13.9 (QUIET)
- `03:21:42` ✅   [edge] edge serves the barometer
## verdict

- `03:21:42` ✅ BAROMETER LIVE — 13.9 (QUIET): shock 1.5% / extreme 27.2% / stretched 15.4% · 388/500 resolved (+TE join, ffill composites) · on the physical board and the page
