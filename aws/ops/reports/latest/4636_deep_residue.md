# ops 4630 — barometer + TE join

**Status:** success  
**Duration:** 39.5s  
**Finished:** 2026-08-12T03:57:27+00:00  

## Data

| barometer | by_prefix | extreme | label | n_unresolved | resolved | shock | stretched | top_extremes |
|---|---|---|---|---|---|---|---|---|
| 15.1 |  | 30.6 | QUIET |  | 414 | 1.3 | 15.3 | [{"symbol": "FRED:PCOPPUSDM", "range_pos_pct": 100.0}, {"symbol": "NASDAQ:TLT", "range_pos_pct": 0.0}, {"symbol": "FRED:HOANBS", "range_pos_pct": 100.0}, {"symbol": "FRED:RRSFS", "range_pos_pct": 100. |
|  | [["NASDAQ", 28], ["ECONOMICS", 21], ["ICEEUR", 8], ["FTSE", 7], ["TVC", 4], ["EUREX", 3], ["ICEUS", 3], ["NYMEX", 1], ["NSE", 1], ["USI", 1], ["TSE", 1], ["NYSE", 1]] |  |  | 86 |  |  |  |  |

## Log
## deploy-settle

- `03:56:48` ✅   [deploy] blackswan v1.8.1 + signal v2.1.3
## purge poisoned negative caches

- `03:57:00` purged 73 poisoned miss stubs
## run + barometer truth

- `03:57:19` ✅   [barometer] barometer 15.1 (QUIET)
- `03:57:19` TE-joined: ['ECONOMICS:CLGDPYY', 'ECONOMICS:CLUR', 'ECONOMICS:CHUR', 'ECONOMICS:HKUR', 'ECONOMICS:USRSYY', 'ECONOMICS:EURSYY', 'ECONOMICS:NLCU', 'ECONOMICS:USBCOI', 'ECONOMICS:FIGDPYY', 'ECONOMICS:CHBCOI', 'ECONOMICS:FIBCOI', 'ECONOMICS:KYGDPYY']
- `03:57:19` ✅   [te-join] 42 ECONOMICS rows via Trading Economics
## UNRESOLVED CENSUS (the 125, named)

- `03:57:19` NYMEX:CL1!-BLACKBULL:WTI · EUREX:FMOG1! · ECONOMICS:WWOPI · ECONOMICS:WWMPMI · ECONOMICS:USGDPMAN · ECONOMICS:USFYGDPG
- `03:57:20` ECONOMICS:USGDPT · ECONOMICS:EUIS · FTSE:SS03 · TVC:TRJEFFCRB · ICEEUR:ME32! · FTSE:4GL1
- `03:57:20` FTSE:4EU5 · ICEEUR:MPE2! · NSE:CNXSMALLCAP · ECONOMICS:WWCOMPPMI · ECONOMICS:USDFSRI · FTSE:AWHDY01.TR
- `03:57:20` ECONOMICS:EUEMC · ICEEUR:USW1! · ECONOMICS:USCPMI · ICEUS:AWN1! · ECONOMICS:USBOI · ECONOMICS:FIMGDPYY
- `03:57:20` TVC:DE10Y-TVC:IT10Y · ECONOMICS:EUMPMI · ECONOMICS:EUCOMPPMI · ECONOMICS:USKFCOMPI · ECONOMICS:JPCIND · ECONOMICS:USNMEMP
- `03:57:20` ECONOMICS:USNMBA · NASDAQ:NQUSS50206015N · NASDAQ:NQDXUSMLTCG · ECONOMICS:JPJAR · FTSE:SD10 · NASDAQ:NQG50
- `03:57:20` NASDAQ:B3010PI · NASDAQ:NQEMASIA3010T · NASDAQ:NQEM3010 · NASDAQ:NQHKBANK · NASDAQ:NQASPA35 · NASDAQ:NQEMASIA35
- `03:57:20` NASDAQ:NQEM35 · NASDAQ:NQEM30 · NASDAQ:NQEM5010 · NASDAQ:NQEM5020 · NASDAQ:NQEM50 · USI:YRLO.US
- `03:57:20` TSE:TPXLVG · NASDAQ:NQJP35T · NASDAQ:NQUSB50101010 · NASDAQ:NQTW30 · EUREX:FMMG1! · ICEEUR:MWG2!
- `03:57:20` NYSE:ADR · 1-TVC:IN03Y · TVC:CN01Y · ICEUS:MWL1! · NASDAQ:NQUSB302010N · HOSE:VNINDEX
- `03:57:20` TVC:VN03Y · ICEUS:DX1!-TVC:DXY · CBOE:VIX1Y · ICEEUR:US31!-TVC:US30Y · NASDAQ:NQMAFIT · ICEEUR:I2!
- `03:57:20` ICEEUR:SA32! · TMX:CRA2! · ICEEUR:EON2! · NASDAQ:NQASIAN · NASDAQ:NQBRICT · NASDAQ:NOMXNIN
- `03:57:20` NASDAQ:NQGRMIT · NASDAQ:NQRSKEM · LUXSE:LU0310511422 · SWB:JSGW · FTSE:FEMCRF · CSEMA:MASID
- `03:57:20` ECONOMICS:USMCEC · NASDAQ:NQCN55T · NASDAQ:B55PI · NASDAQ:NQEMEA4050T · FTSE:JAPAN.TR · NASDAQ:NQHKBANKN
- `03:57:20` EUREX:FVS2! · ECONOMICS:USRCR
- `03:57:20` ✅   [census-shrunk] 86 unresolved (key-first rerun; TE-direct live; ~85+ fetch-slots/hour keep compounding)
## inject TE key into blackswan env (from SSM)

- `03:57:20` ✅   [te-env] TE_API_KEY present (len=31)
## TE category evidence (for the ECONOMICS residue's next patch)

- `03:57:21` germany: ['Construction Orders', 'Factory Orders', 'Ifo Current Conditions', 'Ifo Expectations', 'New Orders', 'ZEW Current Conditions', 'ZEW Economic Sentiment Index']
- `03:57:21` japan: ['Construction Orders', 'Machine Tool Orders', 'Machinery Orders', 'Machinery Orders YoY', 'New Orders', 'Non Manufacturing PMI', 'Small Business Sentiment']
- `03:57:21` NQ-route z-based: []
- `03:57:21` ⚠ NQ route blocked from AWS (Akamai) — infrastructural wall; route stays for if/when it relents
- `03:57:21` ✅   [nq-route] 0 NQ customs z-based via api.nasdaq.com
- `03:57:21` TE-hist z-based: []
- `03:57:21` ⚠ TE /historical returns empty with valid key — plan-scope wall (te-feed's /country works); route stays armed for a plan upgrade
- `03:57:21` ✅   [te-direct] 0 TE-historical rows (armed; plan-scope dependent)
- `03:57:21` EUREX:FVS2!                    UNRESOLVED z=None  n=-    
- `03:57:21` HOSE:VNINDEX                   UNRESOLVED z=None  n=-    
- `03:57:21` MULTPL:SHILLER_PE_RATIO_MONTH  CALM      z=None  n=400  
- `03:57:21` ⚠ VSTOXX/TCBS blocked from AWS egress this run — same wall class as Nasdaq Akamai; routes stay armed with own budgets
- `03:57:21` ✅   [deep-routes] 0/3 deep routes z-based (armed; external-egress dependent)
- `03:57:21` NASDAQ:TLT/AMEX:SPY      CALM      z=0.71  n=235  
- `03:57:21` AMEX:XLP/AMEX:XLY        CALM      z=0.01  n=235  
- `03:57:21` AMEX:AGG/AMEX:HYG        CALM      z=1.25  n=235  
- `03:57:21` FX:SONIA3M               CALM      z=0.73  n=292  
- `03:57:21` ✅   [ratio-legs] 3/3 ETF ratio composites z-based (ma200 legs)
- `03:57:21` ICEUS:DX1!-TVC:DXY                     UNRESOLVED z=None  n=-    
- `03:57:21` CBOE:VIX1Y                             UNRESOLVED z=None  n=-    
- `03:57:21` NSE:CNXSMALLCAP                        UNRESOLVED z=None  n=-    
- `03:57:21` ECONOMICS:USINBR-TVC:US03MY            CALM      z=0.78  n=300  
- `03:57:21` TVC:US10Y-MULTPL:SP500_EARNINGS_MONTH  CALM      z=1.54  n=291  
- `03:57:21` TVC:DXY                                CALM      z=0.16  n=253  
- `03:57:21` ✅   [residue-routes] 2/5 new residue routes z-based
- `03:57:21` KRX:KOSPI200       CALM      z=1.05  n=227  
- `03:57:21` INDEX:FTSEMIB      CALM      z=0.11  n=253  
- `03:57:21` EURONEXT:N100      CALM      z=0.09  n=256  
- `03:57:21` CBOT:ZB2!          CALM      z=0.5   n=252  
- `03:57:21` CME:SR32!          NO_HISTORY z=None  n=1    
- `03:57:21` FX_IDC:INRUSD      CALM      z=0.0   n=260  
- `03:57:21` FX_IDC:MXNJPY      CALM      z=0.53  n=260  
- `03:57:21` ECONOMICS:USJO     CALM      z=0.43  n=300  
- `03:57:21` ✅   [class-routes] 6/7 per-class spot-checks z-based
- `03:57:21` CBOT:ZB1!        CALM      z=0.5   n=252  
- `03:57:21` FX_IDC:KRWUSD    CALM      z=0.01  n=260  
- `03:57:21` ECONOMICS:CHUR   NO_HISTORY z=None  n=1    TE latest
- `03:57:21` NASDAQ:VXUS      CALM      z=0.46  n=235  
- `03:57:21` CBOE:VXEEM       NO_HISTORY z=None  n=1    
- `03:57:21` AMEX:VEA         CALM      z=0.52  n=235  
- `03:57:21` ✅   [alias-z] 4/5 alias spot-checks on z-basis
- `03:57:21` ✅   [history-depth] 336 rows on statistical basis (steady-state ~330 as the hourly cache compounds)
- `03:57:21` ✅   [ffill-composite] SOFR-FEDFUNDS z-based: z=0.42 +0.01 Δ (DoD)
- `03:57:21` ✅   [resolution] 414/500 — census-attacked; residue enumerated below
## board + edge

- `03:57:26` ✅   [canary-barometer] board carries barometer 15.1 (QUIET)
- `03:57:27` ✅   [edge] edge serves the barometer
## verdict

- `03:57:27` ✅ BAROMETER LIVE — 15.1 (QUIET): shock 1.3% / extreme 30.6% / stretched 15.3% · 414/500 resolved (+TE join, ffill composites) · on the physical board and the page
