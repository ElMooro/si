# ops 4630 — barometer + TE join

**Status:** failure  
**Duration:** 112.7s  
**Finished:** 2026-08-12T03:52:50+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | by_prefix | extreme | label | n_unresolved | resolved | shock | stretched | top_extremes |
|---|---|---|---|---|---|---|---|---|
| 15.2 |  | 30.8 | QUIET |  | 408 | 1.3 | 15.2 | [{"symbol": "FRED:PCOPPUSDM", "range_pos_pct": 100.0}, {"symbol": "NASDAQ:TLT", "range_pos_pct": 0.0}, {"symbol": "FRED:HOANBS", "range_pos_pct": 100.0}, {"symbol": "FRED:RRSFS", "range_pos_pct": 100. |
|  | [["NASDAQ", 28], ["ECONOMICS", 27], ["ICEEUR", 8], ["FTSE", 7], ["TVC", 4], ["EUREX", 3], ["ICEUS", 3], ["NYMEX", 1], ["NSE", 1], ["USI", 1], ["TSE", 1], ["NYSE", 1]] |  |  | 92 |  |  |  |  |

## Log
## deploy-settle

- `03:51:28` ✅   [deploy] blackswan v1.8.1 + signal v2.1.3
## purge poisoned negative caches

- `03:51:33` purged 73 poisoned miss stubs
## run + barometer truth

- `03:52:38` ✅   [barometer] barometer 15.2 (QUIET)
- `03:52:38` TE-joined: ['ECONOMICS:CLGDPYY', 'ECONOMICS:CLUR', 'ECONOMICS:CHUR', 'ECONOMICS:HKUR', 'ECONOMICS:USRSYY', 'ECONOMICS:EURSYY', 'ECONOMICS:NLCU', 'ECONOMICS:USBCOI', 'ECONOMICS:FIGDPYY', 'ECONOMICS:CHBCOI', 'ECONOMICS:FIBCOI', 'ECONOMICS:KYGDPYY']
- `03:52:38` ✅   [te-join] 42 ECONOMICS rows via Trading Economics
## UNRESOLVED CENSUS (the 125, named)

- `03:52:38` NYMEX:CL1!-BLACKBULL:WTI · EUREX:FMOG1! · ECONOMICS:WWOPI · ECONOMICS:WWMPMI · ECONOMICS:DEIFOCC · ECONOMICS:USGDPMAN
- `03:52:38` ECONOMICS:USFYGDPG · ECONOMICS:USGDPT · ECONOMICS:EUIS · FTSE:SS03 · TVC:TRJEFFCRB · ICEEUR:ME32!
- `03:52:38` FTSE:4GL1 · FTSE:4EU5 · ICEEUR:MPE2! · NSE:CNXSMALLCAP · ECONOMICS:WWCOMPPMI · ECONOMICS:DENO
- `03:52:38` ECONOMICS:USDFSRI · FTSE:AWHDY01.TR · ECONOMICS:EUEMC · ICEEUR:USW1! · ECONOMICS:USCPMI · ICEUS:AWN1!
- `03:52:38` ECONOMICS:USBOI · ECONOMICS:FIMGDPYY · ECONOMICS:CNNO · TVC:DE10Y-TVC:IT10Y · ECONOMICS:JPMTO · ECONOMICS:EUMPMI
- `03:52:38` ECONOMICS:EUCOMPPMI · ECONOMICS:USKFCOMPI · ECONOMICS:JPCIND · ECONOMICS:USNMEMP · ECONOMICS:USNMBA · NASDAQ:NQUSS50206015N
- `03:52:38` NASDAQ:NQDXUSMLTCG · ECONOMICS:JPJAR · FTSE:SD10 · NASDAQ:NQG50 · NASDAQ:B3010PI · NASDAQ:NQEMASIA3010T
- `03:52:38` NASDAQ:NQEM3010 · NASDAQ:NQHKBANK · NASDAQ:NQASPA35 · NASDAQ:NQEMASIA35 · NASDAQ:NQEM35 · NASDAQ:NQEM30
- `03:52:38` NASDAQ:NQEM5010 · NASDAQ:NQEM5020 · NASDAQ:NQEM50 · USI:YRLO.US · ECONOMICS:JPSBSI · TSE:TPXLVG
- `03:52:38` NASDAQ:NQJP35T · NASDAQ:NQUSB50101010 · NASDAQ:NQTW30 · EUREX:FMMG1! · ICEEUR:MWG2! · NYSE:ADR
- `03:52:38` 1-TVC:IN03Y · TVC:CN01Y · ICEUS:MWL1! · NASDAQ:NQUSB302010N · HOSE:VNINDEX · TVC:VN03Y
- `03:52:38` ICEUS:DX1!-TVC:DXY · CBOE:VIX1Y · ICEEUR:US31!-TVC:US30Y · NASDAQ:NQMAFIT · ICEEUR:I2! · ICEEUR:SA32!
- `03:52:38` TMX:CRA2! · ICEEUR:EON2! · NASDAQ:NQASIAN · NASDAQ:NQBRICT · NASDAQ:NOMXNIN · NASDAQ:NQGRMIT
- `03:52:38` NASDAQ:NQRSKEM · LUXSE:LU0310511422 · SWB:JSGW · FTSE:FEMCRF · CSEMA:MASID · ECONOMICS:USMCEC
- `03:52:38` ECONOMICS:DEZCC · NASDAQ:NQCN55T · NASDAQ:B55PI · NASDAQ:NQEMEA4050T · FTSE:JAPAN.TR · NASDAQ:NQHKBANKN
- `03:52:38` EUREX:FVS2! · ECONOMICS:USRCR
- `03:52:38` ✗   [census-shrunk] CONTRACT MISS — 92 unresolved (92->target <=85; ~85+ fetch-slots/hour keep compounding)
## inject TE key into blackswan env (from SSM)

- `03:52:44` ✅   [te-env] TE_API_KEY present (len=31)
## TE category evidence (for the ECONOMICS residue's next patch)

- `03:52:44` germany: ['Construction Orders', 'Factory Orders', 'Ifo Current Conditions', 'Ifo Expectations', 'New Orders', 'ZEW Current Conditions', 'ZEW Economic Sentiment Index']
- `03:52:45` japan: ['Construction Orders', 'Machine Tool Orders', 'Machinery Orders', 'Machinery Orders YoY', 'New Orders', 'Non Manufacturing PMI', 'Small Business Sentiment']
- `03:52:45` NQ-route z-based: []
- `03:52:45` ⚠ NQ route blocked from AWS (Akamai) — infrastructural wall; route stays for if/when it relents
- `03:52:45` ✅   [nq-route] 0 NQ customs z-based via api.nasdaq.com
- `03:52:45` TE-hist z-based: []
- `03:52:45` ✗   [te-direct] CONTRACT MISS — 0 ECONOMICS residue rows z-based via TE historical
- `03:52:45` EUREX:FVS2!                    UNRESOLVED z=None  n=-    
- `03:52:45` HOSE:VNINDEX                   UNRESOLVED z=None  n=-    
- `03:52:45` MULTPL:SHILLER_PE_RATIO_MONTH  CALM      z=None  n=400  
- `03:52:45` ✗   [deep-routes] CONTRACT MISS — 0/3 deep routes z-based (VSTOXX/TCBS/multpl-slug)
- `03:52:45` NASDAQ:TLT/AMEX:SPY      CALM      z=0.71  n=235  
- `03:52:45` AMEX:XLP/AMEX:XLY        CALM      z=0.01  n=235  
- `03:52:45` AMEX:AGG/AMEX:HYG        CALM      z=1.25  n=235  
- `03:52:45` FX:SONIA3M               CALM      z=0.73  n=292  
- `03:52:45` ✅   [ratio-legs] 3/3 ETF ratio composites z-based (ma200 legs)
- `03:52:45` ICEUS:DX1!-TVC:DXY                     UNRESOLVED z=None  n=-    
- `03:52:45` CBOE:VIX1Y                             UNRESOLVED z=None  n=-    
- `03:52:45` NSE:CNXSMALLCAP                        UNRESOLVED z=None  n=-    
- `03:52:45` ECONOMICS:USINBR-TVC:US03MY            CALM      z=0.78  n=300  
- `03:52:45` TVC:US10Y-MULTPL:SP500_EARNINGS_MONTH  CALM      z=1.54  n=291  
- `03:52:45` TVC:DXY                                CALM      z=0.16  n=253  
- `03:52:45` ✅   [residue-routes] 2/5 new residue routes z-based
- `03:52:45` KRX:KOSPI200       CALM      z=1.05  n=227  
- `03:52:45` INDEX:FTSEMIB      CALM      z=0.11  n=253  
- `03:52:45` EURONEXT:N100      CALM      z=0.09  n=256  
- `03:52:45` CBOT:ZB2!          CALM      z=0.5   n=252  
- `03:52:45` CME:SR32!          NO_HISTORY z=None  n=1    
- `03:52:45` FX_IDC:INRUSD      CALM      z=0.0   n=260  
- `03:52:45` FX_IDC:MXNJPY      CALM      z=0.53  n=260  
- `03:52:45` ECONOMICS:USJO     CALM      z=0.43  n=300  
- `03:52:45` ✅   [class-routes] 6/7 per-class spot-checks z-based
- `03:52:45` CBOT:ZB1!        CALM      z=0.5   n=252  
- `03:52:45` FX_IDC:KRWUSD    CALM      z=0.01  n=260  
- `03:52:45` ECONOMICS:CHUR   NO_HISTORY z=None  n=1    TE latest
- `03:52:45` NASDAQ:VXUS      CALM      z=0.46  n=235  
- `03:52:45` CBOE:VXEEM       CALM      z=0.69  n=400  
- `03:52:45` AMEX:VEA         CALM      z=0.52  n=235  
- `03:52:45` ✅   [alias-z] 4/5 alias spot-checks on z-basis
- `03:52:45` ✅   [history-depth] 331 rows on statistical basis (steady-state ~330 as the hourly cache compounds)
- `03:52:45` ✅   [ffill-composite] SOFR-FEDFUNDS z-based: z=0.42 +0.01 Δ (DoD)
- `03:52:45` ✗   [resolution] CONTRACT MISS — 408/500 — census-attacked; residue enumerated below
## board + edge

- `03:52:50` ✅   [canary-barometer] board carries barometer 15.2 (QUIET)
- `03:52:50` ✅   [edge] edge serves the barometer
## verdict

- `03:52:50` ✗ barometer: 4 red
