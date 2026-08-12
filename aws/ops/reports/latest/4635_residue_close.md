# ops 4630 — barometer + TE join

**Status:** failure  
**Duration:** 82.4s  
**Finished:** 2026-08-12T03:39:14+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | by_prefix | extreme | label | n_unresolved | resolved | shock | stretched | top_extremes |
|---|---|---|---|---|---|---|---|---|
| 14.7 |  | 29.1 | QUIET |  | 403 | 1.4 | 16.1 | [{"symbol": "FRED:PCOPPUSDM", "range_pos_pct": 100.0}, {"symbol": "NASDAQ:TLT", "range_pos_pct": 0.0}, {"symbol": "FRED:HOANBS", "range_pos_pct": 100.0}, {"symbol": "FRED:RRSFS", "range_pos_pct": 100. |
|  | [["NASDAQ", 28], ["ECONOMICS", 27], ["ICEEUR", 8], ["FTSE", 7], ["EUREX", 4], ["TVC", 4], ["ICEUS", 3], ["NYMEX", 1], ["SGX", 1], ["NSE", 1], ["USI", 1], ["TSE", 1]] |  |  | 97 |  |  |  |  |

## Log
## deploy-settle

- `03:37:53` ✅   [deploy] blackswan v1.7.1 + signal v2.1.3
## purge poisoned negative caches

- `03:38:01` purged 51 poisoned miss stubs
## run + barometer truth

- `03:39:08` ✅   [barometer] barometer 14.7 (QUIET)
- `03:39:08` TE-joined: ['ECONOMICS:CLGDPYY', 'ECONOMICS:CLUR', 'ECONOMICS:CHUR', 'ECONOMICS:HKUR', 'ECONOMICS:USRSYY', 'ECONOMICS:EURSYY', 'ECONOMICS:NLCU', 'ECONOMICS:USBCOI', 'ECONOMICS:FIGDPYY', 'ECONOMICS:CHBCOI', 'ECONOMICS:FIBCOI', 'ECONOMICS:KYGDPYY']
- `03:39:08` ✅   [te-join] 42 ECONOMICS rows via Trading Economics
## UNRESOLVED CENSUS (the 125, named)

- `03:39:08` NYMEX:CL1!-BLACKBULL:WTI · EUREX:FMOG1! · ECONOMICS:WWOPI · ECONOMICS:WWMPMI · ECONOMICS:DEIFOCC · ECONOMICS:USGDPMAN
- `03:39:08` ECONOMICS:USFYGDPG · ECONOMICS:USGDPT · SGX:SGP2! · ECONOMICS:EUIS · FTSE:SS03 · TVC:TRJEFFCRB
- `03:39:08` EUREX:FMAA1! · ICEEUR:ME32! · FTSE:4GL1 · FTSE:4EU5 · ICEEUR:MPE2! · NSE:CNXSMALLCAP
- `03:39:08` ECONOMICS:WWCOMPPMI · ECONOMICS:DENO · ECONOMICS:USDFSRI · FTSE:AWHDY01.TR · ECONOMICS:EUEMC · ICEEUR:USW1!
- `03:39:08` ECONOMICS:USCPMI · ICEUS:AWN1! · ECONOMICS:USBOI · ECONOMICS:FIMGDPYY · ECONOMICS:CNNO · TVC:DE10Y-TVC:IT10Y
- `03:39:08` ECONOMICS:JPMTO · ECONOMICS:EUMPMI · ECONOMICS:EUCOMPPMI · ECONOMICS:USKFCOMPI · ECONOMICS:JPCIND · ECONOMICS:USNMEMP
- `03:39:08` ECONOMICS:USNMBA · NASDAQ:NQUSS50206015N · NASDAQ:NQDXUSMLTCG · ECONOMICS:JPJAR · FTSE:SD10 · NASDAQ:NQG50
- `03:39:08` NASDAQ:B3010PI · NASDAQ:NQEMASIA3010T · NASDAQ:NQEM3010 · NASDAQ:NQHKBANK · NASDAQ:NQASPA35 · NASDAQ:NQEMASIA35
- `03:39:08` NASDAQ:NQEM35 · NASDAQ:NQEM30 · NASDAQ:NQEM5010 · NASDAQ:NQEM5020 · NASDAQ:NQEM50 · USI:YRLO.US
- `03:39:08` ECONOMICS:JPSBSI · TSE:TPXLVG · NASDAQ:NQJP35T · NASDAQ:NQUSB50101010 · NASDAQ:NQTW30 · EUREX:FMMG1!
- `03:39:08` ICEEUR:MWG2! · NYSE:ADR · 1-TVC:IN03Y · TVC:CN01Y · MULTPL:SHILLER_PE_RATIO_MONTH · ICEUS:MWL1!
- `03:39:08` OANDA:SG30SGD · NASDAQ:NQUSB302010N · HOSE:VNINDEX · TVC:VN03Y · ICEUS:DX1!-TVC:DXY · CBOE:VIX1Y
- `03:39:08` ICEEUR:US31!-TVC:US30Y · NASDAQ:NQMAFIT · ICEEUR:I2! · FX:SONIA3M · ICEEUR:SA32! · TMX:CRA2!
- `03:39:08` ICEEUR:EON2! · NASDAQ:NQASIAN · NASDAQ:NQBRICT · NASDAQ:NOMXNIN · NASDAQ:NQGRMIT · NASDAQ:NQRSKEM
- `03:39:08` LUXSE:LU0310511422 · SWB:JSGW · FTSE:FEMCRF · CSEMA:MASID · ECONOMICS:USMCEC · ECONOMICS:DEZCC
- `03:39:08` NASDAQ:NQCN55T · NASDAQ:B55PI · NASDAQ:NQEMEA4050T · FTSE:JAPAN.TR · NASDAQ:NQHKBANKN · EUREX:FVS2!
- `03:39:08` ECONOMICS:USRCR
- `03:39:08` ✗   [census-shrunk] CONTRACT MISS — 97 unresolved (106->target <=96; ~85+ fetch-slots/hour keep compounding)
- `03:39:08` NASDAQ:TLT/AMEX:SPY      CALM      z=0.71  n=235  
- `03:39:08` AMEX:XLP/AMEX:XLY        CALM      z=0.01  n=235  
- `03:39:08` AMEX:AGG/AMEX:HYG        CALM      z=1.25  n=235  
- `03:39:08` FX:SONIA3M               UNRESOLVED z=None  n=-    
- `03:39:08` ✅   [ratio-legs] 3/3 ETF ratio composites z-based (ma200 legs)
- `03:39:08` ICEUS:DX1!-TVC:DXY                     UNRESOLVED z=None  n=-    
- `03:39:08` CBOE:VIX1Y                             UNRESOLVED z=None  n=-    
- `03:39:08` NSE:CNXSMALLCAP                        UNRESOLVED z=None  n=-    
- `03:39:08` ECONOMICS:USINBR-TVC:US03MY            CALM      z=0.78  n=300  
- `03:39:08` TVC:US10Y-MULTPL:SP500_EARNINGS_MONTH  CALM      z=1.54  n=291  
- `03:39:08` TVC:DXY                                CALM      z=0.16  n=253  
- `03:39:08` ✗   [residue-routes] CONTRACT MISS — 2/5 new residue routes z-based
- `03:39:08` KRX:KOSPI200       CALM      z=1.05  n=227  
- `03:39:08` INDEX:FTSEMIB      CALM      z=0.11  n=253  
- `03:39:08` EURONEXT:N100      CALM      z=0.09  n=256  
- `03:39:08` CBOT:ZB2!          CALM      z=0.5   n=252  
- `03:39:08` CME:SR32!          NO_HISTORY z=None  n=1    
- `03:39:08` FX_IDC:INRUSD      CALM      z=0.0   n=260  
- `03:39:08` FX_IDC:MXNJPY      CALM      z=0.53  n=260  
- `03:39:08` ECONOMICS:USJO     CALM      z=0.43  n=300  
- `03:39:08` ✅   [class-routes] 6/7 per-class spot-checks z-based
- `03:39:08` CBOT:ZB1!        CALM      z=0.5   n=252  
- `03:39:08` FX_IDC:KRWUSD    CALM      z=0.01  n=260  
- `03:39:08` ECONOMICS:CHUR   NO_HISTORY z=None  n=1    TE latest
- `03:39:08` NASDAQ:VXUS      CALM      z=0.46  n=235  
- `03:39:08` CBOE:VXEEM       CALM      z=0.69  n=400  
- `03:39:08` AMEX:VEA         CALM      z=0.52  n=235  
- `03:39:08` ✅   [alias-z] 4/5 alias spot-checks on z-basis
- `03:39:08` ✅   [history-depth] 317 rows on statistical basis (steady-state ~330 as the hourly cache compounds)
- `03:39:08` ✅   [ffill-composite] SOFR-FEDFUNDS z-based: z=0.42 +0.01 Δ (DoD)
- `03:39:08` ✗   [resolution] CONTRACT MISS — 403/500 — census-attacked; residue enumerated below
## board + edge

- `03:39:13` ✅   [canary-barometer] board carries barometer 14.7 (QUIET)
- `03:39:14` ✅   [edge] edge serves the barometer
## verdict

- `03:39:14` ✗ barometer: 3 red
