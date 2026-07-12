# ops 3163 — harvest audit

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-07-12T20:49:07+00:00  

## Error

```
SystemExit: 0
```

## Data

| distinct_tickers | doc_at | equity | fx | kind_equity_basket | kind_macro_indicator | kind_mixed | kind_too_small | lists_1_2_symbols | lists_3plus | macro | n_fails | n_lists | n_warns | notes_in_mirror | notes_tagged | notes_untagged | other_euronext | other_ftse | other_iceeur | other_intotheblock | other_swb | total_symbol_slots | unique_symbols | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 2026-07-12T20:43:58.749224+00:00 |  |  |  |  |  |  |  |  |  |  | 207 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 15 | 192 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | 3224 | 188 |  |  |  |  |  |  | 6094 |  |  |  |  |  |  | 83 | 524 | 106 | 157 | 86 | 11849 | 6507 |  |
|  |  |  |  | 35 | 131 | 26 | 15 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 1079 |  |  |  |  |  |  |  |  |  |  |  |  |  | 3776 | 3343 | 433 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 0 |  | 1 |  |  |  |  |  |  |  |  |  |  | PASS |

## Log
## 1. Watchlists landed

- `20:49:06` ── your lists (largest first, top 40):
- `20:49:06`   · [500] 71699273                                       CRYPTOCAP:TOTAL2, CAPITALCOM:DXY, BLACKBULL:WTI, FRED:M2SL, FRED:DFEDT
- `20:49:06`   · [500] 82604570                                       ICEUS:KC1!, MATBAROFEX:WTI1!, CBOT:ZC1!, CBOT:ZS1!, CBOT:ZW1!
- `20:49:06`   · [500] Black Swan Event                               TVC:US03MY/FRED:BAMLH0A0HYM2, NYMEX:CL1!-BLACKBULL:WTI, FRED:T10YIEM, 
- `20:49:06`   · [500] Bottom Indicators                              FRED:FEDFUNDS-FRED:BAMLHE00EHYIEY, ECONOMICS:CLTOT, ECONOMICS:PETOT, T
- `20:49:06`   · [500] FTSE                                           FTSE:FIVNM3NU, ADX:FADTECI, FTSE:FIVNM30, FTSE:FIVNM30.TR, FTSE:FVTT
- `20:49:06`   · [500] Red list                                       ECONOMICS:JPM3, FRED:MABMM301JPM189S, ECONOMICS:JPCBBS, FRED:JPNASSETS
- `20:49:06`   · [437] Bitcoin : Nikkei TOP and Bottom in USD ALWAYS  INDEX:BTCUSD, CRYPTOCAP:USDC.D+CRYPTOCAP:USDT.D, FRED:JPNASSETS, TVC:N
- `20:49:06`   · [327] Crypto : Nikkei TOP and Bottom in USD ALWAYS M CRYPTOCAP:TOTAL3ES, FX:5USNOTE, TVC:US02Y-TVC:US10Y, 1-(TVC:US02Y-TVC:
- `20:49:06`   · [319] 68114374                                       OKEX:CVXUSDT, FRED:T5YIE, AMEX:DJP, ECONOMICS:USBBS, COT3:132741_FO_TA
- `20:49:06`   · [273] Brent Johnson Portfolio: THE SHORTTERM SWINGS  AMEX:NAIL, NASDAQ:GT, NASDAQ:NVD, AMEX:AGQ, AMEX:UYM
- `20:49:06`   · [248] Bitcoin - Global Liquidity: GOLD ALWAYS BOTTOM FRED:FEDFUNDS-FRED:BAMLHE00EHYIEY, FRED:RBUSBIS, ECONOMICS:USINBR-TVC:
- `20:49:06`   · [229] Economy                                        NYSE:ADR, ISM:MAN_PMI, FRED:BOGZ1FA895050005Q, FRED:AMTMNO, FRED:NEWOR
- `20:49:06`   · [203] Financial Crisis Signs                         FRED:DTB3, NYSE:ADR, ECONOMICS:USBOI, FX:5USNOTE-FX:10USNOTE, 1-(TVC:U
- `20:49:06`   · [189] Countries Balance of Trade                     ECONOMICS:YEBOT, ECONOMICS:KHBOT, ECONOMICS:GABOT, ECONOMICS:GQBOT, EC
- `20:49:06`   · [180] Breadth: leads the Market                      INDEX:S5TH, INDEX:S5FI, INDEX:S5TW, INDEX:MMFI, USI:BASPRD.US
- `20:49:06`   · [174] Global Food Inflation                          ECONOMICS:BJFI, ECONOMICS:SSFI, ECONOMICS:LRFI, ECONOMICS:KMFI, ECONOM
- `20:49:06`   · [171] Futures                                        NSE:NIFTY1!, NSE:BANKNIFTY1!, OSE:NK225M1!, MCX:NATURALGAS1!, SGX:CN1!
- `20:49:06`   · [170] 87717856                                       AMEX:VHT, BITFINEX:BTCUSDSHORTS, NASDAQ:CALM, NYSE:PM, AMEX:DBA
- `20:49:06`   · [168] GDP YOY                                        ECONOMICS:EUGDPYY, ECONOMICS:JPGDPYY, ECONOMICS:CNGDPYY, ECONOMICS:GBG
- `20:49:06`   · [166] Debt To GDP                                    ECONOMICS:GBGDG, ECONOMICS:SZGDG, ECONOMICS:NZGDG, ECONOMICS:SYGDG, EC
- `20:49:06`   · [158] Emerging markets : BEST GUAGE FOR GLOBAL LIQUI NASDAQ:NQEM, NASDAQ:NQEM50, CBOE:VXEEM, ICEUS:MME1!, AMEX:PCY
- `20:49:06`   · [153] All World Ex US: WHAT GLOBAL STOCK MARKET INDI NASDAQ:FID, AMEX:PEX, AMEX:FM, CBOE:MXS, ICEEUR:EWS2!
- `20:49:06`   · [153] Deposit Interest Rate                          ECONOMICS:ZWDIR, ECONOMICS:ARDIR, ECONOMICS:VEDIR, ECONOMICS:UZDIR, EC
- `20:49:06`   · [129] Banking Sector : Banks = Liquidity Proxy Every NASDAQ:NQEM30, AMEX:KRE, NASDAQ:KRX, NASDAQ:BKXTR, NASDAQ:QABA
- `20:49:06`   · [128] Europe                                         NASDAQ:IEUS, ECONOMICS:EUCA, ECONOMICS:EUEOI, ECONOMICS:EUESI, NASDAQ:
- `20:49:06`   · [123] Foreign Exchange Reserves                      ECONOMICS:NAFER, ECONOMICS:KRFER, ECONOMICS:RUFER, ECONOMICS:JPFER, EC
- `20:49:06`   · [122] Daily Metrics to watch                         FX:2USNOTE, 1/(FRED:TOTLL/FRED:M2SL), ECONOMICS:USINTR, NASDAQ:VNQI, N
- `20:49:06`   · [120] European Bonds                                 TVC:IT10Y, TVC:GB30, TVC:GB30Y, TVC:GB10Y, TVC:DE10Y
- `20:49:06`   · [118] 91314743                                       NASDAQ:ICLN, SPARKS:CANNABIS, USI:SPAX.NV, NASDAQ:BNDX, AMEX:BLV
- `20:49:06`   · [114] Finland : a major producer of pulp & paper (11 ECONOMICS:FIGDPYY, ECONOMICS:FIEXP, ECONOMICS:FIBCOI, ECONOMICS:FIEP, 
- `20:49:06`   · [108] Credit market                                  NASDAQ:NQUSB302010N, NASDAQ:QGLDID, NASDAQ:QUSOID, NASDAQ:QGLDITR, NAS
- `20:49:06`   · [103] BONDS : Fixed income tends to lean itself towa NASDAQ:VWOB, SWB:JSGW, TVC:MOVE, TVC:BTPBUND, NASDAQ:BND
- `20:49:06`   · [101] Financial Crisis                               TVC:DXY, NYMEX:CL1!, FRED:BORROW, FRED:RMFSL, FRED:PALLFNFINDEXQ
- `20:49:06`   · [ 91] Basic Materials                                NASDAQ:NQEM55, OMXBALTIC:B55PI, NASDAQ:NQDM55, NASDAQ:CRSPMT1, NASDAQ:
- `20:49:06`   · [ 91] china                                          ECONOMICS:CNCLI, FX_IDC:CNYUSD, FX_IDC:CNYJPY, SSE:000148, SSE:000806
- `20:49:06`   · [ 90] DXY: Currencies best seen in "3M" : DXY pumpin FRED:WFCDA, ECONOMICS:EUFER, FRED:RBUSBIS, FRED:DTWEXEMEGS, FRED:DTWEX
- `20:49:06`   · [ 89] Central Bank Balance Sheet                     ECONOMICS:USCBBS, ECONOMICS:EUCBBS, ECONOMICS:JPCBBS, ECONOMICS:CNCBBS
- `20:49:06`   · [ 89] Europe Gov DATA                                TVC:EUBUND, ECONOMICS:EUINTR, ECONOMICS:EUIRYY, ECONOMICS:EUGDPYY, ECO
- `20:49:06`   · [ 89] fed plumbing                                   FRED:WUDSHO, BVB:IORB-FRED:DCPF3M, FRED:STLFSI3, FRED:T5YFF, FRED:CFNA
- `20:49:06`   · [ 86] EuroDollar: DXY pumping means tightening and l TVC:JP03MY, CME:SR32!, CBOT:YIT1!, FRED:BAMLEMPBPUBSICRPIEY, FRED:WDFO
## 2. Symbol namespace census (drives tracker v2)

- `20:49:06`   MACRO            6094
- `20:49:06`   EQUITY           3224
- `20:49:06`   OTHER:FTSE       524
- `20:49:06`   FX               188
- `20:49:06`   OTHER:INTOTHEBLOCK 157
- `20:49:06`   OTHER:ICEEUR     106
- `20:49:06`   OTHER:SWB        86
- `20:49:06`   OTHER:EURONEXT   83
- `20:49:06`   OTHER:LSE        82
- `20:49:06`   CRYPTO           77
- `20:49:06` ✅ namespace census complete — tracker v2 will price equity baskets directly and route MACRO/FX lists through FRED/Polygon-equivalent mappings instead of scoring them 0
## 3. Notes mirror

- `20:49:06` ── most-noted tickers: DXY(152), ICEUS:DXY(135), FRED:FEDFUNDS(119), FEDFUNDS(119), SPX(82), CBOE:SPX(80), MU(63), STX(63), NVDA(63), AAPL(62), TVC:MOVE(24), MOVE(24)
- `20:49:06` ✅ 3776 notes in the mirror (3343 carry a ticker → routable by the brain-compiler)
## 4. Brain read-back

- `20:49:07` ⚠ brain read-back unavailable: HTTP Error 403: Forbidden (the mirror at data/tradingview-notes.json is the source the brain-compiler reads)
