# ops 6879 - symbols the chart will not pull, and why (read-only), 2026-11-21T17:17+00:00

**Status:** success  
**Duration:** 497.4s  
**Finished:** 2026-11-21T17:25:31+00:00  

## Data

| coingecko_entries | coingecko_no_route | crypto_ids_charting | crypto_ids_tried | dialog_rows_no_route | map_entries | own_no_route | own_router_rows | own_rows |
|---|---|---|---|---|---|---|---|---|
| 17 | 10 | 7 | 29 | 124 | 4823 | 3374 | 8259 | 10745 |

## Log
- `17:17:15` installing playwright
## 1. The live symbol map against the router

- `17:17:30` 4823 map entries; per source: entries / no route through the router
- `17:17:30`   DBNOMICS          53 /    36  ECONOMICS:AUCCI=OECD/MEI/AUS.CSCICP02., ECONOMICS:BHINTR=IMF/IFS/M.BH.FILR_PA, ECONOMICS:CHBCOI=OECD/MEI/CHE.B6BLPI01., ECONOMICS:CHCCI=OECD/MEI/CHE.CSCICP02., ECONOMICS:CNBCOI=OECD/MEI/CHN.B6BLPI01., ECONOMICS:CNCCI=OECD/MEI/CHN.CSCICP02., ECONOMICS:DECCI=OECD/MEI/DEU.CSCICP02.
- `17:17:30`   MARKET          2321 /    27  CBOE:IBHY1!=IBHY1!, CBOE:VX1!=VX1!, EURONEXT:FCE1!=FCE1!.AS, EURONEXT:PX1=^FCHI, HKEX:FEQ2!=FEQ2!.HK, HKEX:HB11!=HB11!.HK, HKEX:HB12!=HB12!.HK
- `17:17:30`   INTERNALS         21 /    21  INDEX:ADVN=ADVANCERS, INDEX:DECN=DECLINERS, INDEX:MMFI=PCT_ABOVE_50DMA, INDEX:MMTH=PCT_ABOVE_200DMA, INDEX:NCTH=PCT_ABOVE_200DMA, INDEX:R3TH=PCT_ABOVE_200DMA, INDEX:S5FI=PCT_ABOVE_50DMA
- `17:17:30`   COINMETRICS       18 /    18  GLASSNODE:BTC_TOTALTXFEES=btc|FeeTotUSD, GLASSNODE:ETH_TOTALTXFEES=eth|FeeTotUSD, GLASSNODE:MATIC_SUPPLY=matic|SplyCur, INTOTHEBLOCK:BTC_HASHRATE=btc|HashRate, INTOTHEBLOCK:BTC_INFLOWTXCOUNT=btc|TxCnt, INTOTHEBLOCK:BTC_INFLOWTXVOLUME=btc|TxTfrValAdjUSD, INTOTHEBLOCK:BTC_INFLOWTXVOLUMEUSD=btc|TxTfrValAdjUSD
- `17:17:30`   COINGECKO         17 /    10  BITFINEX:BTCUSDLONGS=btclongs, BITFINEX:MATICF0USTF0LONGS=maticf0ustf0longs, BITFINEX:MATICF0USTF0SHORTS=maticf0ustf0shorts, BITFINEX:MATICUSTLONGS=maticustlongs, CME:BTC1!=bitcoin, CME:ETH1!=ethereum, CME:MBT1!=bitcoin
- `17:17:30`   DERIVED           27 /     9  CBOT:ZQ1!=FRED~DFF~hundred_minus, ECONOMICS:EUGDPGA=FRED~CLVMNACSCAB1GQEA1, EUREX:FGBL1!=FRED~IRLTLT01DEM156N~n, GLASSNODE:BTC_ATHDRAWDOWN=COINMETRICS~btc|PriceU, ICEEUR:I2!=DBNOMICS~ECB/FM/M.U2.E, INDEX:HLUS=INTERNALS~NEW_HIGHS~mi, INTOTHEBLOCK:MATIC_VOLATILITY30=COINMETRICS~matic|Pric
- `17:17:30`   FORMULA          322 /     1  QUANDL:MULTPL/SHILLER_PE_RATIO_MONTH=QUANDL:MULTPL/SHILLER_
- `17:17:30`   FRED             675 /     0  
- `17:17:30`   COT               76 /     0  
- `17:17:30`   WORLDBANK       1293 /     0  
- `17:17:30` COINGECKO entries: 17, no route: 10; ids the router's 2-6 letter rule refuses: ['.d', 'bitcoin', 'btc_dominance', 'btclongs', 'ethereum', 'maticf0ustf0longs', 'maticf0ustf0shorts', 'maticustlongs']
- `17:17:30` COINGECKO entries routed to <coin id>-USD although the symbol names another ticker: 0 []
## 2. His own lists

- `17:17:31` 492 lists, 10745 different rows: {'router': 8259, 'us-listing': 1998, 'spread': 488}
- `17:17:31` rows the router has no route for: 3374 of 8259 router rows
- `17:17:31`   ECONOMICS      1000 of 3216 no route | map source {'None': 969, 'DBNOMICS': 30, 'DERIVED': 1} | ECONOMICS:USEPYY, ECONOMICS:USLPS, ECONOMICS:USCSHPI, ECONOMICS:USPCEPI, ECONOMICS:USHPIMM, ECONOMICS:CNBCOI
- `17:17:31`   FTSE            453 of  455 no route | map source {'None': 453} | FTSE:FTUSPLUT, FTSE:FTUSPLUT.TR, FTSE:FTUSPSUT, FTSE:FTUSPSUT.TR, FTSE:FTEMSLU.TR, FTSE:4EU5
- `17:17:31`   ICE             389 of  389 no route | map source {'None': 389} | ICE:MLCXLAER, ICE:ICEFSTS, ICE:ICEFSTST, ICE:MLCXLATR, ICE:SMOVE3M, ICE:MLCXENER
- `17:17:31`   INDEX           181 of  329 no route | map source {'None': 171, 'INTERNALS': 8, 'COINGECKO': 1} | INDEX:BTCUSD, INDEX:ADVN, INDEX:DECN, INDEX:MMFI, INDEX:S5FI, INDEX:HLUS
- `17:17:31`   INTOTHEBLOCK    150 of  150 no route | map source {'None': 133, 'COINMETRICS': 15, 'DERIVED': 2} | INTOTHEBLOCK:BTC_INFLOWTXCOUNT, INTOTHEBLOCK:MATIC_LARGETXVOLUMEUSD, INTOTHEBLOCK:BTC_HASHRATE, INTOTHEBLOCK:BTC_MINERTOTALFLOWSUSD, INTOTHEBLOCK:ETH_MINERTOTALFLOWSUSD, INTOTHEBLOCK:BTC_MINEROUTFLOWSUSD
- `17:17:31`   USI             143 of  143 no route | map source {'None': 130, 'INTERNALS': 13} | USI:PCCE, USI:PCC, USI:BASPRD.NQ, USI:BPI.US, USI:BPI.NQ, USI:CVOLT
- `17:17:31`   TVC             123 of  250 no route | map source {'None': 116, 'DBNOMICS': 6, 'DERIVED': 1} | TVC:HK03Y, TVC:TW02Y, TVC:SG02Y, TVC:NZ02Y, TVC:DEU40, TVC:HGX
- `17:17:31`   SGX             107 of  107 no route | map source {'None': 102, 'MARKET': 5} | SGX:CN2!, SGX:SGP2!, SGX:TWN1!, SGX:CN1!, SGX:NK1!, SGX:X2CN
- `17:17:31`   DJ               87 of   87 no route | map source {'None': 87} | DJ:W1DOW, DJ:D067746T, DJ:D067394T, DJ:D067738T, DJ:D067745T, DJ:D067402T
- `17:17:31`   GLASSNODE        62 of   62 no route | map source {'None': 58, 'COINMETRICS': 3, 'DERIVED': 1} | GLASSNODE:USDC_NEWADDRESSES, GLASSNODE:BTC_TOTALTXFEES, GLASSNODE:ETH_TOTALTXFEES, GLASSNODE:BTC_TXS, GLASSNODE:BTC_SOPR, GLASSNODE:BTC_TXSPS
- `17:17:31`   HSI              52 of   52 no route | map source {'None': 52} | HSI:HSHKS.TRI.GRS, HSI:HSHDYI.TRI.GRS, HSI:HSCAP, HSI:HSHKS, HSI:HSHKMS, HSI:HSHKMS.TRI.GRS
- `17:17:31`   ICEEUR           40 of   43 no route | map source {'None': 39, 'DERIVED': 1} | ICEEUR:MWM2!, ICEEUR:MVE2!, ICEEUR:MWN1!, ICEEUR:ME32!, ICEEUR:EWS2!, ICEEUR:EWW2!
- `17:17:31`   EUREX            38 of   41 no route | map source {'None': 37, 'DERIVED': 1} | EUREX:FBON1!, EUREX:FXGL1!, EUREX:FMIG1!, EUREX:FMSC1!, EUREX:FEHY1!, EUREX:FMAA1!
- `17:17:31`   CSEMA            30 of   30 no route | map source {'None': 30} | CSEMA:MASID, CSEMA:BANK, CSEMA:MSI20, CSEMA:MASI, CSEMA:AGRO, CSEMA:B&MC
- `17:17:31`   CRYPTOCAP        26 of   26 no route | map source {'None': 24, 'COINGECKO': 2} | CRYPTOCAP:TOTAL2, CRYPTOCAP:USDC, CRYPTOCAP:ETH.D, CRYPTOCAP:OTHERS.D, CRYPTOCAP:DOGE.D, CRYPTOCAP:TOTAL3ES
- `17:17:31`   SIX              26 of   39 no route | map source {'None': 23, 'MARKET': 3} | SIX:LYCNB.EUR, SIX:BUYB.USD, SIX:PSFE.EUR, SIX:SNSPIX, SIX:SPIMP, SIX:SPIRPD
- `17:17:31`   EURONEXT         23 of   59 no route | map source {'None': 21, 'MARKET': 2} | EURONEXT:PX1, EURONEXT:FCE1!, EURONEXT:EEUMN, EURONEXT:EEUMP, EURONEXT:EEUMG, EURONEXT:AHYE
- `17:17:31`   CME              22 of   34 no route | map source {'None': 19, 'COINGECKO': 3} | CME:GE1!, CME:GE2!, CME:BTC1!, CME:SR31!, CME:SR11!, CME:SED2!
- `17:17:31`   OMXNORDIC        20 of   20 no route | map source {'None': 20} | OMXNORDIC:OMRXTOT, OMXNORDIC:OMXN40, OMXNORDIC:NOMXNBI, OMXNORDIC:OMRXBOND, OMXNORDIC:OMRXGOVT, OMXNORDIC:OMRXMM
- `17:17:31`   DFM              14 of   14 no route | map source {'None': 14} | DFM:DFMGI, DFM:DFM, DFM:DFMSI, DFM:DFMREI, DFM:DFMUI, DFM:DFMMI
- `17:17:31`   DJCFD            13 of   13 no route | map source {'None': 13} | DJCFD:DE30, DJCFD:REIT, DJCFD:DJT, DJCFD:DJU, DJCFD:DJCIEN, DJCFD:DJCIIK
- `17:17:31`   ADX              13 of   13 no route | map source {'None': 13} | ADX:IHC, ADX:FADTECI, ADX:FADBMI, ADX:FADCDI, ADX:FADGMI, ADX:FADHCI
- `17:17:31`   HKEX             13 of   19 no route | map source {'MARKET': 10, 'None': 3} | HKEX:FEQ2!, HKEX:HB32!, HKEX:TWN1!, HKEX:HB11!, HKEX:HB31!, HKEX:HB12!
- `17:17:31`   SPCFD            13 of   14 no route | map source {'None': 13} | SPCFD:S5UTIL, SPCFD:SPF, SPCFD:SPGSCI, SPCFD:S5INDU, SPCFD:SP500LVOL, SPCFD:S5TELS
- `17:17:31`   MULTPL           12 of   12 no route | map source {'None': 12} | MULTPL:SP500_EARNINGS_YIELD_YEAR, MULTPL:SP500_REAL_EARNINGS_GROWTH_YEAR, MULTPL:SP500_EARNINGS_GROWTH_YEAR, MULTPL:SP500_EARNINGS_GROWTH_QUARTER, MULTPL:SP500_PSR_YEAR, MULTPL:SP500_EARNINGS_YEAR
- `17:17:31`   RUS              11 of   11 no route | map source {'None': 11} | RUS:IRUS, RUS:RUCBITR, RUS:ED1!, RUS:RTSTRR, RUS:MESMTRR, RUS:MEITTRR
- `17:17:31`   CAPITALCOM       10 of   11 no route | map source {'None': 10} | CAPITALCOM:DXY, CAPITALCOM:EU50, CAPITALCOM:IEMG, CAPITALCOM:COPPER, CAPITALCOM:ESU2025, CAPITALCOM:HK50
- `17:17:31`   CBOT             10 of   21 no route | map source {'None': 9, 'DERIVED': 1} | CBOT:ZQ1!, CBOT:YIT1!, CBOT:AW1!, CBOT:F1U1!, CBOT:QCW1!, CBOT:B1U1!
- `17:17:31`   ICEUS            10 of   12 no route | map source {'None': 10} | ICEUS:AWN1!, ICEUS:MWL1!, ICEUS:MMN.B1!, ICEUS:MMN2!, ICEUS:MME1!, ICEUS:NYFANG
- `17:17:31`   FX_IDC           10 of  151 no route | map source {'None': 10} | FX_IDC:JPYWCU, FX_IDC:JPYTHX, FX_IDC:GIPUSD, FX_IDC:SGDWCU, FX_IDC:XDRDKX, FX_IDC:XDRNOX
- `17:17:31`   SPARKS            9 of    9 no route | map source {'None': 9} | SPARKS:FANG, SPARKS:VIDEOGAMES, SPARKS:SPORTSBET, SPARKS:CANNABIS, SPARKS:BANKS, SPARKS:RECESSION
- `17:17:31`   CME_MINI          9 of   16 no route | map source {'None': 9} | CME_MINI:IPO1!, CME_MINI:DVE2!, CME_MINI:R2G1!, CME_MINI:EI2!, CME_MINI:M6E1!, CME_MINI:E71!
- `17:17:31`   SSE               9 of   61 no route | map source {'None': 9} | SSE:N00905, SSE:N00300, SSE:933000, SSE:N00688, SSE:931877, SSE:H11100
- `17:17:31`   AQUISUK           8 of    8 no route | map source {'None': 8} | AQUISUK:BUYBL, AQUISUK:BRFIL, AQUISUK:CEMRD, AQUISUK:IUMOL, AQUISUK:MCEUP, AQUISUK:CBUHD
- `17:17:31`   NEO               7 of    7 no route | map source {'None': 7} | NEO:FGRO, NEO:VMO, NEO:VBU, NEO:ZTS, NEO:ZGSB, NEO:BND
- `17:17:31`   SWB               7 of   58 no route | map source {'None': 7} | SWB:TES5, SWB:FJR, SWB:MI9D, SWB:SM8T, SWB:OSXM, SWB:ABD7
- `17:17:31`   PEPPERSTONE       7 of    7 no route | map source {'None': 7} | PEPPERSTONE:HK50, PEPPERSTONE:USDX, PEPPERSTONE:GASOLINE, PEPPERSTONE:OATS, PEPPERSTONE:EURX, PEPPERSTONE:JPYX
- `17:17:31`   LUXSE             7 of    7 no route | map source {'None': 7} | LUXSE:LU0310511422, LUXSE:LU0267388220, LUXSE:LU0297942194, LUXSE:LU0666200349, LUXSE:LU0666200935, LUXSE:LU1564329032
- `17:17:31`   BMFBOVESPA        6 of    6 no route | map source {'None': 6} | BMFBOVESPA:IBOV, BMFBOVESPA:WSP1!, BMFBOVESPA:ISP1!, BMFBOVESPA:EUR1!, BMFBOVESPA:ICF1!, BMFBOVESPA:IVVB11
- `17:17:31`   OSE               6 of    6 no route | map source {'None': 6} | OSE:TOPIX1!, OSE:NK225M1!, OSE:NK2251!, OSE:DJIA1!, OSE:JBL1!, OSE:MOTHE2!
- `17:17:31` crypto-venue rows in his lists: 446, of which no route: 275
## 3. What the Symbol Search dialog shows, and which rows have no route

- `17:17:39` btc           14 rows, 5 with no route | BTC CRYPTOCAP:TOTAL3-CRYPTOCAP:BTC [INDEX:BTCUSD] CME:BTC1!-INDEX:BTCUSD BITSTAMP:BTCUSD COINBASE:BTCUSD [CRYPTOCAP:BTC.D] [CME:BTC1!] CRYPTOCAP:BTC/CBOE:VXN [GLASSNODE:BTC_TXS] CRYPTOCAP:BTC/NASDAQ:NXTQ CRYPTOCAP:BTC/NASDAQ:NDXL [GLASSNODE:BTC_SOPR] BTCS
- `17:17:46` bitcoin       14 rows, 7 with no route | [INDEX:BTCUSD] CME:BTC1!-INDEX:BTCUSD BITSTAMP:BTCUSD COINBASE:BTCUSD BITSTAMP:BTCUSD/TVC:VIX [BITFINEX:BTCUSDLONGS] BINANCE:BTCUSDH2025 [BITFINEX:BTCUSDSHORTS] [BNC:BLX] [INTOTHEBLOCK:BTC_HASHRATE] [INTOTHEBLOCK:BTC_WHALES] [INTOTHEBLOCK:BTC_INVESTORS] BIXI X:BCHNUSD
- `17:17:52` eth           14 rows, 3 with no route | ETH INDEX:ETHUSD AMEX:ETHT CBOE:ETHU NASDAQ:ETHA AMEX:ETHD NASDAQ:ETHM [CRYPTOCAP:ETH.D] [CME:ETH1!] KRAKEN:ETHBTC CRYPTOCAP:ETH/CRYPTOCAP:BTC BYBIT:ETHUSDU2025 [INTOTHEBLOCK:ETH_INVESTORS] ETHA
- `17:17:59` ethereum      14 rows, 9 with no route | INDEX:ETHUSD BYBIT:ETHUSDU2025 KRAKEN:ETHBTC [INTOTHEBLOCK:ETH_INVESTORS] [INTOTHEBLOCK:ETH_WHALESPERCENTAGE] [INTOTHEBLOCK:ETH_INVESTORSPERCENTAGE] [INTOTHEBLOCK:ETH_WHALESASSETS] [INTOTHEBLOCK:ETH_RETAILPERCENTAGE] [INTOTHEBLOCK:ETH_INVESTORSASSETS] [INTOTHEBLOCK:ETH_MINERTOTALFLOWSUSD] [INTOTHEBLOCK:ETH_RETAILASSETS] [INTOTHEBLOCK:ETH_INFLOWTXVOLUME] X:ENSUSD X:ETCUSD
- `17:18:05` sol           14 rows, 4 with no route | X:SOLUSD polygon:asset:c7f3799b0d7975a7 indicator-bus:SOL [PEPPERSTONE:GASOLINE] FX_IDC:CHFPEN FX_IDC:JPYPEN AMEX:UGA ECONOMICS:USGS FX_IDC:XDRPEN [ECONOMICS:USGSCH] FRED:HSN1F [INTOTHEBLOCK:ERC20_BEARSVOLUME] NASDAQ:KTOS [INTOTHEBLOCK:MATIC_BEARSVOLUME]
- `17:18:12` xrp           14 rows, 3 with no route | XRP XRPN XRPC XRPI XRPM XRPR XRPT XRPZ XRPNW XRPFF [CRYPTOCAP:XRP] [CME:XRP] COINBASE Derivatives:XRP [TSX:XRP]
- `17:18:18` doge          14 rows, 2 with no route | X:DOGEUSD indicator-bus:DOGE [CRYPTOCAP:DOGE.D] [BITTREX:DOGEUSD] I:DOGE10K I:DOGE50K I:DOGE100K I:DOGE200K I:DOGE500K DOGEF DOJE GDOG TDOG TXXD
- `17:18:25` crypto total  14 rows, 14 with no route | [CRYPTOCAP:TOTAL] [CRYPTOCAP:TOTALDEFI] [CRYPTOCAP:TOTAL2] [CRYPTOCAP:TOTALE50] [CRYPTOCAP:OTHERS] [CRYPTOCAP:TOTALE100] [CRYPTOCAP:TOTAL3] [CRYPTOCAP:TOTALES] [CRYPTOCAP:TOTALE50.D] [CRYPTOCAP:OTHERS.D] [CRYPTOCAP:TOTALE100.D] [CRYPTOCAP:TOTAL2ES] [CRYPTOCAP:TOTAL3ES] [CRYPTOCAP:TOTAL3ESBTC]
- `17:18:31` eurusd        14 rows, 8 with no route | C:EURUSD FX:EURUSD fred:DEXUSEU FRED:ECBASSETSW*FX:EURUSD [CMCMARKETS:EURUSDU2025] FX_IDC:EURUSD OANDA:EURUSD [FOREXCOM:EURUSD] [PEPPERSTONE:EURUSD] [TICKMILL:EURUSD] [IC:EURUSD] [VANTAGE:EURUSD] [CAPITALCOM:EURUSD] [CMCMARKETS:EURUSD]
- `17:18:38` usdjpy        14 rows, 8 with no route | C:USDJPY FX:USDJPY fred:DEXJPUS equity-research-tickers:asset:35b4ac041aa83442 [TFEX:USDJPY1!] [MSEI:USDJPY1!] FX_IDC:USDJPY OANDA:USDJPY [FOREXCOM:USDJPY] [JFX:USDJPY] [PEPPERSTONE:USDJPY] [MATSUI:USDJPY] [IC:USDJPY] [CAPITALCOM:USDJPY]
- `17:18:45` gold          14 rows, 3 with no route | COMEX:GC1! TVC:GOLD GLD fred:GOLDAMGBD228NLBM GOLD AMEX:GLD [GLOBALPRIME:XAUUSD] FOREXCOM:XAUUSD [MCX:GOLD1!] [TVC:GOLDSILVER] TVC:GOLD/CAPITALCOM:COPPER CAPITALCOM:COPPER/TVC:GOLD AMEX:CPER/TVC:GOLD SP:SPX/TVC:GOLD
- `17:18:51` silver        14 rows, 3 with no route | COMEX:SI1! TVC:SILVER SLV SPARKS:LITHIUM/TVC:SILVER AMEX:SLV [FOREXCOM:XAGUSD] [MCX:SILVER1!] TVC:SILVER/TVC:GOLD 1/TVC:SILVER/CAPITALCOM:COPPER 1/TVC:GOLDSILVER [TVC:GOLDSILVER] AMEX:AGQ NASDAQ:XAU SVM
- `17:18:58` oil           14 rows, 2 with no route | NYMEX:CL1! USO TVC:USOIL fred:DCOILWTICO fred:DCOILBRENTEU [MARKETSCOM:OIL] NSE:OIL/CAPITALCOM:COPPER FRED:POILBREUSDM TVC:DXY/TVC:USOIL [MCX:CRUDEOIL1!] FRED:PPOILUSDM FRED:POILWTIUSDM FRED:POLVOILUSDM FRED:PROILUSDM
- `17:19:04` copper        14 rows, 3 with no route | COMEX:HG1! fred:PCOPPUSDM [CAPITALCOM:COPPER] 1/TVC:SILVER/CAPITALCOM:COPPER FRED:PCOPPUSDM-MCX:COPPER1! [MCX:COPPER1!] CAPITALCOM:COPPER/TVC:NDQ CAPITALCOM:COPPER/TVC:GOLD CAPITALCOM:COPPER/LSE:IRON FX:COPPER/TVC:GOLD TVC:GOLD/CAPITALCOM:COPPER NSE:OIL/CAPITALCOM:COPPER [COMEX_MINI:MHG1!] SCCO
- `17:19:11` natgas        14 rows, 3 with no route | UNG fred:DHHNGSP [MCX:NATURALGAS1!] AMEX:FCG [ECONOMICS:EUNGI] FRED:PNGASEUUSDM AMEX:UNG AMEX:BOIL AMEX:UNL [ECONOMICS:EUNGIR] NASDAQ:FUMTR NASDAQ:FUMNTR NGS BOIL
- `17:19:17` us10y         14 rows, 1 with no route | ICEEUR:USP1!-TVC:US10Y TVC:CL10Y-TVC:US10Y [ECONOMICS:US10Y] TVC:US10Y FRED:HQMCB10YRP-TVC:US10Y FRED:BAMLC4A0C710YEY-TVC:US10Y FRED:FEDFUNDS-TVC:US10Y TVC:JP10Y-TVC:US10Y TVC:CN10Y-TVC:US10Y TVC:US30Y-TVC:US10Y TVC:US05Y-TVC:US10Y TVC:US02Y-TVC:US10Y TVC:US03MY-TVC:US10Y TVC:US01Y-TVC:US10Y
- `17:19:24` dxy           14 rows, 1 with no route | TVC:DXY fred:DTWEXBGS [CAPITALCOM:DXY] ICEUS:DX1!-TVC:DXY TVC:DXY/TVC:USOIL TVC:DXY/FRED:BAMLH0A0HYM2SYTW TVC:CN10Y/TVC:DXY TVC:CN10Y/TVC:DXY/FRED:BAMLH0A0HYM2 FRED:BAMLH0A0HYM2SYTW/TVC:DXY 1/(TVC:CN10Y/TVC:DXY) ICEEUR:I1!/TVC:DXY 1/TVC:DXY VANTAGE:SP500/TVC:DXY DXYZ
- `17:19:30` vix           14 rows, 3 with no route | TVC:VIX I:VIX fred:VIXCLS CBOE:VIX [CAPITALCOM:VIX] [HOSE:VIX] [PEPPERSTONE:VIX] CBOE:VIX3M CBOE:VIX1Y CBOE:VIX6M TVC:VIX/BITMEX:BVOL BITSTAMP:BTCUSD/TVC:VIX CBOE:SVIX VIXM
- `17:19:37` nasdaq        14 rows, 0 with no route | I:NDX QQQ I:COMP fmp:asset:6231e5c650229642 fmp:asset:b098b843bf59ba9c fmp:asset:52ebd5dfcda0b0c0 NASDAQ:QQQ NASDAQ:QQQE NASDAQ:QQQM NASDAQ:QQQA NASDAQ:QQQ/VANTAGE:SP500 NASDAQ:SHY NASDAQ:DLLL NASDAQ:VRTL
- `17:19:43` dow           14 rows, 8 with no route | I:DJI DIA DOW NYSE:DOW AMEX:DIAL [DJ:W1DOW] [GLASSNODE:CRV_ATHDRAWDOWN] [GLASSNODE:USDT_ATHDRAWDOWN] AMEX:EDOW [GLASSNODE:USDC_ATHDRAWDOWN] [GLASSNODE:BTC_ATHDRAWDOWN] [GLASSNODE:CRV_MEDIANVOLUME] [GLASSNODE:CRV_MEDIANVOLUMEUSD] [GLASSNODE:USDC_MEDIANVOLUMEUSD]
- `17:19:50` russell       14 rows, 4 with no route | I:RUT IWM AMEX:IWM MIL:IWMO LSE:IWMO [AQUISUK:IWMOL] [AQUIS:IWMO.GB] [RUSSELL:RUMIC] RUSSELL:RUT [ECONOMICS:FIWM] CBOEFTSE:RLS CBOEFTSE:RIR INDEX:R2FD INDEX:R3FD
- `17:19:57` dax           14 rows, 1 with no route | DAX XETR:DAX equity-research-tickers:asset:fc8cbed338b581f4 XETR:MDAX EUREX:FDAX1! XETR:TDXP [TVC:DEU40] DXR DAXXF ADXAF SNDX BXRXQ CDXI fred:DAXRNSA
- `17:20:03` nikkei        14 rows, 2 with no route | [SPREADEX:NIKKEI] INDEX:NTH COT3:240741_F_DP_L AMEX:JPXN FRED:HQMCB10YRP FX:2USNOTE TVC:US02Y [TVC:HK03Y] NASDAQ:TQQQ AMEX:TNA AMEX:TMF INDEX:MOTH TVC:NI225 JPXN
- `17:20:10` ftse          14 rows, 12 with no route | INDEX:FTSEMIB [ATHEX:FTSE_BM] FTSE:FTSEMIB [NASDAQDUBAI:FTSESA1!] [FTSE:FTSEMIBN] [FTSE:FTSEMIBSDV] [FTSE:FTSEMIB.TR] [FTSE:FTSEMIBN.TR] [FTSE:SD14] [FTSE:SD11] [FTSE:SS03] [FTSE:AWHDY01.TR] [FTSE:ASX] [FTSE:AXX]
- `17:20:16` hang seng     14 rows, 11 with no route | TVC:HSI [HSI:HSCGSI] [HSI:HSCAP] [HSI:HSSCSM] [HSI:HSI.TRI.NET] [HSI:HSCHDY] [HSI:HSSCCSM] [HSI:HSTECH.TRI.NET] [HSI:HSI.TRI.NET.USD] [HSI:HSESG50.TRI.NET] [HSI:HSCAMCI] [HSI:HSHKS] KTEC HGSFF
- `17:20:23` tesla         14 rows, 3 with no route | NASDAQ:TSLA TSLA TSL TSLP TSLT TSLZ TXLZF YTSLF I:TSLACW I:TSLADI I:TSLAIO [PYTH:TSLA] [ICEEUR:8T9] [OKX:XTSLAUSDT]
- `17:20:29` nvda          14 rows, 1 with no route | NVDA NASDAQ:NVDA equity-research-tickers:asset:42b547c065964ae6 indicator-bus:NVDA [ICE:NY2LNVDA] NASDAQ:NVD I:NVDACW I:NVDADI I:NVDAIO ANV DIPS NVC NVD NVDB
- `17:20:36` spy           14 rows, 0 with no route | SPY AMEX:SPY NASDAQ:TLT/AMEX:SPY AMEX:RSP/AMEX:SPY AMEX:SPYD INDEX:PCSP SPYQ SPYR SYRE SPYA SPYC SPYD SPYG SPYH
- `17:20:42` qqq           14 rows, 0 with no route | QQQ NASDAQ:QQQ equity-research-tickers:asset:24b0390fca2ad27f indicator-bus:QQQ NASDAQ:QQQE NASDAQ:QQQM NASDAQ:QQQA NASDAQ:QQQ/VANTAGE:SP500 NASDAQ:TQQQ FINRA:TQQQ_SHORT_VOLUME AMEX:CQQQ NASDAQ:SQQQ INDEX:PCQQ QQQX
- `17:20:49` tlt           14 rows, 0 with no route | TLT NASDAQ:TLT SP:SPX/NASDAQ:TLT NASDAQ:TECH/NASDAQ:TLT CBOE:TLTX CBOE:TLTPRO NASDAQ:TLT/AMEX:SPY 1/(TVC:BTPBUND/NASDAQ:TLT) FRED:IRLTLT01EZM156N CBOE:PTLT CBOE:VXTLT VANTAGE:SP500/NASDAQ:TLT TLTP TLTW
- `17:20:49` rows shown by the dialog that the router has no route for: 124 over 30 words; by namespace {'CRYPTOCAP': 18, 'INTOTHEBLOCK': 15, 'HSI': 11, 'FTSE': 10, 'GLASSNODE': 9, 'ECONOMICS': 5, 'CAPITALCOM': 5, 'MCX': 5, 'PEPPERSTONE': 4, 'TVC': 4, 'CME': 3, 'FOREXCOM': 3, 'INDEX': 2, 'BITFINEX': 2, 'CMCMARKETS': 2, 'IC': 2, 'BNC': 1, 'TSX': 1, 'BITTREX': 1, 'TICKMILL': 1}
## 4. Crypto ids opened the way the screen opens them

- `17:20:52` INDEX:BTCUSD       NO        watchlist route | chart SPY         8509 bars, last 2026-11-20 (41 h old) close 689.81 | src data/warm/tv-bars/universe | No supported chart route; chart unchanged
- `17:20:56` X:BTCUSD           NO        direct          | chart X:BTCUSD    2268 bars, last 2026-11-20 (41 h old) close 58814.99 | src Warehouse OHLC · provider  | No supported chart route; chart unchanged
- `17:21:02` BTCUSD             NO        direct          | chart BTCUSDT     4441 bars, last 2026-11-21 (17 h old) close 60102.62109375 | src data/warm/katlin/crypto-ba | No supported chart route; chart unchanged
- `17:21:07` BTC                NO        direct          | chart BTC          579 bars, last 2026-11-20 (41 h old) close 25.78 | src data/warm/tv-bars/universe | No supported chart route; chart unchanged
- `17:21:13` BTC-USD            NO        direct          | chart BTC-USD     4441 bars, last 2026-11-21 (17 h old) close 60098.6484375 | src data/warm/katlin/crypto-ba | No supported chart route; chart unchanged
- `17:21:32` COINBASE:BTCUSD    NO        watchlist route | chart BTC-USD     4441 bars, last 2026-11-21 (17 h old) close 60105.6796875 | src data/warm/katlin/crypto-ba | Possible proxy or source substitution; equivalence unverified
- `17:21:49` BINANCE:BTCUSDT    NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | Possible proxy or source substitution; equivalence unverified
- `17:21:58` BITSTAMP:BTCUSD    CHARTS    watchlist route | chart BTC-USD     4441 bars, last 2026-11-21 (17 h old) close 60079.28125 | src data/warm/katlin/crypto-ba | Possible proxy or source substitution; equivalence unverified
- `17:22:17` CRYPTO:BTCUSD      NO        watchlist route | chart BTC-USD     4441 bars, last 2026-11-21 (17 h old) close 60074.21875 | src data/warm/katlin/crypto-ba | Possible proxy or source substitution; equivalence unverified
- `17:22:34` KRAKEN:XBTUSD      NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | Possible proxy or source substitution; equivalence unverified
- `17:22:37` CRYPTOCAP:BTC      NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | No supported chart route; chart unchanged
- `17:22:40` CRYPTOCAP:BTC.D    NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | No supported chart route; chart unchanged
- `17:22:43` CRYPTOCAP:TOTAL    NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | No supported chart route; chart unchanged
- `17:23:00` INDEX:ETHUSD       NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | Possible proxy or source substitution; equivalence unverified
- `17:23:05` X:ETHUSD           CHARTS    direct          | chart X:ETHUSD    2268 bars, last 2026-11-20 (41 h old) close 1619.29 | src Warehouse OHLC · provider  | Possible proxy or source substitution; equivalence unverified
- `17:23:10` ETHUSD             CHARTS    direct          | chart ETHUSDT     3290 bars, last 2026-11-21 (17 h old) close 1681.5999755859375 | src data/warm/katlin/crypto-ba | Possible proxy or source substitution; equivalence unverified
- `17:23:16` COINBASE:ETHUSD    CHARTS    watchlist route | chart ETH-USD     3290 bars, last 2026-11-21 (17 h old) close 1681.219970703125 | src data/warm/katlin/crypto-ba | Possible proxy or source substitution; equivalence unverified
- `17:23:33` BINANCE:ETHUSDT    NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | Possible proxy or source substitution; equivalence unverified
- `17:23:38` X:SOLUSD           CHARTS    direct          | chart X:SOLUSD    2094 bars, last 2026-11-20 (41 h old) close 68.72 | src Warehouse OHLC · provider  | Possible proxy or source substitution; equivalence unverified
- `17:23:55` BINANCE:SOLUSDT    NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | Possible proxy or source substitution; equivalence unverified
- `17:24:13` COINBASE:SOLUSD    NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | Possible proxy or source substitution; equivalence unverified
- `17:24:18` X:XRPUSD           CHARTS    direct          | chart X:XRPUSD    2267 bars, last 2026-11-20 (41 h old) close 0.9086 | src Warehouse OHLC · provider  | Possible proxy or source substitution; equivalence unverified
- `17:24:35` BINANCE:XRPUSDT    NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | Possible proxy or source substitution; equivalence unverified
- `17:24:41` X:DOGEUSD          CHARTS    direct          | chart X:DOGEUSD   2268 bars, last 2026-11-20 (41 h old) close 0.05426 | src Warehouse OHLC · provider  | Possible proxy or source substitution; equivalence unverified
- `17:24:58` BINANCE:DOGEUSDT   NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | Possible proxy or source substitution; equivalence unverified
- `17:25:15` BINANCE:ETHBTC     NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | Possible proxy or source substitution; equivalence unverified
- `17:25:18` CME:BTC1!          NO        watchlist route | chart None           0 bars, last - (- h old) close None | src None | No supported chart route; chart unchanged
- `17:25:23` AMEX:IBIT          NO        watchlist route | chart IBIT         717 bars, last 2026-11-20 (41 h old) close 33.02 | src data/warm/tv-bars/universe | No supported chart route; chart unchanged
- `17:25:31` SPY                NO        direct          | chart SPY         8509 bars, last 2026-11-20 (41 h old) close 689.81 | src data/warm/tv-bars/universe | No supported chart route; chart unchanged
- `17:25:31` page errors: none
- `17:25:31` ✅ read-only; nothing changed
