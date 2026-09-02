# ops 5116 -- symbol directory birth: justhodl-symdir + first build + route verification

**Status:** failure  
**Duration:** 252.2s  
**Finished:** 2026-09-02T12:42:14+00:00  

## Error

```
SystemExit: 1
```

## Data

| docs | elapsed | failed | fred_banked | fred_catalog | load_s | max_ms | memory | ms | ok | p50_ms | p95_ms | queries | state | step | timeout | url |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | 3008 |  |  |  |  |  | Active | S1 | 900 | https://kin3erhrht63e6j7iwbckozy4u0lxsga.lambda-url.us-east-1.on.aws |
| 1370341 | 200.7 |  | 275486 | 746200 |  |  |  |  |  |  |  |  |  | S3 |  |  |
| 1370341 |  |  |  |  | 7.57 |  |  | 7781 |  |  |  |  |  | S5-warm |  |  |
|  |  |  |  |  |  | 2408 |  |  |  | 236 | 788 | 31 |  | S5-search |  |  |
|  |  | 0 |  |  |  |  |  | 404 | 6 |  |  |  |  | S5-quote |  |  |

## Log
## S1 deploy justhodl-symdir

- `12:38:02` BLS_API_KEY inherited from bls-labor-agent
- `12:38:02`   zip: 121788 bytes
## 1. Lambda

- `12:38:02`   Lambda missing — creating
- `12:38:08` ✅   ✓ created justhodl-symdir
- `12:38:08` ✅   ✓ Function URL: https://kin3erhrht63e6j7iwbckozy4u0lxsga.lambda-url.us-east-1.on.aws/
## S2 schedules

- `12:38:09` ✅ schedule created: justhodl-symdir-build cron(40 5 * * ? *)
- `12:38:09` ✅ schedule created: justhodl-symdir-warm rate(5 minutes)
## S3 first real build

- `12:38:29`   polling manifest… An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:38:50`   polling manifest… An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:39:10`   polling manifest… An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:39:30`   polling manifest… An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:39:50`   polling manifest… An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:40:11`   polling manifest… An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:40:31`   polling manifest… An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:40:51`   polling manifest… An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:41:12`   polling manifest… An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:41:32` ✅ directory built: docs=1,370,341 tokens=1,295,865 postings=16,465,518 instruments=55,966 elapsed=200.7s
- `12:41:32`   bytes: {"docs_pkl_gz": 19266075, "index_pkl_gz": 44953086, "instruments_json_gz": 609416}
- `12:41:32`   fred         series=  746,206 datasets=      0 instruments=     0
- `12:41:32`   bls          series=  443,357 datasets=      0 instruments=     0
- `12:41:32`   boj          series=   64,064 datasets=     21 instruments=     0
- `12:41:32`   instrument   series=        0 datasets=      0 instruments=46,346
- `12:41:32`   worldbank    series=        0 datasets= 29,490 instruments=     0
- `12:41:32`   census       series=   11,250 datasets=     21 instruments=     0
- `12:41:32`   tv           series=        0 datasets=      0 instruments= 9,620
- `12:41:32`   statcan      series=        0 datasets=  8,261 instruments=     0
- `12:41:32`   eurostat     series=        0 datasets=  8,152 instruments=     0
- `12:41:32`   oecd         series=        0 datasets=  1,545 instruments=     0
- `12:41:32`   ofr-bsrm     series=      497 datasets=      0 instruments=     0
- `12:41:32`   ofr-hfm      series=      497 datasets=      0 instruments=     0
- `12:41:32`   ofr          series=      400 datasets=      3 instruments=     0
- `12:41:32`   ecb          series=        0 datasets=    318 instruments=     0
- `12:41:32`   imf          series=        0 datasets=    218 instruments=     0
- `12:41:32`   boe          series=       35 datasets=      0 instruments=     0
- `12:41:32`   bis          series=        0 datasets=     29 instruments=     0
- `12:41:32`   nyfed        series=       10 datasets=      0 instruments=     0
- `12:41:32`   cboe         series=        0 datasets=      1 instruments=     0
- `12:41:32`   source instruments: {"counts": {"stocks": 13144, "otc": 17956, "indices": 13336, "crypto": 625, "fx": 1207, "finviz-only": 78, "tv-dictionary": 9620}, "finviz": 11636, "symbology": 10391, "docs": 55966, "s": 5.1}
- `12:41:32`   source fred: {"meta_pages": 2229, "catalog": 746200, "banked": 275486, "te_mirror": 159, "archived": 23, "docs": 746206, "s": 63.8}
- `12:41:32`   source eurostat: {"flows": 8152, "tier0": 8147, "docs": 8152, "s": 0.1}
- `12:41:32`   source ecb: {"flows": 214, "tier0": 207, "docs": 318, "s": 1.0}
- `12:41:32`   source oecd: {"flows": 1546, "docs": 1546, "s": 0.1}
- `12:41:32`   source bis: {"flows": 29, "docs": 29, "s": 0.0}
- `12:41:32`   source imf: {"flows": 218, "catalog": "data/warm/imf-full/manifest.json", "docs": 218, "s": 0.2}
- `12:41:32`   source statcan: {"cubes": 8261, "banked": 8262, "docs": 8261, "s": 2.3}
- `12:41:32`   source nyfed: {"series": 10, "docs": 10, "s": 0.0}
- `12:41:32`   source ofr: {"series": 1394, "docs": 1397, "s": 1.2}
- `12:41:32`   source treasury: {"series": 0, "docs": 0, "s": 0.5}
- `12:41:32`   source boe: {"series": 35, "docs": 35, "s": 0.0}
- `12:41:32`   source census: {"series": 11250, "docs": 11271, "s": 7.1}
- `12:41:32`   source cboe: {"docs": 1, "s": 0.2}
- `12:41:32`   source boj: {"parts": 15963, "series": 64064, "dbs": 21, "partial": false, "docs": 64085, "s": 66.6}
- `12:41:32`   source worldbank: {"indicators": 29544, "countries": 295, "docs": 29544, "s": 0.4}
- `12:41:32`   source bls: {"series": 443357, "files": 55, "docs": 443357, "s": 10.7}
- `12:41:32` ⚠ skipped: bls-no-title:bg.series, bls-no-title:li.series, bls-no-title:bp.series, bls-no-title:jl.series, bls-no-title:pr.series, bls-no-title:eb.series, bls-no-title:ec.series, bls-no-title:gg.series, bls-no-title:jt.series, bls-no-title:ee.series, bls-no-title:in.series, bls-no-title:cc.series, bls-no-title:mw.series, bls-no-title:mu.series, bls-no-title:gp.series, bls-no-title:si.series, bls-no-title:hs.series, bls-no-title:pd.series, bls-no-title:sm.series, bls-no-title:sh.series, bls-no-title:sa.series, bls-no-title:ml.series, bls-no-title:nc.series, bls-no-title:cf.series, bls-no-title:hc.series, bls:cd.series, bls:tu.series, bls:nb.series, bls:cx.series, bls:fa.series, bls:ii.series
## S4 endpoint

- `12:41:32` ✅ data/symdir/endpoint.json published: https://kin3erhrht63e6j7iwbckozy4u0lxsga.lambda-url.us-east-1.on.aws
## S5 routes: /health /warm

- `12:41:32`   /health 284ms: {"ok": true, "version": "1.0.0", "index_loaded": false, "docs": null, "directory_built_at": "2026-09-02T12:41:31+00:00", "directory_docs": 1370341, "routes": ["/search?q=", "/browse?ds=&q=", "/series?id=", "/quote?ids=", "/warm"]}
- `12:41:40`   /warm 7781ms: {"docs": 1370341, "built_at": "2026-09-02T12:41:10+00:00", "ok": true, "load_s": 7.57}
## S5 routes: /search battery

- `12:41:42`   'unemployment rate germany'                      total=  1163  2408ms   2669B  fred:LNS14027662 (fred/ser) Unemployment Rate - Bachelor's Degree  | fred:CGBD2534 (fred/ser) Unemployment Rate - College Graduates  | fred:CGBD2024 (fred/ser) Unemployment Rate - College Graduates  | ECONOMICS:CDUR (tv/ins) Congo, Dem. Rep. — Unemployment Rate | ECONOMICS:DEUR (tv/ins) Germany — Unemployment Rate
- `12:41:43`   'dgs10'                                          total=     1   219ms    482B  fred:DGS10 (fred/ser) Market Yield on U.S. Treasury Securiti
- `12:41:43`   'AAPL'                                           total=    14   210ms   2556B  AAPL (instrument/ins) Apple Inc | NASDAQ:AAPL (tv/ins) Apple Inc. | I:AAPLCW (instrument/ins)  | I:AAPLDI (instrument/ins)  | I:AAPLIO (instrument/ins) 
- `12:41:43`   'apple'                                          total=   150   208ms   2643B  AAPL (instrument/ins) Apple Inc | APLE (instrument/ins) Apple Hospitality REIT Inc | eurostat:ORCH_APPLES2 (eurostat/dat) Apple and pear trees - Area by density | NASDAQ:AAPL (tv/ins) Apple Inc. | AAPX (instrument/ins) T-Rex 2X Long Apple Daily Target ETF
- `12:41:43`   'nvidia'                                         total=     5   211ms   1783B  NVDA (instrument/ins) NVIDIA Corp | NASDAQ:NVDA (tv/ins) Nvidia Corp | NVDQ (instrument/ins) T-Rex 2X Inverse NVIDIA Daily Target E | NVDX (instrument/ins) T-Rex 2X Long NVIDIA Daily Target ETF | NVPS (instrument/ins) PurePlay Nvidia Ecosystem Picks & Shov
- `12:41:44`   'gdp'                                            total=181031   711ms   2367B  fred:GDP (fred/ser) Gross Domestic Product | fred:GDPA (fred/ser) Gross Domestic Product | GDPCW (instrument/ins) GOODRICH PETE CP WT 26 | GDPEF (instrument/ins) RESOURCE CAP GOLD CORP | GDPHF (instrument/ins) ORBIT RESOURCES LTD
- `12:41:45`   'cpi'                                            total=220237   788ms   2433B  imf:CPI (imf/dat) CPI | CPIX (instrument/ins) Cumberland Pharmaceuticals Inc | CPII (instrument/ins) American Beacon Ionic Inflation Protec | fred:CPILFESL (fred/ser) CPILFESL | PMTS (instrument/ins) CPI Card Group Inc
- `12:41:45`   'fed funds'                                      total=  1363   232ms   2733B  fred:FEDFUNDS (fred/ser) Federal Funds Effective Rate | ECONOMICS:MXINTR-FRED:FEDFUNDS (tv/ins) MXINTR-FRED:FEDFUNDS (ECONOMICS) | TVC:US06MY-FRED:FEDFUNDS (tv/ins) US06MY-FRED:FEDFUNDS (TVC) | fred:FEDTARMD (fred/ser) FOMC Summary of Economic Projections f | fred:FRPACBW027SBOG (fred/ser) Fed Funds and Reverse RPs with Banks, 
- `12:41:45`   'sofr'                                           total=   798   216ms   2385B  SOFR (instrument/ins) Amplify Samsung SOFR ETF | nyfed:sofr (nyfed/ser) Secured Overnight Financing Rate (SOFR | fred:SOFR (fred/ser) Secured Overnight Financing Rate | fred:SOFRINDEX (fred/ser) SOFR Index | CMCMARKETS:SOFR3MO (tv/ins) SOFR3MO (CMCMARKETS)
- `12:41:45`   '10 year treasury yield'                         total=   151   236ms   2862B  fred:DGS10 (fred/ser) Market Yield on U.S. Treasury Securiti | fred:DFII10 (fred/ser) Market Yield on U.S. Treasury Securiti | BBBI (instrument/ins) BondBloxx BBB Rated 5-10 Year Corporat | BBBL (instrument/ins) BondBloxx BBB Rated 10+ Year Corporate | fred:BAA10Y (fred/ser) Moody's Seasoned Baa Corporate Bond Yi
- `12:41:46`   'hicp euro area'                                 total=   200   220ms   2988B  eurostat:PRC_HICP_CTR (eurostat/dat) Harmonised index of consumer prices (H | fred:FPCPITOTLZGEMU (fred/ser) Inflation, consumer prices for the Eur | fred:CPHPTT01EZM659N (fred/ser) Consumer Price Index: Harmonised Price | fred:LRHUTTTTEZM156S (fred/ser) Harmonised Unemployment - Monthly Rate | fred:EA19CPHPTT01IXEBM (fred/ser) Consumer Price Index: Harmonised Price
- `12:41:46`   'exchange rate usd eur'                          total=    22   242ms   2950B  fred:DEXUSEU (fred/ser) U.S. Dollars to Euro Spot Exchange Rat | fred:EXUSEU (fred/ser) U.S. Dollars to Euro Spot Exchange Rat | fred:AEXUSEU (fred/ser) U.S. Dollars to Euro Spot Exchange Rat | fred:CCUSMA02EZM618N (fred/ser) Currency Conversions: US Dollar Exchan | fred:CCUSMA02EZQ618N (fred/ser) Currency Conversions: US Dollar Exchan
- `12:41:46`   'nama_10_gdp'                                    total=    14   255ms   2706B  eurostat:NAMA_10_GDP (eurostat/dat) Produit intérieur brut (PIB) et princi | eurostat:NAMA_10_A64 (eurostat/dat) Gross value added and income by detail | eurostat:NAMA_10R_3NLP (eurostat/dat) Nominal Labour productivity by NUTS 3  | eurostat:NAMA_10_LP_ULC (eurostat/dat) Productivité du travail et coût salari | eurostat:NAMA_10R_2NLP (eurostat/dat) Nominal Labour productivity by NUTS 2 
- `12:41:47`   'eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE'      total=     0   512ms    276B  
- `12:41:47`       suggest: ['degree', 'debt', 'deposits', 'depository', 'death', 'dealers', 'deposit', 'development']
- `12:41:48`   'ecb:EXR:EXR.D.USD'                              total=     1  1770ms   1061B  ecb:ECB.DISS:JDF_EXR_HCI_GDP (ecb/dat) Harmonised competitiveness indicators 
- `12:41:48`       series_hits tier=0 known=True in_flow=4027 rows=['ecb:EXR:EXR.D.USD.EUR.SP00.A']
- `12:41:49`   'bank of japan'                                  total=   418   218ms   2284B  fred:JPNASSETS (fred/ser) Bank of Japan: Total Assets for Japan | ECONOMICS:JPCBBS (tv/ins) Bank of Japan: Total Assets for Japan | JPM (instrument/ins) JPMorgan Chase & Co | boj:BP01 (boj/dat) Bank of Japan database BP01 | boj:BS02 (boj/dat) Bank of Japan database BS02
- `12:41:49`   'canada employment'                              total=  1572   249ms   2807B  I:NQUSS50205025 (instrument/ins) Nasdaq US Small Cap Business Training  | I:NQUSS50205025N (instrument/ins) Nasdaq US Small Cap Business Training  | I:NQUSS50205025T (instrument/ins) Nasdaq US Small Cap Business Training  | fred:LFEACNTTCAA647N (fred/ser) Infra-Annual Labor Statistics: Employm | fred:CANPEFANA (fred/ser) Percent of Employment in Manufacturing
- `12:41:49`   'money market funds'                             total=  1788   229ms   2786B  fred:MMMFFAQ027S (fred/ser) Money Market Funds; Total Financial As | fred:MMMFTAQ027S (fred/ser) Money Market Funds; Total Financial As | fred:BOGZ1FL632051103Q (fred/ser) Money Market Funds; Security Repurchas | fred:MMMFFAA027N (fred/ser) Money Market Funds; Total Financial As | fred:BOGZ1FL633061105Q (fred/ser) Money Market Funds; Treasury Securitie
- `12:41:49`   'oil'                                            total= 10931   256ms   2618B  MARKETSCOM:OIL (tv/ins) OIL (MARKETSCOM) | OILD (instrument/ins) MicroSectors Oil & Gas E&P -3X ETN | OILK (instrument/ins) ProShares K-1 Free Crude Oil ETF | OILT (instrument/ins) Texas Capital Texas Oil Index ETF | OILU (instrument/ins) MicroSectors Oil & Gas E&P 3X ETN
- `12:41:50`   'bitcoin'                                        total=   156   234ms   2589B  BIXI (instrument/ins) Bitcoin Infrastructure Acquisition Cor | ABTC (instrument/ins) American Bitcoin Corp | I:BETEIV (instrument/ins) BITCOIN & ETHER EQUAL WEIGHT STRATEGY  | I:BETHIV (instrument/ins) BITCOIN & ETHER MARKET CAP WEIGHT STRA | X:BCHEUR (instrument/ins) Bitcoin Cash - Euro
- `12:41:50`   'vix'                                            total=   443   212ms   2543B  CAPITALCOM:VIX (tv/ins) VIX (CAPITALCOM) | CBOE:VIX (tv/ins) VIX (CBOE) | HOSE:VIX (tv/ins) VIX (HOSE) | PEPPERSTONE:VIX (tv/ins) VIX (PEPPERSTONE) | TVC:VIX (tv/ins) CBOE Volatility Index: VIX
- `12:41:50`   'TVC:VIX'                                        total=   753   227ms   2782B  TVC:VIX (tv/ins) CBOE Volatility Index: VIX | TVC:MOVE (tv/ins) ICE BofA MOVE Index (bond volatility) | fred:VIXCLS (fred/ser) CBOE Volatility Index: VIX | BDVL (instrument/ins) iShares Disciplined Volatility Equity  | CDC (instrument/ins) VictoryShares US EQ Income Enhanced Vo
- `12:41:50`   'world bank gdp'                                 total=   412   249ms   3057B  worldbank:DT.AMT.DECT.CD.MA.RM.US (worldbank/dat) Gross Ext. Debt Pos., Central Bank, On | worldbank:DT.AMT.DLBN.CD.MA.AR.03.US (worldbank/dat) Gross Ext. Debt Pmt, Central Bank, Mor | worldbank:DT.AMT.DLBN.CD.MA.AR.0912.US (worldbank/dat) Gross Ext. Debt Pmt, Central Bank, Mor | worldbank:DT.AMT.DLBN.CD.MA.AR.1218.US (worldbank/dat) Gross Ext. Debt Pmt, Central Bank, Mor | worldbank:DT.AMT.DLBN.CD.MA.AR.1824.US (worldbank/dat) Gross Ext. Debt Pmt, Central Bank, Mor
- `12:41:51`   'durable goods'                                  total= 11315   261ms   2958B  fred:DGORDER (fred/ser) Manufacturers' New Orders: Durable Goo | fred:TX31333100M175FRBDAL (fred/ser) Durable Goods: Agriculture, Constructi | fred:TX31321000A175FRBDAL (fred/ser) Durable Goods: Wood Product Manufactur | fred:TX31321000A674FRBDAL (fred/ser) Durable Goods: Wood Product Manufactur | fred:TX31321000M158FRBDAL (fred/ser) Durable Goods: Wood Product Manufactur
- `12:41:51`   'payrolls'                                       total= 75530   410ms   2810B  RHI (instrument/ins) Robert Half Inc | KFY (instrument/ins) Korn Ferry | TNET (instrument/ins) TriNet Group Inc | MAN (instrument/ins) ManpowerGroup | NSP (instrument/ins) Insperity Inc
- `12:41:51`   'm2'                                             total= 10949   254ms   2223B  fred:M2 (fred/ser) M2 (DISCONTINUED) | fred:M2SL (fred/ser) M2 | fred:M2NS (fred/ser) M2 | fred:M2V (fred/ser) Velocity of M2 Money Stock | CME_MINI:M2K1! (tv/ins) M2K1! (CME_MINI)
- `12:41:52`   'japan cpi'                                      total=  1188   250ms   2556B  fred:QJPN628BIS (fred/ser) Residential Property Prices for Japan | fred:FPCPITOTLZGJPN (fred/ser) Inflation, consumer prices for Japan | I:BXTBJPY (instrument/ins) Cboe TLT 2 OTM BuyWrite NTR Index JPY | I:BXVBWJPY (instrument/ins) Cboe S&P 500 Enhanced 1 OTM BuyWrite N | I:DJJPSDJN (instrument/ins) DJ Japan Select Dividend Index JPY NTR
- `12:41:52`   'spread oas'                                     total=    12   219ms   2701B  OASC (instrument/ins) OneAscent Enhanced Small and Mid Cap E | X:ROSEUSD (instrument/ins) Oasis Network - United States dollar | AOAAS (instrument/ins) ARRIVED STR LLC OASIS | GRNO (instrument/ins) GREEN OASIS ENVIRNMNTL | OHTR (instrument/ins) OASIS HOTEL&RESORT CASINO
- `12:41:52`   'policy rate'                                    total=     9   219ms   2462B  fred:BOERUKM (fred/ser) Bank of England Policy Rate in the Uni | fred:KCPRU (fred/ser) Kansas City Fed's Measure of Policy Ra | fred:KCPRS (fred/ser) Kansas City Fed's Policy Rate Skew (KC | fred:BOERUKQ (fred/ser) Bank of England Policy Rate in the Uni | bis:WS_CBPOL (bis/dat) Central bank policy rates
- `12:41:53`   'ppi'                                            total=179483   629ms   2426B  PPI (instrument/ins) Astoria Real Assets ETF | AMEX:PPI (tv/ins) PPI (AMEX) | imf:PPI (imf/dat) PPI | PPIH (instrument/ins) Perma-Pipe International Holdings Inc | PPINF (instrument/ins) PHILIPPINE NATL BK
- `12:41:53`   'qwzzx'                                          total=     0   208ms    178B  
- `12:41:53`       suggest: ['qwest', 'qwld', 'qwtr']
- `12:41:53` ✅   rank check 'dgs10' -> ['fred:DGS10'] want first=fred:DGS10
- `12:41:53` ✅   rank check 'AAPL' -> ['AAPL', 'NASDAQ:AAPL', 'I:AAPLCW'] want first=AAPL
- `12:41:53` ⚠   rank check 'sofr' -> ['SOFR', 'nyfed:sofr', 'fred:SOFR'] want first=nyfed:sofr
- `12:41:54` ✅   rank check 'unrate' -> ['fred:UNRATE', 'fred:UNRATENSA', 'fred:UNRATEMD'] want first=fred:UNRATE
- `12:41:54` ✅   rank check 'TVC:VIX' -> ['TVC:VIX', 'TVC:MOVE', 'fred:VIXCLS'] want first=TVC:VIX
## S5 routes: /browse

- `12:41:54`   eurostat:NAMA_10_GDP         q='DE'       total=35665 matched=13 scanned=500 tier=0 426ms first=['eurostat:NAMA_10_GDP:A.CLV05_MEUR.D31.DE', 'eurostat:NAMA_10_GDP:A.CLV05_MEUR.P3.DE'] facets={'freq': 1, 'unit': 1, 'na_item': 13, 'geo': 40} err=None
- `12:41:55`   eurostat:UNE_RT_M            q='DE'       total=2000 matched=13 scanned=500 tier=0 312ms first=['eurostat:UNE_RT_M:M.NSA.TOTAL.THS_PER.T.DE', 'eurostat:UNE_RT_M:M.NSA.Y25-74.PC_ACT.F.DE'] facets={'freq': 1, 's_adj': 1, 'age': 3, 'unit': 2, 'sex': 3, 'geo': 38} err=None
- `12:41:55`   ecb:EXR                      q='USD'      total=4027 matched=12 scanned=500 tier=0 246ms first=['ecb:EXR:EXR.A.E01.USD.EN00.A', 'ecb:EXR:EXR.A.E01.USD.ERC0.A'] facets={'FREQ': 1, 'CURRENCY': 9, 'CURRENCY_DENOM': 40, 'EXR_TYPE': 12, 'EXR_SUFFIX': 2, 'SOURCE_AGENCY': 2} err=None
- `12:41:55`   statcan:10100001             q=''         total=None matched=None scanned=None tier=None 270ms first=[] facets={} err=no VECTOR column
- `12:41:55`   worldbank:NY.GDP.MKTP.CD     q='united'   total=295 matched=3 scanned=None tier=None 259ms first=['worldbank:NY.GDP.MKTP.CD:ARE', 'worldbank:NY.GDP.MKTP.CD:GBR'] facets={} err=None
- `12:41:56`   boj:BP01                     q='assets'   total=558 matched=558 scanned=None tier=None 464ms first=['boj:BP01:BPBP6D1A', 'boj:BP01:BPBP6D1A1'] facets={} err=None
- `12:41:56`   census:advm3                 q=''         total=320 matched=320 scanned=None tier=None 426ms first=['census:advm3:MPCNO:31S:no:US', 'census:advm3:MPCNO:31S:yes:US'] facets={} err=None
- `12:41:57`   treasury:debt_to_penny       q=''         total=0 matched=0 scanned=None tier=None 448ms first=[] facets={} err=None
- `12:41:57`   ofr:mmf                      q=''         total=42 matched=42 scanned=None tier=None 454ms first=['ofr:MMF-MMF_AG_TOT-M', 'ofr:MMF-MMF_BRA_TOT-M'] facets={} err=None
## S5 routes: /series (full history, every resolver)

- `12:41:58`   fred:DGS10                                         n= 16134 first=1962-01-02 last=2026-08-06   1019ms   322854B src=warehouse:fred-scoped/Interest_Rates cached=False name='Market Yield on U.S. Treasury Securities at 10-Yea' err=None
- `12:41:59`   fred:UNRATE                                        n=   942 first=1948-01-01 last=2026-07-01    493ms    18231B src=warehouse:fred-scoped/Population,_Employment,_&_Labor_Markets cached=False name='Unemployment Rate' err=None
- `12:41:59`   fred:GDPC1                                         n=   318 first=1947-01-01 last=2026-04-01    326ms     8055B src=warehouse:fred-scoped/National_Accounts cached=False name='Real Gross Domestic Product' err=None
- `12:42:00`   fred:BAMLC0A0CM                                    n=  7745 first=1996-12-31 last=2026-08-28   1011ms   154493B src=warehouse:fred-scoped/Interest_Rates cached=False name='ICE BofA US Corporate Index Option-Adjusted Spread' err=None
- `12:42:01`   eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE          n=    35 first=1991-01-01 last=2025-01-01    731ms     1440B src=eurostat-api cached=False name='Produit intérieur brut (PIB) et principales compos' err=None
- `12:42:01`   eurostat:UNE_RT_M:M.NSA.TOTAL.PC_ACT.T.DE          n=   235 first=2007-01-01 last=2026-07-01    593ms     4970B src=eurostat-api cached=False name='Arbeitslosendaten nach Geschlecht und Alter - mona' err=None
- `12:42:03`   ecb:EXR:EXR.D.USD.EUR.SP00.A                       n=  7083 first=1999-01-04 last=2026-09-01   1613ms   155504B src=ecb-api cached=False name='ECB reference exchange rate, US dollar/Euro, 2.15 ' err=None
- `12:42:04`   ecb:ICP:ICP.M.U2.N.000000.4.ANR                    n=   348 first=1997-01-01 last=2025-12-01   1050ms     7157B src=ecb-api cached=False name='Euro area (changing composition) - HICP - Overall ' err=None
- `12:42:05`   nyfed:sofr                                         n=  2103 first=2018-04-02 last=2026-09-01    694ms    42086B src=nyfed-api cached=False name='Secured Overnight Financing Rate (SOFR)' err=None
- `12:42:06`   nyfed:effr                                         n=  3181 first=2014-01-02 last=2026-08-31    796ms    63579B src=nyfed-api cached=False name='Effective Federal Funds Rate (EFFR)' err=None
- `12:42:06`   worldbank:NY.GDP.MKTP.CD:USA                       n=    66 first=1960-01-01 last=2025-01-01    348ms     2398B src=worldbank-api cached=False name='GDP (current US$) — United States' err=None
- `12:42:06`   boe:CFMBI59                                        n=   144 first=2004-01-31 last=2015-12-31    350ms     3155B src=warehouse:boe iadb cached=False name='Bank of England IADB CFMBI59' err=None
- `12:42:08`   bls:CES0000000001                                  n=  1051 first=1939-01-01 last=2026-07-01   1322ms    24969B src=bls-api-v2 cached=False name='All employees, thousands, total nonfarm, seasonall' err=None
- `12:42:09`   bls:CUUR0000SA0                                    n=  1362 first=1913-01-01 last=2026-07-01   1883ms    28524B src=bls-api-v2 cached=False name='All items in U.S. city average, all urban consumer' err=None
- `12:42:10` ✗   treasury:debt_to_penny:tot_pub_debt_out_amt: HTTP Error 500: Internal Server Error
- `12:42:13`   boj:BP01:BPBP6D1A                                  n=     0 first=None last=None   3013ms      336B src=warehouse:boj api parts (665) cached=False name='External Assets and Liabilities of Banks, etc./Tot' err=None
- `12:42:13` ⚠   boj:BP01:BPBP6D1A: empty (warehouse:boj api parts (665))
- `12:42:13`   census:advm3:MPCNO:31S:no:US                       n=   413 first=1992-03-01 last=2026-07-01    566ms     8520B src=warehouse:census-us cached=False name="Advance Report on Durable Goods Manufacturers' Shi" err=None
- `12:42:14`   ofr:MMF-MMF_AG_TOT-M                               n=   189 first=2010-11-30 last=2026-07-31    304ms     6199B src=warehouse:ofr cached=False name='MMF-MMF_AG_TOT-M' err=None
## S5 routes: /quote batch

- `12:42:14`   fred:DGS10                                       ok=True last=4.69 @2026-08-06 chg%=1.296 mom%=3.077 yoy%=11.137 n=16134 err=None
- `12:42:14`   fred:UNRATE                                      ok=True last=4.1 @2026-07-01 chg%=-2.381 mom%=-2.381 yoy%=-4.651 n=942 err=None
- `12:42:14`   nyfed:sofr                                       ok=True last=3.66 @2026-09-01 chg%=-0.543 mom%=0.0 yoy%=-15.668 n=2103 err=None
- `12:42:14`   ecb:EXR:EXR.D.USD.EUR.SP00.A                     ok=True last=1.159 @2026-09-01 chg%=-0.052 mom%=0.914 yoy%=-1.067 n=7083 err=None
- `12:42:14`   eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE        ok=True last=3096871.1 @2025-01-01 chg%=0.229 mom%=0.229 yoy%=0.229 n=35 err=None
- `12:42:14`   worldbank:NY.GDP.MKTP.CD:USA                     ok=True last=30769700000000.0 @2025-01-01 chg%=5.023 mom%=5.023 yoy%=5.023 n=66 err=None
## verdict

- `12:42:14` elapsed 252s
- `12:42:14` ✗ ranking: 'sofr' first=['SOFR'] want nyfed:sofr
- `12:42:14` ✗ browse statcan:10100001: no VECTOR column
- `12:42:14` ✗ series treasury:debt_to_penny:tot_pub_debt_out_amt failed: HTTP Error 500: Internal Server Error
