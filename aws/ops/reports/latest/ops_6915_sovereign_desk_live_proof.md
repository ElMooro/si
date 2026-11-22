# ops 6915 - the Sovereign button, its desk and the CISS page, real browser on the live site (read-only), 2026-11-22T23:21+00:00

**Status:** success  
**Duration:** 508.4s  
**Finished:** 2026-11-22T23:29:59+00:00  

## Data

| board_rows | ciss_rows | ciss_table_rows | dollar_observations | ended_rows | failed_checks | measures | tiles | world_rows | world_series |
|---|---|---|---|---|---|---|---|---|---|
| 11 | 88 | 13 | 46610 | 6 | 0 | 266 | 14 | 44 | 137 |

## Log
## 0. The live site serves the new files

- `23:28:22` served after 21 tries: {"engine": true, "rail": true, "desk_page": true, "desk_script": true, "dollar_desk": true, "ciss_page": true, "ciss_script": true, "fiscal_page": true}
- `23:28:22` scripts as the pages name them: /jh-sovereign-desk.js?v=0d6a3a68 | /jh-ciss-research.js?v=62f3959f
- `23:28:25` dollar-shortage document 1.1.0, generated 2026-11-22T22:08:24: 15 measures (12 Federal Reserve) in 5 parts, 46610 observations, errors {}
- `23:28:25` documents: desk 2026-11-22T16:39:59, 266 measures, 30 countries, errors {} | history 266 series | warehouse 2026-11-22T07:13:48
- `23:28:25` installing playwright
## 1. The button on the chart

- `23:28:34` button: {"btn": true, "prev": "btn-repo", "next": "btn-usd", "label": "SovSovereign", "title": "Sovereign stress \u00b7 SovCISS, spreads to Germany, TARGET, crisis peaks", "box": [560, -215, 72, 44], "repoPrev": "btn-cds", "overlay": "", "src": null, "head": null}
- `23:28:44` clicked by mouse at box [560, 52, 72, 44] -> overlay on | Sovereign stress · SovCISS, spreads, TARGET, crisis peaks | /sovereign-desk.html
## 2. The Desk, inside the chart

- `23:28:46` as of: ECB data to 19 Nov 2026 · compiled 2026-11-22 16:39 UTC · 266 measures, 30 countries
- `23:28:46`   read: Euro-area sovereign stress (SovCISS, GDP-weighted) reads 0.0112 on 19 Nov 2026 — lower than 76% of its readings since 2000; record 0.7514, 29 Nov 2011.
- `23:28:46`   read: Highest: France 0.1739, Belgium 0.1613, Greece 0.1007. Lowest: Portugal 0.0038. Highest minus lowest: 0.1702 (record 0.9133, 6 Jul 2015).
- `23:28:46`   read: All euro-area governments together pay 59.8 bp more than the AAA-rated ones at 10 years (19 Nov 2026; record 194.8 bp, 23 Jul 2012).
- `23:28:46`   read: Widest 10-year spreads to Germany in the euro area (Oct 2026): France 147.4 bp, Italy 126.4 bp, Greece 99.4 bp, Malta 97.4 bp.
- `23:28:46`   read: Over the past week: largest rise Greece +0.0171; largest fall Belgium −0.1387.
- `23:28:46`   read: Systemic stress outside the euro area, by the ECB's CISS: United States 0.0145, China 0.0170 (13 Nov 2026), United Kingdom 0.0200; euro area 0.0095.
- `23:28:46` tiles: EA_SOV_GDP 0.0112 19 Nov 2026 (19 Nov 2026) | EA_SOV_EQ 0.0069 19 Nov 2026 (19 Nov 2026) | EA_SOV_RANGE 0.1702 19 Nov 2026 (19 Nov 2026) | EA_CURVE_10Y 59.8 bp 19 Nov 2026 (19 Nov 2026) | EA_CURVE_2Y 16.8 bp 19 Nov 2026 (19 Nov 2026) | EA_ALL_10Y 4.04% 19 Nov 2026 (19 Nov 2026) | EA_AAA_10Y 3.44% 19 Nov 2026 (19 Nov 2026) | Y10_U2 4.08% Oct 2026 (Oct 2026) | EA_TGT_DEBT €1,307.5 bn Sep 2026 (Sep 2026) | EA_CISS 0.0095 19 Nov 2026 (19 Nov 2026) | EA_CON_BMN 0.0336 19 Nov 2026 (19 Nov 2026)
- `23:28:46` outside the euro area: CISS_US 0.0145 19 Nov 2026 | CISS_CN 0.0170 13 Nov 2026 | CISS_GB 0.0200 19 Nov 2026
- `23:28:46` board: France 0.1739 | Belgium 0.1613 | Greece 0.1007 | Italy 0.0474 | Austria 0.0422 | Germany 0.0303 | Ireland 0.0242 | Netherlands 0.0173 | Finland 0.0060 | Spain 0.0057 | Portugal 0.0038
- `23:28:46` France's row: France | 0.1739 | +0.1437 | −0.0286 | −0.0812 | +0.0956 | 76 | 0.7635 Jul 2012 | 0.0895 | 4.89% | 147.4 bp | 90.2 bp | A+ | −€183.5 bn | 119.1% | −5.1% | 2.4% | 0.1002 | 
- `23:28:46` SovCISS the ECB stopped: Denmark 0.3226 | Poland 0.1793 | Sweden 0.1234 | United Kingdom 0.0895 | Hungary 0.0850 | Czechia 0.0172
- `23:28:46` CISS by country: France 0.0895 (19 Nov 2026) | Belgium 0.0592 (19 Nov 2026) | United Kingdom 0.0200 (19 Nov 2026) | China 0.0170 (13 Nov 2026) | United States 0.0145 (19 Nov 2026) | Italy 0.0095 (19 Nov 2026) | Finland 0.0087 (19 Nov 2026) | Germany 0.0080 (19 Nov 2026 the ECB's 20 Nov row is blank) | Ireland 0.0073 (19 Nov 2026) | Portugal 0.0053 (19 Nov 2026) | Austria 0.0052 (19 Nov 2026) | Spain 0.0050 (19 Nov 2026 the ECB's 20 Nov row is blank) | Netherlands 0.0049 (19 Nov 2026)
## 2b. Tags and sorting

- `23:28:47` before: france belgium greece italy austria germany ireland netherlands finland spain portugal
- `23:28:47` Italy tagged red, Greece amber: greece italy france belgium austria germany ireland netherlands finland spain portugal | CISS table starts ['italy', 'france']
- `23:28:48` sorted by CISS, high to low: greece italy france belgium finland germany ireland portugal austria spain netherlands
- `23:28:48` sorted by CISS, low to high: greece italy netherlands spain austria portugal ireland germany finland belgium france
- `23:28:48` kept in the browser: {"tags":{"italy":"red","greece":"amber"},"sort":{"sovereign":["ciss",1]},"show":"all","lines":"all"}
- `23:28:48` clicking the country name France opens: France SovCISS
- `23:28:49` clicking United States opens: United States CISS | 0.0145 · 19 Nov 2026 · lower than 74% of its readings since 1980 CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX · source value
## 3. A chart

- `23:28:49` chart: Euro area SovCISS, GDP-weighted | 0.0112 · 19 Nov 2026 · lower than 76% of its readings since 2000 CISS.D.U2.Z0Z.4F.EC.SOV_GDPWN.IDX · source value 0.011230409327541846 the series at the ECB · the respons | canvas 1066x385, 7923 line pixels
- `23:28:49`   History from 5 Jan 2000 · 7,012 observations · high 0.7514 on 29 Nov 2011 · low 0.0004 on 6 Nov 2025 · mean 0.1030 · the line is the last reading of each week; highs and lows are from every day HIGHEST READING INSIDE EACH NAMED WINDOW (SHADED ON THE CHART) · C
- `23:28:50` five years: 2382 line pixels (all history 7923) | under the pointer: block 0.0301 18 Oct 2024
## 4. Crisis peaks: every set with its history

- `23:28:52`   sovciss  18 series, 19 windows | one chart of all 18 over their whole history (61560 line pixels), legend 18 | tagged first: ['EA_SOV_GDP', 'SOV_GR', 'SOV_IT']
- `23:28:52`            SOV_IT in 'italy2011': page 0.9903, document 0.9903 | now 0.0474 | record 0.9903 | overflow 0 px
- `23:28:54`            a cell opens Italy SovCISS | ranges ['1Y', '5Y', '10Y', 'All', 'Italy and Spain drawn in ± 1 year*'] | 1934 line pixels
- `23:28:56`   spread   15 series, 21 windows | one chart of all 15 over their whole history (10991 line pixels), legend 15 | tagged first: ['EA_CURVE_10Y', 'S10_GR', 'S10_IT']
- `23:28:56`            S10_GR in 'spain2012': page 2,652.0, document 2,652.0 | now 99.4 | record 2,739.0 | overflow 0 px
- `23:28:57`   ciss     14 series, 22 windows | one chart of all 14 over their whole history (63912 line pixels), legend 14 | tagged first: ['EA_CISS', 'CISS_IT', 'CISS_US']
- `23:28:57`            CISS_US in 'crash1987': page 0.7194, document 0.7194 | now 0.0145 | record 0.8967 | overflow 0 px
- `23:28:59`   euro     10 series in different units, 22 windows | 10 small history charts, 10 drawn
- `23:28:59`            TGT_IT in 'gilts2022': page −€714.9, document −€714.9 | now −€324.2 | record −€714.9 | overflow 0 px
- `23:28:59` tags cleared: netherlands spain austria portugal
## 5. World and Sources

- `23:29:00` world: 44 rows | first: Turkey | Europe | 33.55% | −264.0 bp | 244.5 bp | BB- | 36.00% | 22 November 2026
## 5b. The World tab's chart: all countries, or only the tagged ones

- `23:29:01` sets on the page: 10-year yields 30* | 5-year CDS, measured 25 | 10-year yields, as reported 44 | CDS, as reported 38
- `23:29:02`   yield           30 of 44 countries drawn (14250 line pixels), legend 30, mode all | sources {'oecd': 25, 'ecb': 5} | from 1953-04-30 | overflow 0 px
- `23:29:02`   cds             25 of 44 countries drawn (19043 line pixels), legend 25, mode all | sources {'dtcc': 25} | from 2024-09-03 | overflow 0 px
- `23:29:03`   yield_reported  44 of 44 countries drawn (38857 line pixels), legend 44, mode all | sources {'reported': 44} | from 2026-09-26 | overflow 0 px
- `23:29:04`   cds_reported    38 of 44 countries drawn (37774 line pixels), legend 38, mode all | sources {'reported': 38} | from 2026-09-26 | overflow 0 px
- `23:29:06` Turkey and Japan tagged, 'Only tagged' on the long yields: 1 line(s) ['WY_JAPAN'] (Turkey has no monthly yield: True)
- `23:29:07` the United States tagged while 'Only tagged' is on: 2 line(s) ['WY_JAPAN', 'WY_UNITED_STATES']
- `23:29:07` under the pointer: 29 Feb 2004 United States 4.08% Japan 1.21%
- `23:29:08`   'Only tagged' carried to cds             3 line(s) ['WC_JAPAN', 'WC_TURKEY', 'WC_UNITED_STATES']
- `23:29:09`   'Only tagged' carried to yield_reported  3 line(s) ['RY_JAPAN', 'RY_TURKEY', 'RY_UNITED_STATES']
- `23:29:09`   'Only tagged' carried to cds_reported    3 line(s) ['RC_JAPAN', 'RC_TURKEY', 'RC_UNITED_STATES']
- `23:29:12`   the cds figure of Turkey opens: Turkey 5-year CDS, measured | 260.4 bp · 20 Nov 2026 · higher than 56% of its readings since 2024 DTCC public price dissemination, 5-year senior CDS as measured by the site's CDS d | 4312 line pixels
- `23:29:13`   the yield figure of Japan opens: Japan 10-year government yield, monthly | 2.76% · Oct 2026 · higher than 78% of its readings since 1989 OECD Main Economic Indicators, long-term government bond yield, monthly average (FRED IR | 1973 line pixels
- `23:29:13` kept in the browser: {"tags":{"turkey":"red","japan":"blue","united states":"green"},"sort":{"sovereign":["ciss",1]},"show":"all","lines":"all"}
## 5c. The Dollar shortage tab

- `23:29:15`   read: The EUR/USD reference rate is 1.0990 on 20 Nov 2026: −0.0062 over a week, −0.0225 over a month, −0.0524 over a year; lower than 70% of its readings since 1999 (highest 1.5990 on 15 Jul 2008, lowest 0.8252 on 26 Oct 2000).
- `23:29:15`   read: Claims on euro-area residents in foreign currency — what the Eurosystem lends its banks in foreign currency — are €20.2 bn in the week to 13 Nov 2026 — lower than 67% of its readings since 1999; +0.4 bn over a week, −2.1 bn over a year. They have stood above €100 bn at 4 separate peaks: €248.9 bn on 5 Dec 2008, €153.8 bn on 8 May 2020, €142.1 bn on 10 Apr 2009, €100.6 bn on 10 Feb 2012.
- `23:29:15`   read: Claims on non-euro-area residents in foreign currency are €521.9 bn in the week to 13 Nov 2026 — higher than 96% of its readings since 1999; −1.6 bn over a week, +27.3 bn over a year. This is asset item 2 of the Eurosystem's weekly statement: its position at the IMF and its deposits, securities and loans abroad, that is its foreign reserves other than gold. It is not a gauge of dollars lent.
- `23:29:15`   read: Dollars lent by the Federal Reserve. The Federal Reserve's swap lines are drawn for $0.146 bn in the week to 18 Nov 2026 (−0.025 bn over a week, +0.115 bn over a year). They have stood above $100 bn at 4 separate peaks: $583.1 bn on 17 Dec 2008, $448.9 bn on 27 May 2020, $282.9 bn on 22 Apr 2009, $109.1 bn on 15 Feb 2012. Its repos with foreign central banks stand at $0.001 bn; their record is $60.0 bn on 22 Mar 2023.
- `23:29:15`   read: The price of the dollar. DXY, rebuilt from the Federal Reserve's H.10 rates, is 103.36 on 13 Nov 2026: +1.21 over a week, +1.14 over a month, +4.32 over a year; higher than 82% of its readings since 1999. The Fed's broad dollar index is 122.65 (higher than 93% of its readings since 2006), and its index against emerging markets 130.89 (higher than 90% of its readings since 2006).
- `23:29:15`   read: Foreign official dollars. Foreign central banks keep $2,544.0 bn of Treasuries in custody at the Federal Reserve in the week to 18 Nov 2026 (+19.8 bn over a week, −201.4 bn over a year). Over 13 weeks that is −42.2 bn; the largest 13-week fall on record is −$163.7 bn in the week to 12 Mar 2014. Their repo pool at the Fed is $316.7 bn (−19.4 bn over a week, −19.8 bn over a year).
- `23:29:15`   read: Banks' dollar funding. SOFR is 1.0 bp above the rate the Federal Reserve pays on reserves on 19 Nov 2026; the record is 315.0 bp on 17 Sep 2019. Foreign banks' US offices owe a net $885.6 bn to their offices abroad in the week to 11 Nov 2026; the lowest reading is −$615.4 bn on 28 May 2008. The TED spread ended on 21 Jan 2022; its record is 458.0 bp on 10 Oct 2008.
- `23:29:15`   read: Not here: a cross-currency basis, the market's own price of a dollar shortage. No free official source publishes one.
- `23:29:15` the three ECB measures, tiles: EUR/USD reference rate = $1.0990 20 Nov 2026 | Claims on euro-area residents in foreign currency = €20.2 bn 13 Nov 2026 | Claims on non-euro-area residents in foreign currency = €521.9 bn 13 Nov 2026
- `23:29:15` the three ECB measures, charts: EUR/USD reference rate 1.0990 20 Nov 2026 US dollars for one euro | Claims on euro-area residents in foreign currency €20.2 bn 13 Nov 2026 | Claims on non-euro-area residents in foreign currency €521.9 bn 13 Nov | from the top of the page: tiles 638 px, their charts 908 px, the table 1455 px, the Federal Reserve's charts 2439 px, the windows table 4480 px
- `23:29:15`   AUX_EURUSD       EUR/USD reference rate ECB · daily · US dollars for one euro | 1.0990 | 20 Nov 2026 | −0.0062 | −0.0225 | −0.0524 | a fall ▼ | lower than 70% of its readings | low 0.8252 Oct 2000
- `23:29:15`   AUX_FX_EA        Claims on euro-area residents in foreign currency ECB · weekly · what the Eurosystem lends euro-area banks in foreign currency | €20.2 bn | 13 Nov 2026 | +0.4 bn | −1.1 bn | −2.1 bn | a rise ▲ | 8.1% of its record | high €248.9 bn Dec 2008
- `23:29:15`   AUX_FX_NONEA     Claims on non-euro-area residents in foreign currency ECB · weekly · the Eurosystem's foreign reserves other than gold | €521.9 bn | 13 Nov 2026 | −1.6 bn | −2.6 bn | +27.3 bn | no side taken | higher than 96% of its readings | €131.2 bn to €553.0 bn
- `23:29:15`   FED_SWAPS        Fed swap lines drawn Federal Reserve · weekly | $0.146 bn | 18 Nov 2026 | −0.025 bn | −0.059 bn | +0.115 bn | a rise ▲ | under 0.1% of its record | high $583.1 bn Dec 2008
- `23:29:15`   FED_FIMA         Fed repos with foreign central banks (FIMA) Federal Reserve · weekly | $0.001 bn | 18 Nov 2026 | +0.001 bn | +0.001 bn | +0.001 bn | a rise ▲ | under 0.1% of its record | high $60.0 bn Mar 2023
- `23:29:15`   USD_DXY          DXY, rebuilt from the Fed's H.10 rates Federal Reserve · daily | 103.36 | 13 Nov 2026 | +1.21 | +1.14 | +4.32 | a rise ▲ | higher than 82% of its readings | high 120.86 Jul 2001
- `23:29:15`   USD_BROAD        Broad dollar index Federal Reserve · daily | 122.65 | 13 Nov 2026 | +0.77 | +1.21 | +1.88 | a rise ▲ | higher than 93% of its readings | high 130.04 Jan 2025
- `23:29:15`   USD_AFE          Dollar against advanced economies Federal Reserve · daily | 116.24 | 13 Nov 2026 | +1.37 | +1.14 | +3.98 | a rise ▲ | higher than 92% of its readings | high 124.85 Sep 2022
- `23:29:15`   USD_EME          Dollar against emerging markets Federal Reserve · daily | 130.89 | 13 Nov 2026 | +0.12 | +1.29 | −0.44 | a rise ▲ | higher than 90% of its readings | high 139.99 Apr 2025
- `23:29:15`   FED_CUSTODY_13W  Treasuries in custody: 13-week change Federal Reserve · weekly | −$42.2 bn | 18 Nov 2026 | +30.5 bn | +4.9 bn | +69.1 bn | a fall ▼ | lower than 83% of its readings | low −$163.7 bn Mar 2014
- `23:29:15`   FED_CUSTODY_UST  Treasuries in custody for foreign central banks Federal Reserve · weekly | $2,544.0 bn | 18 Nov 2026 | +19.8 bn | −6.9 bn | −201.4 bn | no side taken | lower than 82% of its readings | $1,171.8 bn to $3,135.5 bn
- `23:29:15`   FED_FOREIGN_RRP  Foreign repo pool at the Fed Federal Reserve · weekly | $316.7 bn | 18 Nov 2026 | −19.4 bn | −9.9 bn | −19.8 bn | no side taken | higher than 84% of its readings | $17.2 bn to $420.0 bn
- `23:29:15`   USD_SOFR_IOR     SOFR minus interest on reserves Federal Reserve · daily | 1.0 bp | 19 Nov 2026 | +1.0 bp | +4.0 bp | 0.0 bp | a rise ▲ | higher than 85% of its readings | high 315.0 bp Sep 2019
- `23:29:15`   USD_TED          TED spread (ended 2022) Federal Reserve · daily | 9.0 bp | 21 Jan 2022 ended | — | — | — | a rise ▲ | ended at its source | high 458.0 bp Oct 2008
- `23:29:15`   H8_NET_DUE       Foreign banks' US offices: net owed to offices abroad Federal Reserve · weekly | $885.6 bn | 11 Nov 2026 | +9.6 bn | +49.1 bn | +103.0 bn | no side taken | higher than 98% of its readings | −$615.4 bn to $1,007.6 bn
- `23:29:15` charts by part [['ecb', 3], ['backstop', 2], ['price', 4], ['official', 3], ['funding', 3]]: AUX_EURUSD 7141 points, 16619 line pixels | AUX_FX_EA 1455 points, 15170 line pixels | AUX_FX_NONEA 1455 points, 14962 line pixels | FED_SWAPS 987 points, 22783 line pixels | FED_FIMA 346 points, 11423 line pixels | USD_DXY 6988 points, 17081 line pixels | USD_BROAD 5250 points, 16836 line pixels | USD_AFE 5231 points, 17207 line pixels | USD_EME 5231 points, 16603 line pixels | FED_CUSTODY_13W 999 points, 19354 line pixels | FED_CUSTODY_UST 1012 points, 15770 line pixels | FED_FOREIGN_RRP 1249 points, 15217 line pixels | USD_SOFR_IOR 2156 points, 12244 line pixels | USD_TED 5656 points, 15000 line pixels | H8_NET_DUE 1454 points, 16269 line pixels
- `23:29:15` placed: {"AUX_EURUSD": "lower than 70% of its readings", "AUX_FX_EA": "8.1% of its record", "AUX_FX_NONEA": "higher than 96% of its readings", "FED_SWAPS": "under 0.1% of its record", "FED_FIMA": "under 0.1% of its record", "USD_DXY": "higher than 82% of its readings", "USD_BROAD": "higher than 93% of its readings", "USD_AFE": "higher than 92% of its readings", "USD_EME": "higher than 90% of its readings", "FED_CUSTODY_13W": "lower than 83% of its readings", "FED_CUSTODY_UST": "lower than 82% of its readings", "FED_FOREIGN_RRP": "higher than 84% of its readings", "USD_SOFR_IOR": "higher than 85% of its readings", "USD_TED": "ended at its source", "H8_NET_DUE": "higher than 98% of its readings"} | bars {"AUX_EURUSD": "h2", "AUX_FX_EA": "h0", "AUX_FX_NONEA": null, "FED_SWAPS": "h0", "FED_FIMA": "h0", "USD_DXY": "h2", "USD_BROAD": "h3", "USD_AFE": "h3", "USD_EME": "h3", "FED_CUSTODY_13W": "h2", "FED_CUSTODY_UST": null, "FED_FOREIGN_RRP": null, "USD_SOFR_IOR": "h3", "USD_TED": null, "H8_NET_DUE": null}
- `23:29:15` windows: 19 rows, 15 columns under ['THE THREE ECB MEASURES', 'DOLLARS LENT BY CENTRAL BANKS', 'THE PRICE OF THE DOLLAR', 'FOREIGN OFFICIAL DOLLARS AT THE FEDERAL RESERVE', "BANKS' DOLLAR FUNDING"] | Lehman row ['1.2460', '1.4731', '€85.5', '€248.9', '€131.2', '€163.0', '$583.1', '—', '89.15', '106.02', '103.32', '110.12', '$84.6', '—', '458.0'] | now ['1.0990', '€20.2', '€521.9', '$0.146', '$0.001', '103.36', '122.65', '116.24', '130.89', '−$42.2', '1.0', '9.0 ended'] | record ['0.8252', '1.5990', '€5.3', '€248.9', '€131.2', '€553.0', '$583.1', '$60.0', '120.86', '130.04', '124.85', '139.99', '−$163.7', '315.0', '458.0']
- `23:29:16` five years: [1281, 261, 261, 261, 261, 1250, 1250, 1250, 1250, 261, 261, 261, 1247, 1228, 261] points | under the pointer on the swap lines: $0.105 bn 17 Aug 2016
- `23:29:17` the swap lines' high in the COVID window opens: Federal Reserve central bank liquidity swaps outstanding | ranges ['1Y', '5Y', '10Y', 'All', 'COVID ± 1 year*'] | 1430 line pixels | links ['https://fred.stlouisfed.org/series/SWPT', '/data/ciss-dollar-shortage.json']
- `23:29:18` the tile of the non-euro-area claims opens: Claims on non-euro-area residents in foreign currency | History from 1 Jan 1999 · 1,455 observations · high €553.0 bn on 4 Nov 2022 · low €131.2 bn on 19 Sep 2008 · m | 2208 line pixels
- `23:29:19` the TED row opens: TED spread: three-month dollar LIBOR minus the three-month Treasury bill | 9.0 bp · 21 Jan 2022 · this series ended at its source; this is its last reading · lower than 98% of its readings since 1999 Federal Reserve Bank of S | 6766 line pixels
- `23:29:21` the name over the custody chart opens: US Treasury securities held in custody at the Federal Reserve for foreign official and international accounts: change over 13 weeks | History from 3 Oct 2007 · 999 observations · high $271.7 bn on 3 Nov 2010 · low −$163.7 bn on 12 Mar 2014 · mean $17.9 bn LOWEST READING INSIDE EACH NAMED WINDOW (SHADED ON THE CHART) · CLICK ONE TO L
- `23:29:22` the EUR/USD row opens: EUR/USD reference rate | History from 4 Jan 1999 · 7,141 observations · high $1.5990 on 15 Jul 2008 · low $0.8252 on 26 Oct 2000 · mean $1.1814 LOWEST READING INSIDE EACH NAME
- `23:29:22` sources: 11 rows, 19 links, says every response arrived: True | Every ECB number on this desk is read from an ECB response that is kept as it arrived. The run receipt lists them; compiling twice from two separate reads has to give the same document before it is published. ECB RESPONSE SERIES ROWS NEWEST PERIOD RECEIVED ORIGINAL CISS, SovCISS and CLIFS (the warehouse) 67 2026-11-22 07:13 warehouse receipt · CISS register Euro-area government yield curve, AAA issuers and all issuer
- `23:29:23` the retained yield-curve response: 12767481 bytes, SHA-256 as its receipt says: True | run receipt lists 67 originals and 10 added responses
## 6. The desk alone, on a phone

- `23:29:24` deep link ?m=SOV_FR: France SovCISS | 0.1739 · 19 Nov 2026 · higher than 75% of its readings since 2000 CISS.D.FR.Z0Z.4F.EC.SOV_CIN.IDX · source value 0.17391
- `23:29:25` Portugal tagged, page loaded again: portugal france belgium greece
- `23:29:32` the link the Dollar desk's cards open (?view=dollar&m=AUX_FX_EA): tab Dollar shortage, chart Claims on euro-area residents in foreign currency
- `23:29:33` a Federal Reserve card's link (?view=dollar&m=FED_SWAPS): tab Dollar shortage, chart Federal Reserve central bank liquidity swaps outstanding, 8124 line pixels
- `23:29:33` wider than a 390 px screen by: {"desk": 0, "crisis": 0, "world": 0, "dollar": 0, "sources": 0} | page errors []
## 7. The CISS page

- `23:29:36` headline: ECB euro-area CISS: 0.0095 on 2026-11-19 — lower than 86% of its readings since 1980.
- `23:29:36`   Exact source value 0.009462216765937262 · highest reading on record 0.9409 · scale 0 to 1
- `23:29:36` reconciliation: Matched on 2026-11-19 — the six contributions add up to the composite. | difference 1.25E-17 (tolerance 0.0000000001)
- `23:29:36` count: 88 shown · 58 current · 2 with a blank newest row · 28 ended at the ECB · 0 collection errors | warehouse by this page's rule: {"fresh": 58, "blank": 2, "ended": 28} | values of the first rows: ['Austria NEW SovCISS - Composite Indicator of Sovereign Stress Austria CISS.D.AT.Z0Z.4F.EC.SOV_CIN.IDX', '0.0052 dimensionless_index', '0.0547 dimensionless_index'] | cells with seven or more decimals: 0
- `23:29:36`   aux: EUR/USD reference rate 1.099 USD per EUR · 2026-11-20 Read from the ECB response received 2026-11-22 and kept as it arrived; source value 1.099 for 2026-11-20. EXR.D.USD.EUR.SP00.A Original ECB response (.gz) Run receipt Check it against the original
- `23:29:36`   aux: Claims on euro-area residents in foreign currency 20.193 EUR bn · 2026-11-13 Read from the ECB response received 2026-11-22 and kept as it arrived; source value 20193 for 2026-W46. ILM.W.U2.C.A030000.U2.Z06 Original ECB response (.gz) Run receipt Che
- `23:29:36`   aux: Claims on non-euro-area residents in foreign currency 521.892 EUR bn · 2026-11-13 Read from the ECB response received 2026-11-22 and kept as it arrived; source value 521892 for 2026-W46. ILM.W.U2.C.A020000.U4.Z06 Original ECB response (.gz) Run recei
- `23:29:36` check the euro reference rate against its original, in the browser: VERIFIED: same bytes, same row, same value { "sha256": "84b666b398336f6dacbf82bf61f789435e2e92cebfe16463eae7f1c5b2cf3f9f", "source_row": 7202, "original": { "KEY": "EXR.D.USD.EUR.SP00.A", "TIME_PERIOD": "2026-11-20", "OBS_VALUE": "1.099", "OBS_STATUS": "A", "UNIT": "USD", "UNIT_MULT": "0", "TITLE": "US dollar/Euro ECB reference 
- `23:29:38` filters: {"ended": 28, "blank": 2, "fresh": 58} | blank rows: Germany NEW CISS - Composite Indicator of Systemic Stress. CISS.D.DE.Z0Z.4F.EC.SS_CIN.IDX 0.0080 last value, 2026-11-19 · the 2026-11-20 row is blank 2026-11-20 D · status M newest row blank at the ECB 2 days old at collection — i || Spain NEW CISS - Composite Indicator of Systemic Stress. CISS.D.ES.Z0Z.4F.EC.SS_CIN.IDX 0.0050 last value, 2026-11-19 · the 2026-11-20 row is blank 2026-11-20 D · status M newest row blank at the ECB 2 days old at collection — ind
- `23:29:40` a series panel still opens and verifies its row: {"open": true, "text": "VERIFIED original bytes and selected row\n{\n  \"sha256\": \"9ad1", "exact": true}
## 8. The fiscal page that stopped on a null

- `23:29:52` sovereign.html: 8 summary figures, 33 charts, 14792 characters, still loading: False, errors []
- `23:29:52`   FOREIGN UST HOLDINGS $9,270B +649B YoY | CHINA $684B -75B YoY | JAPAN (LARGEST) $1,186B +124B YoY | DEFICIT (TTM) $2,112B 12-mo rolling | TOTAL DEBT $40.7T to the penny | INTEREST EXPENSE $1,457B avg 3.58% | FED SEC-LENDING $37.9B 34%ile take-up | TREASURY FAILS $159B UNQUALIFIED · percentile not available
- `23:29:52`   what stopped it: the fails document's headline carries {"combined_bn": 158.647, "pctile": null}
## 9. The Dollar desk carries the lines of the same document

- `23:29:58` 💵 Dollar indexes | 9 families, 103 cards (catalog 103) | the family: DOLLAR SHORTAGE · CENTRAL-BANK LENDING, OFFICIAL DOLLARS AND THE EURO SIDE
- `23:29:58`   AUX_EURUSD    EUR/USD reference rate · ECB, daily | 1.099 as of 2026-11-20 | W -0.56% M -2.01% Q -5.92% Y -4.55% (own arithmetic -0.56 -2.01 -5.92 -4.55) | strip 1, bears 4
- `23:29:58`   AUX_FX_EA     Claims on euro-area residents in foreign currency ·  | 20.19 as of 2026-11-13 | W +2.08% M -5.23% Q -1.80% Y -9.43% (own arithmetic +2.08 -5.23 -1.80 -9.43) | strip 1, bears 4
- `23:29:58`   AUX_FX_NONEA  Claims on non-euro-area residents in foreign currenc | 521.89 as of 2026-11-13 | W -0.31% M -0.49% Q -0.63% Y +5.52% (own arithmetic -0.31 -0.49 -0.63 +5.52) | strip 1, bears 4
- `23:29:58`   FED_SWAPS     Fed swap lines drawn by foreign central banks · week | 0.146 as of 2026-11-18 | W -14.62% M -28.78% Q +10.61% Y +371% (own arithmetic -14.62 -28.78 +10.61 +370.97) | strip 1, bears 4
- `23:29:58`   FED_FIMA      Fed repos with foreign central banks (FIMA) · weekly | 0.001 as of 2026-11-18 | W — M — Q — Y — (own arithmetic - - - -) | strip 1, bears 4
- `23:29:58`   FED_CUSTODY_UST Treasuries in custody at the Fed for foreign central | 2,544 as of 2026-11-18 | W +0.79% M -0.27% Q -2.03% Y -7.34% (own arithmetic +0.79 -0.27 -2.03 -7.34) | strip 1, bears 4
- `23:29:58`   FED_FOREIGN_RRP Foreign repo pool at the Fed · weekly | 316.71 as of 2026-11-18 | W -5.77% M -3.03% Q -11.38% Y -5.87% (own arithmetic -5.77 -3.03 -11.38 -5.87) | strip 1, bears 4
- `23:29:58`   USD_TED       TED spread · daily, ended January 2022 | 9 as of 2022-01-21 | W -18.18% M -40.00% Q +50.00% Y -30.77% (own arithmetic -18.18 -40.00 +50.00 -30.77) | strip 1, bears 4
- `23:29:59` full screen: Fed swap lines drawn by foreign central banks · weekly | 0.146 USD bn as of 2026-11-18 | its chart button opens ['/sovereign-desk.html?view=dollar&m=FED_SWAPS'] | page errors []
- `23:29:59` favicon.ico: HTTP Error 404: Not Found (the one console 404 the first page of a browser session reports)
- `23:29:59` ✅ the Sovereign button opens the desk; tiles, boards, tags, sorting, charts, crisis histories, the World chart (all and only tagged), the Dollar shortage tab and sources hold against the documents; the Dollar desk carries the lines of the same document; the CISS page reads plainly and checks its auxiliaries against their originals
