# ops 6915 - the Sovereign button, its desk and the CISS page, real browser on the live site (read-only), 2026-11-22T19:50+00:00

**Status:** success  
**Duration:** 90.2s  
**Finished:** 2026-11-22T19:52:02+00:00  

## Data

| board_rows | ciss_rows | ciss_table_rows | dollar_observations | ended_rows | failed_checks | measures | tiles | world_rows | world_series |
|---|---|---|---|---|---|---|---|---|---|
| 11 | 88 | 13 | 10051 | 6 | 0 | 266 | 14 | 44 | 137 |

## Log
## 0. The live site serves the new files

- `19:50:33` served after 1 tries: {"engine": true, "rail": true, "desk_page": true, "desk_script": true, "dollar_desk": true, "ciss_page": true, "ciss_script": true, "fiscal_page": true}
- `19:50:33` scripts as the pages name them: /jh-sovereign-desk.js?v=5513d906 | /jh-ciss-research.js?v=62f3959f
- `19:50:34` documents: desk 2026-11-22T16:39:59, 266 measures, 30 countries, errors {} | history 266 series | warehouse 2026-11-22T07:13:48
- `19:50:34` installing playwright
## 1. The button on the chart

- `19:50:53` button: {"btn": true, "prev": "btn-repo", "next": "btn-usd", "label": "SovSovereign", "title": "Sovereign stress \u00b7 SovCISS, spreads to Germany, TARGET, crisis peaks", "box": [560, -262, 72, 44], "repoPrev": "btn-cds", "overlay": "", "src": null, "head": null}
- `19:50:55` clicked by mouse at box [560, 52, 72, 44] -> overlay on | Sovereign stress · SovCISS, spreads, TARGET, crisis peaks | /sovereign-desk.html
## 2. The Desk, inside the chart

- `19:50:57` as of: ECB data to 19 Nov 2026 · compiled 2026-11-22 16:39 UTC · 266 measures, 30 countries
- `19:50:57`   read: Euro-area sovereign stress (SovCISS, GDP-weighted) reads 0.0112 on 19 Nov 2026 — lower than 76% of its readings since 2000; record 0.7514, 29 Nov 2011.
- `19:50:57`   read: Highest: France 0.1739, Belgium 0.1613, Greece 0.1007. Lowest: Portugal 0.0038. Highest minus lowest: 0.1702 (record 0.9133, 6 Jul 2015).
- `19:50:57`   read: All euro-area governments together pay 59.8 bp more than the AAA-rated ones at 10 years (19 Nov 2026; record 194.8 bp, 23 Jul 2012).
- `19:50:57`   read: Widest 10-year spreads to Germany in the euro area (Oct 2026): France 147.4 bp, Italy 126.4 bp, Greece 99.4 bp, Malta 97.4 bp.
- `19:50:57`   read: Over the past week: largest rise Greece +0.0171; largest fall Belgium −0.1387.
- `19:50:57`   read: Systemic stress outside the euro area, by the ECB's CISS: United States 0.0145, China 0.0170 (13 Nov 2026), United Kingdom 0.0200; euro area 0.0095.
- `19:50:57` tiles: EA_SOV_GDP 0.0112 19 Nov 2026 (19 Nov 2026) | EA_SOV_EQ 0.0069 19 Nov 2026 (19 Nov 2026) | EA_SOV_RANGE 0.1702 19 Nov 2026 (19 Nov 2026) | EA_CURVE_10Y 59.8 bp 19 Nov 2026 (19 Nov 2026) | EA_CURVE_2Y 16.8 bp 19 Nov 2026 (19 Nov 2026) | EA_ALL_10Y 4.04% 19 Nov 2026 (19 Nov 2026) | EA_AAA_10Y 3.44% 19 Nov 2026 (19 Nov 2026) | Y10_U2 4.08% Oct 2026 (Oct 2026) | EA_TGT_DEBT €1,307.5 bn Sep 2026 (Sep 2026) | EA_CISS 0.0095 19 Nov 2026 (19 Nov 2026) | EA_CON_BMN 0.0336 19 Nov 2026 (19 Nov 2026)
- `19:50:57` outside the euro area: CISS_US 0.0145 19 Nov 2026 | CISS_CN 0.0170 13 Nov 2026 | CISS_GB 0.0200 19 Nov 2026
- `19:50:57` board: France 0.1739 | Belgium 0.1613 | Greece 0.1007 | Italy 0.0474 | Austria 0.0422 | Germany 0.0303 | Ireland 0.0242 | Netherlands 0.0173 | Finland 0.0060 | Spain 0.0057 | Portugal 0.0038
- `19:50:57` France's row: France | 0.1739 | +0.1437 | −0.0286 | −0.0812 | +0.0956 | 76 | 0.7635 Jul 2012 | 0.0895 | 4.89% | 147.4 bp | 90.2 bp | A+ | −€183.5 bn | 119.1% | −5.1% | 2.4% | 0.1002 | 
- `19:50:57` SovCISS the ECB stopped: Denmark 0.3226 | Poland 0.1793 | Sweden 0.1234 | United Kingdom 0.0895 | Hungary 0.0850 | Czechia 0.0172
- `19:50:57` CISS by country: France 0.0895 (19 Nov 2026) | Belgium 0.0592 (19 Nov 2026) | United Kingdom 0.0200 (19 Nov 2026) | China 0.0170 (13 Nov 2026) | United States 0.0145 (19 Nov 2026) | Italy 0.0095 (19 Nov 2026) | Finland 0.0087 (19 Nov 2026) | Germany 0.0080 (19 Nov 2026 the ECB's 20 Nov row is blank) | Ireland 0.0073 (19 Nov 2026) | Portugal 0.0053 (19 Nov 2026) | Austria 0.0052 (19 Nov 2026) | Spain 0.0050 (19 Nov 2026 the ECB's 20 Nov row is blank) | Netherlands 0.0049 (19 Nov 2026)
## 2b. Tags and sorting

- `19:50:58` before: france belgium greece italy austria germany ireland netherlands finland spain portugal
- `19:50:58` Italy tagged red, Greece amber: greece italy france belgium austria germany ireland netherlands finland spain portugal | CISS table starts ['italy', 'france']
- `19:50:58` sorted by CISS, high to low: greece italy france belgium finland germany ireland portugal austria spain netherlands
- `19:50:58` sorted by CISS, low to high: greece italy netherlands spain austria portugal ireland germany finland belgium france
- `19:50:58` kept in the browser: {"tags":{"italy":"red","greece":"amber"},"sort":{"sovereign":["ciss",1]},"show":"all","lines":"all"}
- `19:50:59` clicking the country name France opens: France SovCISS
- `19:51:00` clicking United States opens: United States CISS | 0.0145 · 19 Nov 2026 · lower than 74% of its readings since 1980 CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX · source value
## 3. A chart

- `19:51:00` chart: Euro area SovCISS, GDP-weighted | 0.0112 · 19 Nov 2026 · lower than 76% of its readings since 2000 CISS.D.U2.Z0Z.4F.EC.SOV_GDPWN.IDX · source value 0.011230409327541846 the series at the ECB · the respons | canvas 1066x385, 7923 line pixels
- `19:51:00`   History from 5 Jan 2000 · 7,012 observations · high 0.7514 on 29 Nov 2011 · low 0.0004 on 6 Nov 2025 · mean 0.1030 · the line is the last reading of each week; highs and lows are from every day HIGHEST READING INSIDE EACH NAMED WINDOW (SHADED ON THE CHART) · C
- `19:51:01` five years: 2382 line pixels (all history 7923) | under the pointer: block 0.0301 18 Oct 2024
## 4. Crisis peaks: every set with its history

- `19:51:03`   sovciss  18 series, 19 windows | one chart of all 18 over their whole history (61560 line pixels), legend 18 | tagged first: ['EA_SOV_GDP', 'SOV_GR', 'SOV_IT']
- `19:51:03`            SOV_IT in 'italy2011': page 0.9903, document 0.9903 | now 0.0474 | record 0.9903 | overflow 0 px
- `19:51:05`            a cell opens Italy SovCISS | ranges ['1Y', '5Y', '10Y', 'All', 'Italy and Spain drawn in ± 1 year*'] | 1934 line pixels
- `19:51:07`   spread   15 series, 21 windows | one chart of all 15 over their whole history (10991 line pixels), legend 15 | tagged first: ['EA_CURVE_10Y', 'S10_GR', 'S10_IT']
- `19:51:07`            S10_GR in 'spain2012': page 2,652.0, document 2,652.0 | now 99.4 | record 2,739.0 | overflow 0 px
- `19:51:08`   ciss     14 series, 22 windows | one chart of all 14 over their whole history (63912 line pixels), legend 14 | tagged first: ['EA_CISS', 'CISS_IT', 'CISS_US']
- `19:51:08`            CISS_US in 'crash1987': page 0.7194, document 0.7194 | now 0.0145 | record 0.8967 | overflow 0 px
- `19:51:10`   euro     10 series in different units, 22 windows | 10 small history charts, 10 drawn
- `19:51:10`            TGT_IT in 'gilts2022': page −€714.9, document −€714.9 | now −€324.2 | record −€714.9 | overflow 0 px
- `19:51:10` tags cleared: netherlands spain austria portugal
## 5. World and Sources

- `19:51:11` world: 44 rows | first: Turkey | Europe | 33.55% | −264.0 bp | 244.5 bp | BB- | 36.00% | 22 November 2026
## 5b. The World tab's chart: all countries, or only the tagged ones

- `19:51:12` sets on the page: 10-year yields 30* | 5-year CDS, measured 25 | 10-year yields, as reported 44 | CDS, as reported 38
- `19:51:13`   yield           30 of 44 countries drawn (14250 line pixels), legend 30, mode all | sources {'oecd': 25, 'ecb': 5} | from 1953-04-30 | overflow 0 px
- `19:51:13`   cds             25 of 44 countries drawn (19043 line pixels), legend 25, mode all | sources {'dtcc': 25} | from 2024-09-03 | overflow 0 px
- `19:51:14`   yield_reported  44 of 44 countries drawn (38857 line pixels), legend 44, mode all | sources {'reported': 44} | from 2026-09-26 | overflow 0 px
- `19:51:15`   cds_reported    38 of 44 countries drawn (37774 line pixels), legend 38, mode all | sources {'reported': 38} | from 2026-09-26 | overflow 0 px
- `19:51:17` Turkey and Japan tagged, 'Only tagged' on the long yields: 1 line(s) ['WY_JAPAN'] (Turkey has no monthly yield: True)
- `19:51:18` the United States tagged while 'Only tagged' is on: 2 line(s) ['WY_JAPAN', 'WY_UNITED_STATES']
- `19:51:19` under the pointer: 29 Feb 2004 United States 4.08% Japan 1.21%
- `19:51:19`   'Only tagged' carried to cds             3 line(s) ['WC_JAPAN', 'WC_TURKEY', 'WC_UNITED_STATES']
- `19:51:20`   'Only tagged' carried to yield_reported  3 line(s) ['RY_JAPAN', 'RY_TURKEY', 'RY_UNITED_STATES']
- `19:51:21`   'Only tagged' carried to cds_reported    3 line(s) ['RC_JAPAN', 'RC_TURKEY', 'RC_UNITED_STATES']
- `19:51:23`   the cds figure of Turkey opens: Turkey 5-year CDS, measured | 260.4 bp · 20 Nov 2026 · higher than 56% of its readings since 2024 DTCC public price dissemination, 5-year senior CDS as measured by the site's CDS d | 4312 line pixels
- `19:51:24`   the yield figure of Japan opens: Japan 10-year government yield, monthly | 2.76% · Oct 2026 · higher than 78% of its readings since 1989 OECD Main Economic Indicators, long-term government bond yield, monthly average (FRED IR | 1973 line pixels
- `19:51:25` kept in the browser: {"tags":{"turkey":"red","japan":"blue","united states":"green"},"sort":{"sovereign":["ciss",1]},"show":"all","lines":"all"}
## 5c. The Dollar shortage tab

- `19:51:26`   read: Foreign-currency lending to euro-area banks is €20.2 bn in the week to 13 Nov 2026 — lower than 67% of its readings since 1999; +0.4 bn over a week, −2.1 bn over a year. It has stood above €100 bn at 4 separate peaks: €248.9 bn on 5 Dec 2008, €153.8 bn on 8 May 2020, €142.1 bn on 10 Apr 2009, €100.6 bn on 10 Feb 2012.
- `19:51:26`   read: EUR/USD, the ECB's reference rate, is 1.0990 on 20 Nov 2026: −0.0062 over a week, −0.0225 over a month, −0.0524 over a year; lower than 70% of its readings since 1999 (highest 1.5990 on 15 Jul 2008, lowest 0.8252 on 26 Oct 2000).
- `19:51:26`   read: The Eurosystem's foreign-currency claims abroad are €521.9 bn in the week to 13 Nov 2026 — higher than 96% of its readings since 1999; −1.6 bn over a week, +27.3 bn over a year.
- `19:51:26` tiles: AUX_EURUSD $1.0990 20 Nov 2026 | AUX_FX_EA €20.2 bn 13 Nov 2026 | AUX_FX_NONEA €521.9 bn 13 Nov 2026
- `19:51:26` charts: AUX_EURUSD 7141 points, 20080 line pixels | AUX_FX_EA 1455 points, 18554 line pixels | AUX_FX_NONEA 1455 points, 18376 line pixels
- `19:51:26` windows: 19 rows | Lehman row ['1.2460', '1.4731', '€85.5', '€248.9'] | now ['1.0990 20 Nov 2026', '€20.2 13 Nov 2026', '€521.9 13 Nov 2026'] | record ['0.8252', '1.5990', '€5.3', '€248.9', '€131.2', '€553.0'] | marked columns ['EUR/USD low', 'LENDING TO BANKS high']
- `19:51:27` five years: [1281, 261, 261] points | under the pointer on lending: €24.8 bn 10 Dec 2010 Ireland rescue
- `19:51:28` lending's high in the COVID window opens: Claims on euro-area residents in foreign currency | ranges ['1Y', '5Y', '10Y', 'All', 'COVID ± 1 year*'] | 1395 line pixels
- `19:51:29` the EUR/USD tile opens: EUR/USD reference rate | History from 4 Jan 1999 · 7,141 observations · high $1.5990 on 15 Jul 2008 · low $0.8252 on 26 Oct 2000 · mean $1.1814 HIGHEST READING INSIDE EACH NAM
- `19:51:30` sources: 11 rows, 19 links, says every response arrived: True | Every ECB number on this desk is read from an ECB response that is kept as it arrived. The run receipt lists them; compiling twice from two separate reads has to give the same document before it is published. ECB RESPONSE SERIES ROWS NEWEST PERIOD RECEIVED ORIGINAL CISS, SovCISS and CLIFS (the warehouse) 67 2026-11-22 07:13 warehouse receipt · CISS register Euro-area government yield curve, AAA issuers and all issuer
- `19:51:30` the retained yield-curve response: 12767481 bytes, SHA-256 as its receipt says: True | run receipt lists 67 originals and 10 added responses
## 6. The desk alone, on a phone

- `19:51:31` deep link ?m=SOV_FR: France SovCISS | 0.1739 · 19 Nov 2026 · higher than 75% of its readings since 2000 CISS.D.FR.Z0Z.4F.EC.SOV_CIN.IDX · source value 0.17391
- `19:51:32` Portugal tagged, page loaded again: portugal france belgium greece
- `19:51:39` the link the Dollar desk's cards open (?view=dollar&m=AUX_FX_EA): tab Dollar shortage, chart Claims on euro-area residents in foreign currency
- `19:51:39` wider than a 390 px screen by: {"desk": 0, "crisis": 0, "world": 0, "dollar": 0, "sources": 0} | page errors []
## 7. The CISS page

- `19:51:41` headline: ECB euro-area CISS: 0.0095 on 2026-11-19 — lower than 86% of its readings since 1980.
- `19:51:41`   Exact source value 0.009462216765937262 · highest reading on record 0.9409 · scale 0 to 1
- `19:51:41` reconciliation: Matched on 2026-11-19 — the six contributions add up to the composite. | difference 1.25E-17 (tolerance 0.0000000001)
- `19:51:41` count: 88 shown · 58 current · 2 with a blank newest row · 28 ended at the ECB · 0 collection errors | warehouse by this page's rule: {"fresh": 58, "blank": 2, "ended": 28} | values of the first rows: ['Austria NEW SovCISS - Composite Indicator of Sovereign Stress Austria CISS.D.AT.Z0Z.4F.EC.SOV_CIN.IDX', '0.0052 dimensionless_index', '0.0547 dimensionless_index'] | cells with seven or more decimals: 0
- `19:51:41`   aux: EUR/USD reference rate 1.099 USD per EUR · 2026-11-20 Read from the ECB response received 2026-11-22 and kept as it arrived; source value 1.099 for 2026-11-20. EXR.D.USD.EUR.SP00.A Original ECB response (.gz) Run receipt Check it against the original
- `19:51:41`   aux: Claims on euro-area residents in foreign currency 20.193 EUR bn · 2026-11-13 Read from the ECB response received 2026-11-22 and kept as it arrived; source value 20193 for 2026-W46. ILM.W.U2.C.A030000.U2.Z06 Original ECB response (.gz) Run receipt Che
- `19:51:41`   aux: Claims on non-euro-area residents in foreign currency 521.892 EUR bn · 2026-11-13 Read from the ECB response received 2026-11-22 and kept as it arrived; source value 521892 for 2026-W46. ILM.W.U2.C.A020000.U4.Z06 Original ECB response (.gz) Run recei
- `19:51:42` check the euro reference rate against its original, in the browser: VERIFIED: same bytes, same row, same value { "sha256": "84b666b398336f6dacbf82bf61f789435e2e92cebfe16463eae7f1c5b2cf3f9f", "source_row": 7202, "original": { "KEY": "EXR.D.USD.EUR.SP00.A", "TIME_PERIOD": "2026-11-20", "OBS_VALUE": "1.099", "OBS_STATUS": "A", "UNIT": "USD", "UNIT_MULT": "0", "TITLE": "US dollar/Euro ECB reference 
- `19:51:43` filters: {"ended": 28, "blank": 2, "fresh": 58} | blank rows: Germany NEW CISS - Composite Indicator of Systemic Stress. CISS.D.DE.Z0Z.4F.EC.SS_CIN.IDX 0.0080 last value, 2026-11-19 · the 2026-11-20 row is blank 2026-11-20 D · status M newest row blank at the ECB 2 days old at collection — i || Spain NEW CISS - Composite Indicator of Systemic Stress. CISS.D.ES.Z0Z.4F.EC.SS_CIN.IDX 0.0050 last value, 2026-11-19 · the 2026-11-20 row is blank 2026-11-20 D · status M newest row blank at the ECB 2 days old at collection — ind
- `19:51:45` a series panel still opens and verifies its row: {"open": true, "text": "VERIFIED original bytes and selected row\n{\n  \"sha256\": \"9ad1", "exact": true}
## 8. The fiscal page that stopped on a null

- `19:51:57` sovereign.html: 8 summary figures, 33 charts, 14792 characters, still loading: False, errors []
- `19:51:57`   FOREIGN UST HOLDINGS $9,270B +649B YoY | CHINA $684B -75B YoY | JAPAN (LARGEST) $1,186B +124B YoY | DEFICIT (TTM) $2,112B 12-mo rolling | TOTAL DEBT $40.7T to the penny | INTEREST EXPENSE $1,457B avg 3.58% | FED SEC-LENDING $37.9B 34%ile take-up | TREASURY FAILS $159B UNQUALIFIED · percentile not available
- `19:51:57`   what stopped it: the fails document's headline carries {"combined_bn": 158.647, "pctile": null}
## 9. The Dollar desk carries the same three

- `19:52:01` 💵 Dollar indexes | 9 families, 98 cards (catalog 98) | the family: DOLLAR SHORTAGE · THE EURO SIDE, FROM THE ECB
- `19:52:01`   AUX_EURUSD    EUR/USD · ECB reference rate, daily | 1.099 as of 2026-11-20 | W -0.56% M -2.01% Q -5.92% Y -4.55% (own arithmetic -0.56 -2.01 -5.92 -4.55) | strip 1, bears 4
- `19:52:01`   AUX_FX_EA     Eurosystem foreign-currency lending to euro-area ban | 20.19 as of 2026-11-13 | W +2.08% M -5.23% Q -1.80% Y -9.43% (own arithmetic +2.08 -5.23 -1.80 -9.43) | strip 1, bears 4
- `19:52:01`   AUX_FX_NONEA  Eurosystem foreign-currency claims abroad · weekly | 521.89 as of 2026-11-13 | W -0.31% M -0.49% Q -0.63% Y +5.52% (own arithmetic -0.31 -0.49 -0.63 +5.52) | strip 1, bears 4
- `19:52:02` full screen: Eurosystem foreign-currency lending to euro-area banks · weekly | 20.19 EUR bn as of 2026-11-13 | its chart button opens ['/sovereign-desk.html?view=dollar&m=AUX_FX_EA'] | page errors []
- `19:52:02` favicon.ico: HTTP Error 404: Not Found (the one console 404 the first page of a browser session reports)
- `19:52:02` ✅ the Sovereign button opens the desk; tiles, boards, tags, sorting, charts, crisis histories, the World chart (all and only tagged), the Dollar shortage tab and sources hold against the documents; the Dollar desk carries the same three; the CISS page reads plainly and checks its auxiliaries against their originals
