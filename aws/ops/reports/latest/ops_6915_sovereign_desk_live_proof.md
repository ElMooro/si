# ops 6915 - the Sovereign button, its desk and the CISS page, real browser on the live site (read-only), 2026-11-22T15:55+00:00

**Status:** success  
**Duration:** 407.8s  
**Finished:** 2026-11-22T16:02:20+00:00  

## Data

| board_rows | ciss_rows | failed_checks | measures | tiles | world_rows |
|---|---|---|---|---|---|
| 11 | 88 | 0 | 176 | 11 | 44 |

## Log
## 0. The live site serves the new files

- `16:01:22` served after 18 tries: {"engine": true, "rail": true, "desk_page": true, "desk_script": true, "ciss_page": true, "ciss_script": true, "fiscal_page": true}
- `16:01:22` scripts as the pages name them: /jh-sovereign-desk.js?v=04150987 | /jh-ciss-research.js?v=62f3959f
- `16:01:25` documents: desk 2026-11-22T15:34:05, 176 measures, 30 countries, errors {} | history 176 series | warehouse 2026-11-22T07:13:48
- `16:01:25` installing playwright
## 1. The button on the chart

- `16:01:46` button: {"btn": true, "prev": "btn-repo", "next": "btn-usd", "label": "SovSovereign", "title": "Sovereign stress \u00b7 SovCISS, spreads to Germany, TARGET, crisis peaks", "box": [560, -187, 72, 44], "repoPrev": "btn-cds", "overlay": "", "src": null, "head": null}
- `16:01:49` clicked by mouse at box [560, 52, 72, 44] -> overlay on | Sovereign stress · SovCISS, spreads, TARGET, crisis peaks | /sovereign-desk.html
## 2. The Desk, inside the chart

- `16:01:50` as of: ECB data to 19 Nov 2026 · compiled 2026-11-22 15:34 UTC · 176 measures, 30 countries
- `16:01:50`   read: Euro-area sovereign stress (SovCISS, GDP-weighted) reads 0.0112 on 19 Nov 2026 — lower than 76% of its readings since 2000; record 0.7514, 29 Nov 2011.
- `16:01:50`   read: Highest: France 0.1739, Belgium 0.1613, Greece 0.1007. Lowest: Portugal 0.0038. Highest minus lowest: 0.1702 (record 0.9133, 6 Jul 2015).
- `16:01:50`   read: All euro-area governments together pay 59.8 bp more than the AAA-rated ones at 10 years (19 Nov 2026; record 194.8 bp, 23 Jul 2012).
- `16:01:50`   read: Widest 10-year spreads to Germany in the euro area (Oct 2026): France 147.4 bp, Italy 126.4 bp, Greece 99.4 bp, Malta 97.4 bp.
- `16:01:50` tiles: EA_SOV_GDP 0.0112 19 Nov 2026 (19 Nov 2026) | EA_SOV_EQ 0.0069 19 Nov 2026 (19 Nov 2026) | EA_SOV_RANGE 0.1702 19 Nov 2026 (19 Nov 2026) | EA_CURVE_10Y 59.8 bp 19 Nov 2026 (19 Nov 2026) | EA_CURVE_2Y 16.8 bp 19 Nov 2026 (19 Nov 2026) | EA_ALL_10Y 4.04% 19 Nov 2026 (19 Nov 2026) | EA_AAA_10Y 3.44% 19 Nov 2026 (19 Nov 2026) | Y10_U2 4.08% Oct 2026 (Oct 2026) | EA_TGT_DEBT €1,307.5 bn Sep 2026 (Sep 2026) | EA_CISS 0.0095 19 Nov 2026 (19 Nov 2026) | EA_CON_BMN 0.0336 19 Nov 2026 (19 Nov 2026)
- `16:01:50` board: France 0.1739 | Belgium 0.1613 | Greece 0.1007 | Italy 0.0474 | Austria 0.0422 | Germany 0.0303 | Ireland 0.0242 | Netherlands 0.0173 | Finland 0.0060 | Spain 0.0057 | Portugal 0.0038
- `16:01:50` France's row: France | 0.1739 | +0.1437 | −0.0286 | −0.0812 | +0.0956 | 76 | 0.7635 Jul 2012 | 4.89% | 147.4 bp | −€183.5 bn | 119.1% | 0.1002 | 
## 3. A chart

- `16:01:50` chart: Euro area SovCISS, GDP-weighted | 0.0112 · 19 Nov 2026 · lower than 76% of its readings since 2000 CISS.D.U2.Z0Z.4F.EC.SOV_GDPWN.IDX · source value 0.011230409327541846 the series at the ECB · the respons | canvas 1066x385, 7913 line pixels
- `16:01:50`   History from 5 Jan 2000 · 7,012 observations · high 0.7514 on 29 Nov 2011 · low 0.0004 on 6 Nov 2025 · mean 0.1030 · the line is the last reading of each week; highs and lows are from every day HIGHEST READING INSIDE EACH NAMED WINDOW (SHADED ON THE CHART) Leh
- `16:01:51` five years: 2382 line pixels (all history 7913) | under the pointer: block 0.0301 18 Oct 2024
## 4. Crisis peaks

- `16:01:52`   sovciss  12 columns, 18 rows | SOV_IT in 'Italy and Spain drawn in': page 0.9903, document 0.9903 | now 0.0474 | overflow 0 px
- `16:01:52`   highest readings by the data: 0.7514 29 Nov 2011 · Italy and Spain drawn in | 0.6987 11 Jul 2012 · Spain's banks, "whatever it takes" | 0.5547 1 Aug 2011 · Italy and Spain drawn in | 0.5245 9 Dec 2010 · Ireland rescue
- `16:01:52`   spread   11 columns, 18 rows | S10_GR in 'Spain's banks, "whatever it takes"': page 2,652.0, document 2,652.0 | now 99.4 | overflow 0 px
- `16:01:53`   euro     10 columns, 18 rows | TGT_IT in 'UK gilt crisis': page −€714.9, document −€714.9 | now −€324.2 | overflow 0 px
## 5. World and Sources

- `16:01:53` world: 44 rows | first: Turkey | Europe | 33.55% | −264.0 bp | 244.5 bp | BB- | 36.00% | 21 November 2026
- `16:01:54` sources: 8 rows, 13 links, says every response arrived: True | Every number on this desk is read from an ECB response that is kept as it arrived. The run receipt lists them; compiling twice from two separate reads has to give the same document before it is published. ECB RESPONSE SERIES ROWS NEWEST PERIOD RECEIVED ORIGINAL CISS, SovCISS and CLIFS (the warehouse) 61 2026-11-22 07:13 warehouse receipt · CISS register Euro-area government yield curve, AAA issuers and all issuers (d
- `16:01:55` the retained yield-curve response: 12767481 bytes, SHA-256 as its receipt says: True | run receipt lists 61 originals and 7 added responses
## 6. The desk alone, on a phone

- `16:01:55` deep link ?m=SOV_FR: France SovCISS | 0.1739 · 19 Nov 2026 · higher than 75% of its readings since 2000 CISS.D.FR.Z0Z.4F.EC.SOV_CIN.IDX · source value 0.17391
- `16:01:59` wider than a 390 px screen by: {"desk": 0, "crisis": 0, "world": 0, "sources": 0} | page errors []
## 7. The CISS page

- `16:02:02` headline: ECB euro-area CISS: 0.0095 on 2026-11-19 — lower than 86% of its readings since 1980.
- `16:02:02`   Exact source value 0.009462216765937262 · highest reading on record 0.9409 · scale 0 to 1
- `16:02:02` reconciliation: Matched on 2026-11-19 — the six contributions add up to the composite. | difference 1.25E-17 (tolerance 0.0000000001)
- `16:02:02` count: 88 shown · 58 current · 2 with a blank newest row · 28 ended at the ECB · 0 collection errors | warehouse by this page's rule: {"fresh": 58, "blank": 2, "ended": 28} | values of the first rows: ['Austria NEW SovCISS - Composite Indicator of Sovereign Stress Austria CISS.D.AT.Z0Z.4F.EC.SOV_CIN.IDX', '0.0052 dimensionless_index', '0.0547 dimensionless_index'] | cells with seven or more decimals: 0
- `16:02:02`   aux: EUR/USD reference rate 1.099 USD per EUR · 2026-11-20 Read from the ECB response received 2026-11-22 and kept as it arrived; source value 1.099 for 2026-11-20. EXR.D.USD.EUR.SP00.A Original ECB response (.gz) Run receipt Check it against the original
- `16:02:02`   aux: Claims on euro-area residents in foreign currency 20.193 EUR bn · 2026-11-13 Read from the ECB response received 2026-11-22 and kept as it arrived; source value 20193 for 2026-W46. ILM.W.U2.C.A030000.U2.Z06 Original ECB response (.gz) Run receipt Che
- `16:02:02`   aux: Claims on non-euro-area residents in foreign currency 521.892 EUR bn · 2026-11-13 Read from the ECB response received 2026-11-22 and kept as it arrived; source value 521892 for 2026-W46. ILM.W.U2.C.A020000.U4.Z06 Original ECB response (.gz) Run recei
- `16:02:03` check the euro reference rate against its original, in the browser: VERIFIED: same bytes, same row, same value { "sha256": "84b666b398336f6dacbf82bf61f789435e2e92cebfe16463eae7f1c5b2cf3f9f", "source_row": 7202, "original": { "KEY": "EXR.D.USD.EUR.SP00.A", "TIME_PERIOD": "2026-11-20", "OBS_VALUE": "1.099", "OBS_STATUS": "A", "UNIT": "USD", "UNIT_MULT": "0", "TITLE": "US dollar/Euro ECB reference 
- `16:02:04` filters: {"ended": 28, "blank": 2, "fresh": 58} | blank rows: Germany NEW CISS - Composite Indicator of Systemic Stress. CISS.D.DE.Z0Z.4F.EC.SS_CIN.IDX 0.0080 last value, 2026-11-19 · the 2026-11-20 row is blank 2026-11-20 D · status M newest row blank at the ECB 2 days old at collection — i || Spain NEW CISS - Composite Indicator of Systemic Stress. CISS.D.ES.Z0Z.4F.EC.SS_CIN.IDX 0.0050 last value, 2026-11-19 · the 2026-11-20 row is blank 2026-11-20 D · status M newest row blank at the ECB 2 days old at collection — ind
- `16:02:06` a series panel still opens and verifies its row: {"open": true, "text": "VERIFIED original bytes and selected row\n{\n  \"sha256\": \"9ad1", "exact": true}
## 8. The fiscal page that stopped on a null

- `16:02:19` sovereign.html: 8 summary figures, 33 charts, 14795 characters, still loading: False, errors []
- `16:02:19`   FOREIGN UST HOLDINGS $9,270B +649B YoY | CHINA $684B -75B YoY | JAPAN (LARGEST) $1,186B +124B YoY | DEFICIT (TTM) $2,112B 12-mo rolling | TOTAL DEBT $40.7T to the penny | INTEREST EXPENSE $1,457B avg 3.58% | FED SEC-LENDING $37.9B 34%ile take-up | TREASURY FAILS $159B UNQUALIFIED · percentile not available
- `16:02:19`   what stopped it: the fails document's headline carries {"combined_bn": 158.647, "pctile": null}
- `16:02:20` favicon.ico: HTTP Error 404: Not Found (the one console 404 the first page of a browser session reports)
- `16:02:20` ✅ the Sovereign button opens the desk; tiles, board, charts, crisis peaks, world and sources equal the documents; the CISS page reads plainly and checks its auxiliaries against their originals
