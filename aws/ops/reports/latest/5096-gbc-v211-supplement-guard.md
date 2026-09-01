# ops 5096 -- GBC v2.1.1 supplement scale guard + FRED series metadata

**Status:** success  
**Duration:** 68.6s  
**Finished:** 2026-09-01T23:59:43+00:00  

## Data

| cli | iso | last | obs_end | phase | series | src | sup | title | units |
|---|---|---|---|---|---|---|---|---|---|
|  |  | 53.25921 | 2026-06-01 |  | USACSCICP02STSAM |  |  | Consumer Opinion Surveys: Composite Consumer Confidence for  | Percentage balance |
|  |  | 0.0 | 2026-05-01 |  | CHNBSCICP02STSAM |  |  | Business Tendency Surveys (Manufacturing): Confidence Indica | Percent |
| 104.07 | USA |  |  | EXPANSION |  | yahoo:^GSPC | scale-unrecognised (53.25921) -- excluded |  |  |
| 101.58 | CHN |  |  | AT_RISK |  | yahoo:000001.SS | stale 4mo (excluded) |  |  |
| 115.5 | KOR |  |  | AT_RISK |  | yahoo:^KS11 | none |  |  |
| 108.0 | CZE |  |  | EXPANSION |  | basket:CEZ.PR+ERBAG.PR+KOMB.PR+MONET.PR+VIG.PR | none |  |  |
| 109.73 | HUN |  |  | EXPANSION |  | basket:MOL.BD+MTELEKOM.BD+OTP.BD | none |  |  |
| 105.53 | NOR |  |  | AT_RISK |  | basket:AKRBP.OL+DNB.OL+EQNR.OL+MOWI.OL+NHY.OL+OR | none |  |  |
| 111.69 | GRC |  |  | EXPANSION |  | basket:ALPHA.AT+ETE.AT+EUROB.AT+PPC.AT+TPEIR.AT | none |  |  |

## Log
## S1 what FRED serves for the supplement series

- `23:58:35` USA USACSCICP02STSAM: title='Consumer Opinion Surveys: Composite Consumer Confidence for United States' units='Percentage balance' freq='Monthly' seasonal='SA' obs_end=2026-06-01 last_updated=2026-07-16 15:13:38-05
- `23:58:35`   notes: 'OECD Data Filters: \nREF_AREA: USA\nMEASURE: CCICP\nUNIT_MEASURE: PB\nACTIVITY: _Z\nADJUSTMENT: Y\nTRANSFORMATION: _Z\nTIME_HORIZ: _Z\nMETHODOLOGY: N\nFREQ: M\n\nAll OECD data should be cited as follows: OECD (year), (dataset name), (data source) DOI or https://data-explorer.oecd.org/ (https://data-explorer.oecd.org/). (accessed on (date)).'
- `23:58:35`   last obs: [('2026-06-01', '53.25921'), ('2026-05-01', '48.20228'), ('2026-04-01', '53.582'), ('2026-03-01', '57.3478'), ('2026-02-01', '60.89841'), ('2026-01-01', '60.68322')]
- `23:58:36` CHN CHNBSCICP02STSAM: title='Business Tendency Surveys (Manufacturing): Confidence Indicators: Composite Indicators: National Indicator for China' units='Percent' freq='Monthly' seasonal='SA' obs_end=2026-05-01 last_updated=2026-06-15 16:47:16-05
- `23:58:36`   notes: 'OECD Data Filters: \nREF_AREA: CHN\nMEASURE: BCICP\nUNIT_MEASURE: PB\nACTIVITY: C\nADJUSTMENT: Y\nTRANSFORMATION: _Z\nFREQ: M\n\nAll OECD data should be cited as follows: OECD (year), (dataset name), (data source) DOI or https://data-explorer.oecd.org/ (https://data-explorer.oecd.org/). (accessed on (date)).'
- `23:58:36`   last obs: [('2026-05-01', '0.0'), ('2026-04-01', '0.6'), ('2026-03-01', '0.8'), ('2026-02-01', '-2.0'), ('2026-01-01', '-1.4'), ('2025-12-01', '0.2')]
## S2 deploy wait + run

- `23:58:36`   waiting… desc_has_v2.1.1=True status=InProgress
- `23:59:01` ✅ deployed 2026-09-01T23:58:42.000+0000 after 26s
- `23:59:01` async invoke at 2026-09-01T23:59:01+00:00
- `23:59:22`   polling… engine_version=2.1.0 generated_at=2026-09-01T23:54:26.888895+00:00
- `23:59:42` ✅ fresh v2.1.1 feed: generated_at=2026-09-01T23:59:34.009533+00:00 elapsed=31.4s
## S3 verify

- `23:59:42` global_phase=GLOBAL_EXPANSION avg_cli=103.97 with_cli=34/34 sources={"yahoo": 30, "basket": 4} supplements={"USA": {"date": "2026-06-01", "value": 53.25921, "months_stale": 3, "status": "scale-unrecognised (53.25921) -- excluded", "series_id": "USACSCICP02STSAM"}, "CHN": {"date": "2026-05-01", "value": 0.0, "months_stale": 4, "status": "stale 4mo (excluded)", "series_id": "CHNBSCICP02STSAM"}}
- `23:59:42`   USA EXPANSION cli=104.07 equity_composite=8.25 src=yahoo:^GSPC sup=scale-unrecognised (53.25921) -- excluded stale=0d
- `23:59:42`   CHN AT_RISK cli=101.58 equity_composite=3.17 src=yahoo:000001.SS sup=stale 4mo (excluded) stale=1d
- `23:59:42`   KOR AT_RISK cli=115.5 equity_composite=41.33 src=yahoo:^KS11 sup=none stale=1d
- `23:59:42`   CZE EXPANSION cli=108.0 equity_composite=16.94 src=basket:CEZ.PR+ERBAG.PR+KOMB.PR+MONET.PR+VIG.PR sup=none stale=1d
- `23:59:42`   HUN EXPANSION cli=109.73 equity_composite=21.27 src=basket:MOL.BD+MTELEKOM.BD+OTP.BD sup=none stale=1d
- `23:59:42`   NOR AT_RISK cli=105.53 equity_composite=11.36 src=basket:AKRBP.OL+DNB.OL+EQNR.OL+MOWI.OL+NHY.OL+ORK.OL+TEL.OL+YAR.OL sup=none stale=1d
- `23:59:42`   GRC EXPANSION cli=111.69 equity_composite=26.78 src=basket:ALPHA.AT+ETE.AT+EUROB.AT+PPC.AT+TPEIR.AT sup=none stale=1d
- `23:59:42` ✅ USA equity-only: cli 104.07 == 100+20*tanh(0.025*8.25) ; supplement scale-unrecognised (53.25921) -- excluded
- `23:59:43` ✅ history regenerated: schema 2.4 transitions=24
- `23:59:43` physical: {"CONFIRMED": 0, "DIVERGENT": 0, "UNCONFIRMED": 34} countries_with_ports=0 carried=None (portwatch upstream: ArcGIS 429 quota + invalid field -- separate engine)
## verdict

- `23:59:43` ✅ VERDICT: GREEN -- v2.1.1: 34/34, USA 104.07 (scale-unrecognised (53.25921) -- excluded)
