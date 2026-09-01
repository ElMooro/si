# ops 5095 -- justhodl-global-business-cycle v2.1.0 deploy + run + verify

**Status:** success  
**Duration:** 41.3s  
**Finished:** 2026-09-01T23:54:34+00:00  

## Data

| attempts | cli | degraded | iso | phase | phys | src | stale_d |
|---|---|---|---|---|---|---|---|
| 1 | 101.27 | False | AUS | EXPANSION | UNCONFIRMED | yahoo:^AXJO | 1 |
| 1 | 110.64 | False | AUT | EXPANSION | UNCONFIRMED | yahoo:^ATX | 1 |
| 1 | 105.77 | False | BEL | EXPANSION | UNCONFIRMED | yahoo:^BFX | 1 |
| 1 | 105.74 | False | BRA | EXPANSION | UNCONFIRMED | yahoo:^BVSP | 0 |
| 1 | 105.66 | False | CAN | EXPANSION | UNCONFIRMED | yahoo:^GSPTSE | 0 |
| 1 | 104.6 | False | CHE | EXPANSION | UNCONFIRMED | yahoo:^SSMI | 1 |
| 1 | 104.71 | False | CHL | EXPANSION | UNCONFIRMED | yahoo:ECH | 0 |
| 1 | 101.58 | False | CHN | AT_RISK | UNCONFIRMED | yahoo:000001.SS | 1 |
| 10 | 108.0 | False | CZE | EXPANSION | UNCONFIRMED | basket:CEZ.PR+ERBAG.PR+KOMB.PR+MONET.PR+ | 1 |
| 1 | 103.29 | False | DEU | EXPANSION | UNCONFIRMED | yahoo:^GDAXI | 1 |
| 1 | 103.97 | False | DNK | EXPANSION | UNCONFIRMED | yahoo:^OMXC25 | 0 |
| 1 | 108.27 | False | ESP | EXPANSION | UNCONFIRMED | yahoo:^IBEX | 1 |
| 1 | 106.06 | False | FIN | EXPANSION | UNCONFIRMED | yahoo:^OMXH25 | 1 |
| 1 | 101.77 | False | FRA | EXPANSION | UNCONFIRMED | yahoo:^FCHI | 1 |
| 1 | 104.03 | False | GBR | EXPANSION | UNCONFIRMED | yahoo:^FTSE | 5 |
| 11 | 111.69 | False | GRC | EXPANSION | UNCONFIRMED | basket:ALPHA.AT+ETE.AT+EUROB.AT+PPC.AT+T | 1 |
| 7 | 109.73 | False | HUN | EXPANSION | UNCONFIRMED | basket:MOL.BD+MTELEKOM.BD+OTP.BD | 1 |
| 1 | 97.4 | False | IDN | RECOVERY | UNCONFIRMED | yahoo:^JKSE | 1 |
| 1 | 98.91 | False | IND | RECOVERY | UNCONFIRMED | yahoo:^BSESN | 1 |
| 1 | 106.58 | False | IRL | EXPANSION | UNCONFIRMED | yahoo:^ISEQ | 1 |
| 1 | 104.76 | False | ISR | AT_RISK | UNCONFIRMED | yahoo:^TA125.TA | 1 |
| 1 | 105.94 | False | ITA | EXPANSION | UNCONFIRMED | yahoo:FTSEMIB.MI | 1 |
| 1 | 110.37 | False | JPN | EXPANSION | UNCONFIRMED | yahoo:^N225 | 1 |
| 1 | 115.5 | False | KOR | AT_RISK | UNCONFIRMED | yahoo:^KS11 | 1 |
| 1 | 100.15 | False | MEX | AT_RISK | UNCONFIRMED | yahoo:^MXX | 0 |
| 1 | 105.94 | False | NLD | EXPANSION | UNCONFIRMED | yahoo:^AEX | 1 |
| 13 | 105.53 | False | NOR | AT_RISK | UNCONFIRMED | basket:AKRBP.OL+DNB.OL+EQNR.OL+MOWI.OL+N | 1 |
| 1 | 102.23 | False | NZL | EXPANSION | UNCONFIRMED | yahoo:^NZ50 | 0 |
| 1 | 109.81 | False | POL | EXPANSION | UNCONFIRMED | yahoo:EPOL | 0 |
| 1 | 105.51 | False | PRT | EXPANSION | UNCONFIRMED | yahoo:PSI20.LS | 1 |
| 1 | 106.15 | False | SWE | EXPANSION | UNCONFIRMED | yahoo:^OMX | 1 |
| 1 | 106.22 | False | TUR | EXPANSION | UNCONFIRMED | yahoo:XU100.IS | 1 |
| 1 | 101.22 | False | USA | EXPANSION | UNCONFIRMED | yahoo:^GSPC | 0 |
| 1 | 103.0 | False | ZAF | EXPANSION | UNCONFIRMED | yahoo:^J203.JO | 1 |

## Log
- `23:53:53` started 2026-09-01T23:53:53+00:00
## S1 BEFORE (live feed as deployed)

- `23:53:53` generated_at=2026-09-01T23:17:32.778530+00:00 schema=2.0 engine_version=None fresh=33
- `23:53:53`   USA  EXPANSION  CLI=120 3m=0.415 12m=17.374 src=^GSPC stale=0d phys=UNCONFIRMED  supplement=53.25921@2026-06-01
- `23:53:53`   CHN  AT_RISK    CLI=101.19 3m=-1.761 12m=8.724 src=000001.SS stale=1d phys=UNCONFIRMED  supplement=0.0@2026-05-01
- `23:53:53`   KOR  AT_RISK    CLI=120 3m=-16.68 12m=111.43 src=^KS11 stale=1d phys=UNCONFIRMED  supplement=None@None
- `23:53:53`   CZE  UNKNOWN    CLI=None 3m=None 12m=None src=PXTR.PR stale=999d phys=UNCONFIRMED reason=insufficient_history supplement=None@None
- `23:53:53`   HUN  EXPANSION  CLI=111.76 3m=10.557 12m=50.203 src=OTP.BD stale=1d phys=UNCONFIRMED  supplement=None@None
- `23:53:53`   NOR  AT_RISK    CLI=104.78 3m=-2.6 12m=22.17 src=^OSEAX stale=2d phys=UNCONFIRMED  supplement=None@None
- `23:53:53`   GRC  EXPANSION  CLI=106.32 3m=5.983 12m=24.73 src=GD.AT stale=2d phys=UNCONFIRMED  supplement=None@None
- `23:53:53`   physical: {"CONFIRMED": 0, "DIVERGENT": 0, "UNCONFIRMED": 34} countries_with_ports=0
- `23:53:53`   countries pinned at the 120 cap BEFORE: ['USA', 'KOR']
## S2 wait for deploy

- `23:53:53` ✅ deployed: 2026-09-01T23:49:14.000+0000 timeout=900 mem=1536 status=Successful after 0s
## S3 run the engine

- `23:53:53` async invoke status=202 at 2026-09-01T23:53:53+00:00
- `23:54:13`   polling… engine_version=None generated_at=2026-09-01T23:17:32.778530+00:00
- `23:54:33` ✅ fresh v2.1.0 feed on S3: generated_at=2026-09-01T23:54:26.888895+00:00 elapsed=32.6s after 40s
## S4 per-country

- `23:54:33` global_phase=GLOBAL_EXPANSION avg_cli=103.13 coverage=100.0% mix={"EXPANSION": 68.0, "AT_RISK": 26.2, "RECOVERY": 5.8}
- `23:54:33` sources_used={"yahoo": 30, "basket": 4} polygon_lane={"loaded": false, "sessions": 0, "tickers_present": 0, "error": null}
- `23:54:33` supplements={"USA": {"date": "2026-06-01", "value": 53.25921, "months_stale": 3, "status": "fresh", "series_id": "USACSCICP02STSAM"}, "CHN": {"date": "2026-05-01", "value": 0.0, "months_stale": 4, "status": "stale 4mo (excluded)", "series_id": "CHNBSCICP02STSAM"}}
- `23:54:33` acceptance_gate={"min_bars": 252, "max_stale_days": 12} fresh=34/34
- `23:54:33`   AUS  EXPANSION  CLI=101.27 3m=3.97 12m=1.661 src=yahoo:^AXJO stale=1d phys=UNCONFIRMED 
- `23:54:33`   AUT  EXPANSION  CLI=110.64 3m=10.232 12m=46.678 src=yahoo:^ATX stale=1d phys=UNCONFIRMED 
- `23:54:33`   BEL  EXPANSION  CLI=105.77 3m=5.13 12m=23.471 src=yahoo:^BFX stale=1d phys=UNCONFIRMED 
- `23:54:33`   BRA  EXPANSION  CLI=105.74 3m=3.171 12m=29.105 src=yahoo:^BVSP stale=0d phys=UNCONFIRMED 
- `23:54:33`   CAN  EXPANSION  CLI=105.66 3m=3.14 12m=25.993 src=yahoo:^GSPTSE stale=0d phys=UNCONFIRMED 
- `23:54:33`   CHE  EXPANSION  CLI=104.6 3m=7.371 12m=17.478 src=yahoo:^SSMI stale=1d phys=UNCONFIRMED 
- `23:54:33`   CHL  EXPANSION  CLI=104.71 3m=-0.054 12m=26.378 src=yahoo:ECH stale=0d phys=UNCONFIRMED 
- `23:54:33`   CHN  AT_RISK    CLI=101.58 3m=-1.761 12m=8.724 src=yahoo:000001.SS stale=1d phys=UNCONFIRMED 
- `23:54:33`   CZE  EXPANSION  CLI=108.0 3m=12.181 12m=31.446 src=basket:CEZ.PR+ERBAG.PR+KOMB.PR+MONET.PR+VIG.PR stale=1d phys=UNCONFIRMED 
- `23:54:33`   DEU  EXPANSION  CLI=103.29 3m=4.513 12m=9.856 src=yahoo:^GDAXI stale=1d phys=UNCONFIRMED 
- `23:54:33`   DNK  EXPANSION  CLI=103.97 3m=6.304 12m=12.97 src=yahoo:^OMXC25 stale=0d phys=UNCONFIRMED 
- `23:54:33`   ESP  EXPANSION  CLI=108.27 3m=9.315 12m=35.839 src=yahoo:^IBEX stale=1d phys=UNCONFIRMED 
- `23:54:33`   FIN  EXPANSION  CLI=106.06 3m=-0.32 12m=29.459 src=yahoo:^OMXH25 stale=1d phys=UNCONFIRMED 
- `23:54:33`   FRA  EXPANSION  CLI=101.77 3m=1.528 12m=8.887 src=yahoo:^FCHI stale=1d phys=UNCONFIRMED 
- `23:54:33`   GBR  EXPANSION  CLI=104.03 3m=4.386 12m=17.472 src=yahoo:^FTSE stale=5d phys=UNCONFIRMED 
- `23:54:33`   GRC  EXPANSION  CLI=111.69 3m=18.814 12m=44.758 src=basket:ALPHA.AT+ETE.AT+EUROB.AT+PPC.AT+TPEIR.AT stale=1d phys=UNCONFIRMED 
- `23:54:33`   HUN  EXPANSION  CLI=109.73 3m=4.304 12m=45.043 src=basket:MOL.BD+MTELEKOM.BD+OTP.BD stale=1d phys=UNCONFIRMED 
- `23:54:33`   IDN  RECOVERY   CLI=97.4 3m=5.142 12m=-12.88 src=yahoo:^JKSE stale=1d phys=UNCONFIRMED 
- `23:54:33`   IND  RECOVERY   CLI=98.91 3m=3.622 12m=-5.987 src=yahoo:^BSESN stale=1d phys=UNCONFIRMED 
- `23:54:33`   IRL  EXPANSION  CLI=106.58 3m=7.932 12m=25.513 src=yahoo:^ISEQ stale=1d phys=UNCONFIRMED 
- `23:54:33`   ISR  AT_RISK    CLI=104.76 3m=-4.312 12m=29.734 src=yahoo:^TA125.TA stale=1d phys=UNCONFIRMED 
- `23:54:33`   ITA  EXPANSION  CLI=105.94 3m=4.021 12m=24.687 src=yahoo:FTSEMIB.MI stale=1d phys=UNCONFIRMED 
- `23:54:33`   JPN  EXPANSION  CLI=110.37 3m=-0.026 12m=52.869 src=yahoo:^N225 stale=1d phys=UNCONFIRMED 
- `23:54:33`   KOR  AT_RISK    CLI=115.5 3m=-16.68 12m=111.43 src=yahoo:^KS11 stale=1d phys=UNCONFIRMED 
- `23:54:33`   MEX  AT_RISK    CLI=100.15 3m=-5.523 12m=9.066 src=yahoo:^MXX stale=0d phys=UNCONFIRMED 
- `23:54:33`   NLD  EXPANSION  CLI=105.94 3m=5.406 12m=25.103 src=yahoo:^AEX stale=1d phys=UNCONFIRMED 
- `23:54:33`   NOR  AT_RISK    CLI=105.53 3m=-1.166 12m=26.265 src=basket:AKRBP.OL+DNB.OL+EQNR.OL+MOWI.OL+NHY.OL+ORK.OL+TEL.OL+YAR.OL stale=1d phys=UNCONFIRMED 
- `23:54:33`   NZL  EXPANSION  CLI=102.23 3m=4.658 12m=7.171 src=yahoo:^NZ50 stale=0d phys=UNCONFIRMED 
- `23:54:33`   POL  EXPANSION  CLI=109.81 3m=10.126 12m=42.157 src=yahoo:EPOL stale=0d phys=UNCONFIRMED 
- `23:54:33`   PRT  EXPANSION  CLI=105.51 3m=5.344 12m=22.919 src=yahoo:PSI20.LS stale=1d phys=UNCONFIRMED 
- `23:54:33`   SWE  EXPANSION  CLI=106.15 3m=6.937 12m=24.553 src=yahoo:^OMX stale=1d phys=UNCONFIRMED 
- `23:54:33`   TUR  EXPANSION  CLI=106.22 3m=4.598 12m=24.322 src=yahoo:XU100.IS stale=1d phys=UNCONFIRMED 
- `23:54:33`   USA  EXPANSION  CLI=101.22 3m=0.415 12m=17.374 src=yahoo:^GSPC stale=0d phys=UNCONFIRMED 
- `23:54:33`   ZAF  EXPANSION  CLI=103.0 3m=1.418 12m=13.496 src=yahoo:^J203.JO stale=1d phys=UNCONFIRMED 
- `23:54:33` with CLI: 34/34 · UNKNOWN: []
- `23:54:33` pinned at a cap: [] · degraded: []
- `23:54:33` USA: cli=101.22 equity_composite=8.25 supplement=53.25921@2026-06-01 status=fresh
- `23:54:33` physical: counts={"CONFIRMED": 0, "DIVERGENT": 0, "UNCONFIRMED": 34} countries_with_ports=0 carried_from_previous_run=None portwatch_at=2026-09-01T12:10:51.227119+00:00
## S5 history + portwatch

- `23:54:33` history: schema=2.4 engine_version=2.1.0 generated_at=2026-09-01T23:54:28.256585+00:00 countries=34 transitions=24 cli_transform=soft_clip_tanh_v2.1
- `23:54:33` portwatch: generated_at=2026-09-01T12:10:51.227119+00:00 ports=0 with_yoy_pct=0 errors=["daily_choke: {'code': 429, 'message': 'Unable to perform query. Too many requests.', 'details': ['API calls quota exceeded (6015 requ", "daily_ports: {'code': 400, 'message': 'Cannot perform query. Invalid query parameters.', 'details': [\"'Invalid field: chokepoint1' pa", "ports_ref: {'code': 429
## S6 log tail of this run

- `23:54:34` ── stream f2cf4e70b5e2ca8a61dd2e5f (43 events) ──
- `23:54:34`   [gbc] v2.1.0 start, 34 countries (equity-momentum-based)
- `23:54:34`   [gbc] USA  EXPANSION  CLI=101.22 3m=0.415 latest=2026-09-01 (0d) src=yahoo:^GSPC
- `23:54:34`   [gbc] CHN  AT_RISK    CLI=101.58 3m=-1.761 latest=2026-08-31 (1d) src=yahoo:000001.SS
- `23:54:34`   [gbc] JPN  EXPANSION  CLI=110.37 3m=-0.026 latest=2026-08-31 (1d) src=yahoo:^N225
- `23:54:34`   [gbc] DEU  EXPANSION  CLI=103.29 3m=4.513 latest=2026-08-31 (1d) src=yahoo:^GDAXI
- `23:54:34`   [gbc] IND  RECOVERY   CLI=98.91 3m=3.622 latest=2026-08-31 (1d) src=yahoo:^BSESN
- `23:54:34`   [gbc] GBR  EXPANSION  CLI=104.03 3m=4.386 latest=2026-08-27 (5d) src=yahoo:^FTSE
- `23:54:34`   [gbc] FRA  EXPANSION  CLI=101.77 3m=1.528 latest=2026-08-31 (1d) src=yahoo:^FCHI
- `23:54:34`   [gbc] ITA  EXPANSION  CLI=105.94 3m=4.021 latest=2026-08-31 (1d) src=yahoo:FTSEMIB.MI
- `23:54:34`   [gbc] CAN  EXPANSION  CLI=105.66 3m=3.14 latest=2026-09-01 (0d) src=yahoo:^GSPTSE
- `23:54:34`   [gbc] BRA  EXPANSION  CLI=105.74 3m=3.171 latest=2026-09-01 (0d) src=yahoo:^BVSP
- `23:54:34`   [gbc] KOR  AT_RISK    CLI=115.5 3m=-16.68 latest=2026-08-31 (1d) src=yahoo:^KS11
- `23:54:34`   [gbc] AUS  EXPANSION  CLI=101.27 3m=3.97 latest=2026-08-31 (1d) src=yahoo:^AXJO
- `23:54:34`   [gbc] ESP  EXPANSION  CLI=108.27 3m=9.315 latest=2026-08-31 (1d) src=yahoo:^IBEX
- `23:54:34`   [gbc] MEX  AT_RISK    CLI=100.15 3m=-5.523 latest=2026-09-01 (0d) src=yahoo:^MXX
- `23:54:34`   [gbc] IDN  RECOVERY   CLI=97.4 3m=5.142 latest=2026-08-31 (1d) src=yahoo:^JKSE
- `23:54:34`   [gbc] NLD  EXPANSION  CLI=105.94 3m=5.406 latest=2026-08-31 (1d) src=yahoo:^AEX
- `23:54:34`   [gbc] TUR  EXPANSION  CLI=106.22 3m=4.598 latest=2026-08-31 (1d) src=yahoo:XU100.IS
- `23:54:34`   [gbc] CHE  EXPANSION  CLI=104.6 3m=7.371 latest=2026-08-31 (1d) src=yahoo:^SSMI
- `23:54:34`   [gbc] POL  EXPANSION  CLI=109.81 3m=10.126 latest=2026-09-01 (0d) src=yahoo:EPOL
- `23:54:34`   [gbc] BEL  EXPANSION  CLI=105.77 3m=5.13 latest=2026-08-31 (1d) src=yahoo:^BFX
- `23:54:34`   [gbc] SWE  EXPANSION  CLI=106.15 3m=6.937 latest=2026-08-31 (1d) src=yahoo:^OMX
- `23:54:34`   [gbc] IRL  EXPANSION  CLI=106.58 3m=7.932 latest=2026-08-31 (1d) src=yahoo:^ISEQ
- `23:54:34`   [gbc] AUT  EXPANSION  CLI=110.64 3m=10.232 latest=2026-08-31 (1d) src=yahoo:^ATX
- `23:54:34`   [gbc] ^OBX failed: no usable bars
- `23:54:34`   [gbc] ^OSEBX failed: no usable bars
- `23:54:34`   [gbc] NOR  AT_RISK    CLI=105.53 3m=-1.166 latest=2026-08-31 (1d) src=basket:AKRBP.OL+DNB.OL+EQNR.OL+MOWI.OL+NHY.OL+ORK.OL+TEL.OL+YAR.OL
- `23:54:34`   [gbc] ZAF  EXPANSION  CLI=103.0 3m=1.418 latest=2026-08-31 (1d) src=yahoo:^J203.JO
- `23:54:34`   [gbc] DNK  EXPANSION  CLI=103.97 3m=6.304 latest=2026-09-01 (0d) src=yahoo:^OMXC25
- `23:54:34`   [gbc] FIN  EXPANSION  CLI=106.06 3m=-0.32 latest=2026-08-31 (1d) src=yahoo:^OMXH25
- `23:54:34`   [gbc] PX.PR failed: HTTP 404
- `23:54:34`   [gbc] ^PX failed: no usable bars
- `23:54:34`   [gbc] CZE  EXPANSION  CLI=108.0 3m=12.181 latest=2026-08-31 (1d) src=basket:CEZ.PR+ERBAG.PR+KOMB.PR+MONET.PR+VIG.PR
- `23:54:34`   [gbc] BUX.BD failed: HTTP 404
- `23:54:34`   [gbc] ^BUX failed: no usable bars
- `23:54:34`   [gbc] HUN  EXPANSION  CLI=109.73 3m=4.304 latest=2026-08-31 (1d) src=basket:MOL.BD+MTELEKOM.BD+OTP.BD
- `23:54:34`   [gbc] CHL  EXPANSION  CLI=104.71 3m=-0.054 latest=2026-09-01 (0d) src=yahoo:ECH
- `23:54:34`   [gbc] PRT  EXPANSION  CLI=105.51 3m=5.344 latest=2026-08-31 (1d) src=yahoo:PSI20.LS
- `23:54:34`   [gbc] ^ATG failed: no usable bars
- `23:54:34`   [gbc] OPAP.AT failed: HTTP 404
- `23:54:34`   [gbc] MYTIL.AT failed: HTTP 404
## verdict

- `23:54:34` ✅ VERDICT: GREEN -- v2.1.0 live: 34/34 countries, USA CLI 101.22 (was pinned at 120), sources {"yahoo": 30, "basket": 4}, physical {"CONFIRMED": 0, "DIVERGENT": 0, "UNCONFIRMED": 34}
