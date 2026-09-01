# ops 5092 -- global-cycle.html / justhodl-global-business-cycle forensics

**Status:** success  
**Duration:** 86.7s  
**Finished:** 2026-09-01T23:17:42+00:00  

## Data

| acao | age_h | avg_cli | bytes | cache | cf | ctype | elapsed_sec | env_keys | err | expr | first | fmp | fmp_err | fresh | generated_at | global_phase | has_src_const | has_world_topo | last | last_mod | last_modified | last_update | memory | ms | n | n_aggregate_points | n_by_country | n_countries | name | reserved | runtime | s1 | s2 | s3 | s4 | s5 | s6 | schema | src_is_raw_s3 | state | status | stooq | stooq_err | stooq_success_rate | sym | targets | timeout | title_ok | top_keys | total | unknown | weight_covered | with_data | y_brw | y_eng | y_eng_err | yahoo_success_rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 11.2 | 108.68 |  |  |  |  | 72.5 |  |  |  |  |  |  | 33 | 2026-09-01T12:01:33.582946+00:00 | GLOBAL_EXPANSION |  |  |  |  | 2026-09-01 12:01:34+00:00 |  |  |  |  |  | 34 |  |  |  |  | live |  |  |  |  |  | 2.0 |  |  |  |  |  |  |  |  |  |  |  | 34 | 1 | 84.4 |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 2022-08-23 |  |  |  | 2026-09-01T12:01:34.948383+00:00 |  |  |  | 2026-09-01 |  | 2026-09-01 12:01:35+00:00 |  |  |  |  | 1090 |  | 33 |  |  |  | history |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ['aggregate', 'by_country', 'countries_count', 'engine_type', 'frequency', 'generated_at', 'history_elapsed_sec', 'lead_lag_count', 'lead_lag_max_lag_weeks', 'lead_lag_methodology', 'lead_lag_ranking', 'phase_returns_methodology'] |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-01T23:14:33+00:00 |  |  |  |  |  | 2026-09-01 23:14:34+00:00 |  |  |  | None |  |  |  |  |  |  |  | price-redundancy |  |  |  |  |  |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  | None |
|  |  |  |  |  |  |  |  | ['FRED_KEY', 'S3_BUCKET'] |  |  |  |  |  |  |  |  |  |  |  |  | 2026-08-02T20:29:00.000+0000 | Successful | 1536 |  |  |  |  |  |  | None | python3.12 |  |  | config |  |  |  |  |  | Active |  |  |  |  |  |  | 900 |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | cron(0 12 * * ? *) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | justhodl-gbc-daily |  |  |  |  | rule |  |  |  |  |  | ENABLED |  |  |  |  |  | [':function:justhodl-global-business-cycle'] |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 31952 |  |  |  |  |  | None |  |  |  |  |  |  |  | True | True |  |  |  |  |  | 207 |  |  |  |  |  |  |  |  |  |  | page |  |  |  | True |  | 200 |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |
|  |  |  | 64428 |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 46 |  |  |  |  |  |  |  |  |  |  | history_page |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| * |  |  | 30569 | public, max-age=60, s-maxage=300 | None | application/json |  |  | None |  |  |  |  |  | 2026-09-01T12:01:33.582946+00:00 |  |  |  |  | Tue, 01 Sep 2026 12:01:34 GMT |  |  |  | 205 |  |  |  |  |  |  |  |  |  |  | data_proxy |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| * |  |  | 1389497 | public, max-age=60, s-maxage=300 | None | application/json |  |  | None |  |  |  |  |  | 2026-09-01T12:01:34.948383+00:00 |  |  |  |  | Tue, 01 Sep 2026 12:01:35 GMT |  |  |  | 197 |  |  |  |  |  |  |  |  |  |  | data_proxy_hist |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| * |  |  | 30569 | public, max-age=3600, s-maxage=3600 | None | application/json |  |  | None |  |  |  |  |  | 2026-09-01T12:01:33.582946+00:00 |  |  |  |  | Tue, 01 Sep 2026 12:01:34 GMT |  |  |  | 145 |  |  |  |  |  |  |  |  |  |  | raw_s3 |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| * |  |  | 1389497 | public, max-age=3600, s-maxage=3600 | None | application/json |  |  | None |  |  |  |  |  | 2026-09-01T12:01:34.948383+00:00 |  |  |  |  | Tue, 01 Sep 2026 12:01:35 GMT |  |  |  | 122 |  |  |  |  |  |  |  |  |  |  | raw_s3_hist |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 279706 |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 29 |  |  |  |  |  |  |  |  |  |  | d3 |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 7169 |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 18 |  |  |  |  |  |  |  |  |  |  | topojson |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 107761 |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 19 |  |  |  |  |  |  |  |  |  |  | world_atlas |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 8552 |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 47 |  |  |  |  |  |  |  |  |  |  | wss_client |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 8601 |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 51 |  |  |  |  |  |  |  |  |  |  | page_ai |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 29978 |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 76 |  |  |  |  |  |  |  |  |  |  | nav_drawer |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 11867 |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 56 |  |  |  |  |  |  |  |  |  |  | sidebar |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | USA |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^GSPC |  |  |  |  |  |  |  |  | skip/None/None | 200/1253/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CHN |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | 000001.SS |  |  |  |  |  |  |  |  | skip/None/None | 200/1209/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | JPN |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^N225 |  |  |  |  |  |  |  |  | skip/None/None | 200/1221/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | DEU |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^GDAXI |  |  |  |  |  |  |  |  | skip/None/None | 200/1273/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | IND |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^BSESN |  |  |  |  |  |  |  |  | skip/None/None | 200/1232/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | GBR |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^FTSE |  |  |  |  |  |  |  |  | skip/None/None | 200/1260/2026-08-27 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | FRA |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^FCHI |  |  |  |  |  |  |  |  | skip/None/None | 200/1279/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ITA |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | FTSEMIB.MI |  |  |  |  |  |  |  |  | skip/None/None | 200/1269/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CAN |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^GSPTSE |  |  |  |  |  |  |  |  | skip/None/None | 200/1254/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | BRA |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^BVSP |  |  |  |  |  |  |  |  | skip/None/None | 200/1247/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | KOR |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^KS11 |  |  |  |  |  |  |  |  | skip/None/None | 200/1218/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | AUS |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^AXJO |  |  |  |  |  |  |  |  | skip/None/None | 200/1263/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ESP |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^IBEX |  |  |  |  |  |  |  |  | skip/None/None | 200/1277/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | MEX |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^MXX |  |  |  |  |  |  |  |  | skip/None/None | 200/1257/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | IDN |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^JKSE |  |  |  |  |  |  |  |  | skip/None/None | 200/1197/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | NLD |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^AEX |  |  |  |  |  |  |  |  | skip/None/None | 200/1279/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | TUR |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | XU100.IS |  |  |  |  |  |  |  |  | skip/None/None | 200/1249/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CHE |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^SSMI |  |  |  |  |  |  |  |  | skip/None/None | 200/1254/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | POL |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | EPOL |  |  |  |  |  |  |  |  | skip/None/None | 200/1253/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | BEL |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^BFX |  |  |  |  |  |  |  |  | skip/None/None | 200/1279/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | SWE |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^OMX |  |  |  |  |  |  |  |  | skip/None/None | 200/1255/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | IRL |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^ISEQ |  |  |  |  |  |  |  |  | skip/None/None | 200/1270/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | AUT |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^ATX |  |  |  |  |  |  |  |  | skip/None/None | 200/1269/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | NOR |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^OSEAX |  |  |  |  |  |  |  |  | skip/None/None | 200/1223/2026-07-17 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ZAF |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^J203.JO |  |  |  |  |  |  |  |  | skip/None/None | 200/1247/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | DNK |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^OMXC25 |  |  |  |  |  |  |  |  | skip/None/None | 200/1216/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | FIN |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^OMXH25 |  |  |  |  |  |  |  |  | skip/None/None | 200/1253/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CZE |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | PX.PR |  |  |  |  |  |  |  |  | 404/0/None | 404/0/None | HTTP 404 {"chart":{"result":null,"error":{"code":"Not Found" |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | HUN |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | BUX.BD |  |  |  |  |  |  |  |  | 404/0/None | 404/0/None | HTTP 404 {"chart":{"result":null,"error":{"code":"Not Found" |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CHL |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ECH |  |  |  |  |  |  |  |  | skip/None/None | 200/1253/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PRT |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | PSI20.LS |  |  |  |  |  |  |  |  | skip/None/None | 200/1278/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | GRC |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | GD.AT |  |  |  |  |  |  |  |  | skip/None/None | 200/1210/2026-07-17 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | NZL |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^NZ50 |  |  |  |  |  |  |  |  | skip/None/None | 200/1247/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 403/0/None | HTTP 403 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ISR |  |  |  |  |  | None/200/0/None | <!DOCTYPE html><html><head><meta charset |  | ^TA125.TA |  |  |  |  |  |  |  |  | skip/None/None | 200/1009/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | POL-fb |  |  |  |  |  |  |  |  | WIG20.WA |  |  |  |  |  |  |  |  |  | 200/1/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | POL-fb |  |  |  |  |  |  |  |  | ^WIG |  |  |  |  |  |  |  |  |  | 200/0/None |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CZE-fb |  |  |  |  |  |  |  |  | ^PX |  |  |  |  |  |  |  |  |  | 200/0/None |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CZE-fb |  |  |  |  |  |  |  |  | PXTR.PR |  |  |  |  |  |  |  |  |  | 200/1/2026-09-01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | HUN-fb |  |  |  |  |  |  |  |  | ^BUX |  |  |  |  |  |  |  |  |  | 200/0/None |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | HUN-fb |  |  |  |  |  |  |  |  | OTP.BD |  |  |  |  |  |  |  |  |  | 200/1246/2026-08-31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CHL-fb |  |  |  |  |  |  |  |  | ^IPSA |  |  |  |  |  |  |  |  |  | 200/1/2026-07-17 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CHL-fb |  |  |  |  |  |  |  |  | ^SPCLXIPSA |  |  |  |  |  |  |  |  |  | 404/0/None | HTTP 404 {"chart":{"result":null,"error":{"code":"Not Found" |  |
|  |  |  |  |  |  |  | 19.8 |  |  |  |  |  |  | 33 | 2026-09-01T23:17:32.778530+00:00 | GLOBAL_EXPANSION |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | fresh_run |  |  |  |  |  |  |  |  |  |  |  |  | 34 |  |  | 33 |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-01T23:17:34.145922+00:00 |  |  |  |  |  | 2026-09-01 23:17:35+00:00 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | history_after_run |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |

## Log
## S1 live feed on S3

- `23:16:16` UNKNOWN rows: ['CZE']
- `23:16:16` stale>3mo rows: [('CZE', 999)]
- `23:16:16`   AUS  EXPANSION  sym=^AXJO        latest=2026-09-01 stale=0mo cli=101.27 3m=3.923 reason=None phys=CONFIRMED
- `23:16:16`   AUT  EXPANSION  sym=^ATX         latest=2026-09-01 stale=0mo cli=111.59 3m=10.739 reason=None phys=UNCONFIRMED
- `23:16:16`   BEL  EXPANSION  sym=^BFX         latest=2026-09-01 stale=0mo cli=105.86 3m=5.895 reason=None phys=UNCONFIRMED
- `23:16:16`   BRA  EXPANSION  sym=^BVSP        latest=2026-08-31 stale=1mo cli=105.62 3m=3.032 reason=None phys=UNCONFIRMED
- `23:16:16`   CAN  EXPANSION  sym=^GSPTSE      latest=2026-08-31 stale=1mo cli=106.54 3m=4.318 reason=None phys=UNCONFIRMED
- `23:16:16`   CHE  EXPANSION  sym=^SSMI        latest=2026-09-01 stale=0mo cli=104.7 3m=8.071 reason=None phys=UNCONFIRMED
- `23:16:16`   CHL  AT_RISK    sym=ECH          latest=2026-08-31 stale=1mo cli=104.61 3m=-2.002 reason=None phys=DIVERGENT
- `23:16:16`   CHN  AT_RISK    sym=000001.SS    latest=2026-09-01 stale=0mo cli=100.93 3m=-2.336 reason=None phys=CONFIRMED
- `23:16:16`   CZE  UNKNOWN    sym=PXTR.PR      latest=None stale=999mo cli=None 3m=None reason=insufficient_history phys=UNCONFIRMED
- `23:16:16`   DEU  EXPANSION  sym=^GDAXI       latest=2026-09-01 stale=0mo cli=102.87 3m=4.893 reason=None phys=DIVERGENT
- `23:16:16`   DNK  EXPANSION  sym=^OMXC25      latest=2026-09-01 stale=0mo cli=104.21 3m=6.679 reason=None phys=UNCONFIRMED
- `23:16:16`   ESP  EXPANSION  sym=^IBEX        latest=2026-09-01 stale=0mo cli=108.32 3m=9.154 reason=None phys=CONFIRMED
- `23:16:16`   FIN  AT_RISK    sym=^OMXH25      latest=2026-09-01 stale=0mo cli=105.93 3m=-2.133 reason=None phys=CONFIRMED
- `23:16:16`   FRA  EXPANSION  sym=^FCHI        latest=2026-09-01 stale=0mo cli=101.55 3m=2.001 reason=None phys=UNCONFIRMED
- `23:16:16`   GBR  EXPANSION  sym=^FTSE        latest=2026-09-01 stale=0mo cli=103.86 3m=3.724 reason=None phys=UNCONFIRMED
- `23:16:16`   GRC  EXPANSION  sym=GD.AT        latest=2026-09-01 stale=0mo cli=111.23 3m=17.349 reason=None phys=UNCONFIRMED
- `23:16:16`   HUN  EXPANSION  sym=OTP.BD       latest=2026-09-01 stale=0mo cli=110.79 3m=6.684 reason=None phys=UNCONFIRMED
- `23:16:16`   IDN  RECOVERY   sym=^JKSE        latest=2026-09-01 stale=0mo cli=98.07 3m=7.663 reason=None phys=UNCONFIRMED
- `23:16:16`   IND  RECOVERY   sym=^BSESN       latest=2026-09-01 stale=0mo cli=98.79 3m=3.074 reason=None phys=CONFIRMED
- `23:16:16`   IRL  EXPANSION  sym=^ISEQ        latest=2026-09-01 stale=0mo cli=107.26 3m=8.273 reason=None phys=UNCONFIRMED
- `23:16:16`   ISR  AT_RISK    sym=^TA125.TA    latest=2026-09-01 stale=0mo cli=105.01 3m=-4.392 reason=None phys=UNCONFIRMED
- `23:16:16`   ITA  EXPANSION  sym=FTSEMIB.MI   latest=2026-09-01 stale=0mo cli=105.59 3m=4.125 reason=None phys=UNCONFIRMED
- `23:16:16`   JPN  AT_RISK    sym=^N225        latest=2026-09-01 stale=0mo cli=111.0 3m=-1.074 reason=None phys=CONFIRMED
- `23:16:16`   KOR  AT_RISK    sym=^KS11        latest=2026-09-01 stale=0mo cli=120 3m=-19.353 reason=None phys=CONFIRMED
- `23:16:16`   MEX  AT_RISK    sym=^MXX         latest=2026-08-31 stale=1mo cli=100.88 3m=-5.022 reason=None phys=UNCONFIRMED
- `23:16:16`   NLD  EXPANSION  sym=^AEX         latest=2026-09-01 stale=0mo cli=105.95 3m=5.63 reason=None phys=UNCONFIRMED
- `23:16:16`   NOR  AT_RISK    sym=^OSEAX       latest=2026-07-17 stale=2mo cli=104.78 3m=-2.6 reason=None phys=UNCONFIRMED
- `23:16:16`   NZL  EXPANSION  sym=^NZ50        latest=2026-09-01 stale=0mo cli=102.25 3m=4.678 reason=None phys=UNCONFIRMED
- `23:16:16`   POL  EXPANSION  sym=EPOL         latest=2026-08-31 stale=1mo cli=110.84 3m=9.671 reason=None phys=UNCONFIRMED
- `23:16:16`   PRT  EXPANSION  sym=PSI20.LS     latest=2026-09-01 stale=0mo cli=105.95 3m=5.353 reason=None phys=UNCONFIRMED
- `23:16:16`   SWE  EXPANSION  sym=^OMX         latest=2026-09-01 stale=0mo cli=105.61 3m=4.009 reason=None phys=UNCONFIRMED
- `23:16:16`   TUR  EXPANSION  sym=XU100.IS     latest=2026-09-01 stale=0mo cli=106.01 3m=0.626 reason=None phys=UNCONFIRMED
- `23:16:16`   USA  EXPANSION  sym=^GSPC        latest=2026-08-31 stale=1mo cli=120 3m=1.399 reason=None phys=CONFIRMED
- `23:16:16`   ZAF  EXPANSION  sym=^J203.JO     latest=2026-09-01 stale=0mo cli=102.87 3m=2.953 reason=None phys=UNCONFIRMED
- `23:16:16` physical_confirmation: {"countries_with_ports": 11, "counts": {"CONFIRMED": 8, "DIVERGENT": 2, "UNCONFIRMED": 24}, "error": null}
## S2 fleet witness: price-redundancy yahoo_success_rate

## S3 lambda config / rule / metrics / logs

- `23:16:17`   Invocations 14d: 08-18=1, 08-19=1, 08-20=1, 08-21=1, 08-22=1, 08-23=1, 08-24=1, 08-25=1, 08-26=1, 08-27=1, 08-28=1, 08-29=1, 08-30=1, 08-31=1
- `23:16:17`   Errors      14d: 08-18=0, 08-19=0, 08-20=0, 08-21=0, 08-22=0, 08-23=0, 08-24=0, 08-25=0, 08-26=0, 08-27=0, 08-28=0, 08-29=0, 08-30=0, 08-31=0
- `23:16:18`   Throttles   14d: 08-18=0, 08-19=0, 08-20=0, 08-21=0, 08-22=0, 08-23=0, 08-24=0, 08-25=0, 08-26=0, 08-27=0, 08-28=0, 08-29=0, 08-30=0, 08-31=0
- `23:16:18`   Duration    14d: 08-18=99521, 08-19=93625, 08-20=107105, 08-21=39305, 08-22=40059, 08-23=110028, 08-24=55087, 08-25=30666, 08-26=39906, 08-27=77931, 08-28=38529, 08-29=33932, 08-30=51504, 08-31=74052
- `23:16:19`   LOG ── stream f5674572afcb77f0b1ea4c20 (last event 2026-09-01T12:01:35+00:00) ──
- `23:16:19`   LOG INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `23:16:19`   LOG START RequestId: 17563a88-78ad-4a74-a2da-67e408ec6853 Version: $LATEST
- `23:16:19`   LOG [gbc] v2.0 start, 34 countries (equity-momentum-based)
- `23:16:19`   LOG [gbc] PX.PR failed: HTTP 404
- `23:16:19`   LOG [gbc] ^PX failed: no usable bars
- `23:16:19`   LOG [gbc] PX.PR fell back to PXTR.PR
- `23:16:19`   LOG [gbc] BUX.BD failed: HTTP 404
- `23:16:19`   LOG [gbc] ^BUX failed: no usable bars
- `23:16:19`   LOG [gbc] BUX.BD fell back to OTP.BD
- `23:16:19`   LOG [physical] 11 countries · {'CONFIRMED': 8, 'DIVERGENT': 2, 'UNCONFIRMED': 24}
- `23:16:19`   LOG [gbc-history] computing weekly history for 34 countries
- `23:16:19`   LOG [gbc-history] USA 201 weekly points (2022-09-01 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] CHN 192 weekly points (2022-09-16 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] JPN 194 weekly points (2022-09-12 → 2026-08-25)
- `23:16:19`   LOG [gbc-history] DEU 205 weekly points (2022-08-25 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] IND 197 weekly points (2022-09-07 → 2026-09-01)
- `23:16:19`   LOG [gbc-history] GBR 202 weekly points (2022-09-01 → 2026-08-25)
- `23:16:19`   LOG [gbc-history] FRA 206 weekly points (2022-08-23 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] ITA 204 weekly points (2022-08-26 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] CAN 201 weekly points (2022-09-01 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] BRA 199 weekly points (2022-09-02 → 2026-08-24)
- `23:16:19`   LOG [gbc-history] KOR 194 weekly points (2022-09-15 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] AUS 203 weekly points (2022-08-31 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] ESP 206 weekly points (2022-08-25 → 2026-09-01)
- `23:16:19`   LOG [gbc-history] MEX 201 weekly points (2022-08-29 → 2026-08-24)
- `23:16:19`   LOG [gbc-history] IDN 190 weekly points (2022-09-12 → 2026-09-01)
- `23:16:19`   LOG [gbc-history] NLD 206 weekly points (2022-08-23 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] TUR 200 weekly points (2022-09-01 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] CHE 201 weekly points (2022-08-30 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] POL 201 weekly points (2022-09-01 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] BEL 206 weekly points (2022-08-23 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] SWE 201 weekly points (2022-08-31 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] IRL 204 weekly points (2022-08-29 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] AUT 204 weekly points (2022-08-29 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] NOR 195 weekly points (2022-08-31 → 2026-07-17)
- `23:16:19`   LOG [gbc-history] ZAF 200 weekly points (2022-09-05 → 2026-09-01)
- `23:16:19`   LOG [gbc-history] DNK 193 weekly points (2022-09-13 → 2026-08-25)
- `23:16:19`   LOG [gbc-history] FIN 201 weekly points (2022-08-31 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] HUN 199 weekly points (2022-08-31 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] CHL 201 weekly points (2022-09-01 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] PRT 206 weekly points (2022-08-23 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] GRC 192 weekly points (2022-09-06 → 2026-07-15)
- `23:16:19`   LOG [gbc-history] NZL 200 weekly points (2022-09-04 → 2026-09-01)
- `23:16:19`   LOG [gbc-history] ISR 152 weekly points (2022-12-21 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] aggregate has 1090 dates (2022-08-23 → 2026-09-01)
- `23:16:19`   LOG [gbc-history] detected 22 confirmed transitions
- `23:16:19`   LOG [gbc-history]   2022-08-26 GLOBAL_EXPANSION → GLOBAL_CONTRACTION CLI 96.55 · persisted 11w
- `23:16:19`   LOG [gbc-history]   2022-09-09 GLOBAL_CONTRACTION → GLOBAL_EXPANSION CLI 97.78 · persisted 12w
- `23:16:19`   LOG [gbc-history]   2022-09-23 GLOBAL_EXPANSION → GLOBAL_CONTRACTION CLI 96.07 · persisted 46w
- `23:16:19`   LOG [gbc-history]   2022-11-28 GLOBAL_CONTRACTION → MIXED CLI 99.22 · persisted 5w
- `23:16:19`   LOG [gbc-history]   2022-12-05 MIXED → GLOBAL_EXPANSION CLI 99.91 · persisted 71w
- `23:16:19`   LOG [gbc-history]   2023-03-14 GLOBAL_EXPANSION → MIXED CLI 100.18 · persisted 8w
- `23:16:19`   LOG [gbc-history]   2023-03-24 MIXED → GLOBAL_EXPANSION CLI 99.79 · persisted 53w
- `23:16:19`   LOG [gbc-history]   2023-09-29 GLOBAL_EXPANSION → GLOBAL_CONTRACTION CLI 101.68 · persisted 35w
- `23:16:19`   LOG [gbc-history]   2023-11-17 GLOBAL_CONTRACTION → GLOBAL_EXPANSION CLI 102.1 · persisted 166w
- `23:16:19`   LOG [gbc-history]   2024-06-27 GLOBAL_EXPANSION → MIXED CLI 103.22 · persisted 6w
- `23:16:19`   LOG [gbc-history]   2024-07-05 MIXED → GLOBAL_EXPANSION CLI 103.67 · persisted 21w
- `23:16:19`   LOG [gbc-history]   2024-08-05 GLOBAL_EXPANSION → MIXED CLI 101.43 · persisted 18w
- `23:16:19`   LOG [gbc-history]   2024-08-29 MIXED → GLOBAL_EXPANSION CLI 102.93 · persisted 8w
- `23:16:19`   LOG [gbc-history]   2024-09-10 GLOBAL_EXPANSION → MIXED CLI 101.86 · persisted 5w
- `23:16:19`   LOG [gbc-history]   2024-09-17 MIXED → GLOBAL_EXPANSION CLI 102.49 · persisted 15w
- `23:16:19`   LOG [gbc-history]   2025-03-05 GLOBAL_EXPANSION → MIXED CLI 103.04 · persisted 17w
- `23:16:19`   LOG [gbc-history]   2025-04-07 MIXED → GLOBAL_CONTRACTION CLI 98.02 · persisted 7w
- `23:16:19`   LOG [gbc-history]   2025-04-16 GLOBAL_CONTRACTION → MIXED CLI 99.71 · persisted 10w
- `23:16:19`   LOG [gbc-history]   2025-05-13 MIXED → GLOBAL_PEAKING CLI 102.98 · persisted 11w
- `23:16:19`   LOG [gbc-history]   2025-06-03 GLOBAL_PEAKING → GLOBAL_EXPANSION CLI 102.86 · persisted 220w
- `23:16:19`   LOG [gbc-history]   2026-03-16 GLOBAL_EXPANSION → MIXED CLI 104.58 · persisted 17w
- `23:16:19`   LOG [gbc-history]   2026-04-14 MIXED → GLOBAL_EXPANSION CLI 106.47 · persisted 36w
- `23:16:19`   LOG [gbc-history] computed lead/lag for 33 countries in 1.10s
- `23:16:19`   LOG [gbc-history]   #1 IDN lag=+26w corr=0.171
- `23:16:19`   LOG [gbc-history]   #2 MEX lag=+4w corr=0.55
- `23:16:19`   LOG [gbc-history]   #3 BRA lag=+3w corr=0.462
- `23:16:19`   LOG [gbc-history]   #4 CAN lag=+1w corr=0.87
- `23:16:19`   LOG [gbc-history]   #5 ESP lag=+1w corr=0.823
- `23:16:19`   LOG [gbc-history]   #6 POL lag=+1w corr=0.705
- `23:16:19`   LOG [gbc-history]   #7 IRL lag=+1w corr=0.682
- `23:16:19`   LOG [gbc-history]   #8 AUS lag=+1w corr=0.591
- `23:16:19`   LOG [gbc-history]   #9 DEU lag=+1w corr=0.568
- `23:16:19`   LOG [gbc-history]   #10 FRA lag=+1w corr=0.363
- `23:16:19`   LOG [gbc-history] computed phase-conditional returns for 33 countries in 0.03s
- `23:16:19`   LOG [gbc-history]   EXPANSION: top → KOR mean 12.77% hit 0.726 n=106
- `23:16:19`   LOG [gbc-history]   AT_RISK: top → TUR mean 18.35% hit 0.829 n=35
- `23:16:19`   LOG [gbc-history]   RECESSION: top → POL mean 32.52% hit 1.0 n=20
- `23:16:19`   LOG [gbc-history]   RECOVERY: top → GRC mean 13.71% hit 1.0 n=5
- `23:16:19`   LOG ── stream 2d9043ec9cecae5f9ce1ebbd (last event 2026-08-31T12:01:12+00:00) ──
- `23:16:19`   LOG INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `23:16:19`   LOG START RequestId: cb51d565-5d95-4a10-a06b-2b2ddc4d8a21 Version: $LATEST
- `23:16:19`   LOG [gbc] v2.0 start, 34 countries (equity-momentum-based)
- `23:16:19`   LOG [gbc] ^OSEAX failed: HTTP 404
- `23:16:19`   LOG [gbc] PX.PR failed: HTTP 404
- `23:16:19`   LOG [gbc] ^PX failed: no usable bars
- `23:16:19`   LOG [gbc] PX.PR fell back to PXTR.PR
- `23:16:19`   LOG [gbc] BUX.BD failed: HTTP 404
- `23:16:19`   LOG [gbc] ^BUX failed: no usable bars
- `23:16:19`   LOG [gbc] BUX.BD fell back to OTP.BD
- `23:16:19`   LOG [physical] 11 countries · {'CONFIRMED': 8, 'DIVERGENT': 2, 'UNCONFIRMED': 24}
- `23:16:19`   LOG [gbc-history] computing weekly history for 34 countries
- `23:16:19`   LOG [gbc-history] USA 201 weekly points (2022-08-30 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] CHN 192 weekly points (2022-09-15 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] JPN 195 weekly points (2022-09-09 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] DEU 205 weekly points (2022-08-24 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] IND 197 weekly points (2022-09-06 → 2026-08-28)
- `23:16:19`   LOG [gbc-history] GBR 202 weekly points (2022-08-31 → 2026-08-24)
- `23:16:19`   LOG [gbc-history] FRA 206 weekly points (2022-08-22 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] ITA 204 weekly points (2022-08-25 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] CAN 201 weekly points (2022-08-31 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] BRA 200 weekly points (2022-09-01 → 2026-08-28)
- `23:16:19`   LOG [gbc-history] KOR 194 weekly points (2022-09-14 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] AUS 203 weekly points (2022-08-30 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] ESP 206 weekly points (2022-08-24 → 2026-08-28)
- `23:16:19`   LOG [gbc-history] MEX 202 weekly points (2022-08-26 → 2026-08-28)
- `23:16:19`   LOG [gbc-history] IDN 190 weekly points (2022-09-09 → 2026-08-28)
- `23:16:19`   LOG [gbc-history] NLD 206 weekly points (2022-08-22 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] TUR 200 weekly points (2022-08-31 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] CHE 201 weekly points (2022-08-29 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] POL 201 weekly points (2022-08-30 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] BEL 206 weekly points (2022-08-22 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] SWE 201 weekly points (2022-08-30 → 2026-08-25)
- `23:16:19`   LOG [gbc-history] IRL 204 weekly points (2022-08-26 → 2026-08-25)
- `23:16:19`   LOG [gbc-history] AUT 204 weekly points (2022-08-26 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] ZAF 200 weekly points (2022-09-02 → 2026-08-28)
- `23:16:19`   LOG [gbc-history] DNK 194 weekly points (2022-09-09 → 2026-08-28)
- `23:16:19`   LOG [gbc-history] FIN 201 weekly points (2022-08-30 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] HUN 199 weekly points (2022-08-30 → 2026-08-25)
- `23:16:19`   LOG [gbc-history] CHL 201 weekly points (2022-08-30 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] PRT 206 weekly points (2022-08-22 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] NZL 200 weekly points (2022-09-01 → 2026-08-27)
- `23:16:19`   LOG [gbc-history] ISR 152 weekly points (2022-12-20 → 2026-08-26)
- `23:16:19`   LOG [gbc-history] aggregate has 1083 dates (2022-08-22 → 2026-08-31)
- `23:16:19`   LOG [gbc-history] detected 25 confirmed transitions
- `23:16:19`   LOG [gbc-history]   2022-08-25 GLOBAL_EXPANSION → GLOBAL_CONTRACTION CLI 96.99 · persisted 14w
- `23:16:19`   LOG [gbc-history]   2022-09-14 GLOBAL_CONTRACTION → GLOBAL_EXPANSION CLI 98.33 · persisted 8w
- `23:16:19`   LOG [gbc-history]   2022-09-28 GLOBAL_EXPANSION → GLOBAL_CONTRACTION CLI 95.96 · persisted 45w
- `23:16:19`   LOG [gbc-history]   2022-12-01 GLOBAL_CONTRACTION → GLOBAL_EXPANSION CLI 100.09 · persisted 5w
- `23:16:19`   LOG [gbc-history]   2023-03-15 GLOBAL_EXPANSION → MIXED CLI 100.01 · persisted 5w
- `23:16:19`   LOG [gbc-history]   2023-03-22 MIXED → GLOBAL_EXPANSION CLI 99.62 · persisted 31w
- `23:16:19`   LOG [gbc-history]   2023-05-30 GLOBAL_EXPANSION → MIXED CLI 101.81 · persisted 11w
- `23:16:19`   LOG [gbc-history]   2023-06-13 MIXED → GLOBAL_EXPANSION CLI 102.84 · persisted 52w
- `23:16:19`   LOG [gbc-history]   2023-09-07 GLOBAL_EXPANSION → MIXED CLI 102.34 · persisted 14w
- `23:16:19`   LOG [gbc-history]   2023-09-27 MIXED → GLOBAL_CONTRACTION CLI 102.09 · persisted 42w
- `23:16:19`   LOG [gbc-history]   2023-11-15 GLOBAL_CONTRACTION → GLOBAL_EXPANSION CLI 101.8 · persisted 165w
- `23:16:19`   LOG [gbc-history]   2024-06-26 GLOBAL_EXPANSION → MIXED CLI 103.13 · persisted 5w
- `23:16:19`   LOG [gbc-history]   2024-07-03 MIXED → GLOBAL_EXPANSION CLI 103.21 · persisted 23w
- `23:16:19`   LOG [gbc-history]   2024-08-12 GLOBAL_EXPANSION → MIXED CLI 100.49 · persisted 12w
- `23:16:19`   LOG [gbc-history]   2024-08-28 MIXED → GLOBAL_EXPANSION CLI 103.01 · persisted 8w
- `23:16:19`   LOG [gbc-history]   2025-02-27 GLOBAL_EXPANSION → MIXED CLI 103.63 · persisted 15w
- `23:16:19`   LOG [gbc-history]   2025-03-20 MIXED → GLOBAL_CONTRACTION CLI 102.0 · persisted 5w
- `23:16:19`   LOG [gbc-history]   2025-03-27 GLOBAL_CONTRACTION → GLOBAL_PEAKING CLI 102.08 · persisted 5w
- `23:16:19`   LOG [gbc-history]   2025-04-03 GLOBAL_PEAKING → GLOBAL_CONTRACTION CLI 100.6 · persisted 14w
- `23:16:19`   LOG [gbc-history]   2025-04-23 GLOBAL_CONTRACTION → MIXED CLI 99.86 · persisted 7w
- `23:16:19`   LOG [gbc-history]   2025-05-02 MIXED → GLOBAL_PEAKING CLI 101.67 · persisted 5w
- `23:16:19`   LOG [gbc-history]   2025-06-02 GLOBAL_PEAKING → GLOBAL_EXPANSION CLI 102.84 · persisted 208w
- `23:16:19`   LOG [gbc-history]   2026-03-12 GLOBAL_EXPANSION → MIXED CLI 104.7 · persisted 7w
- `23:16:19`   LOG [gbc-history]   2026-03-20 MIXED → GLOBAL_PEAKING CLI 103.72 · persisted 22w
- `23:16:19`   LOG [gbc-history]   2026-04-17 GLOBAL_PEAKING → GLOBAL_EXPANSION CLI 107.1 · persisted 32w
- `23:16:19`   LOG [gbc-history] computed lead/lag for 31 countries in 1.06s
- `23:16:19`   LOG [gbc-history]   #1 IDN lag=+26w corr=0.155
- `23:16:19`   LOG [gbc-history]   #2 MEX lag=+4w corr=0.57
- `23:16:19`   LOG [gbc-history]   #3 BRA lag=+3w corr=0.461
- `23:16:19`   LOG [gbc-history]   #4 POL lag=+2w corr=0.685
- `23:16:19`   LOG [gbc-history]   #5 AUS lag=+2w corr=0.595
- `23:16:19`   LOG [gbc-history]   #6 PRT lag=+2w corr=0.576
- `23:16:19`   LOG [gbc-history]   #7 ESP lag=+1w corr=0.831
- `23:16:19`   LOG [gbc-history]   #8 BEL lag=+1w corr=0.807
- `23:16:19`   LOG [gbc-history]   #9 IRL lag=+1w corr=0.69
- `23:16:19`   LOG [gbc-history]   #10 FIN lag=+1w corr=0.669
- `23:16:19`   LOG [gbc-history] computed phase-conditional returns for 31 countries in 0.03s
- `23:16:19`   LOG [gbc-history]   EXPANSION: top → KOR mean 12.11% hit 0.703 n=111
- `23:16:19`   LOG [gbc-history]   AT_RISK: top → TUR mean 20.2% hit 0.824 n=34
- `23:16:19`   LOG [gbc-history]   RECESSION: top → POL mean 31.15% hit 1.0 n=20
- `23:16:19`   LOG [gbc-history]   RECOVERY: top → ITA mean 14.65% hit 1.0 n=8
## S4 edge probes from the runner

## S5 source probes from the runner (34 countries)

- `23:16:20` FMP key inherited from justhodl-equity-research: yes
- `23:17:11` tally (>=250 bars): {"yahoo_engine_ua": 32, "yahoo_browser_ua": 0, "fmp": 0, "stooq": 0}
- `23:17:11` stooq verified ids: {}
- `23:17:11` polygon-full grouped sessions this year: 166, latest data/warm/polygon-full/grouped/2026/2026-08-31.json.gz
- `23:17:11` ETF proxies present in latest session: 31/31; missing: none
- `23:17:11` latest session doc keys: ['queryCount', 'resultsCount', 'adjusted', 'results', 'status', 'request_id', 'count'] rows=12578
## S6 async engine run, verified on disk

- `23:17:11` async invoke fired at 2026-09-01T23:17:11+00:00 (prev generated_at=2026-09-01T12:01:33.582946+00:00)
## S6 log tail of this run

- `23:17:42`   LOG ── stream 8b2f4a3c9aeb15a2bbe3a03e (last event 2026-09-01T23:17:15+00:00) ──
- `23:17:42`   LOG INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `23:17:42`   LOG START RequestId: 5643a98b-8a54-44bf-90f7-b1d3d77b5809 Version: $LATEST
- `23:17:42`   LOG [gbc] v2.0 start, 34 countries (equity-momentum-based)
- `23:17:42`   LOG [gbc] USA  EXPANSION  CLI=120 3m=0.415 latest=2026-09-01 (0mo)
- `23:17:42`   LOG [gbc] CHN  AT_RISK    CLI=101.19 3m=-1.761 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] JPN  EXPANSION  CLI=111.48 3m=-0.026 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] DEU  EXPANSION  CLI=103.32 3m=4.513 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] IND  RECOVERY   CLI=98.91 3m=3.622 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] GBR  EXPANSION  CLI=104.09 3m=4.386 latest=2026-08-27 (1mo)
- `23:17:42`   LOG [gbc] FRA  EXPANSION  CLI=101.77 3m=1.528 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] ITA  EXPANSION  CLI=106.13 3m=4.021 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] CAN  EXPANSION  CLI=105.81 3m=3.14 latest=2026-09-01 (0mo)
- `23:17:42`   LOG [gbc] BRA  EXPANSION  CLI=105.91 3m=3.171 latest=2026-09-01 (0mo)
- `23:17:42`   LOG [gbc] KOR  AT_RISK    CLI=120 3m=-16.68 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] AUS  EXPANSION  CLI=101.27 3m=3.97 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] ESP  EXPANSION  CLI=108.8 3m=9.315 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] MEX  AT_RISK    CLI=100.15 3m=-5.523 latest=2026-09-01 (0mo)
- `23:17:42`   LOG [gbc] IDN  RECOVERY   CLI=97.38 3m=5.142 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] NLD  EXPANSION  CLI=106.12 3m=5.406 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] TUR  EXPANSION  CLI=106.43 3m=4.598 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] CHE  EXPANSION  CLI=104.68 3m=7.371 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] POL  EXPANSION  CLI=110.73 3m=10.126 latest=2026-09-01 (0mo)
- `23:17:42`   LOG [gbc] BEL  EXPANSION  CLI=105.94 3m=5.13 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] SWE  EXPANSION  CLI=106.36 3m=6.937 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] IRL  EXPANSION  CLI=106.84 3m=7.932 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] AUT  EXPANSION  CLI=111.86 3m=10.232 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] NOR  AT_RISK    CLI=104.78 3m=-2.6 latest=2026-07-17 (2mo)
- `23:17:42`   LOG [gbc] ZAF  EXPANSION  CLI=103.02 3m=1.418 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] DNK  EXPANSION  CLI=104.02 3m=6.304 latest=2026-09-01 (0mo)
- `23:17:42`   LOG [gbc] FIN  EXPANSION  CLI=106.25 3m=-0.32 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] PX.PR failed: HTTP 404
- `23:17:42`   LOG [gbc] ^PX failed: no usable bars
- `23:17:42`   LOG [gbc] PX.PR fell back to PXTR.PR
- `23:17:42`   LOG [gbc] CZE  UNKNOWN    CLI=None 3m=None latest=None (999mo)
- `23:17:42`   LOG [gbc] BUX.BD failed: HTTP 404
- `23:17:42`   LOG [gbc] ^BUX failed: no usable bars
- `23:17:42`   LOG [gbc] BUX.BD fell back to OTP.BD
- `23:17:42`   LOG [gbc] HUN  EXPANSION  CLI=111.76 3m=10.557 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] CHL  EXPANSION  CLI=104.8 3m=-0.054 latest=2026-09-01 (0mo)
- `23:17:42`   LOG [gbc] PRT  EXPANSION  CLI=105.65 3m=5.344 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [gbc] GRC  EXPANSION  CLI=106.32 3m=5.983 latest=2026-07-17 (2mo)
- `23:17:42`   LOG [gbc] NZL  EXPANSION  CLI=102.25 3m=4.681 latest=2026-09-01 (0mo)
- `23:17:42`   LOG [gbc] ISR  AT_RISK    CLI=104.85 3m=-4.312 latest=2026-08-31 (1mo)
- `23:17:42`   LOG [physical] 0 countries · {'CONFIRMED': 0, 'DIVERGENT': 0, 'UNCONFIRMED': 34}
- `23:17:42`   LOG [gbc-history] computing weekly history for 34 countries
- `23:17:42`   LOG [gbc-history] USA 201 weekly points (2022-09-02 → 2026-09-01)
- `23:17:42`   LOG [gbc-history] CHN 192 weekly points (2022-09-16 → 2026-08-27)
- `23:17:42`   LOG [gbc-history] JPN 194 weekly points (2022-09-12 → 2026-08-25)
- `23:17:42`   LOG [gbc-history] DEU 205 weekly points (2022-08-25 → 2026-08-31)
- `23:17:42`   LOG [gbc-history] IND 196 weekly points (2022-09-07 → 2026-08-24)
- `23:17:42`   LOG [gbc-history] GBR 202 weekly points (2022-09-01 → 2026-08-25)
- `23:17:42`   LOG [gbc-history] FRA 206 weekly points (2022-08-23 → 2026-08-27)
- `23:17:42`   LOG [gbc-history] ITA 204 weekly points (2022-08-26 → 2026-08-27)
- `23:17:42`   LOG [gbc-history] CAN 201 weekly points (2022-09-02 → 2026-08-31)
- `23:17:42`   LOG [gbc-history] BRA 199 weekly points (2022-09-05 → 2026-08-25)
- `23:17:42`   LOG [gbc-history] KOR 194 weekly points (2022-09-15 → 2026-08-31)
- `23:17:42`   LOG [gbc-history] AUS 203 weekly points (2022-08-31 → 2026-08-31)
- `23:17:42`   LOG [gbc-history] ESP 205 weekly points (2022-08-25 → 2026-08-24)
- `23:17:42`   LOG [gbc-history] MEX 201 weekly points (2022-08-30 → 2026-08-25)
- `23:17:42`   LOG [gbc-history] IDN 189 weekly points (2022-09-12 → 2026-08-21)
- `23:17:42`   LOG [gbc-history] NLD 206 weekly points (2022-08-23 → 2026-08-27)
- `23:17:42`   LOG [gbc-history] TUR 200 weekly points (2022-09-01 → 2026-08-27)
- `23:17:42`   LOG [gbc-history] CHE 201 weekly points (2022-08-30 → 2026-08-27)
- `23:17:42`   LOG [gbc-history] POL 201 weekly points (2022-09-02 → 2026-09-01)
- `23:17:42`   LOG [gbc-history] BEL 206 weekly points (2022-08-23 → 2026-08-27)
- `23:17:42`   LOG [gbc-history] SWE 201 weekly points (2022-08-31 → 2026-08-26)
- `23:17:42`   LOG [gbc-history] IRL 204 weekly points (2022-08-29 → 2026-08-26)
- `23:17:42`   LOG [gbc-history] AUT 204 weekly points (2022-08-29 → 2026-08-27)
- `23:17:42`   LOG [gbc-history] NOR 195 weekly points (2022-08-31 → 2026-07-17)
- `23:17:42`   LOG [gbc-history] ZAF 199 weekly points (2022-09-05 → 2026-08-24)
- `23:17:42`   LOG [gbc-history] DNK 193 weekly points (2022-09-14 → 2026-08-26)
- `23:17:42`   LOG [gbc-history] FIN 201 weekly points (2022-08-31 → 2026-08-31)
- `23:17:42`   LOG [gbc-history] HUN 199 weekly points (2022-08-31 → 2026-08-26)
- `23:17:42`   LOG [gbc-history] CHL 201 weekly points (2022-09-02 → 2026-09-01)
- `23:17:42`   LOG [gbc-history] PRT 206 weekly points (2022-08-23 → 2026-08-31)
- `23:17:42`   LOG [gbc-history] GRC 192 weekly points (2022-09-06 → 2026-07-15)
- `23:17:42`   LOG [gbc-history] NZL 199 weekly points (2022-09-05 → 2026-08-24)
- `23:17:42`   LOG [gbc-history] ISR 152 weekly points (2022-12-21 → 2026-08-27)
- `23:17:42`   LOG [gbc-history] aggregate has 1097 dates (2022-08-23 → 2026-09-01)
- `23:17:42`   LOG [gbc-history] detected 26 confirmed transitions
- `23:17:42`   LOG [gbc-history]   2022-08-26 GLOBAL_EXPANSION → GLOBAL_CONTRACTION CLI 96.55 · persisted 11w
- `23:17:42`   LOG [gbc-history]   2022-09-12 GLOBAL_CONTRACTION → GLOBAL_EXPANSION CLI 98.7 · persisted 10w
- `23:17:42`   LOG [gbc-history]   2022-09-26 GLOBAL_EXPANSION → GLOBAL_CONTRACTION CLI 95.84 · persisted 46w
- `23:17:42`   LOG [gbc-history]   2022-11-29 GLOBAL_CONTRACTION → GLOBAL_EXPANSION CLI 99.14 · persisted 63w
- `23:17:42`   LOG [gbc-history]   2023-02-27 GLOBAL_EXPANSION → MIXED CLI 100.85 · persisted 5w
- `23:17:42`   LOG [gbc-history]   2023-03-06 MIXED → GLOBAL_EXPANSION CLI 101.05 · persisted 6w
- `23:17:42`   LOG [gbc-history]   2023-05-09 GLOBAL_EXPANSION → GLOBAL_PEAKING CLI 101.74 · persisted 5w
- `23:17:42`   LOG [gbc-history]   2023-05-16 GLOBAL_PEAKING → GLOBAL_EXPANSION CLI 102.01 · persisted 8w
- `23:17:42`   LOG [gbc-history]   2023-05-31 GLOBAL_EXPANSION → MIXED CLI 101.58 · persisted 5w
- `23:17:42`   LOG [gbc-history]   2023-06-07 MIXED → GLOBAL_EXPANSION CLI 102.21 · persisted 52w
- `23:17:42`   LOG [gbc-history]   2023-10-02 GLOBAL_EXPANSION → GLOBAL_CONTRACTION CLI 101.69 · persisted 30w
- `23:17:42`   LOG [gbc-history]   2023-11-16 GLOBAL_CONTRACTION → GLOBAL_EXPANSION CLI 101.53 · persisted 164w
- `23:17:42`   LOG [gbc-history]   2024-06-21 GLOBAL_EXPANSION → MIXED CLI 103.16 · persisted 9w
- `23:17:42`   LOG [gbc-history]   2024-07-04 MIXED → GLOBAL_EXPANSION CLI 103.49 · persisted 22w
- `23:17:42`   LOG [gbc-history]   2024-08-05 GLOBAL_EXPANSION → MIXED CLI 101.12 · persisted 18w
- `23:17:42`   LOG [gbc-history]   2024-08-29 MIXED → GLOBAL_EXPANSION CLI 103.2 · persisted 8w
- `23:17:42`   LOG [gbc-history]   2024-09-10 GLOBAL_EXPANSION → MIXED CLI 102.05 · persisted 5w
- `23:17:42`   LOG [gbc-history]   2024-09-17 MIXED → GLOBAL_EXPANSION CLI 102.41 · persisted 15w
- `23:17:42`   LOG [gbc-history]   2025-03-04 GLOBAL_EXPANSION → MIXED CLI 102.89 · persisted 21w
- `23:17:42`   LOG [gbc-history]   2025-03-28 MIXED → GLOBAL_PEAKING CLI 102.28 · persisted 8w
- `23:17:42`   LOG [gbc-history]   2025-04-07 GLOBAL_PEAKING → GLOBAL_CONTRACTION CLI 99.5 · persisted 8w
- `23:17:42`   LOG [gbc-history]   2025-04-16 GLOBAL_CONTRACTION → MIXED CLI 99.75 · persisted 10w
- `23:17:42`   LOG [gbc-history]   2025-05-07 MIXED → GLOBAL_PEAKING CLI 101.97 · persisted 15w
- `23:17:42`   LOG [gbc-history]   2025-05-29 GLOBAL_PEAKING → GLOBAL_EXPANSION CLI 103.0 · persisted 227w
- `23:17:42`   LOG [gbc-history]   2026-03-12 GLOBAL_EXPANSION → MIXED CLI 104.74 · persisted 15w
- `23:17:42`   LOG [gbc-history]   2026-04-15 MIXED → GLOBAL_EXPANSION CLI 106.62 · persisted 35w
- `23:17:42`   LOG [gbc-history] computed lead/lag for 33 countries in 1.12s
- `23:17:42`   LOG [gbc-history]   #1 IDN lag=+26w corr=0.18
- `23:17:42`   LOG [gbc-history]   #2 MEX lag=+3w corr=0.55
- `23:17:42`   LOG [gbc-history]   #3 BRA lag=+3w corr=0.458
- `23:17:42`   LOG [gbc-history]   #4 IRL lag=+1w corr=0.669
- `23:17:42`   LOG [gbc-history]   #5 NZL lag=+1w corr=0.668
- `23:17:42`   LOG [gbc-history]   #6 AUS lag=+1w corr=0.592
- `23:17:42`   LOG [gbc-history]   #7 PRT lag=+1w corr=0.569
- `23:17:42`   LOG [gbc-history]   #8 DEU lag=+1w corr=0.548
- `23:17:42`   LOG [gbc-history]   #9 FRA lag=+1w corr=0.344
- `23:17:42`   LOG [gbc-history]   #10 CAN lag=+0w corr=0.864
- `23:17:42`   LOG [gbc-history] computed phase-conditional returns for 33 countries in 0.03s
- `23:17:42`   LOG [gbc-history]   EXPANSION: top → KOR mean 12.77% hit 0.726 n=106
- `23:17:42`   LOG [gbc-history]   AT_RISK: top → TUR mean 18.35% hit 0.829 n=35
- `23:17:42`   LOG [gbc-history]   RECESSION: top → POL mean 31.69% hit 1.0 n=20
- `23:17:42`   LOG [gbc-history]   RECOVERY: top → GRC mean 13.71% hit 1.0 n=5
- `23:17:42`   LOG [gbc] done. global_phase=GLOBAL_EXPANSION avg_cli=108.79 fresh=33/34
- `23:17:42`   LOG END RequestId: 5643a98b-8a54-44bf-90f7-b1d3d77b5809
- `23:17:42`   LOG REPORT RequestId: 5643a98b-8a54-44bf-90f7-b1d3d77b5809	Duration: 21319.49 ms	Billed Duration: 21870 ms	Memory Size: 1536 MB	Max Memory Used: 113 MB	Init Duration: 549.97 ms	
XRAY TraceId: 1-6a975cf7-32d67d045b8180994b6d5368	SegmentId: 02d29
- `23:17:42` VERDICT {"live_feed": "FRESH", "live_unknown": 1, "tally": {"yahoo_engine_ua": 32, "yahoo_browser_ua": 0, "fmp": 0, "stooq": 0}, "fresh_with_data": 33}
- `23:17:42` ✅ engine produced a fresh feed with 33/34 countries
