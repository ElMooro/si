# ops 3734 — resolve ERCOT queue report ID + EIA planned-capacity facets

**Status:** success  
**Duration:** 22.6s  
**Finished:** 2026-07-22T21:08:22+00:00  

## Data

| caiso_sheets | eia_facets | probe_only |
|---|---|---|
| 9 | stateid,sector,entityid,plantid,generatorid,unit,technology,energy_source_code,prime_mover_code,balancing_authority_code,status | true |

## Log
## A — EIA operating-generator-capacity metadata

- `21:08:03` ✅ facets: ['stateid', 'sector', 'entityid', 'plantid', 'generatorid', 'unit', 'technology', 'energy_source_code', 'prime_mover_code', 'balancing_authority_code', 'status']
- `21:08:03`   data cols: ['nameplate-capacity-mw', 'net-summer-capacity-mw', 'net-winter-capacity-mw', 'operating-year-month', 'planned-retirement-year-month', 'planned-derate-year-month', 'planned-derate-summer-cap-mw', 'planned-uprate-year-month', 'planned-uprate-summer-cap-mw', 'county', 'longitude', 'latitude']
- `21:08:05` ✅   facet sector values: [('industrial-chp', 'Industrial CHP'), ('industrial-non-chp', 'Industrial Non-CHP'), ('commercial-chp', 'Commercial CHP'), ('ipp-chp', 'IPP CHP'), ('commercial-non-chp', 'Commercial Non-CHP'), ('ipp-non-chp', 'IPP Non-CHP'), ('electric-utility', 'Electric Utility')]
- `21:08:06` ✅   facet status values: [('OP', 'Operating'), ('OS', 'Out of service and NOT expected to return to service in next calendar year'), ('SB', 'Standby/Backup: available for service but not normally used'), ('OA', 'Out of service but expected to return to service in next calendar year')]
## B — EIA planned capacity sample (real facet)

- `21:08:06`   status facet resolved = status
- `21:08:13` ✅ planned-capacity rows=6 total=4752607
- `21:08:13`   sample row: {"period": "2026-04", "stateid": "AK", "stateName": "Alaska", "sector": "electric-utility", "sectorName": "Electric Utility", "entityid": "63560", "entityName": "Sand Point Generating, LLC", "plantid": "1", "plantName": "Sand Point", "generatorid": "1", "technology": "Petroleum Liquids", "energy_source_code": "DFO", "energy-source-desc": "Disillate Fuel Oil", "prime_mover_code": "IC", "balancing_authority_code": null
## C — ERCOT report catalogue search for queue/GIS

- `21:08:13`   15933 -> Co-located_Battery_Identification_Report_June_2026 (167 docs)
- `21:08:14`   12331 -> DAMSPNP4190_csv (64 docs)
- `21:08:15`   12300: Unterminated string starting at: line 1 column 399998 (char 399997)
## D — CAISO public queue workbook

- `21:08:21` ✅ CAISO xlsx bytes=385058 magic=b'PK'
- `21:08:21`   zip members: ['[trash]/0002.dat', 'docProps/app.xml', '[trash]/0003.dat', '[trash]/0000.dat', 'xl/media/image1.jpg', 'xl/printerSettings/printerSettings1.bin', 'xl/printerSettings/printerSettings2.bin', 'xl/printerSettings/printerSettings3.bin', 'xl/styles.xml', 'xl/theme/theme1.xml', 'xl/workbook.xml', 'xl/worksheets/sheet1.xml']
- `21:08:21` ✅   sheets: ['Grid GenerationQueue', 'Completed Generation Projects', 'Withdrawn Generation Projects', '_xlnm._FilterDatabase', '_xlnm._FilterDatabase', '_xlnm._FilterDatabase', '_xlnm.Print_Titles', '_xlnm.Print_Titles', '_xlnm.Print_Titles']
## E — EPA ECHO air facility detail (permit proxy)

- `21:08:22` ✅ ECHO major-source query rows=2699 qid=257
- `21:08:22` ✅   facility rows=1
- `21:08:22`   keys: ['AIRCity', 'AIRClassification', 'AIRCmsCategoryCode', 'AIRCmsCategoryDesc', 'AIRComplStatus', 'AIRCounty', 'AIRDaysLastEval', 'AIRDaysLastFce', 'AIREPARegion', 'AIREvalCnt', 'AIRFceCnt', 'AIRHpvStatus', 'AIRIDs', 'AIRIndianCntryFlg', 'AIRLastEvalDate', 'AIRLastEvalDateEPA', 'AIRLastEvalDateState', 'AIRLastFceDate', 'AIRLastFceDateEPA', 'AIRLastViolDate', 'AIRMacts', 'AIRMnthsWithHpv']
- `21:08:22`   sample: {"AIRName": "121 REGIONAL DISPOSAL FACILITY", "SourceID": "TX0000004808500209", "AIRStreet": "3820 SAM RAYBURN HWY", "AIRCity": "MELISSA", "AIRState": "TX", "LocalControlRegionCode": null, "AIRZip": "75454", "RegistryID": "110043803578", "AIRCounty": "Collin", "AIREPARegion": "06", "FacFederalAgencyCode": null, "FacFederalAgencyName": nul
## VERDICT

- `21:08:22` ✅ PROBE 2 COMPLETE
