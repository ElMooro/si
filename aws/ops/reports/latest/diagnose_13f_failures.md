# Why did 14 of 18 funds fail?

**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-05-03T16:52:57+00:00  

## Log
## Output summary

- `16:52:57`   generated_at: 2026-05-03T16:52:18+00:00
- `16:52:57`   funds_parsed: 3
- `16:52:57`   funds_failed: 14
- `16:52:57`   funds OK: ['BRIDGEWATER', 'LONE_PINE', 'DURATION']
## Fund errors (per-fund)

- `16:52:57`   BERKSHIRE: parse_returned_empty
- `16:52:57`   TWO_SIGMA: parse_returned_empty
- `16:52:57`   RENAISSANCE: parse_returned_empty
- `16:52:57`   AQR: parse_returned_empty
- `16:52:57`   PERSHING: parse_returned_empty
- `16:52:57`   CITADEL: parse_returned_empty
- `16:52:57`   MILLENNIUM: infotable_not_found
- `16:52:57`   GREENLIGHT: parse_returned_empty
- `16:52:57`   TIGER_GLOBAL: parse_returned_empty
- `16:52:57`   SOROS: parse_returned_empty
- `16:52:57`   COATUE: parse_returned_empty
- `16:52:57`   SCION: parse_returned_empty
- `16:52:57`   BAUPOST: parse_returned_empty
- `16:52:57`   POINT72: parse_returned_empty
## Sample one failing fund's filing

- `16:52:57` 
  ── BERKSHIRE (parse_returned_empty) ──
- `16:52:57`   filing dir: https://www.sec.gov/Archives/edgar/data/1067983/000119312526054580/
- `16:52:57`   xml files: ['50240.xml', 'primary_doc.xml']
- `16:52:57`     50240.xml: has_infoTable=True, size=55376
- `16:52:57`     primary_doc.xml: has_infoTable=False, size=5556
- `16:52:57`       preview: <?xml version="1.0" encoding="UTF-8"?> <edgarSubmission xsi:schemaLocation="http://www.sec.gov/edgar/thirteenffiler eis_13F_Filer.xsd" xmlns:ns1="http://www.sec.gov/edgar/common" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.sec.gov/edgar/thirteenffiler">   <schemaVersion>X
- `16:52:57` 
  ── TWO_SIGMA (parse_returned_empty) ──
- `16:52:57`   filing dir: https://www.sec.gov/Archives/edgar/data/1179392/000089914026000232/
- `16:52:57`   xml files: ['informationtable.xml', 'primary_doc.xml']
- `16:52:57`     informationtable.xml: has_infoTable=True, size=2303385
- `16:52:57`     primary_doc.xml: has_infoTable=False, size=5658
- `16:52:57`       preview: <?xml version="1.0" encoding="UTF-8"?> <edgarSubmission xmlns:com="http://www.sec.gov/edgar/common" xmlns="http://www.sec.gov/edgar/thirteenffiler">   <schemaVersion>X0202</schemaVersion>   <headerData>     <submissionType>13F-HR</submissionType>     <filerInfo>       <liveTestFlag>LIVE</liveTestFla
- `16:52:57` 
  ── RENAISSANCE (parse_returned_empty) ──
- `16:52:57`   filing dir: https://www.sec.gov/Archives/edgar/data/1037389/000103738926000023/
- `16:52:57`   xml files: ['primary_doc.xml', 'renaissance13Fq42025_holding.xml']
- `16:52:57`     primary_doc.xml: has_infoTable=False, size=2015
- `16:52:57`       preview: <?xml version="1.0" encoding="UTF-8"?> <edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler" xmlns:com="http://www.sec.gov/edgar/common">   <schemaVersion>X0202</schemaVersion>   <headerData>     <submissionType>13F-HR</submissionType>     <filerInfo>       <liveTestFlag>LIVE</liveTestFla
- `16:52:57`     renaissance13Fq42025_holding.xml: has_infoTable=True, size=1820806
- `16:52:57` 
  ── AQR (parse_returned_empty) ──
- `16:52:57`   filing dir: https://www.sec.gov/Archives/edgar/data/1167557/000108514626000240/
- `16:52:57`   xml files: ['infotable.xml', 'primary_doc.xml']
- `16:52:57`     infotable.xml: has_infoTable=True, size=8666571
- `16:52:57`     primary_doc.xml: has_infoTable=False, size=6312
- `16:52:57`       preview: <?xml version="1.0" encoding="UTF-8"?> <edgarSubmission xsi:schemaLocation="http://www.sec.gov/edgar/thirteenffiler eis_13F_Filer.xsd" xmlns="http://www.sec.gov/edgar/thirteenffiler" xmlns:ns1="http://www.sec.gov/edgar/common" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">   <schemaVersion>X
- `16:52:57` 
  ── PERSHING (parse_returned_empty) ──
- `16:52:57`   filing dir: https://www.sec.gov/Archives/edgar/data/1336528/000117266126001091/
- `16:52:57`   xml files: ['infotable.xml', 'primary_doc.xml']
- `16:52:57`     infotable.xml: has_infoTable=True, size=5536
- `16:52:57`     primary_doc.xml: has_infoTable=False, size=2193
- `16:52:57`       preview: <?xml version="1.0" encoding="UTF-8"?> <edgarSubmission xsi:schemaLocation="http://www.sec.gov/edgar/thirteenffiler eis_13F_Filer.xsd" xmlns="http://www.sec.gov/edgar/thirteenffiler" xmlns:ns1="http://www.sec.gov/edgar/common" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">   <schemaVersion>X
