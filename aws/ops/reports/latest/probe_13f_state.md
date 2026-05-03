# Probe 13F state + SEC infotable format

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-05-03T16:38:34+00:00  

## Log
## 1. Current data/institutional-positions.json

- `16:38:34`   generated_at: 2026-05-02T18:45:13+00:00
- `16:38:34`   tracked_funds: 18
- `16:38:34`   filings_seen: 18
- `16:38:34`   new_filings: 0
- `16:38:34`   by_fund keys: ['BERKSHIRE', 'BRIDGEWATER', 'RENAISSANCE', 'AQR', 'TWO_SIGMA', 'CITADEL', 'MILLENNIUM', 'PERSHING', 'GREENLIGHT', 'SOROS', 'TIGER_GLOBAL', 'COATUE', 'BAUPOST', 'ELLIOTT', 'SCION', 'DURATION', 'POINT72', 'LONE_PINE']
- `16:38:34` 
  Sample (Berkshire):
- `16:38:34`     {
  "name": "BERKSHIRE HATHAWAY INC",
  "cik": "0001067983",
  "latest_filing": {
    "accession": "0001193125-26-054580",
    "filed_at": "2026-02-17",
    "period_of_report": "2025-12-31",
    "form": "13F-HR",
    "primary_doc": "xslForm13F_X02/primary_doc.xml",
    "filing_url": "https://www.sec.gov/Archives/edgar/data/1067983/000119312526054580/xslForm13F_X02/primary_doc.xml",
    "filing_index": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001067983&type=13F-HR&dateb=&owner=include&count=10"
  }
}
## 2. Fetch one real 13F infotable XML to learn schema

- `16:38:34`   filing index URL: https://www.sec.gov/Archives/edgar/data/1067983/000119312526054580/
