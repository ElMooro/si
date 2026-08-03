# ops 4304 -- endpoint truth for FR2004 + OFR

**Status:** success  
**Duration:** 4.4s  
**Finished:** 2026-08-03T00:27:28+00:00  

## Log
- `00:27:24` probe keyid: PDFTR-USTET (U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTE)
- `00:27:24` catalog row keys: ['seriesbreak', 'keyid', 'description']
- `00:27:24` seriesbreaks: ['SBP2001', 'SBP2013', 'SBN2013', 'SBN2015', 'SBN2022', 'SBN2024']
- `00:27:25` all        -> 0 rows {"pd": {"timeseries": []}}
- `00:27:25` bare       -> ERR HTTP Error 400: Bad Request
- `00:27:26` sb:SBN2015 -> 365 rows sample {"asofdate": "2021-12-29", "keyid": "PDFTR-USTET", "value": "83906"}
- `00:27:27` sb:SBN2022 -> 130 rows sample {"asofdate": "2024-06-26", "keyid": "PDFTR-USTET", "value": "122550"}
- `00:27:28` sb:SBN2024 -> 108 rows sample {"asofdate": "2026-07-22", "keyid": "PDFTR-USTET", "value": "94605"}
- `00:27:28` ⚠ OFR metadata: Not a gzipped file (b'["')
- `00:27:28` ✅ probe complete
