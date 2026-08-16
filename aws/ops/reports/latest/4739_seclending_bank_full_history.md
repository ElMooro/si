# ops 4739 -- NY Fed securities lending: discover + bank full history

**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-08-16T14:53:46+00:00  

## Data

| check | earliest | latest | mnemonic | n_dates | value |
|---|---|---|---|---|---|
|  | 2014-08-22 | 2026-08-06 | REPO-TRIV1_AR_AG-F | 2874 |  |
|  | 2018-04-02 | 2026-08-06 | FNYR-SOFR-A | 2085 |  |
| seclending_endpoint_found |  |  |  |  | False |

## Log
## A. Corrected depth proof on banked OFR series

- `14:53:45` ✅ REPO-TRIV1_AR_AG-F: 2874 distinct dates, 2014-08-22 -> 2026-08-06
- `14:53:45` ✅ FNYR-SOFR-A: 2085 distinct dates, 2018-04-02 -> 2026-08-06
## B. Endpoint discovery -- candidate shapes

- `14:53:46` seclending/results/search.json?startDate=2026-07-01&endDate=2026-08-16 -> status=400 json=False body[:150]=
- `14:53:46` seclending/all/results/search.json?startDate=2026-07-01&endDate=2026-08-16 -> status=400 json=False body[:150]=
- `14:53:46` seclending/all/results/lastTwoWeeks.json -> status=400 json=False body[:150]=
- `14:53:46` seclending/results/lastTwoWeeks.json -> status=400 json=False body[:150]=
- `14:53:46` seclending/all/results/last/10.json -> status=400 json=False body[:150]=
- `14:53:46` ⚠ no candidate returned usable JSON -- NOT writing anything. Next step is reading NY Fed's API doc page for the exact seclending path rather than guessing further shapes.
