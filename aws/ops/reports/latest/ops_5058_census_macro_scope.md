## P0 API key

**Status:** success  
**Duration:** 16.6s  
**Finished:** 2026-08-30T16:13:47+00:00  

## Data

| calls | has_key | in_scope | probe |
|---|---|---|---|
| 15650 | True | 1029 | 200 |

## Log
- `16:13:31`   env vars that look key-ish: ['CENSUS_API_KEY']
- `16:13:31`   using CENSUS_API_KEY (len 40, value not printed)
- `16:13:31`   keyed probe -> HTTP 200
- `16:13:31`   sample: [["NAME", "EMP", "PAYANN", "state"], ["California", "16032440", "1361755461", "06"]]
- `16:13:31`   the API answers, so the walker's problem is rate, not access
## P1 what timeseries already gives us

- `16:13:35`   timeseries datasets we already hold cover:
- `16:13:35`     timeseries/intltrade               36
- `16:13:35`     timeseries/eits                    21
- `16:13:35`     timeseries/asm                     9
- `16:13:35`     timeseries/aies                    6
- `16:13:35`     timeseries/poverty                 3
- `16:13:35`     timeseries/qwi                     3
- `16:13:35`     timeseries/idb                     2
- `16:13:35`     timeseries/pseo                    2
- `16:13:35`     timeseries/healthins               1
- `16:13:35`     timeseries/govs                    1
- `16:13:35`     timeseries/bds                     1
- `16:13:35`     timeseries/hps                     1
- `16:13:35`     timeseries/soma                    1
- `16:13:35`     timeseries/hhpulse                 1
- `16:13:35`   -> EITS, BDS, QWI and ASM are ALREADY IN. The headline
- `16:13:35`      macro indicators are not the gap.
## P2 classify the rest against the scope

- `16:13:35`   IN SCOPE (curated economic programs):
- `16:13:35`     cps        703 entries  1989–2026    Current Population Survey — the employment survey behind the u
- `16:13:35`     sipp       178 entries  1990–2024    Income & Program Participation — household income and wealth
- `16:13:35`     cbp         38 entries  1986–2023    County Business Patterns — establishments, employment, payroll
- `16:13:35`     nonemp      27 entries  1997–2023    Nonemployer Statistics — self-employment, business formation
- `16:13:35`     zbp         25 entries  1994–2018    ZIP Code Business Patterns — the same at ZIP granularity
- `16:13:35`     ecn         12 entries  2012–2022    Economic Census — manufacturing, retail, wholesale, services; 
- `16:13:35`     ase          9 entries  2014–2016    Annual Survey of Entrepreneurs
- `16:13:35`     abscb        7 entries  2017–2023    Annual Business Survey — company characteristics
- `16:13:35`     abscbo       7 entries  2017–2023    Annual Business Survey — business owners
- `16:13:35`     abscs        7 entries  2017–2023    Annual Business Survey — company summary
- `16:13:35`     absnesd      6 entries  2018–2023    Annual Business Survey — nonemployer demographics
- `16:13:35`     absnesdo     6 entries  2018–2023    Annual Business Survey — nonemployer owners
- `16:13:35`     ewks         4 entries  1997–2012    Economic Census, earlier vintages
- `16:13:35`     total in scope: 1029 entries
- `16:13:35`   OUT OF SCOPE (demographics/social, dropped deliberately):
- `16:13:35`     acs        270 entries  2004–2024
- `16:13:35`     pep         80 entries  1990–2023
- `16:13:35`     dec         64 entries  2000–2020
- `16:13:35`     pdb         23 entries  2015–2026
- `16:13:35`     popproj     10 entries  2012–2017
- `16:13:35`     cre          9 entries  2016–2024
- `16:13:35`     geoinfo      6 entries  2020–2025
- `16:13:35`     intltrade    5 entries  2014–2018
- `16:13:35`     crepuertorico    5 entries  2019–2024
- `16:13:35`   KEYWORD SWEEP over families I did not curate -- anything
- `16:13:35`   economic here would be a miss on my part:
- `16:13:35`     absmcb          4 entries (4 match) 2020–2023   2020 Annual Business Survey: Technology, Financing, and Mana
- `16:13:35`     rhfs            4 entries (4 match) 2015–2024   Rental Housing Finance Survey
- `16:13:35`     cfspum          4 entries (4 match) 2012–2017   Commodity Flow Survey  Public Use Microdata: Origin of Shipm
- `16:13:35`     ecnbranddeal    3 entries (3 match) 2012–2022   Finance and Insurance: Subject Series - Misc Subjects: Broke
- `16:13:35`     ecnbridge1      3 entries (3 match) 2012–2022   All sectors: Core Business Statistics Series: Industry Bridg
- `16:13:35`     ecnbridge2      3 entries (3 match) 2012–2022   All sectors: Core Business Statistics Series: Industry Bridg
- `16:13:35`     ecnccard        3 entries (3 match) 2012–2022   Finance and Insurance: Subject Series - Misc Subjects: Credi
- `16:13:35`     ecnclcust       3 entries (3 match) 2012–2022   2012 Economic Census - Economic Census US Economic Class of 
- `16:13:35`     ecncomm         3 entries (3 match) 2012–2022   Wholesale Trade: Subject Series - Misc Subjects: Sales and C
- `16:13:35`     ecncomp         3 entries (3 match) 2012–2022   All sectors: Core Business Statistics Series: Comparative St
- `16:13:35`     ecnconact       3 entries (3 match) 2012–2022   Subject Series - Misc Subjects: Construction Activity for Se
- `16:13:35`     ecncrfin        3 entries (3 match) 2012–2022   Finance and Insurance: Subject Series - Misc Subjects: Types
## P3 cost of the shortlist only

- `16:13:36`   cps        703 entries ·   349 vars ·  2 geo -> ~11,248 calls
- `16:13:37`   sipp       178 entries ·  1021 vars ·  1 geo -> ~4,094 calls
- `16:13:37`   cbp         38 entries ·    22 vars ·  3 geo -> ~114 calls
- `16:13:38`   nonemp      27 entries ·    13 vars ·  3 geo -> ~81 calls
- `16:13:38`   zbp         25 entries ·    17 vars ·  1 geo -> ~25 calls
- `16:13:45`   ecn         12 entries ·    30 vars ·  1 geo -> ~12 calls
- `16:13:46`   ase          9 entries ·    42 vars ·  3 geo -> ~27 calls
- `16:13:46`   abscb        7 entries ·    44 vars ·  7 geo -> ~49 calls
- `16:13:46`   SHORTLIST TOTAL: ~15,650 requests
- `16:13:46`   versus ~112,446 for 5057's top six -- dropping the
- `16:13:46`   decennial census alone removes ~101,376 of them
- `16:13:46`   at 500/day (no key): 31 days.  with a key: hours.
## P4 the plan

- `16:13:46`   1. CBP + ZBP + nonemp -- establishments, employment and
- `16:13:46`      payroll by NAICS and geography, 1986–2023. Cheapest and
- `16:13:46`      the most directly useful to the physical-economy and
- `16:13:46`      regional desks.
- `16:13:46`   2. ECN + EWKS -- the Economic Census: manufacturing,
- `16:13:46`      retail, wholesale, services, 1997–2022.
- `16:13:46`   3. ABS family -- business formation and ownership.
- `16:13:46`   4. CPS -- 703 entries, 1989–2026, the employment survey.
- `16:13:46`      Largest of the four; worth its own lane.
- `16:13:46`   5. SIPP -- household income and wealth, if consumer
- `16:13:46`      balance-sheet work matters.
- `16:13:46`   Each gets a resumable walker on the pattern the eurostat
- `16:13:46`   and ecb lanes now use: state cursor, budget checked inside
- `16:13:46`   the row loop, hash-skipped idempotent writes, stall breaker.
- `16:13:47`   -> data/ops/census-macro-scope.json
- `16:13:47` ops 5058 GREEN -- scope decided on measurements
