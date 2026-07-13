# ops 3262 — semantic parity: duplicates vs guard vs absent

**Status:** success  
**Duration:** 253.6s  
**Finished:** 2026-07-13T14:15:22+00:00  

## Data

| brain_notes | covered_in_brain | duplicates_by_text | genuinely_absent | guard_rejected | id_missing | mirror | n_fails | n_warns | push_failed | pushed | substantive_unique | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 12152 |  |  |  |  |  | 3322 |  |  |  |  |  |  |
|  |  | 3 | 120 | 250 | 373 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 0 | 120 |  |  |
|  | 3069 |  |  |  |  |  |  |  |  |  | 3069 |  |
|  |  |  |  |  |  |  | 0 | 0 |  |  |  | PASS |

## Log
- `14:11:10`   guard e.g.: '[TV:TVC:MOVE] Move: US BOAML US Bond Market option volatilit'
- `14:11:10`   guard e.g.: '[TV:ECONOMICS:USRR] When the repo rate is high banks are bet'
- `14:11:10`   guard e.g.: '[TV:TVC:DXY] THE DOLLAR RISING DRAMATICALLY IS THE SIGN OF T'
- `14:11:10`   absent e.g.: '[TV:NYMEX:CL1!] THERE ARE VERY FEW THINGS IN THE WORLD THAT ARE MORE I'
- `14:11:10`   absent e.g.: '[TV:AMEX:SVXY] IF YOU GONNA BUY OPTIONS IT’S MUCH BETTER TO DO IT WHEN'
- `14:11:10`   absent e.g.: '[TV:AMEX:UVXY] IF YOU GONNA BUY OPTIONS IT’S MUCH BETTER TO DO IT WHEN'
## Push the genuinely-absent (with error bodies)

- `14:15:22` ✅ SEMANTIC PARITY: 3069/3069 substantive unique notes in the brain. The 3 id-gap duplicates and 250 guard-rejected fragments are the brain's OWN protections working — by Khalid's design.
