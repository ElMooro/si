# G0. both live feeds, field-level

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-08-17T17:24:52+00:00  

## Log
- `17:24:51` ✅   foreign-flows bindings OK
- `17:24:51` ✅   global-flows bindings OK (peru T1.26)
# 1. committed-HTML + ORDER

- `17:24:51` ✅   id="tic-ff-card" exactly once
- `17:24:51` ✅   id="global-ff-card" exactly once
- `17:24:51` ✅   ORDER: US card @10246 < world @14475 < legacy @24890 (TOP confirmed)
- `17:24:51` ✅   label 'US Treasuries (LT+ST)' present
- `17:24:51` ✅   label 'Agency / MBS (mortgage channel)' present
- `17:24:51` ✅   label 'NAR annual survey' present
- `17:24:51` ✅   label 'Gov bonds bought by nonresidents' present
# 2. served (<=8 min)

- `17:24:52` ✅ BOTH cards SERVED in order after 0s
# 3. verdict

- `17:24:52` ✅ capital-flow.html: US-by-destination + world inflow grid now lead the page
