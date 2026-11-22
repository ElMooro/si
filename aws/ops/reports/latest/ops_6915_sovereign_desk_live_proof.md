# ops 6915 - the Sovereign button, its desk and the CISS page, real browser on the live site (read-only), 2026-11-22T19:48+00:00

**Status:** failure  
**Duration:** 3.5s  
**Finished:** 2026-11-22T19:48:11+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6915_sovereign_desk_live_proof.py", line 171, in main
    and all(ds["measures"][k]["value"] == M[k]["value"] and ds["measures"][k]["date"] == M[k]["date"] and len(ds["series"][k]["d"]) == M[k]["n"] for k in ds["order"]),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6915_sovereign_desk_live_proof.py", line 171, in <genexpr>
    and all(ds["measures"][k]["value"] == M[k]["value"] and ds["measures"][k]["date"] == M[k]["date"] and len(ds["series"][k]["d"]) == M[k]["n"] for k in ds["order"]),
                                          ^
NameError: cannot access free variable 'M' where it is not associated with a value in enclosing scope

```

## Log
## 0. The live site serves the new files

- `19:48:08` served after 1 tries: {"engine": true, "rail": true, "desk_page": true, "desk_script": true, "dollar_desk": true, "ciss_page": true, "ciss_script": true, "fiscal_page": true}
- `19:48:08` scripts as the pages name them: /jh-sovereign-desk.js?v=5513d906 | /jh-ciss-research.js?v=62f3959f
