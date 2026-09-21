# ops 5860 -- Gear B on the new supply

**Status:** success  
**Duration:** 6.0s  
**Finished:** 2026-09-21T18:05:29+00:00  

## Data

| launch | max_family_share | max_jobs_per_day | min_sft_rows | model_source |
|---|---|---|---|---|
| True | 0.85 | 4 | 500 | own |

## Log
## 1. Jobs since 2026-09-19

- `18:05:24` 09-20 01:07 jh-exam-gen20-20260920-010752 Completed
- `18:05:24` 09-20 00:07 jh-gearb-gen20-20260920-000745 Completed
- `18:05:24` 09-19 11:07 jh-exam-gen19-20260919-110750 Completed
- `18:05:24` 09-19 10:12 jh-gearb-gen19-20260919-101211 Completed
- `18:05:24` 09-19 09:07 jh-exam-gen18-20260919-090750 Completed
- `18:05:24` 09-19 08:12 jh-gearb-gen18-20260919-081222 Completed
- `18:05:24` 09-19 07:07 jh-exam-gen17-20260919-070751 Completed
- `18:05:24` 09-19 05:12 jh-gearb-gen17-20260919-051210 Completed
- `18:05:24` 09-19 00:07 jh-gearb-gen16-20260919-000742 Stopped
## 2. Control + newest dataset manifest

- `18:05:24` gen-21 kept=693 families=3 seen={"skills_seen": 1, "traces_seen": 1, "unmapped": 1534, "verified_seen": 3752} launched=None at=2026-09-20T02:12:24.266656Z
- `18:05:25` gen-22 kept=2010 families=3 seen={"skills_seen": 1, "traces_seen": 1, "unmapped": 1534, "verified_seen": 5185} launched=None at=2026-09-21T18:03:48.783394Z
- `18:05:25` gen-23 kept=2010 families=3 seen={"skills_seen": 1, "traces_seen": 1, "unmapped": 1534, "verified_seen": 5185} launched=None at=2026-09-21T18:04:37.664831Z
## 3. A tick, dry (launch=false) -- what would it do?

- `18:05:29` {"ok": true, "result": {"at": "2026-09-21T18:05:26.800563Z", "polled": [], "built": {"ok": true, "generation": 23, "kept": 2010, "floor": 500, "missing_rows": null, "reason": null}, "launched": null, "refusal": "launch disabled", "decided": null, "examined": null, "doctrine": "self-improve.3.1"}}
- `18:05:29` ✅ done
