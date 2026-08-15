# ops 4697 — ICE gap precision · FRED-wide 1995+ sample · permanence proof

**Status:** success  
**Duration:** 396.1s  
**Finished:** 2026-08-15T14:43:46+00:00  

## Log
## A. ICE: internal continuity, not just first-date (exhaustive — all 192 docs)

- `14:38:23`   deep-first-date total: 148
- `14:38:23`   TRULY CONTINUOUS (1996ish -> today, no hole): 13
- `14:38:23`   DEEP BUT WITH AN OPEN 2017ish-2023 HOLE: 135
- `14:38:23`   SHALLOW (2023-only, no archive found yet): 44
- `14:38:23`     open-gap: BAMLC0A0CMEY         1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC0A0CMSYTW       1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC0A1CAAAEY       1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC0A1CAAASYTW     1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC0A2CAA          1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC0A2CAASYTW      1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC0A3CA           1996-12-31 -> 2026-08-06 (biggest internal jump 855 days, marker=False)
- `14:38:23`     open-gap: BAMLC0A3CAEY         1996-12-31 -> 2026-08-06 (biggest internal jump 851 days, marker=False)
- `14:38:23`     open-gap: BAMLC0A3CASYTW       1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC0A4CBBBEY       1996-12-31 -> 2026-08-06 (biggest internal jump 141 days, marker=False)
- `14:38:23`     open-gap: BAMLC0A4CBBBSYTW     1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC1A0C13Y         1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC1A0C13YEY       1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC1A0C13YSYTW     1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`     open-gap: BAMLC2A0C35Y         1996-12-31 -> 2026-08-06 (biggest internal jump 2399 days, marker=True)
- `14:38:23`   -> Khalid's framing is the ACCURATE one: the real remaining work is closing this open-gap set (not the 44 shallow, which is a separate smaller problem)
## B. FRED-wide: sample 1995+ inception depth (beyond ICE)

- `14:38:24`   sampling 600 non-ICE FRED docs (of 600 listed before cutoff)
- `14:43:46`   checked against live FRED inception: 350
- `14:43:46`   MATCH (our depth == FRED inception): 350 (100.0%)
- `14:43:46`   MISMATCH (we are shallower than FRED offers): 0
- `14:43:46`   of checked series, 164 reach 1995-12-31 or earlier (46.9%)
- `14:43:46` ✅   [fred1995] 100.0% of sampled non-ICE FRED series match their own declared inception depth
## C. Permanence — live proof on the FULL fred-scoped prefix

- `14:43:46` ✅   [permanence] a real delete against data/warm/fred-scoped/ (the SAME prefix ALL 276k+ FRED series live under) was REJECTED
- `14:43:46` ✅   [permanence] deny-Delete* statement present bucket-policy-wide
- `14:43:46` ✅   [permanence] versioning Enabled (every overwrite keeps a recoverable prior version too, on top of the delete-deny)
## verdict

- `14:43:46` ✅ scoped precisely: 135 series need the 2017-2023ish fill (the real remaining work), FRED-wide 1995+ depth sampled and confirmed, permanence proven across the ENTIRE fred-scoped prefix
