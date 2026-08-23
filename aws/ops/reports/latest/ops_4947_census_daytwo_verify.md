## G1 drain to COMPLETE (stall-kicks only)

**Status:** failure  
**Duration:** 1824.2s  
**Finished:** 2026-08-23T15:52:36+00:00  

## Error

```
SystemExit: 1
```

## Log
- `15:22:13`   cold start: state 376s stale -> async kick
- `15:22:13`   t+   0s phase=DRAIN n_done=42/56 rows=3033679 q=12 fail=2
- `15:27:17`   stall 300s -> async kick #1
- `15:32:20`   stall 300s -> async kick #2
- `15:37:24`   stall 300s -> async kick #3
- `15:42:28`   stall 300s -> async kick #4
- `15:47:32` G1 FAIL phase=DRAIN identity 42+2==56 -> False kicks=4
## G2 failures ledger (named, bounded)

- `15:47:32`   FAIL aies-miscsector    no data any mode (last HTTP 400)
- `15:47:32`   FAIL asm-industry       no data any mode (last HTTP 400)
- `15:47:32` G2 PASS n_failures=2 all_named=True
## G3 inception proof (new families)

- `15:47:32`   bds       bds                      tp=time mode=full_for   rows=5516     2022..2022
- `15:47:32` G3 FAIL bds_1970s=False qwi<=2000=False govs_ok=7 poverty=0 healthins=0
## inception coverage table

- `15:47:32`   govsstatefi govsstatefin                 full       tp=time rows=273258   2012..2024
- `15:47:32`   govslocalfi govslocalfin                 full       tp=time rows=227588   2017..2024
- `15:47:32`   govsemp     govsemp                      full       tp=time rows=176862   1992..2025
- `15:47:32`   asm         asm-value2017                full_for   tp=time rows=146160   2018..2021
- `15:47:32`   govspension govspension                  full       tp=time rows=53037    2012..2025
- `15:47:32`   asm         asm-product                  full_for   tp=time rows=32292    2002..2016
- `15:47:32`   govsstateta govsstatetax                 full       tp=time rows=16102    2016..2025
- `15:47:32`   asm         asm-value2012                year       tp=YEAR rows=8119     2013..2016
- `15:47:32`   bds         bds                          full_for   tp=time rows=5516     2022..2022
- `15:47:32`   govsschfin  govsschfin                   full_for   tp=time rows=2925     2012..2024
- `15:47:32`   asm         asm-benchmark2017            year_for   tp=YEAR rows=2608     2013..2016
- `15:47:32`   aies        aies-exp01                   full_for   tp=time rows=2530     2023..2023
- `15:47:32`   asm         asm-state                    full_for   tp=time rows=1318     2003..2016
- `15:47:32`   aies        aies-inv                     full_for   tp=time rows=1279     2023..2023
- `15:47:32`   govs        govs                         full_for   tp=time rows=912      2017..2024
- `15:47:32`   asm         asm-area2012                 year       tp=YEAR rows=208      2013..2016
- `15:47:32`   aies        aies-ecom                    full_for   tp=time rows=139      2023..2023
- `15:47:32`   asm         asm-area2017                 full_for   tp=time rows=4        2018..2021
- `15:47:32`   asm         asm-benchmark2022            year_for   tp=YEAR rows=4        2018..2021
- `15:47:32`   aies        aies-basic                   full_for   tp=time rows=1        2023..2023
- `15:47:32`   aies        aies-exp02                   full_for   tp=time rows=1        2023..2023
- `15:47:32`   eits        m3                           full_for   tp=time rows=600278   1..1
- `15:47:32`   eits        qfr                          full       tp=time rows=378854   2000..2026
- `15:47:32`   eits        mrts                         full       tp=time rows=195161   1992..2026
- `15:47:32`   eits        mwts                         full       tp=time rows=157052   1992..2026
- `15:47:32`   eits        qss                          full       tp=time rows=151236   2003..2026
- `15:47:32`   eits        advm3                        full_for   tp=time rows=132240   1..1
- `15:47:32`   eits        bfs                          full_for   tp=time rows=116256   1..1
- `15:47:32`   eits        resconst                     full       tp=time rows=90361    1959..2026
- `15:47:32`   eits        vip                          full       tp=time rows=83904    2002..2026
- `15:47:32`   eits        marts                        full       tp=time rows=58730    1992..2026
- `15:47:32`   eits        ressales                     full       tp=time rows=28416    1963..2026
- `15:47:32`   eits        mtis                         full       tp=time rows=23816    1992..2026
- `15:47:32`   eits        mhs                          full       tp=time rows=22140    1959..2014
- `15:47:32`   eits        hv                           full       tp=time rows=12240    1956..2026
- `15:47:32`   eits        mrtsadv                      full       tp=time rows=8598     1992..2026
- `15:47:32`   eits        mwtsadv                      full       tp=time rows=8598     1992..2026
- `15:47:32`   eits        qpr                          full       tp=time rows=5830     1968..2026
- `15:47:32`   eits        mhs2                         full_for   tp=time rows=3678     1..1
- `15:47:32`   eits        ftdadv                       full       tp=time rows=2484     1992..2026
- `15:47:32`   eits        ftd                          full       tp=time rows=2472     1992..2026
- `15:47:32`   eits        qtax                         full_for   tp=time rows=472      1..1
## G4 final rows/bytes census

- `15:47:32` G4 PASS rows_total=3033679 (+950863 vs v1.0) store=21.71MB (v1.0 11.36) keys=98
## G5 header-fix watchlist (no gate; heals on refresh)

- `15:47:32`   m3     y=1..1 refreshed=2026-08-23 (expect real span after next daily refresh)
- `15:47:32`   advm3  y=1..1 refreshed=2026-08-23 (expect real span after next daily refresh)
- `15:47:32`   bfs    y=1..1 refreshed=2026-08-23 (expect real span after next daily refresh)
- `15:47:32`   mhs2   y=1..1 refreshed=2026-08-23 (expect real span after next daily refresh)
- `15:47:32`   qtax   y=1..1 refreshed=2026-08-23 (expect real span after next daily refresh)
## G6 sentinel pipeline

- `15:52:36` G6 FAIL pipeline={'name': 'census-us', 'status': 'RUNNING', 'detail': 'DRAIN 42/56 datasets · 3033679 rows · 2 source failures logged', 'age_min': 13.8}
- `15:52:36` ops 4947 RED: G1 completion; G3 inception; G6 sentinel
