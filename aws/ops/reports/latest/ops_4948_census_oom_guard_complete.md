## G0 zip-settle v1.1.1

**Status:** failure  
**Duration:** 548.0s  
**Finished:** 2026-08-23T16:09:54+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:00:46`   t+   0s marker=True state=Active
- `16:01:12` G0 PASS marker deployed after 26s (sha mky8lw3gM2gd)
## G0b redo bds (wrong-grammar bank)

- `16:01:32` G0b PASS bds reset/rescanning (tp=None ok=False q=True)
## G1 drain to COMPLETE (attempts-aware fingerprint)

- `16:01:32`   t+   0s DRAIN done=41/56 rows=3028163 q=13 head=bds ry=None att=1 fail=2
- `16:03:39`   t+ 126s DRAIN done=42/56 rows=3281899 q=12 head=healthins-sahie ry=None att=1 fail=2
- `16:04:55`   t+ 202s DRAIN done=47/56 rows=3284102 q=7 head=poverty-saipe-schdist ry=None att=1 fail=2
- `16:05:20`   t+ 227s DRAIN done=47/56 rows=3284102 q=6 head=pseo-earnings ry=None att=1 fail=3
- `16:06:10`   t+ 278s DRAIN done=47/56 rows=3284102 q=5 head=pseo-flows ry=None att=1 fail=4
- `16:06:36`   t+ 303s DRAIN done=47/56 rows=3284102 q=4 head=qwi-rh ry=None att=1 fail=5
- `16:07:01`   t+ 328s DRAIN done=47/56 rows=3284102 q=4 head=qwi-rh ry=2022 att=2 fail=5
- `16:07:26`   t+ 354s DRAIN done=47/56 rows=3284102 q=3 head=qwi-sa ry=None att=1 fail=6
- `16:07:52`   t+ 379s DRAIN done=47/56 rows=3284102 q=2 head=qwi-se ry=None att=1 fail=7
- `16:08:17`   t+ 404s DRAIN done=47/56 rows=3284102 q=1 head=soma ry=None att=1 fail=8
- `16:09:33`   t+ 480s COMPLETE done=48/56 rows=3892162 q=0 head=None ry=None att=None fail=8
- `16:09:33` G1 PASS phase=COMPLETE identity 48+8==56 -> True kicks=0
## G2 failures ledger

- `16:09:33`   FAIL aies-miscsector    no data any mode (last HTTP 400)
- `16:09:33`   FAIL asm-industry       no data any mode (last HTTP 400)
- `16:09:33`   FAIL poverty-saipe-schdist no data any mode (last HTTP 400)
- `16:09:33`   FAIL pseo-earnings      no data any mode (last HTTP 400)
- `16:09:33`   FAIL pseo-flows         no data any mode (last HTTP 400)
- `16:09:33`   FAIL qwi-rh             no data any mode (last HTTP 400)
- `16:09:33`   FAIL qwi-sa             no data any mode (last HTTP 400)
- `16:09:33`   FAIL qwi-se             no data any mode (last HTTP 400)
- `16:09:33` G2 FAIL n_failures=8 all_named=True
## G3 inception proof

- `16:09:33` G3 FAIL bds_1970s=True qwi<=2000=False govs=7 poverty=2 healthins=1
## inception coverage (new families first)

- `16:09:33`   soma        soma                         year_for   tp=time rows=608060   2014..2026
- `16:09:33`   govsstatefi govsstatefin                 full       tp=time rows=273258   2012..2024
- `16:09:33`   bds         bds                          year_for   tp=YEAR rows=253736   1978..2023
- `16:09:33`   govslocalfi govslocalfin                 full       tp=time rows=227588   2017..2024
- `16:09:33`   govsemp     govsemp                      full       tp=time rows=176862   1992..2025
- `16:09:33`   asm         asm-value2017                full_for   tp=time rows=146160   2018..2021
- `16:09:33`   govspension govspension                  full       tp=time rows=53037    2012..2025
- `16:09:33`   asm         asm-product                  full_for   tp=time rows=32292    2002..2016
- `16:09:33`   govsstateta govsstatetax                 full       tp=time rows=16102    2016..2025
- `16:09:33`   asm         asm-value2012                year       tp=YEAR rows=8119     2013..2016
- `16:09:33`   govsschfin  govsschfin                   full_for   tp=time rows=2925     2012..2024
- `16:09:33`   asm         asm-benchmark2017            year_for   tp=YEAR rows=2608     2013..2016
- `16:09:33`   aies        aies-exp01                   full_for   tp=time rows=2530     2023..2023
- `16:09:33`   healthins   healthins-sahie              full_for   tp=time rows=1566     2006..2024
- `16:09:33`   asm         asm-state                    full_for   tp=time rows=1318     2003..2016
- `16:09:33`   aies        aies-inv                     full_for   tp=time rows=1279     2023..2023
- `16:09:33`   govs        govs                         full_for   tp=time rows=912      2017..2024
- `16:09:33`   poverty     poverty-histpov2             full_for   tp=time rows=487      1959..2024
- `16:09:33`   asm         asm-area2012                 year       tp=YEAR rows=208      2013..2016
- `16:09:33`   aies        aies-ecom                    full_for   tp=time rows=139      2023..2023
- `16:09:33`   hhpulse     hhpulse                      full_for   tp=time rows=117      2024..2024
- `16:09:33`   poverty     poverty-saipe                full_for   tp=time rows=32       1989..2024
- `16:09:33`   asm         asm-area2017                 full_for   tp=time rows=4        2018..2021
- `16:09:33`   asm         asm-benchmark2022            year_for   tp=YEAR rows=4        2018..2021
- `16:09:33`   aies        aies-basic                   full_for   tp=time rows=1        2023..2023
- `16:09:33`   aies        aies-exp02                   full_for   tp=time rows=1        2023..2023
- `16:09:33`   hps         hps                          full_for   tp=time rows=1        2020..2020
- `16:09:33`   eits        m3                           full_for   tp=time rows=600278   1..1
- `16:09:33`   eits        qfr                          full       tp=time rows=378854   2000..2026
- `16:09:33`   eits        mrts                         full       tp=time rows=195161   1992..2026
- `16:09:33`   eits        mwts                         full       tp=time rows=157052   1992..2026
- `16:09:33`   eits        qss                          full       tp=time rows=151236   2003..2026
- `16:09:33`   eits        advm3                        full_for   tp=time rows=132240   1..1
- `16:09:33`   eits        bfs                          full_for   tp=time rows=116256   1..1
- `16:09:33`   eits        resconst                     full       tp=time rows=90361    1959..2026
- `16:09:33`   eits        vip                          full       tp=time rows=83904    2002..2026
- `16:09:33`   eits        marts                        full       tp=time rows=58730    1992..2026
- `16:09:33`   eits        ressales                     full       tp=time rows=28416    1963..2026
- `16:09:33`   eits        mtis                         full       tp=time rows=23816    1992..2026
- `16:09:33`   eits        mhs                          full       tp=time rows=22140    1959..2014
- `16:09:33`   eits        hv                           full       tp=time rows=12240    1956..2026
- `16:09:33`   eits        mrtsadv                      full       tp=time rows=8598     1992..2026
- `16:09:33`   eits        mwtsadv                      full       tp=time rows=8598     1992..2026
- `16:09:33`   eits        qpr                          full       tp=time rows=5830     1968..2026
- `16:09:33`   eits        mhs2                         full_for   tp=time rows=3678     1..1
- `16:09:33`   eits        ftdadv                       full       tp=time rows=2484     1992..2026
- `16:09:33`   eits        ftd                          full       tp=time rows=2472     1992..2026
- `16:09:33`   eits        qtax                         full_for   tp=time rows=472      1..1
## G4 final rows/bytes

- `16:09:33` G4 PASS rows_total=3892162 (+1809346 vs v1.0) store=32.13MB keys=168
## G5 header-fix watchlist (heals on scheduled refresh)

- `16:09:33`   m3     y=1..1 refreshed=2026-08-23
- `16:09:33`   advm3  y=1..1 refreshed=2026-08-23
- `16:09:33`   bfs    y=1..1 refreshed=2026-08-23
- `16:09:33`   mhs2   y=1..1 refreshed=2026-08-23
- `16:09:33`   qtax   y=1..1 refreshed=2026-08-23
## G6 sentinel

- `16:09:54` G6 PASS pipeline={'name': 'census-us', 'status': 'COMPLETE', 'detail': 'COMPLETE 48/56 datasets · 3892162 rows · 8 source failures logged', 'age_min': 0.1}
- `16:09:54` ops 4948 RED: G2 failures; G3 inception
