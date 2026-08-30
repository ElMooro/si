## P0 deploy + lanes intact

**Status:** success  
**Duration:** 958.4s  
**Finished:** 2026-08-30T00:36:17+00:00  

## Data

| ecb | eurostat | left |
|---|---|---|
| 17 | 536 | 980 |

## Log
- `00:20:19`   code fresh 2026-08-30T00:20:13.000+0000
- `00:20:20`   eurostat  flows_done=8147 n_pages=1128408 series=564204000 (must be unchanged)
- `00:20:20`   ecb       flows_done=207 n_pages=6481 series=3240500 (must be unchanged)
## P1 wire the t1 targets

- `00:20:20`   targets: [('ecb', '{"provider": "ecb"}'), ('t1', '{"provider": "eurostat"}'), ('t1ecb', '{"provider": "ecb", "mode": "t1"}'), ('t1eurost', '{"provider": "eurostat", "mode": "t1"}')]
- `00:20:20`   cadence -> rate(2 minutes) for the build window (restore to hourly when candidates_left hits 0)
## P2 build

- `00:22:10`   ecb t1 status=200 err=None
- `00:22:10`   {"statusCode": 200, "body": "{\"provider\": \"ecb\", \"built\": [[\"ICP\", 60268, 15], [\"WTS\", 60157, 15], [\"CBD2\", 66100, 17], [\"BSI\", 68798, 17], [\"BSI_PUB\", 163, 1], [\"PTT\", 77889, 20], [\"PSS\", 79225, 20], [\"PAY\", 82899, 21], [\"SHSS\", 85321, 21], [\"HICP\", 89883, 22], [\"PCP\", 138242, 34], [\"IVF\", 273854, 67], [\"SAFE\", 292319, 72], [\"SPF\", 505823, 124], [\"CSEC\", 738218
- `00:22:10`   eurostat t1 kicked (Event)
- `00:23:11`   t+ 1min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:23:11`   t+ 1min eurostat  flows=50 left=None entries=2,225,902 blocks=578 0.21 GB last=EARN_SES06_15
- `00:24:11`   t+ 2min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:24:11`   t+ 2min eurostat  flows=96 left=None entries=4,622,540 blocks=1,185 0.43 GB last=LFSO_14BEDUC
- `00:25:11`   t+ 3min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:25:11`   t+ 3min eurostat  flows=140 left=None entries=7,093,355 blocks=1,806 0.66 GB last=AVIA_PAR_FR
- `00:26:12`   t+ 4min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:26:12`   t+ 4min eurostat  flows=184 left=None entries=9,557,094 blocks=2,437 0.90 GB last=INN_CIS12_MRKT
- `00:27:12`   t+ 5min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:27:12`   t+ 5min eurostat  flows=226 left=None entries=11,993,243 blocks=3,052 1.12 GB last=PRC_HICP_ADMP
- `00:28:12`   t+ 6min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:28:12`   t+ 6min eurostat  flows=266 left=None entries=14,473,331 blocks=3,676 1.36 GB last=ISOC_CI_CE_I
- `00:29:13`   t+ 7min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:29:13`   t+ 7min eurostat  flows=305 left=None entries=16,924,812 blocks=4,296 1.58 GB last=LFSO_19FXWT07
- `00:30:13`   t+ 8min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:30:13`   t+ 8min eurostat  flows=342 left=None entries=19,261,172 blocks=4,881 1.80 GB last=EARN_SES06_38
- `00:31:13`   t+ 9min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:31:13`   t+ 9min eurostat  flows=377 left=None entries=21,556,714 blocks=5,458 2.02 GB last=EDAT_LFSE_31
- `00:32:14`   t+10min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:32:14`   t+10min eurostat  flows=411 left=None entries=24,103,988 blocks=6,099 2.26 GB last=LFST_R_LFE2EN2
- `00:33:14`   t+11min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:33:14`   t+11min eurostat  flows=445 left=None entries=26,677,654 blocks=6,745 2.50 GB last=MIGR_DUBRINFO
- `00:34:14`   t+12min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:34:14`   t+12min eurostat  flows=478 left=None entries=29,102,308 blocks=7,352 2.72 GB last=TRNG_LFS_10
- `00:35:15`   t+13min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:35:15`   t+13min eurostat  flows=509 left=None entries=31,466,197 blocks=7,946 2.95 GB last=ISOC_EB_DAN2
- `00:36:15`   t+14min ecb       flows=17 left=0 entries=2,619,231 blocks=649 0.24 GB last=DCM
- `00:36:15`   t+14min eurostat  flows=536 left=980 entries=33,768,326 blocks=8,521 3.17 GB last=EARN_SES18_29
## P3 real range read against a built flow

- `00:36:15`   ecb/IVF: 273,854 entries, 67 blocks, map 6 KB, data 24.1 MB
- `00:36:16`   binary-searched 67 blocks -> block 33, Range read 354 KB, found=True
- `00:36:16`   read amplification: 359 KB fetched vs 24.1 MB for the whole flow (66x less)
- `00:36:16`   eurostat/MAR_GO_AM_SI: 50,700 entries, 13 blocks, map 1 KB, data 4.8 MB
- `00:36:17`   binary-searched 13 blocks -> block 6, Range read 376 KB, found=True
- `00:36:17`   read amplification: 377 KB fetched vs 4.8 MB for the whole flow (12x less)
- `00:36:17`   -> data/ops/index-tier1.json
- `00:36:17` ops 5050 GREEN -- Tier 1 building, range read proven end to end
