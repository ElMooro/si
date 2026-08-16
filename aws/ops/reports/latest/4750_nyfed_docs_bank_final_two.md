# ops 4750 -- marketshare + guidesheets as documents (10/10)

**Status:** success  
**Duration:** 1.8s  
**Finished:** 2026-08-16T16:07:42+00:00  

## Data

| banked | check | family | payload_bytes | value | variant |
|---|---|---|---|---|---|
|  | templates_marketshare |  |  | 2 |  |
|  | templates_guidesheets |  |  | 2 |  |
| True |  | guidesheets | 2603 |  | si-latest |
| True |  | guidesheets | 1611 |  | wi-latest |
| True |  | guidesheets | 657 |  | fs-latest |
| True |  | guidesheets | 2603 |  | si-previous |
| True |  | guidesheets | 1166 |  | wi-previous |
| True |  | guidesheets | 655 |  | fs-previous |
|  | documents_banked_total |  |  | 6 |  |

## Log
- `16:07:41` marketshare/default/latest: status=200 bytes=64733 -> skipped (no usable JSON)
- `16:07:41` marketshare/default/latest: status=200 bytes=64807 -> skipped (no usable JSON)
- `16:07:41` ✅ guidesheets/si-latest: 2603 bytes, top-level: guidesheet
- `16:07:41` ✅ guidesheets/wi-latest: 1611 bytes, top-level: guidesheet
- `16:07:41` ✅ guidesheets/fs-latest: 657 bytes, top-level: guidesheet
- `16:07:41` ✅ guidesheets/si-previous: 2603 bytes, top-level: guidesheet
- `16:07:42` ✅ guidesheets/wi-previous: 1166 bytes, top-level: guidesheet
- `16:07:42` ✅ guidesheets/fs-previous: 655 bytes, top-level: guidesheet
- `16:07:42` ✅ 6 reference documents banked -- all 10 NY Fed API families now held
