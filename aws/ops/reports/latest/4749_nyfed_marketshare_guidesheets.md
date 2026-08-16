# ops 4749 -- final two families: marketshare + guidesheets

**Status:** success  
**Duration:** 1.1s  
**Finished:** 2026-08-16T16:05:24+00:00  

## Data

| banked | check | family | reason | value |
|---|---|---|---|---|
|  | templates_marketshare |  |  | 2 |
| False |  | marketshare | no_template_validated |  |
|  | templates_guidesheets |  |  | 2 |
| False |  | guidesheets | no_template_validated |  |

## Log
- `16:05:23` marketshare: /api/marketshare/ytd/latest.{format} subs={'format': 'json'} -> status=200 rows=0
- `16:05:24` marketshare: /api/marketshare/qtrly/latest.{format} subs={'format': 'json'} -> status=200 rows=0
- `16:05:24` ⚠ marketshare: no template validated
- `16:05:24` guidesheets: /api/guidesheets/{guidesheetType}/latest.{format} subs={'guidesheetType': 'si', 'format': 'json'} -> status=200 rows=0
- `16:05:24` guidesheets: /api/guidesheets/{guidesheetType}/previous.{format} subs={'guidesheetType': 'si', 'format': 'json'} -> status=200 rows=0
- `16:05:24` ⚠ guidesheets: no template validated
