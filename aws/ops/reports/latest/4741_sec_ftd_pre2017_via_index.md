# ops 4741 -- pre-2017 FTD files via SEC's own index pages

**Status:** success  
**Duration:** 251.5s  
**Finished:** 2026-08-16T15:24:25+00:00  

## Data

| check | value |
|---|---|
| banked_before | 209 |
| distinct_files_discovered | 409 |
| new_files_to_bank | 200 |
| banked_this_run | 200 |
| errors_this_run | 0 |
| total_files_banked | 409 |
| earliest_tag_banked | 200907a |
| latest_tag_banked | 202607a |
| total_rows_banked | 23328640 |

## Log
- `15:20:14` https://www.sec.gov/data/foiadocsfailsdatahtm -> 409 cnsfails hrefs
- `15:20:14` https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data -> 409 cnsfails hrefs
- `15:20:14` https://www.sec.gov/foia/docs/failsdata.htm -> 409 cnsfails hrefs
- `15:20:14` sample new: 200907a, 200907b, 200908a, 200908b, 200909a, 200909b
- `15:20:14` sample url: https://www.sec.gov/files/data/frequently-requested-foia-document-fails-deliver-data/cnsfails200907a.zip
- `15:20:46`   progress: 25 (latest 201007a, 55123 rows)
- `15:21:19`   progress: 50 (latest 201107b, 66417 rows)
- `15:21:50`   progress: 75 (latest 201208a, 61607 rows)
- `15:22:21`   progress: 100 (latest 201308b, 65662 rows)
- `15:22:52`   progress: 125 (latest 201409a, 52529 rows)
- `15:23:24`   progress: 150 (latest 201509b, 70476 rows)
- `15:23:54`   progress: 175 (latest 201610a, 39389 rows)
- `15:24:24`   progress: 200 (latest 202605b, 55391 rows)
## Result

- `15:24:25` ✅ archive now 409 files / 23328640 rows, permanent
