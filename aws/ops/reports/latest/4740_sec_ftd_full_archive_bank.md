# ops 4740 -- SEC CNS fails-to-deliver: full archive -> permanent warm

**Status:** success  
**Duration:** 393.6s  
**Finished:** 2026-08-16T15:18:25+00:00  

## Data

| check | value |
|---|---|
| tags_total | 544 |
| already_banked | 0 |
| known_404 | 0 |
| banked_this_run | 209 |
| errors_this_run | 0 |
| new_404_this_run | 335 |
| total_files_banked | 209 |
| earliest_tag_banked | 201706b |
| latest_tag_banked | 202607a |
| total_rows_banked | 11332197 |
| total_gz_mb | 263.9 |
| stopped_for_time_cap | False |

## Log
- `15:15:31`   progress: 25 banked this run (latest 201806b, 55807 rows)
- `15:15:54`   progress: 50 banked this run (latest 201907a, 41026 rows)
- `15:16:22`   progress: 75 banked this run (latest 202011a, 45357 rows)
- `15:16:44`   progress: 100 banked this run (latest 202111b, 67349 rows)
- `15:17:07`   progress: 125 banked this run (latest 202212a, 57945 rows)
- `15:17:30`   progress: 150 banked this run (latest 202312b, 55140 rows)
- `15:17:52`   progress: 175 banked this run (latest 202501a, 44302 rows)
- `15:18:14`   progress: 200 banked this run (latest 202601b, 60729 rows)
## Result

- `15:18:25` 404 tags by year (naming-scheme gaps to chase against SEC's own index page, NOT assumed absent): 2004=24, 2005=24, 2006=24, 2007=24, 2008=24, 2009=24, 2010=24, 2011=24, 2012=24, 2013=24, 2014=24, 2015=24, 2016=24, 2017=11, 2019=1, 2020=6, 2026=5
- `15:18:25` ✅ warm bank now holds 209 half-month files, 11332197 raw rows, 263.9 MB gz -- under deny-Delete, versioned, permanent
