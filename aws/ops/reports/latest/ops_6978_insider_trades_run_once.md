# 6978 - one run of the insider engine by hand, 2026-11-26T16:36+00:00

**Status:** success  
**Duration:** 50.7s  
**Finished:** 2026-11-26T16:36:57+00:00  

## Data

| buys | failed_checks | set_aside | total_usd |
|---|---|---|---|
| 706 | 0 | 1 | 831781966.01 |

## Log
- `16:36:07` before: version 3.0, written 2026-11-25T21:10:34+00:00, 628 buys, total $10,689,562,023
- `16:36:56` invoked once: status 200, function error None, answer {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": "{\"ok\": true, \"stats\": {\"total_buys\": 706, \"total_value_usd\": 831781966.01, \"unique_companies\": 300, \"unique_insiders\": 373, \"cluster_count\": 7, \"big_buy_count\": 50, \"fet
- `16:36:56` PASS  the run finished without an error
- `16:36:57` after: version 3.1, written 2026-11-26T16:36:55+00:00, 706 buys, total $831,781,966, clusters 7, big buys 50
- `16:36:57` price check: {"n_checked": 3909, "n_in_range": 3840, "n_off_tape": 33, "n_no_tape": 35, "n_set_aside": 1, "usd_set_aside_buys": 9919161280.9, "usd_set_aside_sells": 0.0, "tickers_on_the_book": 962}
- `16:36:57`    set aside: LONA buy 3,149,470 shares at 3149.47 = $9,919,161,281 | tape 2.71 | priced at 1,162.2 times the tape and worth more than the whole company (92,804% of its market value)
- `16:36:57` largest purchases now: NFRX $45,199,994, BBTX $37,361,744, KNOP $34,863,375, LYEL $24,999,992, LYEL $24,999,992, NFRX $24,462,564
- `16:36:57` PASS  the document is 3.1 and carries the price check
- `16:36:57` PASS  the LONA line is set aside and no longer among the purchases
- `16:36:57` PASS  the 30-day buying total no longer carries it ($831,781,966)
- `16:36:57` PASS  the rows set aside are kept as filed and the factor book answered
- `16:36:57` ✅ the insider document is right as of this run
