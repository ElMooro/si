# ops 4904 — StatCan lane · deep truth · MIDAS v3

**Status:** success  
**Duration:** 63.4s  
**Finished:** 2026-08-19T14:45:14+00:00  

## Data

| action | diag | failures_after | failures_before | hrefs_market_or_data | json_refs | moving | parts_t0 | parts_t1 | recovered | script_srcs | stage | statcan_lane | status | top_remaining | verbatim_500 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  | settle | True |  |  |  |
|  |  | 175 | 290 |  |  |  |  |  | 115 |  | statcan-paced-retry |  |  | [["URLError: <urlopen error [Errno 104] Connect", 83], ["URLError: <urlopen error [Errno 110] Connect", 49], ["HTTPError: HTTP Error 502: Bad Gateway", 24], ["HTTPError: HTTP Error 429: Too Many Requests", 14], ["HTTPError: HTTP Error 404: Not Found", 4]] |  |
| created |  |  |  |  |  |  |  |  |  |  | schedule |  |  |  |  |
|  | {"mode": "backfill", "lease_free": true, "sample_flow": "CSEC", "window_status_hist": [["pending", 194], ["done", 28], ["err:HTTP502", 1]], "kicked": true} |  |  |  |  | False | 237 | 237 |  |  | deep-parts-truth |  |  |  |  |
|  |  |  |  | ["/data-research", "/data", "/data-research/taxonomies", "/data-research/statistics-data-visualizations", "/data-research/investment-management-data", "/featured-topics/market-structure-analytics", "/data-research/sec-data-resources", "/data-research/final-data-quality-assurance-guidelines", "/data-research/interactive-data-public-test-suite", "/da | [] |  |  |  |  | ["/files/js/js_N3Oj9ye2UfyARNS25SiHexg8ZcuBgk9ipMd2qH11yDw.js?scope=header&amp;delta=0&amp;language=en&amp;theme=uswds_sec&amp;include=eJxdjuEOgyAMhF8I5JFIhQa7FVhombqnn1G3zP1pel8vd8VFmcrdxdYfwAMe0qRaE6NXSC5t418PcIPlCrNhWGtXP3biiM2HLlozvbC5xHUEtqLrFp6MYPAQMxX38YBSLWJ3aKFEqxNm3I0YEzQnGZp6QWhh2vGTcBb33TzTaLrMUTZTOHPt2RtEfm4TQrTXj95AQmX5", "/modules/co | midas-v3 |  | 200 |  |   <!DOCTYPE html> <html lang="en" dir="ltr" prefix="og: https://ogp.me/ns#" class="no-js">   <head>     <meta charset="utf-8" /> <meta name="MobileOptimized" content="width" /> <meta name="HandheldFriendly" content="true" /> <meta name="viewport" content="width=device-width, initial-scale=1.0" /> <link rel="icon" href="/themes/custom/uswds_sec/asse |

## Log
- `14:45:14` VERDICT: PASS_WITH_PENDING · {"statcan_lane_deployed": "PASS", "statcan_failures_decreasing": "PASS", "statcan_drain_scheduled": "PASS", "deep_grinding": "PENDING", "midas_evidence": "PASS"}
- `14:45:14` report written: aws/ops/reports/4904.json
