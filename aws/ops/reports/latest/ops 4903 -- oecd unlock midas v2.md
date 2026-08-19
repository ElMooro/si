# ops 4903 — OECD paced lane · MIDAS v2 · deep

**Status:** success  
**Duration:** 123.5s  
**Finished:** 2026-08-19T14:39:42+00:00  

## Data

| action | failures_after | failures_before | first | mode | n_complete | n_flows | n_links | recovered | retried_ok | snippet | stage | top | top_remaining | total | workers_override |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  | settle |  |  |  | True |
|  | 966 | 991 |  |  |  |  |  | 25 | 25 |  | oecd-paced-retry |  | [["HTTPError: HTTP Error 429: Too Many Requests", 956], ["HTTPError: HTTP Error 500: Internal Server E", 7], ["HTTPError: HTTP Error 404: Not Found", 3]] |  |  |
| created |  |  |  |  |  |  |  |  |  |  | schedule |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | statcan-histogram | [["HTTPError: HTTP Error 429: Too Many Requests", 106], ["URLError: <urlopen error [Errno 104] Connect", 83], ["URLError: <urlopen error [Errno 110] Connect", 72], ["HTTPError: HTTP Error 502: Bad Gateway", 24], ["HTTPError: HTTP Error 404: Not Found", 4], ["ValueError: tiny 0b", 1]] |  | 290 |  |
|  |  |  |  |  |  |  | 0 |  |  | Downloads</title>     <link rel="apple-touch-icon" sizes="180x180" href="/themes/custom/uswds_sec/assets/img/favicons/apple-touch-icon.png">     <link rel="icon" type="image/png" sizes="32x32" href="/themes/custom/uswds_sec/assets/img/favicons/favicon-32x32.png">     <link rel="icon" type="image/png" sizes="16x16" href="/themes/custom/uswds_sec/assets/img/favicons/favicon-16x16.png">     <link rel="manifest" href="/themes/custom/uswds_sec/assets/img/favicons/site.webmanifest">     <link rel="mask-icon" href="/themes/custom/uswds_sec/assets/img/favicons/safari-pinned-tab.svg" color="#2f64b2">     <meta name="msapplication-TileColor" content="#2f64b2">     <meta name="theme-color" content="#2f64b2">         <script type="module">!function(){var e=navigator.userAgent,a=document.documentElement,n=a.className;n=n.replace("no-js","js"),/iPad|iPhone|iPod/.test(e)&&!window.MSStream&&(n+=" ua-ios | midas-harvest |  |  |  |  |
|  |  |  |  | backfill | 30 | 48 |  |  |  |  | deep |  |  |  |  |

## Log
- `14:39:42` VERDICT: PASS_WITH_PENDING · {"walker_workers_deployed": "PASS", "oecd_failures_decreasing": "PASS", "oecd_drain_scheduled": "PASS", "statcan_audited": "PASS", "midas_harvest": "PENDING"}
- `14:39:42` report written: aws/ops/reports/4903.json
