# ops 6690 - data/universe.json: state, history, the builder's runs, the bank fallback, the consumers (read-only)

**Status:** success  
**Duration:** 15.0s  
**Finished:** 2026-10-27T18:31:59+00:00  

## Log
## A. the head

- `18:31:45` data/universe.json: 1435640 bytes, LastModified 2026-10-27T18:06:32+00:00, VersionId q70uKlaOPTPfciWurases9xvqMGLHQO7 | generated_at 2026-10-27T18:06:31+00:00 | total_stocks 5230 | by_cap_bucket {"nano": 1015, "micro": 907, "small": 1296, "mid": 1090, "large": 850, "mega": 72, "unknown": 0} | method universe_builder_v4_exhaustive_bisection | duration_s 9.6
- `18:31:45` stats: {"total_stocks": 5230, "by_cap_bucket": {"nano": 1015, "micro": 907, "small": 1296, "mid": 1090, "large": 850, "mega": 72, "unknown": 0}, "by_sector_top_10": {"Healthcare": 1032, "Technology": 802, "Financial Services": 781, "Industrials": 702, "Consumer Cyclical": 490, "Basic Materials": 313, "Real Estate": 267, "Energy": 260, "Communication Services": 222, "Consumer Defensive": 222}, "by_exchange": {"NASDAQ Global Select": 1109, "New York Stock Exchange": 1869, "NASDAQ": 611, "NASDAQ Global Market": 646, "New York Stock Exchange Arca": 236, "NASDAQ Capital Market": 758, "NYSE": 1}, "n_adr": 
## B. versions

- `18:31:45` bucket versioning: Enabled
- `18:31:45` versions listed: 7 (newest 2026-10-27T18:06:32+00:00, oldest 2026-10-16T19:53:27+00:00)
- `18:31:45`   10-27 18:06 |  1435640 B | total 5230 | generated 2026-10-27T18:06:31+00:00 | buckets {"nano": 1015, "micro": 907, "small": 1296, "mid": 1090, "large": 850, "mega": 72, "unknown": 0} | LATEST
- `18:31:45`   10-26 18:06 |      730 B | total 0 | generated 2026-10-26T18:06:56+00:00 | buckets {"nano": 0, "micro": 0, "small": 0, "mid": 0, "large": 0, "mega": 0, "unknown": 0}
- `18:31:46`   10-24 18:06 |  1435351 B | total 5240 | generated 2026-10-24T18:06:25+00:00 | buckets {"nano": 1015, "micro": 919, "small": 1298, "mid": 1093, "large": 843, "mega": 72, "unknown": 0}
- `18:31:46`   10-23 18:06 |  1434880 B | total 5228 | generated 2026-10-23T18:06:24+00:00 | buckets {"nano": 1013, "micro": 914, "small": 1292, "mid": 1095, "large": 843, "mega": 71, "unknown": 0}
- `18:31:46`   10-22 18:06 |  1434961 B | total 5229 | generated 2026-10-22T18:06:24+00:00 | buckets {"nano": 1016, "micro": 907, "small": 1295, "mid": 1096, "large": 844, "mega": 71, "unknown": 0}
- `18:31:46`   10-21 18:06 |  1435460 B | total 5229 | generated 2026-10-21T18:06:25+00:00 | buckets {"nano": 996, "micro": 929, "small": 1294, "mid": 1082, "large": 853, "mega": 75, "unknown": 0}
- `18:31:46`   10-16 19:53 |      729 B | total 0 | generated 2026-10-16T19:53:26+00:00 | buckets {"nano": 0, "micro": 0, "small": 0, "mid": 0, "large": 0, "mega": 0, "unknown": 0}
- `18:31:46` ✅ last good version: q70uKlaOPTPfciWurases9xvqMGLHQO7 (2026-10-27T18:06:32+00:00, 5230 stocks, generated 2026-10-27T18:06:31+00:00)
## C. the builder's runs since 2026-10-15

- `18:31:47` config: 512 MB / 420 s, LastModified 2026-10-26T23:29:20.000+0000, CodeSha256 VgDmZnfX/3p6
- `18:31:53` runs found: 11
- `18:31:53` run 10-15 05:48 | failed ranges 0
- `18:31:53`      [universe] mega ($200000M-): raw=131
- `18:31:53`      [universe] large ($10000M-): raw=1858
- `18:31:53`      [universe] small ($300M-): raw=3591
- `18:31:53`      [universe] nano ($1M-): raw=2517
- `18:31:53`      [universe] micro ($50M-): raw=2979
- `18:31:53`      [universe] mid ($2000M-): raw=2493
- `18:31:53`      [universe] combined: 13569 unique symbols
- `18:31:53`      [universe] valid: 5158 (filtered industry: 8319, symbol: 23, other: 69)
- `18:31:53`      [universe] curated seed added: 5
- `18:31:53`      [universe] wrote 1414266b
- `18:31:53`      [universe] by_bucket: {"nano": 962, "micro": 912, "small": 1281, "mid": 1087, "large": 848, "mega": 73, "unknown": 0}
- `18:31:53`      REPORT RequestId: 9adc893b-6e9d-4a76-abed-bcc7e70a093e Duration: 3028.66 ms Billed Duration: 3550 ms Memory Size: 512 MB Max Memory Used: 140 MB Init Duration: 521.12 ms 
XRAY TraceId: 1-6acfc49c-0fc8b242355d4645117ff36d SegmentId: 6ebb0bea
- `18:31:53` run 10-16 19:53 | failed ranges 4
- `18:31:53`      [universe] range nano 1000000-50000000 failed: HTTP Error 429: Too Many Requests
- `18:31:53`      [universe] range mid 2000000000-10000000000 failed: HTTP Error 429: Too Many Requests[universe] range mega 200000000000-100000000000000 failed: HTTP Error 429: Too Many Requests[universe] range micro 
- `18:31:53`      [universe] micro ($50M-): raw=0
- `18:31:53`      [universe] mega ($200000M-): raw=0
- `18:31:53`      [universe] mid ($2000M-): raw=0
- `18:31:53`      [universe] small ($300M-): raw=0
- `18:31:53`      [universe] large ($10000M-): raw=0
- `18:31:53`      [universe] combined: 0 unique symbols
- `18:31:53`      [universe] valid: 0 (filtered industry: 0, symbol: 0, other: 0)
- `18:31:53`      [universe] curated seed added: 0
- `18:31:53`      [universe] wrote 729b
- `18:31:53`      [universe] by_bucket: {"nano": 0, "micro": 0, "small": 0, "mid": 0, "large": 0, "mega": 0, "unknown": 0}
- `18:31:53`      REPORT RequestId: d248f9fc-fbbf-4fcc-b708-b19174071662 Duration: 2547.31 ms Billed Duration: 3044 ms Memory Size: 512 MB Max Memory Used: 107 MB Init Duration: 496.22 ms 
XRAY TraceId: 1-6ad2679c-630b0fe670d1a1415e9bb315 SegmentId: 9b91b0c5
- `18:31:53` run 10-21 18:06 | failed ranges 0
- `18:31:53`      [universe] mega ($200000M-): raw=133
- `18:31:53`      [universe] large ($10000M-): raw=1866
- `18:31:53`      [universe] nano ($1M-): raw=2635
- `18:31:53`      [universe] mid ($2000M-): raw=2488
- `18:31:53`      [universe] micro ($50M-): raw=3029
- `18:31:53`      [universe] small ($300M-): raw=3641
- `18:31:53`      [universe] combined: 13792 unique symbols
- `18:31:53`      [universe] valid: 5224 (filtered industry: 8475, symbol: 25, other: 68)
- `18:31:53`      [universe] curated seed added: 5
- `18:31:53`      [universe] wrote 1435460b
- `18:31:53`      [universe] by_bucket: {"nano": 996, "micro": 929, "small": 1294, "mid": 1082, "large": 853, "mega": 75, "unknown": 0}
- `18:31:53`      REPORT RequestId: b68f3c1f-6011-4639-86c9-f673f300f7da Duration: 3958.19 ms Billed Duration: 4483 ms Memory Size: 512 MB Max Memory Used: 136 MB Init Duration: 524.14 ms 
XRAY TraceId: 1-6ad8ff1c-0a58907d3e3bc24b6afdcb83 SegmentId: 076a6e14
- `18:31:53` run 10-22 18:06 | failed ranges 0
- `18:31:53`      [universe] mega ($200000M-): raw=129
- `18:31:53`      [universe] large ($10000M-): raw=1856
- `18:31:53`      [universe] nano ($1M-): raw=2659
- `18:31:53`      [universe] small ($300M-): raw=3635
- `18:31:53`      [universe] mid ($2000M-): raw=2501
- `18:31:53`      [universe] micro ($50M-): raw=3010
- `18:31:53`      [universe] combined: 13790 unique symbols
- `18:31:53`      [universe] valid: 5224 (filtered industry: 8473, symbol: 25, other: 68)
- `18:31:53`      [universe] curated seed added: 5
- `18:31:53`      [universe] wrote 1434961b
- `18:31:53`      [universe] by_bucket: {"nano": 1016, "micro": 907, "small": 1295, "mid": 1096, "large": 844, "mega": 71, "unknown": 0}
- `18:31:53`      REPORT RequestId: 21cde47c-339b-4c40-81a3-7b41050cc63e Duration: 3509.04 ms Billed Duration: 3914 ms Memory Size: 512 MB Max Memory Used: 134 MB Init Duration: 404.37 ms 
XRAY TraceId: 1-6ada509c-5dcbdc6c1f9fa9b148af17c2 SegmentId: 74ca8a97
- `18:31:53` run 10-23 18:06 | failed ranges 0
- `18:31:53`      [universe] mega ($200000M-): raw=129
- `18:31:53`      [universe] large ($10000M-): raw=1855
- `18:31:53`      [universe] micro ($50M-): raw=3024
- `18:31:53`      [universe] nano ($1M-): raw=2657
- `18:31:53`      [universe] mid ($2000M-): raw=2492
- `18:31:53`      [universe] small ($300M-): raw=3633
- `18:31:53`      [universe] combined: 13790 unique symbols
- `18:31:53`      [universe] valid: 5223 (filtered industry: 8474, symbol: 25, other: 68)
- `18:31:53`      [universe] curated seed added: 5
- `18:31:53`      [universe] wrote 1434880b
- `18:31:53`      [universe] by_bucket: {"nano": 1013, "micro": 914, "small": 1292, "mid": 1095, "large": 843, "mega": 71, "unknown": 0}
- `18:31:53`      REPORT RequestId: 4d51eee3-a8fc-40bf-a371-875874c84f68 Duration: 2725.62 ms Billed Duration: 3263 ms Memory Size: 512 MB Max Memory Used: 135 MB Init Duration: 537.37 ms 
XRAY TraceId: 1-6adba21c-50aa67ba34d855a403325731 SegmentId: 68bbe863
- `18:31:53` run 10-24 18:06 | failed ranges 0
- `18:31:53`      [universe] mega ($200000M-): raw=130
- `18:31:53`      [universe] large ($10000M-): raw=1849
- `18:31:53`      [universe] nano ($1M-): raw=2659
- `18:31:53`      [universe] micro ($50M-): raw=3037
- `18:31:53`      [universe] mid ($2000M-): raw=2501
- `18:31:53`      [universe] small ($300M-): raw=3634
- `18:31:53`      [universe] combined: 13810 unique symbols
- `18:31:53`      [universe] valid: 5235 (filtered industry: 8482, symbol: 25, other: 68)
- `18:31:53`      [universe] curated seed added: 5
- `18:31:53`      [universe] wrote 1435351b
- `18:31:53`      [universe] by_bucket: {"nano": 1015, "micro": 919, "small": 1298, "mid": 1093, "large": 843, "mega": 72, "unknown": 0}
- `18:31:53`      REPORT RequestId: 954a3976-9e48-423d-b7cc-cab3ec342304 Duration: 3459.23 ms Billed Duration: 3903 ms Memory Size: 512 MB Max Memory Used: 130 MB Init Duration: 442.86 ms 
XRAY TraceId: 1-6adcf39c-58f2517e52c28232327ba6a7 SegmentId: a07654e5
- `18:31:53` run 10-25 18:06 | failed ranges 0
- `18:31:53`      [universe] mega ($200000M-): raw=130
- `18:31:53`      [universe] large ($10000M-): raw=1850
- `18:31:53`      [universe] micro ($50M-): raw=3037
- `18:31:53`      [universe] nano ($1M-): raw=2659
- `18:31:53`      [universe] mid ($2000M-): raw=2501
- `18:31:53`      [universe] small ($300M-): raw=3633
- `18:31:53`      [universe] combined: 13810 unique symbols
- `18:31:53`      [universe] valid: 5235 (filtered industry: 8482, symbol: 25, other: 68)
- `18:31:53`      REPORT RequestId: 74938f45-a168-4d59-9d1f-f0a2881abed1 Duration: 9122.59 ms Billed Duration: 9645 ms Memory Size: 512 MB Max Memory Used: 512 MB Init Duration: 521.55 ms Status: error Error Type: Runtime.OutOfMemory
XRAY TraceId: 1-6ade451c
- `18:31:53` run 10-25 18:07 | failed ranges 0
- `18:31:53`      [universe] mega ($200000M-): raw=130
- `18:31:53`      [universe] large ($10000M-): raw=1850
- `18:31:53`      [universe] mid ($2000M-): raw=2501
- `18:31:53`      [universe] nano ($1M-): raw=2659
- `18:31:53`      [universe] micro ($50M-): raw=3037
- `18:31:53`      [universe] small ($300M-): raw=3633
- `18:31:53`      [universe] combined: 13810 unique symbols
- `18:31:53`      [universe] valid: 5235 (filtered industry: 8482, symbol: 25, other: 68)
- `18:31:53`      REPORT RequestId: 74938f45-a168-4d59-9d1f-f0a2881abed1 Duration: 8341.13 ms Billed Duration: 8342 ms Memory Size: 512 MB Max Memory Used: 512 MB Status: error Error Type: Runtime.OutOfMemory
XRAY TraceId: 1-6ade451c-6f00a4b04eebee9b6207c32a
- `18:31:53` run 10-25 18:09 | failed ranges 0
- `18:31:53`      [universe] mega ($200000M-): raw=130
- `18:31:53`      [universe] mid ($2000M-): raw=2501
- `18:31:53`      [universe] micro ($50M-): raw=3037
- `18:31:53`      [universe] nano ($1M-): raw=2659
- `18:31:53`      [universe] large ($10000M-): raw=1850
- `18:31:53`      [universe] small ($300M-): raw=3633
- `18:31:53`      [universe] combined: 13810 unique symbols
- `18:31:53`      [universe] valid: 5235 (filtered industry: 8482, symbol: 25, other: 68)
- `18:31:53`      REPORT RequestId: 74938f45-a168-4d59-9d1f-f0a2881abed1 Duration: 8415.66 ms Billed Duration: 8416 ms Memory Size: 512 MB Max Memory Used: 512 MB Status: error Error Type: Runtime.OutOfMemory
XRAY TraceId: 1-6ade451c-6f00a4b04eebee9b6207c32a
- `18:31:53` run 10-26 18:06 | failed ranges 6
- `18:31:53`      [universe] range small 300000000-2000000000 failed: HTTP Error 429: Too Many Requests
- `18:31:53`      [universe] range micro 50000000-300000000 failed: HTTP Error 429: Too Many Requests
- `18:31:53`      [universe] small ($300M-): raw=0
- `18:31:53`      [universe] nano ($1M-): raw=0
- `18:31:53`      [universe] mega ($200000M-): raw=0
- `18:31:53`      [universe] micro ($50M-): raw=0
- `18:31:53`      [universe] large ($10000M-): raw=0
- `18:31:53`      [universe] mid ($2000M-): raw=0
- `18:31:53`      [universe] combined: 0 unique symbols
- `18:31:53`      [universe] valid: 0 (filtered industry: 0, symbol: 0, other: 0)
- `18:31:53`      [universe] curated seed added: 0
- `18:31:53`      [universe] wrote 730b
- `18:31:53`      [universe] by_bucket: {"nano": 0, "micro": 0, "small": 0, "mid": 0, "large": 0, "mega": 0, "unknown": 0}
- `18:31:53`      REPORT RequestId: 15b60f4b-4c63-473d-98bb-5eb053c6a4a0 Duration: 35263.35 ms Billed Duration: 35778 ms Memory Size: 512 MB Max Memory Used: 454 MB Init Duration: 513.89 ms 
XRAY TraceId: 1-6adf969c-13322b22270ba49a49c04919 SegmentId: e452d0
- `18:31:53` run 10-27 18:06 | failed ranges 0
- `18:31:53`      [universe] mega ($200000M-): raw=130
- `18:31:53`      [universe] large ($10000M-): raw=1862
- `18:31:53`      [universe] small ($300M-): raw=3635
- `18:31:53`      [universe] nano ($1M-): raw=2651
- `18:31:53`      [universe] micro ($50M-): raw=3012
- `18:31:53`      [universe] mid ($2000M-): raw=2496
- `18:31:53`      [universe] combined: 13786 unique symbols
- `18:31:53`      [universe] valid: 5225 (filtered industry: 8469, symbol: 25, other: 67)
- `18:31:53`      [universe] curated seed added: 5
- `18:31:53`      [universe] wrote 1435640b
- `18:31:53`      [universe] by_bucket: {"nano": 1015, "micro": 907, "small": 1296, "mid": 1090, "large": 850, "mega": 72, "unknown": 0}
- `18:31:53`      REPORT RequestId: 205cfccc-0060-441e-9c48-6a7c72bc3910 Duration: 9914.35 ms Billed Duration: 10556 ms Memory Size: 512 MB Max Memory Used: 139 MB Init Duration: 641.01 ms 
XRAY TraceId: 1-6ae0e81c-6a8ae7fb4035738c58f3309d SegmentId: cb341ff
## D. the bank's profile-bulk as a universe source

- `18:31:53` profile-bulk objects 12; latest parts 4; dated copies on 2 days (newest ['2026-10-24', '2026-10-25'])
- `18:31:53`   data/warm/fmp-bank/profile-bulk/part-0/latest.csv.gz 10500011 B LastModified 2026-10-25T23:13:14+00:00 meta {"sha256": "b3f514882a90f35cafc15c681ab0f63c2ea38c2d403eedad6e9b30f40ae5c7c2", "part": "0", "endpoint": "profile-bulk", "engine": "justhodl-fmp-bank", "rows": "23437"}
- `18:31:55`   data/warm/fmp-bank/profile-bulk/part-1/latest.csv.gz 9539512 B LastModified 2026-10-25T23:13:16+00:00 meta {"sha256": "d649368883cbe4a7d69ddb38ccf5253cde2fee45ce24af0c8fd9d304c5546264", "part": "1", "endpoint": "profile-bulk", "engine": "justhodl-fmp-bank", "rows": "23441"}
- `18:31:56`   data/warm/fmp-bank/profile-bulk/part-2/latest.csv.gz 9301575 B LastModified 2026-10-25T23:13:18+00:00 meta {"sha256": "53c7abfda40ed5ae148b4431503356c09cba82ed79bb4374d5392fa0dc06e841", "part": "2", "endpoint": "profile-bulk", "engine": "justhodl-fmp-bank", "rows": "23441"}
- `18:31:57`   data/warm/fmp-bank/profile-bulk/part-3/latest.csv.gz 8710100 B LastModified 2026-10-25T23:13:20+00:00 meta {"sha256": "8a8ea33d62b8155025e3ecb8b5ccb314efc333eb9f5c0155a51094fbe05fcd6f", "part": "3", "endpoint": "profile-bulk", "engine": "justhodl-fmp-bank", "rows": "23427"}
- `18:31:58` profile-bulk rows 93746 decoded in 4.5s; columns ['symbol', 'price', 'marketCap', 'beta', 'lastDividend', 'range', 'change', 'changePercentage', 'volume', 'averageVolume', 'companyName', 'currency', 'cik', 'isin', 'cusip', 'exchangeFullName', 'exchange', 'industry', 'website', 'description', 'ceo', 'sector', 'country', 'fullTimeEmployees', 'phone', 'address', 'city', 'state', 'zip', 'image', 'ipoDate', 'defaultImage', 'isEtf', 'isActivelyTrading', 'isAdr', 'isFund']
- `18:31:58` exchanges (top 12): [('NASDAQ', 14622), ('OTC', 14527), ('LSE', 7137), ('NYSE', 4663), ('BSE', 4437), ('JPX', 4361), ('AMEX', 4185), ('HKSE', 3181), ('NSE', 2949), ('ASX', 2703), ('XETRA', 2684), ('SHZ', 2511)]
- `18:31:58` bank-derived universe (builder filter): 5591 stocks, by bucket {"small": 1372, "large": 887, "nano": 1120, "mid": 1148, "mega": 93, "micro": 971}
- `18:31:58` vs last good screener universe (5230): both 5213 (99.7% of screener), only screener 17, only bank 378
- `18:31:58`    only screener (first 25): ['AETR', 'BACCR', 'BPMC', 'EXAS', 'FCDH', 'GPMT', 'GUNN', 'INFN', 'NUWE', 'OBTC', 'PPCB', 'PSTG', 'PTIV', 'QRVO', 'SBFM', 'TWG', 'VIVK']
- `18:31:58`    only bank (first 25): ['AACIU', 'AACIW', 'ABLVW', 'ACA', 'ACGLN', 'ACGLO', 'ADAMG', 'ADAMH', 'ADAMI', 'ADAML', 'ADAMM', 'ADAMN', 'ADAMO', 'ADAMZ', 'ADSEW', 'AERTW', 'AFRIW', 'AGM-A', 'AGNCL', 'AGNCM', 'AGNCN', 'AGNCO', 'AGNCP', 'AGNCZ', 'AIIOW']
- `18:31:58`    market-cap ratio |bank/screener - 1| median 0.023, p90 0.087
- `18:31:58`    same cap bucket: 5122 of 5213
## E. consumers: Lambda Errors / Invocations per UTC day, last 72 h

- `18:31:59` consumers with errors in 72 h: 9 of 43
- `18:31:59`    justhodl-earnings-pead: 12 errors / 13 invocations
- `18:31:59`    justhodl-eps-revision-velocity: 9 errors / 13 invocations
- `18:31:59`    justhodl-momentum-breakout: 6 errors / 7 invocations
- `18:31:59`    justhodl-revenue-acceleration: 6 errors / 7 invocations
- `18:31:59`    justhodl-volatility-squeeze-hunter: 6 errors / 7 invocations
- `18:31:59`    justhodl-microcap-float-squeeze: 3 errors / 5 invocations
- `18:31:59`    justhodl-options-flow-scanner: 3 errors / 5 invocations
- `18:31:59`    justhodl-bank-factors: 1 errors / 7 invocations
- `18:31:59`    justhodl-money-flow-state: 1 errors / 4 invocations
- `18:31:59` ✅ ops 6690 complete (read-only)
