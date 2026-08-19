# ops 4908 — deep v1.3 · OECD 15-min · MIDAS hop3

**Status:** success  
**Duration:** 453.6s  
**Finished:** 2026-08-19T16:33:31+00:00  

## Data

| action | failures_after | failures_before | files | grew | mode | n | n_complete | n_files | next_hops | parts_after | parts_before | per | recovered | stage | status | v13 | verbatim |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | deep-settle |  | True |  |
| updated-to-15min |  |  |  |  |  |  |  |  |  |  |  | 60 |  | oecd-crank |  |  |  |
|  | 879 | 930 |  |  |  |  |  |  |  |  |  |  | 51 | oecd-pass |  |  |  |
|  |  |  |  |  |  | 5 |  |  |  |  |  |  |  | statcan-residual |  |  | {"12100153": "HTTPError: HTTP Error 404: Not Found", "12100154": "HTTPError: HTTP Error 404: Not Found", "12100155": "HTTPError: HTTP Error 404: Not Found", "12100156": "HTTPError: HTTP Error 404: Not Found", "13100019": "ValueError: tiny 0b"} |
|  |  |  | ["/files/opa/data/market-structure/metrics-individual-security-exchange/individual_security_exchange_2026_q2.zip", "/files/opa/data/market-structure/metrics-individual-security-exchange/individual_security_exchange_2026_q1.zip", "/files/opa/data/market-structure/metrics-individual-security-exchange/individual_security_exchange_2025_q4.zip", "/files/opa/data/market-structure/met |  |  |  |  | 16 | ["/files/css/css_mSy9X9KRt_QHVJ1kv5BIiBWhNbCfaLtydbpeXjfD8NE.css?delta=0&amp;language=en&amp;theme=uswds_sec&amp;include=eJxNjUEOwyAMBD9EyqnvQQ5YiMbgiIUm6euLmkTqxdLYs2u_cEhN69Ot0mMqbiW_uFQCl-ZmUb_YfzBeCDhsZoAiw3itbEPtK8mDXrSfi6I1k6QPG96bpLLcyoVG6NA-OnuSwNX5jqZ5-NVG0ZlkQjuGGA3Yu8rCBIa9NWpJCwwONM52Hjf |  |  |  |  | midas-hop3 | 200 |  |  |
|  |  |  |  | False | backfill |  | 30 |  |  | 243 | 243 |  |  | deep-cure-proof |  |  |  |

## Log
- `16:33:31` VERDICT: PASS_WITH_PENDING · {"deep_v13_deployed": "PASS", "oecd_cranked": "PASS", "statcan_residual_classified": "PASS", "midas_hop3": "PASS", "deep_unblocked": "PENDING"}
- `16:33:31` report written: aws/ops/reports/4908.json
