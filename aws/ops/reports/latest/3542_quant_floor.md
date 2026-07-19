# ops 3542 — quant floor

**Status:** success  
**Duration:** 146.8s  
**Finished:** 2026-07-19T22:03:55+00:00  

## Log
- `22:01:28` PASS  P1_ci — {'beta': 2.0, 'bt_excess_1y': 12.3}
- `22:01:28`   zip: 89502 bytes
## 1. Lambda

- `22:01:29`   Lambda exists — updating
- `22:01:32` ✅   ✓ updated justhodl-fundamental-census
- `22:01:34`   zip: 81983 bytes
## 1. Lambda

- `22:01:34`   Lambda missing — creating
- `22:01:39` ✅   ✓ created justhodl-screen-backtest
- `22:01:40` ✅   ✓ Function URL: https://qwegtcji2yjk2n3efc7wc76ygq0wolru.lambda-url.us-east-1.on.aws/
- `22:01:40` PASS  P2_bt_url — https://qwegtcji2yjk2n3efc7wc76ygq0wolru.lambda-url.us-east-1.on.aws
- `22:02:52` FAIL  P3_matrix — {'tech_n': 495, 'beta_n': 494, 'short_n': 496, 'insider_n': (496, 496), 'retail_n': 0, 'n_dt': 214, 'n_db': 300, 'n_gc': 77, 'combo_top10': [('INCY', 99.5), ('FTNT', 97.4), ('AAPL', 97.4), ('ALL', 96.3), ('TRV', 95.9), ('PRU', 92.9), ('BBY', 92.0), ('MAS', 91.9), ('ANET', 91.4), ('NTAP', 91.4)], 'conviction_top5': [('AAPL', 85.5), ('MU', 81.6), ('LRCX', 81.6), ('DXCM', 81.5), ('SNDK', 80.8)], 'double_bottoms': ['WAT', 'NVDA', 'GEN', 'CMS', 'VST', 'MCHP', 'UNP', 'WY', 'PNW', 'AVGO', 'LMT', 'CVNA']}
- `22:02:54` PASS  P4_bt_smoke — {'weeks': 320, 'basket_3y': 80.0, 'spx_3y': 18.0, 'excess_3y': 62.0}
- `22:03:55` PASS  P5_page — {'node': True}
