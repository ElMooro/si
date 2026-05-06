- `08:56:32`     source: 15642 chars

# 1) Build zip + create/update Lambda

- `08:56:32`     zip: 15792b
- `08:56:32`     creating
- `08:56:36`     ✓ deployed at 2026-05-06T08:56:32.763+0000

# 2) Schedule daily 10:00 UTC

- `08:56:37`     ✓ permission added

# 3) Smoke invoke

- `08:56:38`     status: 200, dur: 1.8s
- `08:56:38`     body: {"statusCode": 200, "body": "{\"n_classified\": 340, \"n_sectors\": 11, \"n_alerts\": 5, \"duration_s\": 1.0}"}
- `08:56:38`       START RequestId: 14fcde97-21a8-4a23-9725-407793564076 Version: $LATEST
- `08:56:38`       [sec-diff] starting v1.0
- `08:56:38`       [sec-diff] universe: 340 stocks
- `08:56:38`       [sec-diff] eps-velocity prior data: 218 tickers
- `08:56:38`       [sec-diff] classified: 340, skipped: 0
- `08:56:38`       [sec-diff] sectors: 11, industries: 45
- `08:56:38`       [sec-diff] wrote 57853b
- `08:56:38`       [sec-diff] top sectors: [('Industrials', 80.9), ('Communication Services', 90.0), ('Financial Services', 69.2), ('Consumer Cyclical', 67.6), ('Technology', 55.2)]
- `08:56:38`       END RequestId: 14fcde97-21a8-4a23-9725-407793564076
- `08:56:38`       REPORT RequestId: 14fcde97-21a8-4a23-9725-407793564076	Duration: 1045.02 ms	Billed Duration: 1578 ms	Memory Size: 1024 MB	Max Memory Used: 101 MB	Init Duration: 532.84 ms

# 4) Inspect output

- `08:56:39`     generated_at: 2026-05-06T08:56:38+00:00
- `08:56:39`     stats: {"n_universe": 340, "n_classified": 340, "n_skipped": 0, "n_sectors": 11, "n_industries": 45, "n_alerts": 5}
- `08:56:39`   
- `08:56:39`     ── TOP 15 SECTORS BY DIFFUSION ──
- `08:56:39`       🔥 Industrials                      n=47   up= 80.9%  strong= 21.3%  avg_lift=+95.8%  regime=BULLISH_ALL_IN
- `08:56:39`       🔥 Communication Services           n=10   up= 90.0%  strong= 10.0%  avg_lift=+14.8%  regime=BULLISH_ALL_IN
- `08:56:39`       ✓ Financial Services               n=39   up= 69.2%  strong= 20.5%  avg_lift=+13.5%  regime=BULLISH
- `08:56:39`       ✓ Consumer Cyclical                n=34   up= 67.6%  strong= 17.6%  avg_lift=+12.0%  regime=BULLISH
- `08:56:39`       ✓ Technology                       n=67   up= 55.2%  strong= 28.4%  avg_lift=+11.8%  regime=BULLISH
- `08:56:39`       ✓ Basic Materials                  n=11   up= 63.6%  strong=  9.1%  avg_lift= +9.9%  regime=BULLISH
- `08:56:39`       ✓ Healthcare                       n=49   up= 55.1%  strong=  6.1%  avg_lift=+18.1%  regime=BULLISH
- `08:56:39`       ✓ Real Estate                      n=19   up= 42.1%  strong= 10.5%  avg_lift= +6.9%  regime=NEUTRAL_BULLISH
- `08:56:39`       🔥 Utilities                        n=20   up= 85.0%  strong=  0.0%  avg_lift= +6.9%  regime=BULLISH_ALL_IN
- `08:56:39`       ✓ Consumer Defensive               n=14   up= 64.3%  strong=  0.0%  avg_lift= +9.3%  regime=BULLISH
- `08:56:39`       ○ Energy                           n=18   up= 27.8%  strong=  5.6%  avg_lift= +5.2%  regime=NEUTRAL_BEARISH
- `08:56:39`   
- `08:56:39`     ── TOP 15 INDUSTRIES BY DIFFUSION ──
- `08:56:39`       Aerospace & Defense                      n=6    up=100.0%  strong= 33.3%  avg_lift=+656.6%  regime=BULLISH_ALL_IN
- `08:56:39`       Hardware, Equipment & Parts              n=5    up=100.0%  strong= 80.0%  avg_lift=+22.6%  regime=BULLISH_ALL_IN
- `08:56:39`       Medical - Diagnostics & Research         n=9    up= 77.8%  strong= 11.1%  avg_lift=+63.4%  regime=BULLISH_ALL_IN
- `08:56:39`       Internet Content & Information           n=4    up= 75.0%  strong= 25.0%  avg_lift=+21.8%  regime=BULLISH_ALL_IN
- `08:56:39`       Computer Hardware                        n=3    up= 66.7%  strong= 66.7%  avg_lift=+16.7%  regime=BULLISH
- `08:56:39`       Asset Management                         n=7    up= 85.7%  strong= 28.6%  avg_lift=+12.4%  regime=BULLISH_ALL_IN
- `08:56:39`       Travel Services                          n=3    up=100.0%  strong= 66.7%  avg_lift=+17.5%  regime=BULLISH_ALL_IN
- `08:56:39`       Electrical Equipment & Parts             n=4    up= 50.0%  strong= 25.0%  avg_lift=+28.1%  regime=NEUTRAL_BULLISH
- `08:56:39`       Engineering & Construction               n=4    up= 75.0%  strong= 25.0%  avg_lift=+12.0%  regime=BULLISH_ALL_IN
- `08:56:39`       Financial - Capital Markets              n=3    up= 66.7%  strong= 33.3%  avg_lift=+15.2%  regime=BULLISH
- `08:56:39`       Industrial - Machinery                   n=11   up= 90.9%  strong= 27.3%  avg_lift=+11.0%  regime=BULLISH_ALL_IN
- `08:56:39`       Financial - Data & Stock Exchanges       n=5    up= 60.0%  strong= 20.0%  avg_lift=+20.1%  regime=BULLISH
- `08:56:39`       Medical - Devices                        n=6    up= 66.7%  strong= 16.7%  avg_lift= +8.7%  regime=BULLISH
- `08:56:39`       Software - Infrastructure                n=11   up= 63.6%  strong= 18.2%  avg_lift=+11.9%  regime=BULLISH
- `08:56:39`       Semiconductors                           n=14   up= 42.9%  strong= 42.9%  avg_lift=+16.9%  regime=NEUTRAL_BULLISH