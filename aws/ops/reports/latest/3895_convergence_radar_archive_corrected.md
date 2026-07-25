# ops 3895 — LIST the real convergence-radar archive keys, don't guess the format

**Status:** success  
**Duration:** 3.1s  
**Finished:** 2026-07-25T23:45:18+00:00  

## Data

| earliest | latest | n_archive_files | n_sustained_2plus | n_tickers_seen_at_all |
|---|---|---|---|---|
| data/archive/convergence-radar/20260601_1643.json | data/archive/convergence-radar/20260725_2330.json | 2613 |  |  |
|  |  |  | 42 | 54 |

## Log
## 1. list real keys under the archive prefix

- `23:45:17`   sample of 10 spread across the range: ['data/archive/convergence-radar/20260601_1643.json', 'data/archive/convergence-radar/20260607_0130.json', 'data/archive/convergence-radar/20260612_1200.json', 'data/archive/convergence-radar/20260617_2230.json', 'data/archive/convergence-radar/20260623_0830.json', 'data/archive/convergence-radar/20260628_1900.json', 'data/archive/convergence-radar/20260704_0530.json', 'data/archive/convergence-radar/20260709_1600.json', 'data/archive/convergence-radar/20260715_0130.json', 'data/archive/convergence-radar/20260720_1200.json']
## 2. pull a spread of ~8 archive snapshots across the full available range

- `23:45:17`   data/archive/convergence-radar/20260601_1643.json: 130 rows, sample keys of first row: ['avg_signal', 'convergence_score', 'domain_coverage', 'engines', 'is_accelerating', 'is_new_high', 'is_ultra', 'n_domains', 'n_engines', 'prior_n_engines', 'ticker', 'tier']
- `23:45:17`   data/archive/convergence-radar/20260608_1000.json: 137 rows, sample keys of first row: ['avg_signal', 'bearish_engines', 'bullish_engines', 'convergence_score', 'directional_score', 'domain_coverage', 'engines', 'exclude_from_longs', 'is_accelerating', 'is_new_high', 'is_ultra_new', 'n_bearish_eng', 'n_bullish_eng', 'n_domains', 'n_engines', 'n_neutral_eng', 'prior_n_engines', 'pump_category', 'pump_components', 'pump_likelihood', 'ticker', 'tier']
- `23:45:17`   data/archive/convergence-radar/20260615_0500.json: 158 rows, sample keys of first row: ['avg_signal', 'bearish_engines', 'bullish_engines', 'convergence_score', 'directional_score', 'domain_coverage', 'engines', 'exclude_from_longs', 'is_accelerating', 'is_new_high', 'is_ultra_new', 'n_bearish_eng', 'n_bullish_eng', 'n_domains', 'n_engines', 'n_neutral_eng', 'prior_n_engines', 'pump_category', 'pump_components', 'pump_likelihood', 'ticker', 'tier']
- `23:45:17`   data/archive/convergence-radar/20260621_2330.json: 191 rows, sample keys of first row: ['avg_signal', 'bearish_engines', 'bullish_engines', 'convergence_score', 'directional_score', 'domain_coverage', 'engines', 'exclude_from_longs', 'is_accelerating', 'is_new_high', 'is_ultra_new', 'n_bearish_eng', 'n_bullish_eng', 'n_domains', 'n_engines', 'n_neutral_eng', 'prior_n_engines', 'pump_category', 'pump_components', 'pump_likelihood', 'ticker', 'tier']
- `23:45:17`   data/archive/convergence-radar/20260628_1830.json: 200 rows, sample keys of first row: ['avg_signal', 'bearish_engines', 'bullish_engines', 'convergence_score', 'directional_score', 'domain_coverage', 'engines', 'exclude_from_longs', 'is_accelerating', 'is_new_high', 'is_ultra_new', 'n_bearish_eng', 'n_bullish_eng', 'n_domains', 'n_engines', 'n_neutral_eng', 'prior_n_engines', 'pump_category', 'pump_components', 'pump_likelihood', 'ticker', 'tier']
- `23:45:18`   data/archive/convergence-radar/20260705_1330.json: 199 rows, sample keys of first row: ['avg_signal', 'bearish_engines', 'bullish_engines', 'convergence_score', 'directional_score', 'domain_coverage', 'engines', 'exclude_from_longs', 'is_accelerating', 'is_new_high', 'is_ultra_new', 'n_bearish_eng', 'n_bullish_eng', 'n_domains', 'n_engines', 'n_neutral_eng', 'prior_n_engines', 'pump_category', 'pump_components', 'pump_likelihood', 'ticker', 'tier']
- `23:45:18`   data/archive/convergence-radar/20260712_0730.json: 179 rows, sample keys of first row: ['avg_signal', 'bearish_engines', 'bullish_engines', 'convergence_score', 'directional_score', 'domain_coverage', 'engines', 'exclude_from_longs', 'is_accelerating', 'is_new_high', 'is_ultra_new', 'n_bearish_eng', 'n_bullish_eng', 'n_domains', 'n_engines', 'n_neutral_eng', 'prior_n_engines', 'pump_category', 'pump_components', 'pump_likelihood', 'ticker', 'tier']
- `23:45:18`   data/archive/convergence-radar/20260719_0230.json: 200 rows, sample keys of first row: ['avg_signal', 'bearish_engines', 'bullish_engines', 'convergence_score', 'directional_score', 'domain_coverage', 'engines', 'exclude_from_longs', 'is_accelerating', 'is_new_high', 'is_ultra_new', 'n_bearish_eng', 'n_bullish_eng', 'n_domains', 'n_engines', 'n_neutral_eng', 'prior_n_engines', 'pump_category', 'pump_components', 'pump_likelihood', 'ticker', 'tier']
## 3. tickers flagged across MULTIPLE distinct snapshots (a sustained, not one-off, signal)

- `23:45:18`   AVGO: [('20260601_1643', 12), ('20260608_1000', 11), ('20260615_0500', 8), ('20260621_2330', 11), ('20260628_1830', 9), ('20260705_1330', 10), ('20260712_0730', 11), ('20260719_0230', 9)]
- `23:45:18`   AMD: [('20260601_1643', 8), ('20260608_1000', 8), ('20260615_0500', 5), ('20260621_2330', 9), ('20260628_1830', 7), ('20260705_1330', 9), ('20260712_0730', 8), ('20260719_0230', 11)]
- `23:45:18`   TSLA: [('20260601_1643', 9), ('20260608_1000', 9), ('20260615_0500', 5), ('20260621_2330', 10), ('20260628_1830', 8), ('20260705_1330', 10), ('20260712_0730', 12), ('20260719_0230', 12)]
- `23:45:18`   AMZN: [('20260601_1643', 8), ('20260608_1000', 8), ('20260615_0500', 6), ('20260621_2330', 10), ('20260628_1830', 10), ('20260705_1330', 10), ('20260712_0730', 11), ('20260719_0230', 10)]
- `23:45:18`   NVDA: [('20260601_1643', 9), ('20260608_1000', 10), ('20260615_0500', 7), ('20260621_2330', 10), ('20260628_1830', 8), ('20260705_1330', 9), ('20260712_0730', 9), ('20260719_0230', 10)]
- `23:45:18`   MU: [('20260601_1643', 9), ('20260608_1000', 8), ('20260615_0500', 5), ('20260621_2330', 10), ('20260628_1830', 8), ('20260705_1330', 8), ('20260712_0730', 7), ('20260719_0230', 7)]
- `23:45:18`   GOOG: [('20260601_1643', 6), ('20260608_1000', 6), ('20260615_0500', 5), ('20260621_2330', 8), ('20260628_1830', 7), ('20260705_1330', 8), ('20260712_0730', 8), ('20260719_0230', 8)]
- `23:45:18`   META: [('20260601_1643', 10), ('20260608_1000', 9), ('20260615_0500', 7), ('20260621_2330', 10), ('20260628_1830', 9), ('20260705_1330', 10), ('20260712_0730', 14), ('20260719_0230', 15)]
- `23:45:18`   MSFT: [('20260601_1643', 8), ('20260608_1000', 10), ('20260615_0500', 6), ('20260621_2330', 10), ('20260628_1830', 9), ('20260705_1330', 10), ('20260712_0730', 13), ('20260719_0230', 12)]
- `23:45:18`   AAPL: [('20260601_1643', 8), ('20260608_1000', 9), ('20260615_0500', 6), ('20260621_2330', 8), ('20260628_1830', 8), ('20260705_1330', 8), ('20260712_0730', 12), ('20260719_0230', 13)]
- `23:45:18`   GOOGL: [('20260601_1643', 6), ('20260608_1000', 6), ('20260615_0500', 4), ('20260621_2330', 8), ('20260628_1830', 7), ('20260705_1330', 8), ('20260712_0730', 9), ('20260719_0230', 7)]
- `23:45:18`   PLTR: [('20260601_1643', 8), ('20260608_1000', 9), ('20260615_0500', 5), ('20260621_2330', 7), ('20260628_1830', 5), ('20260712_0730', 6), ('20260719_0230', 7)]
- `23:45:18`   GE: [('20260601_1643', 6), ('20260608_1000', 5), ('20260615_0500', 5), ('20260621_2330', 6), ('20260628_1830', 6), ('20260705_1330', 7), ('20260712_0730', 6)]
- `23:45:18`   INTC: [('20260601_1643', 6), ('20260608_1000', 4), ('20260615_0500', 4), ('20260621_2330', 7), ('20260628_1830', 6), ('20260705_1330', 7), ('20260712_0730', 6)]
- `23:45:18`   ORCL: [('20260601_1643', 8), ('20260608_1000', 9), ('20260615_0500', 7), ('20260621_2330', 9), ('20260628_1830', 8), ('20260705_1330', 8), ('20260719_0230', 7)]
## 4. cross-check sustained tickers against REAL current price/flow (constituent-pressure)

- `23:45:18`     AVGO: perf_m=-0.04% perf_w=2.99% quadrant=NEUTRAL — flagged 8x in convergence-radar archive
- `23:45:18`     PLTR: perf_m=8.3% perf_w=-7.15% quadrant=DISTRIBUTION_RALLY — flagged 7x in convergence-radar archive
- `23:45:18`     AMD: perf_m=0.43% perf_w=5.28% quadrant=NEUTRAL — flagged 8x in convergence-radar archive
- `23:45:18`     TSLA: perf_m=-16.64% perf_w=-17.81% quadrant=NEUTRAL — flagged 8x in convergence-radar archive
- `23:45:18`     AMZN: perf_m=-0.92% perf_w=-6.12% quadrant=NEUTRAL — flagged 8x in convergence-radar archive
- `23:45:18`     NVDA: perf_m=3.94% perf_w=1.99% quadrant=NEUTRAL — flagged 8x in convergence-radar archive
- `23:45:18`     MU: perf_m=-12.17% perf_w=8.48% quadrant=STEALTH_ACCUMULATION — flagged 8x in convergence-radar archive
- `23:45:18`     GOOG: perf_m=-7.52% perf_w=-7.81% quadrant=NEUTRAL — flagged 8x in convergence-radar archive
- `23:45:18`     META: perf_m=6.73% perf_w=-7.87% quadrant=NEUTRAL — flagged 8x in convergence-radar archive
- `23:45:18`     GE: perf_m=-3.32% perf_w=1.4% quadrant=NEUTRAL — flagged 7x in convergence-radar archive
- `23:45:18`     INTC: perf_m=-29.87% perf_w=-2.86% quadrant=NEUTRAL — flagged 7x in convergence-radar archive
- `23:45:18`     MSFT: perf_m=4.44% perf_w=-3.08% quadrant=NEUTRAL — flagged 8x in convergence-radar archive
- `23:45:18`     ARM: perf_m=-27.59% perf_w=-2.69% quadrant=NEUTRAL — flagged 2x in convergence-radar archive
- `23:45:18`     ORCL: perf_m=-27.0% perf_w=-9.03% quadrant=NEUTRAL — flagged 7x in convergence-radar archive
- `23:45:18`     AAPL: perf_m=13.63% perf_w=-0.22% quadrant=NEUTRAL — flagged 8x in convergence-radar archive
- `23:45:18` ✅ PROBE COMPLETE
