# ops 3973 — fleet paths for dead vault symbols + JPEXPYY integrity

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-07-27T06:10:11+00:00  

## Data

| jpexpyy_equals_korea | n_dead |
|---|---|
| True |  |
|  | 106 |

## Log
## A. CONFIRM/CLEAR the JPEXPYY mislabel

- `06:10:11`   JPEXPYY in vault : status=LIVE value=47.96 src=fleet:data/asia-leads.json n_notes=2
- `06:10:11`   asia-leads korea_exports.yoy_pct = 47.96
- `06:10:11` ✗   CONFIRMED — Japan's symbol is publishing KOREA's export YoY
## B. asia-leads.json — every scalar path

- `06:10:11`   top-level keys: ['disclaimer', 'elapsed_s', 'engine', 'generated_at', 'korea_exports', 'korea_flash', 'korea_flash_tape', 'methodology', 'siblings', 'sources', 'taiwan_exports', 'taiwan_orders', 'version']
- `06:10:11`     korea_exports.chg_3m_pct = 30.41
- `06:10:11`     korea_exports.frequency = M
- `06:10:11`     korea_exports.last_period = 2026-04-01
- `06:10:11`     korea_exports.last_value = 85867480000.0
- `06:10:11`     korea_exports.n_obs = 136
- `06:10:11`     korea_exports.source = FRED XTEXVA01KRM667N
- `06:10:11`     korea_exports.yoy_pct = 47.96
- `06:10:11`     korea_flash.error = no 수출입 현황 item on board
- `06:10:11`     korea_flash.via = cf-edge /gov
- `06:10:11`     korea_flash_tape.articles_scanned = 66
- `06:10:11`     korea_flash_tape.latest.period = 2026-07-01..20
- `06:10:11`     korea_flash_tape.latest.published = 2026-07-21
- `06:10:11`     korea_flash_tape.latest.via = gnews
- `06:10:11`     korea_flash_tape.latest.yoy_pct = 52.3
- `06:10:11`     korea_flash_tape.method = news-tape
- `06:10:11`     taiwan_exports.chg_3m_pct = 57.36
- `06:10:11`     taiwan_exports.frequency = M
- `06:10:11`     taiwan_exports.label = Taiwan goods exports (monthly)
- `06:10:11`     taiwan_exports.last_period = 2026-05-01
- `06:10:11`     taiwan_exports.last_value = 78596.0
- `06:10:11`     taiwan_exports.n_obs = 137
- `06:10:11`     taiwan_exports.source = FRED VALEXPTWM052N
- `06:10:11`     taiwan_exports.yoy_pct = 48.33
- `06:10:11`     taiwan_orders.latest_usd_bn = 95.26
- `06:10:11`     taiwan_orders.levels_cached = 2
- `06:10:11`     taiwan_orders.period = Jun. 2026
- `06:10:11`     taiwan_orders.stage6_hit.sn = 464
- `06:10:11`     taiwan_orders.stage6_hit.title = Value of Export Orders
- `06:10:11`     taiwan_orders.via_stage1 = edge
- `06:10:11`     taiwan_orders.via_stage2 = edge
## C. boj-detail.json — Japan CPI / JGB

- `06:10:11`   top-level keys: ['balance_sheet', 'boj_injection_score', 'carry_read', 'carry_unwind_risk', 'cross_reference', 'elapsed_s', 'errors', 'generated_at', 'headline', 'inflation', 'jgb_10y', 'method', 'ok', 'policy_rate', 'rate_differential', 'schema_version', 'sources', 'stance_label', 'usdjpy']
- `06:10:11`     carry_unwind_risk.components.jgb_yield = 20
- `06:10:11`     generated_at = 2026-07-26T11:20:44.929658+00:00
- `06:10:11`     inflation.cpi_yoy_pct = -0.4
- `06:10:11`     jgb_10y.change_12m_pp = 1.25
- `06:10:11`     jgb_10y.change_6m_pp = 0.61
- `06:10:11`     jgb_10y.yield_pct = 2.67
- `06:10:11`     policy_rate.change_12m_pp = 0.364
- `06:10:11`     policy_rate.change_6m_pp = 0.284
- `06:10:11`     policy_rate.interbank_3m_pct = 1.274
- `06:10:11`     policy_rate.policy_rate_pct = 0.841
- `06:10:11`     policy_rate.rate_source = call money rate
- `06:10:11`     policy_rate.stance = TIGHTENING
- `06:10:11`     rate_differential.change_6m_pp = -0.16
- `06:10:11`     rate_differential.differential_pp = 2.04
- `06:10:11`     rate_differential.jp_10y_pct = 2.67
- `06:10:11`     rate_differential.us_10y_pct = 4.71
## D. china-liquidity.json — TSF / credit impulse

- `06:10:11`   top-level keys: ['credit_impulse', 'currency', 'dr_copper', 'elapsed_s', 'fred_failed', 'generated_at', 'interbank_rate', 'method', 'money', 'regime', 'regime_read', 'schema_version', 'series_resolved', 'tsf']
- `06:10:11`     credit_impulse.signal = credit decelerating — forward headwind
- `06:10:11`     credit_impulse.value_pp = -5.52
- `06:10:11`     dr_copper.copper_yoy_pct = 37.8
- `06:10:11`     money.m1_yoy_pct = 1.5
- `06:10:11`     money.m2_yoy_pct = 8.21
- `06:10:11`     series_resolved.m1 = MANMM101CNM189S
- `06:10:11`     series_resolved.m2 = MYAGM2CNM189N
- `06:10:11`     tsf.pboc_cn.flow_trn_cny = 26.56
- `06:10:11`     tsf.pboc_cn.label = PBoC CN TSF monthly flow (社会融资规模增量)
- `06:10:11`     tsf.pboc_cn.period = 2025-01..08 (cum)
- `06:10:11`     tsf.pboc_cn.title = 2025年8月社会融资规模增量统计数据报告
- `06:10:11`     tsf.pboc_cn.via = edge
- `06:10:11`     tsf.pboc_cn.yoy_delta_trn = 4.66
- `06:10:11`     tsf.source_annual = DBnomics NBS/A_A0L08
## E. candidate wirings for still-dead symbols

- `06:10:11`   still dead among the targets: ['TWEXPYY', 'JPIRYY', 'CNPPIYY', 'CNEAI', 'CNINTR', 'CNLIVRR', 'CN10Y', 'TWINTR', 'JPCIND', 'JPJV', 'JPMTO', 'JPGDG', 'TOPIX', 'USFER', 'EUFER', 'CHFER', 'USCF', 'USTOT', 'USBCOI', 'USSBSI']
- `06:10:11`   → match these against the paths printed in B/C/D. Only wire a symbol when the fleet path is THE SAME MEASURE for THE SAME COUNTRY. The JPEXPYY case is exactly what happens when that rule is skipped.
- `06:10:11` ✅   JPEXPYY integrity checked
- `06:10:11` ✅ PROBE DONE — paths dumped; wiring op follows for exact matches only
