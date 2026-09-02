# ops 5100 -- global cycle v3: feature store birth + v3.0.0 engine verification

**Status:** failure  
**Duration:** 69.0s  
**Finished:** 2026-09-02T00:42:15+00:00  

## Error

```
SystemExit: 1
```

## Data

| activity | basis | cli | conf | countries | eq_cli | equity | error | expr | financial | iso | last_update | latest | memory | n_feat | ok | phase | source | state | step | survey | timeout | trade |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  | Successful |  | 2048 |  |  |  |  | Active | S1 |  | 600 |  |
|  |  |  |  |  |  |  |  | cron(30 10 * * ? *) |  |  |  |  |  |  |  |  |  | ENABLED | S2 |  |  |  |
|  |  |  |  | 17 |  |  |  |  |  |  |  | 2026-06 |  |  | True |  | oecd_cli |  |  |  |  |  |
|  |  |  |  | None |  |  |  |  |  |  |  | None |  |  | True |  | oecd_kei |  |  |  |  |  |
|  |  |  |  | 27 |  |  |  |  |  |  |  | None |  |  | True |  | oecd_lfs |  |  |  |  |  |
|  |  |  |  | 34 |  |  |  |  |  |  |  | None |  |  | True |  | bis_WS_TC |  |  |  |  |  |
|  |  |  |  | 34 |  |  |  |  |  |  |  | None |  |  | True |  | bis_WS_SPP |  |  |  |  |  |
|  |  |  |  | 8 |  |  |  |  |  |  |  | None |  |  | True |  | bis_WS_EER |  |  |  |  |  |
|  |  |  |  | 34 |  |  |  |  |  |  |  | None |  |  | True |  | bis_WS_CREDIT_GAP |  |  |  |  |  |
|  |  |  |  | 28 |  |  |  |  |  |  |  | None |  |  | True |  | bis_WS_DSR |  |  |  |  |  |
|  |  |  |  | 17 |  |  |  |  |  |  |  | None |  |  | True |  | eurostat_EI_BSCO_M |  |  |  |  |  |
|  |  |  |  | 17 |  |  |  |  |  |  |  | None |  |  | True |  | eurostat_EI_BSSI_M_R2 |  |  |  |  |  |
|  |  |  |  | 0 |  |  |  |  |  |  |  | None |  |  | True |  | eurostat_EI_BSIN_M_R2 |  |  |  |  |  |
|  |  |  |  | 22 |  |  |  |  |  |  |  | None |  |  | True |  | eurostat_EI_LMHR_M |  |  |  |  |  |
|  |  |  |  | None |  |  |  |  |  |  |  | 2026-06 |  |  | True |  | asia_leads_korea |  |  |  |  |  |
|  |  |  |  | None |  |  |  |  |  |  |  | None |  |  | True |  | global_sovereign_curve_nowcast |  |  |  |  |  |
| -0.486 | multi-pillar | 98.8 | 0.38 |  | 100.64 | -0.537 |  |  | -0.459 | AUS |  |  |  | 6 |  | RECOVERY |  |  |  | 0.449 |  | None |
| -0.061 | multi-pillar | 97.95 | 0.5 |  | 110.64 | 1.624 |  |  | -0.358 | AUT |  |  |  | 9 |  | RECESSION |  |  |  | -0.701 |  | None |
| -0.332 | multi-pillar | 96.67 | 0.6 |  | 105.77 | 1.116 |  |  | -0.263 | BEL |  |  |  | 10 |  | RECESSION |  |  |  | -0.805 |  | None |
| 0.147 | multi-pillar | 103.12 | 0.38 |  | 105.74 | 0.569 |  |  | -0.5 | BRA |  |  |  | 8 |  | EXPANSION |  |  |  | 0.919 |  | None |
| -0.126 | multi-pillar | 104.32 | 0.38 |  | 105.66 | 0.645 |  |  | -0.328 | CAN |  |  |  | 8 |  | AT_RISK |  |  |  | 1.251 |  | None |
| -1.081 | multi-pillar | 96.57 | 0.38 |  | 104.6 | 1.476 |  |  | 0.146 | CHE |  |  |  | 8 |  | RECESSION |  |  |  | -0.799 |  | None |
| None | thin | 104.71 | 0.19 |  | 104.71 | 0.063 |  |  | -0.764 | CHL |  |  |  | 4 |  | EXPANSION |  |  |  | None |  | None |
| -0.842 | multi-pillar | 93.7 | 0.64 |  | 101.58 | 0.027 |  |  | -1.303 | CHN |  |  |  | 7 |  | RECOVERY |  |  |  | -0.274 |  | None |
| -0.088 | multi-pillar | 102.66 | 0.67 |  | 108.0 | 0.277 |  |  | 0.479 | CZE |  |  |  | 9 |  | AT_RISK |  |  |  | 0.317 |  | None |
| -0.247 | multi-pillar | 96.48 | 0.58 |  | 103.29 | -0.173 |  |  | -0.238 | DEU |  |  |  | 9 |  | RECOVERY |  |  |  | -0.555 |  | None |
| 0.502 | multi-pillar | 98.44 | 0.33 |  | 103.97 | 1.183 |  |  | 0.043 | DNK |  |  |  | 9 |  | RECOVERY |  |  |  | -1.058 |  | None |
| -0.081 | multi-pillar | 101.56 | 0.45 |  | 108.27 | 0.881 |  |  | 0.247 | ESP |  |  |  | 10 |  | AT_RISK |  |  |  | 0.021 |  | None |
| 0.76 | multi-pillar | 102.11 | 0.38 |  | 106.06 | 1.197 |  |  | -0.752 | FIN |  |  |  | 8 |  | EXPANSION |  |  |  | 0.305 |  | None |
| -0.397 | multi-pillar | 96.88 | 0.52 |  | 101.77 | 0.052 |  |  | -0.45 | FRA |  |  |  | 10 |  | RECOVERY |  |  |  | -0.276 |  | None |
| 0.503 | multi-pillar | 98.42 | 0.32 |  | 104.03 | 0.81 |  |  | -0.661 | GBR |  |  |  | 7 |  | RECESSION |  |  |  | -0.454 |  | None |
| -0.085 | multi-pillar | 102.58 | 0.47 |  | 111.69 | None |  |  | 0.147 | GRC |  |  |  | 8 |  | EXPANSION |  |  |  | 0.535 |  | None |
| 0.135 | multi-pillar | 101.27 | 0.42 |  | 109.73 | -0.096 |  |  | -0.174 | HUN |  |  |  | 9 |  | AT_RISK |  |  |  | 0.401 |  | None |
| -0.765 | multi-pillar | 98.07 | 0.45 |  | 97.4 | -0.863 |  |  | -0.141 | IDN |  |  |  | 5 |  | RECOVERY |  |  |  | 0.286 |  | None |
| 0.066 | multi-pillar | 101.47 | 0.45 |  | 98.91 | -1.134 |  |  | 0.273 | IND |  |  |  | 5 |  | EXPANSION |  |  |  | 0.469 |  | None |
| -0.896 | multi-pillar | 95.02 | 0.56 |  | 106.58 | 0.836 |  |  | -0.43 | IRL |  |  |  | 8 |  | RECESSION |  |  |  | -0.727 |  | None |
| 1.044 | multi-pillar | 103.3 | 0.25 |  | 104.76 | -0.252 |  |  | -0.001 | ISR |  |  |  | 6 |  | EXPANSION |  |  |  | None |  | None |
| -0.127 | multi-pillar | 99.04 | 0.38 |  | 105.94 | 0.393 |  |  | 0.334 | ITA |  |  |  | 10 |  | RECESSION |  |  |  | -0.526 |  | None |
| 0.352 | multi-pillar | 105.65 | 0.64 |  | 108.9 | 0.614 |  |  | 0.56 | JPN |  |  |  | 7 |  | AT_RISK |  |  |  | 0.718 |  | None |
| -0.207 | multi-pillar | 104.29 | 0.38 |  | 114.93 | 0.822 |  |  | -0.511 | KOR |  |  |  | 8 |  | AT_RISK |  |  |  | 1.37 |  | None |
| -0.124 | multi-pillar | 103.82 | 0.25 |  | 100.15 | -0.517 |  |  | 0.066 | MEX |  |  |  | 6 |  | AT_RISK |  |  |  | 1.166 |  | None |
| 0.193 | multi-pillar | 100.9 | 0.42 |  | 105.94 | 1.439 |  |  | -0.318 | NLD |  |  |  | 9 |  | EXPANSION |  |  |  | -0.064 |  | None |
| 0.052 | multi-pillar | 100.01 | 0.17 |  | 105.53 | 0.815 |  |  | -0.365 | NOR |  |  |  | 6 |  | EXPANSION |  |  |  | None |  | None |
| None | thin | 101.93 | 0.08 |  | 101.93 | 0.508 |  |  | -0.419 | NZL |  |  |  | 3 |  | EXPANSION |  |  |  | None |  | None |
| -0.253 | multi-pillar | 100.63 | 0.38 |  | 109.81 | 0.417 |  |  | -0.021 | POL |  |  |  | 8 |  | EXPANSION |  |  |  | 0.203 |  | None |
| -0.174 | multi-pillar | 103.24 | 0.42 |  | 105.51 | 0.926 |  |  | 0.966 | PRT |  |  |  | 9 |  | AT_RISK |  |  |  | -0.015 |  | None |
| 0.758 | multi-pillar | 100.34 | 0.42 |  | 106.15 | 1.204 |  |  | -1.225 | SWE |  |  |  | 9 |  | AT_RISK |  |  |  | 0.185 |  | None |
| 0.193 | multi-pillar | 100.67 | 0.52 |  | 106.22 | -0.639 |  |  | -0.221 | TUR |  |  |  | 10 |  | EXPANSION |  |  |  | 0.403 |  | None |
| 0.154 | multi-pillar | 100.8 | 0.47 |  | 101.65 | 0.068 |  |  | -0.578 | USA |  |  |  | 8 |  | AT_RISK |  |  |  | 0.51 |  | None |
| -0.104 | multi-pillar | 101.04 | 0.38 |  | 103.0 | -0.161 |  |  | -0.073 | ZAF |  |  |  | 6 |  | EXPANSION |  |  |  | 0.426 |  | None |

## Log
- `00:41:06` started 2026-09-02T00:41:06+00:00
## S1 deploy justhodl-cycle-features

- `00:41:06`   zip: 107236 bytes
## 1. Lambda

- `00:41:07`   Lambda exists — updating
- `00:41:11` ✅   ✓ updated justhodl-cycle-features
## S2 schedule

- `00:41:14` ✅ schedule created
## S3 first real run

- `00:41:34` ✅ manifest generated_at=2026-09-02T00:41:15+00:00 elapsed=12.0s n_countries=34
- `00:41:34`   feature counts: {"cli_oecd": 17, "bci": 23, "cci": 0, "esi": 17, "cons_conf_eu": 17, "ind_conf_eu": 0, "ip_yoy": 32, "retail_yoy": 28, "unemp_12m": 28, "credit_impulse": 34, "house_real_yoy": 34, "reer_12m": 8, "curve": 0, "exports_yoy": 1, "imports_yoy": 0, "credit_gap": 34, "dsr": 28}
- `00:41:34`   source oecd_cli: {"ok": true, "source": "live", "rows": 7319, "countries": 17, "latest": "2026-06"}
- `00:41:34`   source oecd_kei: {"ok": true, "file_age_h": 607.6, "measures_seen": {"PRVM": 135357, "TOVM": 43827, "PP": 27055, "H_EARN": 19487, "BCICP": 7711}, "features": {"ip_yoy": 32, "retail_yoy": 28, "bci": 23}}
- `00:41:34`   source oecd_lfs: {"ok": true, "countries": 27, "file_age_h": 313.5}
- `00:41:34`   source bis_WS_TC: {"ok": true, "countries": 34, "file_age_h": 616.3}
- `00:41:34`   source bis_WS_SPP: {"ok": true, "countries": 34}
- `00:41:34`   source bis_WS_EER: {"ok": true, "countries": 8}
- `00:41:34`   source bis_WS_CREDIT_GAP: {"ok": true, "countries": 34}
- `00:41:34`   source bis_WS_DSR: {"ok": true, "countries": 28}
- `00:41:34`   source eurostat_EI_BSCO_M: {"ok": true, "countries": 17, "file_age_h": 609.9}
- `00:41:34`   source eurostat_EI_BSSI_M_R2: {"ok": true, "countries": 17, "file_age_h": 609.9}
- `00:41:34`   source eurostat_EI_BSIN_M_R2: {"ok": true, "countries": 0, "file_age_h": 609.9}
- `00:41:34`   source eurostat_EI_LMHR_M: {"ok": true, "countries": 22, "file_age_h": 609.8}
- `00:41:34`   source asia_leads_korea: {"ok": true, "latest": "2026-06"}
- `00:41:34`   source global_sovereign_curve_nowcast: {"ok": true, "countries_extended": 0}
- `00:41:34`   AUS: 7 features · pillars ['activity', 'financial', 'survey'] · fresh 4 · cli_oecd@2026-06(3mo), unemp_12m@2026-05(4mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), reer_12m@2026-06(3mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   AUT: 10 features · pillars ['activity', 'financial', 'survey'] · fresh 7 · ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-01(8mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), reer_12m@2026-06(3mo), credit_gap@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   BEL: 11 features · pillars ['activity', 'financial', 'survey'] · fresh 6 · ip_yoy@2026-03(6mo), retail_yoy@2026-05(4mo), bci@2026-04(5mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), reer_12m@2026-06(3mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   BRA: 9 features · pillars ['activity', 'financial', 'survey'] · fresh 6 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), reer_12m@2026-06(3mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   CAN: 9 features · pillars ['activity', 'financial', 'survey'] · fresh 4 · cli_oecd@2026-06(3mo), ip_yoy@2026-04(5mo), retail_yoy@2026-04(5mo), unemp_12m@2026-05(4mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), reer_12m@2026-06(3mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   CHE: 9 features · pillars ['activity', 'financial', 'survey'] · fresh 4 · ip_yoy@2026-03(6mo), retail_yoy@2026-05(4mo), bci@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), reer_12m@2026-06(3mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), unemp_12m@2026-03(6mo)
- `00:41:34`   CHL: 6 features · pillars ['activity', 'financial'] · fresh 1 · ip_yoy@2023-10(35mo), unemp_12m@2010-03(198mo), credit_impulse@2025-12(9mo), house_real_yoy@2025-12(9mo), reer_12m@2026-06(3mo), credit_gap@2025-12(9mo)
- `00:41:34`   CHN: 8 features · pillars ['activity', 'financial', 'survey'] · fresh 5 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), bci@2026-05(4mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), reer_12m@2026-06(3mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   CZE: 10 features · pillars ['activity', 'financial', 'survey'] · fresh 7 · ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-05(4mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   DEU: 11 features · pillars ['activity', 'financial', 'survey'] · fresh 7 · cli_oecd@2026-06(3mo), ip_yoy@2023-12(33mo), retail_yoy@2026-05(4mo), bci@2026-05(4mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   DNK: 10 features · pillars ['activity', 'financial', 'survey'] · fresh 5 · ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-04(5mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2025-12(9mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   ESP: 11 features · pillars ['activity', 'financial', 'survey'] · fresh 8 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-06(3mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   FIN: 10 features · pillars ['activity', 'financial', 'survey'] · fresh 6 · ip_yoy@2023-12(33mo), retail_yoy@2026-05(4mo), bci@2026-05(4mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   FRA: 11 features · pillars ['activity', 'financial', 'survey'] · fresh 6 · cli_oecd@2026-06(3mo), ip_yoy@2026-04(5mo), retail_yoy@2026-05(4mo), bci@2026-04(5mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   GBR: 9 features · pillars ['activity', 'financial', 'survey'] · fresh 5 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), retail_yoy@2026-06(3mo), bci@2026-06(3mo), unemp_12m@2025-12(9mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   GRC: 9 features · pillars ['activity', 'financial', 'survey'] · fresh 6 · ip_yoy@2026-05(4mo), retail_yoy@2026-04(5mo), bci@2026-05(4mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   HUN: 10 features · pillars ['activity', 'financial', 'survey'] · fresh 6 · ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-04(5mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   IDN: 6 features · pillars ['activity', 'financial', 'survey'] · fresh 2 · cli_oecd@2026-06(3mo), ip_yoy@2026-03(6mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   IND: 6 features · pillars ['activity', 'financial', 'survey'] · fresh 3 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   IRL: 9 features · pillars ['activity', 'financial', 'survey'] · fresh 6 · ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-01(8mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   ISR: 6 features · pillars ['activity', 'financial'] · fresh 2 · ip_yoy@2026-04(5mo), retail_yoy@2026-04(5mo), unemp_12m@2026-05(4mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo)
- `00:41:34`   ITA: 11 features · pillars ['activity', 'financial', 'survey'] · fresh 7 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-04(5mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   JPN: 8 features · pillars ['activity', 'financial', 'survey'] · fresh 4 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2025-12(9mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   KOR: 10 features · pillars ['activity', 'financial', 'survey', 'trade'] · fresh 7 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-05(4mo), unemp_12m@2026-05(4mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), exports_yoy@2026-06(3mo)
- `00:41:34`   MEX: 8 features · pillars ['activity', 'financial', 'survey'] · fresh 4 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), unemp_12m@2004-12(261mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   NLD: 10 features · pillars ['activity', 'financial', 'survey'] · fresh 5 · ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-03(6mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2025-12(9mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   NOR: 7 features · pillars ['activity', 'financial'] · fresh 4 · ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   NZL: 3 features · pillars ['financial'] · fresh 1 · credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo)
- `00:41:34`   POL: 10 features · pillars ['activity', 'financial', 'survey'] · fresh 6 · ip_yoy@2026-05(4mo), retail_yoy@2026-06(3mo), bci@2026-02(7mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   PRT: 10 features · pillars ['activity', 'financial', 'survey'] · fresh 7 · ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-05(4mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   SWE: 10 features · pillars ['activity', 'financial', 'survey'] · fresh 7 · ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-06(3mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   TUR: 11 features · pillars ['activity', 'financial', 'survey'] · fresh 8 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), bci@2026-06(3mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo), cons_conf_eu@2026-07(2mo), esi@2026-07(2mo)
- `00:41:34`   USA: 9 features · pillars ['activity', 'financial', 'survey'] · fresh 6 · cli_oecd@2026-06(3mo), ip_yoy@2026-06(3mo), retail_yoy@2026-05(4mo), bci@2026-06(3mo), unemp_12m@2026-06(3mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:34`   ZAF: 7 features · pillars ['activity', 'financial', 'survey'] · fresh 4 · cli_oecd@2026-06(3mo), ip_yoy@2026-05(4mo), retail_yoy@2026-05(4mo), credit_impulse@2025-12(9mo), house_real_yoy@2026-03(6mo), credit_gap@2025-12(9mo), dsr@2025-12(9mo)
- `00:41:35`   log: [cycle-features] OECD DF_CLI (live): 7319 rows, 17 countries, latest 2026-06
- `00:41:35`   log: [cycle-features] OECD KEI: measures ['PRVM', 'TOVM', 'BCICP', 'H_EARN', 'PP'] -> features {'ip_yoy': 32, 'retail_yoy': 28, 'bci': 23}
- `00:41:35`   log: [cycle-features] OECD LFS unemployment: 27 countries
- `00:41:35`   log: [cycle-features] BIS WS_TC credit impulse: 34 countries
- `00:41:35`   log: [cycle-features] BIS WS_SPP real house prices: 34 countries
- `00:41:35`   log: [cycle-features] BIS WS_EER REER 12m: 8 countries
- `00:41:35`   log: [cycle-features] Eurostat EI_BSCO_M -> cons_conf_eu: 17 countries
- `00:41:35`   log: [cycle-features] Eurostat EI_BSSI_M_R2 -> esi: 17 countries
- `00:41:35`   log: [cycle-features] Eurostat EI_BSIN_M_R2 -> ind_conf_eu: 0 countries
- `00:41:35`   log: [cycle-features] Eurostat EI_LMHR_M -> unemp_eu: 22 countries
- `00:41:35`   log: [cycle-features] global-sovereign: extended 0 curve series to the current month
## S4 GBC v3.0.0 deploy wait + run

- `00:41:35` ✅ GBC v3.0.0 deployed 2026-09-02T00:41:27.000+0000 after 0s
- `00:41:55`   polling… engine_version=2.1.2
- `00:42:15` ✅ fresh v3.0.0 feed generated_at=2026-09-02T00:42:10.372501+00:00 elapsed=34.3s
## S5 verify v3 output

- `00:42:15` composite: {"available": true, "features_generated_at": "2026-09-02T00:41:15+00:00", "features_age_h": 0.0, "features_version": "1.0.0", "countries_multi_pillar": 32, "countries_thin_or_equity": 2, "pillar_counts": {"survey": 30, "activity": 32, "financial": 32, "equity": 31}, "pillar_weights": {"survey": 0.35, "financial": 0.25, "activity": 0.2, "trade": 0.1, "equity": 0.1}, "method": "own-history z (120 obs, min 36, clip 3) per feature, sign-adjusted; pillar = mean; composite = weighted mean over present pillars; CLI = 100 + 20*tanh(z/2)"}
- `00:42:15` downturn_probability_6m: {"ok": true, "horizon_months": 6, "n_obs": 263, "base_rate": 0.384, "coefficients": {"intercept": -0.5866, "global_z": -1.3848, "d3": -1.4538}, "in_sample_auc": 0.727, "in_sample_hit_rate": 0.688, "sample_start": "2004-01", "sample_end": "2025-11", "probability_now": 0.349, "target": "GDP-weighted industrial production y/y (OECD KEI) six months ahead < 0", "caveat": "in-sample calibration on this platform's own history; a monitoring signal, not a validated forecast", "target_series_months": 305, "target_latest": "2026-05"}
- `00:42:15` global_composite_latest: {"period": "2026-09", "global_z": -0.014, "cli": 99.86, "n": 26, "weight_covered": 55.8, "expansion_breadth_pct": 16.1, "breadth_unweighted_pct": 53.8}
- `00:42:15` aggregate: phase=GLOBAL_PEAKING avg_cli=99.17 mix={"EXPANSION": 13.0, "AT_RISK": 44.1, "RECESSION": 9.7, "RECOVERY": 33.2} coverage=100.0
- `00:42:15`   AUS RECOVERY   cli=98.8 basis=multi-pillar eq_cli=100.64 eq_phase=EXPANSION conf=0.38 pillars=survey:0.449/1 activity:-0.486/1 financial:-0.459/3 equity:-0.537/1
- `00:42:15`   AUT RECESSION  cli=97.95 basis=multi-pillar eq_cli=110.64 eq_phase=EXPANSION conf=0.5 pillars=activity:-0.061/3 financial:-0.358/3 survey:-0.701/2 equity:1.624/1
- `00:42:15`   BEL RECESSION  cli=96.67 basis=multi-pillar eq_cli=105.77 eq_phase=EXPANSION conf=0.6 pillars=activity:-0.332/3 survey:-0.805/3 financial:-0.263/3 equity:1.116/1
- `00:42:15`   BRA EXPANSION  cli=103.12 basis=multi-pillar eq_cli=105.74 eq_phase=EXPANSION conf=0.38 pillars=survey:0.919/2 activity:0.147/2 financial:-0.5/3 equity:0.569/1
- `00:42:15`   CAN AT_RISK    cli=104.32 basis=multi-pillar eq_cli=105.66 eq_phase=EXPANSION conf=0.38 pillars=survey:1.251/1 activity:-0.126/3 financial:-0.328/3 equity:0.645/1
- `00:42:15`   CHE RECESSION  cli=96.57 basis=multi-pillar eq_cli=104.6 eq_phase=EXPANSION conf=0.38 pillars=activity:-1.081/3 survey:-0.799/1 financial:0.146/3 equity:1.476/1
- `00:42:15`   CHL EXPANSION  cli=104.71 basis=thin eq_cli=104.71 eq_phase=EXPANSION conf=0.19 pillars=financial:-0.764/3 equity:0.063/1
- `00:42:15`   CHN RECOVERY   cli=93.7 basis=multi-pillar eq_cli=101.58 eq_phase=AT_RISK conf=0.64 pillars=survey:-0.274/2 activity:-0.842/1 financial:-1.303/3 equity:0.027/1
- `00:42:15`   CZE AT_RISK    cli=102.66 basis=multi-pillar eq_cli=108.0 eq_phase=EXPANSION conf=0.67 pillars=activity:-0.088/3 survey:0.317/3 financial:0.479/2 equity:0.277/1
- `00:42:15`   DEU RECOVERY   cli=96.48 basis=multi-pillar eq_cli=103.29 eq_phase=EXPANSION conf=0.58 pillars=survey:-0.555/4 activity:-0.247/2 financial:-0.238/2 equity:-0.173/1
- `00:42:15`   DNK RECOVERY   cli=98.44 basis=multi-pillar eq_cli=103.97 eq_phase=EXPANSION conf=0.33 pillars=activity:0.502/3 survey:-1.058/3 financial:0.043/2 equity:1.183/1
- `00:42:15`   ESP AT_RISK    cli=101.56 basis=multi-pillar eq_cli=108.27 eq_phase=EXPANSION conf=0.45 pillars=survey:0.021/4 activity:-0.081/3 financial:0.247/2 equity:0.881/1
- `00:42:15`   FIN EXPANSION  cli=102.11 basis=multi-pillar eq_cli=106.06 eq_phase=EXPANSION conf=0.38 pillars=activity:0.76/2 survey:0.305/3 financial:-0.752/2 equity:1.197/1
- `00:42:15`   FRA RECOVERY   cli=96.88 basis=multi-pillar eq_cli=101.77 eq_phase=EXPANSION conf=0.52 pillars=survey:-0.276/4 activity:-0.397/3 financial:-0.45/2 equity:0.052/1
- `00:42:15`   GBR RECESSION  cli=98.42 basis=multi-pillar eq_cli=104.03 eq_phase=EXPANSION conf=0.32 pillars=survey:-0.454/2 activity:0.503/2 financial:-0.661/2 equity:0.81/1
- `00:42:15`   GRC EXPANSION  cli=102.58 basis=multi-pillar eq_cli=111.69 eq_phase=EXPANSION conf=0.47 pillars=activity:-0.085/3 survey:0.535/3 financial:0.147/2
- `00:42:15`   HUN AT_RISK    cli=101.27 basis=multi-pillar eq_cli=109.73 eq_phase=EXPANSION conf=0.42 pillars=activity:0.135/3 survey:0.401/3 financial:-0.174/2 equity:-0.096/1
- `00:42:15`   IDN RECOVERY   cli=98.07 basis=multi-pillar eq_cli=97.4 eq_phase=RECOVERY conf=0.45 pillars=survey:0.286/1 activity:-0.765/1 financial:-0.141/2 equity:-0.863/1
- `00:42:15`   IND EXPANSION  cli=101.47 basis=multi-pillar eq_cli=98.91 eq_phase=RECOVERY conf=0.45 pillars=survey:0.469/1 activity:0.066/1 financial:0.273/2 equity:-1.134/1
- `00:42:15`   IRL RECESSION  cli=95.02 basis=multi-pillar eq_cli=106.58 eq_phase=EXPANSION conf=0.56 pillars=activity:-0.896/3 financial:-0.43/2 survey:-0.727/2 equity:0.836/1
- `00:42:15`   ISR EXPANSION  cli=103.3 basis=multi-pillar eq_cli=104.76 eq_phase=AT_RISK conf=0.25 pillars=activity:1.044/3 financial:-0.001/2 equity:-0.252/1
- `00:42:15`   ITA RECESSION  cli=99.04 basis=multi-pillar eq_cli=105.94 eq_phase=EXPANSION conf=0.38 pillars=survey:-0.526/4 activity:-0.127/3 financial:0.334/2 equity:0.393/1
- `00:42:15`   JPN AT_RISK    cli=105.65 basis=multi-pillar eq_cli=108.9 eq_phase=AT_RISK conf=0.64 pillars=survey:0.718/1 activity:0.352/3 financial:0.56/2 equity:0.614/1
- `00:42:15`   KOR AT_RISK    cli=104.29 basis=multi-pillar eq_cli=114.93 eq_phase=AT_RISK conf=0.38 pillars=survey:1.37/2 activity:-0.207/3 financial:-0.511/2 equity:0.822/1
- `00:42:15`   MEX AT_RISK    cli=103.82 basis=multi-pillar eq_cli=100.15 eq_phase=AT_RISK conf=0.25 pillars=survey:1.166/1 activity:-0.124/2 financial:0.066/2 equity:-0.517/1
- `00:42:15`   NLD EXPANSION  cli=100.9 basis=multi-pillar eq_cli=105.94 eq_phase=EXPANSION conf=0.42 pillars=activity:0.193/3 survey:-0.064/3 financial:-0.318/2 equity:1.439/1
- `00:42:15`   NOR EXPANSION  cli=100.01 basis=multi-pillar eq_cli=105.53 eq_phase=AT_RISK conf=0.17 pillars=activity:0.052/3 financial:-0.365/2 equity:0.815/1
- `00:42:15`   NZL EXPANSION  cli=101.93 basis=thin eq_cli=101.93 eq_phase=EXPANSION conf=0.08 pillars=financial:-0.419/2 equity:0.508/1
- `00:42:15`   POL EXPANSION  cli=100.63 basis=multi-pillar eq_cli=109.81 eq_phase=EXPANSION conf=0.38 pillars=activity:-0.253/3 financial:-0.021/2 survey:0.203/2 equity:0.417/1
- `00:42:15`   PRT AT_RISK    cli=103.24 basis=multi-pillar eq_cli=105.51 eq_phase=EXPANSION conf=0.42 pillars=activity:-0.174/3 survey:-0.015/3 financial:0.966/2 equity:0.926/1
- `00:42:15`   SWE AT_RISK    cli=100.34 basis=multi-pillar eq_cli=106.15 eq_phase=EXPANSION conf=0.42 pillars=activity:0.758/3 survey:0.185/3 financial:-1.225/2 equity:1.204/1
- `00:42:15`   TUR EXPANSION  cli=100.67 basis=multi-pillar eq_cli=106.22 eq_phase=EXPANSION conf=0.52 pillars=survey:0.403/4 activity:0.193/3 financial:-0.221/2 equity:-0.639/1
- `00:42:15`   USA AT_RISK    cli=100.8 basis=multi-pillar eq_cli=101.65 eq_phase=EXPANSION conf=0.47 pillars=survey:0.51/2 activity:0.154/3 financial:-0.578/2 equity:0.068/1
- `00:42:15`   ZAF EXPANSION  cli=101.04 basis=multi-pillar eq_cli=103.0 eq_phase=EXPANSION conf=0.38 pillars=survey:0.426/1 activity:-0.104/2 financial:-0.073/2 equity:-0.161/1
- `00:42:15` multi-pillar countries: 32/34
- `00:42:15` ✅ composite history: 34 countries, global 273 points 2004-01..2026-09, size via S3 LastModified 2026-09-02 00:42:11+00:00
- `00:42:15`   USA history: 273 points; last 6: [{"period": "2026-04", "cli": 101.22, "phase": "EXPANSION", "pillars": {"survey": 0.43, "activity": 0.052, "financial": -0.621, "equity": 1.041}}, {"period": "2026-05", "cli": 102.33, "phase": "EXPANSION", "pillars": {"survey": 0.571, "activity": 0.207, "financial": -0.621, "equity": 1.253}}, {"period": "2026-06", "cli": 101.59, "phase": "EXPANSION", "pillars": {"survey": 0.51, "activity": 0.154, "financial": -0.621, "equity": 0.892}}, {"period": "2026-07", "cli": 103.43, "phase": "EXPANSION", "pillars": {"survey": 0.51, "activity": 0.154, "equity": 0.157}}, {"period": "2026-08", "cli": 103.29, "phase": "EXPANSION", "pillars": {"survey": 0.51, "activity": 0.071, "equity": 0.228}}, {"period": "2026-09", "cli": 100.68, "phase": "AT_RISK", "pillars": {"equity": 0.068}}]
## verdict

- `00:42:15` ✗ feature bci: 23 countries < 25
