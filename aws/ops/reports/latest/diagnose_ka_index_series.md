# Diagnose KA Index time series — why bimodal at 0/43?

**Status:** success  
**Duration:** 22.9s  
**Finished:** 2026-04-26T15:18:58+00:00  

## Log
## 1. archive/intelligence/ contains 612 keys in last 365d

- `15:18:36`   earliest: archive/intelligence/2026/02/23/0815.json  (2026-02-23 08:15:01+00:00)
- `15:18:36`   latest:   archive/intelligence/2026/04/26/1300.json  (2026-04-26 13:00:23+00:00)
- `15:18:36`   sample 5:
- `15:18:36`     archive/intelligence/2026/02/23/0815.json  (2026-02-23 08:15:01+00:00)
- `15:18:36`     archive/intelligence/2026/02/23/1205.json  (2026-02-23 12:05:43+00:00)
- `15:18:36`     archive/intelligence/2026/02/23/1210.json  (2026-02-23 12:10:48+00:00)
- `15:18:36`     archive/intelligence/2026/02/23/1305.json  (2026-02-23 13:05:43+00:00)
- `15:18:36`     archive/intelligence/2026/02/23/1405.json  (2026-02-23 14:05:43+00:00)
## 2. Sample values from first 20 files

- `15:18:36`   0815.json
- `15:18:36`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:36`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:36`     ka_index=None  khalid_index=49
- `15:18:36`   1205.json
- `15:18:36`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:36`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:36`     ka_index=None  khalid_index=49
- `15:18:36`   1210.json
- `15:18:36`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:36`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:36`     ka_index=None  khalid_index=49
- `15:18:36`   1305.json
- `15:18:36`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:36`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:36`     ka_index=None  khalid_index=49
- `15:18:36`   1405.json
- `15:18:36`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:36`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:36`     ka_index=None  khalid_index=49
- `15:18:37`   1505.json
- `15:18:37`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:37`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:37`     ka_index=None  khalid_index=49
- `15:18:37`   1605.json
- `15:18:37`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:37`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:37`     ka_index=None  khalid_index=49
- `15:18:37`   1705.json
- `15:18:37`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:37`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:37`     ka_index=None  khalid_index=49
- `15:18:37`   1805.json
- `15:18:37`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:37`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:37`     ka_index=None  khalid_index=49
- `15:18:37`   1905.json
- `15:18:37`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:37`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:37`     ka_index=None  khalid_index=49
- `15:18:37`   2005.json
- `15:18:37`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:37`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:37`     ka_index=None  khalid_index=49
- `15:18:37`   2105.json
- `15:18:37`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:37`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:37`     ka_index=None  khalid_index=49
- `15:18:37`   2205.json
- `15:18:37`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:37`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:37`     ka_index=None  khalid_index=49
- `15:18:37`   2305.json
- `15:18:37`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:37`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:37`     ka_index=None  khalid_index=49
- `15:18:38`   1205.json
- `15:18:38`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:38`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:38`     ka_index=None  khalid_index=49
- `15:18:38`   1210.json
- `15:18:38`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:38`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:38`     ka_index=None  khalid_index=49
- `15:18:38`   1305.json
- `15:18:38`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:38`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:38`     ka_index=None  khalid_index=49
- `15:18:38`   1405.json
- `15:18:38`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:38`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:38`     ka_index=None  khalid_index=49
- `15:18:38`   1505.json
- `15:18:38`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:38`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:38`     ka_index=None  khalid_index=49
- `15:18:38`   1605.json
- `15:18:38`     top-level keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color']
- `15:18:38`     scores keys:    ['khalid_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score', 'carry_risk_score', 'vix', 'move']
- `15:18:38`     ka_index=None  khalid_index=49
## 3. Distribution of values from those 20 samples

- `15:18:38`   count:  20
- `15:18:38`   min:    49.0
- `15:18:38`   max:    49.0
- `15:18:38`   mean:   49.00
- `15:18:38`   median: 49.0
- `15:18:38`      50: ████████████████████ (20)
## 4. Full distribution across last 200 files (the loader's actual cap)

- `15:18:58`   count:        200
- `15:18:58`   unique values: 2
- `15:18:58`   min/max:      0.00 / 43.00
- `15:18:58`   mean:         2.15
- `15:18:58`   histogram (buckets of 5):
- `15:18:58`       0: ████████████████████████████████████████ (190)
- `15:18:58`      45: ██████████ (10)
- `15:18:58`   top 10 most-common values:
- `15:18:58`        0.0: x190
- `15:18:58`       43.0: x10
## FINAL

- `15:18:58` Done
