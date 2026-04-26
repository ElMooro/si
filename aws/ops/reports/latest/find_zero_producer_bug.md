# Find the producer of zero-valued archives Mar 9 → Apr 24

**Status:** success  
**Duration:** 6.5s  
**Finished:** 2026-04-26T15:22:55+00:00  

## Log
## 1. Inspect a recent (working) and old (broken) archive entry

- `15:22:49` 
- `15:22:49` === BROKEN (Mar 15-16) ===
- `15:22:49`   archive/intelligence/2026/03/15/1210.json
- `15:22:49`     top keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color', 'action_required', 'forecast', 'scores', 'signals']
- `15:22:49`     version: 3.0
- `15:22:49`     data_sources: {'main_terminal': False, 'repo_plumbing': False, 'ml_predictions': False, 'sources_active': 0, 'agents_online': 0, 'total_agents': 0}
- `15:22:49`     scores: {"khalid_index": 0, "crisis_distance": 60, "plumbing_stress": 0, "ml_risk_score": 0, "carry_risk_score": 0, "vix": null, "move": null}
- `15:22:49`   archive/intelligence/2026/03/16/1205.json
- `15:22:49`     top keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color', 'action_required', 'forecast', 'scores', 'signals']
- `15:22:49`     version: 3.0
- `15:22:49`     data_sources: {'main_terminal': False, 'repo_plumbing': False, 'ml_predictions': False, 'sources_active': 0, 'agents_online': 0, 'total_agents': 0}
- `15:22:49`     scores: {"khalid_index": 0, "crisis_distance": 60, "plumbing_stress": 0, "ml_risk_score": 0, "carry_risk_score": 0, "vix": null, "move": null}
- `15:22:49` 
- `15:22:49` === WORKING (Apr 26) ===
- `15:22:49`   archive/intelligence/2026/04/26/1254.json
- `15:22:49`     top keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color', 'action_required', 'forecast', 'scores', 'signals']
- `15:22:49`     version: 3.0
- `15:22:49`     data_sources: {'main_terminal': True, 'repo_plumbing': True, 'ml_predictions': True, 'sources_active': 3, 'agents_online': 0, 'total_agents': 0}
- `15:22:49`     scores: {"khalid_index": 43, "crisis_distance": 60, "plumbing_stress": 14, "ml_risk_score": 56, "carry_risk_score": 14, "vix": 19.31, "move": null, "calibrated_composite": 31.75, "raw_composite": 31.75, "ka_index": 43}
- `15:22:49`   archive/intelligence/2026/04/26/1300.json
- `15:22:49`     top keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color', 'action_required', 'forecast', 'scores', 'signals']
- `15:22:49`     version: 3.0
- `15:22:49`     data_sources: {'main_terminal': True, 'repo_plumbing': True, 'ml_predictions': True, 'sources_active': 3, 'agents_online': 0, 'total_agents': 0}
- `15:22:49`     scores: {"khalid_index": 43, "crisis_distance": 60, "plumbing_stress": 14, "ml_risk_score": 56, "carry_risk_score": 14, "vix": 19.31, "move": null, "calibrated_composite": 31.75, "raw_composite": 31.75, "ka_index": 43}
## 2. List candidate producer Lambdas (matching 'intelligence' or 'daily-report')

- `15:22:49`   macro-report-api                          modified=2026-04-25T10:24:48.000+0000  runtime=python3.9
- `15:22:49`   justhodl-email-reports-v2                 modified=2026-04-26T12:18:10.000+0000  runtime=python3.11
- `15:22:49`   justhodl-daily-macro-report               modified=2026-04-25T10:25:39.000+0000  runtime=python3.11
- `15:22:49`   macro-financial-report-viewer             modified=2026-04-25T10:25:43.000+0000  runtime=python3.11
- `15:22:49`     desc: Public report viewer for Claude to fetch
- `15:22:49`   permanent-market-intelligence             modified=2026-04-26T12:18:24.000+0000  runtime=python3.9
- `15:22:49`     desc: Complete market intelligence with all metrics
- `15:22:49`   justhodl-daily-report-v3                  modified=2026-04-26T12:52:31.000+0000  runtime=python3.12
- `15:22:49`   daily-liquidity-report                    modified=2026-04-25T10:26:55.000+0000  runtime=python3.11
- `15:22:49`   justhodl-intelligence                     modified=2026-04-26T12:52:38.000+0000  runtime=python3.12
- `15:22:49`   justhodl-reports-builder                  modified=2026-04-26T12:53:09.000+0000  runtime=python3.12
- `15:22:49`     desc: Builds reports/scorecard.json from SSM calibration + DDB signals/outcomes
- `15:22:49`   justhodl-email-reports                    modified=2026-04-26T12:18:03.000+0000  runtime=python3.12
- `15:22:49`   justhodl-morning-intelligence             modified=2026-04-26T12:52:54.000+0000  runtime=python3.12
- `15:22:49`   macro-financial-intelligence              modified=2026-04-25T10:29:33.000+0000  runtime=python3.11
## 3. CloudWatch logs around Apr 24 → Apr 25 transition

- `15:22:50` 
- `15:22:50`   justhodl-daily-report-v3 — 10 recent log streams
- `15:22:50`     2026/04/26/[$LATEST]2633f81b575f441fb34dd2d189140787          last event: 2026-04-26 15:05:25.497000+00:00
- `15:22:50`     2026/04/26/[$LATEST]da87c75bacbf4840995a261fe03ddc1a          last event: 2026-04-26 14:56:45.247000+00:00
- `15:22:50`     2026/04/26/[$LATEST]cea2153c5bb742a3abec9e8f639dc033          last event: 2026-04-26 14:54:56.434000+00:00
- `15:22:50` 
- `15:22:50`   justhodl-intelligence — 10 recent log streams
- `15:22:50`     2026/04/26/[$LATEST]d2c85aa04127431ab837c9d768bea193          last event: 2026-04-26 13:00:22.079000+00:00
- `15:22:50`     2026/04/26/[$LATEST]9201521ab6ed45efa9ab8d7d14ad99a9          last event: 2026-04-26 12:54:07.016000+00:00
- `15:22:50`     2026/04/26/[$LATEST]c846c36aac1b4a7aa1285e9fb9a0c8da          last event: 2026-04-26 12:10:49.039000+00:00
- `15:22:50` 
- `15:22:50`   justhodl-morning-intelligence — 10 recent log streams
- `15:22:50`     2026/04/26/[$LATEST]c7e9ab7772b741958abb23bb51a5cb89          last event: 2026-04-26 13:01:53.654000+00:00
- `15:22:50`     2026/04/25/[$LATEST]c3616a4b6efd460c88264e8a4cafa66e          last event: 2026-04-25 19:03:37.599000+00:00
- `15:22:50`     2026/04/25/[$LATEST]4c00d71bacec47839b43a51dc4e41c84          last event: 2026-04-25 18:45:29.154000+00:00
## 4. Search logs around 2026-04-24 → 2026-04-25 cutover

- `15:22:50`   searching /aws/lambda/justhodl-daily-report-v3 for 'khalid_index' or 'ka_index' or '0' messages...
- `15:22:55`     0 matching events in window
## FINAL

- `15:22:55` Done
