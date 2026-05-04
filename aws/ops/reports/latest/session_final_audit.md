# 1) Lambda count + recent activity (last 24h)

**Status:** success  
**Duration:** 27.3s  
**Finished:** 2026-05-04T19:25:59+00:00  

## Log
- `19:25:33`   total Lambdas in account: 150
- `19:25:33`   modified last 24h: 23
- `19:25:33`     2026-05-04T18:55:16  justhodl-momentum-scanner
- `19:25:33`     2026-05-04T12:27:19  justhodl-event-study
- `19:25:33`     2026-05-04T19:17:32  justhodl-ai-brief
- `19:25:33`     2026-05-04T19:23:03  justhodl-wave-signal-logger
- `19:25:33`     2026-05-04T12:19:04  justhodl-yield-curve
- `19:25:33`     2026-05-04T13:23:58  justhodl-sector-rotation
- `19:25:33`     2026-05-04T12:42:18  justhodl-ab-test
- `19:25:33`     2026-05-04T18:39:51  justhodl-allocator
- `19:25:33`     2026-05-04T12:15:19  justhodl-macro-surprise
- `19:25:33`     2026-05-04T12:21:35  justhodl-signal-portfolio
- `19:25:33`     2026-05-04T00:13:17  justhodl-short-interest
- `19:25:33`     2026-05-04T12:25:28  justhodl-historical-analogs
- `19:25:33`     2026-05-04T13:32:56  justhodl-alert-router
- `19:25:33`     2026-05-04T12:47:37  justhodl-morning-brief-tg
- `19:25:33`     2026-05-04T12:42:30  justhodl-correlation-surface
# 2) Fresh S3 outputs (modified < 6h)

- `19:25:33`   ✓ fresh  intelligence-report.json                          age=   19min  size=    4,819b
- `19:25:33`   ✓ fresh  data/calibration-snapshot.json                    age=   13min  size=   34,833b
- `19:25:33`   ✓ fresh  data/sector-rotation.json                         age=    1min  size=   13,488b
- `19:25:33`   ✓ fresh  data/momentum-scanner.json                        age=   29min  size=   80,619b
- `19:25:34`   ✓ fresh  data/alert-history.json                           age=   22min  size=    5,247b
- `19:25:34`   ✓ fresh  data/correlation-surface.json                     age=  264min  size=   38,656b
- `19:25:34`   ✓ fresh  data/macro-surprise.json                          age=   69min  size=   11,259b
- `19:25:34`   ✓ fresh  data/yield-curve.json                             age=   65min  size=    4,961b
- `19:25:34`   ✗ MISS   data/eurodollar-stress.json
- `19:25:34`   ✓ fresh  data/auction-crisis.json                          age=   10min  size=   11,666b
- `19:25:34`   ⚠ stale  divergence/current.json                           age=  384min  size=    6,275b
- `19:25:34`   ✓ fresh  cot/extremes/current.json                         age=    7min  size=    8,334b
- `19:25:34`   ✓ fresh  data/etf-flows.json                               age=   66min  size=   27,650b
- `19:25:34`   ✓ fresh  data/earnings-tracker.json                        age=   98min  size=   29,894b
- `19:25:34`   ✓ fresh  data/short-interest.json                          age=   81min  size=   58,474b
- `19:25:34`   ✓ fresh  data/insider-trades.json                          age=   21min  size=   15,119b
- `19:25:34`   ⚠ stale  data/historical-analogs.json                      age=  419min  size=    5,790b
- `19:25:34`   ✓ fresh  data/event-study.json                             age=  324min  size=   11,318b
- `19:25:34`   ✓ fresh  data/whats-changed.json                           age=  145min  size=    1,657b
- `19:25:34`   ✗ MISS   data/feedback.json
- `19:25:35`   ✓ fresh  data/ab-test-results.json                         age=  205min  size=      651b
- `19:25:35`   ⚠ stale  portfolio/signal-portfolio-state.json             age=  423min  size=    7,244b
- `19:25:35`   ✓ fresh  data/allocator.json                               age=   23min  size=    4,804b
- `19:25:35`   ✗ MISS   data/risk-sizer.json
- `19:25:35`   ✗ MISS   data/asymmetric-scorer.json
- `19:25:35`   ✓ fresh  data/ai-brief.json                                age=    7min  size=   12,378b
- `19:25:35`   totals: fresh=19, stale=3, missing=4
# 3) Anthropic-dependent Lambdas (BROKEN until credits topped up)

- `19:25:57`   found 16 Lambdas with ANTHROPIC_KEY/ANTHROPIC_API_KEY env:
- `19:25:57`     • justhodl-ab-test
- `19:25:57`     • justhodl-ai-brief
- `19:25:57`     • justhodl-ai-chat
- `19:25:57`     • justhodl-bloomberg-v8
- `19:25:57`     • justhodl-chat-api
- `19:25:57`     • justhodl-crypto-intel
- `19:25:57`     • justhodl-financial-secretary
- `19:25:57`     • justhodl-investor-agents
- `19:25:57`     • justhodl-ka-metrics
- `19:25:57`     • justhodl-khalid-metrics
- `19:25:57`     • justhodl-morning-intelligence
- `19:25:57`     • justhodl-news-sentiment
- `19:25:57`     • justhodl-prompt-iterator
- `19:25:57`     • justhodl-stock-ai-research
- `19:25:57`     • justhodl-telegram-bot
- `19:25:57`     • justhodl-watchlist-debate
# 4) Wave 1+2+3 Lambda inventory (this rebuild)

- `19:25:57`     ✓ justhodl-earnings-tracker                  state=Active   mod=2026-05-03T23:58:02
- `19:25:57`     ✓ justhodl-short-interest                    state=Active   mod=2026-05-04T00:13:17
- `19:25:57`     ✓ justhodl-etf-flows                         state=Active   mod=2026-05-04T00:18:41
- `19:25:57`     ✓ justhodl-macro-surprise                    state=Active   mod=2026-05-04T12:15:19
- `19:25:57`     ✓ justhodl-yield-curve                       state=Active   mod=2026-05-04T12:19:04
- `19:25:57`     ✓ justhodl-signal-portfolio                  state=Active   mod=2026-05-04T12:21:35
- `19:25:57`     ✓ justhodl-historical-analogs                state=Active   mod=2026-05-04T12:25:28
- `19:25:58`     ✓ justhodl-event-study                       state=Active   mod=2026-05-04T12:27:19
- `19:25:58`     ✓ justhodl-correlation-surface               state=Active   mod=2026-05-04T12:42:30
- `19:25:58`     ✓ justhodl-ab-test                           state=Active   mod=2026-05-04T12:42:18
- `19:25:58`     ✓ justhodl-feedback                          state=Active   mod=2026-05-04T12:47:16
- `19:25:58`     ✓ justhodl-morning-brief-tg                  state=Active   mod=2026-05-04T12:47:37
- `19:25:58`     ✓ justhodl-whats-changed                     state=Active   mod=2026-05-04T12:47:47
- `19:25:58`     ✓ justhodl-calibration-snapshot              state=Active   mod=2026-05-04T13:19:08
- `19:25:58`     ✓ justhodl-sector-rotation                   state=Active   mod=2026-05-04T13:23:58
- `19:25:59`     ✓ justhodl-alert-router                      state=Active   mod=2026-05-04T13:32:56
- `19:25:59`     ✓ justhodl-momentum-scanner                  state=Active   mod=2026-05-04T18:55:16
- `19:25:59`     ✓ justhodl-wave-signal-logger                state=Active   mod=2026-05-04T19:23:03
- `19:25:59`     ✓ justhodl-ai-brief                          state=Active   mod=2026-05-04T19:17:32
- `19:25:59`     ✓ justhodl-allocator                         state=Active   mod=2026-05-04T18:39:51
