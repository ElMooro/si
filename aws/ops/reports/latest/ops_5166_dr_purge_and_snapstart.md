# ops 5166 -- DR mirror purge (evidence-gated) + SnapStart off

**Status:** failure  
**Duration:** 2093.5s  
**Finished:** 2026-09-03T19:16:14+00:00  

## Error

```
SystemExit: 1
```

## Data

| current | current_gb | noncurrent | noncurrent_gb | prefix | section |
|---|---|---|---|---|---|
| 143098 | 2.61 | 46180 | 0.76 | backup/ | A_keep |
| 6 | 0.0 | 0 | 0.0 | quarantine/ | A_keep |

## Log
## A. DR mirror census and evidence gate

- `18:41:20` ✅    live bucket: no replication configuration (ops 4988 held)
- `18:41:21`    justhodl-dashboard-live-dr: replication-source config: none
- `18:41:22`    justhodl-dashboard-live-dr               versioning=Enabled object_lock=none notifications=0 size=2764 GB {'Standard': 0, 'StandardIA': 2543, 'StandardIASizeOverhead': 222}
- `18:41:22`    justhodl-dr-usw2-857687956942: replication-source config: none
- `18:41:23`    justhodl-dr-usw2-857687956942            versioning=Enabled object_lock=none notifications=0 size=11 GB {'Standard': 6, 'StandardIA': 1, 'StandardIASizeOverhead': 4}
- `18:41:23`    top-level prefixes: ['13f-cache/', '13f/', '_health/', 'air/', 'analytics/', 'archive/', 'asia/', 'backtest/', 'backup/', 'backups/', 'base-rates/', 'boom/', 'calibration/', 'cb-cache/', 'chokepoint/', 'config/', 'cot/', 'credit/', 'data-census/', 'data/', 'discovery/', 'divergence/', 'domain-barometers/', 'edgar-insiders/', 'equity-critique/', 'equity-prewarm/', 'equity-research-history/', 'equity-research/', 'errors/', 'estimate-revisions/', 'etf-constituents-v2/', 'etf-constituents/', 'etf-flows/', 'flow-anomalies/', 'foreign-flows/', 'geo/', 'history/', 'investor-debate/', 'js/', 'lake/', 'learning/', 'llm-cache/', 'macro-attribution/', 'macro/', 'opportunities/', 'ops/', 'plumbing-composite/', 'portfolio/', 'quarantine/', 'readthrough/', 'regime/', 'reports/', 'risk/', 'screener/', 'sec-filings-cache/', 'sec/', 'sentiment/', 'signals/', 'spx-beaters/', 'spx-ma/', 'state/', 'stock-analysis/', 'symbol-resolver/', 'system-events/', 'telegram/', 'tools/', 'transcripts/'] ; root objects: 10 ['alerts-state.json', 'crypto-intel.json', 'edge-data.json', 'flow-confluence.html', 'flow-data.json', 'intelligence-report.json']
- `18:42:21`    KEEP backup/        current 143,098 (2.6 GB) noncurrent 46,180 (0.8 GB) newest=2026-09-03 06:01
- `18:42:21`    KEEP quarantine/    current 6 (0.0 GB) noncurrent 0 (0.0 GB) newest=2026-08-01 18:48
- `18:42:24` ✅    13f-cache/             headed 20  REPLICA 20  in-live 20  newest 2026-08-21 14:44  -> PURGE
- `18:42:24` ⚠    13f/                   headed  2  REPLICA  2  in-live  2  newest 2026-08-24 08:30  -> HELD
- `18:42:26` ⚠    _health/               headed  6  REPLICA  6  in-live  6  newest 2026-08-26 02:08  -> HELD
- `18:42:26` ⚠    air/                   headed  1  REPLICA  1  in-live  1  newest 2026-08-26 10:40  -> HELD
- `18:42:27` ⚠    analytics/             headed  6  REPLICA  6  in-live  6  newest 2026-08-26 09:00  -> HELD
- `18:42:30` ✅    archive/               headed 20  REPLICA 20  in-live 20  newest 2026-05-26 15:05  -> PURGE
- `18:42:31` ⚠    asia/                  headed  2  REPLICA  2  in-live  2  newest 2026-08-26 10:21  -> HELD
- `18:42:32` ⚠    backtest/              headed  3  REPLICA  3  in-live  3  newest 2026-08-25 22:30  -> HELD
- `18:42:33` ⚠    backups/               headed  3  REPLICA  3  in-live  3  newest 2026-08-01 14:16  -> HELD
- `18:42:33` ⚠    base-rates/            headed  1  REPLICA  1  in-live  1  newest 2026-08-22 14:30  -> HELD
- `18:42:34` ⚠    boom/                  headed  1  REPLICA  1  in-live  1  newest 2026-08-26 12:30  -> HELD
- `18:42:37` ✅    calibration/           headed 20  REPLICA 20  in-live 20  newest 2026-08-23 04:00  -> PURGE
- `18:42:37` ⚠    cb-cache/              headed  2  REPLICA  2  in-live  2  newest 2026-08-05 00:00  -> HELD
- `18:42:38` ⚠    chokepoint/            headed  1  REPLICA  1  in-live  1  newest 2026-08-25 15:31  -> HELD
- `18:42:40` ✅    config/                headed  9  REPLICA  9  in-live  9  newest 2026-08-01 21:00  -> PURGE
- `18:42:43` ✅    cot/                   headed 20  REPLICA 20  in-live 20  newest 2026-08-21 19:00  -> PURGE
- `18:42:43` ⚠    credit/                headed  1  REPLICA  1  in-live  1  newest 2026-08-25 22:35  -> HELD
- `18:42:44` ⚠    data-census/           headed  1  REPLICA  1  in-live  1  newest 2026-08-26 12:52  -> HELD
- `18:42:50` ✅    data/                  headed 48  REPLICA 48  in-live 48  newest 2026-08-26 14:43  -> PURGE
- `18:42:51` ⚠    discovery/             headed  2  REPLICA  2  in-live  2  newest 2026-08-25 13:30  -> HELD
- `18:42:51` ⚠    divergence/            headed  2  REPLICA  2  in-live  2  newest 2026-08-01 11:00  -> HELD
- `18:42:52` ⚠    domain-barometers/     headed  1  REPLICA  1  in-live  1  newest 2026-08-26 12:20  -> HELD
- `18:42:55` ✅    edgar-insiders/        headed 20  REPLICA 20  in-live 20  newest 2026-08-26 08:01  -> PURGE
- `18:42:58` ✅    equity-critique/       headed 20  REPLICA 20  in-live 20  newest 2026-08-26 08:01  -> PURGE
- `18:43:01` ✅    equity-prewarm/        headed 20  REPLICA 20  in-live 20  newest 2026-06-27 08:36  -> PURGE
- `18:43:04` ✅    equity-research-history/ headed 20  REPLICA 20  in-live 20  newest 2026-06-02 15:41  -> PURGE
- `18:43:07` ✅    equity-research/       headed 20  REPLICA 20  in-live 20  newest 2026-08-26 08:00  -> PURGE
- `18:43:10` ✅    errors/                headed 20  REPLICA 20  in-live 20  newest 2026-08-25 11:51  -> PURGE
- `18:43:10` ⚠    estimate-revisions/    headed  1  REPLICA  1  in-live  1  newest 2026-08-26 13:40  -> HELD
- `18:43:13` ✅    etf-constituents-v2/   headed 20  REPLICA 20  in-live 20  newest 2026-08-25 23:15  -> PURGE
- `18:43:16` ✅    etf-constituents/      headed 20  REPLICA 20  in-live 20  newest 2026-06-20 04:35  -> PURGE
- `18:43:19` ✅    etf-flows/             headed 20  REPLICA 20  in-live 20  newest 2026-07-03 22:34  -> PURGE
- `18:43:22` ✅    flow-anomalies/        headed 20  REPLICA 20  in-live 20  newest 2026-08-25 23:00  -> PURGE
- `18:43:22` ⚠    foreign-flows/         headed  1  REPLICA  1  in-live  1  newest 2026-08-17 21:31  -> HELD
- `18:43:23` ⚠    geo/                   headed  1  REPLICA  1  in-live  1  newest 2026-08-26 11:30  -> HELD
- `18:43:26` ✅    history/               headed 20  REPLICA 20  in-live 20  newest 2026-05-30 11:30  -> PURGE
- `18:43:28` ✅    investor-debate/       headed 10  REPLICA 10  in-live 10  newest 2026-07-14 22:05  -> PURGE
- `18:43:29` ⚠    js/                    headed  1  REPLICA  1  in-live  1  newest 2026-06-09 21:17  -> HELD
- `18:43:31` ✅    lake/                  headed 20  REPLICA 20  in-live 20  newest 2026-08-04 17:42  -> PURGE
- `18:43:32` ⚠    learning/              headed  3  REPLICA  3  in-live  3  newest 2026-08-25 21:00  -> HELD
- `18:43:35` ✅    llm-cache/             headed 20  REPLICA 20  in-live 20  newest 2026-08-06 13:22  -> PURGE
- `18:43:36` ⚠    macro-attribution/     headed  1  REPLICA  1  in-live  1  newest 2026-08-26 12:05  -> HELD
- `18:43:39` ✅    macro/                 headed 20  REPLICA 20  in-live 20  newest 2026-06-21 22:15  -> PURGE
- `18:43:39` ⚠    opportunities/         headed  1  REPLICA  1  in-live  1  newest 2026-08-04 21:36  -> HELD
- `18:43:40` ⚠    ops/                   headed  1  REPLICA  1  in-live  1  newest 2026-06-08 13:57  -> HELD
- `18:43:40` ⚠    plumbing-composite/    headed  1  REPLICA  1  in-live  1  newest 2026-08-25 10:45  -> HELD
- `18:43:42` ✅    portfolio/             headed 13  REPLICA 13  in-live 13  newest 2026-08-26 14:43  -> PURGE
- `18:43:43` ⚠    readthrough/           headed  1  REPLICA  1  in-live  1  newest 2026-08-26 13:20  -> HELD
- `18:43:44` ⚠    regime/                headed  2  REPLICA  2  in-live  2  newest 2026-07-31 18:06  -> HELD
- `18:43:44` ⚠    reports/               headed  1  REPLICA  1  in-live  1  newest 2026-07-31 17:52  -> HELD
- `18:43:45` ⚠    risk/                  headed  1  REPLICA  1  in-live  1  newest 2026-08-01 11:00  -> HELD
- `18:43:48` ✅    screener/              headed 20  REPLICA 20  in-live 20  newest 2026-08-05 20:47  -> PURGE
- `18:43:49` ⚠    sec-filings-cache/     headed  2  REPLICA  2  in-live  2  newest 2026-07-30 12:00  -> HELD
- `18:43:49` ⚠    sec/                   headed  1  REPLICA  1  in-live  1  newest 2026-08-21 21:05  -> HELD
- `18:43:50` ⚠    sentiment/             headed  1  REPLICA  1  in-live  1  newest 2026-08-25 16:45  -> HELD
- `18:43:51` ⚠    signals/               headed  6  REPLICA  6  in-live  6  newest 2026-08-26 14:33  -> HELD
- `18:43:52` ⚠    spx-beaters/           headed  2  REPLICA  2  in-live  2  newest 2026-08-23 20:49  -> HELD
- `18:43:52` ⚠    spx-ma/                headed  1  REPLICA  1  in-live  1  newest 2026-08-25 21:15  -> HELD
- `18:43:53` ⚠    state/                 headed  1  REPLICA  1  in-live  1  newest 2026-08-26 12:10  -> HELD
- `18:43:54` ⚠    stock-analysis/        headed  2  REPLICA  2  in-live  2  newest 2026-06-12 18:44  -> HELD
- `18:43:54` ⚠    symbol-resolver/       headed  1  REPLICA  1  in-live  1  newest 2026-08-26 11:51  -> HELD
- `18:43:57` ✅    system-events/         headed 20  REPLICA 20  in-live 20  newest 2026-06-19 23:58  -> PURGE
- `18:43:58` ⚠    telegram/              headed  1  REPLICA  1  in-live  1  newest 2026-08-26 11:44  -> HELD
- `18:43:59` ⚠    tools/                 headed  7  REPLICA  7  in-live  7  newest 2026-08-15 02:43  -> HELD
- `18:44:02` ✅    transcripts/           headed 20  REPLICA 20  in-live 20  newest 2026-08-06 14:00  -> PURGE
- `18:44:03`    root objects: 10, replica-and-present-in-live: 10
- `18:44:03` ✅    GATE PASS for ['13f-cache/', 'archive/', 'calibration/', 'config/', 'cot/', 'data/', 'edgar-insiders/', 'equity-critique/', 'equity-prewarm/', 'equity-research-history/', 'equity-research/', 'errors/', 'etf-constituents-v2/', 'etf-constituents/', 'etf-flows/', 'flow-anomalies/', 'history/', 'investor-debate/', 'lake/', 'llm-cache/', 'macro/', 'portfolio/', 'screener/', 'system-events/', 'transcripts/'] -- stale CRR replica of the live bucket; purge approved by Khalid 2026-09-03; held: ['13f/', '_health/', 'air/', 'analytics/', 'asia/', 'backtest/', 'backups/', 'base-rates/', 'boom/', 'cb-cache/', 'chokepoint/', 'credit/', 'data-census/', 'discovery/', 'divergence/', 'domain-barometers/', 'estimate-revisions/', 'foreign-flows/', 'geo/', 'js/', 'learning/', 'macro-attribution/', 'opportunities/', 'ops/', 'plumbing-composite/', 'readthrough/', 'regime/', 'reports/', 'risk/', 'sec-filings-cache/', 'sec/', 'sentiment/', 'signals/', 'spx-beaters/', 'spx-ma/', 'state/', 'stock-analysis/', 'symbol-resolver/', 'telegram/', 'tools/']
- `18:44:08`    justhodl-dr-usw2-857687956942: prefixes ['13f-cache/'] ; sampled 40, REPLICA 0, in live 40
- `18:44:08` ⚠    justhodl-dr-usw2-857687956942: gate not met -- left untouched
## B. Preserve what is NOT replica: quarantine zips -> live bucket; backup/ retention 30d

- `18:44:08` ✅    quarantine/2026-08-01/macro-report-api.zip -> s3://justhodl-dashboard-live/data/ops/archive/dr-quarantine/2026-08-01/macro-report-api.zip (29862 bytes)
- `18:44:09` ✅    quarantine/2026-08-01/multi-agent-orchestrator.zip -> s3://justhodl-dashboard-live/data/ops/archive/dr-quarantine/2026-08-01/multi-agent-orchestrator.zip (1505 bytes)
- `18:44:09` ✅    quarantine/2026-08-01/nyfed-financial-stability-fetcher.zip -> s3://justhodl-dashboard-live/data/ops/archive/dr-quarantine/2026-08-01/nyfed-financial-stability-fetcher.zip (2769 bytes)
- `18:44:10` ✅    quarantine/2026-08-01/nyfed-primary-dealer-fetcher.zip -> s3://justhodl-dashboard-live/data/ops/archive/dr-quarantine/2026-08-01/nyfed-primary-dealer-fetcher.zip (1692 bytes)
- `18:44:10` ✅    quarantine/2026-08-01/nyfedapi-isolated.zip -> s3://justhodl-dashboard-live/data/ops/archive/dr-quarantine/2026-08-01/nyfedapi-isolated.zip (4426 bytes)
- `18:44:10` ✅    quarantine/2026-08-01/ultimate-multi-agent.zip -> s3://justhodl-dashboard-live/data/ops/archive/dr-quarantine/2026-08-01/ultimate-multi-agent.zip (1931 bytes)
- `18:44:11`    backup/ snapshot days: 104 (backup/2026-05-23/ .. backup/2026-09-03/)
- `18:44:11`    latest snapshot backup/2026-09-03/: 1,751 objects, 0.05 GB -> ~1.5 GB per 30 days
## C. Lifecycle first -- the purge completes on its own even if the sweep below is cut short

## D. Accelerated sweep -- parallel version delete, 30-minute budget

- `18:44:14`    root-level replica versions deleted: 982
- `18:46:14`    t+ 294s  deleted 134,653 versions (8.0 GB)  tasks 456  errors 0
- `18:48:14`    t+ 414s  deleted 208,839 versions (24.8 GB)  tasks 477  errors 0
- `18:50:15`    t+ 534s  deleted 408,057 versions (33.8 GB)  tasks 486  errors 0
- `18:52:15`    t+ 654s  deleted 663,683 versions (36.9 GB)  tasks 890  errors 0
- `18:54:15`    t+ 774s  deleted 818,506 versions (43.1 GB)  tasks 892  errors 0
- `18:56:15`    t+ 895s  deleted 1,200,310 versions (197.5 GB)  tasks 1161  errors 0
- `18:58:15`    t+1015s  deleted 1,298,294 versions (341.6 GB)  tasks 1168  errors 0
- `19:00:15`    t+1135s  deleted 1,579,484 versions (413.2 GB)  tasks 1333  errors 0
- `19:02:15`    t+1255s  deleted 1,816,652 versions (428.0 GB)  tasks 1333  errors 0
- `19:04:15`    t+1375s  deleted 1,951,841 versions (529.2 GB)  tasks 1344  errors 0
- `19:06:15`    t+1495s  deleted 2,351,122 versions (648.0 GB)  tasks 1344  errors 0
- `19:08:15`    t+1615s  deleted 2,351,122 versions (648.0 GB)  tasks 1344  errors 0
- `19:10:16`    t+1735s  deleted 2,351,122 versions (648.0 GB)  tasks 1344  errors 0
- `19:12:16`    t+1855s  deleted 2,351,122 versions (648.0 GB)  tasks 1344  errors 0
- `19:14:16`    sweep done: 2,853,122 versions / delete markers removed, 773.0 GB of version bytes, 0 errors, 1344 prefix tasks, 1975s
- `19:14:16` ⚠    budget reached -- the lifecycle rules finish the remainder within ~24-48h
## E. SnapStart: justhodl-ai-chat off + delete every snapshotted published version (alias/URL-safe)

- `19:14:16`    ai-chat SnapStart before: {'ApplyOn': 'PublishedVersions', 'OptimizationStatus': 'Off'}  runtime=python3.12
- `19:14:19` ✅    ai-chat SnapStart now: {'ApplyOn': 'None', 'OptimizationStatus': 'Off'} (LastUpdateStatus=Successful)
- `19:16:08` ✅    snapshotted versions deleted: 8 ['justhodl-stock-analyzer:1', 'justhodl-ai-chat:1', 'justhodl-stock-screener:1', 'justhodl-investor-agents:1', 'justhodl-reports-builder:1', 'cftc-futures-positioning-agent:1', 'justhodl-morning-intelligence:1', 'justhodl-edge-engine:1']
## F. Verification of the ops-5164 stand-down (last 6h)

- `19:16:09`    justhodl-repo                    invocations     68  errors    0  6.7 Lambda-hours
- `19:16:10`    justhodl-fundamental-census      invocations     11  errors    0  0.2 Lambda-hours
- `19:16:10`    justhodl-census-us               invocations    935  errors    0  15.8 Lambda-hours
- `19:16:10`    justhodl-boj-full                invocations    276  errors    0  2.2 Lambda-hours
- `19:16:11`    justhodl-ecb-deep                invocations     36  errors    0  0.8 Lambda-hours
- `19:16:11`    justhodl-sdmx-walker             invocations    513  errors    0  0.2 Lambda-hours
- `19:16:12`    live bucket size (latest daily point): 1954 GB
- `19:16:14`    DR bucket size (latest daily point, lags a day): 2764 GB
- `19:16:14` ✅    ledger written to s3://justhodl-dashboard-live/data/ops/ops5166-dr-and-snapstart.json
- `19:16:14` ✗ lifecycle rules not readable back: ['ops5166-purge-replica-13f-cache', 'ops5166-purge-replica-archive', 'ops5166-purge-replica-calibration', 'ops5166-purge-replica-config', 'ops5166-purge-replica-cot', 'ops5166-purge-replica-data', 'ops5166-purge-replica-edgar-insiders', 'ops5166-purge-replica-equity-critique', 'ops5166-purge-replica-equity-prewarm', 'ops5166-purge-replica-equity-research-history', 'ops5166-purge-replica-equity-research', 'ops5166-purge-replica-errors', 'ops5166-purge-replica-etf-constituents-v2', 'ops5166-purge-replica-etf-constituents', 'ops5166-purge-replica-etf-flows', 'ops5166-purge-replica-flow-anomalies', 'ops5166-purge-replica-history', 'ops5166-purge-replica-investor-debate', 'ops5166-purge-replica-lake', 'ops5166-purge-replica-llm-cache', 'ops5166-purge-replica-macro', 'ops5166-purge-replica-portfolio', 'ops5166-purge-replica-screener', 'ops5166-purge-replica-system-events', 'ops5166-purge-replica-transcripts', 'ops5166-backup-30d', 'ops5166-expired-markers']
