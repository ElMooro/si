# ops 6655 - justhodl-polygon-eod: declare, first fold + 1 kick, data.html card

**Status:** success  
**Duration:** 222.2s  
**Finished:** 2026-10-25T22:30:35+00:00  

## Data

| errors | kick | phase | rows | sessions | steps | tickers_written | unchanged |
|---|---|---|---|---|---|---|---|
| 0 | 1 | BACKFILL | 1311809 | 120 | [{"step": "fold", "seconds": 17.4, "sessions": 120, "rows": 1311809}, {"step": "write", "seconds": 74.5, "tickers": 12706, "unchanged": 0, "left_unwritten": 0, "timed_out": false}] | 12706 | 0 |
| 0 | 2 | BACKFILL | 1340404 | 120 | [{"step": "fold", "seconds": 16.8, "sessions": 120, "rows": 1340404}, {"step": "write", "seconds": 87.3, "tickers": 12682, "unchanged": 0, "left_unwritten": 0, "timed_out": false}] | 12682 | 0 |

## Log
## 1. function + schedule

- `22:26:53` justhodl-polygon-eod: runtime python3.12, memory 3008, timeout 900, state Active, modified 2026-10-25T22:08:51
- `22:26:53` schedule justhodl-polygon-eod-daily: cron(10 3 * * ? *) ENABLED tz UTC target justhodl-polygon-eod
- `22:26:53` justhodl-polygon-full modified 2026-10-25T22:09:17 (v1.1.0 hole repair expected)
## 2. deployed schedule manifest := committed (additions-only)

- `22:26:53` S3 1035 entries, repo 1035; added [] removed 0 changed 0
- `22:26:53` ✅ manifest already equal
## 3. first fold + kick

- `22:26:53` kick 1: async invoke 202 at 22:26:53
- `22:28:34` kick 2: async invoke 202 at 22:28:34
- `22:30:34` manifest: phase BACKFILL sessions available 1294 folded 240 pending 1054 tickers 13409 first 2021-08-26 last 2022-08-09
- `22:30:34` gaps (weekdays with no session, not recorded holidays): ['2021-09-06', '2021-11-25', '2021-12-24', '2022-01-17', '2022-02-21', '2022-04-15', '2022-05-30', '2022-06-20', '2022-07-04', '2022-09-05', '2022-11-24', '2022-12-26', '2023-01-02', '2023-01-16', '2023-02-20', '2023-04-07', '2023-05-29', '2023-06-19', '2023-07-04', '2023-09-04', '2023-11-23', '2023-12-25', '2024-01-01', '2024-01-15', '2024-02-19', '2024-03-29', '2024-05-27', '2024-06-19', '2024-07-04', '2024-09-02', '2024-11-28', '2024-12-25', '2025-01-01', '2025-01-09', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26', '2025-06-19', '2025-07-04']
- `22:30:34` holidays recorded by polygon-full: []
## 4. data.html card (extra-providers.json, add-only) + catalog refresh

- `22:30:35` ✅ extra-providers.json += polygon-eod (17 entries)
- `22:30:35` justhodl-provider-catalog invoked async (card lands within ~10 min; daily 06:20 otherwise)
## 5. one ticker read back

- `22:30:35` AAPL: n 240 first 2021-08-26 last 2022-08-09 fields ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n'] last row [1660075200000, 164.02, 165.82, 163.25, 164.92, 63075503.0, 164.8395, 480552]
- `22:30:35` ✅ ops 6655 complete
