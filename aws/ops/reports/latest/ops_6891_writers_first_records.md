# ops 6891 - the changed writers' first records (read-only), 2026-11-22T15:42+00:00

**Status:** success  
**Duration:** 33.8s  
**Finished:** 2026-11-22T15:43:08+00:00  

## Data

| bond_desk_regions_holding_world_score | justhodl-bank-histories | justhodl-polygon-bank | justhodl-repo | repo_counters |
|---|---|---|---|---|
| ["em", "global_funding", "japan", "us"] | {"files": 27141, "written_today": 25032, "kept_from_before_today": 2109} | {"files": 132915, "written_today": 9110, "kept_from_before_today": 123805} | {"files": 1841, "written_today": 103, "kept_from_before_today": 1738} | {"history_written": 5, "history_unchanged": 1798} |

## Log
## 1. Files by the time of their last write

- `15:42:35` justhodl-repo -> data/repo-history/ : 1841 files, 42.6 MB | package deployed 2026-11-22T07:04:44
- `15:42:35`   by day of last write: 2026-11-22 103, 2026-11-21 1700, 2026-09-18 26, 2026-08-17 12
- `15:42:35`   newest day 2026-11-22 by hour: 06:00 103
- `15:43:01` justhodl-polygon-bank -> data/warm/polygon-bank/ : 132915 files, 4809.8 MB | package deployed 2026-11-21T19:18:14
- `15:43:01`   by day of last write: 2026-11-22 9110, 2026-11-21 8040, 2026-11-20 7405, 2026-11-19 6722, 2026-11-18 8824, 2026-11-17 8994, 2026-11-16 7331, 2026-11-15 6858 ... (28 days)
- `15:43:01`   newest day 2026-11-22 by hour: 10:00 1305, 11:00 1581, 12:00 2305, 13:00 1565, 14:00 1574, 15:00 780
- `15:43:07` justhodl-bank-histories -> data/warm/history-book/snapshots/latest/ : 27141 files, 62.5 MB | package deployed 2026-11-21T19:22:52
- `15:43:07`   by day of last write: 2026-11-22 25032, 2026-11-21 2109
- `15:43:07`   newest day 2026-11-22 by hour: 03:00 25032
## 2. What each engine recorded

- `15:43:07` data/repo.json written 2026-11-22 06:41 | as_of=2026-11-22T06:37:16+00:00; counts.skipped=7; counts.history_written=5; counts.history_unchanged=1798; skipped=[7]; settlement_fails.contract.schema_version=1.0; settlement_fails.contract.generated_at=2026-11-21T21:30:39.834735+00:00; settlement_fails.contract.errors=[0]
- `15:43:07` polygon-bank state (data/warm/polygon-bank/_state/state.json) written 2026-11-22 15:10 | version=1.0.0; started_at=2026-10-25T16:43:33.662760+00:00; last_run.started_at=2026-11-22T14:57:25.139835+00:00; last_run.puts=785; last_run.unchanged=369; last_run.put_bytes=39610064; last_run.finished_at=2026-11-22T15:10:05.573659+00:00
- `15:43:07` polygon-bank manifest (data/warm/polygon-bank/manifest.json) written 2026-11-22 15:10 | version=1.2.1; spec_version=1.1.0; skipped=[8]; run.started_at=2026-11-22T14:57:25.139835+00:00; run.puts=785; run.unchanged=369; run.put_bytes=39610064; run.finished_at=2026-11-22T15:10:05.573659+00:00
- `15:43:07` history-book state (data/warm/history-book/_state/state.json) written 2026-11-22 03:14 | last_run.started=2026-11-22T03:05:28.737223+00:00; last_run.puts=26051; last_run.unchanged=247032
- `15:43:08` history-book manifest (data/warm/history-book/manifest.json) written 2026-11-22 03:14 | version=1.3.0; as_of=2026-11-21T23:25:20.360454+00:00; as_of_meaning=blocks.<id>.as_of = the bank vintage the block was last checked agains; last_run.started=2026-11-22T03:05:28.737223+00:00
## 3. Bond desk: the regions behind its world score

- `15:43:08` data/bond-desk.json written 2026-11-21 19:43 | world_anxiety None | regime UNAVAILABLE | flow_status dated_issuer_cohorts
- `15:43:08`   em       score None   fresh False calls_eligible False -> HOLDS THE WORLD SCORE: not fresh, no score, calls_eligible false {"status": "unqualified_flow_forecast"}
- `15:43:08`   europe   score 80.1   fresh True  calls_eligible None  -> counts 
- `15:43:08`   global_funding score None   fresh False calls_eligible None  -> HOLDS THE WORLD SCORE: not fresh, no score {"status": "unqualified_funding_vote"}
- `15:43:08`   japan    score None   fresh False calls_eligible None  -> HOLDS THE WORLD SCORE: not fresh, no score {"status": "unqualified_yen_vote"}
- `15:43:08`   us       score None   fresh False calls_eligible False -> HOLDS THE WORLD SCORE: not fresh, no score, calls_eligible false {"status": "unqualified_bond_vol_vote"}
- `15:43:08` ⚠ the bond desk's world score is withheld by 4 region(s): em, global_funding, japan, us - its own required-input rule; not changed here
- `15:43:08` ✅ read-only; nothing changed
