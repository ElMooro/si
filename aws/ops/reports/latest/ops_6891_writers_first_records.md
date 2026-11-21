# ops 6891 - the changed writers' first records (read-only), 2026-11-21T19:46+00:00

**Status:** success  
**Duration:** 33.9s  
**Finished:** 2026-11-21T19:47:08+00:00  

## Data

| bond_desk_regions_holding_world_score | justhodl-bank-histories | justhodl-polygon-bank | justhodl-repo | repo_counters |
|---|---|---|---|---|
| ["em", "global_funding", "japan", "us"] | {"files": 27127, "written_today": 27127, "kept_from_before_today": 0} | {"files": 131536, "written_today": 7162, "kept_from_before_today": 124374} | {"files": 1698, "written_today": 1658, "kept_from_before_today": 40} | {"history_written": null, "history_unchanged": null} |

## Log
## 1. Files by the time of their last write

- `19:46:35` justhodl-repo -> data/repo-history/ : 1698 files, 38.8 MB | package deployed 2026-11-21T19:18:41
- `19:46:35`   by day of last write: 2026-11-21 1658, 2026-09-18 26, 2026-09-09 2, 2026-08-17 12
- `19:46:35`   newest day 2026-11-21 by hour: 06:00 1658
- `19:47:02` justhodl-polygon-bank -> data/warm/polygon-bank/ : 131536 files, 4800.9 MB | package deployed 2026-11-21T19:18:14
- `19:47:02`   by day of last write: 2026-11-21 7162, 2026-11-20 7407, 2026-11-19 6722, 2026-11-18 8828, 2026-11-17 10056, 2026-11-16 9694, 2026-11-15 7472, 2026-11-14 5951 ... (27 days)
- `19:47:02`   newest day 2026-11-21 by hour: 10:00 1397, 11:00 1130, 12:00 1651, 13:00 1067, 14:00 1392, 15:00 525
- `19:47:07` justhodl-bank-histories -> data/warm/history-book/snapshots/latest/ : 27127 files, 62.6 MB | package deployed 2026-11-21T19:22:52
- `19:47:07`   by day of last write: 2026-11-21 27127
- `19:47:07`   newest day 2026-11-21 by hour: 19:00 27127
## 2. What each engine recorded

- `19:47:08` data/repo.json written 2026-11-21 06:18 | as_of=2026-11-21T06:10:17+00:00; diag.dtcc.error=HTTPError:HTTP Error 404: Not Found; counts.skipped=4; skipped=[4]; settlement_fails.contract.schema_version=1.0; settlement_fails.contract.generated_at=2026-11-20T21:30:39.667691+00:00; settlement_fails.contract.errors=[0]
- `19:47:08` polygon-bank state (data/warm/polygon-bank/_state/state.json) written 2026-11-21 15:03 | version=1.0.0; started_at=2026-10-25T16:43:33.662760+00:00; last_run.started_at=2026-11-21T14:52:42.667569+00:00; last_run.puts=889; last_run.unchanged=354; last_run.put_bytes=26309224; last_run.finished_at=2026-11-21T15:03:36.877295+00:00
- `19:47:08` polygon-bank manifest (data/warm/polygon-bank/manifest.json) written 2026-11-21 15:03 | version=1.2.0; spec_version=1.1.0; skipped=[8]; run.started_at=2026-11-21T14:52:42.667569+00:00; run.puts=889; run.unchanged=354; run.put_bytes=26309224; run.finished_at=2026-11-21T15:03:36.877295+00:00
- `19:47:08` history-book state (data/warm/history-book/_state/state.json) written 2026-11-21 19:26 | last_run.started=2026-11-21T19:24:06.023175+00:00; last_run.puts=27127; last_run.unchanged=0
- `19:47:08` history-book manifest (data/warm/history-book/manifest.json) written 2026-11-21 19:26 | version=1.3.0; as_of=2026-11-20T12:02:55.733758+00:00; as_of_meaning=blocks.<id>.as_of = the bank vintage the block was last checked agains; last_run.started=2026-11-21T19:24:06.023175+00:00
## 3. Bond desk: the regions behind its world score

- `19:47:08` data/bond-desk.json written 2026-11-21 19:43 | world_anxiety None | regime UNAVAILABLE | flow_status dated_issuer_cohorts
- `19:47:08`   em       score None   fresh False calls_eligible False -> HOLDS THE WORLD SCORE: not fresh, no score, calls_eligible false {"status": "unqualified_flow_forecast"}
- `19:47:08`   europe   score 80.1   fresh True  calls_eligible None  -> counts 
- `19:47:08`   global_funding score None   fresh False calls_eligible None  -> HOLDS THE WORLD SCORE: not fresh, no score {"status": "unqualified_funding_vote"}
- `19:47:08`   japan    score None   fresh False calls_eligible None  -> HOLDS THE WORLD SCORE: not fresh, no score {"status": "unqualified_yen_vote"}
- `19:47:08`   us       score None   fresh False calls_eligible False -> HOLDS THE WORLD SCORE: not fresh, no score, calls_eligible false {"status": "unqualified_bond_vol_vote"}
- `19:47:08` ⚠ the bond desk's world score is withheld by 4 region(s): em, global_funding, japan, us - its own required-input rule; not changed here
- `19:47:08` ✅ read-only; nothing changed
