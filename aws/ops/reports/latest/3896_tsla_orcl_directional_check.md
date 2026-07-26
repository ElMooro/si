# ops 3896 — TSLA/ORCL full directional time-series + earnings catalyst check

**Status:** success  
**Duration:** 5.6s  
**Finished:** 2026-07-26T00:28:50+00:00  

## Data

| ORCL_bearish_first | ORCL_bearish_last | TSLA_bearish_first | TSLA_bearish_last | earliest | latest | n_daily_thinned | n_dense_snapshots | n_total_archive_files |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  | data/archive/convergence-radar/20260601_1643.json | data/archive/convergence-radar/20260726_0000.json |  |  | 2614 |
|  |  |  |  |  |  | 37 | 1731 |  |
|  |  | 20260620=0 | 20260726=0 |  |  |  |  |  |
| 20260620=1 | 20260726=1 |  |  |  |  |  |  |  |

## Log
## 1. list every archive key (real S3 listing, not a guessed date format)

## 2. dense sample across June 20 - July 25 specifically (the decline window)

## 3. full directional record for TSLA and ORCL, every day, June 20 - July 25

- `00:28:49`   === TSLA — day-by-day, 37 snapshots found ===
- `00:28:49`     20260620: n_eng=9 bull=2 bear=0 dir_score=38.0 conv=93.0 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260621: n_eng=10 bull=2 bear=0 dir_score=38.5 conv=92.5 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260622: n_eng=10 bull=2 bear=0 dir_score=37.3 conv=90.5 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260623: n_eng=10 bull=2 bear=0 dir_score=37.3 conv=91.1 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260624: n_eng=9 bull=3 bear=0 dir_score=54.3 conv=92.4 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260625: n_eng=11 bull=3 bear=0 dir_score=51.0 conv=92.2 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260626: n_eng=9 bull=3 bear=0 dir_score=53.8 conv=92.4 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260627: n_eng=10 bull=3 bear=0 dir_score=52.6 conv=91.9 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260628: n_eng=8 bull=2 bear=0 dir_score=43.8 conv=91.6 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260629: n_eng=8 bull=2 bear=0 dir_score=43.8 conv=91.6 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260630: n_eng=10 bull=3 bear=0 dir_score=53.6 conv=94.3 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260701: n_eng=8 bull=2 bear=0 dir_score=42.7 conv=86.0 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260702: n_eng=10 bull=2 bear=1 dir_score=31.4 conv=97.4 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260703: n_eng=11 bull=2 bear=1 dir_score=34.5 conv=98.5 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260704: n_eng=10 bull=2 bear=1 dir_score=35.4 conv=94.9 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260705: n_eng=10 bull=2 bear=0 dir_score=37.3 conv=93.1 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260706: n_eng=10 bull=2 bear=0 dir_score=41.6 conv=89.8 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260707: n_eng=11 bull=2 bear=0 dir_score=36.1 conv=93.5 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260708: n_eng=11 bull=2 bear=0 dir_score=36.1 conv=92.4 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260709: n_eng=10 bull=2 bear=0 dir_score=36.7 conv=93.3 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260710: n_eng=10 bull=2 bear=0 dir_score=39.5 conv=86.8 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260711: n_eng=10 bull=2 bear=0 dir_score=39.6 conv=93.3 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260712: n_eng=12 bull=2 bear=0 dir_score=36.6 conv=93.5 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260713: n_eng=10 bull=2 bear=0 dir_score=41.0 conv=94.4 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260714: n_eng=11 bull=2 bear=0 dir_score=36.1 conv=92.7 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260715: n_eng=11 bull=2 bear=0 dir_score=36.1 conv=92.5 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260716: n_eng=10 bull=2 bear=0 dir_score=39.5 conv=86.8 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260717: n_eng=9 bull=2 bear=0 dir_score=40.5 conv=90.1 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260718: n_eng=10 bull=2 bear=0 dir_score=39.6 conv=94.6 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260719: n_eng=12 bull=2 bear=0 dir_score=35.6 conv=94.4 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260720: n_eng=12 bull=2 bear=0 dir_score=35.6 conv=94.1 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260721: n_eng=11 bull=3 bear=0 dir_score=50.5 conv=94.7 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260722: n_eng=10 bull=2 bear=0 dir_score=39.5 conv=86.8 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260723: n_eng=12 bull=3 bear=0 dir_score=49.9 conv=92.7 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260724: n_eng=12 bull=2 bear=0 dir_score=35.4 conv=92.1 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260725: n_eng=11 bull=2 bear=0 dir_score=36.5 conv=91.3 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260726: n_eng=10 bull=2 bear=0 dir_score=38.7 conv=92.7 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`   === ORCL — day-by-day, 37 snapshots found ===
- `00:28:49`     20260620: n_eng=9 bull=3 bear=1 dir_score=37.0 conv=88.7 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260621: n_eng=8 bull=3 bear=1 dir_score=38.5 conv=88.4 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260622: n_eng=9 bull=3 bear=1 dir_score=37.7 conv=86.9 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260623: n_eng=7 bull=3 bear=1 dir_score=39.6 conv=85.3 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260624: n_eng=8 bull=3 bear=1 dir_score=36.0 conv=89.3 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260625: n_eng=8 bull=3 bear=1 dir_score=40.0 conv=91.8 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260626: n_eng=8 bull=3 bear=1 dir_score=36.0 conv=89.5 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260627: n_eng=8 bull=3 bear=1 dir_score=38.6 conv=88.0 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260628: n_eng=9 bull=3 bear=1 dir_score=37.3 conv=88.0 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260629: n_eng=8 bull=3 bear=1 dir_score=37.9 conv=88.9 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260630: n_eng=8 bull=3 bear=1 dir_score=40.0 conv=91.6 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260701: n_eng=8 bull=3 bear=1 dir_score=37.9 conv=88.0 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260702: n_eng=8 bull=3 bear=0 dir_score=44.8 conv=83.0 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260703: n_eng=10 bull=3 bear=1 dir_score=36.2 conv=87.3 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260704: n_eng=8 bull=3 bear=1 dir_score=39.2 conv=90.6 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260705: n_eng=8 bull=4 bear=1 dir_score=50.6 conv=90.7 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260706: n_eng=7 bull=3 bear=1 dir_score=39.6 conv=84.9 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260707: n_eng=6 bull=3 bear=0 dir_score=48.4 conv=77.6 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260708: n_eng=9 bull=4 bear=0 dir_score=54.6 conv=91.2 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260709: n_eng=7 bull=3 bear=0 dir_score=45.9 conv=82.9 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260710: n_eng=6 bull=3 bear=0 dir_score=57.9 conv=81.9 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260711: n_eng=6 bull=3 bear=0 dir_score=57.9 conv=83.4 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260712: n_eng=5 bull=3 bear=0 dir_score=62.0 conv=71.6 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260713: n_eng=6 bull=3 bear=1 dir_score=51.0 conv=84.6 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260714: n_eng=8 bull=3 bear=1 dir_score=48.3 conv=92.0 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260715: n_eng=7 bull=3 bear=1 dir_score=49.5 conv=90.1 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260716: n_eng=6 bull=3 bear=0 dir_score=57.9 conv=81.4 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260717: n_eng=7 bull=3 bear=1 dir_score=48.2 conv=89.6 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260718: n_eng=8 bull=3 bear=1 dir_score=46.5 conv=87.0 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260719: n_eng=7 bull=3 bear=1 dir_score=49.0 conv=89.3 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260720: n_eng=6 bull=3 bear=0 dir_score=59.2 conv=81.9 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260721: n_eng=8 bull=3 bear=1 dir_score=48.7 conv=90.1 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260722: n_eng=7 bull=3 bear=0 dir_score=55.5 conv=82.1 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260723: n_eng=7 bull=3 bear=0 dir_score=55.5 conv=82.1 exclude_from_longs=False tier=HIGH accelerating=False
- `00:28:49`     20260724: n_eng=9 bull=3 bear=1 dir_score=44.7 conv=85.7 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260725: n_eng=8 bull=3 bear=0 dir_score=55.1 conv=83.5 exclude_from_longs=False tier=ULTRA accelerating=False
- `00:28:49`     20260726: n_eng=8 bull=3 bear=1 dir_score=47.8 conv=88.7 exclude_from_longs=False tier=ULTRA accelerating=False
## 4. did the bearish count meaningfully RISE before/during the decline, for either name

## 5. earnings-tracker — was there a real, dated earnings catalyst for either name

- `00:28:50`   TSLA EARNINGS FOUND: {"ticker": "TSLA", "filing_date": "2026-07-22", "period_end": "Q2 2026", "eps_actual": 0.33, "eps_estimate": 0.44, "eps_surprise_pct": -25.0, "revenue_actual": 28236000000.0, "revenue_surprise_pct": 11.89, "importance": 5, "returns": {"1d": -14.52, "5d": null, "20d": null}, "pead_label": "NEGATIVE_DRIFT", "pead_score": 23, "surprise_source": "benzinga"}
- `00:28:50`   ORCL: no entry in recent_results_30d (no earnings report in the last 30d, or it aged out — decline is NOT earnings-driven per this feed)
- `00:28:50` ✅ PROBE COMPLETE
