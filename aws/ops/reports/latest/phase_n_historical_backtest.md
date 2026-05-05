
# 0) Fetch SPY 270d history (single fetch)

- `23:58:35`     ✓ SPY: 270 days, range 2025-04-08 → 2026-05-05

# 1) Run as-if backtest for each pump-list name

- `23:58:35`     At each evaluation date, only data ≤ that date is used.
- `23:58:35`     We track: when did score first cross 60 and 75? And what was % gain at that point?
- `23:58:35`   
- `23:58:35`     ── AXTI (today's total gain: +464%) ──
- `23:58:35`       First crossed 60: 2025-10-27  score=70  gain_so_far=+369%  remaining_gain=+1637%
- `23:58:35`       Never crossed 75
- `23:58:35`   
- `23:58:35`     ── LWLG (today's total gain: +408%) ──
- `23:58:36`       First crossed 60: 2025-10-03  score=70  gain_so_far=+395%  remaining_gain=+256%
- `23:58:36`       Never crossed 75
- `23:58:36`   
- `23:58:36`     ── AAOI (today's total gain: +353%) ──
- `23:58:36`       First crossed 60: 2025-09-10  score=65  gain_so_far=+159%  remaining_gain=+551%
- `23:58:36`       First crossed 75: 2026-01-28  score=79  gain_so_far=+323%  remaining_gain=+299%
- `23:58:36`   
- `23:58:36`     ── AEHR (today's total gain: +277%) ──
- `23:58:36`       First crossed 60: 2025-07-21  score=65  gain_so_far=+143%  remaining_gain=+454%
- `23:58:36`       First crossed 75: 2025-08-26  score=75  gain_so_far=+282%  remaining_gain=+252%
- `23:58:36`   
- `23:58:36`     ── ICHR (today's total gain: +138%) ──
- `23:58:36`       First crossed 60: 2025-10-27  score=62  gain_so_far=+41%  remaining_gain=+189%
- `23:58:36`       First crossed 75: 2026-02-09  score=78  gain_so_far=+105%  remaining_gain=+99%
- `23:58:36`   
- `23:58:36`     ── MRVL (today's total gain: +130%) ──
- `23:58:37`       First crossed 60: 2025-09-30  score=61  gain_so_far=+68%  remaining_gain=+101%
- `23:58:37`       Never crossed 75
- `23:58:37`   
- `23:58:37`     ── INTC (today's total gain: +122%) ──
- `23:58:37`       First crossed 60: 2025-09-18  score=80  gain_so_far=+69%  remaining_gain=+254%
- `23:58:37`       First crossed 75: 2025-09-18  score=80  gain_so_far=+69%  remaining_gain=+254%
- `23:58:37`   
- `23:58:37`     ── LITE (today's total gain: +116%) ──
- `23:58:37`       First crossed 60: 2025-07-30  score=62  gain_so_far=+120%  remaining_gain=+805%
- `23:58:37`       First crossed 75: 2025-08-13  score=77  gain_so_far=+140%  remaining_gain=+727%
- `23:58:37`   
- `23:58:37`     ── CRDO (today's total gain: +101%) ──
- `23:58:37`       First crossed 60: 2025-07-29  score=61  gain_so_far=+210%  remaining_gain=+77%
- `23:58:37`       Never crossed 75
- `23:58:37`   

# 2) Conclusion — early-detection capability assessment

- `23:58:37`     A 'good catch' = score crosses 60 BEFORE more than 50% of the gain has happened.
- `23:58:37`     A 'late catch' = score crosses 60 AFTER more than 50% of gain.