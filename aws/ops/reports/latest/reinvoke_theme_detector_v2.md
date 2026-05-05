- `13:48:34`   ✓ active, code mod: 2026-05-05T13:40:15.000+0000
# 1) Re-invoke with LogType=Tail (captures full last 4KB)
**Status:** success  
**Duration:** 29.5s  
**Finished:** 2026-05-05T13:49:04+00:00  

## Log

- `13:49:03`   duration: 28.9s, status: 200
- `13:49:03` 
- `13:49:03`   ── Tail logs (last 4KB) ────────────────────────
- `13:49:03`     [theme-detector] fetching 550 tickers from Polygon
- `13:49:03`     [poly] ABB no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] AZRE no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] BMWYY no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] DDAIF no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] DML no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] EDP no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] FM no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] FMG no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] FRES no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] ICICI no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] LTHM no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] LYC no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] NDA no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] MRO no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] ORSTED no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] RELIANCE no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] RWE no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] SQ no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] SXTA no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] TMR no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] TRQ no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] TTM no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] VLKAY no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] VEDL no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] VWS no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] TKAYY no_results status=DELAYED count=0 err=None
- `13:49:03`     [poly] WRK no_results status=DELAYED count=0 err=None
- `13:49:03`     [theme-detector] fetched 522 ok / 28 failed in 26.9s
- `13:49:03`     [theme-detector] wrote 57,869b to data/themes-detected.json
- `13:49:03`     [theme-detector] phase distribution: {'DORMANT': 5, 'EMERGING': 32, 'ACCELERATING': 4, 'EXTENDED': 8, 'PEAKING': 11, 'COOLING': 13, 'DYING': 6}
- `13:49:03`     [theme-detector] hottest: ['USO', 'REMX', 'SOXX', 'LIT', 'XOP', 'SMH']
- `13:49:03`     [theme-detector] tier-2 hunt grounds: ['XOP', 'REMX', 'USO', 'PICK', 'SLV']
- `13:49:03`   ────────────────────────────────────────────────
# 2) Parse response body

- `13:49:03`   n_themes:           79
- `13:49:03`   duration_s:         28.0
- `13:49:03`   phase_distribution: {'DORMANT': 5, 'EMERGING': 32, 'ACCELERATING': 4, 'EXTENDED': 8, 'PEAKING': 11, 'COOLING': 13, 'DYING': 6}
- `13:49:03`   HOTTEST:            ['USO', 'REMX', 'SOXX', 'LIT', 'XOP', 'SMH']
- `13:49:03`   TIER-2 grounds:     ['XOP', 'REMX', 'USO', 'PICK', 'SLV']
- `13:49:03`   EMERGING:           ['AIQ', 'ROBO', 'GRID', 'EEM', 'BOTZ']
- `13:49:03`   DYING:              ['IHI', 'UNG', 'KWEB', 'IGV', 'WCLD']
# 3) Pull fresh CloudWatch log stream (more than 4KB tail)

- `13:49:04`   stream: 2026/05/05/[$LATEST]24def5c17c5f405c85557f44cbe7e0ec
- `13:49:04` 
- `13:49:04`   Full log events (max 300):
- `13:49:04` 
- `13:49:04`   ── [poly] lines (errors): 27 ──
- `13:49:04`     [poly] ABB no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] AZRE no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] BMWYY no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] DDAIF no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] DML no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] EDP no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] FM no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] FMG no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] FRES no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] ICICI no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] LTHM no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] LYC no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] NDA no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] MRO no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] ORSTED no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] RELIANCE no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] RWE no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] SQ no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] SXTA no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] TMR no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] TRQ no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] TTM no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] VLKAY no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] VEDL no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] VWS no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] TKAYY no_results status=DELAYED count=0 err=None
- `13:49:04`     [poly] WRK no_results status=DELAYED count=0 err=None
- `13:49:04` 
- `13:49:04`   ── Other lines: 1 ──
- `13:49:04`     [theme-detector] fetching 550 tickers from Polygon
# 4) S3 themes-detected.json contents

- `13:49:04`   generated_at: 2026-05-05T13:48:35.685942+00:00
- `13:49:04`   n_themes:     79
- `13:49:04`   fetch_stats:  {'n_tickers': 550, 'n_ok': 522, 'n_fail': 28, 'fetch_duration_s': 26.9}
- `13:49:04`   summary:      {
  "n_themes": 79,
  "phase_distribution": {
    "DORMANT": 5,
    "EMERGING": 32,
    "ACCELERATING": 4,
    "EXTENDED": 8,
    "PEAKING": 11,
    "COOLING": 13,
    "DYING": 6
  },
  "hottest_themes": [
    "USO",
    "REMX",
    "SOXX",
    "LIT",
    "XOP",
    "SMH"
  ],
  "best_for_tier2_hunting": [
    "XOP",
    "REMX",
    "USO",
    "PICK",
    "SLV"
  ],
  "dying_themes": [
    "IHI",
    "UNG",
    "KWEB",
    "IGV",
    "WCLD"
  ],
  "emerging_themes": [
    "AIQ",
    "ROBO",
    "GRID",
    "EEM",
    "BOTZ"
  ],
  "spy_returns": {
    "5d": 0.45,
    "30d": 10.07,
    "90d": 4
- `13:49:04` 
- `13:49:04`   Top 8 themes (EXTENDED + ACCELERATING first):
- `13:49:04`     XOP   Oil & Gas E&P                    EXTENDED      score=100
- `13:49:04`       30d=0.68% 90d=28.01% 180d=43.44% 365d=58.4%
- `13:49:04`       → Oil & Gas E&P has run hard: +43.4% 6m, +58.4% 12m, vol pct +82.4. TIER-2 HUNT GROUND — laggards inside this theme are the trade. Tier-1 leader: MPC.
- `13:49:04`     REMX  Rare Earth Metals                EXTENDED      score=100
- `13:49:04`       30d=16.77% 90d=15.09% 180d=62.76% 365d=173.98%
- `13:49:04`       → Rare Earth Metals has run hard: +62.8% 6m, +174.0% 12m, vol pct +94.1. TIER-2 HUNT GROUND — laggards inside this theme are the trade. Tier-1 leader: MP.
- `13:49:04`     USO   Oil                              EXTENDED      score=100
- `13:49:04`       30d=4.39% 90d=85.85% 180d=102.7% 365d=130.85%
- `13:49:04`       → Oil has run hard: +102.7% 6m, +130.8% 12m, vol pct +99.5. TIER-2 HUNT GROUND — laggards inside this theme are the trade. Tier-1 leader: USO.
- `13:49:04`     PICK  Mining (broad)                   EXTENDED      score=90
- `13:49:04`       30d=6.64% 90d=0.26% 180d=35.19% 365d=70.6%
- `13:49:04`       → Mining (broad) has run hard: +35.2% 6m, +70.6% 12m, vol pct +99.0. TIER-2 HUNT GROUND — laggards inside this theme are the trade. Tier-1 leader: BHP.
- `13:49:04`     SLV   Silver (physical)                EXTENDED      score=90
- `13:49:04`       30d=1.46% 90d=-13.27% 180d=52.92% 365d=126.42%
- `13:49:04`       → Silver (physical) has run hard: +52.9% 6m, +126.4% 12m, vol pct +97.6. TIER-2 HUNT GROUND — laggards inside this theme are the trade. Tier-1 leader: SLV.
- `13:49:04`     OIH   Oil Services                     EXTENDED      score=85
- `13:49:04`       30d=10.24% 90d=23.11% 180d=55.18% 365d=104.55%
- `13:49:04`       → Oil Services has run hard: +55.2% 6m, +104.5% 12m, vol pct +3.4. TIER-2 HUNT GROUND — laggards inside this theme are the trade. Tier-1 leader: SLB.
- `13:49:04`     FAN   Wind Energy                      EXTENDED      score=70
- `13:49:04`       30d=7.84% 90d=16.36% 180d=29.89% 365d=68.53%
- `13:49:04`       → Wind Energy has run hard: +29.9% 6m, +68.5% 12m, vol pct +87.8. TIER-2 HUNT GROUND — laggards inside this theme are the trade. Tier-1 leader: NEE.
- `13:49:04`     SLX   Steel                            EXTENDED      score=60
- `13:49:04`       30d=11.84% 90d=5.83% 180d=34.08% 365d=67.38%
- `13:49:04`       → Steel has run hard: +34.1% 6m, +67.4% 12m, vol pct +82.4. TIER-2 HUNT GROUND — laggards inside this theme are the trade. Tier-1 leader: NUE.
