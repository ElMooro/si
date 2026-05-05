
# 1) L4 nobrainers.json — full breakdown

- `17:07:28`     generated_at: 2026-05-05T16:32:42.050041+00:00
- `17:07:28`     TIER_A:    9
- `17:07:28`     TIER_B:    33
- `17:07:28`     TIER_C:    11
- `17:07:28`     MU-grade:  25
- `17:07:28`     total scored: None
- `17:07:28`   
- `17:07:28`     ── 9 TIER_A NOBRAINERS ──
- `17:07:28`       TX     SLX    score= 86.5  ta= 77.5  inflate= 50.0  supply= 94.7  val= 68.8  catalyst=100.0
- `17:07:28`       USAR   REMX   score= 85.8  ta= 89.5  inflate= 50.0  supply= 91.8  val= 60.2  catalyst=100.0
- `17:07:28`       CSTM   REMX   score= 83.0  ta= 89.5  inflate= 50.0  supply= 91.8  val= 70.2  catalyst= 50.0
- `17:07:28`       MT     SLX    score= 82.1  ta= 77.5  inflate= 50.0  supply= 94.7  val= 72.9  catalyst= 50.0
- `17:07:28`       APA    XOP    score= 81.8  ta= 89.5  inflate= 50.0  supply= 68.6  val= 73.7  catalyst=100.0
- `17:07:28`       TS     SLX    score= 81.5  ta= 77.5  inflate= 50.0  supply= 94.7  val= 50.6  catalyst=100.0
- `17:07:28`       OVV    XOP    score= 80.9  ta= 89.5  inflate= 50.0  supply= 68.6  val= 70.3  catalyst=100.0
- `17:07:28`       AAUKF  PICK   score= 80.8  ta= 86.5  inflate= 50.0  supply= 84.5  val= 73.3  catalyst= 50.0
- `17:07:28`       DVN    XOP    score= 80.4  ta= 89.5  inflate= 50.0  supply= 68.6  val= 68.3  catalyst=100.0

# 2) Top 12 MU-grade (mcap_to_rev<=3 with high score)

- `17:07:28`       TX     SLX    score= 86.5  mcap/rev=0.551  P/S=0.5514070231084334  rev=$15.58B
- `17:07:28`       CSTM   REMX   score= 83.0  mcap/rev=0.472  P/S=0.47446413384606545  rev=$9.34B
- `17:07:28`       MT     SLX    score= 82.1  mcap/rev=0.696  P/S=0.6936128256906032  rev=$62.02B
- `17:07:28`       APA    XOP    score= 81.8  mcap/rev=1.641  P/S=1.633484529147982  rev=$8.88B
- `17:07:28`       OVV    XOP    score= 80.9  mcap/rev=1.825  P/S=2.0109465431888016  rev=$9.68B
- `17:07:28`       DVN    XOP    score= 80.4  mcap/rev=1.906  P/S=1.907346564900662  rev=$16.62B
- `17:07:28`       TSM    SOXX   score= 79.2  mcap/rev=2.515  P/S=14.2177154284592  rev=$820.83B
- `17:07:28`       OXY    XOP    score= 78.5  mcap/rev=2.353  P/S=2.361577419160232  rev=$24.96B
- `17:07:28`       RES    OIH    score= 78.0  mcap/rev=1.015  P/S=1.0594376840533983  rev=$1.70B
- `17:07:28`       RIO    SLX    score= 77.6  mcap/rev=2.806  P/S=2.8022300430975795  rev=$57.81B
- `17:07:28`       UPS    BOTZ   score= 77.5  mcap/rev=0.942  P/S=0.9420610188939831  rev=$88.30B
- `17:07:28`       AIN    ROBO   score= 77.0  mcap/rev=1.364  P/S=1.3657934247684533  rev=$1.21B

# 3) Verify schema match for nobrainers.html

- `17:07:28`     data top-level keys: ['schema_version', 'method', 'generated_at', 'duration_s', 'layers_loaded', 'n_candidates_scored', 'n_unique_tickers', 'summary', 'all_scored', 'schema']
- `17:07:28`     schema expectations in HTML: ['fcf_yield', 'gross_margin', 'innerHTML', 'market_cap', 'mcap_to_rev', 'p_e', 'p_s', 'rev_growth_ttm', 'revenue_ttm']

# 4) L5 Telegram digest verification

- `17:07:28`     L5 env keys: ['SKIP_CLAUDE', 'TELEGRAM_BOT_TOKEN', 'N_THESES', 'MIN_SCORE', 'ANTHROPIC_KEY', 'N_DIGEST']
- `17:07:28`     TELEGRAM_BOT_TOKEN present: True
- `17:07:28`     ANTHROPIC_KEY present: True
- `17:07:28`     N_DIGEST: 5  N_THESES: 12  MIN_SCORE: 70
- `17:07:28`     SKIP_CLAUDE: 0
- `17:07:28`     no SSM param /justhodl/telegram/last_nobrainer_digest

# 5) S3 dashboard pages — themes/nobrainers reachability

- `17:07:28`     ⚠ themes.html not in S3 (served from GitHub Pages): An error occurred (404) when calling the HeadObject operation: Not Found
- `17:07:28`     ⚠ nobrainers.html not in S3 (served from GitHub Pages): An error occurred (404) when calling the HeadObject operation: Not Found

# 6) GitHub Pages liveness check via curl from inside Action

- `17:07:28`     200  https://justhodl.ai/nobrainers.html
- `17:07:28`     200  https://justhodl.ai/themes.html
- `17:07:28`     200  https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/nobrainers.json