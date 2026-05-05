
# 1) Lambda configuration

- `18:51:40`     state: Active  mem=1536MB  timeout=900s
- `18:51:40`     modified: 2026-05-05T18:39:33.000+0000
- `18:51:40`       CLUSTER_MIN_INSIDERS=2
- `18:51:40`       FMP_KEY=***UlS8xb
- `18:51:40`       LOOKBACK_DAYS=30
- `18:51:40`       MAX_FILINGS_TO_PARSE=3000
- `18:51:40`       MIN_BUY_VALUE_USD=5000
- `18:51:40`       N_BUSINESS_DAYS_INDEX=7
- `18:51:40`       N_WORKERS=12
- `18:51:40`       S3_BUCKET=justhodl-dashboard-live
- `18:51:40`       S3_KEY=***s.json
- `18:51:40`       SEC_USER_AGENT=JustHodl Research raafouis@gmail.com

# 2) EventBridge schedule

- `18:51:40`     rule: justhodl-insider-cluster-scanner-daily  expr=cron(30 14 * * ? *)  state=ENABLED

# 3) S3 output freshness

- `18:51:40`     size: 43,345b
- `18:51:40`     modified: 2026-05-05 18:50:08+00:00

# 4) Full top-25 leaderboard

- `18:51:40`     schema: 2.0
- `18:51:40`     generated_at: 2026-05-05T18:50:07.885475+00:00
- `18:51:40`     stats: {"n_form4_filings_scanned": 7398, "n_form4_parsed": 3000, "n_buy_transactions": 218, "n_unique_tickers": 55, "n_clusters": 22, "n_strong_signals": 8, "n_smart_money_dual": 4, "n_ceo_conviction": 1, "n_cluster_buys": 5, "n_contrarian_clusters": 7}
- `18:51:40`     total clusters: 22
- `18:51:40`   
- `18:51:40`        # Ticker    Score Signal                 Ins  TX     $Total   %52H Mcap       Sector                  CEO  CFO
- `18:51:40`        1 SRAD       90.8 executive_cluster        7  10 $    4.67M   -58% $4.0B      Technology                ✓     
- `18:51:40`        2 SPGI       86.2 executive_cluster        3   3 $    2.58M   -27% $125.2B    Financial Services        ✓     
- `18:51:40`        3 SUNE       80.8 smart_money_dual         2   2 $    1.20M   -57% $5M        Industrials               ✓    ✓
- `18:51:40`        4 FND        78.5 smart_money_dual         2   2 $    0.37M   -48% $5.2B      Consumer Cyclical         ✓    ✓
- `18:51:40`        5 OPCH       78.5 smart_money_dual         2   2 $    0.59M   -42% $3.4B      Healthcare                ✓    ✓
- `18:51:40`        6 CSGP       77.2 ceo_conviction           1   2 $    2.51M   -64% $14.1B     Real Estate               ✓     
- `18:51:40`        7 EPAM       73.5 smart_money_dual         6   6 $    0.04M   -51% $5.8B      Technology                ✓    ✓
- `18:51:40`        8 NWBI       70.0 executive_cluster        3   3 $    0.19M    -2% $2.1B      Financial Services             ✓
- `18:51:40`        9 PSUS       66.0 cluster_buy              8  13 $  311.39M    -3% $1.7B                                      
- `18:51:40`       10 NONE       59.0 cluster_buy              3   3 $   17.20M    +0% ?                                          
- `18:51:40`       11 FGBI       52.8 lone_buy                 2   2 $    2.00M    -7% $148M      Financial Services              
- `18:51:40`       12 AVLN       52.5 lone_buy                 2   3 $   15.00M   -11% $1.1B      Healthcare                      
- `18:51:40`       13 PS         48.0 lone_buy                 1   3 $   19.02M   -16% $12.8B     Financial Services              
- `18:51:40`       14 GLND       46.5 lone_buy                 2   2 $    0.28M   -87% $76M       Energy                          
- `18:51:40`       15 CECO       44.2 lone_buy                 1   2 $    1.10M    -1% $3.1B      Industrials                     
- `18:51:40`       16 AUID       41.5 lone_buy                 2   2 $    0.19M   -82% $16M       Technology                      
- `18:51:40`       17 NMM        38.0 lone_buy                 1   3 $    0.25M    -1% $2.2B      Industrials                     
- `18:51:40`       18 NWFL       36.5 lone_buy                 2   2 $    0.02M    -8% $274M      Financial Services              
- `18:51:40`       19 AXR        34.0 lone_buy                 1   2 $    0.22M    -4% $148M      Real Estate                     
- `18:51:40`       20 XZO        33.0 lone_buy                 1   2 $    0.06M   -30% $1.6B      Financial Services              
- `18:51:40`       21 PCSA       32.5 lone_buy                 2   2 $    0.01M   -86% $6M        Healthcare                      
- `18:51:40`       22 FNLC       32.0 lone_buy                 1   2 $    0.04M    -5% $325M      Financial Services              

# 5) Detailed view of top-3 clusters (full insider lists)

- `18:51:40`   
- `18:51:40`     ── #1 SRAD (Sportradar Group AG) ──
- `18:51:40`       score: 90.8
- `18:51:40`       signal: executive_cluster  rationale: CEO (+6 other) bought $4.67M of SRAD over 1d at $13.04 avg — stock 58% off 52w high
- `18:51:40`       7 insiders, 10 TX, $4,668,318 total
- `18:51:40`       avg price: $13.04  window: 2026-04-30 → 2026-05-01
- `18:51:40`       fundamentals: mcap=$3.98B  ph=-58.3%  pe=None
- `18:51:40`       insiders:
- `18:51:40`         • Koerl Carsten                    Director, Chief Executive Officer      $  3,342,856
- `18:51:40`         • Bigley Deirdre Mary              Director                               $     49,526
- `18:51:40`         • Fleet George                     Director                               $     99,930
- `18:51:40`         • KURTZ WILLIAM                    Director                               $    103,786
- `18:51:40`         • Ramanathan Rajani                Director                               $    100,000
- `18:51:40`         • Walder Marc                      Director                               $    842,820
- `18:51:40`         • YABUKI JEFFERY W                 Director                               $    129,400
- `18:51:40`   
- `18:51:40`     ── #2 SPGI (S&P Global Inc.) ──
- `18:51:40`       score: 86.2
- `18:51:40`       signal: executive_cluster  rationale: CEO (+2 other) bought $2.58M of SPGI over 2d at $431.40 avg — stock 27% off 52w high
- `18:51:40`       3 insiders, 3 TX, $2,576,773 total
- `18:51:40`       avg price: $431.40  window: 2026-04-29 → 2026-05-01
- `18:51:40`       fundamentals: mcap=$125.19B  ph=-27.0%  pe=None
- `18:51:40`       insiders:
- `18:51:40`         • Clay Catherine R                 CEO, S&P Dow Jones Indices             $  1,078,475
- `18:51:40`         • Moritz Robert Edward Jr.         Director                               $    500,001
- `18:51:40`         • CHEUNG MARTINA                   Director, CEO & President              $    998,297
- `18:51:40`   
- `18:51:40`     ── #3 SUNE (SUNation Energy, Inc.) ──
- `18:51:40`       score: 80.8
- `18:51:40`       signal: smart_money_dual  rationale: CEO + CFO + 1 director(s) bought $1.20M of SUNE over 0d at $1.77 avg — stock 57% off 52w high
- `18:51:40`       2 insiders, 2 TX, $1,200,000 total
- `18:51:40`       avg price: $1.77  window: 2026-04-14 → 2026-04-14
- `18:51:40`       fundamentals: mcap=$0.01B  ph=-56.9%  pe=None
- `18:51:40`       insiders:
- `18:51:40`         • Maskin Scott                     Director, Chief Executive Officer, 10% $    981,840
- `18:51:40`         • Brennan James Robert             Chief Financial Officer                $    218,160

# 6) Cross-reference: insider-cluster ∩ nobrainer leaderboard

- `18:51:41`     nobrainer top-25 set: ['AAUKF', 'AIN', 'AMAT', 'APA', 'CRM', 'CSTM', 'DVN', 'FCX', 'LTHM', 'MELI', 'MT', 'NEM', 'OVV', 'OXY', 'REEMF', 'RES', 'RIO', 'RIVN', 'SLI', 'TS', 'TSM', 'TX', 'UPS', 'USAR', 'WTTR']
- `18:51:41`   
- `18:51:41`     insider-cluster set (22): ['AUID', 'AVLN', 'AXR', 'CECO', 'CSGP', 'EPAM', 'FGBI', 'FND', 'FNLC', 'GLND', 'NMM', 'NONE', 'NWBI', 'NWFL', 'OPCH', 'PCSA', 'PS', 'PSUS', 'SPGI', 'SRAD', 'SUNE', 'XZO']
- `18:51:41`   
- `18:51:41`     ── COMPOUND SIGNALS (both lists) ──
- `18:51:41`       (none today — different universes)

# 7) Stats summary

- `18:51:41`     filings scanned: 7398
- `18:51:41`     filings parsed: 3000
- `18:51:41`     buy transactions: 218
- `18:51:41`     unique tickers: 55
- `18:51:41`     clusters: 22
- `18:51:41`     STRONG signals (score≥70): 8
- `18:51:41`     smart_money_dual: 4
- `18:51:41`     ceo_conviction: 1
- `18:51:41`     cluster_buys: 5
- `18:51:41`     contrarian (down >25% from 52H): 7