
# 1) Build zip

- `19:29:07`     zip size: 13,882b

# 2) Create or update Lambda

- `19:29:08`     creating new Lambda
- `19:29:12`     ✓ deployed, mod=2026-05-05T19:29:08.526+0000

# 3) Schedule daily 09:00 UTC

- `19:29:13`     ✓ justhodl-smart-money-cluster-daily scheduled cron(0 9 * * ? *)

# 4) Smoke-invoke

- `19:29:16`     status: 200  duration: 3.4s
- `19:29:16`     body: {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"n_clusters_scored\": 85, \"n_strong\": 6, \"n_high_conviction\": 20, \"n_legend_fund_buys\": 35, \"duration_s\": 2.5, \"top_5\": [{\"ticker\": \"MOH\", \"score\": 86.0, \"flag\": \"STRONG_CONVICTION\"}, {\"ticker\": \"LLY\", \"score\": 82.8, \"flag\": \"STRONG_CONVICTION\"}, {\"ticker\": \"AMZN\", \"score\": 76.4, \"flag\": \"STRONG_CONVICTION\"}, {\"ticker\": \"CAH\", \"score\": 75.1, \"flag\": \"STRONG_CONVICTION\
- `19:29:16`     ── tail ──
- `19:29:16`       START RequestId: c93da451-87e6-4d2e-9dd4-ad75a1807bbf Version: $LATEST
- `19:29:16`       [smart-money] starting smart-money cluster scanner
- `19:29:16`       [smart-money] loaded 7940 13F-tracked stocks
- `19:29:16`       [smart-money] as_of_quarter: 2025-12-31
- `19:29:16`       [smart-money] candidates passed filter: 85
- `19:29:16`       [smart-money] fetching FMP quotes for 85 tickers
- `19:29:16`       [smart-money] fetched 85 quotes in 1.3s
- `19:29:16`       [smart-money] wrote 152,894b to data/smart-money-clusters.json
- `19:29:16`       [smart-money] strong: 6  high: 20  legend buys: 35
- `19:29:16`       [smart-money] TOP 5: [('MOH', 86.0, 'STRONG_CONVICTION'), ('LLY', 82.8, 'STRONG_CONVICTION'), ('AMZN', 76.4, 'STRONG_CONVICTION'), ('CAH', 75.1, 'STRONG_CONVICTION'), ('AXP', 74.1, 'STRONG_CONVICTION')]
- `19:29:16`       END RequestId: c93da451-87e6-4d2e-9dd4-ad75a1807bbf
- `19:29:16`       REPORT RequestId: c93da451-87e6-4d2e-9dd4-ad75a1807bbf	Duration: 2666.77 ms	Billed Duration: 3190 ms	Memory Size: 512 MB	Max Memory Used: 159 MB	Init Duration: 522.95 ms

# 5) Read S3 + dump top 25 clusters

- `19:29:16`     generated_at: 2026-05-05T19:29:16+00:00
- `19:29:16`     stats: {"n_total_13f_stocks": 7940, "n_candidates": 85, "n_clusters_scored": 85, "n_strong": 6, "n_high_conviction": 20, "n_moderate": 26, "n_legend_fund_buys": 35, "n_new_init_clusters": 9, "n_deep_value": 12, "n_consensus_buys": 14}
- `19:29:16`     n_clusters: 85
- `19:29:16`   
- `19:29:16`        # Ticker   Score Flag                   #Buy #Sell #New  %52H Legends                  
- `19:29:16`        1 MOH       86.0 STRONG_CONVICTION         6     4    3  -42% SCION,LONE_PINE          
- `19:29:16`        2 LLY       82.8 STRONG_CONVICTION         8     1    2  -13% SOROS,DURATION           
- `19:29:16`        3 AMZN      76.4 STRONG_CONVICTION        10     3    2   -2% PERSHING,SOROS,COATUE,LON
- `19:29:16`        4 CAH       75.1 STRONG_CONVICTION         6     1    2  -16% DURATION                 
- `19:29:16`        5 AXP       74.1 STRONG_CONVICTION         6     1    1  -18% BERKSHIRE,DURATION       
- `19:29:16`        6 AVGO      72.0 STRONG_CONVICTION         9     0    1   -1% SOROS,BAUPOST            
- `19:29:16`        7 ALLY      67.0 HIGH_CONVICTION           7     3    1   -8% BERKSHIRE,SOROS,DURATION 
- `19:29:16`        8 CHKP      66.3 HIGH_CONVICTION           6     3    1  -50%                          
- `19:29:16`        9 V         65.0 HIGH_CONVICTION           6     2    1  -14% BAUPOST                  
- `19:29:16`       10 VST       64.7 HIGH_CONVICTION           5     1    1  -27%                          
- `19:29:16`       11 NVDA      62.9 HIGH_CONVICTION           6     5    1   -9% SOROS,SCION              
- `19:29:16`       12 TSLA      62.5 HIGH_CONVICTION           5     3    1  -21% SOROS                    
- `19:29:16`       13 STLA      62.0 HIGH_CONVICTION           6     2    0  -41% DURATION                 
- `19:29:16`       14 HD        61.4 HIGH_CONVICTION           4     3    2  -26%                          
- `19:29:16`       15 GOOG      60.6 HIGH_CONVICTION           7     2    0   -2% PERSHING,COATUE,DURATION 
- `19:29:16`       16 ALB       59.3 HIGH_CONVICTION           5     2    1   -9% DURATION                 
- `19:29:16`       17 GOOGL     58.8 HIGH_CONVICTION           9     4    0   -1% BERKSHIRE,SOROS,COATUE,TI
- `19:29:16`       18 MMC       58.0 HIGH_CONVICTION           4     1    1  -26%                          
- `19:29:16`       19 WMT       57.5 HIGH_CONVICTION           5     3    1   -2% SOROS                    
- `19:29:16`       20 AMP       56.3 HIGH_CONVICTION           6     3    1  -13%                          
- `19:29:16`       21 EEM       56.0 HIGH_CONVICTION           4     1    2   -1%                          
- `19:29:16`       22 VRTX      55.5 HIGH_CONVICTION           5     3    1  -16%                          
- `19:29:16`       23 MA        55.4 HIGH_CONVICTION           4     3    1  -17% BAUPOST                  
- `19:29:16`       24 AMGN      55.4 HIGH_CONVICTION           4     3    1  -16% DURATION                 
- `19:29:16`       25 NKE       55.3 HIGH_CONVICTION           4     2    1  -46%                          

# 6) Detailed view of top 3

- `19:29:16`   
- `19:29:16`     ── MOH (Molina Healthcare) ──
- `19:29:16`       score: 86.0  flag: STRONG_CONVICTION
- `19:29:16`       signal types: ['NEW_INITIATION_CLUSTER', 'DEEP_VALUE_CONSENSUS', 'LEGEND_FUND_BUY']
- `19:29:16`       rationale: SCION, LONE_PINE initiated new positions in MOH — stock 42% off 52w high — contrarian timing
- `19:29:16`       6 buyers / 4 sellers / 3 new init
- `19:29:16`       legend buyers: ['SCION', 'LONE_PINE']  quant buyers: ['TWO_SIGMA', 'CITADEL', 'MILLENNIUM', 'POINT72']
- `19:29:16`       pct from 52w high: -42.1
- `19:29:16`       fundamentals: {'price': 192.91, 'market_cap': 10050611000, 'year_high': 333, 'year_low': 121.06, 'pe_ratio': None, 'volume': 448740, 'exchange': 'NYSE', 'industry': None}
- `19:29:16`       fund actions (10):
- `19:29:16`         BRIDGEWATER    TRIM   $   4.7M   0.02% port  Δ -46.29
- `19:29:16`         RENAISSANCE    TRIM   $  11.2M   0.02% port  Δ -81.7
- `19:29:16`         TWO_SIGMA      ADD    $ 143.2M   0.19% port  Δ 770.56
- `19:29:16`         AQR            TRIM   $  71.2M   0.04% port  Δ -87.32
- `19:29:16`         CITADEL        ADD    $  93.8M   0.01% port  Δ 106.2
- `19:29:16`         MILLENNIUM     ADD    $  37.0M   0.01% port  Δ 54.9
- `19:29:16`         SCION          NEW    $  23.9M   1.73% port  Δ None
- `19:29:16`         LONE_PINE      NEW    $ 108.5M   2.05% port  Δ None
- `19:29:16`   
- `19:29:16`     ── LLY (Eli Lilly) ──
- `19:29:16`       score: 82.8  flag: STRONG_CONVICTION
- `19:29:16`       signal types: ['NEW_INITIATION_CLUSTER', 'CONSENSUS_BUY', 'LEGEND_FUND_BUY']
- `19:29:16`       rationale: SOROS, DURATION initiated new positions in LLY — stock 13% off 52w high
- `19:29:16`       8 buyers / 1 sellers / 2 new init
- `19:29:16`       legend buyers: ['SOROS', 'DURATION']  quant buyers: ['RENAISSANCE', 'AQR', 'CITADEL', 'MILLENNIUM', 'POINT72']
- `19:29:16`       pct from 52w high: -13.0
- `19:29:16`       fundamentals: {'price': 986.665, 'market_cap': 932219838635, 'year_high': 1133.95, 'year_low': 623.78, 'pe_ratio': None, 'volume': 195266, 'exchange': 'NYSE', 'industry': None}
- `19:29:16`       fund actions (9):
- `19:29:16`         BRIDGEWATER    ADD    $  78.8M   0.29% port  Δ 888.71
- `19:29:16`         RENAISSANCE    NEW    $ 178.2M   0.25% port  Δ None
- `19:29:16`         TWO_SIGMA      TRIM   $  17.3M   0.02% port  Δ -88.85
- `19:29:16`         AQR            ADD    $ 544.7M   0.29% port  Δ 64.65
- `19:29:16`         CITADEL        ADD    $5400.2M   0.80% port  Δ 23.96
- `19:29:16`         SOROS          NEW    $  24.0M   0.28% port  Δ None
- `19:29:16`         MILLENNIUM     ADD    $1043.1M   0.42% port  Δ 53.98
- `19:29:16`         DURATION       ADD    $2369.1M   1.41% port  Δ 33.71
- `19:29:16`   
- `19:29:16`     ── AMZN (Amazon.com Inc) ──
- `19:29:16`       score: 76.4  flag: STRONG_CONVICTION
- `19:29:16`       signal types: ['NEW_INITIATION_CLUSTER', 'LEGEND_FUND_BUY']
- `19:29:16`       rationale: PERSHING, SOROS, COATUE initiated new positions in AMZN
- `19:29:16`       10 buyers / 3 sellers / 2 new init
- `19:29:16`       legend buyers: ['PERSHING', 'SOROS', 'COATUE', 'LONE_PINE']  quant buyers: ['RENAISSANCE', 'TWO_SIGMA', 'AQR', 'CITADEL', 'POINT72']
- `19:29:16`       pct from 52w high: -1.7
- `19:29:16`       fundamentals: {'price': 273.7601, 'market_cap': 2944085047919.0005, 'year_high': 278.56, 'year_low': 183.85, 'pe_ratio': None, 'volume': 32951269, 'exchange': 'NASDAQ', 'industry': None}
- `19:29:16`       fund actions (15):
- `19:29:16`         BRIDGEWATER    ADD    $ 449.7M   1.64% port  Δ 82.05
- `19:29:16`         BERKSHIRE      TRIM   $ 525.3M   0.19% port  Δ -76.07
- `19:29:16`         RENAISSANCE    NEW    $ 205.6M   0.28% port  Δ None
- `19:29:16`         TWO_SIGMA      ADD    $ 314.5M   0.41% port  Δ 17.1
- `19:29:16`         AQR            ADD    $2059.6M   1.08% port  Δ 34.63
- `19:29:16`         CITADEL        ADD    $13202.9M   1.97% port  Δ 31.01
- `19:29:16`         PERSHING       ADD    $2217.7M  14.28% port  Δ 73.44
- `19:29:16`         SOROS          ADD    $ 613.9M   7.11% port  Δ 17.66