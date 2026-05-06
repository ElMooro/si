- `09:13:10`     source: 18294 chars (v2)

# 1) Force-deploy v2

- `09:13:12`     ✓ deployed

# 2) Smoke invoke

- `09:13:23`     status: 200, dur: 11.4s
- `09:13:23`     body: {"statusCode": 200, "body": "{\"n_articles\": 6000, \"n_themes\": 53, \"n_tier_a\": 0, \"n_tier_b\": 2, \"duration_s\": 10.2}"}
- `09:13:23`       START RequestId: 2aa759ed-2972-4001-af20-6159e2a87eb0 Version: $LATEST
- `09:13:23`       [narrative-v2] starting v2.0, 53 themes
- `09:13:23`       [narrative-v2] fetched 6000 articles across 30 pages
- `09:13:23`       [narrative-v2] wrote 75149b
- `09:13:23`       [narrative-v2] TOP: [('agentic_ai', 45, 'TIER_B_BUILDING'), ('crypto_general', 45, 'TIER_B_BUILDING'), ('blockchain', 43, 'WATCH'), ('autonomous', 37, 'WATCH'), ('ai_general', 36, 'WATCH')]
- `09:13:23`       END RequestId: 2aa759ed-2972-4001-af20-6159e2a87eb0
- `09:13:23`       REPORT RequestId: 2aa759ed-2972-4001-af20-6159e2a87eb0	Duration: 10447.00 ms	Billed Duration: 11079 ms	Memory Size: 512 MB	Max Memory Used: 130 MB	Init Duration: 631.75 ms

# 3) Inspect output

- `09:13:23`     generated_at: 2026-05-06T09:13:23+00:00
- `09:13:23`     stats: {"n_articles_total": 6000, "n_themes_total": 53, "n_tier_a": 0, "n_tier_b": 2, "pages_fetched": 30}
- `09:13:23`   
- `09:13:23`     ── TOP 15 NARRATIVE THEMES BY DENSITY ──
- `09:13:23`       Agentic AI                       score= 45.0 TIER_B_BUILDING     today=10   7d=29    30d=101    accel_t/7=3.16x  accel_7/30=1.23x
- `09:13:23`         flags: TODAY_3X_BASELINE,MED_VOLUME_30D
- `09:13:23`         top tickers: NVDA(29),GOOG(29),GOOGL(29),MSFT(21),INTC(17),AMZN(16)
- `09:13:23`         sample: eGain Launches AI Agent IVA to Deliver Accurate, Conversational Customer Service
- `09:13:23`       Cryptocurrency                   score= 45.0 TIER_B_BUILDING     today=8    7d=20    30d=106    accel_t/7=4.00x  accel_7/30=0.81x
- `09:13:23`         flags: TODAY_3X_BASELINE,MED_VOLUME_30D
- `09:13:23`         top tickers: ETHV(14),BLK(13),DIVB(13),COIN(12),SCBFY(10),XRPC(7)
- `09:13:23`         sample: 21shares Launches Strategy Yield ETN (STRC) on the London Stock Exchange, Strengthening UK Presence
- `09:13:23`       Blockchain / DeFi                score= 43.0 WATCH               today=17   7d=53    30d=221    accel_t/7=2.83x  accel_7/30=1.03x
- `09:13:23`         flags: TODAY_2X_BASELINE,HIGH_VOLUME_30D
- `09:13:23`         top tickers: ATRA(18),ETHV(10),GOOG(10),GOOGL(10),XRPC(8),COIN(7)
- `09:13:23`         sample: Is XRP Too Risky to Own -- or Too Cheap to Ignore?
- `09:13:23`       Autonomous Vehicles              score= 37.0 WATCH               today=6    7d=14    30d=93     accel_t/7=4.50x  accel_7/30=0.65x
- `09:13:23`         flags: TODAY_3X_BASELINE,MED_VOLUME_30D
- `09:13:23`         top tickers: TSLA(62),UBER(22),GOOG(18),GOOGL(18),RIVN(14),NVDA(13)
- `09:13:23`         sample: PONY AI Inc. to Report First Quarter 2026 Financial Results on May 26, 2026
- `09:13:23`       AI / Artificial Intelligence     score= 36.0 WATCH               today=15   7d=68    30d=326    accel_t/7=1.70x  accel_7/30=0.89x
- `09:13:23`         flags: HIGH_VOLUME_30D
- `09:13:23`         top tickers: NVDA(125),GOOG(83),GOOGL(83),AMZN(72),MSFT(61),META(60)
- `09:13:23`         sample: Nova Klúbburinn hf.: Kosmos – nýtt fyrirtæki kemur inn á fjarskiptamarkaðinn með lægstu verð í net- og farsímaþjónustu
- `09:13:23`       AI Data Center                   score= 36.0 WATCH               today=16   7d=77    30d=245    accel_t/7=1.57x  accel_7/30=1.35x
- `09:13:23`         flags: HIGH_VOLUME_30D
- `09:13:23`         top tickers: NVDA(113),GOOG(65),GOOGL(65),META(62),AMZN(61),MSFT(55)
- `09:13:23`         sample: Stock Market Today, May 5: Cipher Digital Surges on AI Data Center Shift Backed by Hyperscale Leases
- `09:13:23`       AI Infrastructure                score= 35.0 WATCH               today=23   7d=124   30d=429    accel_t/7=1.37x  accel_7/30=1.24x
- `09:13:23`         flags: HIGH_VOLUME_30D,HIGH_TODAY
- `09:13:23`         top tickers: NVDA(182),GOOG(143),GOOGL(143),AMZN(124),META(124),MSFT(101)
- `09:13:23`         sample: Why Marvell Stock Soared 67% in April
- `09:13:23`       Robotics / Humanoid              score= 35.0 WATCH               today=8    7d=24    30d=112    accel_t/7=3.00x  accel_7/30=0.92x
- `09:13:23`         flags: TODAY_2X_BASELINE,MED_VOLUME_30D
- `09:13:23`         top tickers: TSLA(35),NVDA(20),GOOG(17),GOOGL(17),AMZN(16),MSFT(12)
- `09:13:23`         sample: Tesla: 3 Reasons the Stock Could Hit $400 in May
- `09:13:23`       Large Language Models            score= 31.0 WATCH               today=3    7d=8     30d=37     accel_t/7=3.60x  accel_7/30=0.93x
- `09:13:23`         flags: TODAY_3X_BASELINE
- `09:13:23`         top tickers: GOOG(22),GOOGL(22),MSFT(10),AMZN(8),NVDA(6),META(5)
- `09:13:23`         sample: Alphabet Stock Gains On Report Of Massive Anthropic Cloud, Chip Deal
- `09:13:23`       AI Chip / Semiconductor          score= 28.0 WATCH               today=10   7d=54    30d=215    accel_t/7=1.36x  accel_7/30=1.08x
- `09:13:23`         flags: HIGH_VOLUME_30D
- `09:13:23`         top tickers: NVDA(150),AVGO(73),GOOG(72),GOOGL(72),AMZN(62),TSM(50)
- `09:13:23`         sample: Alphabet Just Signaled That the Next Phase of the AI Revolution Has Arrived -- and Google's Parent Is Coming for Nvidia's Crown
- `09:13:23`       Tariff / Trade War               score= 27.0 WATCH               today=6    7d=20    30d=100    accel_t/7=2.57x  accel_7/30=0.86x
- `09:13:23`         flags: TODAY_2X_BASELINE,MED_VOLUME_30D
- `09:13:23`         top tickers: PINS(23),LAKE(8),AMZN(7),MSFT(6),INTC(4),AAPL(4)
- `09:13:23`         sample: S&P 500 and Dow Jones Climb; Intel's 14% Surge Steals the Show
- `09:13:23`       Alzheimer's drugs                score= 25.0 WATCH               today=2    7d=3     30d=8      accel_t/7=4.00x  accel_7/30=1.43x
- `09:13:23`         flags: TODAY_3X_BASELINE
- `09:13:23`         top tickers: BIIB(2),ESAIY(2),LLY(2),DNLI(1),HALO(1),ABOS(1)
- `09:13:23`         sample: Privium Fund Opens $5.07 Million Denali Stake Ahead of FDA Drug Approval
- `09:13:23`       Middle East tensions             score= 24.0 QUIET               today=6    7d=26    30d=72     accel_t/7=1.80x  accel_7/30=1.55x
- `09:13:23`         top tickers: CVX(8),INTC(7),MHK(7),XOM(6),TSLA(6),AAPL(5)
- `09:13:23`         sample: Privium Fund Buys $15.97 Million Stake in Alaska Air Amid Fuel Cost Crisis
- `09:13:23`       GLP-1 / Obesity                  score= 21.0 QUIET               today=3    7d=10    30d=57     accel_t/7=2.57x  accel_7/30=0.75x
- `09:13:23`         flags: TODAY_2X_BASELINE
- `09:13:23`         top tickers: LLY(32),NVO(24),PFE(6),VKTX(5),PEP(4),CNTA(3)
- `09:13:23`         sample: Novo Nordisk's adjusted operating profit reached DKK 32,858 million in Q1 2026
- `09:13:23`       Water treatment infrastructure   score= 21.0 QUIET               today=2    7d=8     30d=19     accel_t/7=2.00x  accel_7/30=1.80x
- `09:13:23`         top tickers: CWT(3),ZWS(2),ARTNA(1),GEV(1),BE(1),KGS(1)
- `09:13:23`         sample: Artesian Announces 2% Increase in Quarterly Common Stock Dividend