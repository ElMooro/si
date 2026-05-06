- `08:17:16`     Waiting for any in-progress updates...
- `08:17:16`     Lambda ready

# 1) Force-deploy theme-rotation v2 (with curated holdings fallback)

- `08:17:16`     source: 28972 chars
- `08:17:16`       ✓ fetch_etf_holdings(ticker, fallback_top=fallback)
- `08:17:16`       ✓ curated_lookup
- `08:17:16`       ✓ Try newer stable endpoint first
- `08:17:18`     ✓ deployed at 2026-05-06T08:17:17.000+0000

# 2) Force-invoke

- `08:17:24`     status: 200, dur: 6.6s
- `08:17:24`     body: {"statusCode": 200, "body": "{\"n_themes\": 118, \"n_alerts\": 10, \"duration_s\": 5.7}"}
- `08:17:24`       [theme-rot] wrote 91996b to data/theme-rotation.json
- `08:17:24`       [theme-rot] alerts: 10
- `08:17:24`       TOP_MOMENTUM_THEME: Defiance Hydrogen ETF has top momentum: score 100, RS +62.4% vs SPY 60d
- `08:17:24`       TOP_MOMENTUM_THEME: SPDR Semiconductor has top momentum: score 100, RS +39.7% vs SPY 60d
- `08:17:24`       TOP_MOMENTUM_THEME: Invesco Dynamic Semis has top momentum: score 100, RS +37.4% vs SPY 60d
- `08:17:24`       TOP_MOMENTUM_THEME: Bitwise Crypto Industry has top momentum: score 100, RS +31.8% vs SPY 60d
- `08:17:24`       TOP_MOMENTUM_THEME: Roundhill Generative AI has top momentum: score 100, RS +30.7% vs SPY 60d
- `08:17:24`       CONVERGENT_BREADTH: Defiance Hydrogen ETF has CONVERGENT BREADTH: RS +63.3%, 85.7% of constituents beating SPY
- `08:17:24`       CONVERGENT_BREADTH: SPDR Semiconductor has CONVERGENT BREADTH: RS +42.3%, 100.0% of constituents beating SPY
- `08:17:24`       CONVERGENT_BREADTH: Invesco Dynamic Semis has CONVERGENT BREADTH: RS +30.5%, 100.0% of constituents beating SPY
- `08:17:24`       END RequestId: 7f46261f-b7ed-441c-966c-464a9ed300ab
- `08:17:24`       REPORT RequestId: 7f46261f-b7ed-441c-966c-464a9ed300ab	Duration: 5752.69 ms	Billed Duration: 6294 ms	Memory Size: 1024 MB	Max Memory Used: 107 MB	Init Duration: 540.71 ms

# 3) Read theme rotation + 7-feed compound

- `08:17:25`     theme-rotation: 114 themes, 19 with breadth
- `08:17:25`     compound: 15 multi-signal names
- `08:17:25`     compound symbols: AVGO, AMZN, FCX, OXY, HUM, AMAT, CSGP, EPAM, EXAS, APA, FIX, PLXS, GOOGL, NUE, COP

# 4) Top 10 themes — show their breadth and constituents

- `08:17:25`   
- `08:17:25`     ── HYDR (Defiance Hydrogen ETF) — momentum=100  RS_20d=+63.3%  breadth=85.7% ──
- `08:17:25`          BE     ret_20d=+117.2%
- `08:17:25`          PLUG   ret_20d=+31.7%
- `08:17:25`          BLDP   ret_20d=+80.4%
- `08:17:25`          FCEL   ret_20d=+116.1%
- `08:17:25`          CMI    ret_20d=+21.2%
- `08:17:25`          APD    ret_20d= +3.9%
- `08:17:25`   
- `08:17:25`     ── XSD (SPDR Semiconductor) — momentum=100  RS_20d=+42.3%  breadth=100.0% ──
- `08:17:25`          MRVL   ret_20d=+54.3%
- `08:17:25`          INTC   ret_20d=+104.4%
- `08:17:25`          CRDO   ret_20d=+81.3%
- `08:17:25`          ALAB   ret_20d=+81.3%
- `08:17:25`          AMD    ret_20d=+60.4%
- `08:17:25`          ON     ret_20d=+60.9%
- `08:17:25`   
- `08:17:25`     ── PSI (Invesco Dynamic Semis) — momentum=100  RS_20d=+30.5%  breadth=100.0% ──
- `08:17:25`          MXL    ret_20d=+343.7%
- `08:17:25`          AMD    ret_20d=+60.4%
- `08:17:25`          TXN    ret_20d=+40.7%
- `08:17:25`          MU     ret_20d=+69.6%
- `08:17:25`       🎯 AVGO   ret_20d=+28.0%  COMPOUND #3 (eps_velocity,momentum,smart_money) score=443
- `08:17:25`          KLAC   ret_20d=+11.9%
- `08:17:25`   
- `08:17:25`     ── BITQ (Bitwise Crypto Industry) — momentum=100  RS_20d=+23.8%  breadth=100.0% ──
- `08:17:25`          IREN   ret_20d=+53.2%
- `08:17:25`          COIN   ret_20d=+12.9%
- `08:17:25`          MSTR   ret_20d=+51.1%
- `08:17:25`          APLD   ret_20d=+58.4%
- `08:17:25`          CIFR   ret_20d=+57.7%
- `08:17:25`          CLSK   ret_20d=+48.0%
- `08:17:25`   
- `08:17:25`     ── CHAT (Roundhill Generative AI) — momentum=100  RS_20d=+17.7%  breadth=93.3% ──
- `08:17:25`       🎯 GOOGL  ret_20d=+27.2%  COMPOUND #2 (momentum,smart_money) score=181
- `08:17:25`          NVDA   ret_20d=+10.3%
- `08:17:25`          AMD    ret_20d=+60.4%
- `08:17:25`          MU     ret_20d=+69.6%
- `08:17:25`       🎯 AMZN   ret_20d=+28.0%  COMPOUND #3 (momentum,pre_pump,smart_money) score=397
- `08:17:25`       🎯 AVGO   ret_20d=+28.0%  COMPOUND #3 (eps_velocity,momentum,smart_money) score=443
- `08:17:25`   
- `08:17:25`     ── DRIV (Global X Autonomous & EV) — momentum=100  RS_20d=+13.7%  breadth=66.7% ──
- `08:17:25`          NVDA   ret_20d=+10.3%
- `08:17:25`       🎯 GOOGL  ret_20d=+27.2%  COMPOUND #2 (momentum,smart_money) score=181
- `08:17:25`          TSLA   ret_20d=+12.3%
- `08:17:25`          MSFT   ret_20d=+10.5%
- `08:17:25`          HON    ret_20d= -6.6%
- `08:17:25`          INTC   ret_20d=+104.4%
- `08:17:25`   
- `08:17:25`     ── LIT (Global X Lithium) — momentum=100  RS_20d=+10.2%  breadth=80.0% ──
- `08:17:25`          RIO    ret_20d= +6.2%
- `08:17:25`          ALB    ret_20d=+12.3%
- `08:17:25`          SQM    ret_20d=+18.0%
- `08:17:25`          TSLA   ret_20d=+12.3%
- `08:17:25`          ENS    ret_20d=+23.9%
- `08:17:25`   
- `08:17:25`     ── LIT (Lithium & Batteries) — momentum=100  RS_20d=+10.2%  breadth=80.0% ──
- `08:17:25`          RIO    ret_20d= +6.2%
- `08:17:25`          ALB    ret_20d=+12.3%
- `08:17:25`          SQM    ret_20d=+18.0%
- `08:17:25`          TSLA   ret_20d=+12.3%
- `08:17:25`          ENS    ret_20d=+23.9%
- `08:17:25`   
- `08:17:25`     ── ICLN (iShares Clean Energy) — momentum=100  RS_20d=+9.5%  breadth=80.0% ──
- `08:17:25`          BE     ret_20d=+117.2%
- `08:17:25`          FSLR   ret_20d=+14.1%
- `08:17:25`          NXT    ret_20d=+13.0%
- `08:17:25`          ENPH   ret_20d=+12.4%
- `08:17:25`          PLUG   ret_20d=+31.7%
- `08:17:25`          SEDG   ret_20d= +1.8%
- `08:17:25`   
- `08:17:25`     ── BLCN (Reality Shares Blockchain) — momentum=100  RS_20d=+8.1%  breadth=78.6% ──
- `08:17:25`          NVDA   ret_20d=+10.3%
- `08:17:25`          MSTR   ret_20d=+51.1%
- `08:17:25`          GLXY   ret_20d=+62.7%
- `08:17:25`          MU     ret_20d=+69.6%
- `08:17:25`          AMD    ret_20d=+60.4%
- `08:17:25`          JPM    ret_20d= +4.0%

# 5) THE INSTITUTIONAL CONVERGENCE — names inside rotating themes WITH compound signal

- `08:17:25`     This is the highest-conviction setup the system can produce:
- `08:17:25`     Theme is rotating IN (institutions are buying the basket)
- `08:17:25`     AND the specific name appears on 2+ of our hunter systems
- `08:17:25`   
- `08:17:25`     Found 4 institutional-convergence picks:
- `08:17:25`   
- `08:17:25`     AVGO   theme=PSI    momentum=100  RS=+30.5%  ret_in_theme=+28.0%
- `08:17:25`            compound: #3  score=443  (eps_velocity,momentum,smart_money)
- `08:17:25`     AMZN   theme=CHAT   momentum=100  RS=+17.7%  ret_in_theme=+28.0%
- `08:17:25`            compound: #3  score=397  (momentum,pre_pump,smart_money)
- `08:17:25`     AMAT   theme=PSI    momentum=100  RS=+30.5%  ret_in_theme=+15.9%
- `08:17:25`            compound: #2  score=228  (eps_velocity,nobrainers)
- `08:17:25`     GOOGL  theme=CHAT   momentum=100  RS=+17.7%  ret_in_theme=+27.2%
- `08:17:25`            compound: #2  score=181  (momentum,smart_money)

# 6) Save institutional-convergence to S3

- `08:17:25`     ✓ wrote 1373b to data/institutional-convergence.json