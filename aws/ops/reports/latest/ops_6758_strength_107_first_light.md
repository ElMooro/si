# ops 6758 - strength 1.0.7 first light 2026-11-06T01:48+00:00

**Status:** success  
**Duration:** 25.9s  
**Finished:** 2026-11-06T01:48:42+00:00  

## Data

| as_of | bytes | dump_days | ranked | rows | unclassed | version |
|---|---|---|---|---|---|---|
| 2026-11-04 | 2638323 | 27 | 818 | 852 | 0 | 1.0.7 |

## Log
- `01:48:17` justhodl-strength: modified 2026-11-06T01:45:23, sha WacWUuAyQAmx, MAX_UNIVERSE 1200
- `01:48:42` invoke: status 200 error None 25s payload {"statusCode": 200, "key": "data/strength.json", "bytes": 2638323, "rows": 852, "ranked": 818, "as_of": "2026-11-04", "run_seconds": 23.6, "errors": 0}
- `01:48:42` data/strength.json: 2638323 bytes | version 1.0.7 | as_of 2026-11-04 | rows 852 ranked 818 | run 23.6s | errors 0 []
- `01:48:42` universe: {"n": 853, "n_rows": 852, "n_ranked": 818, "scopes": {"desk": 163, "etf-desk": 116, "sp500": 503, "13f": 300}, "max": 1200} | unbanked [] | skipped []
- `01:48:42` classes counted {"stock": 669, "equity_etf": 48, "country": 37, "bond": 14, "high_yield": 11, "cash": 2, "gold": 2, "silver": 2, "metals": 13, "commodity": 10, "currency": 9, "crypto": 8, "volatility": 2, "leveraged": 25}
- `01:48:42` classes ranked  {"stock": 666, "equity_etf": 48, "country": 37, "bond": 14, "high_yield": 11, "cash": 2, "gold": 2, "silver": 2, "metals": 13, "commodity": 10, "currency": 9, "crypto": 8, "volatility": 2, "leveraged": 0}
- `01:48:42` rows without a class: 0 []
- `01:48:42` leveraged kept out (25): FAS +10.30%, UDOW +9.22%, SOXS +8.76%, UPRO +4.88%, SPXL +4.85%, SVXY +4.76%, TNA +4.40%, SSO +3.25%, TQQQ +2.79%, QLD +1.88%, TECS +0.15%, TECL -0.16%, PSQ -0.89%, TBT -1.58%, SH -1.61%, QID -1.83%, SQQQ -2.78%, SDS -3.24%, TZA -4.42%, SPXS -4.89%, SPXU -4.89%, SOXL -8.81%, SDOW -9.23%, FAZ -10.05%, UVXY -14.12%
- `01:48:42` leaders: MRNA 99.9 stock, TDOC 99.8 stock, ADT 99.7 stock, ACN 99.6 stock, CROX 99.5 stock, HAE 99.3 stock, ASO 99.2 stock, FISV 99.1 stock, AMC 99.0 stock, GEN 98.8 stock, SWK 98.7 stock, IDXX 98.6 stock, TYL 98.5 stock, RKT 98.4 stock, SPOT 98.2 stock, ROP 98.1 stock, GEHC 98.0 stock, CVNA 97.9 stock, DLTR 97.8 stock, MDLN 97.6 stock, OSCR 97.5 stock, ZBH 97.4 stock, MORN 97.3 stock, HLF 97.1 stock, ETSY 97.0 stock
- `01:48:42` laggards: CIFR 0.1 stock, STRL 0.2 stock, HUT 0.3 stock, TTMI 0.4 stock, INTC 0.5 stock, SITM 0.7 stock, AGX 0.8 stock, FIX 0.9 stock, CIEN 1.0 stock, TSEM 1.2 stock, LITE 1.3 stock, CRDO 1.4 stock, RSI 1.5 stock, COHR 1.6 stock, FORM 1.8 stock, TER 1.9 stock, FLEX 2.0 stock, MRVL 2.1 stock, SMTC 2.2 stock, NBIS 2.4 stock, SNDK 2.5 stock, STX 2.6 stock, AMKR 2.7 stock, DDOG 2.9 stock, CORZ 3.0 stock
- `01:48:42`   Stocks               n  660 median rank  58.3 | vs SPY today  +0.83% 1W  +4.92% 1M  +8.84% 3M  +1.14% | beat SPY 1M 73.8% 3M 54.5% | best MRNA TDOC ADT ACN CROX | worst CIFR STRL HUT TTMI INTC
- `01:48:42`   Equity ETFs          n   48 median rank  34.9 | vs SPY today  -0.03% 1W  +2.26% 1M  +1.82% 3M  -0.02% | beat SPY 1M 60.4% 3M 45.8% | best ITB XLV XRT XLP IBB | worst URA SOXX SMH MTUM OIH
- `01:48:42`   Countries            n   37 median rank  37.3 | vs SPY today  +0.94% 1W  +1.59% 1M  +0.32% 3M  -4.99% | beat SPY 1M 51.4% 3M 18.9% | best EWL EWH INDA EWU EWG | worst EIDO EWZ EWY EWT ARGT
- `01:48:42`   Bonds                n   14 median rank  39.0 | vs SPY today  -1.19% 1W  +1.22% 1M  +3.69% 3M  -0.81% | beat SPY 1M 100.0% 3M 0.0% | best TLT MUB IEF MBB AGG | worst VCSH SHY TIP VCIT EMB
- `01:48:42`   High yield           n   11 median rank  33.3 | vs SPY today  -0.92% 1W  +0.63% 1M  +1.83% 3M  -2.44% | beat SPY 1M 100.0% 3M 9.1% | best HYD BKLN FALN SRLN HYLB | worst SJNK ANGL JNK USHY EMHY
- `01:48:42`   Cash / T-bills       n    2 median rank  34.7 | vs SPY today  -1.64% 1W  +0.46% 1M  +1.57% 3M  +0.22% | beat SPY 1M 100.0% 3M 100.0% | best SGOV BIL | worst BIL SGOV
- `01:48:42`   Gold                 n    2 median rank  30.2 | vs SPY today  +2.03% 1W  +0.45% 1M  -0.97% 3M  -4.15% | beat SPY 1M 0.0% 3M 0.0% | best GLD IAU | worst IAU GLD
- `01:48:42`   Silver               n    2 median rank  13.9 | vs SPY today  +3.59% 1W  -3.13% 1M -13.37% 3M -15.24% | beat SPY 1M 0.0% 3M 0.0% | best SIVR SLV | worst SLV SIVR
- `01:48:42`   Metals & miners      n   13 median rank  20.9 | vs SPY today  +2.57% 1W  -0.80% 1M  -8.25% 3M -10.20% | beat SPY 1M 7.7% 3M 7.7% | best GDX GDXJ DBB SIL CPER | worst REMX XME PALL LIT PPLT
- `01:48:42`   Commodities          n   10 median rank  24.0 | vs SPY today  -2.35% 1W  -1.72% 1M  -2.84% 3M  +8.36% | beat SPY 1M 10.0% 3M 60.0% | best USO BNO SOYB CORN DBC | worst WEAT UNG DBA PDBC GSG
- `01:48:42`   Currencies           n    9 median rank  36.7 | vs SPY today  -1.26% 1W  +1.16% 1M  +1.92% 3M  -0.12% | beat SPY 1M 100.0% 3M 44.4% | best FXB FXF FXC UUP UDN | worst FXE FXY FXA CEW UDN
- `01:48:42`   Crypto               n    8 median rank  48.8 | vs SPY today  +1.27% 1W  -1.05% 1M  -4.98% 3M +24.46% | beat SPY 1M 0.0% 3M 100.0% | best ETHA ETHE ARKB IBIT FBTC | worst BITO GBTC BITB FBTC IBIT
- `01:48:42`   Volatility           n    2 median rank  14.0 | vs SPY today -11.29% 1W  -3.17% 1M  +3.43% 3M -15.44% | beat SPY 1M 100.0% 3M 0.0% | best VXX VIXY | worst VIXY VXX
- `01:48:42`   HYG   high_yield/corporate strength 33.3 in-class 50.0 sleeve ETF excluded None
- `01:48:42`   JNK   high_yield/corporate strength 32.5 in-class 22.7 sleeve HIGH YIELD excluded None
- `01:48:42`   TLT   bond/treasury strength 44.4 in-class 96.4 sleeve ETF excluded None
- `01:48:42`   LQD   bond/ig_corporate strength 38.3 in-class 46.4 sleeve BOND excluded None
- `01:48:42`   GLD   gold/bullion strength 30.2 in-class None sleeve ETF excluded None
- `01:48:42`   SLV   silver/bullion strength 13.8 in-class None sleeve METAL excluded None
- `01:48:42`   SIVR  silver/bullion strength 13.9 in-class None sleeve METAL excluded None
- `01:48:42`   PPLT  metals/pgm strength 16.4 in-class 34.6 sleeve METAL excluded None
- `01:48:42`   CPER  metals/copper strength 22.6 in-class 65.4 sleeve METAL excluded None
- `01:48:42`   GDX   metals/gold_miners strength 43.1 in-class 96.2 sleeve METAL excluded None
- `01:48:42`   USO   commodity/energy strength 27.2 in-class 95.0 sleeve COMMODITY excluded None
- `01:48:42`   UUP   currency/None strength 37.4 in-class 61.1 sleeve CURRENCY excluded None
- `01:48:42`   IBIT  crypto/None strength 48.8 in-class 56.2 sleeve CRYPTO excluded None
- `01:48:42`   VXX   volatility/None strength 14.0 in-class None sleeve VOLATILITY excluded None
- `01:48:42`   EWJ   country/None strength 27.6 in-class 44.6 sleeve ETF excluded None
- `01:48:42`   XLE   equity_etf/sector strength 27.9 in-class 40.6 sleeve ETF excluded None
- `01:48:42`   QQQ   equity_etf/broad strength 20.0 in-class 19.8 sleeve ETF excluded None
- `01:48:42`   AAPL  stock/None strength 65.2 in-class 58.6 sleeve STOCK excluded None
- `01:48:42`   TQQQ  leveraged/None strength None in-class None sleeve ETF excluded leveraged
- `01:48:42`   SQQQ  leveraged/None strength None in-class None sleeve ETF excluded leveraged
- `01:48:42`   SOXS  leveraged/None strength None in-class None sleeve ETF excluded leveraged
- `01:48:42`   BIL   cash/t_bill strength 34.5 in-class None sleeve BOND excluded None
- `01:48:42` thesis: window {"sessions": 252, "from": "2025-11-04", "to": "2026-11-04", "n_rows": 824} | 3.5s | error None
- `01:48:42` VERDICT: On the 27 sessions of the last 252 when SPY fell 1.0% or more, the names ranked in the top strength decile the session before beat SPY on 12 (44.4%): their average move was -1.67% against SPY's, an excess of -0.23 pp; 51.3% of them held up (flat, up, or down less than SPY) against 44.8% of the bottom decile. Stocks alone: top decile excess -0.44 pp, held up 49.7%, beat SPY on 37.0% of those days. Last session 2026-11-04 (SPY +1.65%): top decile +2.82% (excess +1.17 pp), held up 84.0%, beat SPY 69.1%.
- `01:48:42` dump days 27 deep 2 | all: {"n_days": 27, "top_decile": {"mean_excess_pp": -0.23, "mean_ret_pct": -1.67, "hit_pct": 51.3, "held_up_pct": 51.3, "days_beat_spy": 12, "days_beat_spy_pct": 44.4}, "bottom_decile": {"mean_excess_pp": -0.58, "mean_ret_pct": -2.02, "hit_pct": 44.8, "held_up_pct": 44.8}, "by_decile_mean_excess_pp": [-0.58, 0.17, 0.5, 0.58, 0.59, 0.66, 0.6, 0.45, 0.28, -0.23], "top_minus_bottom_pp": 0.35, "monotonic": false}
- `01:48:42` stocks: {"n_days": 27, "top_decile": {"mean_excess_pp": -0.44, "mean_ret_pct": -1.88, "hit_pct": 49.7, "held_up_pct": 49.7, "days_beat_spy": 10, "days_beat_spy_pct": 37.0}, "bottom_decile": {"mean_excess_pp": -0.61, "mean_ret_pct": -2.05, "hit_pct": 45.6, "held_up_pct": 45.6}, "by_decile_mean_excess_pp": [-0.61, 0.22, 0.51, 0.57, 0.66, 0.74, 0.64, 0.48, 0.2, -0.44], "top_minus_bottom_pp": 0.17, "monotonic": false}
- `01:48:42` by class on dump days: {"stock": {"n_days": 27, "mean_ret_pct": -1.14, "mean_excess_pp": 0.3, "held_up_pct": 60.0, "hit_pct": 60.0, "avg_n": 661.0}, "equity_etf": {"n_days": 27, "mean_ret_pct": -1.45, "mean_excess_pp": -0.01, "held_up_pct": 52.3, "hit_pct": 52.3, "avg_n": 48.0}, "country": {"n_days": 27, "mean_ret_pct": -1.35, "mean_excess_pp": 0.09, "held_up_pct": 59.7, "hit_pct": 59.7, "avg_n": 37.0}, "bond": {"n_days": 27, "mean_ret_pct": -0.11, "mean_excess_pp": 1.33, "held_up_pct": 99.2, "hit_pct": 99.2, "avg_n": 14.0}, "high_yield": {"n_days": 27, "mean_ret_pct": -0.27, "mean_excess_pp": 1.17, "held_up_pct": 99.7, "hit_pct": 99.7, "avg_n": 11.0}, "cash": {"n_days": 27, "mean_ret_pct": 0.02, "mean_excess_pp": 1.46, "held_up_pct": 100.0, "hit_pct": 100.0, "avg_n": 2.0}, "gold": {"n_days": 27, "mean_ret_pct": -1.16, "mean_excess_pp": 0.28, "held_up_pct": 40.7, "hit_pct": 40.7, "avg_n": 2.0}, "silver": {"n_days": 27, "mean_ret_pct": -3.08, "mean_excess_pp": -1.65, "held_up_pct": 33.3, "hit_pct": 33.3, "avg_n": 2.0}, "metals": {"n_days": 27, "mean_ret_pct": -2.41, "mean_excess_pp": -0.97, "held_up_pct": 35.9, "hit_pct": 35.9, "avg_n": 13.0}, "commodity": {"n_days": 27, "mean_ret_pct": 0.56, "mean_excess
- `01:48:42` all days: {"top_excess_pp": 0.1, "top_beat_spy_pct": 56.7, "bottom_excess_pp": 0.06, "by_decile_mean_excess_pp": [0.06, 0.03, 0.05, 0.02, -0.02, -0.01, -0.01, 0.0, 0.03, 0.1]}
- `01:48:42` last session 2026-11-04 (ranked as of 2026-11-03) SPY +1.65% dump False | top {"n": 81, "mean_ret_pct": 2.82, "median_ret_pct": 3.3, "mean_excess_pp": 1.17, "hit_pct": 69.1, "up_pct": 80.2, "flat_pct": 3.7, "down_less_pct": 0.0, "down_more_pct": 16.0, "held_up_pct": 84.0} | bottom {"n": 82, "mean_ret_pct": -1.11, "median_ret_pct": -0.08, "mean_excess_pp": -2.77, "hit_pct": 41.5, "up_pct": 48.8, "flat_pct": 2.4, "down_less_pct": 0.0, "down_more_pct": 48.8, "held_up_pct": 51.2}
- `01:48:42`   2026-10-30 SPY -1.77% | top excess 0.62 pp held 60.5% beat 60.5% | bottom excess -1.58 held 40.2% | stocks top excess 0.49
- `01:48:42`   2026-10-22 SPY -1.18% | top excess 1.93 pp held 75.3% beat 75.3% | bottom excess -2.73 held 36.6% | stocks top excess 1.67
- `01:48:42`   2026-10-16 SPY -1.11% | top excess -0.55 pp held 46.9% beat 46.9% | bottom excess -1.99 held 31.7% | stocks top excess -0.69
- `01:48:42`   2026-10-14 SPY -1.08% | top excess -0.34 pp held 55.6% beat 55.6% | bottom excess -1.41 held 41.5% | stocks top excess -0.57
- `01:48:42`   2026-07-29 SPY -1.54% | top excess 2.08 pp held 73.2% beat 73.2% | bottom excess -2.79 held 19.5% | stocks top excess 2.2
- `01:48:42`   2026-07-23 SPY -1.23% | top excess 1.16 pp held 72.0% beat 72.0% | bottom excess 0.24 held 59.0% | stocks top excess 1.04
- `01:48:42`   2026-06-23 SPY -1.45% | top excess -3.81 pp held 20.7% beat 20.7% | bottom excess 2.29 held 86.7% | stocks top excess -4.11
- `01:48:42`   2026-06-17 SPY -1.25% | top excess 1.48 pp held 65.9% beat 65.9% | bottom excess -0.71 held 38.6% | stocks top excess 1.58
- `01:48:42`   2026-06-10 SPY -1.58% | top excess -0.38 pp held 42.7% beat 42.7% | bottom excess -0.5 held 38.6% | stocks top excess -0.54
- `01:48:42`   2026-06-05 SPY -2.58% | top excess -4.17 pp held 22.0% beat 22.0% | bottom excess 1.74 held 73.5% | stocks top excess -4.54
- `01:48:42`   2026-05-15 SPY -1.20% | top excess -1.11 pp held 32.9% beat 32.9% | bottom excess 0.49 held 56.1% | stocks top excess -1.13
- `01:48:42`   2026-03-27 SPY -1.71% | top excess 2.22 pp held 80.2% beat 80.2% | bottom excess 0.7 held 48.8% | stocks top excess 1.92
- `01:48:42`   2026-03-26 SPY -1.79% | top excess -0.49 pp held 55.6% beat 55.6% | bottom excess 0.91 held 67.1% | stocks top excess -0.78
- `01:48:42`   2026-03-20 SPY -1.70% | top excess -0.89 pp held 46.9% beat 46.9% | bottom excess -0.32 held 50.0% | stocks top excess -1.33
- `01:48:42`   2026-03-18 SPY -1.40% | top excess 1.67 pp held 75.3% beat 75.3% | bottom excess -0.5 held 34.1% | stocks top excess 1.52
- `01:48:42`   2026-03-12 SPY -1.52% | top excess 1.41 pp held 64.2% beat 64.2% | bottom excess -1.8 held 20.7% | stocks top excess 0.9
- `01:48:42`   2026-03-06 SPY -1.31% | top excess 0.5 pp held 55.6% beat 55.6% | bottom excess -1.53 held 14.6% | stocks top excess -0.56
- `01:48:42`   2026-02-23 SPY -1.02% | top excess 0.67 pp held 61.7% beat 61.7% | bottom excess -2.52 held 22.0% | stocks top excess 0.52
- `01:48:42`   2026-02-12 SPY -1.54% | top excess -1.25 pp held 37.0% beat 37.0% | bottom excess -0.94 held 37.8% | stocks top excess -1.04
- `01:48:42`   2026-02-05 SPY -1.25% | top excess -0.6 pp held 58.0% beat 58.0% | bottom excess -3.33 held 24.4% | stocks top excess -0.17
- `01:48:42`   2026-01-20 SPY -2.04% | top excess 2.36 pp held 74.1% beat 74.1% | bottom excess 0.26 held 46.3% | stocks top excess 2.04
- `01:48:42`   2025-12-17 SPY -1.10% | top excess 0.14 pp held 55.6% beat 55.6% | bottom excess 0.68 held 61.0% | stocks top excess -0.22
- `01:48:42`   2025-12-12 SPY -1.08% | top excess -2.29 pp held 25.9% beat 25.9% | bottom excess 0.37 held 65.9% | stocks top excess -2.5
- `01:48:42`   2025-11-20 SPY -1.52% | top excess -2.07 pp held 38.3% beat 38.3% | bottom excess -0.98 held 37.8% | stocks top excess -2.29
- `01:48:42`   2025-11-13 SPY -1.66% | top excess -1.57 pp held 28.4% beat 28.4% | bottom excess -0.15 held 48.8% | stocks top excess -1.88
- `01:48:42`   2025-11-06 SPY -1.07% | top excess -0.68 pp held 42.0% beat 42.0% | bottom excess -0.32 held 45.1% | stocks top excess -0.92
- `01:48:42`   2025-11-04 SPY -1.19% | top excess -2.35 pp held 18.5% beat 18.5% | bottom excess 0.77 held 63.4% | stocks top excess -2.42
- `01:48:42` history data/strength/history/2026-11-04.json.gz: 412993 bytes, modified 2026-11-06T01:48:43+00:00
- `01:48:42` ✅ ops 6758 complete: strength 1.0.7 live as of 2026-11-04
