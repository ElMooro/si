- `08:06:43`     source: 28147 chars

# 1) Build zip + deploy

- `08:06:43`     zip: 28635b
- `08:06:43`     creating
- `08:06:46`     ✓ deployed at 2026-05-06T08:06:43.347+0000

# 2) Schedule daily 13:45 UTC

- `08:06:47`     ✓ permission added

# 3) Smoke invoke (~120-180s for 105 ETFs + breadth)

- `08:06:50`     status: 200, dur: 3.0s
- `08:06:50`     body: {"statusCode": 200, "body": "{\"n_themes\": 118, \"n_alerts\": 5, \"duration_s\": 2.0}"}
- `08:06:50`       START RequestId: 48307b1e-494e-4530-869e-c150a6b2a4b1 Version: $LATEST
- `08:06:50`       [theme-rot] starting v1.0
- `08:06:50`       [theme-rot] fetching SPY benchmark...
- `08:06:50`       [theme-rot] SPY 20d return: 9.79%
- `08:06:50`       [theme-rot] computed metrics for 118 themes
- `08:06:50`       [theme-rot] computing breadth for top 20 themes...
- `08:06:50`       [theme-rot] wrote 62590b to data/theme-rotation.json
- `08:06:50`       [theme-rot] alerts: 5
- `08:06:50`       TOP_MOMENTUM_THEME: Defiance Hydrogen ETF has top momentum: score 100, RS +62.4% vs SPY 60d
- `08:06:50`       TOP_MOMENTUM_THEME: SPDR Semiconductor has top momentum: score 100, RS +39.7% vs SPY 60d
- `08:06:50`       TOP_MOMENTUM_THEME: Invesco Dynamic Semis has top momentum: score 100, RS +37.4% vs SPY 60d
- `08:06:50`       TOP_MOMENTUM_THEME: Bitwise Crypto Industry has top momentum: score 100, RS +31.8% vs SPY 60d
- `08:06:50`       TOP_MOMENTUM_THEME: Roundhill Generative AI has top momentum: score 100, RS +30.7% vs SPY 60d
- `08:06:50`       END RequestId: 48307b1e-494e-4530-869e-c150a6b2a4b1
- `08:06:50`       REPORT RequestId: 48307b1e-494e-4530-869e-c150a6b2a4b1	Duration: 2125.16 ms	Billed Duration: 2680 ms	Memory Size: 1024 MB	Max Memory Used: 106 MB	Init Duration: 554.73 ms

# 4) Inspect output — top themes by momentum

- `08:06:50`     generated_at: 2026-05-06T08:06:50+00:00
- `08:06:50`     spy_ret_20d: 9.79%
- `08:06:50`     stats: {"n_themes_evaluated": 118, "n_with_breadth": 0, "n_alerts": 5, "n_rotation_deltas": 0}
- `08:06:50`   
- `08:06:50`     ── TOP 10 THEMES BY MOMENTUM ──
- `08:06:50`       HYDR   100.0  HYDROGEN                RS_5d=+23.1% RS_20d=+63.3% RS_60d=+62.4%  breadth=  N/A
- `08:06:50`       XSD    100.0  AI_SEMI_SMID            RS_5d=+15.1% RS_20d=+42.3% RS_60d=+39.7%  breadth=  N/A
- `08:06:50`       PSI    100.0  AI_SEMI_SMID            RS_5d= +9.7% RS_20d=+30.5% RS_60d=+37.4%  breadth=  N/A
- `08:06:50`       BITQ   100.0  CRYPTO_EQ               RS_5d=+10.6% RS_20d=+23.8% RS_60d=+31.8%  breadth=  N/A
- `08:06:50`       CHAT   100.0  AI_BROAD                RS_5d= +7.2% RS_20d=+17.7% RS_60d=+30.7%  breadth=  N/A
- `08:06:50`       DRIV   100.0  EV_AUTO                 RS_5d= +5.8% RS_20d=+13.7% RS_60d=+12.6%  breadth=  N/A
- `08:06:50`       LIT    100.0  LITHIUM                 RS_5d= +3.9% RS_20d=+10.2% RS_60d=+19.6%  breadth=  N/A
- `08:06:50`       LIT    100.0  BATTERIES               RS_5d= +3.9% RS_20d=+10.2% RS_60d=+19.6%  breadth=  N/A
- `08:06:50`       ICLN   100.0  CLEAN_ENERGY            RS_5d= +5.6% RS_20d= +9.5% RS_60d= +9.6%  breadth=  N/A
- `08:06:50`       BLCN   100.0  BLOCKCHAIN              RS_5d= +7.5% RS_20d= +8.1% RS_60d= +4.0%  breadth=  N/A
- `08:06:50`   
- `08:06:50`     ── BOTTOM 10 THEMES (rotation OUT) ──
- `08:06:50`       XLV      0.0  HEALTHCARE              RS_20d=-10.7% RS_60d=-12.7%
- `08:06:50`       TLT      0.0  RATES_LONG              RS_20d=-11.2% RS_60d= -7.2%
- `08:06:50`       XAR      0.0  DEFENSE                 RS_20d=-11.7% RS_60d=-11.1%
- `08:06:50`       AGNG     0.0  DEMOGRAPHICS            RS_20d=-12.1% RS_60d=-11.9%
- `08:06:50`       ITA      0.0  DEFENSE                 RS_20d=-13.3% RS_60d=-12.7%
- `08:06:50`       IHI      0.0  MED_DEVICES             RS_20d=-16.2% RS_60d=-19.5%
- `08:06:50`       SILJ     0.0  SILVER                  RS_20d=-16.6% RS_60d=-17.8%
- `08:06:50`       GDXJ     0.0  GOLD_JR                 RS_20d=-17.7% RS_60d=-16.8%
- `08:06:50`       GDX      0.0  GOLD                    RS_20d=-19.4% RS_60d=-16.7%
- `08:06:50`       VIXY     0.0  VOLATILITY              RS_20d=-28.0% RS_60d= -1.1%

# 5) Category-level aggregation

- `08:06:50`       HYDROGEN               n= 1  avg_RS_20d=+63.3%  avg_momentum=100.0  top=HYDR
- `08:06:50`       AI_SEMI_SMID           n= 2  avg_RS_20d=+36.4%  avg_momentum=100.0  top=XSD
- `08:06:50`       CRYPTO_EQ              n= 1  avg_RS_20d=+23.8%  avg_momentum=100.0  top=BITQ
- `08:06:50`       EV_AUTO                n= 2  avg_RS_20d= +9.2%  avg_momentum=100.0  top=DRIV
- `08:06:50`       LITHIUM                n= 1  avg_RS_20d=+10.2%  avg_momentum=100.0  top=LIT
- `08:06:50`       BATTERIES              n= 1  avg_RS_20d=+10.2%  avg_momentum=100.0  top=LIT
- `08:06:50`       CLEAN_ENERGY           n= 2  avg_RS_20d=+13.4%  avg_momentum= 96.5  top=ICLN
- `08:06:50`       BLOCKCHAIN             n= 2  avg_RS_20d=+10.6%  avg_momentum= 96.5  top=BLCN
- `08:06:50`       AI_SEMI                n= 3  avg_RS_20d=+65.0%  avg_momentum= 93.0  top=SOXL
- `08:06:50`       TECH_BROAD             n= 2  avg_RS_20d= +9.9%  avg_momentum= 93.0  top=XLK
- `08:06:50`       MOMENTUM_FACTOR        n= 1  avg_RS_20d= +7.0%  avg_momentum= 93.0  top=MTUM
- `08:06:50`       RARE_EARTH             n= 1  avg_RS_20d= +6.8%  avg_momentum= 93.0  top=REMX
- `08:06:50`       NEXT_GEN               n= 1  avg_RS_20d= +6.0%  avg_momentum= 93.0  top=ARKW
- `08:06:50`       EM_BROAD               n= 1  avg_RS_20d= +4.3%  avg_momentum= 93.0  top=EEM
- `08:06:50`       SOLAR                  n= 1  avg_RS_20d= +3.8%  avg_momentum= 93.0  top=TAN

# 6) Convergent breadth themes (institutional buy signal)


# 7) Alerts to fire

- `08:06:50`       [TOP_MOMENTUM_THEME] Defiance Hydrogen ETF has top momentum: score 100, RS +62.4% vs SPY 60d
- `08:06:50`       [TOP_MOMENTUM_THEME] SPDR Semiconductor has top momentum: score 100, RS +39.7% vs SPY 60d
- `08:06:50`       [TOP_MOMENTUM_THEME] Invesco Dynamic Semis has top momentum: score 100, RS +37.4% vs SPY 60d
- `08:06:50`       [TOP_MOMENTUM_THEME] Bitwise Crypto Industry has top momentum: score 100, RS +31.8% vs SPY 60d
- `08:06:50`       [TOP_MOMENTUM_THEME] Roundhill Generative AI has top momentum: score 100, RS +30.7% vs SPY 60d