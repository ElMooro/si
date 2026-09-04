# ops 5195 -- bond war room v1.2.2 page: regime banner stacks on mobile

**Status:** success  
**Duration:** 225.6s  
**Finished:** 2026-09-04T14:04:50+00:00  

## Log
- `14:01:05`   Lambda exists — updating
- `14:01:08` ✅   ✓ updated justhodl-bond-warroom
- `14:01:24`    run (11s, error=None) -> {"ok": true, "elapsed_s": 10.1, "n_series": 91, "heartbeat": 5, "regime": "CALM", "equity": "CALM", "eurodollar": "NONE", "red": ["GB10Y", "JP30Y", "AU03Y"], "notes": [], "freshness": {"mof_jgb": "2026-09-03", "tradingview": "2026-09-04", "tradingview_symbols": 28, "official": {"Bank of Canada Valet": "2026-09-03", "Bundesbank": "2026-09-04", "Bank of England": "2026-09-02", "RBA F2": "2026-09-02", "treasury.gov par curve": "2026-09-03"}, "official_n": 20, "ecb": "2026-09-03", "fred": "2026-09-03", "yahoo": "2026-09-04"}}
## feed

- `14:01:24`    heartbeat 5 CALM -- Bond markets are calm: UK 10Y (Gilt) -10.8bp (z -2.2); Australia 3Y +10.1bp (z +2.0); Japan 30Y (JGB) -7.0bp (z -1.9); Australia 2s10s -4.6bp (z -2.1); Australia 2Y +9.7bp (z +2.0).
- `14:01:24`    equity: CALM LOW -- Bond prices and yields are inside their normal daily range (TLT +0.3% (z +0.6); 10Y +0.6bp (z +0.1); MOVE 75 -5.0pts (z -1.2); HY OAS +1.0bp (z +0.2)). No rates-driven pressure on stocks either way.
- `14:01:24`    eurodollar: NONE 0 -- No eurodollar-shortage signature: periphery spreads, the dollar and EM/Euro credit are quiet.
- `14:01:24`    freshness={'mof_jgb': '2026-09-03', 'tradingview': '2026-09-04', 'tradingview_symbols': 28, 'official': {'Bank of Canada Valet': '2026-09-03', 'Bundesbank': '2026-09-04', 'Bank of England': '2026-09-02', 'RBA F2': '2026-09-02', 'treasury.gov par curve': '2026-09-03'}, 'official_n': 20, 'ecb': '2026-09-03', 'fred': '2026-09-03', 'yahoo': '2026-09-04'} notes=[]
- `14:01:24`    panel us_rates        12 rows: US02Y 4.379 GREEN, US05Y 4.543 GREEN, US07Y 4.63 GREEN, US10Y 4.776 GREEN, US20Y 5.25 GREEN, US30Y 5.236 GREEN, DGS3MO 3.92 GREEN, DFII10 2.45 GREEN, T10YIE 2.35 GREEN
- `14:01:24`    panel volatility       9 rows: ^MOVE 74.68 GREEN, MOVE_TV 74.6812 GREEN, VIXCLS 15.2 GREEN, TLT 82.34 GREEN, IEF 92.285 GREEN, SHY 81.68 GREEN, HYG 79.175 GREEN, LQD 105.56 GREEN, EMB 94.455 GREEN
- `14:01:24`    panel japan            5 rows: JP02Y 1.85 GREEN, JP10Y 2.966 GREEN, JP30Y 4.052 RED, JP2s30s 2.202 AMBER, US-JGB 1.804 GREEN
- `14:01:24`    panel europe          17 rows: DE02Y 2.94 GREEN, DE10Y 3.37 GREEN, DE30Y 3.8 GREEN, FR10Y 4.1823 GREEN, IT02Y 3.1277 GREEN, IT10Y 4.1427 GREEN, ES10Y 3.7758 GREEN, NL10Y 3.411 GREEN, PT10Y 3.657 GREEN
- `14:01:24`    panel europe_spreads  12 rows: BTP-Bund 0.7727 GREEN, OAT-Bund 0.8123 GREEN, Bono-Bund 0.4058 GREEN, IT-ES 0.3669 GREEN, PT-Bund 0.287 GREEN, GR-Bund 0.6304 GREEN, Gilt-Bund 1.764 AMBER, US-Bund 1.406 GREEN, EA-periphery 0.4241 GREEN
- `14:01:24`    panel world           15 rows: AU02Y 4.831 AMBER, AU03Y 4.819 RED, AU05Y 4.871 AMBER, AU10Y 5.172 GREEN, CA02Y 3.1 GREEN, CA05Y 3.41 GREEN, CA10Y 3.761 GREEN, CA30Y 4.16 GREEN, CN10Y 1.684 GREEN
- `14:01:24`    panel credit          12 rows: BAMLH0A0HYM2 2.66 GREEN, BAMLC0A0CM 0.81 GREEN, BAMLC0A1CAAA 0.43 GREEN, BAMLC0A4CBBB 0.99 GREEN, BAMLH0A1HYBB 1.53 GREEN, BAMLH0A2HYB 2.76 GREEN, BAMLH0A3HYC 10.53 GREEN, BAMLHE00EHYIOAS 2.62 GREEN, BAMLEMCBPIOAS 1.38 GREEN
- `14:01:24`    panel funding          4 rows: SOFR 3.66 GREEN, DTB3 3.78 GREEN, SOFR-TB3 -0.13 GREEN, DTWEXBGS 118.747 GREEN
## official histories (v1.2)

- `14:01:24`    official sources: {"Bank of Canada Valet": "2026-09-03", "Bundesbank": "2026-09-04", "Bank of England": "2026-09-02", "RBA F2": "2026-09-02", "treasury.gov par curve": "2026-09-03"} (n=20)
- `14:01:24`    US02Y     4.379 hist=1501 z=0.81 z_ready=True dod=3.9 dod%=0.9 flag=GREEN src=treasury.gov par curve + TradingView liv
- `14:01:24`    US10Y     4.776 hist=1501 z=0.08 z_ready=True dod=0.6 dod%=0.13 flag=GREEN src=treasury.gov par curve + TradingView liv
- `14:01:24`    US30Y     5.236 hist=1501 z=-0.44 z_ready=True dod=-1.4 dod%=-0.27 flag=GREEN src=treasury.gov par curve + TradingView liv
- `14:01:24`    DE02Y     2.94 hist=1046 z=-0.61 z_ready=True dod=-2.0 dod%=-0.68 flag=GREEN src=Bundesbank
- `14:01:24`    DE10Y     3.37 hist=1046 z=-0.36 z_ready=True dod=-1.0 dod%=-0.3 flag=GREEN src=Bundesbank
- `14:01:24`    DE30Y     3.8 hist=1046 z=-0.07 z_ready=True dod=0.0 dod%=0.0 flag=GREEN src=Bundesbank
- `14:01:24`    GB10Y     5.134 hist=1938 z=-2.16 z_ready=True dod=-10.76 dod%=-2.05 flag=RED src=Bank of England + TradingView live
- `14:01:24`    CA10Y     3.761 hist=1501 z=-0.72 z_ready=True dod=-2.9 dod%=-0.77 flag=GREEN src=Bank of Canada Valet + TradingView live
- `14:01:24`    AU10Y     5.172 hist=1501 z=-1.14 z_ready=True dod=-4.8 dod%=-0.92 flag=GREEN src=RBA F2 + TradingView live
- `14:01:24`    CH10Y     0.4311 hist=2 z=None z_ready=False dod=-0.79 dod%=-1.8 flag=GREEN src=TradingView TVC:CH10Y
- `14:01:24`    JP10Y     2.966 hist=9932 z=-1.46 z_ready=True dod=-4.0 dod%=-1.33 flag=GREEN src=MOF Japan
- `14:01:24`    BTP-Bund  0.7727 hist=2 z=None z_ready=False dod=-2.27 dod%=-2.85 flag=GREEN src=spread of Bundesbank / TradingView TVC:I
- `14:01:24`    IT-ES     0.3669 hist=2 z=None z_ready=False dod=-1.13 dod%=-2.99 flag=GREEN src=spread of TradingView TVC:ES10Y / Tradin
- `14:01:24`    MOF JGB curve 2026-09-03 tenors=15 err=None
- `14:01:24`    auction desk: 2026-09-03 $12.5B buyback at max · $185.5B bills -> risk-on supportive tags=['LIQUIDITY EASY', 'EASY-POLICY SIGNAL', 'RISK-ASSET BULLISH'] preds=6
- `14:01:24`    RED=['GB10Y', 'JP30Y', 'AU03Y'] AMBER=['AU02Y', 'AU05Y', 'Gilt-Bund', 'JP2s30s', 'AU2s10s']
## schedules (America/New_York, Mon-Fri)

- `14:01:24` ✅    justhodl-bond-warroom-early updated cron(30 7 ? * MON-FRI *) ET
- `14:01:24` ✅    justhodl-bond-warroom-mid updated cron(0 10 ? * MON-FRI *) ET
- `14:01:24` ✅    justhodl-bond-warroom-after-auction updated cron(35 13 ? * MON-FRI *) ET
- `14:01:24` ✅    justhodl-bond-warroom-close updated cron(45 16 ? * MON-FRI *) ET
- `14:01:24` ✅    justhodl-bond-warroom-evening updated cron(15 19 ? * MON-FRI *) ET
## page

- `14:03:55`    bonds.html carries the war room: True
- `14:04:41`    1440px: {"score": "5", "regime": "CALM", "headline": "Bond markets are calm: UK 10Y (Gilt) -10.8bp (z -2.2); Australia 3Y +10.1bp (z +2.0); Japa", "panels": 8, "rows": 86, "flags": 86, "reds": 3, "jgb": 60, "auction": "$12.5B buyback at max \u00b7 $185.5B bills -> risk-on supportive", "regimeBanner": true, "overflow": 0} errors=[]
- `14:04:50`     390px: {"score": "5", "regime": "CALM", "headline": "Bond markets are calm: UK 10Y (Gilt) -10.8bp (z -2.2); Australia 3Y +10.1bp (z +2.0); Japa", "panels": 8, "rows": 86, "flags": 86, "reds": 3, "jgb": 60, "auction": "$12.5B buyback at max \u00b7 $185.5B bills -> risk-on supportive", "regimeBanner": true, "overflow": 0} errors=[]
- `14:04:50` ✅    GREEN: bond war room live
