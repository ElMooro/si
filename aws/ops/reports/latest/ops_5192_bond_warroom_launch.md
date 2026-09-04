# ops 5192 -- bond war room v1.2 -- official histories

**Status:** failure  
**Duration:** 265.5s  
**Finished:** 2026-09-04T13:45:23+00:00  

## Error

```
SystemExit: 1
```

## Log
- `13:40:58`   Lambda exists — updating
- `13:41:03` ✅   ✓ updated justhodl-bond-warroom
- `13:41:24`    run (15s, error=None) -> {"ok": true, "elapsed_s": 14.5, "n_series": 91, "heartbeat": 5, "regime": "CALM", "equity": "CALM", "eurodollar": "NONE", "red": ["GB10Y", "JP30Y", "AU03Y"], "notes": [], "freshness": {"mof_jgb": "2026-09-03", "tradingview": "2026-09-04", "tradingview_symbols": 28, "official": {"Bank of Canada Valet": "2026-09-03", "treasury.gov par curve": "2026-09-03", "RBA F2": "2026-09-02", "Bundesbank BBSSY": "2026-09-04", "Bank of England": "2026-09-02"}, "official_n": 18, "ecb": "2026-09-03", "fred": "2026-09-03", "yahoo": "2026-09-04"}}
## feed

- `13:41:24`    heartbeat 5 CALM -- Bond markets are calm: UK 10Y (Gilt) -11.7bp (z -2.3); Australia 3Y +10.1bp (z +2.0); Japan 30Y (JGB) -7.0bp (z -1.9); Australia 2s10s -4.6bp (z -2.1); Australia 2Y +9.7bp (z +2.0).
- `13:41:24`    equity: CALM LOW -- Bond prices and yields are inside their normal daily range (TLT +0.4% (z +0.7); 10Y -0.2bp (z -0.1); MOVE 75 -5.0pts (z -1.2); HY OAS +1.0bp (z +0.2)). No rates-driven pressure on stocks either way.
- `13:41:24`    eurodollar: NONE 0 -- No eurodollar-shortage signature: periphery spreads, the dollar and EM/Euro credit are quiet.
- `13:41:24`    freshness={'mof_jgb': '2026-09-03', 'tradingview': '2026-09-04', 'tradingview_symbols': 28, 'official': {'Bank of Canada Valet': '2026-09-03', 'treasury.gov par curve': '2026-09-03', 'RBA F2': '2026-09-02', 'Bundesbank BBSSY': '2026-09-04', 'Bank of England': '2026-09-02'}, 'official_n': 18, 'ecb': '2026-09-03', 'fred': '2026-09-03', 'yahoo': '2026-09-04'} notes=[]
- `13:41:24`    panel us_rates        12 rows: US02Y 4.377 GREEN, US05Y 4.536 GREEN, US07Y 4.63 GREEN, US10Y 4.768 GREEN, US20Y 5.25 GREEN, US30Y 5.233 GREEN, DGS3MO 3.92 GREEN, DFII10 2.45 GREEN, T10YIE 2.35 GREEN
- `13:41:24`    panel volatility       9 rows: ^MOVE 74.68 GREEN, MOVE_TV 74.6812 GREEN, VIXCLS 15.2 GREEN, TLT 82.3701 GREEN, IEF 92.325 GREEN, SHY 81.685 GREEN, HYG 79.175 GREEN, LQD 105.575 GREEN, EMB 94.475 GREEN
- `13:41:24`    panel japan            5 rows: JP02Y 1.85 GREEN, JP10Y 2.966 GREEN, JP30Y 4.052 RED, JP2s30s 2.202 AMBER, US-JGB 1.804 GREEN
- `13:41:24`    panel europe          17 rows: DE02Y 2.9292 GREEN, DE10Y 3.34 GREEN, DE30Y 3.7966 GREEN, FR10Y 4.1745 GREEN, IT02Y 3.1271 GREEN, IT10Y 4.1381 GREEN, ES10Y 3.7758 GREEN, NL10Y 3.404 GREEN, PT10Y 3.6522 GREEN
- `13:41:24`    panel europe_spreads  12 rows: BTP-Bund 0.7981 GREEN, OAT-Bund 0.8345 GREEN, Bono-Bund 0.4358 GREEN, IT-ES 0.3623 GREEN, PT-Bund 0.3122 GREEN, GR-Bund 0.6613 GREEN, Gilt-Bund 1.785 AMBER, US-Bund 1.428 GREEN, EA-periphery 0.4241 GREEN
- `13:41:24`    panel world           15 rows: AU02Y 4.831 AMBER, AU03Y 4.819 RED, AU05Y 4.871 AMBER, AU10Y 5.183 GREEN, CA02Y 3.1 GREEN, CA05Y 3.41 GREEN, CA10Y 3.752 GREEN, CA30Y 4.16 GREEN, CN10Y 1.684 GREEN
- `13:41:24`    panel credit          12 rows: BAMLH0A0HYM2 2.66 GREEN, BAMLC0A0CM 0.81 GREEN, BAMLC0A1CAAA 0.43 GREEN, BAMLC0A4CBBB 0.99 GREEN, BAMLH0A1HYBB 1.53 GREEN, BAMLH0A2HYB 2.76 GREEN, BAMLH0A3HYC 10.53 GREEN, BAMLHE00EHYIOAS 2.62 GREEN, BAMLEMCBPIOAS 1.38 GREEN
- `13:41:24`    panel funding          4 rows: SOFR 3.66 GREEN, DTB3 3.78 GREEN, SOFR-TB3 -0.13 GREEN, DTWEXBGS 118.747 GREEN
## official histories (v1.2)

- `13:41:24`    official sources: {"Bank of Canada Valet": "2026-09-03", "treasury.gov par curve": "2026-09-03", "RBA F2": "2026-09-02", "Bundesbank BBSSY": "2026-09-04", "Bank of England": "2026-09-02"} (n=18)
- `13:41:24`    US02Y     4.377 hist=1501 z=0.76 z_ready=True dod=3.7 dod%=0.85 flag=GREEN src=treasury.gov par curve + TradingView liv
- `13:41:24`    US10Y     4.768 hist=1501 z=-0.12 z_ready=True dod=-0.2 dod%=-0.04 flag=GREEN src=treasury.gov par curve + TradingView liv
- `13:41:24`    US30Y     5.233 hist=1501 z=-0.52 z_ready=True dod=-1.7 dod%=-0.32 flag=GREEN src=treasury.gov par curve + TradingView liv
- `13:41:24`    DE02Y     2.9292 hist=2 z=None z_ready=False dod=-3.6 dod%=-1.21 flag=GREEN src=TradingView TVC:DE02Y
- `13:41:24`    DE10Y     3.34 hist=1046 z=-0.66 z_ready=True dod=-2.0 dod%=-0.6 flag=GREEN src=Bundesbank BBSSY
- `13:41:24`    DE30Y     3.7966 hist=2 z=None z_ready=False dod=-3.45 dod%=-0.9 flag=GREEN src=TradingView TVC:DE30Y
- `13:41:24`    GB10Y     5.125 hist=1938 z=-2.34 z_ready=True dod=-11.66 dod%=-2.22 flag=RED src=Bank of England + TradingView live
- `13:41:24`    CA10Y     3.752 hist=1501 z=-0.93 z_ready=True dod=-3.8 dod%=-1.0 flag=GREEN src=Bank of Canada Valet + TradingView live
- `13:41:24`    AU10Y     5.183 hist=1501 z=-0.89 z_ready=True dod=-3.7 dod%=-0.71 flag=GREEN src=RBA F2 + TradingView live
- `13:41:24`    CH10Y     0.434 hist=2 z=None z_ready=False dod=-0.5 dod%=-1.14 flag=GREEN src=TradingView TVC:CH10Y
- `13:41:24`    JP10Y     2.966 hist=9932 z=-1.46 z_ready=True dod=-4.0 dod%=-1.33 flag=GREEN src=MOF Japan
- `13:41:24`    BTP-Bund  0.7981 hist=2 z=None z_ready=False dod=-1.73 dod%=-2.12 flag=GREEN src=spread of Bundesbank BBSSY / TradingView
- `13:41:24`    IT-ES     0.3623 hist=2 z=None z_ready=False dod=-1.59 dod%=-4.2 flag=GREEN src=spread of TradingView TVC:ES10Y / Tradin
- `13:41:24`    MOF JGB curve 2026-09-03 tenors=15 err=None
- `13:41:24`    auction desk: 2026-09-03 $12.5B buyback at max · $185.5B bills -> risk-on supportive tags=['LIQUIDITY EASY', 'EASY-POLICY SIGNAL', 'RISK-ASSET BULLISH'] preds=6
- `13:41:24`    RED=['GB10Y', 'JP30Y', 'AU03Y'] AMBER=['AU02Y', 'AU05Y', 'Gilt-Bund', 'JP2s30s', 'AU2s10s']
## schedules (America/New_York, Mon-Fri)

- `13:41:25` ✅    justhodl-bond-warroom-early updated cron(30 7 ? * MON-FRI *) ET
- `13:41:25` ✅    justhodl-bond-warroom-mid updated cron(0 10 ? * MON-FRI *) ET
- `13:41:25` ✅    justhodl-bond-warroom-after-auction updated cron(35 13 ? * MON-FRI *) ET
- `13:41:25` ✅    justhodl-bond-warroom-close updated cron(45 16 ? * MON-FRI *) ET
- `13:41:25` ✅    justhodl-bond-warroom-evening updated cron(15 19 ? * MON-FRI *) ET
## page

- `13:44:57`    bonds.html carries the war room: True
- `13:45:14`    1440px: {"score": "5", "regime": "CALM", "headline": "Bond markets are calm: UK 10Y (Gilt) -11.7bp (z -2.3); Australia 3Y +10.1bp (z +2.0); Japa", "panels": 8, "rows": 86, "flags": 86, "reds": 3, "jgb": 60, "auction": "$12.5B buyback at max \u00b7 $185.5B bills -> risk-on supportive", "regimeBanner": true, "overflow": 0} errors=[]
- `13:45:23`     390px: {"score": "5", "regime": "CALM", "headline": "Bond markets are calm: UK 10Y (Gilt) -11.7bp (z -2.3); Australia 3Y +10.1bp (z +2.0); Japa", "panels": 8, "rows": 86, "flags": 86, "reds": 3, "jgb": 60, "auction": "$12.5B buyback at max \u00b7 $185.5B bills -> risk-on supportive", "regimeBanner": true, "overflow": 100} errors=[]
- `13:45:23`    390px overflow offenders (element, px past edge, width): [["A", 959, 81], ["A", 878, 51], ["A", 827, 96], ["A", 730, 96], ["A", 634, 58], ["A", 576, 89], ["TABLE", 516, 891], ["THEAD", 516, 891], ["TR", 516, 891], ["TH", 516, 68], ["TBODY", 516, 891], ["TR.GREEN", 516, 891]]
- `13:45:23` ✗    390px overflow 100px
