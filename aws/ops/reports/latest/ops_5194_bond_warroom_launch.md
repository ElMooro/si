# ops 5194 -- bond war room v1.2.2 -- SNB dropped (stale), true overflow hunter

**Status:** failure  
**Duration:** 299.6s  
**Finished:** 2026-09-04T13:58:35+00:00  

## Error

```
SystemExit: 1
```

## Log
- `13:53:35`   Lambda exists — updating
- `13:53:39` ✅   ✓ updated justhodl-bond-warroom
- `13:53:56`    run (12s, error=None) -> {"ok": true, "elapsed_s": 10.9, "n_series": 91, "heartbeat": 5, "regime": "CALM", "equity": "CALM", "eurodollar": "NONE", "red": ["GB10Y", "JP30Y", "AU03Y"], "notes": [], "freshness": {"mof_jgb": "2026-09-03", "tradingview": "2026-09-04", "tradingview_symbols": 28, "official": {"Bank of England": "2026-09-02", "RBA F2": "2026-09-02", "Bank of Canada Valet": "2026-09-03", "Bundesbank": "2026-09-04", "treasury.gov par curve": "2026-09-03"}, "official_n": 20, "ecb": "2026-09-03", "fred": "2026-09-03", "yahoo": "2026-09-04"}}
## feed

- `13:53:56`    heartbeat 5 CALM -- Bond markets are calm: UK 10Y (Gilt) -11.2bp (z -2.3); Australia 3Y +10.1bp (z +2.0); Japan 30Y (JGB) -7.0bp (z -1.9); Australia 2s10s -4.6bp (z -2.1); Australia 2Y +9.7bp (z +2.0).
- `13:53:56`    equity: CALM LOW -- Bond prices and yields are inside their normal daily range (TLT +0.4% (z +0.7); 10Y -0.2bp (z -0.1); MOVE 75 -5.0pts (z -1.2); HY OAS +1.0bp (z +0.2)). No rates-driven pressure on stocks either way.
- `13:53:56`    eurodollar: NONE 0 -- No eurodollar-shortage signature: periphery spreads, the dollar and EM/Euro credit are quiet.
- `13:53:56`    freshness={'mof_jgb': '2026-09-03', 'tradingview': '2026-09-04', 'tradingview_symbols': 28, 'official': {'Bank of England': '2026-09-02', 'RBA F2': '2026-09-02', 'Bank of Canada Valet': '2026-09-03', 'Bundesbank': '2026-09-04', 'treasury.gov par curve': '2026-09-03'}, 'official_n': 20, 'ecb': '2026-09-03', 'fred': '2026-09-03', 'yahoo': '2026-09-04'} notes=[]
- `13:53:56`    panel us_rates        12 rows: US02Y 4.374 GREEN, US05Y 4.536 GREEN, US07Y 4.63 GREEN, US10Y 4.768 GREEN, US20Y 5.25 GREEN, US30Y 5.231 GREEN, DGS3MO 3.92 GREEN, DFII10 2.45 GREEN, T10YIE 2.35 GREEN
- `13:53:56`    panel volatility       9 rows: ^MOVE 74.68 GREEN, MOVE_TV 74.6812 GREEN, VIXCLS 15.2 GREEN, TLT 82.3999 GREEN, IEF 92.3201 GREEN, SHY 81.685 GREEN, HYG 79.205 GREEN, LQD 105.595 GREEN, EMB 94.48 GREEN
- `13:53:56`    panel japan            5 rows: JP02Y 1.85 GREEN, JP10Y 2.966 GREEN, JP30Y 4.052 RED, JP2s30s 2.202 AMBER, US-JGB 1.804 GREEN
- `13:53:56`    panel europe          17 rows: DE02Y 2.94 GREEN, DE10Y 3.37 GREEN, DE30Y 3.8 GREEN, FR10Y 4.1765 GREEN, IT02Y 3.1223 GREEN, IT10Y 4.1396 GREEN, ES10Y 3.7758 GREEN, NL10Y 3.404 GREEN, PT10Y 3.6523 GREEN
- `13:53:56`    panel europe_spreads  12 rows: BTP-Bund 0.7696 GREEN, OAT-Bund 0.8065 GREEN, Bono-Bund 0.4058 GREEN, IT-ES 0.3638 GREEN, PT-Bund 0.2823 GREEN, GR-Bund 0.6265 GREEN, Gilt-Bund 1.7592 AMBER, US-Bund 1.398 GREEN, EA-periphery 0.4241 GREEN
- `13:53:56`    panel world           15 rows: AU02Y 4.831 AMBER, AU03Y 4.819 RED, AU05Y 4.871 AMBER, AU10Y 5.173 GREEN, CA02Y 3.1 GREEN, CA05Y 3.41 GREEN, CA10Y 3.758 GREEN, CA30Y 4.16 GREEN, CN10Y 1.684 GREEN
- `13:53:56`    panel credit          12 rows: BAMLH0A0HYM2 2.66 GREEN, BAMLC0A0CM 0.81 GREEN, BAMLC0A1CAAA 0.43 GREEN, BAMLC0A4CBBB 0.99 GREEN, BAMLH0A1HYBB 1.53 GREEN, BAMLH0A2HYB 2.76 GREEN, BAMLH0A3HYC 10.53 GREEN, BAMLHE00EHYIOAS 2.62 GREEN, BAMLEMCBPIOAS 1.38 GREEN
- `13:53:56`    panel funding          4 rows: SOFR 3.66 GREEN, DTB3 3.78 GREEN, SOFR-TB3 -0.13 GREEN, DTWEXBGS 118.747 GREEN
## official histories (v1.2)

- `13:53:56`    official sources: {"Bank of England": "2026-09-02", "RBA F2": "2026-09-02", "Bank of Canada Valet": "2026-09-03", "Bundesbank": "2026-09-04", "treasury.gov par curve": "2026-09-03"} (n=20)
- `13:53:56`    US02Y     4.374 hist=1501 z=0.69 z_ready=True dod=3.4 dod%=0.78 flag=GREEN src=treasury.gov par curve + TradingView liv
- `13:53:56`    US10Y     4.768 hist=1501 z=-0.12 z_ready=True dod=-0.2 dod%=-0.04 flag=GREEN src=treasury.gov par curve + TradingView liv
- `13:53:56`    US30Y     5.231 hist=1501 z=-0.58 z_ready=True dod=-1.9 dod%=-0.36 flag=GREEN src=treasury.gov par curve + TradingView liv
- `13:53:56`    DE02Y     2.94 hist=1046 z=-0.61 z_ready=True dod=-2.0 dod%=-0.68 flag=GREEN src=Bundesbank
- `13:53:56`    DE10Y     3.37 hist=1046 z=-0.36 z_ready=True dod=-1.0 dod%=-0.3 flag=GREEN src=Bundesbank
- `13:53:56`    DE30Y     3.8 hist=1046 z=-0.07 z_ready=True dod=0.0 dod%=0.0 flag=GREEN src=Bundesbank
- `13:53:56`    GB10Y     5.1292 hist=1938 z=-2.26 z_ready=True dod=-11.24 dod%=-2.14 flag=RED src=Bank of England + TradingView live
- `13:53:56`    CA10Y     3.758 hist=1501 z=-0.79 z_ready=True dod=-3.2 dod%=-0.84 flag=GREEN src=Bank of Canada Valet + TradingView live
- `13:53:56`    AU10Y     5.173 hist=1501 z=-1.11 z_ready=True dod=-4.7 dod%=-0.9 flag=GREEN src=RBA F2 + TradingView live
- `13:53:56`    CH10Y     0.4319 hist=2 z=None z_ready=False dod=-0.71 dod%=-1.62 flag=GREEN src=TradingView TVC:CH10Y
- `13:53:56`    JP10Y     2.966 hist=9932 z=-1.46 z_ready=True dod=-4.0 dod%=-1.33 flag=GREEN src=MOF Japan
- `13:53:56`    BTP-Bund  0.7696 hist=2 z=None z_ready=False dod=-2.58 dod%=-3.24 flag=GREEN src=spread of Bundesbank / TradingView TVC:I
- `13:53:56`    IT-ES     0.3638 hist=2 z=None z_ready=False dod=-1.44 dod%=-3.81 flag=GREEN src=spread of TradingView TVC:ES10Y / Tradin
- `13:53:56`    MOF JGB curve 2026-09-03 tenors=15 err=None
- `13:53:56`    auction desk: 2026-09-03 $12.5B buyback at max · $185.5B bills -> risk-on supportive tags=['LIQUIDITY EASY', 'EASY-POLICY SIGNAL', 'RISK-ASSET BULLISH'] preds=6
- `13:53:56`    RED=['GB10Y', 'JP30Y', 'AU03Y'] AMBER=['AU02Y', 'AU05Y', 'Gilt-Bund', 'JP2s30s', 'AU2s10s']
## schedules (America/New_York, Mon-Fri)

- `13:53:56` ✅    justhodl-bond-warroom-early updated cron(30 7 ? * MON-FRI *) ET
- `13:53:57` ✅    justhodl-bond-warroom-mid updated cron(0 10 ? * MON-FRI *) ET
- `13:53:57` ✅    justhodl-bond-warroom-after-auction updated cron(35 13 ? * MON-FRI *) ET
- `13:53:57` ✅    justhodl-bond-warroom-close updated cron(45 16 ? * MON-FRI *) ET
- `13:53:57` ✅    justhodl-bond-warroom-evening updated cron(15 19 ? * MON-FRI *) ET
## page

- `13:58:14`    bonds.html carries the war room: True
- `13:58:26`    1440px: {"score": "5", "regime": "CALM", "headline": "Bond markets are calm: UK 10Y (Gilt) -11.2bp (z -2.3); Australia 3Y +10.1bp (z +2.0); Japa", "panels": 8, "rows": 86, "flags": 86, "reds": 3, "jgb": 60, "auction": "$12.5B buyback at max \u00b7 $185.5B bills -> risk-on supportive", "regimeBanner": true, "overflow": 0} errors=[]
- `13:58:35`     390px: {"score": "5", "regime": "CALM", "headline": "Bond markets are calm: UK 10Y (Gilt) -11.2bp (z -2.3); Australia 3Y +10.1bp (z +2.0); Japa", "panels": 8, "rows": 86, "flags": 86, "reds": 3, "jgb": 60, "auction": "$12.5B buyback at max \u00b7 $185.5B bills -> risk-on supportive", "regimeBanner": true, "overflow": 100} errors=[]
- `13:58:35`    390px overflow offenders (element, px past edge, width): {"body": 490, "html": 490, "cw": 390, "top": [["DIV.consensus", 100, 154, "static", 336], ["DIV#regime-consensus.big", 100, 154, "static", 336], ["DIV#regime-days.sub", 100, 154, "static", 336], ["DIV.jhnav-backdrop", 100, 490, "fixed", 0], ["DIV.jh-tenor-pill", 88, 133, "fixed", 345], ["DIV.jh-liq-pill", 88, 135, "fixed", 343], ["DIV.jh-lce-pill", 88, 148, "fixed", 330], ["BUTTON#jhpai-fab", 84, 173, "fixed", 301], ["BUTTON.jhi-fab", 82, 104, "fixed", 368], ["DIV.jhi-panel", 82, 350, "fixed", 122]]}
- `13:58:35` ✗    390px overflow 100px
