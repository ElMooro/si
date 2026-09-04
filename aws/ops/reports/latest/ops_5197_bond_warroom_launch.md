# ops 5197 -- bond war room v1.3.0 warehouse-first + official-yields lane

**Status:** failure  
**Duration:** 807.3s  
**Finished:** 2026-09-04T14:36:44+00:00  

## Error

```
SystemExit: 1
```

## Log
- `14:23:17`   Lambda exists — updating
- `14:23:20` ✅   ✓ updated justhodl-bond-warroom
- `14:23:56`    run (30s, error=None) -> {"ok": true, "elapsed_s": 29.4, "n_series": 91, "heartbeat": 5, "regime": "CALM", "equity": "CALM", "eurodollar": "NONE", "red": ["GB10Y", "JP30Y", "AU03Y"], "notes": [], "freshness": {"mof_jgb": "2026-09-03", "tradingview": "2026-09-04", "tradingview_symbols": 28, "official": {"warehouse:boe + Bank of England": "2026-09-02", "warehouse:official-yields + Bundesbank": "2026-09-04", "warehouse:official-yields + Bank of Canada": "2026-09-03", "warehouse:treasury-par + treasury.gov": "2026-09-03", "warehouse:official-yields + RBA": "2026-09-02"}, "official_n": 20, "ecb": "2026-09-03", "fred": null, "yahoo": null, "warehouse_lane": {"official_yields_banked": 33}}}
## feed

- `14:23:56`    heartbeat 5 CALM -- Bond markets are calm: UK 10Y (Gilt) -11.2bp (z -2.2); Australia 3Y +10.1bp (z +2.0); Japan 30Y (JGB) -7.0bp (z -1.9); Australia 2s10s -4.6bp (z -2.1); Australia 2Y +9.7bp (z +2.0).
- `14:23:56`    equity: CALM LOW -- Bond prices and yields are inside their normal daily range (TLT +0.4% (z +0.8); 10Y -0.4bp (z -0.2); MOVE 75 -5.0pts (z -1.2); HY OAS -1.0bp (z -0.2)). No rates-driven pressure on stocks either way.
- `14:23:56`    eurodollar: NONE 0 -- No eurodollar-shortage signature: periphery spreads, the dollar and EM/Euro credit are quiet.
- `14:23:56`    freshness={'mof_jgb': '2026-09-03', 'tradingview': '2026-09-04', 'tradingview_symbols': 28, 'official': {'warehouse:boe + Bank of England': '2026-09-02', 'warehouse:official-yields + Bundesbank': '2026-09-04', 'warehouse:official-yields + Bank of Canada': '2026-09-03', 'warehouse:treasury-par + treasury.gov': '2026-09-03', 'warehouse:official-yields + RBA': '2026-09-02'}, 'official_n': 20, 'ecb': '2026-09-03', 'fred': None, 'yahoo': None, 'warehouse_lane': {'official_yields_banked': 33}} notes=[]
- `14:23:56`    panel us_rates        12 rows: US02Y 4.368 GREEN, US05Y 4.532 GREEN, US07Y 4.63 GREEN, US10Y 4.766 GREEN, US20Y 5.25 GREEN, US30Y 5.231 GREEN, DGS3MO 3.92 GREEN, DFII10 2.45 GREEN, T10YIE 2.35 GREEN
- `14:23:56`    panel volatility       9 rows: ^MOVE 74.68 GREEN, MOVE_TV 74.6812 GREEN, VIXCLS 14.32 GREEN, TLT 82.405 GREEN, IEF 92.33 GREEN, SHY 81.685 GREEN, HYG 79.19 GREEN, LQD 105.6 GREEN, EMB 94.49 GREEN
- `14:23:56`    panel japan            5 rows: JP02Y 1.85 GREEN, JP10Y 2.966 GREEN, JP30Y 4.052 RED, JP2s30s 2.202 AMBER, US-JGB 1.804 GREEN
- `14:23:56`    panel europe          17 rows: DE02Y 2.94 GREEN, DE10Y 3.37 GREEN, DE30Y 3.8 GREEN, FR10Y 4.1819 GREEN, IT02Y 3.1253 GREEN, IT10Y 4.1412 GREEN, ES10Y 3.7587 GREEN, NL10Y 3.4125 GREEN, PT10Y 3.6559 GREEN
- `14:23:56`    panel europe_spreads  12 rows: BTP-Bund 0.7712 GREEN, OAT-Bund 0.8119 GREEN, Bono-Bund 0.3887 GREEN, IT-ES 0.3825 GREEN, PT-Bund 0.2859 GREEN, GR-Bund 0.6302 GREEN, Gilt-Bund 1.7598 AMBER, US-Bund 1.396 GREEN, EA-periphery 0.4241 GREEN
- `14:23:56`    panel world           15 rows: AU02Y 4.831 AMBER, AU03Y 4.819 RED, AU05Y 4.871 AMBER, AU10Y 5.177 GREEN, CA02Y 3.1 GREEN, CA05Y 3.41 GREEN, CA10Y 3.752 GREEN, CA30Y 4.16 GREEN, CN10Y 1.684 GREEN
- `14:23:56`    panel credit          12 rows: BAMLH0A0HYM2 2.65 GREEN, BAMLC0A0CM 0.81 GREEN, BAMLC0A1CAAA 0.44 GREEN, BAMLC0A4CBBB 1.0 GREEN, BAMLH0A1HYBB 1.52 GREEN, BAMLH0A2HYB 2.76 GREEN, BAMLH0A3HYC 10.51 GREEN, BAMLHE00EHYIOAS 2.65 GREEN, BAMLEMCBPIOAS 1.38 GREEN
- `14:23:56`    panel funding          4 rows: SOFR 3.66 GREEN, DTB3 3.78 GREEN, SOFR-TB3 -0.13 GREEN, DTWEXBGS 118.747 GREEN
## official histories (v1.2)

- `14:23:56`    official sources: {"warehouse:boe + Bank of England": "2026-09-02", "warehouse:official-yields + Bundesbank": "2026-09-04", "warehouse:official-yields + Bank of Canada": "2026-09-03", "warehouse:treasury-par + treasury.gov": "2026-09-03", "warehouse:official-yields + RBA": "2026-09-02"} (n=20)
- `14:23:56`    US02Y     4.368 hist=1501 z=0.56 z_ready=True dod=2.8 dod%=0.65 flag=GREEN src=warehouse:treasury-par + treasury.gov + 
- `14:23:56`    US10Y     4.766 hist=1501 z=-0.17 z_ready=True dod=-0.4 dod%=-0.08 flag=GREEN src=warehouse:treasury-par + treasury.gov + 
- `14:23:56`    US30Y     5.231 hist=1501 z=-0.58 z_ready=True dod=-1.9 dod%=-0.36 flag=GREEN src=warehouse:treasury-par + treasury.gov + 
- `14:23:56`    DE02Y     2.94 hist=1046 z=-0.61 z_ready=True dod=-2.0 dod%=-0.68 flag=GREEN src=warehouse:official-yields + Bundesbank
- `14:23:56`    DE10Y     3.37 hist=7381 z=-0.36 z_ready=True dod=-1.0 dod%=-0.3 flag=GREEN src=warehouse:official-yields + Bundesbank
- `14:23:56`    DE30Y     3.8 hist=1046 z=-0.07 z_ready=True dod=0.0 dod%=0.0 flag=GREEN src=warehouse:official-yields + Bundesbank
- `14:23:56`    GB10Y     5.1298 hist=11290 z=-2.24 z_ready=True dod=-11.18 dod%=-2.13 flag=RED src=warehouse:boe + Bank of England + Tradin
- `14:23:56`    CA10Y     3.752 hist=1501 z=-0.93 z_ready=True dod=-3.8 dod%=-1.0 flag=GREEN src=warehouse:official-yields + Bank of Cana
- `14:23:56`    AU10Y     5.177 hist=1501 z=-1.03 z_ready=True dod=-4.3 dod%=-0.82 flag=GREEN src=warehouse:official-yields + RBA + Tradin
- `14:23:56`    CH10Y     0.4229 hist=2 z=None z_ready=False dod=-1.61 dod%=-3.67 flag=GREEN src=TradingView TVC:CH10Y
- `14:23:56`    JP10Y     2.966 hist=9932 z=-1.46 z_ready=True dod=-4.0 dod%=-1.33 flag=GREEN src=MOF Japan
- `14:23:56`    BTP-Bund  0.7712 hist=2 z=None z_ready=False dod=-2.42 dod%=-3.04 flag=GREEN src=spread of TradingView TVC:IT10Y / wareho
- `14:23:56`    IT-ES     0.3825 hist=2 z=None z_ready=False dod=0.43 dod%=1.14 flag=GREEN src=spread of TradingView TVC:ES10Y / Tradin
## warehouse-first (v1.3)

- `14:23:56`    rows sourced warehouse-first: 48 / 86
- `14:23:56`    DGS10          src=None asof=None
- `14:23:56`    BAMLH0A0HYM2   src=warehouse:fred + FRED tail BAMLH0A0HYM2 asof=2026-09-03
- `14:23:56`    TLT            src=warehouse:tv-bars + Yahoo tail TLT asof=2026-09-04
- `14:23:56`    ^MOVE          src=warehouse:tv-bars + Yahoo tail ^MOVE asof=2026-09-03
- `14:23:56`    US10Y          src=warehouse:treasury-par + treasury.gov + TradingView live asof=2026-09-04
- `14:23:56`    DE10Y          src=warehouse:official-yields + Bundesbank asof=2026-09-04
- `14:23:56`    GB10Y          src=warehouse:boe + Bank of England + TradingView live asof=2026-09-04
- `14:23:56`    CA10Y          src=warehouse:official-yields + Bank of Canada + TradingView live asof=2026-09-04
- `14:23:56`    AU10Y          src=warehouse:official-yields + RBA + TradingView live asof=2026-09-04
- `14:23:56`    EA_AAA10Y      src=warehouse:official-yields + ECB YC asof=2026-09-03
- `14:23:56`    sources_doctrine: warehouse first: every series is read from our own AWS (fred-scoped, treasury-par, boe iadb, tv-bars, official-yields) through the symdir resolver, the official
- `14:23:57`    official-yields lane: 34 objects: ['_state.json', 'au-10y-rba.json', 'au-2y-rba.json', 'au-3y-rba.json', 'au-5y-rba.json', 'br-10y-tv.json', 'ca-10y-boc.json', 'ca-2y-boc.json', 'ca-5y-boc.json', 'ca-long-boc.json', 'ch-10y-tv.json', 'cn-10y-tv.json', 'de-10y-bbk.json', 'de-2y-bbk.json', 'de-30y-bbk.json', 'ea-aaa-10y-ecb.json', 'ea-all-10y-ecb.json', 'es-10y-tv.json', 'fr-10y-tv.json', 'gb-10y-boe.json', 'gb-20y-boe.json', 'gb-2y-tv.json', 'gb-5y-boe.json', 'gr-10y-tv.json', 'in-10y-tv.json', 'it-10y-tv.json', 'it-2y-tv.json', 'jp-10y-mof.json', 'jp-2y-mof.json', 'jp-30y-mof.json', 'kr-10y-tv.json', 'mx-10y-tv.json', 'nl-10y-tv.json', 'pt-10y-tv.json']
- `14:23:57`    lane state: count=33 banked_at=2026-09-04T14:23:56+00:00 sample=[{'id': 'au-10y-rba', 'key': 'data/warm/official-yields/au-10y-rba.json', 'last': '2026-09-02', 'n_obs': 1500}, {'id': 'au-2y-rba', 'key': 'data/warm/official-yields/au-2y-rba.json', 'last': '2026-09-02', 'n_obs': 1500}, {'id': 'au-3y-rba', 'key': 'data/warm/official-yields/au-3y-rba.json', 'last': '2026-09-02', 'n_obs': 1500}, {'id': 'au-5y-rba', 'key': 'data/warm/official-yields/au-5y-rba.json', 'last': '2026-09-02', 'n_obs': 1500}]
- `14:23:57`    /series official-yields:ca-10y-boc       n=1500 last=['2026-09-03', 3.79] src=warehouse:official-yields (Bank of Canada Valet)
- `14:23:58`    /series official-yields:jp-10y-mof       n=9932 last=['2026-09-03', 2.966] src=warehouse:official-yields (Japan MOF)
- `14:23:59`    /series official-yields:it-10y-tv        n=2 last=['2026-09-04', 4.1412] src=warehouse:official-yields (TradingView scanner)
- `14:23:59`    /series official-yields:gb-5y-boe        n=1937 last=['2026-09-02', 4.6975] src=warehouse:official-yields (Bank of England IADB)
## data.html provider catalog

- `14:23:59`   Lambda exists — updating
- `14:24:03` ✅   ✓ updated justhodl-provider-catalog
- `14:26:41`    data/providers/official-yields.json: {"slug": "official-yields", "name": "Official daily sovereign yields \u2014 Treasury, Bundesbank, BoE, BoC, RBA, MOF, ECB + market closes", "series": {"count": 33, "ids": ["au-10y-rba", "au-2y-rba", "au-3y-rba", "au-5y-rba", "br-10y-tv", "ca-10y-boc", "ca-2y-boc", "ca-5y-boc", "ca-long-boc", "ch-10y
- `14:26:41`    MOF JGB curve 2026-09-03 tenors=15 err=None
- `14:26:41`    auction desk: 2026-09-03 $12.5B buyback at max · $185.5B bills -> risk-on supportive tags=['LIQUIDITY EASY', 'EASY-POLICY SIGNAL', 'RISK-ASSET BULLISH'] preds=6
- `14:26:41`    RED=['GB10Y', 'JP30Y', 'AU03Y'] AMBER=['AU02Y', 'AU05Y', 'Gilt-Bund', 'JP2s30s', 'AU2s10s']
## schedules (America/New_York, Mon-Fri)

- `14:26:42` ✅    justhodl-bond-warroom-early updated cron(30 7 ? * MON-FRI *) ET
- `14:26:42` ✅    justhodl-bond-warroom-mid updated cron(0 10 ? * MON-FRI *) ET
- `14:26:42` ✅    justhodl-bond-warroom-after-auction updated cron(35 13 ? * MON-FRI *) ET
- `14:26:42` ✅    justhodl-bond-warroom-close updated cron(45 16 ? * MON-FRI *) ET
- `14:26:42` ✅    justhodl-bond-warroom-evening updated cron(15 19 ? * MON-FRI *) ET
## page

- `14:36:44`    bonds.html carries the war room: False
- `14:36:44` ✗    DGS10 not warehouse-first (None)
- `14:36:44` ✗    page deploy not observed
