# ops 6937 - Risk Map 2.0.0: first light of the deployed engine, 2026-11-24T16:10+00:00

**Status:** success  
**Duration:** 27.0s  
**Finished:** 2026-11-24T16:11:26+00:00  

## Data

| booming | cross_asset | destroyed | failed_checks | regime | seconds | tiles | universe |
|---|---|---|---|---|---|---|---|
| 8 | LIQUIDATION | 100 | 0 | BROAD RISK-OFF | 11.2 | 293 | 293 |

## Log
## 1. Guard: the deployed function is this checkout's

- `16:11:00` justhodl-regime-map updated 2026-11-24T16:09:51 | memory 1024 MB, timeout 300 s | files as in this checkout: {'lambda_function.py': True, 'risk_math.py': True, 'risk_universe.py': True, 'leaders_tape.py': True}
- `16:11:00` schedule: justhodl-regime-map-daily cron(0 21 * * ? *) ENABLED
- `16:11:01` before: data/regime-map.json version 1.0.0, 16040 bytes, generated 2026-11-23T21:00:51.734190+00:00, regime BROAD RISK-OFF, 38 instruments
## 2. One dry run inside Lambda (nothing written)

- `16:11:14` HTTP 200, function error None, 12.7 s | {"statusCode": 200, "dry_run": true, "regime": "BROAD RISK-OFF", "cross_asset": "LIQUIDATION", "tiles": 293, "universe": 293, "booming": 8, "destroyed": 100, "left_out": 0}
- `16:11:14` the feed and the snapshot are byte for byte what they were: the dry run wrote nothing; left out: none
## 3. One publication (a one-off run outside the 21:00 UTC schedule)

- `16:11:25` HTTP 200, function error None, 11.2 s | {"statusCode": 200, "body": "{\"ok\": true, \"regime\": \"BROAD RISK-OFF\", \"n\": 38, \"tiles\": 293, \"equity_avg\": -11.9, \"crypto_avg\": -61.0}"}
## 4. Read back

- `16:11:26` data/regime-map.json: 221844 bytes written 16:11:26 | risk-map.v2 2.0.0 | generated 2026-11-24T16:11:14.599858+00:00 | built in 10.7 s
- `16:11:26` 293 of 293 instruments: {'fund': 216, 'coin': 40, 'fx': 23, 'fut': 10, 'idx': 4} | dates {'fund': '2026-11-24', 'coin': '2026-11-23', 'fx': '2026-11-23', 'fut': '2026-11-23', 'idx': '2026-11-23', 'funds_forming': 216, 'funds_forming_as_of': '2026-11-24T15:55:28+00:00', 'bank_last_session': '2026-11-23', 'coin_listing_day': '2026-11-23'}
- `16:11:26` regime BROAD RISK-OFF (was BROAD RISK-OFF under 1.0.0): {'concentration_spread_3m': -2.4, 'equity_breadth_pct': 36, 'equity_avg': -11.9, 'crypto_avg': -61.0, 'rates_avg': -15.8, 'commodities_avg': -33.0, 'intl_avg': -41.0}
- `16:11:26` cross-asset LIQUIDATION: Risk assets are falling and the usual stores of value are not rising with them; the dollar is. Cash is the haven. Risk assets -35.0, stores of value -31.1 (Treasuries -11.7, gold -68, yen and franc -13.5), dollar +34.
- `16:11:26` booming (8): VIX, ZR, VVIX, VIXY, BTAL, UAE, XLV, PHO
- `16:11:26` destroyed (100): WGMI, REMX, SLV, URA, APT, DOGE, ADA, HBAR, DOT, COPX, XRP, SHIB, BCH, PALL, TAN, NLR, WLD, HE, SIL, PPLT, FIL, MSOS, GDXJ, LIT, BLOK, ETC, ARGT, ATOM, XME, XLM, DRIV, PICK, EWY, ICLN, REM, UFO, ENA, GDX, TUR, ONDO, SUI, EWZ, GLD, OIH, ETH, BOTZ, EPU, KWEB, TAO, IPO, SRVR, ALI, ILF, ECH, SLX, VET, EIS, SOL, SMH, LTC, RPG, OJ, CTA, EZA, RENDER, BRL, BTC, POL, DBB, TIA, IWC, WEAT, EIDO, EPHE, MCHI, AAXJ, EEM, QTUM, BNB, CWB, IYZ, ALGO, EFNL, KC, AVAX, AAVE, ICP, ASHR, CPER, AMLP, LINK, ARKK, EWT, MTUM, IWO, THD, FXI, EWM, PFF, WOOD
- `16:11:26` held back from booming / destroyed: [('IBB', 'scores as booming (+47) but neither the month nor the three months is up 5%'), ('MOVE', 'scores as booming (+80) but the last month is down'), ('NEAR', 'scores as booming (+50) but the last month is down and the close is under its 50-day average')]
- `16:11:26` left out: none | notes: none
- `16:11:26` turns since the run before: 4 on the first map, 0 on the whole map (the snapshot before this run held only the first map's 38)
- `16:11:26`     US stocks       93 | score  -27.8 | rising  29% | booming 2 risk-on 13 neutral 19 risk-off 28 destroyed 31
- `16:11:26`     World stocks    52 | score  -36.2 | rising  12% | booming 1 risk-on 3 neutral 3 risk-off 24 destroyed 21
- `16:11:26`     Bonds           34 | score  -17.6 | rising   9% | risk-on 2 neutral 18 risk-off 12 destroyed 2
- `16:11:26`     Currencies      25 | score  -21.8 | rising  24% | risk-on 5 neutral 1 risk-off 18 destroyed 1
- `16:11:26`     Commodities     29 | score  -30.9 | rising  21% | booming 1 risk-on 2 neutral 6 risk-off 9 destroyed 11
- `16:11:26`     Crypto          40 | score  -57.5 | rising  10% | risk-on 2 neutral 4 risk-off 3 destroyed 31
- `16:11:26`     Real estate      5 | score  -36.2 | rising  20% | neutral 2 risk-off 1 destroyed 2
- `16:11:26`     Volatility       5 | score   64.2 | rising 100% | booming 3 risk-on 2
- `16:11:26`     Alternatives    10 | score   -3.9 | rising  40% | booming 1 risk-on 1 neutral 4 risk-off 3 destroyed 1
- `16:11:26` this op's own checks on the published document: 0 problems []
- `16:11:26` served at https://justhodl.ai/data/regime-map.json: version 2.0.0, generated 2026-11-24T16:11:14.599858+00:00
- `16:11:26` ✅ FIRST LIGHT: the deployed engine published 293 instruments in nine asset classes in 11.2 s; 8 booming, 100 destroyed, each by the rule
