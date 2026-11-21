# ops 6880 - crypto prices: every lane, and how old each is (read-only), 2026-11-21T17:31+00:00

**Status:** success  
**Duration:** 49.1s  
**Finished:** 2026-11-21T17:32:42+00:00  

## Data

| coins_banked | coins_tried | ohlc_daily_ok | series_x_ok |
|---|---|---|---|
| 93 | 40 | 40 | 40 |

## Log
## 1. The banked crypto bars

- `17:31:53` 93 coins banked: AAVE ADA AERO AKT ALGO APE APT AR ARB ASTR ATOM AVAX AXS BCH BNB BONK BTC CFX CHZ COMP CRV DASH DOGE DOT EGLD EIGEN ENA ENS EOS ETC ETH FET FIL FLOW FLR GALA GRT HBAR HYPE ICP IMX INJ IOTA JTO JUP KAS KAVA LDO LINK LTC MANA MATIC MINA MKR NEAR NEO ONDO OP PENDLE PEPE POL PYTH QNT RENDER RNDR ROSE RUNE SAND SEI SHIB SNX SOL STRK STX SUI TAO THETA TIA TON TRX UNI VET VIRTUAL W WIF WLD XLM XMR XRP XTZ ZEC ZK ZRO
- `17:31:53` last written between 09-04 19:07 and 11-21 03:48
- `17:31:53`   BTC: 2268 rows, source polygon X:BTCUSD daily, banked 2026-11-21T03:48, last rows [['2026-11-19', 62981.71, 64095.0, 61350.0, 61913.23, 21364.957687344813], ['2026-11-20', 61913.5, 62640.0, 57655.17, 58814.99, 40212.316444158176]]
- `17:31:53`   ETH: 2268 rows, source polygon X:ETHUSD daily, banked 2026-11-21T03:48, last rows [['2026-11-19', 1774.55, 1817.3, 1734.23, 1755.38, 237848.6772889725], ['2026-11-20', 1755.37, 1770.9, 1577.65, 1619.29, 524683.8621731261]]
- `17:31:53`   SOL: 2094 rows, source polygon X:SOLUSD daily, banked 2026-11-21T03:48, last rows [['2026-11-19', 73.54, 75.61, 72.01, 73.06, 1526483.8877165387], ['2026-11-20', 73.06, 73.5, 67.01, 68.72, 2847821.019047826]]
## 2. The data proxy's /ohlc for <COIN>-USD (the chart's crypto tape)

- `17:31:54`   BTC    day : HTTP 200  4449 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 60026.578125 | warehouse+yahoo
- `17:31:55`   ETH    day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 1679.8599853515625 | warehouse+yahoo
- `17:31:56`   SOL    day : HTTP 200  2417 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 70.61000061035156 | warehouse+yahoo
- `17:31:56`   XRP    day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.9330000281333923 | warehouse+yahoo
- `17:31:57`   DOGE   day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.05516999959945679 | warehouse+yahoo
- `17:31:58`   ADA    day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.14523999392986298 | warehouse+yahoo
- `17:31:58`   AVAX   day : HTTP 200  2254 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 6.623000144958496 | warehouse+yahoo
- `17:31:59`   DOT    day : HTTP 200  2285 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.6572999954223633 | warehouse+yahoo
- `17:32:00`   LINK   day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 8.576000213623047 | warehouse+yahoo
- `17:32:00`   LTC    day : HTTP 200  4449 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 40.03099822998047 | warehouse+yahoo
- `17:32:01`   BCH    day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 197.91000366210938 | warehouse+yahoo
- `17:32:02`   ATOM   day : HTTP 200  2810 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 1.1894999742507935 | warehouse+yahoo
- `17:32:02`   NEAR   day : HTTP 200  2230 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 2.8736000061035156 | warehouse+yahoo
- `17:32:03`   APT    day : HTTP 200  1494 bars | last 2026-11-20T00:00+00:00 (41.5 h old) close 0.3691 | warehouse
- `17:32:04`   SUI    day : HTTP 200  1297 bars | last 2026-11-20T00:00+00:00 (41.5 h old) close 0.65 | warehouse
- `17:32:04`   PEPE   day : HTTP 200  1284 bars | last 2026-11-20T00:00+00:00 (41.5 h old) close 3.47e-06 | warehouse
- `17:32:05`   SHIB   day : HTTP 200  2304 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 3.6199999158270657e-06 | warehouse+yahoo
- `17:32:06`   BNB    day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 547.2000122070312 | warehouse+yahoo
- `17:32:06`   TRX    day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.3048979640007019 | warehouse+yahoo
- `17:32:07`   TON    day : HTTP 200  2276 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.0018038507550954819 | warehouse+yahoo
- `17:32:07`   XLM    day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.12870199978351593 | warehouse+yahoo
- `17:32:08`   UNI    day : HTTP 200  2255 bars | last 2026-11-20T00:00+00:00 (41.5 h old) close 4.4628 | warehouse
- `17:32:09`   ETC    day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 5.495999813079834 | warehouse+yahoo
- `17:32:09`   FIL    day : HTTP 200  3266 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.5160999894142151 | warehouse+yahoo
- `17:32:10`   ICP    day : HTTP 200  2022 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 1.9305000305175781 | warehouse+yahoo
- `17:32:11`   HBAR   day : HTTP 200  2623 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.051330000162124634 | warehouse+yahoo
- `17:32:11`   ARB    day : HTTP 200  3282 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.0006290287710726261 | warehouse+yahoo
- `17:32:12`   OP     day : HTTP 200  1714 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.0701799988746643 | warehouse+yahoo
- `17:32:12`   AAVE   day : HTTP 200  2242 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 99.3499984741211 | warehouse+yahoo
- `17:32:13`   MKR    day : HTTP 200  3289 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 1442.3092041015625 | warehouse+yahoo
- `17:32:13`   INJ    day : HTTP 200  2223 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 6.1529998779296875 | warehouse+yahoo
- `17:32:14`   MATIC  day : HTTP 200  1678 bars | last 2025-10-14T00:00+00:00 (9689.5 h old) close 0.1995 | warehouse
- `17:32:15`   POL    day : HTTP 200   983 bars | last 2026-11-20T00:00+00:00 (41.5 h old) close 0.07587 | warehouse
- `17:32:15`   ALGO   day : HTTP 200  2711 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.0745600014925003 | warehouse+yahoo
- `17:32:16`   XMR    day : HTTP 200  3300 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 481.61376953125 | warehouse+yahoo
- `17:32:17`   RNDR   day : HTTP 200  1245 bars | last 2026-09-29T00:00+00:00 (1289.5 h old) close 1.896 | warehouse
- `17:32:17`   SEI    day : HTTP 200  1195 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.03452000021934509 | warehouse+yahoo
- `17:32:18`   TIA    day : HTTP 200  1728 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.3255000114440918 | warehouse+yahoo
- `17:32:18`   WIF    day : HTTP 200  1069 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 0.1204100027680397 | warehouse+yahoo
- `17:32:19`   BONK   day : HTTP 200  1423 bars | last 2026-11-21T00:00+00:00 (17.5 h old) close 2.0400000266818097e-06 | warehouse+yahoo
- `17:32:19` daily bars through /ohlc for 40 of 40 coins: BTC ETH SOL XRP DOGE ADA AVAX DOT LINK LTC BCH ATOM NEAR APT SUI PEPE SHIB BNB TRX TON XLM UNI ETC FIL ICP HBAR ARB OP AAVE MKR INJ MATIC POL ALGO XMR RNDR SEI TIA WIF BONK
- `17:32:20`   BTC    1 hour  : HTTP 200     0 bars | last - (- min old) close None | polygon (warehouse empty)
- `17:32:20`   BTC    5 minute: HTTP 200     0 bars | last - (- min old) close None | polygon (warehouse empty)
- `17:32:21`   ETH    1 hour  : HTTP 200     0 bars | last - (- min old) close None | polygon (warehouse empty)
- `17:32:22`   ETH    5 minute: HTTP 200     0 bars | last - (- min old) close None | polygon (warehouse empty)
- `17:32:22`   SOL    1 hour  : HTTP 200     0 bars | last - (- min old) close None | polygon (warehouse empty)
- `17:32:23`   SOL    5 minute: HTTP 200     0 bars | last - (- min old) close None | polygon (warehouse empty)
- `17:32:24`   DOGE   1 hour  : HTTP 200     0 bars | last - (- min old) close None | polygon (warehouse empty)
- `17:32:24`   DOGE   5 minute: HTTP 200     0 bars | last - (- min old) close None | polygon (warehouse empty)
## 3. The directory's /series for X:<COIN>USD (a search pick of the directory's crypto id)

- `17:32:25`   X:BTCUSD   HTTP 200  2268 points | last ["2026-11-20", 58814.99] | warehouse:data/warm/katlin/crypto-bars/BTC.json.gz
- `17:32:25`   X:ETHUSD   HTTP 200  2268 points | last ["2026-11-20", 1619.29] | warehouse:data/warm/katlin/crypto-bars/ETH.json.gz
- `17:32:26`   X:SOLUSD   HTTP 200  2094 points | last ["2026-11-20", 68.72] | warehouse:data/warm/katlin/crypto-bars/SOL.json.gz
- `17:32:26`   X:XRPUSD   HTTP 200  2267 points | last ["2026-11-20", 0.9086] | warehouse:data/warm/katlin/crypto-bars/XRP.json.gz
- `17:32:27`   X:DOGEUSD  HTTP 200  2268 points | last ["2026-11-20", 0.05426] | warehouse:data/warm/katlin/crypto-bars/DOGE.json.gz
- `17:32:27`   X:ADAUSD   HTTP 200  2268 points | last ["2026-11-20", 0.14286] | warehouse:data/warm/katlin/crypto-bars/ADA.json.gz
- `17:32:27`   X:AVAXUSD  HTTP 200  1837 points | last ["2026-11-20", 6.508] | warehouse:data/warm/katlin/crypto-bars/AVAX.json.gz
- `17:32:28`   X:DOTUSD   HTTP 200  2268 points | last ["2026-11-20", 0.6619] | warehouse:data/warm/katlin/crypto-bars/DOT.json.gz
- `17:32:29`   X:LINKUSD  HTTP 200  2267 points | last ["2026-11-20", 8.525] | warehouse:data/warm/katlin/crypto-bars/LINK.json.gz
- `17:32:29`   X:LTCUSD   HTTP 200  2267 points | last ["2026-11-20", 40.552] | warehouse:data/warm/katlin/crypto-bars/LTC.json.gz
- `17:32:30`   X:BCHUSD   HTTP 200  2268 points | last ["2026-11-20", 194.29] | warehouse:data/warm/katlin/crypto-bars/BCH.json.gz
- `17:32:30`   X:ATOMUSD  HTTP 200  2268 points | last ["2026-11-20", 1.2255] | warehouse:data/warm/katlin/crypto-bars/ATOM.json.gz
- `17:32:31`   X:NEARUSD  HTTP 200  1782 points | last ["2026-11-20", 2.8448] | warehouse:data/warm/katlin/crypto-bars/NEAR.json.gz
- `17:32:31`   X:APTUSD   HTTP 200  1494 points | last ["2026-11-20", 0.3691] | warehouse:data/warm/katlin/crypto-bars/APT.json.gz
- `17:32:31`   X:SUIUSD   HTTP 200  1297 points | last ["2026-11-20", 0.65] | warehouse:data/warm/katlin/crypto-bars/SUI.json.gz
- `17:32:32`   X:PEPEUSD  HTTP 200  1284 points | last ["2026-11-20", 3.47e-06] | warehouse:data/warm/katlin/crypto-bars/PEPE.json.gz
- `17:32:32`   X:SHIBUSD  HTTP 200  1898 points | last ["2026-11-20", 3.58e-06] | warehouse:data/warm/katlin/crypto-bars/SHIB.json.gz
- `17:32:33`   X:BNBUSD   HTTP 200   533 points | last ["2026-11-20", 539.32] | warehouse:data/warm/katlin/crypto-bars/BNB.json.gz
- `17:32:33`   X:TRXUSD   HTTP 200  2226 points | last ["2026-10-10", 0.330595] | warehouse:data/warm/katlin/crypto-bars/TRX.json.gz
- `17:32:34`   X:TONUSD   HTTP 200  1285 points | last ["2026-11-20", 0.9771] | warehouse:data/warm/katlin/crypto-bars/TON.json.gz
- `17:32:34`   X:XLMUSD   HTTP 200  2267 points | last ["2026-11-20", 0.127205] | warehouse:data/warm/katlin/crypto-bars/XLM.json.gz
- `17:32:34`   X:UNIUSD   HTTP 200  2255 points | last ["2026-11-20", 4.4628] | warehouse:data/warm/katlin/crypto-bars/UNI.json.gz
- `17:32:35`   X:ETCUSD   HTTP 200  2268 points | last ["2026-11-20", 5.467] | warehouse:data/warm/katlin/crypto-bars/ETC.json.gz
- `17:32:35`   X:FILUSD   HTTP 200  2228 points | last ["2026-11-20", 0.5346] | warehouse:data/warm/katlin/crypto-bars/FIL.json.gz
- `17:32:35`   X:ICPUSD   HTTP 200  2020 points | last ["2026-11-20", 2.0218] | warehouse:data/warm/katlin/crypto-bars/ICP.json.gz
- `17:32:36`   X:HBARUSD  HTTP 200  1491 points | last ["2026-11-20", 0.05155] | warehouse:data/warm/katlin/crypto-bars/HBAR.json.gz
- `17:32:36`   X:ARBUSD   HTTP 200  1339 points | last ["2026-11-20", 0.11388] | warehouse:data/warm/katlin/crypto-bars/ARB.json.gz
- `17:32:36`   X:OPUSD    HTTP 200  1633 points | last ["2026-11-20", 0.07] | warehouse:data/warm/katlin/crypto-bars/OP.json.gz
- `17:32:36`   X:AAVEUSD  HTTP 200  2168 points | last ["2026-11-20", 97.87] | warehouse:data/warm/katlin/crypto-bars/AAVE.json.gz
- `17:32:37`   X:MKRUSD   HTTP 200  1957 points | last ["2026-09-29", 1788.0] | warehouse:data/warm/katlin/crypto-bars/MKR.json.gz
- `17:32:37`   X:INJUSD   HTTP 200  1928 points | last ["2026-11-20", 6.362] | warehouse:data/warm/katlin/crypto-bars/INJ.json.gz
- `17:32:37`   X:MATICUSD HTTP 200  1678 points | last ["2025-10-14", 0.1995] | warehouse:data/warm/katlin/crypto-bars/MATIC.json.gz
- `17:32:38`   X:POLUSD   HTTP 200   983 points | last ["2026-11-20", 0.07587] | warehouse:data/warm/katlin/crypto-bars/POL.json.gz
- `17:32:38`   X:ALGOUSD  HTTP 200  2268 points | last ["2026-11-20", 0.0734] | warehouse:data/warm/katlin/crypto-bars/ALGO.json.gz
- `17:32:39`   X:XMRUSD   HTTP 200  2225 points | last ["2026-10-10", 517.06] | warehouse:data/warm/katlin/crypto-bars/XMR.json.gz
- `17:32:39`   X:RNDRUSD  HTTP 200  1245 points | last ["2026-09-29", 1.896] | warehouse:data/warm/katlin/crypto-bars/RNDR.json.gz
- `17:32:39`   X:SEIUSD   HTTP 200  1193 points | last ["2026-11-20", 0.03519] | warehouse:data/warm/katlin/crypto-bars/SEI.json.gz
- `17:32:40`   X:TIAUSD   HTTP 200  1116 points | last ["2026-11-20", 0.3278] | warehouse:data/warm/katlin/crypto-bars/TIA.json.gz
- `17:32:40`   X:WIFUSD   HTTP 200  1015 points | last ["2026-11-20", 0.12183] | warehouse:data/warm/katlin/crypto-bars/WIF.json.gz
- `17:32:40`   X:BONKUSD  HTTP 200  1073 points | last ["2026-11-20", 2.1e-06] | warehouse:data/warm/katlin/crypto-bars/BONK.json.gz
- `17:32:40` /series answers for 40 of 40: BTC ETH SOL XRP DOGE ADA AVAX DOT LINK LTC BCH ATOM NEAR APT SUI PEPE SHIB BNB TRX TON XLM UNI ETC FIL ICP HBAR ARB OP AAVE MKR INJ MATIC POL ALGO XMR RNDR SEI TIA WIF BONK
## 4. Polygon with the account's key (never printed)

- `17:32:41` all-crypto snapshot: HTTP 403 status NOT_AUTHORIZED, 0 tickers | You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing
- `17:32:41` grouped daily for today (2026-11-21): HTTP 403 status NOT_AUTHORIZED, 0 rows | Attempted to request today's data before end of day. Please upgrade your plan at https://polygon.io/pricing
- `17:32:41` X:BTCUSD 1 day aggregates today: HTTP 403 status NOT_AUTHORIZED, 0 rows | newest - close - (- min old) | Your plan doesn't include this data timeframe. Please upgrade your plan at https://polygon.io/pricin
- `17:32:41` X:BTCUSD 1 hour aggregates today: HTTP 403 status NOT_AUTHORIZED, 0 rows | newest - close - (- min old) | Your plan doesn't include this data timeframe. Please upgrade your plan at https://polygon.io/pricin
- `17:32:41` X:BTCUSD 5 minute aggregates today: HTTP 403 status NOT_AUTHORIZED, 0 rows | newest - close - (- min old) | Your plan doesn't include this data timeframe. Please upgrade your plan at https://polygon.io/pricin
- `17:32:42` last trade X:BTCUSD: HTTP 403 {"status": "NOT_AUTHORIZED", "message": "You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing", "error": null}
- `17:32:42` ✅ read-only; nothing changed
