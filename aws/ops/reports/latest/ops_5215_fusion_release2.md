# ops 5215 -- fusion Release 2: API v1 + Fusion Desk + shadow ledger

**Status:** success  
**Duration:** 239.8s  
**Finished:** 2026-09-07T18:43:09+00:00  

## Error

```
SystemExit: 0
```

## Data

| agree | capital | direction | entity | existing | fusion |
|---|---|---|---|---|---|
| 0/0 | OPEN | slightly_bullish | market:US_EQUITY |  | 0.2245 |
| 1/2 | OPEN | bullish | etf:SPY | dealer_gex,etf_flows,institutional_13f_flows | 0.5619 |
| 2/2 | OPEN | slightly_bearish | etf:QQQ | dealer_gex,etf_flows,institutional_13f_flows | -0.2616 |
| 2/3 | OPEN | slightly_bearish | etf:IWM | dealer_gex,etf_flows,institutional_13f_flows | -0.2198 |
| 1/4 | OPEN | slightly_bearish | equity:NVDA | catalyst,dealer_gex,estimate_revisions,institutional_13f_flows | -0.1657 |
| 1/1 | OPEN | bullish | equity:TSM | institutional_13f_flows | 0.5619 |
| 1/1 | OPEN | bullish | equity:ASML | institutional_13f_flows | 0.5619 |
| 1/3 | OPEN | slightly_bearish | equity:AAPL | dealer_gex,institutional_13f_flows,short_interest | -0.2616 |
| 4/4 | OPEN | bullish | equity:MSFT | catalyst,dealer_gex,institutional_13f_flows,short_interest | 0.5204 |
| 2/2 | OPEN | bullish | equity:AMZN | catalyst,dealer_gex,institutional_13f_flows | 0.5619 |
| 0/0 | OPEN | neutral | equity:GOOGL | catalyst,dealer_gex,institutional_13f_flows,short_interest | 0.1418 |
| 3/3 | OPEN | bullish | equity:META | dealer_gex,institutional_13f_flows,short_interest | 0.7404 |
| 0/0 | OPEN | slightly_bullish | crypto:BTC |  | 0.2245 |
| 0/0 | OPEN | slightly_bullish | crypto:ETH |  | 0.2245 |

## Log
## fusion Lambda

- `18:39:10`   Lambda exists — updating
- `18:39:13` ✅   ✓ updated justhodl-jh-fusion
- `18:39:29` ✅    fusion 20260907T183919Z-cebe2a5a v1.1.0: 14 entities, shadow=True, shadow block {"key": "data/jh-fusion/shadow.json", "agreement_rate": 0.72, "n_compared": 25, "logging": {"enabled": true, "logged": 12, "skipped": 2, "errors": []}}
- `18:39:29` ✅    shadow.json: 14 rows, agreement 0.72 on 25 reads, logging {"enabled": true, "logged": 12, "skipped": 2, "errors": []}
- `18:39:29`    jh_fusion ledger rows today: 12 -> ['SPY UP 770.19', 'QQQ DOWN 718.96', 'IWM DOWN 296.01', 'NVDA DOWN 230.36', 'TSM UP 428.91', 'ASML UP 1714.88', 'AAPL DOWN 319.97', 'MSFT UP 499.7', 'AMZN UP 258.51', 'META UP 616.77', 'BTC-USD UP 79112.24', 'ETH-USD UP 2487.77']
## API v1 (justhodl.ai/api/v1/*)

- `18:39:30`    /api/v1/health live at the edge: True (0s)
- `18:39:30`    /api/v1/fusion -> HTTP 200, 12657 bytes, ok=True
- `18:39:30`    /api/v1/fusion/NVDA -> HTTP 200, 8598 bytes, ok=True
- `18:39:30`    /api/v1/fusion/equity:NVDA?horizon=INTERMEDIATE&full=1 -> HTTP 200, 8106 bytes, ok=True
- `18:39:30`    /api/v1/fusion/NVDA/changes -> HTTP 200, 1427 bytes, ok=True
- `18:39:30`    /api/v1/signals/SPY?family=RISK -> HTTP 200, 1215 bytes, ok=True
- `18:39:30`    /api/v1/regime/current -> HTTP 200, 1110 bytes, ok=True
- `18:39:30`    /api/v1/opportunities?min_confidence=0.3&limit=5 -> HTTP 200, 2423 bytes, ok=True
- `18:39:30`    /api/v1/fusion/ZZZZ -> HTTP 404, 238 bytes, ok=True
## fusion.html

- `18:42:46`    fusion.html carries JH_FUSION_DESK_V1 at the edge: True
- `18:42:59`    1440px: {"score": "+0.27", "posture": "MILDLY SUPPORTIVE", "legs": 5, "board": 14, "evid": 217, "evrows": 3, "tabs": 3, "helps": 12, "defs": 13, "opps": 20, "shadow": 14, "overflow": 0, "err": ""} errors=[] csp=[]
- `18:43:09`     390px: {"score": "+0.27", "posture": "MILDLY SUPPORTIVE", "legs": 5, "board": 14, "evid": 217, "evrows": 3, "tabs": 3, "helps": 12, "defs": 13, "opps": 20, "shadow": 14, "overflow": 0, "err": ""} errors=[] csp=[]
- `18:43:09` ✅ GREEN -- Release 2 live: /api/v1/*, fusion.html, shadow ledger (jh_fusion in justhodl-signals) -- still shadow mode
