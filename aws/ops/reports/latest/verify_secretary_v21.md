# Verify Secretary v2.1 scan output

**Status:** success  
**Duration:** 45.1s  
**Finished:** 2026-04-23T12:15:33+00:00  

## Data

| card | movers | present | signals |
|---|---|---|---|
| options |  | yes | 2 |
| crypto | 10 | yes |  |
| sector_rotation |  | yes |  |

## Log
- `12:14:47` Waiting 45s for async scan to complete…
## Top-level scan metadata

- `12:15:33`   version: 2.1
- `12:15:33`   timestamp: 2026-04-23 07:13:51 ET
- `12:15:33`   scan_time_seconds: 36.0
- `12:15:33`   recommendations: 10
- `12:15:33`   top_buys: 4
- `12:15:33`   has tier2 key: True
- `12:15:33`   has deltas key: True
- `12:15:33`   has cftc key: True
## tier2.options

- `12:15:33`   put_call_ratio: 0.148
- `12:15:33`   pc_signal: COMPLACENCY
- `12:15:33`   gamma_regime: POSITIVE_GAMMA
- `12:15:33`   max_gamma_strike: 697
- `12:15:33`   spy_price: 711.21
- `12:15:33`   trading_signals count: 2
- `12:15:33`     - CONTRARIAN_SELL (MODERATE): P/C 0.15 - extreme call buying
- `12:15:33`     - RISK_OFF (MODERATE): $8374M rotating to safe havens
## tier2.crypto

- `12:15:33`   btc_dominance: 58.1
- `12:15:33`   total_mcap_fmt: $2.68T
- `12:15:33`   mcap_change_24h: -1.04%
- `12:15:33`   stablecoin_net_signal: INFLOW
- `12:15:33`   fear_greed_value: 46 (Fear)
- `12:15:33`   risk_score: {'score': 45, 'regime': 'MODERATE', 'action': 'NORMAL', 'signals': ['     Stablecoin inflows']}
- `12:15:33`   top_movers count: 10
- `12:15:33`     - BTC: $77823 (-0.63%)
- `12:15:33`     - ETH: $2332.1 (-2.80%)
- `12:15:33`     - USDT: $1.0 (+0.01%)
- `12:15:33`     - XRP: $1.42 (-2.50%)
- `12:15:33`     - BNB: $634.29 (-1.47%)
## tier2.sector_rotation

- `12:15:33`   keys: ['top_inflow', 'top_inflow_name', 'top_inflow_flow', 'top_outflow', 'top_outflow_name', 'top_outflow_flow', 'rotation_signal']
## AI briefing snippet

- `12:15:33`   length: 5733 chars
- `12:15:33`     # KHALID'S MARKET BRIEFING
- `12:15:33`     
- `12:15:33`     ---
- `12:15:33`     
- `12:15:33`     ## 1. VERDICT
- `12:15:33`     **NEUTRAL-TO-BEARISH** — Positive gamma expiry mechanics and stablecoin inflows mask deteriorating breadth (0% hit rate yesterday), moderate risk-off signals in options, and Fed ti
- `12:15:33`     
- `12:15:33`     ---
- `12:15:33`     
- `12:15:33`     ## 2. LIQUIDITY
- `12:15:33`     Net liquidity flat at $0B with **tightening regime intact**—no fresh Fed support, RRP/TGA neutral. The 1M change of $0B suggests we're in a maintenance phase, not expansion; this i
- `12:15:33`     
- `12:15:33`     ---
- `12:15:33`     
- `12:15:33`     ## 3. RISK
- `12:15:33`     VIX at 20.0 is **false comfort**—we're in the sweet spot of mean reversion *before* volatility reprices. HY at 4.00% is tight but not alarming; the real tell is **2s10s at 0.00% (f
- `12:15:33`     
- `12:15:33`     ---
- `12:15:33`     
- `12:15:33`     ## 4. TOP 5 TRADES
- `12:15:33`     
- `12:15:33`     | **TICKER** | **ENTRY RANGE** | **1ST TARGET** | **STOP** | **THESIS** |
- `12:15:33`     |---|---|---|---|---|
- `12:15:33`     | **TLT** | $86.50–$86.90 | $89.20 | $84.80 | Long-duration bond bid into 0% 2s10s signals panic-hedge demand; +9.6% upside vs. +6.0% downside skew matches duration unwind into ris
- `12:15:33`     | **XLU** | $44.70–$44.95 | $47.30 | $43.50 | Utilities' +7.2% upside / +4.5% downside ratio is **only defensive name with >150bps skew edge**. Tightening regime = stable utility c
- `12:15:33`     | **GLD** | $434.80–$435.70 | $445.00 | $428.50 | Fear/Greed at 46 (Fear) + stablecoin inflows (BTC -0.8%, ETH -2.8%) = liquidity seeking hard assets. Gold's +9.6% upside / -6.0% d
- `12:15:33`     | **BTC** | $76,900–$77,400 | $81,200 | $75,200 | BTC dominance 58.1% is ironically *contrarian bullish*—means capital rotating into largest-cap crypto as fear reprices. 7d +4.0% v
- `12:15:33`     | **XRP** | $1.38–$1.42 | $1.58 | $1.32 | **Contrarian entry**: -2.5% 24h in a market consolidating is washout. Put/call 0.148 (extreme complacency) + MODERATE crypto risk regime =
- `12:15:33`     
- `12:15:33`     ---
- `12:15:33`     
- `12:15:33`     ## 5. WHAT CHANGED vs YESTERDAY
- `12:15:33`     
- `12:15:33`     - **Hit rate collapsed to 0%**: Yesterday's picks missed completely—suggests either model drift or regime inflection. Tightening persistence (no liquidity Δ) is reason, not market 
- `12:15:33`     - **Risk score +1.5 → 24.5/100**: Still "low" but *directionally tightening*. This is pre-VIX move; vol is repricing ahead of curve.  
- `12:15:33`     - **Crypto's Fear/Greed @46**: Dropped materially from implied yesterday (inflows signal contrarian reversal, not capitulation yet). Stablecoin inflows + MVRV 0.81 = positioning fo
- `12:15:33`     - **No macro data**: ISM/CPI/DXY/Oil all N/A—means today is positioning day, not fundamental repricing. Technicals + options flow dominate.
- `12:15:33`     
- `12:15:33`     ---
- `12:15:33`     
- `12:15:33` Done
