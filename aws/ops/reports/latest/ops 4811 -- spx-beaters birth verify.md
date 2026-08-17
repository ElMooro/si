# G0. live feed contracts

**Status:** success  
**Duration:** 22.7s  
**Finished:** 2026-08-17T03:53:02+00:00  

## Data

| env_POLYGON | mem | mom_status | regime | state | timeout |
|---|---|---|---|---|---|
|  | 1536 |  |  | Active | 900 |
| present |  |  |  |  |  |
|  |  |  | {"risk_gate_sizing": null, "rotation_regime": null, "spx_erp_ttm_pct": -0.59, "spy_ret_6m_pct": 13.9} |  |  |
|  |  | {"m6": true, "m12_1": false, "note": "12-1 momentum unlocks at 53 weeks (have 30) -- accretes weekly"} |  |  |  |

## Log
- `03:52:40` ✅   G0 data/universe.json.stocks = 5239
- `03:52:40` ✅   G0 data/asset-compass.json.assets = 31
- `03:52:40` ✅   G0 data/rotation-dashboard.json.assets = 37
- `03:52:40` ✅   G0 data/industry-boom.json.league = 131
- `03:52:40` ✅   G0 data/stock-buying.json.top = 300
- `03:52:40` ✅   G0 data/13f-flows-by-ticker.json.t = 6778
- `03:52:40` ⚠   G0 data/best-setups.json.setups empty/absent (top keys: ['alpha_trust_wiring', 'bond_vol_regime', 'brain_aligned', 'buildout_threats', 'by_verdict', 'contested_picks', 'duration_s', 'engine'])
- `03:52:41` ⚠   G0 data/invest.json.stock_picks empty/absent (top keys: ['elapsed_s', 'engine', 'gate_params', 'generated_at', 'grading_candidates', 'industry_gates', 'leading_indicators', 'method_notes'])
- `03:52:41` ✅   G0 data/sp500.json.members = 495
# 1. function + env + settle

- `03:52:41` ✅ marker settled (attempt 1)
# 2. weekly schedule

- `03:52:42` ✅ schedule justhodl-spx-beaters-weekly created -> cron(0 13 ? * SAT *)
# 3. Event-invoke + poll (<=13 min)

- `03:53:02` ✅   fresh doc after ~20s
# 4. league truths

- `03:53:02` ✅   ledger weeks = 30 (target 53, fetched_now 30)
- `03:53:02` ✅   scanned stocks = 5239
- `03:53:02` ✅   buckets = 8: {"large": 377, "mid": 388, "small": 434, "micro": 308, "etf_equity": 21, "etf_bond": 0, "etf_commodity": 2, "etf_crypto_alt": 0}
- `03:53:02` ✅   listed rows = 77, contract violations = 0
# 5. league readout (top of each bucket)

- `03:53:02` ✅   large          SPCX    94.9  legs={"mom": 1.0, "flows": 0.85}
- `03:53:02`       why: momentum: 6m +539% (top 0%, 12-1 pending 30/53 wks), +525pp vs SPY 6m | 13F net +$7650M last quarters
- `03:53:02` ✅   mid            APGE    92.3  legs={"mom": 0.96, "flows": 0.85}
- `03:53:02`       why: momentum: 6m +99% (top 4%, 12-1 pending 30/53 wks), +85pp vs SPY 6m | 13F net +$75M last quarters
- `03:53:02` ✅   small          SHAZ    93.8  legs={"mom": 0.98, "flows": 0.85}
- `03:53:02`       why: momentum: 6m +149% (top 2%, 12-1 pending 30/53 wks), +135pp vs SPY 6m | 13F net +$102M last quarters
- `03:53:02` ✅   micro          MGRX    83.5  legs={"mom": 0.83, "flows": 0.85}
- `03:53:02`       why: momentum: 6m +35% (top 17%, 12-1 pending 30/53 wks), +21pp vs SPY 6m | 13F net +$129M last quarters
- `03:53:02` ✅   etf_equity     IWD     92.0  legs={"mom": 0.85, "rotation": 1.0}
- `03:53:02`       why: momentum: 6m +16% (+2pp vs SPY) | trend gate PASS (px>200d & 12m>cash); rotation rank #3; RRG LEADING
- `03:53:02` ⚠   etf_bond       (none >= 55.0 yet)
- `03:53:02` ✅   etf_commodity  USO     91.9  legs={"mom": 0.98, "rotation": 0.85}
- `03:53:02`       why: momentum: 6m +66% (+52pp vs SPY) | trend gate PASS (px>200d & 12m>cash); rotation rank #12
- `03:53:02` ⚠   etf_crypto_alt (none >= 55.0 yet)
# 6. verdict

- `03:53:02` ✅ justhodl-spx-beaters LIVE -- weekly all-cap + ETF + asset-class beat-the-SPX league
