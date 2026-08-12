# ops 4639 — schema realignment

**Status:** failure  
**Duration:** 57.1s  
**Finished:** 2026-08-12T21:29:22+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | fn_error | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|
|  | None |  |  |  |  |  |
| 15.1 |  | 686 | 17.4 | FORMING TURN TO EASE | MIXED | 13.0 |

## Log
## deploy (ops-side) + settle

- `21:28:26` ✅   [deploy] v1.4.1 live
## fleet-store shapes (BDI/CRYPTOCAP evidence)

- `21:28:27` data/freight-pulse.json: {"ok": true, "version": "2.0.0", "generated_at": "2026-08-12T11:50:17.996738+0", "engine_class": "physical_trade_slow_confirma", "composite_role": "slow_confirmation_leg", "lag_months": -2}
- `21:28:27` data/cryptoquant-series.json: {"generated_at": "2026-08-12T21:05:07+00:00", "series": {"btc_exchange_netflow": {"d": "list", "v": "list"}, "btc_exchange_inflow": {"d": "list", "v": "list"}, "btc_exchange_outflow": {"d": "list", "v": "list"}, "btc_exchange_reserve": {"d": "list", "v": "list"}, "btc_exchange_addr_in": {"d": "list"
- `21:28:27` cryptoquant metric keys (54): ['btc_addresses_active', 'btc_blockreward', 'btc_coinbase_premium', 'btc_difficulty', 'btc_exchange_addr_in', 'btc_exchange_inflow', 'btc_exchange_netflow', 'btc_exchange_outflow', 'btc_exchange_reserve', 'btc_exchange_supply_ratio', 'btc_fees_total', 'btc_fees_tx_mean', 'btc_fund_flow_ratio', 'btc_funding_rates', 'btc_hashrate', 'btc_liquidations', 'btc_miner_netflow', 'btc_miner_outflow', 'btc_miner_reserve', 'btc_mpi', 'btc_mvrv', 'btc_nupl', 'btc_nvm', 'btc_nvt', 'btc_nvt_golden', 'btc_open_interest', 'btc_puell', 'btc_realized_price', 'btc_sopr', 'btc_sopr_ratio', 'btc_ssr', 'btc_stablecoins_ratio', 'btc_stock_to_flow', 'btc_supply_total', 'btc_taker_ratio', 'btc_tokens_transferred', 'btc_tx_count', 'btc_utxo_count', 'btc_velocity', 'btc_whale_ratio']
- `21:28:27` data/coinmarketcap.json: MISS An error occurred (NoSuchKey) when calling the GetObject ope
## run + row-schema truth

- `21:29:22` CAPITALCOM:COPPER/TVC:GOLD res=True  z=1.63  trend=UP    
- `21:29:22` ECONOMICS:USM2             res=None  z=None  trend=None  
- `21:29:22` FOREXCOM:USDJPY            res=None  z=None  trend=None  
- `21:29:22` TVC:GB10Y                  res=None  z=None  trend=None  
- `21:29:22` CRYPTOCAP:TOTAL            res=None  z=None  trend=None  
- `21:29:22` INDEX:BDI                  res=None  z=None  trend=None  
- `21:29:22` CRYPTOCAP:TOTAL                      res=None  last=None           n=None 
- `21:29:22` CRYPTOCAP:BTC.D                      res=None  last=None           n=None 
- `21:29:22` CRYPTOCAP:USDT.D+CRYPTOCAP:USDC.D    res=False last=None           n=None 
- `21:29:22` ✗   [cryptocap] CONTRACT MISS — TOTAL $0.00T · BTC.D None% · USDT+USDC.D None% — self-building series seeded
- `21:29:22` ✅   [mined-routes] commodity-leg route proven (COPPER/GOLD z-based); tenor/FX prefixes armed for member symbols
- `21:29:22` ✅   [resolution] 686/1086 resolved — residue is wall-class (NQ product, TE plan, licenses) + level-only vault rows; CRYPTOCAP join armed by key census above
- `21:29:22` FRED:WALCL               chg=+0.15% (WoW)     z=0.79  trend=UP    rev=NONE
- `21:29:22` FRED:DGS10               chg=+1.51% (DoD)     z=1.49  trend=UP    rev=NONE
- `21:29:22` AMEX:JNK                 chg=-0.17% (DoD)     z=0.68  trend=DOWN  rev=NONE
- `21:29:22` TVC:DE10Y-TVC:IT10Y      chg=-3.60% (MoM)     z=0.27  trend=UP    rev=REVERSAL_UP
- `21:29:22` ✅   [row-schema] 4/4 sample rows carry chg+trend+reversal(alias)
- `21:29:22` ✅   [dials] TREND 13.0 (MIXED) · REV 17.4 (FORMING TURN TO EASE)
## edge page/payload

- `21:29:22` ✅   [edge] page reads evolved keys; payload rows render-ready
## verdict

- `21:29:22` ✗ schema realign: 1 red
