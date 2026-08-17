# 1. invoke-to-complete (<=3 rounds)

**Status:** success  
**Duration:** 21.4s  
**Finished:** 2026-08-17T04:05:59+00:00  

## Data

| bond_crypto_candidates | counts | listed_from_set | spcx_league | spcx_universe | with_ledger_closes |
|---|---|---|---|---|---|
|  |  |  |  | {"symbol": "SPCX", "name": "Space Exploration Technologies Corp.", "sector": "Industrials", "industry": "Aerospace & Defense", "exchange": "NASDAQ", "country": "US", "is_adr": false, "market_cap": 1421869608163, "price": 108.74, "cap_bucket": "mega", "source": "screener"} |  |
|  |  |  | {"bucket": "large", "t": "SPCX", "name": "Space Exploration Technologies Corp.", "sector": "Industrials", "industry": "Aerospace & Defense", "mcap": 1421869608163.0, "score": 94.7, "legs": {"mom": 0.99, "flows": 0.85}, "n_legs": 2, "ret_6m_pct": 539.2, "rs_6m_pp": 525.3, "ret_12_1_pct": 402.3, "why": ["momentum: 12-1 +402% (top 1% of all stocks), +525pp vs SPY 6m", "13F net +$7650M last quarters"] |  |  |
| 12 |  | 0 |  |  | 12 |
|  | {"large": 369, "mid": 364, "small": 400, "micro": 196, "etf_equity": 22, "etf_bond": 0, "etf_commodity": 3, "etf_crypto_alt": 0} |  |  |  |  |

## Log
- `04:05:59` ✅   round 1: weeks=53 fetched_now=23 (~20s)
# 2. 12-1 truths

- `04:05:59` ✅   ledger weeks = 53 / 53
- `04:05:59` ✅   mom_status.m12_1 = True
- `04:05:59` ✅   SPY 12-1 = 15.5%
- `04:05:59` ✅   listed rows with 12-1 = 71/78
# 3a. SPCX forensics

- `04:05:59` ⚠   SPCX looks like a FUND inside universe.stocks -- upstream universe hygiene item, note for universe-builder
# 3b. bond/crypto coverage forensics

- `04:05:59`     BIL      T-Bills / Cash               class~bond      ledger=Y bucket=-
- `04:05:59`     BTC      Bitcoin                      class~crypto    ledger=Y bucket=-
- `04:05:59`     EMB      EM Sovereign Debt            class~bond      ledger=Y bucket=-
- `04:05:59`     ETH      Ethereum                     class~crypto    ledger=Y bucket=-
- `04:05:59`     HYG      US High Yield (junk)         class~bond      ledger=Y bucket=-
- `04:05:59`     IBIT     Bitcoin (spot ETF)           class~crypto    ledger=Y bucket=-
- `04:05:59`     IEF      US Treasuries 7-10y          class~bond      ledger=Y bucket=-
- `04:05:59`     LQD      US IG Corporates             class~bond      ledger=Y bucket=-
- `04:05:59`     MUB      US Munis                     class~bond      ledger=Y bucket=-
- `04:05:59`     SHY      1-3y Treasuries              class~bond      ledger=Y bucket=-
- `04:05:59`     TIP      US TIPS                      class~bond      ledger=Y bucket=-
- `04:05:59`     TLT      US Treasuries 20y+           class~bond      ledger=Y bucket=-
- `04:05:59` ✅   candidates exist with ledger closes but none cleared score>=55 with >=2 legs -- HONEST ZERO this week (bonds under trend-gate pressure)
# 4. league re-read (12-1 live)

- `04:05:59` ✅   large          SPCX    94.7  6m=539.2% 12-1=402.3%
- `04:05:59`       momentum: 12-1 +402% (top 1% of all stocks), +525pp vs SPY 6m | 13F net +$7650M last quarters
- `04:05:59` ✅   mid            APGE    93.2  6m=98.5% 12-1=259.3%
- `04:05:59`       momentum: 12-1 +259% (top 2% of all stocks), +85pp vs SPY 6m | 13F net +$75M last quarters
- `04:05:59` ✅   small          SHAZ    93.8  6m=148.5% 12-1=None%
- `04:05:59`       momentum: 6m +149% (top 2%, 12-1 pending 53/53 wks), +135pp vs SPY 6m | 13F net +$102M last quarters
- `04:05:59` ✅   micro          QTTB    74.7  6m=288.5% 12-1=664.0%
- `04:05:59`       momentum: 12-1 +664% (top 0% of all stocks), +275pp vs SPY 6m | 13F net -$0M last quarters
- `04:05:59` ✅   etf_equity     XLE     92.9  6m=13.9% 12-1=34.8%
- `04:05:59`       momentum: 6m +14% (+0pp vs SPY) | trend gate PASS (px>200d & 12m>cash); rotation rank #4; RRG LEADING
- `04:05:59`   etf_bond       (none)
- `04:05:59` ✅   etf_commodity  USO     91.2  6m=66.1% 12-1=70.3%
- `04:05:59`       momentum: 6m +66% (+52pp vs SPY) | trend gate PASS (px>200d & 12m>cash); rotation rank #12
- `04:05:59`   etf_crypto_alt (none)
# 5. verdict

- `04:05:59` ✅ ledger COMPLETE (53w) -- 12-1 momentum live market-wide; forensics recorded
