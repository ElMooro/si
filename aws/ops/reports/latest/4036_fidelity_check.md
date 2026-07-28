# ops 4036 — note-fidelity: exactly as in TradingView

**Status:** failure  
**Duration:** 0.6s  
**Finished:** 2026-07-28T17:54:32+00:00  

## Error

```
SystemExit: 1
```

## Data

| byte_exact | count_mismatches | notes_at_cap | orphan_symbols_not_in_any_list | sampled | symbols_with_notes | symbols_with_tv_source | tagged_symbols | unique_symbols | watchlisted_with_notes | watchlists |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 771 | 0 |  | 10319 |  | 491 |
|  | 28 |  | 38 |  |  |  | 1029 |  | 991 |  |
| 12 |  |  |  | 30 |  |  |  |  |  |  |
|  |  | 0 |  |  |  |  |  |  |  |  |

## Log
## A. count parity per symbol (watchlisted only)

- `17:54:32`     MISMATCH GC2!: brain=6 workbench=4
- `17:54:32`     MISMATCH UNEMPLOY: brain=7 workbench=6
- `17:54:32`     MISMATCH VIX: brain=5 workbench=1
- `17:54:32`     MISMATCH WLRRAL: brain=6 workbench=4
- `17:54:32`     MISMATCH RRPONTSYD: brain=10 workbench=9
- `17:54:32`     MISMATCH US02Y: brain=12 workbench=10
- `17:54:32`     MISMATCH VOO: brain=10 workbench=9
- `17:54:32`     MISMATCH CL1!: brain=19 workbench=18
- `17:54:32`     orphans (INFO): ['CBOE:SPX', 'UNTAGGED', 'ICEUS:DXY', 'TSX:WCN', 'XDN', 'NASDAQ:XDN']
## B. verbatim fidelity — 30 random notes, byte-exact

- `17:54:32`     NOT VERBATIM FEDFUNDS: 'THERE ARE TWO MAIN ECONOMIC POLICIES: ECONOMIC AND FISCAL: \r'
- `17:54:32`     NOT VERBATIM FEDFUNDS: 'The united states can either pump and drain global liquidity'
- `17:54:32`     NOT VERBATIM MOVE: 'MOVE >150 is a symptom of Fed losing control.'
- `17:54:32`     NOT VERBATIM DXY: '5% US 10 YEAR TREASURY YIELDS ARE THE RISK BEARING LOAD FOR '
- `17:54:32`     NOT VERBATIM VIX: 'The Cboe Volatility Index (VIX) is a real-time index that re'
- `17:54:32`     NOT VERBATIM ECONOMICS:USRI: 'The Redbook Index is a sales-weighted index of year-over-yea'
- `17:54:32`     NOT VERBATIM SOFR: 'Monitoring intraday SOFR (Secured Overnight Financing Rate) '
- `17:54:32`     NOT VERBATIM RBUSBIS: 'The Global system (global economy, global trade and global f'
- `17:54:32`     NOT VERBATIM DTWEXBGS: 'Bull and Bear Markets are the result of the Central Banks po'
- `17:54:32`     NOT VERBATIM YIT1!: 'More Dollars are created every single day outside of America'
- `17:54:32`     NOT VERBATIM ECONOMICS:DEIRYY: 'Inflation can have a significant impact on currency values. '
- `17:54:32`     NOT VERBATIM FRED:BAMLH0A0HYM2SYTW: 'Credit spreads are always positive: for corporations to attr'
- `17:54:32`     NOT VERBATIM FEDFUNDS: 'THE DESIGN OF THE SYSTEM DETERMINES THAT THE PREPHERY ECONOM'
- `17:54:32`     NOT VERBATIM UGA: 'The United States Gasoline Fund, LP (USO) is an exchange-tra'
- `17:54:32`     NOT VERBATIM AMEX:KRE: 'Utilities rally is money wants to move into a defensive sect'
- `17:54:32`     NOT VERBATIM FRED:FEDFUNDS: 'GEORGE SOROS IMPERIAL CIRCLE: A STRONG DOLLAR WOULD CAUSE A '
- `17:54:32`     NOT VERBATIM FEDFUNDS: 'What is the Milkshake theory? Based on the design of the mon'
- `17:54:32`     NOT VERBATIM FRED:RBUSBIS: 'Manufacturing PMI "3M" predicts forex currency future moves '
## C. truncation detector

- `17:54:32` ✗   workbench is v1.1 full-fidelity
- `17:54:32` ✗   every watchlisted symbol carries ALL its notes
- `17:54:32` ✗   30/30 sampled notes byte-exact
- `17:54:32` ✅   zero notes hitting the cap
- `17:54:32` ✗ FAILED: ['workbench is v1.1 full-fidelity', 'every watchlisted symbol carries ALL its notes', '30/30 sampled notes byte-exact']
