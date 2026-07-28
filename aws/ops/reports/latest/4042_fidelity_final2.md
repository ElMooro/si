# ops 4036 — note-fidelity: exactly as in TradingView

**Status:** success  
**Duration:** 15.7s  
**Finished:** 2026-07-28T18:11:25+00:00  

## Data

| byte_exact | count_mismatches | heaviest | notes_at_cap | orphan_symbols_not_in_any_list | sampled | symbols_with_notes | symbols_with_tv_source | tagged_symbols | unique_symbols | watchlisted_with_notes | watchlists |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 771 | 0 |  | 10319 |  | 491 |
|  | 0 |  |  | 25 |  |  |  | 1029 |  | 1004 |  |
| 30 |  |  |  |  | 30 |  |  |  |  |  |  |
|  |  | [["CAPITALCOM:DXY", 140], ["TVC:NDQ/TVC:DXY", 140], ["TVC:DXY", 140], ["TVC:CN10Y/TVC:DXY", 140], ["ICEUS:DX1!-TVC:DXY", 140]] | 0 |  |  |  |  |  |  |  |  |

## Log
- `18:11:25` ✅   v1.1.3 artifact after ~15s
## A. count parity per symbol (watchlisted only)

- `18:11:25`     orphans (INFO): ['UNTAGGED', 'XDN', 'NASDAQ:XDN', 'MC', 'EURONEXT:MC', 'AKZA']
## B. verbatim fidelity — 30 random notes, byte-exact

## C. truncation detector

- `18:11:25` ✅   workbench is v1.1.3 bare-union
- `18:11:25` ✅   every watchlisted symbol carries ALL its notes
- `18:11:25` ✅   30/30 sampled notes byte-exact
- `18:11:25` ✅   zero notes hitting the cap
- `18:11:25` ✅ PASS_ALL — 1004 watchlisted symbols carry all their notes verbatim; 25 orphans (symbols in no list) reported honestly; zero truncation
