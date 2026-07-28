# ops 4036 — note-fidelity: exactly as in TradingView

**Status:** failure  
**Duration:** 17.9s  
**Finished:** 2026-07-28T18:00:24+00:00  

## Error

```
SystemExit: 1
```

## Data

| byte_exact | count_mismatches | notes_at_cap | orphan_symbols_not_in_any_list | sampled | symbols_with_notes | symbols_with_tv_source | tagged_symbols | unique_symbols | watchlisted_with_notes | watchlists |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 771 | 0 |  | 10319 |  | 491 |
|  | 0 |  | 25 |  |  |  | 1029 |  | 1004 |  |
| 29 |  |  |  | 30 |  |  |  |  |  |  |
|  |  | 147 |  |  |  |  |  |  |  |  |

## Log
- `18:00:24` ✅   v1.1.1 artifact after ~15s
## A. count parity per symbol (watchlisted only)

- `18:00:24`     orphans (INFO): ['UNTAGGED', 'XDN', 'NASDAQ:XDN', 'MC', 'EURONEXT:MC', 'AKZA']
## B. verbatim fidelity — 30 random notes, byte-exact

- `18:00:24`     NOT VERBATIM DXY: 'A rising yield is dollar bullish. A falling yield is dollar '
## C. truncation detector

- `18:00:24` ✅   workbench is v1.1.1 bare-union
- `18:00:24` ✅   every watchlisted symbol carries ALL its notes
- `18:00:24` ✗   30/30 sampled notes byte-exact
- `18:00:24` ✗   zero notes hitting the cap
- `18:00:24` ✗ FAILED: ['30/30 sampled notes byte-exact', 'zero notes hitting the cap']
