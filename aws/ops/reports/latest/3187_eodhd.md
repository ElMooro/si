# ops 3187 — does the EODHD token earn its keep?

**Status:** success  
**Duration:** 87.2s  
**Finished:** 2026-07-13T01:26:25+00:00  

## Error

```
SystemExit: 0
```

## Data

| aapl_obs | n_fails | n_warns | symbols_eodhd_delivers | symbols_still_missing | token_live | verdict |
|---|---|---|---|---|---|---|
| 2896 |  |  |  |  | True |  |
|  |  |  | 133 | 732 |  |  |
|  | 0 | 0 |  |  |  | PASS |

## Log
## 1. Store the key + prove it works

- `01:24:58` ✅ SSM /justhodl/eodhd-api-key set (single source of truth)
- `01:24:59` ✅ token LIVE — AAPL.US 2896 obs (2015-01-02 → 2026-07-10)
## 2. THE BUCKETS THAT FAILED FOR FREE — does EODHD have them?

- `01:25:03`   FTSE         448 symbols · EODHD hit   0%  
- `01:25:10`   EURONEXT      37 symbols · EODHD hit  60%  EURONEXT:AEX→AEX.LSE (2910 obs, 2015–2026); EURONEXT:AGEB→AGEB
- `01:25:15`   BER           14 symbols · EODHD hit  40%  BER:0252→0252.KLSE (989 obs, 2022–2026); BER:0255→0255.KLSE (9
- `01:25:20`   SSE           52 symbols · EODHD hit  60%  SSE:000001→000001.SHG (2796 obs, 2015–2026); SSE:000028→000028
- `01:25:24`   SWB           46 symbols · EODHD hit  20%  SWB:B500→B500.F (2844 obs, 2015–2026)
- `01:25:29`   XETR          15 symbols · EODHD hit  80%  XETR:4RT6→4RT6.XETRA (2923 obs, 2015–2026); XETR:BBCK→BBCK.XET
- `01:25:34`   FWB           14 symbols · EODHD hit  40%  FWB:B500→B500.F (2844 obs, 2015–2026); FWB:DX2Z→DX2Z.F (2920 o
- `01:25:38`   TRADEGATE     29 symbols · EODHD hit  40%  TRADEGATE:36B7→36B7.F (1830 obs, 2019–2026); TRADEGATE:BBCK→BB
- `01:25:42`   GETTEX        10 symbols · EODHD hit  40%  GETTEX:BNP→BNP.F (2919 obs, 2015–2026); GETTEX:DX2Z→DX2Z.F (29
- `01:25:46`   CBOEEU        40 symbols · EODHD hit   0%  
- `01:25:50`   EUREX         39 symbols · EODHD hit   0%  
- `01:25:54`   ICEEUR        34 symbols · EODHD hit   0%  
- `01:25:58`   SIX           15 symbols · EODHD hit  40%  SIX:C1000P→C1000P.SW (168 obs, 2024–2024); SIX:GCVB→GCVB.SW (1
- `01:26:03`   HKEX          15 symbols · EODHD hit 100%  HKEX:2039→2039.HK (2832 obs, 2015–2026); HKEX:2819→2819.HK (22
- `01:26:10`   MIL           14 symbols · EODHD hit  80%  MIL:AHYE→AHYE.PA (2779 obs, 2015–2026); MIL:B500→B500.XETRA (2
- `01:26:14`   LSE           43 symbols · EODHD hit   0%  
## 3. What it actually buys

- `01:26:14`   ✅ SSE           52 symbols (60%)
- `01:26:14`   ✅ EURONEXT      37 symbols (60%)
- `01:26:14`   ✅ XETR          15 symbols (80%)
- `01:26:14`   ✅ HKEX          15 symbols (100%)
- `01:26:14`   ✅ MIL           14 symbols (80%)
- `01:26:14`   ❌ FTSE         448 symbols (0%) — EODHD does NOT carry these either
- `01:26:14`   ❌ SWB           46 symbols (20%) — EODHD does NOT carry these either
- `01:26:14`   ❌ LSE           43 symbols (0%) — EODHD does NOT carry these either
- `01:26:14`   ❌ CBOEEU        40 symbols (0%) — EODHD does NOT carry these either
- `01:26:14`   ❌ EUREX         39 symbols (0%) — EODHD does NOT carry these either
- `01:26:14`   ❌ ICEEUR        34 symbols (0%) — EODHD does NOT carry these either
- `01:26:14`   ❌ TRADEGATE     29 symbols (40%) — EODHD does NOT carry these either
- `01:26:14`   ❌ SIX           15 symbols (40%) — EODHD does NOT carry these either
- `01:26:14`   ❌ BER           14 symbols (40%) — EODHD does NOT carry these either
- `01:26:14`   ❌ FWB           14 symbols (40%) — EODHD does NOT carry these either
- `01:26:14`   ❌ GETTEX        10 symbols (40%) — EODHD does NOT carry these either
- `01:26:14` ⚠ FTSE (448 symbols) NOT covered even with the token — it is FTSE Russell licensed index product, exactly as suspected. His largest bucket stays dark.
## 4. Wire it into the fleet

- `01:26:18` ✅ justhodl-wl-engines: EODHD key armed
- `01:26:21` ✅ justhodl-thesis-engine: EODHD key armed
- `01:26:25` ✅ justhodl-symbol-dictionary: EODHD key armed
