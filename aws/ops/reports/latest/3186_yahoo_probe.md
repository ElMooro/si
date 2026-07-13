# ops 3186 — probe the free path before spending

**Status:** success  
**Duration:** 84.8s  
**Finished:** 2026-07-13T01:22:02+00:00  

## Error

```
SystemExit: 0
```

## Data

| n_fails | n_warns | symbols_free | symbols_not_worth_buying | symbols_vendor_helps | verdict |
|---|---|---|---|---|---|
|  |  | 246 | 654 | 418 |  |
| 0 | 0 |  |  |  | PASS |

## Log
## 1. Real fetches through the FREE path (sampled)

- `01:20:38`   FTSE            448 symbols · free-path hit   0%  
- `01:20:38`   INTOTHEBLOCK    147 symbols · free-path hit   0%  
- `01:20:38`   USI             108 symbols · free-path hit   0%  
- `01:20:38`   GLASSNODE        59 symbols · free-path hit   0%  
- `01:20:38`   COT3             54 symbols · free-path hit   0%  
- `01:20:43`   SSE              52 symbols · free-path hit  17%  SSE:000001→000001.SS (2796 obs, 2015–2026)
- `01:20:47`   SWB              46 symbols · free-path hit  33%  SWB:B500→B500.DE (2140 obs, 2018–2026); SWB:BBCK→BBCK.DE (2926 o
- `01:20:49`   LSE              43 symbols · free-path hit 100%  LSE:0E3R→0E3R.L (373 obs, 2018–2026); LSE:0LMD→0LMD.L (1356 obs,
- `01:20:49`   CBOEEU           40 symbols · free-path hit   0%  
- `01:20:49`   EUREX            39 symbols · free-path hit   0%  
- `01:20:54`   EURONEXT         37 symbols · free-path hit   0%  
- `01:20:54`   ICEEUR           34 symbols · free-path hit   0%  
- `01:20:57`   TRADEGATE        29 symbols · free-path hit  50%  TRADEGATE:36B7→36B7.DE (1831 obs, 2019–2026); TRADEGATE:BBCK→BBC
- `01:21:00`   HKEX             15 symbols · free-path hit  50%  HKEX:2039→2039.HK (2835 obs, 2015–2026); HKEX:2819→2819.HK (541 
- `01:21:03`   SIX              15 symbols · free-path hit  50%  SIX:C8300P→C8300P.SW (244 obs, 2015–2015); SIX:GCVB→GCVB.SW (289
- `01:21:06`   TSX              15 symbols · free-path hit  67%  TSX:BND→BND.TO (2685 obs, 2015–2026); TSX:FLCI→FLCI.TO (2134 obs
- `01:21:09`   XETR             15 symbols · free-path hit  50%  XETR:BBCK→BBCK.DE (2926 obs, 2015–2026); XETR:DAX→DAX.DE (2926 o
- `01:21:14`   BER              14 symbols · free-path hit   0%  
- `01:21:17`   FWB              14 symbols · free-path hit  33%  FWB:B500→B500.DE (2140 obs, 2018–2026); FWB:DX2Z→DX2Z.DE (2923 o
- `01:21:20`   MIL              14 symbols · free-path hit 100%  MIL:AHYE→AHYE.MI (2096 obs, 2018–2026); MIL:B500→B500.MI (2906 o
- `01:21:23`   GETTEX           10 symbols · free-path hit  33%  GETTEX:BNP→BNP.DE (2928 obs, 2015–2026); GETTEX:DX2Z→DX2Z.DE (29
- `01:21:26`   BMV               9 symbols · free-path hit 100%  BMV:EFNL→EFNL.MX (2868 obs, 2015–2026); BMV:EMHY→EMHY.MX (2877 o
- `01:21:30`   MUN               8 symbols · free-path hit  17%  MUN:0BYQ→0BYQ.MU (2108 obs, 2015–2026)
- `01:21:34`   SZSE              8 symbols · free-path hit  50%  SZSE:002475→002475.SZ (2796 obs, 2015–2026); SZSE:300077→300077.
- `01:21:37`   TWSE              7 symbols · free-path hit  50%  TWSE:00728→00728.TW (2008 obs, 2018–2026); TWSE:2330→2330.TW (28
- `01:21:41`   TSE               6 symbols · free-path hit  33%  TSE:1497→1497.T (2187 obs, 2017–2026); TSE:8032→8032.T (2837 obs
- `01:21:44`   KRX               5 symbols · free-path hit   0%  
- `01:21:48`   SGX               5 symbols · free-path hit   0%  
- `01:21:50`   DUS               4 symbols · free-path hit   0%  
- `01:21:53`   NSE               4 symbols · free-path hit   0%  
- `01:21:55`   OMXCOP            3 symbols · free-path hit   0%  
- `01:21:57`   BME               2 symbols · free-path hit  50%  BME:AENA→AENA.MC (2919 obs, 2015–2026)
- `01:21:58`   OMXHEX            2 symbols · free-path hit   0%  
- `01:21:59`   VIE               2 symbols · free-path hit   0%  
- `01:22:00`   BSE               1 symbols · free-path hit   0%  
- `01:22:00`   IDX               1 symbols · free-path hit 100%  IDX:XISB→XISB.JK (2277 obs, 2017–2026)
- `01:22:01`   LSIN              1 symbols · free-path hit 100%  LSIN:0E41→0E41.L (450 obs, 2018–2026)
- `01:22:01`   NZX               1 symbols · free-path hit 100%  NZX:AGG→AGG.NZ (1778 obs, 2019–2026)
- `01:22:02`   OMXSTO            1 symbols · free-path hit   0%  
## 2. Verdict per bucket

- `01:22:02` ── FREE (do not pay):
- `01:22:02`   ✅ USI           108 symbols — already COMPUTED by justhodl-market-internals ($0)
- `01:22:02`   ✅ COT3           54 symbols — CFTC is free; justhodl-cot-tracker already owns this
- `01:22:02`   ✅ LSE            43 symbols — Yahoo covers it (100% of sample)
- `01:22:02`   ✅ TSX            15 symbols — Yahoo covers it (67% of sample)
- `01:22:02`   ✅ MIL            14 symbols — Yahoo covers it (100% of sample)
- `01:22:02`   ✅ BMV             9 symbols — Yahoo covers it (100% of sample)
- `01:22:02`   ✅ IDX             1 symbols — Yahoo covers it (100% of sample)
- `01:22:02`   ✅ LSIN            1 symbols — Yahoo covers it (100% of sample)
- `01:22:02`   ✅ NZX             1 symbols — Yahoo covers it (100% of sample)
- `01:22:02` ── VENDOR WOULD HELP:
- `01:22:02`   💰 SSE            52 symbols — free path only 17% — EODHD would genuinely add these
- `01:22:02`   💰 SWB            46 symbols — free path only 33% — EODHD would genuinely add these
- `01:22:02`   💰 CBOEEU         40 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 EUREX          39 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 EURONEXT       37 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 ICEEUR         34 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 TRADEGATE      29 symbols — free path only 50% — EODHD would genuinely add these
- `01:22:02`   💰 HKEX           15 symbols — free path only 50% — EODHD would genuinely add these
- `01:22:02`   💰 SIX            15 symbols — free path only 50% — EODHD would genuinely add these
- `01:22:02`   💰 XETR           15 symbols — free path only 50% — EODHD would genuinely add these
- `01:22:02`   💰 BER            14 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 FWB            14 symbols — free path only 33% — EODHD would genuinely add these
- `01:22:02`   💰 GETTEX         10 symbols — free path only 33% — EODHD would genuinely add these
- `01:22:02`   💰 MUN             8 symbols — free path only 17% — EODHD would genuinely add these
- `01:22:02`   💰 SZSE            8 symbols — free path only 50% — EODHD would genuinely add these
- `01:22:02`   💰 TWSE            7 symbols — free path only 50% — EODHD would genuinely add these
- `01:22:02`   💰 TSE             6 symbols — free path only 33% — EODHD would genuinely add these
- `01:22:02`   💰 KRX             5 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 SGX             5 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 DUS             4 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 NSE             4 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 OMXCOP          3 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 BME             2 symbols — free path only 50% — EODHD would genuinely add these
- `01:22:02`   💰 OMXHEX          2 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 VIE             2 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 BSE             1 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02`   💰 OMXSTO          1 symbols — free path only 0% — EODHD would genuinely add these
- `01:22:02` ── NOT WORTH BUYING:
- `01:22:02`   ❌ FTSE          448 symbols — FTSE Russell licensed INDEX product — not exchange data; EODHD's EOD tier likely does NOT carry it
- `01:22:02`   ❌ INTOTHEBLOCK  147 symbols — on-chain — Glassnode API is $799+/mo; not worth 1-2 engines
- `01:22:02`   ❌ GLASSNODE      59 symbols — on-chain — Glassnode API is $799+/mo; not worth 1-2 engines
- `01:22:02` ✅ 418 symbols would come from EODHD's EOD tier
