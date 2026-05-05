
# 0) Read current universe builder source

- `23:42:05`     size: 12655 chars
- `23:42:05`     current MIN_MCAP env default: $300M

# 1) Patch source: lower MIN_MCAP + expand seed list

- `23:42:05`     ✓ lowered MIN_MCAP default to $100M
- `23:42:05`     ✓ added AI_SUPPLY_CHAIN_SEED list (~80 microcap semi/AI names)
- `23:42:05`     wrote 13638 chars
- `23:42:05`     ✓ valid python syntax

# 2) Force-deploy universe builder

- `23:42:08`     ✓ deployed at 2026-05-05T23:42:07.000+0000
- `23:42:08`     env MIN_MCAP: 100000000

# 3) Force-invoke universe builder

- `23:42:18`     status: 200, dur: 10.4s
- `23:42:18`     body: {"n_total": 338, "duration_s": 9.5}
- `23:42:18`       START RequestId: b0cf4c75-2d67-4b92-8527-df5af71d894c Version: $LATEST
- `23:42:18`       [universe] starting v2.0, min_mcap=$0.10B, max_enrich=2400
- `23:42:18`       [universe] seeds after curated lists: 929
- `23:42:18`       [universe] seeds after screener: 1158
- `23:42:18`       [universe] seeds after 13F: 1168
- `23:42:18`       [universe] enriching 1168 candidates with 20 workers...
- `23:42:18`       [universe] enriched: 338 stocks, statuses: {'ok': 338, 'filtered': 830, 'deadline': 0}
- `23:42:18`       [universe] runtime: 9.5s
- `23:42:18`       [universe] wrote 105,098b to data/universe.json
- `23:42:18`       END RequestId: b0cf4c75-2d67-4b92-8527-df5af71d894c
- `23:42:18`       REPORT RequestId: b0cf4c75-2d67-4b92-8527-df5af71d894c	Duration: 9619.39 ms	Billed Duration: 10240 ms	Memory Size: 1024 MB	Max Memory Used: 161 MB	Init Duration: 620.57 ms

# 4) Verify universe expanded

- `23:42:19`     total stocks: 338
- `23:42:19`   
- `23:42:19`     ── coverage of pump-list names in expanded universe ──
- `23:42:19`       AXTI   ❌ MISSING
- `23:42:19`       LWLG   ❌ MISSING
- `23:42:19`       AAOI   ❌ MISSING
- `23:42:19`       AEHR   ❌ MISSING
- `23:42:19`       SNDK   ❌ MISSING
- `23:42:19`       ICHR   ❌ MISSING
- `23:42:19`       MRVL   ❌ MISSING
- `23:42:19`       INTC   ✓ present
- `23:42:19`       VIAV   ❌ MISSING
- `23:42:19`       LITE   ❌ MISSING
- `23:42:19`       CRDO   ❌ MISSING
- `23:42:19`       MU     ❌ MISSING
- `23:42:19`       TER    ❌ MISSING
- `23:42:19`       WOLF   ❌ MISSING
- `23:42:19`       ON     ❌ MISSING
- `23:42:19`       QRVO   ❌ MISSING