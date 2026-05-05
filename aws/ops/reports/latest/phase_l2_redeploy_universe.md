
# 1) Verify source has AI_SUPPLY_CHAIN_SEED wired

- `23:46:52`     AI_SUPPLY_CHAIN_SEED defined: True
- `23:46:52`     Wired into gather_seeds: True

# 2) Force-deploy

- `23:46:56`     ✓ deployed at 2026-05-05T23:46:53.000+0000

# 3) Force-invoke

- `23:47:08`     status: 200
- `23:47:08`     body: {"n_total": 563, "duration_s": 10.5}
- `23:47:08`       START RequestId: 932ce15d-fd56-4121-84c8-844e9d857d0f Version: $LATEST
- `23:47:08`       [universe] starting v2.0, min_mcap=$0.10B, max_enrich=2400
- `23:47:08`       [universe] seeds after curated lists: 965
- `23:47:08`       [universe] seeds after screener: 1183
- `23:47:08`       [universe] seeds after 13F: 1193
- `23:47:08`       [universe] enriching 1193 candidates with 20 workers...
- `23:47:08`       [universe] enriched: 563 stocks, statuses: {'ok': 563, 'filtered': 630, 'deadline': 0}
- `23:47:08`       [universe] runtime: 10.5s
- `23:47:08`       [universe] wrote 174,742b to data/universe.json
- `23:47:08`       END RequestId: 932ce15d-fd56-4121-84c8-844e9d857d0f
- `23:47:08`       REPORT RequestId: 932ce15d-fd56-4121-84c8-844e9d857d0f	Duration: 10609.34 ms	Billed Duration: 11135 ms	Memory Size: 1024 MB	Max Memory Used: 161 MB	Init Duration: 525.23 ms

# 4) Verify pump-list coverage

- `23:47:08`     total stocks: 563
- `23:47:08`   
- `23:47:08`     ── pump-list coverage ──
- `23:47:08`       ✓ AXTI   $  4987M  Technology
- `23:47:08`       ✓ LWLG   $  2518M  Basic Materials
- `23:47:08`       ✓ AAOI   $ 14255M  Technology
- `23:47:08`       ✓ AEHR   $  2868M  Technology
- `23:47:08`       ❌ SNDK    MISSING
- `23:47:08`       ✓ ICHR   $  2355M  Technology
- `23:47:08`       ✓ MRVL   $147565M  Technology
- `23:47:08`       ✓ INTC   $543562M  Technology
- `23:47:08`       ❌ VIAV    MISSING
- `23:47:08`       ✓ LITE   $ 71012M  Technology
- `23:47:08`       ✓ CRDO   $ 35704M  Technology
- `23:47:08`       ✓ MU     $721973M  Technology
- `23:47:08`       ❌ TER     MISSING
- `23:47:08`       ❌ WOLF    MISSING
- `23:47:08`       ✓ ON     $ 40383M  Technology
- `23:47:08`       ✓ QRVO   $  8932M  Technology
- `23:47:08`       ✓ COHR   $ 53238M  Technology
- `23:47:08`       ❌ FN      MISSING
- `23:47:08`   
- `23:47:08`     Coverage: 13/18 = 72%