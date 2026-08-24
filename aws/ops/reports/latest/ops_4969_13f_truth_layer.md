# ops 4969 -- 13F truth-layer

**Status:** failure  
**Duration:** 233.2s  
**Finished:** 2026-08-24T19:50:15+00:00  

## Error

```
SystemExit: 1
```

## Log
- `19:46:21` mark 2026-08-24T19:46:21+00:00
- `19:46:21` G-1 PASS
- `19:46:21` P0 live cusip-map claimants for ['CPAY', 'IBIA', 'ICLN', 'MOBL', 'ORCL']
- `19:46:22`   CPAY  219948106 src=sec          CORPAY INC
- `19:46:22`   IBIA  464286822 src=fmp          iShares Trust
- `19:46:22`   ICLN  464288224 src=figi         ISHARES GLOBAL CLEAN ENERGY
- `19:46:22`   ORCL  68389X105 src=sec          ORACLE CORP
- `19:46:22`   ORCL  68389X204 src=sec          ORACLE CORP
- `19:46:22` P0 feed BEFORE: residual=['AACB', 'AACI', 'AACO', 'AACP', 'ABLV', 'ACGCU', 'ACHR', 'ADAC'] famous_raw_exits=0 max_cov=2027.1 poison_rows=34
- `19:46:23` P0 brief: current err=['error', 'fallback']; good history at data/ai-commentary/history/13f/2026-08-06.json
- `19:46:28` P0b seeded 23/23 ETF identities into the live map
- `19:46:29` P0c ledger industries 115 -> 79 (pruned 36 cache-fill marks)
- `19:46:29`   settle justhodl-13f-positions OK (0s)
- `19:46:53`   settle justhodl-page-ai-commentary OK (25s)
- `19:46:53` G0 PASS
- `19:50:06`   G2 fresh              PASS
- `19:50:06`   G2 poison_gone        FAIL
- `19:50:06`   G2 residual_clean     PASS
- `19:50:06`   G2 exits_improved     PASS
- `19:50:06`   G2 coverage<=115      PASS
- `19:50:06`   G2 ni_new_backed      FAIL
- `19:50:06`   G2 honeywell_stamped  PASS
- `19:50:06`   poison 34->10 · residual ['AACB', 'AACI', 'AACO', 'AACP', 'ABLV', 'ACGCU', 'ACHR', 'ADAC'] · famous_raw_exits 0->0 · max_cov 2027.1->100.0 · corp_action_rows=14 · ni_bad=['Marine Shipping']
- `19:50:06`     POISON ('CPAY', 'PG&E CORP')
- `19:50:06`     POISON ('CPAY', 'PG&E CORP')
- `19:50:06`     POISON ('CPAY', 'THE ODP CORP')
- `19:50:06`     POISON ('CPAY', 'THE ODP CORP')
- `19:50:06`     POISON ('CPAY', 'THE ODP CORP')
- `19:50:06`     POISON ('CPAY', 'PG&E CORP')
- `19:50:14` G3 PASS — brief has content (preserved_from=2026-08-06T14:02:06.989752+00:00)
- `19:50:14`   justhodl.ai serving spotlight guard (try 1)
- `19:50:15`   proxy brief non-error: True
- `19:50:15` G4 PASS
- `19:50:15` ops 4969 RED: G2
