# ops 4332 -- four bugs, one wave

**Status:** success  
**Duration:** 170.3s  
**Finished:** 2026-08-03T20:22:40+00:00  

## Log
## 1. liquidity-flow

- `20:20:40` ✅ artifact generated_at=2026-08-03T20:20:39+00:00
## 2. pump-radar plain

- `20:21:37` first bytes: 7b227363
- `20:21:37` ✅ parses as JSON · keys ['schema_version', 'generated_at', 'elapsed_sec', 'sources_freshness', 'sources_loaded', 'conviction']
## 3. ai-rerating squeeze provenance

- `20:22:40` [shrt] source=data/finra-short.json n=25 max=75.0
- `20:22:40` ✅ squeeze map loads with provenance
## 4. opportunity-engine basis (soft)

- `20:22:40` code-level fix deployed; outgrow_basis field will show fwd_vs_fwd|fwd_vs_trailing on next scheduled run -- auditor's ZERO_SCOPE watches the artifact
- `20:22:40` ✅ OPS 4332 PASS -- the queue shrinks with receipts
