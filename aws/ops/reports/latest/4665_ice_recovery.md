# ops 4665 — ICE BofA recovery + route probes

**Status:** success  
**Duration:** 18.2s  
**Finished:** 2026-08-14T20:13:46+00:00  

## Log
## 1. RECOVER 3 series from verified archives

- `20:13:33`   located 3/3 docs in 5s: {'BAMLC0A0CM': 'data/warm/fred-scoped/Interest_Rates/BAMLC0A0CM.json', 'BAMLC0A4CBBB': 'data/warm/fred-scoped/Interest_Rates/BAMLC0A4CBBB.json', 'BAMLH0A0HYM2': 'data/warm/fred-scoped/Interest_Rates/BAMLH0A0HYM2.json'}
- `20:13:34`   BAMLC0A0CM: archive=7638 overlap=695 splice-mm=0 recovered=6943 -> doc now 7729 obs since 1996-12-31
- `20:13:34` ✅   [BAMLC0A0CM] full lineage 1996-12-31 -> present, splice clean
- `20:13:35`   BAMLC0A4CBBB: archive=7639 overlap=696 splice-mm=0 recovered=6943 -> doc now 7730 obs since 1996-12-31
- `20:13:35` ✅   [BAMLC0A4CBBB] full lineage 1996-12-31 -> present, splice clean
- `20:13:35`   BAMLH0A0HYM2: archive=7639 overlap=696 splice-mm=0 recovered=6943 -> doc now 7730 obs since 1996-12-31
- `20:13:35` ✅   [BAMLH0A0HYM2] full lineage 1996-12-31 -> present, splice clean
## 2. ALFRED — settled by 4664: window enforced retroactively across vintages (rolling, front at 2023-08-15 today); loophole CLOSED

- `20:13:36`   vintage 2025-06-02: first obs 2023-08-15 (count field 477) -> window enforced retroactively
- `20:13:37`   vintage 2026-03-02: first obs 2023-08-15 (count field 676) -> window enforced retroactively
## 3. PROBE — DBnomics FRED mirror

- `20:13:41`   BAMLH0A0HYM2 search: []
- `20:13:43`   BAMLC0A0CM search: []
- `20:13:45`   BAMLEMRLCRPILAOAS search: []
## 4. PROBE — TradingView vault BAML coverage

- `20:13:46` ⚠   vault probe: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does
## verdict

- `20:13:46` ✅ 3/3 recovered to 1996 with clean splices; probes on record for the other 189; engine merge-guard makes the recovered rows permanent
