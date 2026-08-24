## provider depth table

**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-08-24T00:22:36+00:00  

## Data

| classified | queue | thin |
|---|---|---|
| 20 | bls,worldbank,imf,boe,coinmetrics,bcb,banxico,boj,snb,frb-ddp,cboe,occ | 12 |

## Log
- `00:22:35`   bls              0 keys      0.00MB obs=None..None  THIN
- `00:22:35`   worldbank        0 keys      0.00MB obs=None..None  THIN
- `00:22:35`   imf              0 keys      0.00MB obs=None..None  THIN
- `00:22:35`   boe              1 keys      0.04MB obs=None..None  THIN
- `00:22:35`   snb              0 keys      0.00MB obs=None..None  THIN
- `00:22:35`   boj              0 keys      0.00MB obs=None..None  THIN
- `00:22:35`   bcb              0 keys      0.00MB obs=None..None  THIN
- `00:22:35`   banxico          1 keys      0.13MB obs=2026-08..2026-08  THIN
- `00:22:35`   dbnomics         0 keys      0.00MB obs=None..None  SCOPED_BY_DESIGN
- `00:22:35`   coinmetrics      0 keys      0.00MB obs=None..None  THIN
- `00:22:35`   cboe             0 keys      0.00MB obs=None..None  THIN
- `00:22:35`   occ              0 keys      0.00MB obs=None..None  THIN
- `00:22:35`   dol-eta          0 keys      0.00MB obs=None..None  FULL
- `00:22:35`   nasa-power       0 keys      0.00MB obs=None..None  SCOPED_BY_DESIGN
- `00:22:35`   frb-ddp          0 keys      0.00MB obs=None..None  THIN
- `00:22:36`   ofr            446 keys      6.48MB obs=2010-11..2026-08  FULL
- `00:22:36`   ofr-hf           0 keys      0.00MB obs=None..None  FULL
- `00:22:36`   gdelt            1 keys      0.03MB obs=None..None  SCOPED_BY_DESIGN
- `00:22:36`   polygon          0 keys      0.00MB obs=None..None  SCOPED_BY_DESIGN
- `00:22:36`   te-feed          1 keys      0.88MB obs=2022-12..2026-09  SCOPED_BY_DESIGN
## DRAIN QUEUE (ranked)

- `00:22:36`    1. bls          0.00MB -- download.bls.gov time.series: full CPI/CES/JOLTS/LAUS/PPI flat files, millions o
- `00:22:36`    2. worldbank    0.00MB -- api.worldbank.org: ~16k indicators x 260 economies x 60y
- `00:22:36`    3. imf          0.00MB -- IMF SDMX: IFS/BOP/DOT/GFS full country x indicator matrices
- `00:22:36`    4. boe          0.04MB -- BoE IADB: thousands of series
- `00:22:36`    5. coinmetrics  0.00MB -- community API: full daily history, all assets x metrics
- `00:22:36`    6. bcb          0.00MB -- BCB SGS: ~30k series
- `00:22:36`    7. banxico      0.13MB -- Banxico SIE: ~30k series
- `00:22:36`    8. boj          0.00MB -- BOJ stat-search: MD/LA/IR/FM/CO db families, thousands
- `00:22:36`    9. snb          0.00MB -- SNB data portal full cube
- `00:22:36`   10. frb-ddp      0.00MB -- DDP packages H15/H8/Z1/G19 full zips
- `00:22:36`   11. cboe         0.00MB -- settlement/volume archives years deep
- `00:22:36`   12. occ          0.00MB -- daily/weekly volume archives
- `00:22:36` PASS classified=20 thin=12 artifact=data/warm/_audit/depth-audit.json
- `00:22:36` ops 4957 GREEN -- the ledger every full-history arc drains; bls-full walker ships in the SAME session
