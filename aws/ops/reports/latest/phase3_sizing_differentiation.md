# Phase 3 — composite-score-weighted sizing

**Status:** success  
**Duration:** 8.6s  
**Finished:** 2026-04-25T18:45:38+00:00  

## Data

| size_max | size_min | spread | total |
|---|---|---|---|
| 5.15% | 3.39% | 1.76% | 75.01 |

## Log
## 1. Patch sizing logic to weight by composite_score

- `18:45:29` ✅   Patched sizing logic with composite-score weighting
## 2. Validate + write

- `18:45:29` ✅   Syntax OK
## 3. Deploy

- `18:45:33` ✅   Deployed (21,227B)
## 4. Test invoke

- `18:45:38` ✅   Invoked in 1.5s
- `18:45:38`   Total size: 75.01%
## 5. Verify sizing now differentiates by quality

- `18:45:38`   Size range: 3.39% - 5.15% (spread 1.76%)
- `18:45:38` ✅   ✅ Sizing now differentiates (1.76% spread)
- `18:45:38` 
  Top 10 (sorted by size):
- `18:45:38`     FSLR   comp= 89.1 weight=1.151 size= 5.15% cluster=sector_energy
- `18:45:38`     INCY   comp= 93.3 weight=1.205 size= 5.02% cluster=sector_healthcare
- `18:45:38`     DECK   comp= 82.8 weight= 1.07 size= 4.79% cluster=sector_consumer_cycl
- `18:45:38`     PTC    comp= 82.1 weight=1.061 size= 4.75% cluster=sector_technology
- `18:45:38`     FOXA   comp= 81.8 weight=1.057 size= 4.72% cluster=sector_communication
- `18:45:38`     FOX    comp= 81.8 weight=1.057 size= 4.72% cluster=sector_communication
- `18:45:38`     RMD    comp= 84.7 weight=1.094 size= 4.56% cluster=sector_healthcare
- `18:45:38`     TTD    comp= 77.9 weight=1.006 size=  4.5% cluster=sector_technology
- `18:45:38`     JKHY   comp= 77.7 weight=1.004 size= 4.49% cluster=sector_technology
- `18:45:38`     DXCM   comp= 81.8 weight=1.057 size=  4.4% cluster=sector_healthcare
- `18:45:38` Done
