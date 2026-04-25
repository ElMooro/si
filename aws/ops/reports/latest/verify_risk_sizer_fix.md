# Verify Phase 3 — risk-sizer post-fix

**Status:** success  
**Duration:** 2.0s  
**Finished:** 2026-04-25T16:23:06+00:00  

## Data

| invoke_s | n_clusters | n_ideas | n_warnings | regime | total_size_pct |
|---|---|---|---|---|---|
| 1.6 | 17 | 17 | 0 | NEUTRAL | 74.97 |

## Log
## 1. Confirm Lambda is updated

- `16:23:04`   LastModified: 2026-04-25T16:22:27.000+0000
- `16:23:04`   CodeSha256: DCU6UrQUg6A+W/fy...
## 2. Test invoke

- `16:23:06` ✅   Invoked in 1.6s
- `16:23:06` 
  Response body:
- `16:23:06`     regime                    NEUTRAL
- `16:23:06`     max_gross_exposure_pct    75.0
- `16:23:06`     current_drawdown_pct      0.0
- `16:23:06`     drawdown_multiplier       1.0
- `16:23:06`     n_ideas                   17
- `16:23:06`     n_clusters                17
- `16:23:06`     total_size_pct            74.97
- `16:23:06`     n_warnings                0
- `16:23:06`     top_5_sized               [{'symbol': 'INCY', 'size_pct': 4.41}, {'symbol': 'FSLR', 'size_pct': 4.41}, {'symbol': 'RMD', 'size_pct': 4.41}, {'symbol': 'DECK', 'size_pct': 4.41}, {'symbol': 'PTC', 'size_pct': 4.41}]
## 3. Read risk/recommendations.json — full report

- `16:23:06`   Regime: NEUTRAL (strength 57.9)
- `16:23:06`   Max gross exposure: 75.0%
- `16:23:06`   Drawdown: 0.0% (peak: None)
- `16:23:06`   DD multiplier: ×1.0  (no trigger)
- `16:23:06`   Candidate ideas: 17
- `16:23:06`   Clusters: 17
- `16:23:06`   Pre-cap signal sum: 136.0%
- `16:23:06`   Total recommended (after caps): 74.97%
- `16:23:06` 
  Top 10 sized recommendations:
- `16:23:06`     INCY   Healthcare     size= 4.41%  conv=0.830  cluster=isolated_INCY     
- `16:23:06`       → Phase2B 4/4 dims (composite 93.3) | gross cap: ×0.55
- `16:23:06`     FSLR   Energy         size= 4.41%  conv=0.817  cluster=isolated_FSLR     
- `16:23:06`       → Phase2B 4/4 dims (composite 89.1) | gross cap: ×0.55
- `16:23:06`     RMD    Healthcare     size= 4.41%  conv=0.754  cluster=isolated_RMD      
- `16:23:06`       → Phase2B 3/4 dims (composite 84.7) | gross cap: ×0.55
- `16:23:06`     DECK   Consumer Cycli size= 4.41%  conv=0.748  cluster=isolated_DECK     
- `16:23:06`       → Phase2B 3/4 dims (composite 82.8) | gross cap: ×0.55
- `16:23:06`     PTC    Technology     size= 4.41%  conv=0.746  cluster=isolated_PTC      
- `16:23:06`       → Phase2B 3/4 dims (composite 82.1) | gross cap: ×0.55
- `16:23:06`     DXCM   Healthcare     size= 4.41%  conv=0.745  cluster=isolated_DXCM     
- `16:23:06`       → Phase2B 3/4 dims (composite 81.8) | gross cap: ×0.55
- `16:23:06`     FOXA   Communication  size= 4.41%  conv=0.745  cluster=isolated_FOXA     
- `16:23:06`       → Phase2B 3/4 dims (composite 81.8) | gross cap: ×0.55
- `16:23:06`     FOX    Communication  size= 4.41%  conv=0.745  cluster=isolated_FOX      
- `16:23:06`       → Phase2B 3/4 dims (composite 81.8) | gross cap: ×0.55
- `16:23:06`     TTD    Technology     size= 4.41%  conv=0.734  cluster=isolated_TTD      
- `16:23:06`       → Phase2B 3/4 dims (composite 77.9) | gross cap: ×0.55
- `16:23:06`     JKHY   Technology     size= 4.41%  conv=0.733  cluster=isolated_JKHY     
- `16:23:06`       → Phase2B 3/4 dims (composite 77.7) | gross cap: ×0.55
- `16:23:06` 
  Cluster summary (top 8 by size):
- `16:23:06`     isolated_INCY             size=1 avg_corr=0
- `16:23:06`     isolated_FSLR             size=1 avg_corr=0
- `16:23:06`     isolated_RMD              size=1 avg_corr=0
- `16:23:06`     isolated_DECK             size=1 avg_corr=0
- `16:23:06`     isolated_PTC              size=1 avg_corr=0
- `16:23:06`     isolated_DXCM             size=1 avg_corr=0
- `16:23:06`     isolated_FOXA             size=1 avg_corr=0
- `16:23:06`     isolated_FOX              size=1 avg_corr=0
## 4. Verify EventBridge schedule

- `16:23:06` ⚠   Rule check: An error occurred (ResourceNotFoundException) when calling the DescribeRule operation: Rule justhodl-risk-sizer-daily does not exist on EventBus default.
- `16:23:06` Done
