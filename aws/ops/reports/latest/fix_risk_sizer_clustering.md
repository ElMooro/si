# Fix risk-sizer — sector fallback clustering + schedule

**Status:** success  
**Duration:** 9.0s  
**Finished:** 2026-04-25T16:25:52+00:00  

## Data

| invoke_s | multi_member_clusters | n_clusters | regime | total_size_pct | zip_size |
|---|---|---|---|---|---|
| 1.6 | 5 | 10 | NEUTRAL | 74.97 | 20199 |

## Log
## 1. Read current source from disk

- `16:25:43`   Current source: 17,520B, 464 LOC
## 2. Patch cluster_by_correlation to use sector fallback

- `16:25:43` ✅   Replaced cluster_by_correlation with sector-fallback version
## 3. Patch lambda_handler to pass sector_by_symbol

- `16:25:43` ✅   Patched call site to pass sector_by_symbol
## 4. Validate + write fixed source

- `16:25:43` ✅   Syntax OK; new size: 19,178B
## 5. Deploy fixed Lambda

- `16:25:47` ✅   Deployed (20,199B)
## 6. Test invoke

- `16:25:52` ✅   Invoked in 1.6s
- `16:25:52`   Response body:
- `16:25:52`     regime                    NEUTRAL
- `16:25:52`     max_gross_exposure_pct    75.0
- `16:25:52`     current_drawdown_pct      0.0
- `16:25:52`     drawdown_multiplier       1.0
- `16:25:52`     n_ideas                   17
- `16:25:52`     n_clusters                10
- `16:25:52`     total_size_pct            74.97
- `16:25:52`     n_warnings                0
- `16:25:52`     top_5_sized               [{'symbol': 'INCY', 'size_pct': 4.41}, {'symbol': 'FSLR', 'size_pct': 4.41}, {'symbol': 'RMD', 'size_pct': 4.41}, {'symbol': 'DECK', 'size_pct': 4.41}, {'symbol': 'PTC', 'size_pct': 4.41}]
## 7. Verify sector clustering is producing meaningful clusters

- `16:25:52`   Total clusters: 10
- `16:25:52` 
  Cluster breakdown:
- `16:25:52`     sector_healthcare              size= 3 method=sector       members=['DXCM', 'INCY', 'RMD']
- `16:25:52`     sector_technology              size= 3 method=sector       members=['JKHY', 'PTC', 'TTD']
- `16:25:52`     sector_communication_services  size= 2 method=sector       members=['FOX', 'FOXA']
- `16:25:52`     sector_industrials             size= 2 method=sector       members=['AOS', 'HWM']
- `16:25:52`     sector_financial_services      size= 2 method=sector       members=['CBOE', 'WTW']
- `16:25:52` ✅ 
  ✅ 5 multi-member clusters formed (sector-based)
- `16:25:52` 
  Sized recommendations w/ cluster info:
- `16:25:52`     INCY   sector=Healthcare     size= 4.41%  cluster=sector_healthcare        
- `16:25:52`     FSLR   sector=Energy         size= 4.41%  cluster=sector_energy            
- `16:25:52`     RMD    sector=Healthcare     size= 4.41%  cluster=sector_healthcare        
- `16:25:52`     DECK   sector=Consumer Cycli size= 4.41%  cluster=sector_consumer_cyclical 
- `16:25:52`     PTC    sector=Technology     size= 4.41%  cluster=sector_technology        
- `16:25:52`     DXCM   sector=Healthcare     size= 4.41%  cluster=sector_healthcare        
- `16:25:52`     FOXA   sector=Communication  size= 4.41%  cluster=sector_communication_serv
- `16:25:52`     FOX    sector=Communication  size= 4.41%  cluster=sector_communication_serv
- `16:25:52`     TTD    sector=Technology     size= 4.41%  cluster=sector_technology        
- `16:25:52`     JKHY   sector=Technology     size= 4.41%  cluster=sector_technology        
## 8. Create EventBridge schedule (missing from step 147)

- `16:25:52` ✅   Created rule cron(45 13 ? * MON-FRI *)
- `16:25:52` ✅   Added invoke permission
- `16:25:52` Done
