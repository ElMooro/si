## A. Plain-URL diagnosis (what Khalid sees)

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-07-14T19:21:34+00:00  

## Data

| fails | final_ofr_html | final_primary-dealers_html | ofr_html | primary-dealers_html | purge_err | purge_errors | purge_success | stale_urls | warns | zone_id_found |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | {'markers_ok': True, 'cf_cache_status': 'DYNAMIC', 'age': '0', 'cache_control': 'max-age=600', 'len': 76746} |  |  |  |  |  |  |  |
|  |  |  |  | {'markers_ok': True, 'cf_cache_status': 'DYNAMIC', 'age': '0', 'cache_control': 'max-age=600', 'len': 23956} |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | [] |  |  |
|  |  |  |  |  |  |  |  |  |  | True |
|  |  |  |  |  | 401 {"result":null,"success":false,"errors":[{"code":10000,"message":"Authentication error"}],"messages":[]} | None | None |  |  |  |
|  | {'markers_ok': True, 'cf_cache_status': 'DYNAMIC'} |  |  |  |  |  |  |  |  |  |
|  |  | {'markers_ok': True, 'cf_cache_status': 'DYNAMIC'} |  |  |  |  |  |  |  |  |
| [] |  |  |  |  |  |  |  |  | [] |  |

## Log
## B. Cloudflare purge

## C. Re-verify plain URLs

- `19:21:34` OPS 3309 PASS — bare URLs now serve the latest HTML; pages.yml self-purges CF on every deploy going forward.
