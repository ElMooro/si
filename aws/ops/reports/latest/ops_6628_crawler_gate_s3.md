# ops 6628 - the crawler list at the bucket (S3 half of the crawler gate)

**Status:** success  
**Duration:** 14.3s  
**Finished:** 2026-10-23T18:37:18+00:00  

## Data

| added | crawler_tokens | crawlers_refused | must_pass | policy_bytes |
|---|---|---|---|---|
| 22 | 24 | 24 | 28 | 18544 |

## Log
- `18:37:04` live policy 18194 bytes; adding 22 patterns -> 18544 bytes (limit 20000)
- `18:37:09` probe set 51 objects; Safari before: {200: 44, 403: 7}; must_pass before: {200: 28}
- `18:37:09` ✅ rollback copy: s3://justhodl-ops-private-857687956942/backups/bucket-policy/justhodl-dashboard-live-20261023T183709Z.json
- `18:37:13` ✅ put + read-back: 11 statements, 18544 bytes, +22 patterns: *Googlebot*, *Google-InspectionTool*, *GoogleOther*, *AdsBot-Google*, *Storebot-Google*, *bingbot*, *BingPreview*, *DuckDuckBot*, *YandexBot*, *Baiduspider*, *PetalBot*, *Bytespider*, *AhrefsBot*, *SemrushBot*, *SiteAuditBot*, *DataForSeoBot*, *Amazonbot*, *GPTBot*, *ClaudeBot*, *CCBot*, *PerplexityBot*, *meta-externalagent*
- `18:37:18` Safari probes after: 51 of 51 unchanged; differing: none
- `18:37:18` crawler sample agents -> {"Googlebot": 403, "Google-InspectionTool": 403, "GoogleOther": 403, "AdsBot-Google": 403, "Storebot-Google": 403, "bingbot": 403, "BingPreview": 403, "Applebot": 403, "DuckDuckBot": 403, "YandexBot": 403, "Baiduspider": 403, "PetalBot": 403, "Bytespider": 403, "AhrefsBot": 403, "SemrushBot": 403, "SiteAuditBot": 403, "DataForSeoBot": 403, "Amazonbot": 403, "GPTBot": 403, "ClaudeBot": 403, "CCBot": 403, "PerplexityBot": 403, "meta-externalagent": 403, "Lightpanda": 403}
- `18:37:18` must_pass agents unchanged: True; statuses {200: 28}
- `18:37:18` ✅ every listed crawler is refused at the bucket; every browser, probe, script and engine agent answers exactly as before
