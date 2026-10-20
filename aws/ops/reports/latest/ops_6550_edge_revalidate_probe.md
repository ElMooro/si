# ops 6550 - live proof of the revalidated edge copy (read-only)

**Status:** failure  
**Duration:** 33.9s  
**Finished:** 2026-10-20T17:01:22+00:00  

## Error

```
SystemExit: 1
```

## Log
- `17:00:50` data/share-flows.json | first 200 MISS | second 200 REVALIDATED cc=no-store | etag site="44081df3175ec s3="44081df3175ec | exact 200 MISS
- `17:00:53` data/best-setups.json | first 200 MISS | second 200 REVALIDATED cc=no-store | etag site="0d7c50c6c738f s3="0d7c50c6c738f | exact 200 MISS
- `17:00:55` data/forensic-screen.json | first 200 MISS | second 200 REVALIDATED cc=no-store | etag site="ac6a0cec0ebe4 s3="ac6a0cec0ebe4 | exact 200 MISS
- `17:00:58` data/provider-fund-flow-research.json | first 200 MISS | second 200 REVALIDATED cc=no-store | etag site="2f89779f1242d s3="2f89779f1242d | exact 200 MISS
- `17:01:00` data/dark-pool.json | first 200 MISS | second 200 REVALIDATED cc=no-store | etag site="2e2873510aa22 s3="2e2873510aa22 | exact 200 MISS
- `17:01:03` data/futures-research.json | first 200 MISS | second 200 REVALIDATED cc=no-store | etag site="c33745d1dc6ac s3="c33745d1dc6ac | exact 200 MISS
- `17:01:19` data/estimate-revisions.json | first 200 MISS | second 200 HIT cc=public, max-age=14400, s-maxage=300 | etag site="1b284e6713489 s3="1b284e6713489 | exact 200 MISS
- `17:01:21` data/buyback-engine.json | first 200 MISS | second 200 HIT cc=public, max-age=14400, s-maxage=300 | etag site="191ffb430268a s3="191ffb430268a | exact 200 MISS
- `17:01:22` bucket: Lightpanda -> 403 (want 403), browser -> 200 (want 200)
- `17:01:22` ✗ data/estimate-revisions.json second answer was HIT, not REVALIDATED
- `17:01:22` ✗ data/estimate-revisions.json Cache-Control public, max-age=14400, s-maxage=300
- `17:01:22` ✗ data/buyback-engine.json second answer was HIT, not REVALIDATED
- `17:01:22` ✗ data/buyback-engine.json Cache-Control public, max-age=14400, s-maxage=300
