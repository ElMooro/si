# ops 6550 - live proof of the revalidated edge copy (read-only)

**Status:** success  
**Duration:** 16.1s  
**Finished:** 2026-10-20T17:18:08+00:00  

## Log
- `17:17:54` data/share-flows.json | first 200 REVALIDATED | second 200 REVALIDATED cc=no-store | etag site="44081df3175ec s3="44081df3175ec | exact 200 MISS
- `17:17:58` data/best-setups.json | first 200 REVALIDATED | second 200 REVALIDATED cc=no-store | etag site="0d7c50c6c738f s3="0d7c50c6c738f | exact 200 MISS
- `17:18:00` data/forensic-screen.json | first 200 REVALIDATED | second 200 REVALIDATED cc=no-store | etag site="ac6a0cec0ebe4 s3="ac6a0cec0ebe4 | exact 200 MISS
- `17:18:02` data/provider-fund-flow-research.json | first 200 REVALIDATED | second 200 REVALIDATED cc=no-store | etag site="2f89779f1242d s3="2f89779f1242d | exact 200 MISS
- `17:18:04` data/dark-pool.json | first 200 REVALIDATED | second 200 REVALIDATED cc=no-store | etag site="2e2873510aa22 s3="2e2873510aa22 | exact 200 MISS
- `17:18:06` data/futures-research.json | first 200 REVALIDATED | second 200 REVALIDATED cc=no-store | etag site="c33745d1dc6ac s3="c33745d1dc6ac | exact 200 MISS
- `17:18:07` data/estimate-revisions.json | ordinary file: 200 MISS / 200 HIT cc=public, max-age=14400, s-maxage=300
- `17:18:07` data/buyback-engine.json | ordinary file: 200 MISS / 200 HIT cc=public, max-age=14400, s-maxage=300
- `17:18:08` bucket: Lightpanda -> 403 (want 403), browser -> 200 (want 200)
- `17:18:08` ✅ second reads are REVALIDATED, never stale, exact reads bypass, Lightpanda refused
