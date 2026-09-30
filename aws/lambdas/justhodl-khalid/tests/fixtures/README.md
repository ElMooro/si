# Captured public Radar fixture

Captured read-only from https://justhodl.ai/data/capital-flow-radar.json on
2026-09-30 around 19:06 UTC. This is the full parsed public packet serialized as
JSON and gzip-compressed with mtime=0, not a newly generated observation.

- Radar generated_at: `2026-09-29T22:30:05.863044+00:00`
- Canonical generated_at: `2026-09-29T22:01:03.718009+00:00`
- Common economic end: `2026-09-28`
- Source expires: `2026-10-01T00:00:00+00:00`
- Decompressed fixture SHA-256: `9cc7f3629d444fad37477414eeebd31b33a91bceba85ce6cfdbd7981654e604d`

Tests freeze time at 2026-09-30 19:15 UTC to reproduce then-available evidence.
Expiry tests explicitly advance beyond the retained source lifetime. Production
uses the actual engine clock and the browser checks expiry every minute.
The fixture contains public retained-source references only; no private original
bodies, Brain content, credentials, vendor calls or synthetic flow amounts.
Synthetic mutation cases are labelled by their individual regression tests.
