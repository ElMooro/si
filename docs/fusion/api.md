# Read API v1 (Release 2, ops 5215)

Served by the Cloudflare `justhodl-data-proxy` worker (`cloudflare/workers/justhodl-data-proxy/src/fusion_api.js`) on
`https://justhodl.ai/api/v1/*` (zone routes added in `wrangler.toml`) and on the workers.dev host. Read-only, CORS `*`,
edge-cached 60 s (the two S3 read models are cached 120 s at the edge), header `X-JH-API: v1`. Payloads are bounded:
signal rows are dropped unless `full=1`.

| Route | Returns |
|---|---|
| `GET /api/v1/fusion` | screener rows (best horizon per entity: fusion, conviction, direction, confidence, coverage, independent/raw counts, contradiction, regime fit, capital decision, size modifier, veto counts, missing families, family scores, velocity) + regime + stats + methodology |
| `GET /api/v1/fusion/{entity}` | one entity, every horizon; `?horizon=SWING` narrows (404 when that horizon has no evidence); `?full=1` keeps the per-signal factor rows |
| `GET /api/v1/fusion/{entity}/changes` | `what_changed` per horizon (previous score, delta, new/gone engines, contribution deltas), `velocity`, `history` |
| `GET /api/v1/signals/{entity}` | live JHSIGNAL compact rows from the current-state snapshot; `?family=FLOW`, `?horizon=SWING` |
| `GET /api/v1/regime/current` | regime axis, label, certainty, legs, critical dependencies, run stats |
| `GET /api/v1/opportunities` | ranked horizon reads; `?min_fusion= &min_confidence= &min_independent= &max_contradiction= &horizon= &direction=bullish|bearish &limit=` |
| `GET /api/v1/health` | freshness of both read models |

`{entity}` accepts a canonical id (`equity:NVDA`) or a bare ticker (`NVDA`, `brk.b`), resolved against the read model.
Opportunity rank = `|fusion| x confidence x (0.5 + 0.5 coverage) x (1 - contradiction/200) x size_modifier`, zero when
capital is BLOCKED. `historical_edge` and `asymmetry` are absent until Release 6 (never fabricated).
