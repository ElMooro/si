# JustHodl fusion map — Grok pass 2026-09-11

Fleet size: 333 Lambdas. Doctrine: warehouse-only joins, only fields a consumer needs, fail-soft, never fabricate, never add a pending op that cancels the queue.

## Deploy-write rule (Grok lane)

GitHub Contents API / `push_files` **cannot carry ~40 KB**. Partial writes have already produced stubs on `main`.

- Safe: files ≲ ~18 KB after a GET size-check on the blob.
- Unsafe: `justhodl-stock-buying` (~40 KB), `justhodl-apac-flows` (~35 KB), `best-setups` / `master-ranker` / `signal-board`.
- After every write: GET `size`. If it is not the expected byte count, **cancel deploy-lambdas immediately** and restore from a known blob.
- Large patches: `aws/ops/staged/` + a shell lane (`git apply` / local commit). Never a placeholder on `main`.
- **Do not write** `aws/lambdas/justhodl-stock-buying/**` (Claude restored v1.5.2 e219b4b / deploy 3179).

## Shipped and deploy-green this pass

| Engine | Version | Deploy | What |
|---|---|---|---|
| justhodl-best-ideas | schema 1.1.1-grok-fusion | 3180 | harvest fallbacks for stealth/flow/squeeze list keys |
| justhodl-flow-confluence | 1.1 | 3181 | options+squeeze joins; regime/Beneish haircuts |
| justhodl-options-confluence | 1.1 | 3182 | flow-data + squeeze-pretrigger; overlays |
| justhodl-boom-radar | 1.1.0 | 3183 | fills FLOW/SQUEEZE from those synthesizers; haircuts |
| justhodl-shadow-lab | 1.0.1 | earlier | dropped dead `/api/v3` fallback |
| justhodl-stock-buying | 1.5.2 | 3179 Claude | `/stable/` + managed_secret; Grok must not touch |

## Already fused (do not rebuild)

Best Ideas, signal-board, conviction-engine, master-ranker, stock-buying, risk-gate.

## Needed fields only

Name engines: symbol + one score + generated_at. FLOW/RISK are overlays or existing families — not score inflation.
Macro: one regime bit. Squeeze: FINRA volume + float, options as confirm only.

## Still open

| Item | Why blocked | Hand-off |
|---|---|---|
| apac-flows leftover `/api/v3` fallback | 35 KB file | staged script `aws/ops/staged/grok_apac_flows_drop_v3.py` |
| alpha-confluence tape overlays | 15–17 KB write risked truncation | do not retry via Contents API |
| stock-buying FMP_BUDGET warm reset | v1.5.3 | Claude. Ops 5421 repaired stale env key to SSM; SSM key is valid (HTTP 200) |
| FINRA / PatentsView / CoinGecko keys | Khalid | KHALID_ACTIONS.md |
