# AI — the SageMaker front window (`justhodl-ai` + `ai.html`)

Launched by ops 5300 (2026-09-09). Khalid's directive: a front window to train SageMaker, an engine
called `ai` with its page `ai.html`, preload whatever SageMaker already holds, learn from the Brain,
and reach the most powerful training tiers SageMaker offers — without repeating the August cost mistakes.

## What it is

| Piece | Where | Role |
|---|---|---|
| `justhodl-ai` Lambda | `aws/lambdas/justhodl-ai/` | inventory writer (hourly, `justhodl-ai-inventory` Scheduler) + service-token-gated action API on a Function URL |
| `ai.html` | repo root | the window: inventory, catalog, Brain pipeline, playground, cost guard, power tiers |
| `/ai/*` bridge | `cloudflare/workers/justhodl-data-proxy/src/index.js` | owner/service role only; resolves the Function URL from `data/ai/control.json`; forwards with `X-JH-Service-Token` |
| `data/ai.json` | public bucket | read model (counts, statuses, prices, catalog) — never note text |
| `justhodl-ai-857687956942` | private bucket | datasets, embeddings, indexes, repacked models, jobs, policy, pricing cache |
| `justhodl-sagemaker-execution-role` | IAM | the role SageMaker jobs/endpoints assume |
| `justhodl-ai-sagemaker-control` | IAM inline on `lambda-execution-role` | what the Lambda may do to SageMaker (PassRole scoped to the execution role only) |

## The article, applied (tier 1)

`Use pre-trained financial language models for transfer learning in Amazon SageMaker JumpStart` (AWS ML blog,
Sep 2021): deploy a RoBERTa-SEC embedding endpoint → embed your documents → train a classifier on the embeddings.
Here the documents are the Brain notes and the labels are the categories Khalid gave them (`philosophy`, `rule`,
`thesis`, `macro`, `watchlist`, `lesson`, `reminder`) plus `pinned`. The classifier is the XGBoost built-in
(managed spot, `MaxRuntime` capped) served serverless; the embeddings double as a retrieval index
("which of my notes speak to this text").

Hub card ids the engine looks for first: `mxnet-tcembedding-robertafin-{base,base-wiki,large,large-wiki}-uncased`.
If the hub no longer lists them the catalog says so and the pipeline uses the best available text-embedding card.

## The AI's read of the market (v1.2, ops 5302/5303)

Quant computes, the AI explains (fusion doctrine). `market_read.py`:

1. **Board** — the fleet's fresh artifacts with per-source freshness (FRESH/STALE/MISSING, never filled): jh-fusion regime + entities,
   risk-gate, khalid-risk authority, katlin war room + picks, bottom, fortress, bond war room, crisis composite, GBC, regime composite,
   `screener/metals-miners.json`, `crypto-intel.json`, `data/crypto-cycle-risk.json`, the Brain's own regime read, the signal scorecard.
2. **Playbook** — each asset-class setup is written as a sentence from the board, embedded through the live RoBERTa-SEC endpoint and
   matched to the operator's nearest Brain notes.
3. **Read** — one Sonnet call (proprietary tier, `on_demand=True`, bounded direct fallback when the router gates) → strict JSON:
   overall, macro, stocks/bonds/metals/crypto stances + reads, best opportunities, what would change its mind, data gaps, up to 6 dated
   calls restricted to tickers the fleet surfaced.
4. **Ledger** — calls are logged as `signal_type=ai_market_read` via `signals_emit.log_signal` (5/21/63d windows); outcome-checker
   prices them forward; `GET /read` grades every call and the page shows hit rates and per-call returns.

Private artifact `ai/market-read/latest.json` (quotes notes → owner route only); public `data/ai.json.market_read` carries stances,
counts, freshness and hit rates. Daily schedule `justhodl-ai-market-read` cron(45 5 * * ? *) UTC; on-demand from the page (20-min gap).
The pipeline verdict of the last launch/re-arm op is written to `data/ai/verdict.json` and shown on the page.

## Tiers

1. Transfer learning (above) — cents.
2. JumpStart fine-tune — any hub card with `TrainingSupported` (weights as the `model` channel, recipe via `sagemaker_submit_directory`).
3. Autopilot / AutoML V2 — text classification on the Brain CSV, or tabular on any warehouse CSV + target.
4. HyperPod — locked by policy (`hyperpod_unlocked`) and a typed confirmation; bills per node-hour.

## Cost guard

Live Price List (`pricing:GetProducts`), `daily_budget_usd` (default 5), instance allow-lists, endpoint TTL (default 3h)
and idle reaping (0 invocations in `idle_hours`), `MaxRuntimeInSeconds` on every job, spot by default, serverless by
default, Cost Explorer MTD on the page. Only endpoints tagged `justhodl-ai-managed=true` are ever reaped.

## Actions (`POST https://api.justhodl.ai/ai/<action>`, owner sign-in)

`/inventory` `/catalog` `GET /model?model_id=` `/deploy` `/dataset/build` `/embed` `/train/classifier` `/train/finetune`
`/train/automl` `/deploy-trained` `/infer` `/endpoint/delete` `/job/stop` `/policy` `/hyperpod/create`

## Doctrine kept

No literal keys (env-first, SSM), no classic EventBridge rule, no self-invocation (chain-guard: the hourly
schedule resumes embedding passes), real data only (a missing hub field is an error with the document's key
list, never a default), private note text never leaves the private bucket.
