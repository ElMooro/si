# aws/

CI/CD-managed infrastructure for JustHodl.AI.

**First time? Read [SETUP.md](./SETUP.md) for the one-time configuration.**

## Layout

```
aws/
├── SETUP.md                 ← one-time setup (do this first)
├── README.md                ← you are here
├── lambdas/                 ← Lambda source code, one dir per function
│   └── <function-name>/
│       ├── source/
│       │   └── lambda_function.py
│       └── config.json      ← optional; overrides function name
└── ops/
    ├── pending/             ← scripts awaiting execution (GitHub Actions runs these on push)
    └── history/             ← scripts already run (moved here after success)
```

## Adding a new Lambda to the pipeline

1. Create `aws/lambdas/<function-name>/source/lambda_function.py`
2. (Optional) Create `aws/lambdas/<function-name>/config.json`:
   ```json
   { "function_name": "justhodl-my-function", "region": "us-east-1" }
   ```
3. Push to main. GitHub Actions deploys it. Done.

## Running a one-off deploy/fix script

1. Drop it at `aws/ops/pending/<descriptive-name>.py`
2. Push. GitHub Actions runs it with AWS credentials.
3. After confirmed success, move it to `aws/ops/history/` in a subsequent commit
   so it doesn't re-run.

Every script should be idempotent — safe to run twice.

## Managed Lambdas

Populated as Claude migrates them from direct-deploy into source-control. Goal
is to have all production Lambdas version-controlled here:

- [ ] justhodl-ai-chat
- [ ] justhodl-telegram-bot
- [ ] justhodl-morning-intelligence
- [ ] justhodl-investor-agents
- [ ] justhodl-daily-report-v3
- [ ] justhodl-ka-metrics
- [ ] justhodl-khalid-metrics  <!-- LEGACY — Phase 4 of Khalid→KA rebrand. Delete after 7-day grace ~2026-05-03. -->
- [ ] justhodl-crypto-enricher
- [ ] justhodl-crypto-intel
- [ ] justhodl-options-flow
- [ ] justhodl-valuations-agent
- [ ] justhodl-edge-engine
- [ ] justhodl-intelligence
- [ ] justhodl-stock-analyzer
- [ ] justhodl-stock-screener
- [ ] cftc-futures-positioning-agent
- [ ] justhodl-signal-logger / outcome-checker / calibrator
