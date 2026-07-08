# Memory archive — edit #06 (migrated verbatim 2026-07-08, Khalid-approved memory diet)

Source: Claude memory edit #06. This file is the authoritative archive; the memory slot now holds a one-line pointer here. Grep this directory before building anything fleet-related.

---

Public API Auth Tiers Phase 1+2A+2B+2C+2D LIVE 2026-05-06. aws/shared/api_auth.py authorize(event, allowed_origins=[...]). DDB justhodl-api-keys+justhodl-api-rate. FREE=100/hr, PRO=5k/hr, ENT=unlimited. Admin key = [REDACTED — lives in the Claude memory Keys/auth edit + SSM], token in SSM /justhodl/api-admin/token. 10 Lambdas migrated. CF Worker injects Origin:justhodl.ai. 55/56 E2E. Excluded: stock-screener PROTECTED, ai-chat own auth, telegram-bot webhook.
