# Verify which PAT is active in dex-scanner

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-04-25T22:01:46+00:00  

## Data

| dex_scanner_pat | recommendation |
|---|---|
| fine-grained | delete 3 never-used tokens, keep Claude-Deploy |

## Log
## 1. Lambda justhodl-dex-scanner TOKEN env var

- `22:01:46`   TOKEN prefix: github_pat_...
- `22:01:46`   TOKEN length: 93 chars
- `22:01:46` ✅   ✅ NEW fine-grained PAT (github_pat_*) — rotation succeeded
- `22:01:46`   Lambda LastModified: 2026-04-25T21:18:09
- `22:01:46`   Lambda CodeSha256: GSS6NicVuzeelGGv...
## 2. GitHub-related secrets in use

- `22:01:46`   Lambda env vars referencing PATs:
- `22:01:46`     justhodl-dex-scanner.TOKEN — fine-grained PAT (just rotated)
- `22:01:46` 
- `22:01:46`   GitHub Actions workflow secrets (configured on repo):
- `22:01:46`     AWS_ACCESS_KEY_ID         — AWS, NOT a GitHub PAT
- `22:01:46`     AWS_SECRET_ACCESS_KEY     — AWS, NOT a GitHub PAT
- `22:01:46`     AWS_REGION                — AWS region literal
- `22:01:46`     ANTHROPIC_API_KEY         — Anthropic, NOT a GitHub PAT
- `22:01:46`     ANTHROPIC_API_KEY_NEW     — Anthropic, NOT a GitHub PAT
- `22:01:46`     TELEGRAM_BOT_TOKEN        — Telegram, NOT a GitHub PAT
- `22:01:46`     GITHUB_TOKEN              — auto-injected by GHA, NOT a stored PAT
## 3. Recommendation for the 4 classic PATs in Khalid's account

- `22:01:46` 
- `22:01:46`   ┌─────────────────────┬──────────────┬──────────────────────────┐
- `22:01:46`   │ PAT name            │ Status       │ Recommendation           │
- `22:01:46`   ├─────────────────────┼──────────────┼──────────────────────────┤
- `22:01:46`   │ Claude-Deploy       │ used last wk │ KEEP for now             │
- `22:01:46`   │ Render deploy       │ never used   │ DELETE — orphan          │
- `22:01:46`   │ CloudShell Git      │ never used   │ DELETE — orphan          │
- `22:01:46`   │ Cloud Shell Git     │ never used   │ DELETE — duplicate       │
- `22:01:46`   └─────────────────────┴──────────────┴──────────────────────────┘
- `22:01:46` 
- `22:01:46`   KEEP 'Claude-Deploy':
- `22:01:46`     Last used within the past week — actively in use somewhere.
- `22:01:46`     Not in GitHub Actions secrets (those use AWS_*, ANTHROPIC_*).
- `22:01:46`     Likely used outside CI (local dev, third-party integration,
- `22:01:46`     or an Anthropic-side automation).
- `22:01:46`     DON'T DELETE without confirming what's using it.
- `22:01:46` 
- `22:01:46`   DELETE the 3 'never used' tokens:
- `22:01:46`     They've been sitting there for years with broad 'repo' scope.
- `22:01:46`     Reduces attack surface. Zero risk to delete since
- `22:01:46`     GitHub confirms they've never been used.
- `22:01:46` 
- `22:01:46`   IMPORTANT: the OLD shared PAT (ghp_e6apGL...) you rotated AWAY
- `22:01:46`     from is NOT in this list. That means it was probably:
- `22:01:46`     (a) The 'Claude-Deploy' token (and rotation moved dex-scanner
- `22:01:46`         to a new fine-grained PAT, leaving Claude-Deploy still
- `22:01:46`         in use elsewhere — KEEP it), OR
- `22:01:46`     (b) Already deleted/expired before this session.
- `22:01:46`     EITHER WAY: don't delete Claude-Deploy without verifying.
- `22:01:46` Done
