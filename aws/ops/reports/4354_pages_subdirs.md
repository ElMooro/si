# ops 4354 — pages artifact: subdirectory pages restored (crypto/ 404 root cause)

**Symptom:** justhodl.ai/crypto.html "isn't working" (user report, 2026-08-03).

**Root cause (two layers):**
1. `pages.yml` "lean whitelisted artifact" copied only root `*.html *.js *.css`
   + `assets js css img fonts tools`. All **44 page subdirectories**
   (`crypto/ intel/ screener/ stock/ euro/ agent/ ...`) were absent from the
   deployed artifact → 404 at the edge while present in the repo.
2. No root `/crypto.html` has existed since the May link-audit; four active
   pages (`intel/index.html`, `web/intel/index.html`, `altseason.html`,
   `rotation-radar.html`) still linked to the flat URL.

**Fix:**
- Artifact assembly now includes any top-level dir containing `.html`, minus
  infra denylist (`aws .github scripts ci config cloudflare ops docs supabase
  tools-src chrome-extension node_modules _partials _site`). Dynamic — future
  page dirs deploy with zero workflow edits. `**/*.html` added to push paths.
- Root `/crypto.html` meta-redirect stub → `/crypto/` (bookmarks safe).
- Four hrefs repaired to `/crypto/`.

**Pre-flight (local, exact CI-gate replica):** 560 files / 13M, 425 root pages,
`crypto/index.html` present, 0 template leaks, dash count 1005 (budget
bootstraps — no repo `ci/dash_budget.txt`).

**Related, NOT fixed here:** `justhodl-crypto-intel` in work_collapse since
~07-30 (58.3s→5.2s, ops 4252) alongside 4 other LLM engines; ops 4253 shows
`credential_ok:false` (Anthropic 400s, Telegram 401). Feed's deterministic
layers still write; AI Intel layer degraded. Needs credential rotation.
