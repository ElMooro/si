# Security rotation runbook — PAT + repo visibility (prepared 2026-07-08)

## The critical precondition nobody flagged before

justhodl.ai is served by GitHub Pages from this repo. **GitHub Pages on a
private repo requires GitHub Pro ($4/mo) or higher; on a Free personal
account it only works on public repos.** Flipping this repo private on the
Free plan takes the site DOWN (404) until you upgrade or re-publicize.
Also: private repos meter GitHub Actions (Free 2,000 min/mo, Pro 3,000;
overage ~$0.008/min Linux) — this pipeline runs many workflows daily, so
expect small overage charges some months. Public repos have unlimited free
Actions minutes and free secret scanning.

## Path A (recommended): Pro + private + rotate — ~10 min of Khalid's time

1. Upgrade ElMooro to GitHub Pro (github.com/settings/billing) — $4/mo.
2. Create the new token: Settings → Developer settings → Fine-grained
   tokens → Generate. Resource owner ElMooro; ONLY repository ElMooro/si;
   expiration 1 year (calendar the renewal); Repository permissions:
   **Contents: Read+Write · Workflows: Read+Write · Actions: Read ·
   Metadata: Read**. Nothing else.
3. Paste the new token to Claude in chat. Claude then autonomously:
   updates the git remote, updates repo secrets PAGES_PAT and GH_API_TOKEN
   (sealed-box PUT), pushes a verification ops that exercises push + run-ops
   + Actions polling with the new token, updates the ⛔ MASTER BOOTSTRAP
   memory card + this file, and confirms green.
4. Only after Claude confirms green: revoke the old "Claude-Deploy"
   classic token (Developer settings → Tokens (classic) → Delete).
5. Flip repo private: Settings → General → Danger Zone → Change
   visibility. Verify justhodl.ai still serves (~2 min).
6. Rotate the compromised Telegram bot token (@BotFather → /revoke) and
   paste to Claude — Claude updates Lambda envs + SSM fleet-wide via ops.
7. Later (optional, Claude does it on "go"): keys.py SSM-first sweep to
   strip the ~700 hardcoded provider keys from source, then rotate those
   provider keys at leisure.

## Path B (stay Free): keep public, rotate everything instead

Keep unlimited Actions + free secret scanning. Do steps 2-4 and 6 above,
plus the keys.py SSM-first sweep + provider-key rotations (FMP, Polygon,
AlphaVantage — all exposed in history). Going private later remains open.

## Why rotation matters more than visibility

The old secrets live in git HISTORY; anyone who already cloned keeps them
forever, and flipping private does not un-leak them. Rotation is the fix;
privacy stops NEW exposure. Do rotation in both paths.
