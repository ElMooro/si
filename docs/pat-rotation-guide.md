# PAT Rotation — One-Page Guide

The **dex-scanner Lambda** uses a GitHub PAT to push updates to `dex.html` in `ElMooro/si`. That PAT is currently shared with the Claude-Deploy automation, which is a security risk: if either consumer leaks, both are compromised. This guide walks through rotating the dex-scanner to its own dedicated PAT.

**Time required:** 2–3 minutes.
**Risk level:** very low — the workflow auto-rolls back on any failure.

---

## Step 1: Generate a fresh PAT

GitHub now has two PAT types. As of 2026, **fine-grained PATs are GA and recommended** for new tokens.

### Option A — Fine-grained PAT (recommended)

1. Go to <https://github.com/settings/personal-access-tokens/new>
2. Fill in:
   - **Token name:** `justhodl-dex-scanner`
   - **Expiration:** 90 days (or longer; pick what you're comfortable rotating)
   - **Description:** "PAT for dex-scanner Lambda — pushes dex.html to ElMooro/si"
   - **Repository access:** Only select repositories → `ElMooro/si`
   - **Repository permissions:**
     - Contents → **Read and write** (this is the only one needed)
     - Metadata → Read-only (auto-selected, mandatory)
3. Click **Generate token**
4. **Copy the token immediately** (it's shown only once). Format: `github_pat_...`

### Option B — Classic PAT (still works if you prefer)

1. Go to <https://github.com/settings/tokens/new>
2. Fill in:
   - **Note:** `justhodl-dex-scanner`
   - **Expiration:** 90 days
   - **Scopes:** tick **only** `repo`
3. Click **Generate token**
4. **Copy immediately** (format: `ghp_...`)

---

## Step 2: Run the rotation workflow

1. Go to <https://github.com/ElMooro/si/actions/workflows/rotate-dex-scanner-pat.yml>
2. Click **Run workflow** (top right)
3. In the dialog:
   - **new_pat:** paste the token you just copied
   - **revoke_check_only:** leave unchecked (uncheck = actually swap; checked = dry-run only)
4. Click **Run workflow**

Wait ~30 seconds. The workflow will:
- ✅ Validate the PAT format (`ghp_*` or `github_pat_*`)
- ✅ Verify it can authenticate to GitHub API
- ✅ Verify it has push permission on `ElMooro/si`
- ✅ Capture the old TOKEN for rollback safety
- ✅ Update `justhodl-dex-scanner` Lambda's `TOKEN` env var
- ✅ Smoke-test by sync-invoking the Lambda
- 🔄 Auto-rollback if any of the above fails

---

## Step 3: Revoke the old PAT (after Step 2 succeeds)

If the workflow shows green:

1. Go to <https://github.com/settings/tokens>
2. Find the **old** PAT (the one starting with `ghp_e6apGL`... that was previously shared)
3. Click **Delete** on that row

You can keep the Claude-Deploy PAT (it's a separate one used by GitHub Actions, stored as repository secrets — different concern).

---

## Troubleshooting

**Workflow fails with "PAT validation failed":**
- The token wasn't copied correctly, or
- It doesn't have the right repository/scope permissions
- → Re-generate following Step 1 carefully

**Workflow fails with "Smoke test failed: Lambda invoke error":**
- Auto-rollback already restored the old TOKEN, so dex-scanner keeps working
- Dig into the workflow logs for the specific failure
- → Probably means the Lambda has another issue unrelated to the PAT

**Workflow fails with "Push permission check failed":**
- Fine-grained PAT missing "Contents: Read and write" on `ElMooro/si`, or
- Classic PAT missing `repo` scope
- → Re-generate with correct permissions

---

## What changes after rotation

- `justhodl-dex-scanner` Lambda → uses new dedicated PAT
- Old shared PAT → can be revoked
- GitHub Actions deploy automation → **unaffected** (uses separate `AWS_*` secrets, not this PAT)
- Claude-Deploy automation → **unaffected** (still uses its own PAT)

The dex-scanner runs on a 6h schedule (`cron(0 */6 * * ? *)`) so the next dex.html update after rotation is the proof point.
