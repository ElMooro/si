# JustHodl.AI — CI/CD Setup

**One-time setup for zero-touch AWS deploys.** After this, Claude can deploy
any code change by pushing to this repo. No local commands, no Git Bash.

## Architecture

```
Claude  ──push──▶  GitHub Repo (ElMooro/si)  ──GitHub Actions──▶  AWS
                         │
                         ├─ aws/lambdas/<n>/source/  → deploys that Lambda
                         └─ aws/ops/pending/<n>.py   → runs that script
```

## One-time setup (~5 minutes)

### 1. Install the two workflow files

GitHub blocks Personal Access Tokens from touching `.github/workflows/` (this
is a GitHub security rule, not something we can bypass). So the two workflow
files are staged at `aws/ops/staging/workflows/` — you copy them into place
once, and from then on Claude can edit everything.

Open Git Bash:

```bash
cd /c/Users/Adam/Desktop/justhodl/si
git pull origin main
mkdir -p .github/workflows
cp aws/ops/staging/workflows/*.yml .github/workflows/
git add .github/workflows/
git commit -m "ci: install GitHub Actions workflows"
git push origin main
```

That's the only local command you ever run for CI/CD.

### 2. Create a dedicated IAM user for GitHub Actions

Go to [AWS IAM → Users](https://console.aws.amazon.com/iam/home#/users):

- **Create user** → name it `github-actions-justhodl`
- Do **not** enable console access
- Attach policies directly:
  - `AWSLambda_FullAccess`
  - `AmazonSSMFullAccess`
  - `AmazonS3FullAccess`
  - `IAMFullAccess` (needed to attach policies to `lambda-execution-role`)
  - `CloudWatchLogsFullAccess`
- After creating: click into the user → **Security credentials** → **Create access key** → **Application running outside AWS**
- Copy the **Access key ID** and **Secret access key**

> These permissions are broad. Once things are stable we can narrow to
> specific Lambda ARNs and the `/justhodl/*` SSM prefix. Fine for now.

### 3. Add GitHub secrets

Go to
[https://github.com/ElMooro/si/settings/secrets/actions](https://github.com/ElMooro/si/settings/secrets/actions):

| Secret name                 | Value                                   |
|-----------------------------|-----------------------------------------|
| `AWS_ACCESS_KEY_ID`         | (from step 2)                           |
| `AWS_SECRET_ACCESS_KEY`     | (from step 2)                           |
| `AWS_REGION`                | `us-east-1`                             |
| `ANTHROPIC_API_KEY`         | (optional — your existing key)          |
| `TELEGRAM_BOT_TOKEN`        | (optional — your existing bot token)    |

### 4. Tell Claude "secrets are in"

In the next message, just say the secrets are configured. Claude will push
the next set of changes and they'll deploy automatically.

## How deploys work after setup

**When Claude changes a Lambda:**

Claude edits `aws/lambdas/<function>/source/lambda_function.py`, pushes.
GitHub Actions:
1. Detects which Lambda changed (git diff HEAD^ HEAD)
2. Zips the `source/` directory
3. Runs `aws lambda update-function-code`
4. Waits for `function_updated`

**When Claude runs a one-off script:**

Claude drops a Python script into `aws/ops/pending/<name>.py`, pushes.
GitHub Actions installs `boto3` and executes it with AWS creds as env.
After confirmed success, Claude moves the file to `aws/ops/history/` so it
doesn't re-run.

You do nothing. Every run shows up at
[https://github.com/ElMooro/si/actions](https://github.com/ElMooro/si/actions).

## Monitoring & rollback

- **View all deploys:** [Actions tab](https://github.com/ElMooro/si/actions)
- **Rollback a Lambda:** `git revert <commit>` on your laptop and push — the
  old code redeploys automatically
- **Read an SSM secret:**
  ```
  aws ssm get-parameter --name /justhodl/ai-chat/auth-token --with-decryption --region us-east-1 --query 'Parameter.Value' --output text
  ```

## Security

- AWS keys are injected as env vars into a one-time VM; never persisted
- Anyone with repo write access can trigger a deploy — keep the access list tight
- For production-hardening, upgrade to **OIDC federation** (no long-lived
  keys). Standard practice at Amazon/Google/Meta. Ask Claude when ready.
