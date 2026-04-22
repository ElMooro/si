# Cloudflare Workers — Setup

One-time setup for deploying Workers via GitHub Actions. After this, any push
to `cloudflare/workers/**` on `main` auto-deploys the affected Worker.

## What Claude needs from you (one time, ~3 minutes)

Two more GitHub secrets, sourced from your Cloudflare account:

### 1. Get your Cloudflare Account ID

- Go to [dash.cloudflare.com](https://dash.cloudflare.com)
- Click any domain (or "Workers & Pages" in the sidebar)
- The **Account ID** appears in the right sidebar — a hex string like
  `a1b2c3d4e5f6...`. Copy it.

### 2. Create a Cloudflare API Token

- Go to
  [dash.cloudflare.com/profile/api-tokens](https://dash.cloudflare.com/profile/api-tokens)
- Click **Create Token**
- Scroll to the **Edit Cloudflare Workers** template → click **Use template**
- Confirm the scopes shown:
  - `Account > Workers Scripts > Edit`
  - `Account > Account Settings > Read`
  - `Zone > Workers Routes > Edit` (fine even if we don't use custom domains yet)
- (Optional) Restrict to your specific account under **Account Resources**
- Click **Continue to summary** → **Create Token**
- Copy the token value (you won't see it again)

### 3. Add the two secrets to GitHub

Go to
[github.com/ElMooro/si/settings/secrets/actions](https://github.com/ElMooro/si/settings/secrets/actions):

| Name | Value |
|---|---|
| `CLOUDFLARE_ACCOUNT_ID` | (from step 1) |
| `CLOUDFLARE_API_TOKEN` | (from step 2) |

### 4. Tell Claude "cloudflare is in"

Claude will manually trigger the workflow (or push a trivial commit) and
`justhodl-ai-proxy` will deploy to your account.

## After deployment

The Worker lives at:

```
https://justhodl-ai-proxy.<your-cf-subdomain>.workers.dev
```

The `<your-cf-subdomain>` is shown in your Cloudflare dashboard under
Workers & Pages. First deploy picks a default; you can rename it in CF UI
if you want.

Claude will update `dex.html` to call this URL instead of the Lambda URL
directly, then the chat widget on justhodl.ai will work again — but this
time with the auth token safely server-side.

## Rotation

Rotate the upstream Lambda auth token:

```
aws ssm put-parameter \
  --name /justhodl/ai-chat/auth-token \
  --value "$(python -c 'import secrets; print(secrets.token_urlsafe(32))')" \
  --type SecureString --overwrite --region us-east-1
```

Then push any trivial change under `cloudflare/workers/justhodl-ai-proxy/`
and the Worker picks up the new token.

## Future: point api.justhodl.ai at the Worker

Once comfortable, we can move the Worker from `*.workers.dev` to a proper
subdomain like `api.justhodl.ai`. Requires adding a DNS record + Worker
Route. Ask Claude when ready.
