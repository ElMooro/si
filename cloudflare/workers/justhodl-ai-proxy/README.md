# justhodl-ai-proxy

Cloudflare Worker that sits between `justhodl.ai` and the AWS Lambda
`justhodl-ai-chat`. It injects the Lambda's auth token server-side so it
never leaves Cloudflare.

## Architecture

```
Browser (justhodl.ai)
     │ POST /chat  (no auth token in request)
     ▼
justhodl-ai-proxy (Cloudflare Worker)
     │ - Origin check (only justhodl.ai + www)
     │ - Body size cap (32KB)
     │ - Adds x-justhodl-token header from encrypted Secret
     ▼
justhodl-ai-chat (AWS Lambda)
     │ - Validates x-justhodl-token
     │ - Validates Origin
     │ - Runs Claude Haiku 4.5
     ▼
Response streamed back through Worker to browser
```

## Deployment

Push any change to `cloudflare/workers/justhodl-ai-proxy/**` on `main`.
GitHub Actions runs `.github/workflows/deploy-workers.yml` which:

1. Reads `AI_CHAT_TOKEN` from AWS SSM (`/justhodl/ai-chat/auth-token`)
2. Runs `wrangler deploy`
3. Sets the fetched token as the Worker's `AI_CHAT_TOKEN` secret

No manual wrangler calls required.

## Rotation

Rotating the Lambda auth token is a single command (run locally):

```
aws ssm put-parameter \
  --name /justhodl/ai-chat/auth-token \
  --value "$(python -c 'import secrets; print(secrets.token_urlsafe(32))')" \
  --type SecureString --overwrite --region us-east-1
```

Then push any trivial change to the Worker dir (or trigger the workflow
manually) and the Worker picks up the new value on next deploy.

## Local testing

```
npm install -g wrangler
cd cloudflare/workers/justhodl-ai-proxy
wrangler dev --local
```

## Monitoring

Cloudflare dashboard → Workers & Pages → `justhodl-ai-proxy` → Logs tab.
With `observability` enabled in wrangler.toml, every request logs the
status code, CPU time, and any console output.
