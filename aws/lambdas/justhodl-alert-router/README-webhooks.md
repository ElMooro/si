# Webhook alerting (justhodl-alert-router v1.1)

In addition to Telegram, alert-router can POST to any number of webhook
URLs every 30 minutes when alerts fire. Slack, Discord, and generic
HTTP receivers are all supported.

## Configuration

Webhook URLs live in SSM SecureString at:

```
/justhodl/alerts/webhook_urls
```

Value is a JSON list. Each item is either a **plain URL string** (type
auto-detected from the host) or a **dict** for fine-grained control:

```json
[
  "https://hooks.slack.com/services/T0/B0/abc",
  {
    "url": "https://discord.com/api/webhooks/123/abc",
    "type": "discord"
  },
  {
    "url": "https://example.com/justhodl-alert",
    "type": "generic",
    "min_severity": "HIGH"
  }
]
```

### Fields per webhook

| Field | Required | Default | Description |
|---|---|---|---|
| `url` | yes | — | Webhook endpoint |
| `type` | no | auto-detect from URL | `slack` \| `discord` \| `generic` |
| `min_severity` | no | `LOW` | `LOW` \| `MEDIUM` \| `HIGH` — only fires at or above this severity |

### Auto-detection rules

- `slack.com` in URL → `slack` payload format
- `discord.com` or `discordapp.com` → `discord` payload format
- Anything else → `generic` (raw alert dict envelope)

## Adding a Slack webhook

1. Create an incoming webhook in your Slack workspace:
   https://api.slack.com/messaging/webhooks
2. Copy the URL (looks like `https://hooks.slack.com/services/T.../B.../...`)
3. Add it via AWS CLI:

```bash
# First time:
aws ssm put-parameter \
  --name /justhodl/alerts/webhook_urls \
  --type SecureString \
  --value '["https://hooks.slack.com/services/T0/B0/abc"]' \
  --overwrite

# Or add to existing list — fetch, modify, put back:
existing=$(aws ssm get-parameter --name /justhodl/alerts/webhook_urls \
  --with-decryption --query 'Parameter.Value' --output text)
echo "$existing" | jq '. + ["https://new-url"]' | \
  xargs -I {} aws ssm put-parameter \
    --name /justhodl/alerts/webhook_urls \
    --type SecureString --value '{}' --overwrite
```

## Adding a Discord webhook

1. In the channel settings → Integrations → Webhooks → New Webhook
2. Copy URL
3. Same SSM put-parameter flow as Slack

## Severity filtering

Each alert has a severity (`HIGH`, `MEDIUM`, `LOW`). Use `min_severity`
to send to a webhook only when severity is at or above a threshold —
e.g., dump everything into a #alerts-low channel but only HIGH into
#alerts-pager.

## Verification

After adding a URL, manually trigger the alert-router:

```bash
aws lambda invoke --function-name justhodl-alert-router /tmp/out.json
cat /tmp/out.json
```

Look for `"webhooks_configured": N` and `"webhook_ok": M` in the body.
The latest run is also visible at `data/alert-history.json` —
each alert now carries a `webhook_results` list:

```json
{
  "id": "...",
  "telegram_sent": true,
  "webhook_results": [
    {"host": "hooks.slack.com", "type": "slack", "ok": true, "info": "200 ok"}
  ]
}
```

## Privacy note

The webhook history stored in S3 (`data/alert-history.json`) only
records the host (e.g., `hooks.slack.com`) — never the full URL,
since URLs contain webhook secrets.

## Payload examples

**Slack** (Block Kit attachment):
```json
{
  "attachments": [{
    "color": "#ff174a",
    "title": ":red_circle: VIX Curve Inversion",
    "text": "VIX1M / VIX3M = 1.08, term structure inverted",
    "fields": [
      {"title": "source", "value": "vix-curve", "short": true},
      {"title": "value", "value": "1.08", "short": true}
    ],
    "footer": "JustHodl alert-router"
  }]
}
```

**Discord** (Embed):
```json
{
  "username": "JustHodl",
  "embeds": [{
    "title": "🔴 VIX Curve Inversion",
    "description": "VIX1M / VIX3M = 1.08, term structure inverted",
    "color": 16718410,
    "fields": [
      {"name": "source", "value": "vix-curve", "inline": true}
    ]
  }]
}
```

**Generic**:
```json
{
  "service": "justhodl-alert-router",
  "version": "1.1",
  "alert": { "id": "...", "severity": "HIGH", "title": "...", "...": "..." },
  "ts": "2026-05-06T17:00:00Z"
}
```
