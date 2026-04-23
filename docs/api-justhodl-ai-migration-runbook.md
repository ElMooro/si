# api.justhodl.ai Migration Runbook

**Goal**: Point `api.justhodl.ai` at the Cloudflare Worker `justhodl-ai-proxy`, retiring both the `raafouis.workers.dev` branding and the legacy CloudFront distribution at that subdomain.

**Estimated wall-clock time**: 30-60 minutes of active work + up to 48 hours of DNS propagation waiting.

**Risk level**: Medium. The nameserver swap at step 4 can break email and other services if we don't re-create every existing DNS record in Cloudflare first. That's why steps 1-3 are the critical prep.

---

## Current state (verified 2026-04-23)

| Item | Value |
|---|---|
| Cloudflare account | `2e120c8358c6c85dcaba07eb16947817` (RAAfouis@gmail.com) |
| Workers.dev subdomain | `raafouis.workers.dev` |
| Worker name | `justhodl-ai-proxy` |
| Worker current URL | `https://justhodl-ai-proxy.raafouis.workers.dev` |
| Worker target URL | `https://api.justhodl.ai` |
| Worker source in repo | `cloudflare/workers/justhodl-ai-proxy/` |
| Worker secret | `AI_CHAT_TOKEN` (already set in Worker environment) |
| Zone `justhodl.ai` DNS | **Namecheap** (currently NOT on Cloudflare) |
| `justhodl.ai` → | `185.199.108.153` (GitHub Pages) |
| `www.justhodl.ai` → | `185.199.109.153` (GitHub Pages) |
| `api.justhodl.ai` → | `3.168.51.48` (AWS CloudFront — legacy) |
| GitHub Pages repo | `ElMooro/si` → `justhodl.ai` |
| Legacy API on CloudFront | Original orchestrator (Sept-Oct 2025) — will be retired |

---

## Decision checkpoints before starting

**Stop and confirm these before beginning step 1.**

1. **Are you OK with `justhodl.ai` DNS being managed by Cloudflare going forward?**
   - Advantage: free CDN, free DDoS protection, future Worker integration is 1-click
   - Cost: you manage DNS via Cloudflare dashboard instead of Namecheap
   - Email: if you have MX records at Namecheap for email, they'll be re-created in Cloudflare (steps 2a + 3). If not, nothing to migrate.

2. **Is any production traffic hitting `api.justhodl.ai` right now?**
   - Check CloudFront/API Gateway CloudWatch metrics for the old orchestrator
   - If yes, those clients will break when `api.justhodl.ai` flips to the Worker (the Worker only proxies to the AI chat Lambda, not the old orchestrator)
   - If old API is in use, you need to either (a) make the Worker smart enough to proxy both, or (b) migrate callers to a different endpoint first

3. **Is email on `@justhodl.ai`?**
   - Check for MX records in Namecheap. If they exist and you use them, step 2a becomes critical.
   - If email uses Gmail/etc with `@gmail.com`, skip email concerns entirely.

---

## STEP 1 — AUDIT current Namecheap DNS records (5 min)

Goal: capture every DNS record so we don't lose any during migration.

1. Log in to Namecheap → Domain List → `justhodl.ai` → **Advanced DNS** tab.
2. Screenshot the page OR copy all records to a text file. You're looking for:
   - A records (e.g., `justhodl.ai` → `185.199.108.153`)
   - CNAME records (e.g., `www` → `elmooro.github.io`)
   - **MX records** (critical for email — likely blank if you don't use custom email)
   - TXT records (SPF, DKIM, domain verification — easy to miss but critical)
   - Any other records (SRV, CAA, AAAA, etc.)

**Save the full list to `/home/adam/justhodl-dns-audit.txt` on your Windows machine** (or anywhere you can reference later).

Expected records based on current DNS:

```
A       @              185.199.108.153
A       @              185.199.109.153
A       @              185.199.110.153
A       @              185.199.111.153
CNAME   www            elmooro.github.io
CNAME   api            <aws-cloudfront-distribution>.cloudfront.net
```

If you see additional TXT/MX records, write them all down verbatim — they all need to be re-created.

---

## STEP 2 — ADD `justhodl.ai` zone to Cloudflare (5-10 min)

1. Visit https://dash.cloudflare.com → **Add a Site**.
2. Enter `justhodl.ai` and click Continue.
3. Select the **Free** plan (all your needs fit within it) → Continue.
4. Cloudflare will automatically scan Namecheap's DNS and import whatever it can detect.
5. **Verify the scan caught everything from STEP 1.** Cross-reference your audit file.
6. For each missing record, click **Add record** and add it manually.

### 2a — Critical: email records

If you have MX records, they MUST be in Cloudflare before step 4. Double-check.

### 2b — Important: Proxy status (the orange cloud)

For each record, Cloudflare asks: "Proxied" (orange cloud) or "DNS only" (gray cloud)?

- Set these to **DNS only (gray cloud)** to match current Namecheap behavior:
  - GitHub Pages A records for `justhodl.ai` (GH Pages doesn't play nicely with Cloudflare proxy)
  - `www` CNAME to `elmooro.github.io`
  - Any MX records (email doesn't proxy)
  - Any TXT records (they're not traffic)
- Set this to **Proxied (orange cloud)** when we add it in STEP 5:
  - `api.justhodl.ai` (Worker needs proxy to bind)

7. At the top of the dashboard, note the **two Cloudflare nameservers** assigned to your zone (e.g., `ines.ns.cloudflare.com` and `wade.ns.cloudflare.com`). You'll need these in step 4.

---

## STEP 3 — VERIFY the Cloudflare zone before nameserver swap (5 min)

**Don't skip this.** Before changing nameservers, confirm every record is correctly set up. One missing MX record = email down for days.

1. Screenshot the Cloudflare DNS tab.
2. Compare side-by-side with your Namecheap audit from STEP 1.
3. Every record in Namecheap must be in Cloudflare (except `api` — we're intentionally repointing that).

At this point Cloudflare has your zone defined but **isn't serving DNS for it yet** — traffic still goes through Namecheap. You can take your time here.

---

## STEP 4 — NAMESERVER SWAP (2 min manual + 1-48h propagation)

**This is the irreversible-feeling step.** Up to 48 hours of propagation, but typically resolves in 30 min - 4 hours.

1. Log in to Namecheap → Domain List → `justhodl.ai` → **Domain** tab.
2. Find **Nameservers** section. Change from "Namecheap BasicDNS" to **Custom DNS**.
3. Enter the two Cloudflare nameservers from step 2.7.
4. Click the green checkmark to save.

Namecheap will email you to confirm. The change is active in Namecheap within minutes, but global DNS propagation takes longer.

### Watch propagation from your shell

```bash
# Run from Git Bash on your Windows machine
while true; do
  result=$(dig +short NS justhodl.ai @1.1.1.1)
  echo "$(date +%H:%M:%S)  $result"
  if echo "$result" | grep -q cloudflare; then
    echo "✓ Nameservers on Cloudflare"; break
  fi
  sleep 30
done
```

Or use https://www.whatsmydns.net/ (enter `justhodl.ai`, select NS records) and watch the global map turn green.

### While waiting — validate that your site still works

Keep `justhodl.ai` and `www.justhodl.ai` open in a browser. The moment Cloudflare starts serving DNS for them, refresh. **If the site is broken, you fix it in Cloudflare DNS** (not Namecheap — those records no longer matter). If you can't fix it, change the Namecheap nameserver back to BasicDNS and that rolls it all back.

---

## STEP 5 — ATTACH the Worker to `api.justhodl.ai` (5 min, AFTER step 4 fully propagates)

Once `dig +short NS justhodl.ai` shows the Cloudflare nameservers consistently, the zone is under Cloudflare's control and you can bind the Worker.

### 5a — Delete the old `api` CNAME from Cloudflare DNS

The previous CloudFront-pointing CNAME (from step 2 import) must go first, or Cloudflare won't allow the Worker Custom Domain binding.

1. Cloudflare dashboard → `justhodl.ai` → **DNS → Records**.
2. Find the `api` record (CNAME or A pointing at CloudFront).
3. Click **Edit** → **Delete**. Confirm.

### 5b — Bind the Worker to `api.justhodl.ai`

Option A — via Cloudflare dashboard:
1. **Workers & Pages** → `justhodl-ai-proxy` → **Settings → Triggers**.
2. Under **Custom Domains**, click **Add Custom Domain**.
3. Enter `api.justhodl.ai`. Click **Add Custom Domain**.
4. Cloudflare will auto-create the DNS record + provision a TLS cert. Takes ~1-2 min.

Option B — via `wrangler` CLI (if your dev machine has it):
```bash
# From within cloudflare/workers/justhodl-ai-proxy/ in the repo
# Add this block to wrangler.toml:
#   [[routes]]
#   pattern = "api.justhodl.ai"
#   custom_domain = true
# Then deploy:
wrangler deploy
```

### 5c — Verify

From Git Bash:
```bash
# Basic reachability
curl -sS -o /dev/null -w "%{http_code}\n" https://api.justhodl.ai/
# Should return 403 (Worker rejecting non-POST / missing Origin)

# Proper POST with correct Origin
curl -X POST https://api.justhodl.ai/ \
  -H "Origin: https://justhodl.ai" \
  -H "Content-Type: application/json" \
  -d '{"message":"hello"}'
# Should return AI chat response JSON

# Confirm TLS cert is Cloudflare, not GitHub Pages or legacy CloudFront
curl -sS -I https://api.justhodl.ai/ 2>&1 | grep -i "server\|cf-ray"
# Expect: server: cloudflare + cf-ray: xxxxx
```

---

## STEP 6 — UPDATE frontend + Worker allowlist (2 min)

The frontend dashboards currently point at `justhodl-ai-proxy.raafouis.workers.dev`. They should now point at `api.justhodl.ai`.

### 6a — Find and replace in the repo

```bash
cd /c/Users/Adam/Desktop/justhodl/si
grep -rln "raafouis.workers.dev\|justhodl-ai-proxy.raafouis" --include="*.html" --include="*.js"
```

Update each hit from `https://justhodl-ai-proxy.raafouis.workers.dev` to `https://api.justhodl.ai`.

### 6b — Worker: tighten ALLOWED_ORIGINS if needed

The Worker's current `ALLOWED_ORIGINS` is:
```javascript
new Set(["https://justhodl.ai", "https://www.justhodl.ai"])
```

This is already correct — the frontend will still be served from `justhodl.ai` so the Origin header stays the same. **No Worker change needed** for this step. But if you ever add a new site, add it here.

### 6c — Commit + push

```bash
git add -A
git commit -m "feat: switch frontend to api.justhodl.ai (retires raafouis.workers.dev)"
git push origin main
```

GitHub Pages will redeploy automatically from `ElMooro/si` main.

---

## STEP 7 — RETIRE the legacy CloudFront distribution at api.justhodl.ai (optional, after confirmed stable)

The AWS CloudFront distribution that used to serve `api.justhodl.ai` is now orphaned — no DNS record points at it. It's silently costing money.

**Wait at least 48 hours** after step 5 before doing this, to be certain nothing still routes through it.

Find it:
```bash
# From your AWS CLI or the console
aws cloudfront list-distributions \
  --query 'DistributionList.Items[?contains(Aliases.Items[0] || `""`, `justhodl`)].{Id:Id,Alias:Aliases.Items[0],Status:Status}' \
  --output table
```

Then either:
- **Disable** the distribution (keeps config, saves 100% of costs): `aws cloudfront update-distribution ...`
- **Delete** it (permanent): Disable first, wait for `Status: Deployed`, then delete.

Recommend: **Disable for 2 weeks, then delete.** That way if something was silently depending on it, you have time to discover the breakage and re-enable.

---

## STEP 8 — COMMIT the Worker wrangler config with the new custom domain (5 min)

So the binding is version-controlled and future deploys preserve it.

```bash
cd /c/Users/Adam/Desktop/justhodl/si/cloudflare/workers/justhodl-ai-proxy

# Edit wrangler.toml to add:
cat >> wrangler.toml << 'EOF'

[[routes]]
pattern = "api.justhodl.ai"
custom_domain = true
EOF

git add wrangler.toml
git commit -m "feat(worker): bind justhodl-ai-proxy to api.justhodl.ai"
git push origin main
```

The existing `.github/workflows/deploy-workers.yml` will pick this up and ensure the route stays bound on subsequent deploys.

---

## ROLLBACK PLAN

Each step is reversible until step 4 hits full propagation. After that, rollback = "change Namecheap nameservers back to BasicDNS", which also takes up to 48 hours.

| Step | Rollback |
|---|---|
| 2 | Just delete the zone from Cloudflare — nameservers haven't changed yet |
| 3 | Same — zone not live yet |
| 4 | Namecheap → Custom DNS → back to BasicDNS. Wait 1-48h. Everything returns to pre-migration state. |
| 5 | Delete the Worker Custom Domain binding in Cloudflare. `api.justhodl.ai` returns a 1016 error until you re-add a CNAME in Cloudflare DNS. |
| 6 | Revert the commits in `ElMooro/si`. Dashboards return to using the `workers.dev` URL. |
| 7 | Re-enable the CloudFront distribution (instant if disabled, re-create config if deleted). |
| 8 | Revert the wrangler.toml commit. |

---

## COMMON ISSUES

**"Cloudflare can't verify domain ownership."**
- You probably haven't completed step 4 (nameserver swap). Wait for propagation.
- Or you have "DNSSEC" enabled in Namecheap. Disable it before the swap — Cloudflare re-enables DNSSEC on its side.

**"api.justhodl.ai returns 525 Handshake Failed."**
- TLS cert is still provisioning. Wait 5-10 minutes. If still failing, check the Worker's Custom Domain page for cert status.

**"api.justhodl.ai returns 1016 Origin DNS error."**
- The `api` DNS record got re-created as a CNAME somewhere. Delete it. The Worker Custom Domain binding creates its own AAAA/A records implicitly.

**Website goes down mid-propagation.**
- Usually because a DNS record was missed in step 2. Add it to Cloudflare DNS now. Fix is immediate.

**Email stops working after step 4.**
- MX records were missed in step 2. Add them to Cloudflare DNS immediately. Mail servers retry delivery for up to 72 hours so incoming mail is usually recoverable if you fix quickly.

---

## CHECKLIST

Print this, check off as you go:

```
[ ]  STEP 1 — DNS audit file saved (all Namecheap records captured)
[ ]  STEP 2 — Zone added to Cloudflare, all records re-created (except api)
[ ]  STEP 3 — Cloudflare DNS verified against audit, no gaps
[ ]  STEP 4 — Nameservers changed at Namecheap
[ ]  STEP 4 — Propagation confirmed (dig shows Cloudflare NS)
[ ]  STEP 4 — justhodl.ai and www.justhodl.ai still serve the dashboard
[ ]  STEP 5a — Legacy api CNAME deleted from Cloudflare DNS
[ ]  STEP 5b — Worker Custom Domain api.justhodl.ai bound to justhodl-ai-proxy
[ ]  STEP 5c — curl POST to api.justhodl.ai returns AI chat response
[ ]  STEP 6 — All HTML/JS files in repo updated to api.justhodl.ai
[ ]  STEP 6 — Commits pushed to ElMooro/si main
[ ]  STEP 6 — GitHub Pages redeployed, browser test passed
[ ]  STEP 7 — Legacy CloudFront distribution disabled (after 48h)
[ ]  STEP 8 — wrangler.toml updated with custom_domain route
```

---

## POST-MIGRATION BENEFITS

- `api.justhodl.ai` is the branded endpoint.
- `raafouis.workers.dev` no longer leaks into any user-facing URL.
- DNS is managed by Cloudflare — cleaner, free CDN, free DDoS, ability to add WAF rules.
- Future Workers can be bound to `*.justhodl.ai` subdomains with one click.
- Legacy CloudFront distribution at `api.justhodl.ai` can be safely retired.

When you're ready to execute this, say so and I'll be on standby to help with any of the individual steps. Steps 5, 6, and 8 can be partially automated — ping me and I'll script what's scriptable.
