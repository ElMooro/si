# Memory archive — edit #15 (migrated verbatim 2026-07-08, Khalid-approved memory diet)

Source: Claude memory edit #15. This file is the authoritative archive; the memory slot now holds a one-line pointer here. Grep this directory before building anything fleet-related.

---

Cloudflare Worker proxy (2026-04-22): justhodl-ai-proxy at https://justhodl-ai-proxy.raafouis.workers.dev forwards browser → justhodl-ai-chat Lambda. Holds AI_CHAT_TOKEN as encrypted Worker Secret from AWS SSM. Source at cloudflare/workers/justhodl-ai-proxy/. Auto-deploys via .github/workflows/deploy-workers.yml. CF Account: 2e120c8358c6c85dcaba07eb16947817. dex.html uses this Worker for AI chat.
