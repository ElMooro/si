# Memory archive — edit #07 (migrated verbatim 2026-07-08, Khalid-approved memory diet)

Source: Claude memory edit #07. This file is the authoritative archive; the memory slot now holds a one-line pointer here. Grep this directory before building anything fleet-related.

---

Portfolio Manager LIVE 2026-05-16 (ops 724-734). Book=DDB justhodl-portfolio pk=POSITION. portfolio-manager.html → Cloudflare Worker route api.justhodl.ai/portfolio-admin (gated by Manager PIN x-mgr-pass, SSM /justhodl/portfolio-admin/manager-pass, delivered via Telegram); Worker injects the infra token → justhodl-portfolio-admin Lambda; mutations auto-trigger portfolio-snapshot → portfolio-risk → pm-decision. Gotcha: deploy-workers needs Node 22; verify Workers via *.workers.dev.
