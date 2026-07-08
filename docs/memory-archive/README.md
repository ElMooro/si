# docs/memory-archive — the fleet-state knowledge base

Created 2026-07-08 (Khalid-approved memory diet). Each file is the VERBATIM
text of a Claude memory edit that was migrated here to cut per-chat token
overhead and free memory slots. The memory slots now hold one-line pointers
to these files.

Contract:
- These files are the authoritative archive. NOTHING was deleted — content
  moved from memory into git, then the memory slot was shrunk. Git history
  additionally preserves every version.
- **Grep this directory before building anything** (`grep -ril <topic>
  docs/memory-archive/`). The audit-first / never-rebuild rule applies with
  this directory as a primary source alongside the repo and past chats.
- Some snapshots contain live state that has since evolved (e.g. counts,
  "Last ops=" markers, provider status). The repo + STATE.md + engine
  registry are the source of truth for CURRENT state; these files are the
  source of truth for design decisions, schemas, gotchas, and history.
- Kept (NOT migrated) memory edits: the ⛔ MASTER BOOTSTRAP card, the Keys
  edit, the screener protection, and the behavioral rules (pressure-test,
  auto-continue, build-rule, session-start, deploy-verify-fix).

Index: 01 options/GEX fleet · 03 capital-flow radar · 05 CFTC agent ·
06 API auth tiers · 07 portfolio manager · 09 ops-doctrine archive +
design-audit/coverage arcs · 10 edge-measurement stack · 11 morning
intelligence · 12 LLM config + cost governance · 13 ship loop (full
detail incl. pages self-heal) · 14 retail edges · 15 CF worker proxy ·
16 signal board · 17 ship gotchas/constants · 18 research terminal +
chart layer + 298-page audit · 19 crisis KB + eurodollar stack ·
20 data-infra hardening · 21 multi-user SaaS · 24 canary grid + site
shell + gap sweep + research desk v2 · 25 data entitlements + AI
engines + basket verdict + gotchas · 29 firm risk + RORO ·
30 recent arcs 2026-07 (compass/IR/theory-stack/flows/orphan-triage).
