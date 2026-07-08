# Memory archive — edit #21 (migrated verbatim 2026-07-08, Khalid-approved memory diet)

Source: Claude memory edit #21. This file is the authoritative archive; the memory slot now holds a one-line pointer here. Grep this directory before building anything fleet-related.

---

MULTI-USER SAAS (2026-06-03): JustHodl.AI going public, ALWAYS build per-user not single-user. Stack: Supabase (email/Google/Facebook) + Stripe (FREE/PRO/ELITE) + Cloudflare KV. Built auth.js, auth-config.js, pricing.html. chart-pro.html watchlist syncs per-user via storageKey(). KV ns justhodl-user-data 43a046649f05407dac2b9e50a489e2e9 = USER_DATA on data-proxy; routes /userdata/:uid /quotes /tv-search. PENDING: Khalid makes Supabase+OAuth+Stripe accounts to wire login+billing live.
