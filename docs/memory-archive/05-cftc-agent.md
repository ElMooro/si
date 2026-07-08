# Memory archive — edit #05 (migrated verbatim 2026-07-08, Khalid-approved memory diet)

Source: Claude memory edit #05. This file is the authoritative archive; the memory slot now holds a one-line pointer here. Grep this directory before building anything fleet-related.

---

CFTC Futures Positioning Agent: Lambda=cftc-futures-positioning-agent, URL=https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/, Data=CFTC.gov COT (TFF+Disagg+Legacy) + Polygon futures, 29 contracts across 7 categories, auto-updates Fridays 6PM UTC via EventBridge rule cftc-cot-weekly-update. Endpoints: /cot/all, /cot/{CONTRACT}, /cot/category/{CAT}, /futures, /analysis, /signals, /debug/{CONTRACT}
