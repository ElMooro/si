# ops 4407 — P0 fixes (Perplexity audit) — PASS — CSP fixed, 1/3 stalled feeds healed, audit acked
- CSP: DEPLOYED — plumbing.html regional S3 URL -> same-origin
- stalled feeds: {
 "data/plumbing-stress.json": {
  "healed": false,
  "writer": null,
  "age_before": 117.7,
  "age_after": null,
  "rebound": null
 },
 "data/auction-tenor-signals.json": {
  "healed": true,
  "writer": "justhodl-tenor-signal-interpreter",
  "age_before": 117.5,
  "age_after": 0.0,
  "rebound": null
 },
 "data/page-ai-live.json": {
  "healed": false,
  "writer": null,
  "age_before": 742.7,
  "age_after": null,
  "rebound": null
 }
}
- plumbing-history age: 117.7
- NaN: central fmt()/fmtNum()/fmtPct() already guard null/NaN; the 3+2 NaN leaks are INLINE arithmetic bypassing them (e.g. (a/b).toFixed() computed in a template literal before formatting). Needs per-panel 
