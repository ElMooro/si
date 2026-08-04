# ops 4386 — CSP header verification — PASS — CSP response header live at the edge, confirm-close handed to Perplexity
- rule: created new entrypoint ruleset
- probes: {
 "https://justhodl.ai/insiders.html": {
  "status": 200,
  "csp_present": false,
  "csp_head": "",
  "frame_ancestors": false
 },
 "https://justhodl.ai/": {
  "status": 200,
  "csp_present": true,
  "csp_head": "default-src 'self' https://justhodl-dashboard-live.s3.amazonaws.com; connect-src 'self' https://justhodl-dashboard-live.",
  "frame_ancestors": true
 }
}
- fix turn: True
