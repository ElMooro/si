# ops 4380 — perpetual audit loop — PASS — loop live on rate(2 hours), 72 findings banked, handoff active
- bus=updated loop=created schedule=bound rate(2 hours)
- inventory: {"engines": 0, "pages": 476, "manifest_fns": 276}
- shard runs: [{"shard": {"engines": 0, "pages": 15}, "new_findings": 36, "open_total": 36, "critical": 0, "filed_to_bus": [{"id": "247b557e9ceb", "posted": true, "err": null}, {"id": "aafde21c9450", "posted": true, "err": null}, {"id": "db3b42d153e5", "posted": true, "err": null}], "telegram": false}, {"shard": {"engines": 0, "pages": 15}, "new_findings": 0, "open_total": 72, "critical": 0, "filed_to_bus": [], "telegram": false}]
- handoff coverage: {"engines": "0/0", "pages": "30/476", "cycle": 1, "runs": 2} | open=72 crit=0

## TOP OPEN FINDINGS
- [warn] page:0dte/index.html — xss_heuristic: 7 innerHTML sites, zero escape helpers
- [info] page:0dte/index.html — no_csp: no CSP meta on a page with dynamic HTML
- [info] page:0dte/index.html — bare_fetch: fetch without timeout/abort
- [info] page:13f.html — no_csp: no CSP meta on a page with dynamic HTML
- [info] page:13f.html — bare_fetch: fetch without timeout/abort
- [info] page:accumulation.html — no_csp: no CSP meta on a page with dynamic HTML
- [info] page:accumulation.html — bare_fetch: fetch without timeout/abort
- [warn] page:accuracy.html — feed_stale:data/calibration-snapshot.json: data/calibration-snapshot.json is 102.2h old

## AUDIT THREAD (audit-loop-main)

### claude-audit -> perplexity [propose] 2026-08-04T21:06:32+00:00
[warn] page:0dte/index.html — xss_heuristic: 7 innerHTML sites, zero escape helpers (id 247b557e9ceb). Verify or refute with evidence.
evidence: [{"kind": "url", "ref": "https://justhodl.ai/0dte/index.html", "resolved": true}]

### claude-audit -> perplexity [propose] 2026-08-04T21:06:33+00:00
[warn] page:accuracy.html — feed_stale:data/calibration-snapshot.json: data/calibration-snapshot.json is 102.2h old (id aafde21c9450). Verify or refute with evidence.
evidence: [{"kind": "log", "ref": "data/calibration-snapshot.json", "resolved": true}]

### claude-audit -> perplexity [propose] 2026-08-04T21:06:33+00:00
[warn] page:accuracy.html — xss_heuristic: 7 innerHTML sites, zero escape helpers (id db3b42d153e5). Verify or refute with evidence.
evidence: [{"kind": "url", "ref": "https://justhodl.ai/accuracy.html", "resolved": true}]

### claude-audit -> perplexity [propose] 2026-08-04T21:07:01+00:00
[warn] page:alpha-families.html — feed_stale:data/alpha-triage.json: data/alpha-triage.json is 429.5h old (id 996e1e22ad8b). Verify or refute with evidence.
evidence: [{"kind": "log", "ref": "data/alpha-triage.json", "resolved": true}]

### claude-audit -> perplexity [propose] 2026-08-04T21:07:02+00:00
[warn] page:alpha-scoreboard.html — feed_stale:data/asymmetric-scorer.json: data/asymmetric-scorer.json is 82.1h old (id 1d9655c463a0). Verify or refute with evidence.
evidence: [{"kind": "log", "ref": "data/asymmetric-scorer.json", "resolved": true}]

### claude-audit -> perplexity [propose] 2026-08-04T21:07:02+00:00
[warn] page:alpha-scoreboard.html — xss_heuristic: 3 innerHTML sites, zero escape helpers (id c5d97fb80d1f). Verify or refute with evidence.
evidence: [{"kind": "url", "ref": "https://justhodl.ai/alpha-scoreboard.html", "resolved": true}]
