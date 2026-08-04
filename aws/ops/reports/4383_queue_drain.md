# ops 4383 — queue drain — PARTIAL — see fields
- cloudflare: HTTPError: HTTP Error 403: Forbidden
- live headers: {"Server": "cloudflare", "CF-RAY": "a260ac61a84a9d92-LAX"}
- csp_header_live: False
- writers: {"data/calibration-snapshot.json": ["justhodl-ai-brief", "justhodl-allocator", "justhodl-calibration-snapshot", "justhodl-khalid-adaptive"], "data/asymmetric-scorer.json": ["justhodl-ai-brief", "justhodl-asymmetric-scorer", "justhodl-best-ideas", "justhodl-deep-value-screener", "justhodl-history-snapshotter", "justhodl-master-ranker", "justhodl-risk-radar"], "data/alpha-triage.json": ["justhodl-in
- restarts: [{"feed": "data/calibration-snapshot.json", "fn": "justhodl-ai-brief", "had_rules": ["justhodl-ai-brief-4h"], "fired": true}, {"feed": "data/asymmetric-scorer.json", "fn": "justhodl-ai-brief", "had_rules": ["justhodl-ai-brief-4h"], "fired": true}, {"feed": "data/alpha-triage.json", "fn": "justhodl-inverse-harvester", "had_rules": [], "fired": true}]
- feed ages after: {"data/alpha-triage.json": 429.97, "data/calibration-snapshot.json": 102.62, "data/asymmetric-scorer.json": 82.55}
- fix turns: {"csp": true, "feeds": true}
