# Comprehensive system audit

**Status:** success  
**Duration:** 22.7s  
**Finished:** 2026-04-27T22:32:27+00:00  

## Log
## 1. Load expectations

- `22:32:05`   loaded 63 components
- `22:32:05`     29 S3 specs, 26 Lambda specs
## 2. S3 file freshness

- `22:32:06`   S3 issues: 2 of 29
- `22:32:06`     [missing        ] data/options-gamma.json
- `22:32:06`         404 not found
- `22:32:06`     [size_too_small ] data/exchange-flows.json
- `22:32:06`         size 244 < expected 5000
## 3. Lambda errors and idleness

- `22:32:27`   Lambda issues: 1 of 26
- `22:32:27`     [dead              ] justhodl-calibrator
- `22:32:27`         no invocations in 24h
- `22:32:27`         24h: 0 inv | 0 err | avg 0ms | max 0ms
## 4. HTML page broken references

- `22:32:27`   2 broken S3 references in HTML
- `22:32:27`     signals.html  →  data/options-gamma.json
- `22:32:27`     today.html  →  data/options-gamma.json
## 5. Summary

- `22:32:27`   S3 files:        29 tracked, 2 issues
- `22:32:27`   Lambdas:         26 tracked, 1 issues
- `22:32:27`   HTML refs broken: 2
- `22:32:27`   TOTAL ISSUES:    5
- `22:32:27` 
  ✓ structured artifact: aws/ops/audit/system_issues_20260427_223227.json
