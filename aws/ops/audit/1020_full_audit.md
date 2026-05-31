# JustHodl.AI — Full System Audit

Generated: 2026-05-31T12:01:10.135108+00:00
Account: 857687956942 · Region: us-east-1

## System Scale

- **Lambdas**:           430
- **Scheduled Lambdas**: 47
- **S3 data keys**:       5001
- **DDB tables**:         18
- **SSM parameters**:     73

## Issues Summary

- **dead_unscheduled**: 367
- **stale_outputs**: 6
- **expensive_invokes**: 3
- **high_error_rate**: 2
- **broken_scheduled**: 0
- **low_use_scheduled**: 0
- **disabled_rules**: 0
- **no_recent_logs**: 0

## Top Issues (Detail)

### dead_unscheduled (367)
- {"name": "justhodl-merger-arb", "last_modified": "2026-05-30T18:27:06.000+0000", "code_size_kb": 6.5}
- {"name": "justhodl-data-collector", "last_modified": "2026-05-30T18:27:08.000+0000", "code_size_kb": 1.2}
- {"name": "justhodl-leading-markets", "last_modified": "2026-05-30T18:27:10.000+0000", "code_size_kb": 5.5}
- {"name": "justhodl-vix-backwardation-trigger", "last_modified": "2026-05-30T18:27:15.000+0000", "code_size_kb": 7.3}
- {"name": "justhodl-synthetic-monitor", "last_modified": "2026-05-30T18:26:55.000+0000", "code_size_kb": 3.0}
- {"name": "justhodl-stablecoin-flow", "last_modified": "2026-05-30T18:27:18.000+0000", "code_size_kb": 7.1}
- {"name": "justhodl-asymmetric-scorer", "last_modified": "2026-05-30T18:27:23.000+0000", "code_size_kb": 8.6}
- {"name": "ofrapi", "last_modified": "2026-05-22T11:00:47.000+0000", "code_size_kb": 21.4}
- {"name": "openbb-websocket-broadcast", "last_modified": "2026-05-22T10:55:26.000+0000", "code_size_kb": 3.5}
- {"name": "justhodl-fedwatch-rate-probability", "last_modified": "2026-05-30T18:27:25.000+0000", "code_size_kb": 5.4}
- {"name": "justhodl-cds-proxy", "last_modified": "2026-05-30T18:27:29.000+0000", "code_size_kb": 4.1}
- {"name": "justhodl-esi", "last_modified": "2026-05-30T18:27:31.000+0000", "code_size_kb": 4.0}
- {"name": "justhodl-momentum-scanner", "last_modified": "2026-05-30T18:27:38.000+0000", "code_size_kb": 3.5}
- {"name": "justhodl-divergence-interpreter", "last_modified": "2026-05-30T18:27:42.000+0000", "code_size_kb": 5.8}
- {"name": "bls-labor-agent", "last_modified": "2026-05-22T10:55:26.000+0000", "code_size_kb": 2.1}
- … and 352 more (see JSON report)

### stale_outputs (6)
- {"key": "data/_archive/dex-scanner-data.json", "modified": "2026-05-17T19:01:58+00:00", "age_days": 13.7, "size_kb": 140.9}
- {"key": "data/_archive/institutional-convergence.json", "modified": "2026-05-17T19:01:58+00:00", "age_days": 13.7, "size_kb": 1.3}
- {"key": "data/_archive/pre-pump-calibration.json", "modified": "2026-05-17T19:01:58+00:00", "age_days": 13.7, "size_kb": 2.8}
- {"key": "data/_archive/skew.json", "modified": "2026-05-17T19:01:58+00:00", "age_days": 13.7, "size_kb": 0.3}
- {"key": "data/_freshness-status.json", "modified": "2026-05-22T13:09:24+00:00", "age_days": 9.0, "size_kb": 3.2}
- {"key": "data/_freshness-manifest.json", "modified": "2026-05-23T15:20:01+00:00", "age_days": 7.9, "size_kb": 3.4}

### high_error_rate (2)
- {"name": "justhodl-liquidity-credit-engine", "error_rate_pct": 32.43, "errors_7d": 12, "invocations_7d": 37}
- {"name": "justhodl-crisis-plumbing", "error_rate_pct": 21.21, "errors_7d": 7, "invocations_7d": 33}

### expensive_invokes (3)
- {"name": "justhodl-crypto-opportunities", "duration_avg_ms": 81418.7, "timeout_s": 240, "memory_mb": 512}
- {"name": "justhodl-liquidity-credit-engine", "duration_avg_ms": 63626.2, "timeout_s": 300, "memory_mb": 512}
- {"name": "justhodl-outcome-checker", "duration_avg_ms": 62668.1, "timeout_s": 300, "memory_mb": 256}