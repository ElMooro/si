# Historical replay / backtest mode

Fusion is a pure function: `run_fusion(snapshot, registry_doc, universe_doc, flags, reliability, corr, prior, now)`.
Given the archived signal stream (`data/jhsignal/archive/YYYY/MM/DD/*.jsonl.gz`, append-only, chronological) a
replay at time T:
1. loads every archived signal with `published_at <= T`,
2. rebuilds the snapshot with `jh_state_store.build_snapshot(signals, now=T)` (newest data_asof per key wins,
   freshness computed at T -- nothing published after T is visible: no look-ahead),
3. calls `run_fusion(..., now=T, prior=<result at the previous step>)`.
Reliability and correlation inputs must also be point-in-time (`data/engine-trust.json` and the orthogonality doc
are versioned in S3; use the version current at T).

Tested in `test_fusion_v1.py::TestRunLevel::test_replay_is_deterministic_and_point_in_time` (same inputs -> same
result; 20 days later -> only decay changes). The batch harness that walks the archive day by day and grades the
fused decision vs realised returns is Release 5 (it plugs into the existing outcome-checker windows).
