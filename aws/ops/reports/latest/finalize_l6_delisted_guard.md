
# 1) Patch source — add early-skip guard inside the loop

- `16:52:01`     ✓ added guard inside for-loop
- `16:52:01`     source written: 15,196 chars

# 2) Deploy

- `16:52:01`     zip: 17,228b
- `16:52:05`     ✅ deployed mod=2026-05-05T16:52:02.000+0000

# 3) Force-invoke and check no LTHM error

- `16:52:07`     status: 200
- `16:52:07`     body: {"statusCode": 200, "body": "{\"n_logged\": 0, \"n_skipped\": 25, \"n_errors\": 0, \"n_total_ever\": 24, \"duration_s\": 0.7}"}
- `16:52:07`     ── tail ──
- `16:52:07`       [track] leaderboard: 25  candidates >= 60.0: 25
- `16:52:07`       [track] regime: {'khalid_score': 48, 'regime': 'NEUTRAL'}
- `16:52:07`       [track] SKIP TX/SLX — dedup (last logged 1.9h ago, score 86.5→86.5)
- `16:52:07`       [track] SKIP USAR/REMX — dedup (last logged 1.9h ago, score 85.8→85.8)
- `16:52:07`       [track] SKIP CSTM/REMX — dedup (last logged 1.9h ago, score 83.0→83.0)
- `16:52:07`       [track] SKIP MT/SLX — dedup (last logged 1.9h ago, score 82.1→82.1)
- `16:52:07`       [track] SKIP APA/XOP — dedup (last logged 1.9h ago, score 81.8→81.8)
- `16:52:07`       [track] SKIP TS/SLX — dedup (last logged 1.9h ago, score 81.5→81.5)
- `16:52:07`       [track] SKIP OVV/XOP — dedup (last logged 1.9h ago, score 80.9→80.9)
- `16:52:07`       [track] SKIP AAUKF/PICK — dedup (last logged 1.9h ago, score 80.8→80.8)
- `16:52:07`       [track] SKIP DVN/XOP — dedup (last logged 1.9h ago, score 80.4→80.4)
- `16:52:07`       [track] SKIP MELI/BOTZ — dedup (last logged 1.9h ago, score 79.4→79.4)
- `16:52:07`       [track] SKIP TSM/SOXX — dedup (last logged 1.9h ago, score 79.2→79.2)
- `16:52:07`       [track] SKIP AMAT/SMH — dedup (last logged 1.9h ago, score 78.8→78.8)
- `16:52:07`       [track] SKIP OXY/XOP — dedup (last logged 1.9h ago, score 78.5→78.5)
- `16:52:07`       [track] SKIP RES/OIH — dedup (last logged 1.9h ago, score 78.0→78.0)
- `16:52:07`       [track] SKIP NEM/PICK — dedup (last logged 1.9h ago, score 77.6→77.6)
- `16:52:07`       [track] SKIP RIO/SLX — dedup (last logged 1.9h ago, score 77.6→77.6)
- `16:52:07`       [track] SKIP UPS/BOTZ — dedup (last logged 1.9h ago, score 77.5→77.5)
- `16:52:07`       [track] SKIP AIN/ROBO — dedup (last logged 1.9h ago, score 77.0→77.0)
- `16:52:07`       [track] SKIP SLI/LIT — dedup (last logged 1.9h ago, score 76.8→76.8)
- `16:52:07`       [track] SKIP LTHM/LIT — delisted/merged
- `16:52:07`       [track] SKIP FCX/PICK — dedup (last logged 1.9h ago, score 76.2→76.2)
- `16:52:07`       [track] SKIP RIVN/LIT — dedup (last logged 1.9h ago, score 75.3→75.3)
- `16:52:07`       [track] SKIP CRM/AIQ — dedup (last logged 1.9h ago, score 75.2→75.2)
- `16:52:07`       [track] SKIP REEMF/REMX — dedup (last logged 1.9h ago, score 74.8→74.8)
- `16:52:07`       [track] SKIP WTTR/OIH — dedup (last logged 1.9h ago, score 74.4→74.4)
- `16:52:07`       [track] done — logged=0 skipped=25 err=0 (total ever: 24)
- `16:52:07`       END RequestId: 4bc8f31d-5640-4ab5-8ead-ddcfd43b4350
- `16:52:07`       REPORT RequestId: 4bc8f31d-5640-4ab5-8ead-ddcfd43b4350	Duration: 663.62 ms	Billed Duration: 1295 ms	Memory Size: 512 MB	Max Memory Used: 114 MB	Init Duration: 630.74 ms	
- `16:52:07`     ✅ no baseline-price-unavailable error