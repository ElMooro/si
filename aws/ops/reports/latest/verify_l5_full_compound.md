
# 1) Verify deployed L5 has all 3 signal sources

- `19:38:26`     modified: 2026-05-05T19:02:46.000+0000
- `19:38:26`     ✓ L4 layer: data/nobrainers.json
- `19:38:26`     ✓ insider load: data/insider-clusters.json
- `19:38:26`     ❌ smart-money load: data/smart-money-clusters.json
- `19:38:26`     ✓ insider helper: def _insider_block
- `19:38:26`     ❌ smart-money helper: def _smart_money_block
- `19:38:26`     ✓ insider prompt section: INSIDER CLUSTER SIGNAL
- `19:38:26`     ❌ smart-money prompt section: 13F SMART-MONEY CLUSTER SIGNAL
- `19:38:26`     ❌ smart-money dict: smart_money_by_ticker = {}
- `19:38:26`     ❌ passes both clusters: build_thesis_prompt(c, cl, sm)

# 2) Force-invoke L5 (sync, ~140s)

- `19:40:44`     status: 200  duration: 138.0s
- `19:40:44`     body: {"statusCode": 200, "body": "{\"n_theses\": 12, \"n_claude_ok\": 12, \"n_claude_fail\": 0, \"duration_s\": 137.3}"}

# 3) Tail logs — find compound signals + load lines

- `19:40:44`     load lines:
- `19:40:44`       [rationale] loaded 22 insider clusters
- `19:40:44`   
- `19:40:44`     COMPOUND signal hits (0):
- `19:40:44`   
- `19:40:44`     ── full tail (last 25) ──
- `19:40:44`       START RequestId: 8990afc2-5ff0-415b-b0b0-1ae5a37eca84 Version: $LATEST
- `19:40:44`       [rationale] Layer 5 — nobrainer-rationale starting
- `19:40:44`       [rationale] leaderboard: 25  mu_grade: 15
- `19:40:44`       [rationale] loaded 22 insider clusters
- `19:40:44`       [rationale] writing theses for top 12 above 70.0
- `19:40:44`       [rationale] TX/SLX thesis ok (3244 chars, in=954 out=900, 12.5s)
- `19:40:44`       [rationale] USAR/REMX thesis ok (2651 chars, in=960 out=714, 10.3s)
- `19:40:44`       [rationale] CSTM/REMX thesis ok (2845 chars, in=966 out=846, 11.6s)
- `19:40:44`       [rationale] MT/SLX thesis ok (2889 chars, in=956 out=764, 11.0s)
- `19:40:44`       [rationale] APA/XOP thesis ok (2805 chars, in=963 out=819, 11.5s)
- `19:40:44`       [rationale] TS/SLX thesis ok (2393 chars, in=961 out=673, 9.7s)
- `19:40:44`       [rationale] OVV/XOP thesis ok (3026 chars, in=968 out=877, 12.3s)
- `19:40:44`       [rationale] AAUKF/PICK thesis ok (2660 chars, in=963 out=769, 10.6s)
- `19:40:44`       [rationale] DVN/XOP thesis ok (2549 chars, in=963 out=762, 10.4s)
- `19:40:44`       [rationale] MELI/BOTZ thesis ok (2851 chars, in=969 out=869, 11.9s)
- `19:40:44`       [rationale] TSM/SOXX thesis ok (2610 chars, in=965 out=741, 10.5s)
- `19:40:44`       [rationale] AMAT/SMH thesis ok (3020 chars, in=961 out=895, 12.6s)
- `19:40:44`       [rationale] wrote 53299b to data/nobrainers-rationale.json
- `19:40:44`       [tg] sent ok=True message_id=669
- `19:40:44`       END RequestId: 8990afc2-5ff0-415b-b0b0-1ae5a37eca84
- `19:40:44`       REPORT RequestId: 8990afc2-5ff0-415b-b0b0-1ae5a37eca84	Duration: 137281.25 ms	Billed Duration: 137819 ms	Memory Size: 512 MB	Max Memory Used: 102 MB	Init Duration: 537.47 ms

# 4) Inspect fresh thesis output for compound mentions

- `19:40:45`     generated_at: 2026-05-05T19:40:43.940186+00:00
- `19:40:45`     n_theses: 12
- `19:40:45`     theses mentioning insider buying: 0/12: []
- `19:40:45`     theses mentioning smart-money: 0/12: []

# 5) Final compound-signal summary across the 3 systems

- `19:40:45`     Nobrainers (top 25): ['AAUKF', 'AIN', 'AMAT', 'APA', 'CRM', 'CSTM', 'DVN', 'FCX', 'LTHM', 'MELI', 'MT', 'NEM', 'OVV', 'OXY', 'REEMF', 'RES', 'RIO', 'RIVN', 'SLI', 'TS', 'TSM', 'TX', 'UPS', 'USAR', 'WTTR']
- `19:40:45`     Insiders (≥50): ['AVLN', 'CSGP', 'EPAM', 'FGBI', 'FND', 'NONE', 'NWBI', 'OPCH', 'PSUS', 'SPGI', 'SRAD', 'SUNE']
- `19:40:45`     Smart Money (≥55): ['ALB', 'ALLY', 'AMGN', 'AMP', 'AMZN', 'AVGO', 'AXP', 'CAH', 'CHKP', 'EEM', 'FDX', 'GOOG', 'GOOGL', 'HD', 'LLY', 'MA', 'MMC', 'MOH', 'NKE', 'NVDA', 'STLA', 'TSLA', 'V', 'VRTX', 'VST', 'WMT']
- `19:40:45`   
- `19:40:45`     ── COMPOUND OVERLAPS ──
- `19:40:45`     Nobrainer ∩ Insider:    []
- `19:40:45`     Nobrainer ∩ SmartMoney: []
- `19:40:45`     Insider ∩ SmartMoney:   []
- `19:40:45`     ALL THREE:              []