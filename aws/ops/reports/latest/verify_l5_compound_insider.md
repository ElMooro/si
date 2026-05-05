
# 1) Verify L5 deployed with insider integration

- `19:06:26`     modified: 2026-05-05T19:02:46.000+0000
- `19:06:26`     state: Active  mem=512MB  timeout=600s
- `19:06:26`     ✓ insider data dict: insider_by_ticker = {}
- `19:06:26`     ✓ loads insider S3: data/insider-clusters.json
- `19:06:26`     ✓ prompt section: INSIDER CLUSTER SIGNAL
- `19:06:26`     ✓ helper function: def _insider_block
- `19:06:26`     ✓ passes insider to prompt: build_thesis_prompt(c, cl)

# 2) Force-invoke L5 sync (will take ~60-90s)

- `19:08:46`     status: 200  duration: 139.9s
- `19:08:46`     body keys: ['statusCode', 'body']
- `19:08:46`     inner: {"n_theses": 12, "n_claude_ok": 12, "n_claude_fail": 0, "duration_s": 139.0}

# 3) CloudWatch tail — look for compound-signal hits

- `19:08:46`     ✓ [rationale] loaded 22 insider clusters
- `19:08:46`     no overlap between top nobrainers and insider clusters today (expected — different universes)
- `19:08:46`     thesis lines: 12
- `19:08:46`   
- `19:08:46`     ── full tail (last 25 lines) ──
- `19:08:46`       START RequestId: aa23d450-9ed6-49ee-b3b7-e2fdc8a21b04 Version: $LATEST
- `19:08:46`       [rationale] Layer 5 — nobrainer-rationale starting
- `19:08:46`       [rationale] leaderboard: 25  mu_grade: 15
- `19:08:46`       [rationale] loaded 22 insider clusters
- `19:08:46`       [rationale] writing theses for top 12 above 70.0
- `19:08:46`       [rationale] TX/SLX thesis ok (3384 chars, in=954 out=901, 13.0s)
- `19:08:46`       [rationale] USAR/REMX thesis ok (3366 chars, in=960 out=937, 13.0s)
- `19:08:46`       [rationale] CSTM/REMX thesis ok (2366 chars, in=966 out=679, 8.9s)
- `19:08:46`       [rationale] MT/SLX thesis ok (2522 chars, in=956 out=715, 10.0s)
- `19:08:46`       [rationale] APA/XOP thesis ok (2813 chars, in=963 out=816, 11.4s)
- `19:08:46`       [rationale] TS/SLX thesis ok (2913 chars, in=961 out=825, 12.5s)
- `19:08:46`       [rationale] OVV/XOP thesis ok (2656 chars, in=968 out=792, 11.3s)
- `19:08:46`       [rationale] AAUKF/PICK thesis ok (2733 chars, in=963 out=807, 11.7s)
- `19:08:46`       [rationale] DVN/XOP thesis ok (2580 chars, in=963 out=760, 11.0s)
- `19:08:46`       [rationale] MELI/BOTZ thesis ok (3007 chars, in=969 out=834, 12.2s)
- `19:08:46`       [rationale] TSM/SOXX thesis ok (2996 chars, in=965 out=831, 11.8s)
- `19:08:46`       [rationale] AMAT/SMH thesis ok (2517 chars, in=961 out=724, 10.2s)
- `19:08:46`       [rationale] wrote 53617b to data/nobrainers-rationale.json
- `19:08:46`       [tg] sent ok=True message_id=667
- `19:08:46`       END RequestId: aa23d450-9ed6-49ee-b3b7-e2fdc8a21b04
- `19:08:46`       REPORT RequestId: aa23d450-9ed6-49ee-b3b7-e2fdc8a21b04	Duration: 138999.20 ms	Billed Duration: 139711 ms	Memory Size: 512 MB	Max Memory Used: 102 MB	Init Duration: 711.04 ms

# 4) Read fresh thesis output, find any compound signal in text

- `19:08:46`     generated_at: 2026-05-05T19:08:45.932424+00:00
- `19:08:46`     n_theses: 12  n_ok: 12  n_fail: 0
- `19:08:46`     theses mentioning insider buying: 3/12
- `19:08:46`       OVV: 2656 chars, mentions insiders
- `19:08:46`       AAUKF: 2733 chars, mentions insiders
- `19:08:46`       MELI: 3007 chars, mentions insiders