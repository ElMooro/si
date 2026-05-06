- `09:46:38`     source: 18706 chars (v2)
- `09:46:38`       ✓ v2.0 — daily-index edition
- `09:46:38`       ✓ fetch_master_idx
- `09:46:38`       ✓ fetch_subject_from_filing
- `09:46:38`       ✓ SUBJECT[\s\-]*COMPANY
- `09:46:38`       ✓ cascade investment
- `09:46:38`       ✓ RESOLVE_SUBJECTS

# 1) Force-deploy v2 + bump memory/timeout (subject resolution is HTTP-heavy)

- `09:46:44`     ✓ deployed at 2026-05-06T09:46:43.000+0000 mem=1024MB to=600s

# 2) Smoke invoke (parses 5 days of master.idx + resolves subjects)

- `09:46:45`     status: 200, dur: 1.1s
- `09:46:45`     body: {"errorMessage": "HTTP Error 403: Forbidden", "errorType": "HTTPError", "requestId": "6fd09ada-fcb2-42a3-85de-c3bcc7dc7898", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 330, in lambda_handler\n    filings = fetch_master_idx(check_dt)\n", "  File \"/var/task/lambda_function.py\", li
- `09:46:45`           with urllib.request.urlopen(req, timeout=timeout) as r:
- `09:46:45`         File "/var/lang/lib/python3.12/urllib/request.py", line 215, in urlopen
- `09:46:45`           return opener.open(url, data, timeout)
- `09:46:45`         File "/var/lang/lib/python3.12/urllib/request.py", line 521, in open
- `09:46:45`           response = meth(req, response)
- `09:46:45`         File "/var/lang/lib/python3.12/urllib/request.py", line 630, in http_response
- `09:46:45`           response = self.parent.error(
- `09:46:45`         File "/var/lang/lib/python3.12/urllib/request.py", line 559, in error
- `09:46:45`           return self._call_chain(*args)
- `09:46:45`         File "/var/lang/lib/python3.12/urllib/request.py", line 492, in _call_chain
- `09:46:45`           result = func(*args)
- `09:46:45`         File "/var/lang/lib/python3.12/urllib/request.py", line 639, in http_error_default
- `09:46:45`           raise HTTPError(req.full_url, code, msg, hdrs, fp)
- `09:46:45`       END RequestId: 6fd09ada-fcb2-42a3-85de-c3bcc7dc7898
- `09:46:45`       REPORT RequestId: 6fd09ada-fcb2-42a3-85de-c3bcc7dc7898	Duration: 315.85 ms	Billed Duration: 878 ms	Memory Size: 1024 MB	Max Memory Used: 103 MB	Init Duration: 561.44 ms

# 3) Inspect output

- `09:46:45`     generated_at: 2026-05-06T09:28:47+00:00
- `09:46:45`     stats: {"n_filings_total": 1, "n_unique_tickers": 0, "n_multi_activist": 0, "n_new_filings": 1, "n_new_tier_a": 0, "n_new_tier_b": 0, "n_in_universe": 0}
- `09:46:45`   
- `09:46:45`     ── TOP 15 FILINGS BY SCORE ──
- `09:46:45`         ?       SC 13D              SC 13D/A - GENCO SHIPPING &amp  tier=                    score= 25  level=NOTABLE
- `09:46:45`   
- `09:46:45`     ── TOP 10 FILINGS WITH IN-UNIVERSE TICKER ──