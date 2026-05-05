
# 1) Pull SEC daily index (today + last 2 biz days)

- `18:34:43`     2026-05-05: 0 Form 4 filings
- `18:34:43`     2026-05-04: 2000 Form 4 filings
- `18:34:43`     TOTAL: 2000 filings

# 2) Fetch + parse 50 — verbose per-reject reasons

- `18:34:43`     fetching/parsing 50 samples (~12s minimum)…
- `18:34:55`   
- `18:34:55`     TOTAL: 50  OK_BUYS: 7
- `18:34:55`   
- `18:34:55`     ── fetch failures ──
- `18:34:55`   
- `18:34:55`     ── parse rejections ──
- `18:34:55`       no_open_market_buy                  39
- `18:34:55`       holdings_only                       4
- `18:34:55`   
- `18:34:55`     ── samples (one per reason) ──
- `18:34:55`       PARSE:no_open_market_buy         TACT: ['M/A']
- `18:34:55`       OK:ok                            PS: $19,015,801 (['J/A', 'P/A', 'P/A', 'P/A', 'J/A', 'J/A', 'A/A', 'G/D', 'D/D'])
- `18:34:55`       PARSE:holdings_only              AMN (0 holdings)

# 3) Implications

- `18:34:55`     Buy-extraction rate: 14.0% — 7 of 50
- `18:34:55`     
- `18:34:55`     Most Form 4 filings are NOT open-market buys. They include:
- `18:34:55`     - Tax withholdings on RSU vest (code F)
- `18:34:55`     - Restricted stock grants (code A, but ad=A and price=$0)
- `18:34:55`     - Option exercises (code M)
- `18:34:55`     - Sells (code S, ad=D)
- `18:34:55`     - Holdings updates (no transactions)
- `18:34:55`     - Derivative-only filings
- `18:34:55`     
- `18:34:55`     A 5-15% true-buy rate is NORMAL. To get more buys to score, increase MAX_FILINGS_TO_PARSE.