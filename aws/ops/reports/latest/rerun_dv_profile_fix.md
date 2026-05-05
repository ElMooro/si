
# 1) Verify deep-value deployed with /profile fix

- `21:30:58`     /profile fix in deployed code: True

# 2) Re-invoke deep-value

- `21:31:06`     status: 200
- `21:31:06`     body: {"n_universe": 500, "n_qualifying": 57, "n_tier_a": 2, "duration_s": 8.0}
- `21:31:07`   
- `21:31:07`     ── new top_25 (financials excluded) ──
- `21:31:07`       CNC     100.0  DEEP_VALUE_TIER_A         Healthcare
- `21:31:07`       BAC      80.2  DEEP_VALUE_TIER_A         
- `21:31:07`       WFC      72.6  DEEP_VALUE_TIER_B         
- `21:31:07`       HUM      72.4  NET_CASH_WATCH            Healthcare
- `21:31:07`       MRNA     32.0  NET_CASH_WATCH            Healthcare
- `21:31:07`   
- `21:31:07`     ── top excluded (financials/REITs) ──
- `21:31:07`       EG       30.0  FINANCIAL_BOOK_EXCLUDED     Financial Services
- `21:31:07`       AIZ      30.0  FINANCIAL_BOOK_EXCLUDED     Financial Services
- `21:31:07`       MET      30.0  FINANCIAL_BOOK_EXCLUDED     Financial Services
- `21:31:07`       ALL      30.0  FINANCIAL_BOOK_EXCLUDED     Financial Services
- `21:31:07`       L        28.7  FINANCIAL_BOOK_EXCLUDED     Financial Services
- `21:31:07`       PGR      28.4  FINANCIAL_BOOK_EXCLUDED     Financial Services
- `21:31:07`       COF      28.4  FINANCIAL_BOOK_EXCLUDED     Financial Services
- `21:31:07`       SYF      28.3  FINANCIAL_BOOK_EXCLUDED     Financial Services

# 3) Re-aggregate compound signals

- `21:31:08`     ✓ wrote 2656b
- `21:31:08`     feed_stats: {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 5, "eps_velocity": 25}
- `21:31:08`     total: 156, multi: 6, 3+: 0
- `21:31:08`   
- `21:31:08`     ── compound leaderboard ──
- `21:31:08`       CSGP   #2  comp= 220.7  (eps_velocity,insiders)
- `21:31:08`       OXY    #2  comp= 178.4  (nobrainers,smart_money)
- `21:31:08`       HUM    #2  comp= 177.5  (deep_value,smart_money)
- `21:31:08`       WFC    #2  comp= 172.3  (deep_value,smart_money)
- `21:31:08`       BAC    #2  comp= 165.3  (deep_value,smart_money)
- `21:31:08`       FCX    #2  comp= 156.9  (nobrainers,smart_money)

# 4) Send final Telegram digest

- `21:31:08`     message length: 739 chars
- `21:31:08`     ✅ delivered, message_id=677