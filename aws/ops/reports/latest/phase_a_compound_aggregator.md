
# 0) Build Lambda directory + write source

- `21:45:36`     wrote aws/lambdas/justhodl-compound-aggregator/source/lambda_function.py: 13693 chars

# 1) Validate syntax

- `21:45:36`     ✓ valid python

# 2) Build deployment zip

- `21:45:36`     zip: 13,865b

# 3) Create or update Lambda

- `21:45:36`     creating new
- `21:45:40`     ✓ ready, mem=512MB to=120s

# 4) Schedule hourly

- `21:45:41`     ✓ permission added
- `21:45:41`     rule: justhodl-compound-aggregator-hourly expr=rate(1 hour)

# 5) Smoke invoke

- `21:45:43`     status: 200  duration: 1.8s
- `21:45:43`     body: {"statusCode": 200, "body": "{\"n_compound\": 6, \"n_3_plus\": 0, \"n_alerts\": 1, \"duration_s\": 0.4}"}
- `21:45:43`     ── tail ──
- `21:45:43`       [compound] nobrainers: 25 entries
- `21:45:43`       [compound] insiders: 22 entries
- `21:45:43`       [compound] smart_money: 85 entries
- `21:45:43`       [compound] deep_value: 5 entries
- `21:45:43`       [compound] eps_velocity: 25 entries
- `21:45:43`       [compound] aggregated: 156 names, 6 multi-signal
- `21:45:43`       [compound] new alerts this run: 1
- `21:45:43`       [compound] wrote 3294b to data/compound-signals.json
- `21:45:43`       [compound] wrote state: 1 alerted_keys tracked
- `21:45:43`       [compound] alert send: ok=True info=683
- `21:45:43`       END RequestId: 6120a7a6-bce1-49d6-998b-a3d92909ac3a
- `21:45:43`       REPORT RequestId: 6120a7a6-bce1-49d6-998b-a3d92909ac3a	Duration: 994.16 ms	Billed Duration: 1621 ms	Memory Size: 512 MB	Max Memory Used: 101 MB	Init Duration: 626.03 ms

# 6) Verify output

- `21:45:43`     schema: 2
- `21:45:43`     feed_stats: {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 5, "eps_velocity": 25}
- `21:45:43`     stats: {"n_total_names": 156, "n_multi_signal": 6, "n_3_plus": 0, "n_compound_over_200": 1, "n_compound_over_300": 0}