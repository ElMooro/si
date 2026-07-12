## 1. Gate on CODE truth (LastModified), settle updates

**Status:** success  
**Duration:** 16.7s  
**Finished:** 2026-07-12T02:39:38+00:00  

## Data

| check | code_sha | crisis | handler | jolts_openings_k | last_modified | memory | n_fails | n_warns | national_live | ok | payrolls_mom_chg_k | quits_rate | runtime | sahm | states_live | temp_help_yoy_pct | timeout | u6 | unrate | unrate_period | value | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | pB9bw4QY+bBX |  | lambda_bls_agent.lambda_handler |  | 2026-07-12T02:38:01.000+0000 | 512 |  |  |  |  |  |  | python3.9 |  |  |  | 300 |  |  |  |  |  |
| runner BLS key valid on v2 |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | REQUEST_SUCCEEDED |  |
|  |  |  |  |  |  |  |  |  | 58 |  |  |  |  |  | 51 |  |  |  |  |  |  |  |
| UNRATE sane |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | 4.2 |  |
| U6 >= U3 |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | 7.9 |  |
| payrolls level sane (k) |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | 158984 |  |
| JOLTS openings sane (k) |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | 7594 |  |
| LFPR sane |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | 61.5 |  |
| UNRATE history depth |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | 317 |  |
| crisis engine |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | 28/WATCH sahm=0.07 |  |
|  |  | 28/WATCH |  | 7594 |  |  |  |  |  |  | 57 | 1.9 |  | 0.07 |  | -0.22 |  | 7.9 | 4.2 | 2026-06 |  |  |
| legacy doc fresh |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | live=20 |  |
| public S3 feed URL fresh |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  | 2026-07-12T02:39:26.275897+00:00 |  |
| bls.html serves rebuilt page |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 0 | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |

## Log
- `02:39:21` code landed via deploy-lambdas -- gate PASS
## 2. Key probe + env/description/runtime sync

- `02:39:22` function env BLS_API_KEY == runner secret
- `02:39:22` config updated (runtime -> python3.12, desc, env)
## 3. Invoke async + poll S3 for fresh employment doc

- `02:39:37` fresh doc: generated_at=2026-07-12T02:39:26.275897+00:00 api=v2 key_valid=True
## 4. Content assertions + headline numbers

## 5. Legacy regression: bls-labor.json still publishing

## 6. Public feed URL + live page cutover

## 7. Retire the failed 3133 gate script

- `02:39:38` moved ops_3133_bls_employment_desk.py -> ran/ (unpassable desc-gate superseded by this op)
## verdict

- `02:39:38` PASS
