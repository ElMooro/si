# ops 5169 -- ecb-deep completeness audit (read-only)

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-09-03T21:42:32+00:00  

## Data

| done | err | flow | gap | section | tail |
|---|---|---|---|---|---|
| 36 | 14 | CSEC | 1 | err_flows | 13 |
| 0 | 11 | ECB.DISS:JDF_ICP_COICOP_ANR | 0 | err_flows | 11 |
| 0 | 11 | ECB.DISS:JDF_ICP_COICOP_INX | 0 | err_flows | 11 |
| 0 | 11 | ECB.DISS:JDF_PUB_BSI_CROSS_BORDER_POSITIONS | 0 | err_flows | 11 |
| 0 | 11 | ECB.DISS:JDF_PUB_BSI_MFI_BALANCE_SHEET | 0 | err_flows | 11 |
| 0 | 11 | ESTAT:E09 | 0 | err_flows | 11 |
| 0 | 11 | ESTAT:E11 | 0 | err_flows | 11 |
| 0 | 11 | ESTAT:GFS | 0 | err_flows | 11 |
| 0 | 11 | ESTAT:IDCM | 0 | err_flows | 11 |
| 0 | 11 | ESTAT:IDCS | 0 | err_flows | 11 |
| 0 | 11 | ESTAT:MNA | 0 | err_flows | 11 |
| 0 | 11 | ESTAT:QSA | 0 | err_flows | 11 |
| 0 | 11 | EUROSTAT:IEAF | 0 | err_flows | 11 |
| 0 | 11 | EUROSTAT:IEAQ | 0 | err_flows | 11 |
| 0 | 11 | IMF:BP6 | 0 | err_flows | 11 |
| 0 | 11 | IMF:BPS | 0 | err_flows | 11 |
| 0 | 11 | IMF:RA6 | 0 | err_flows | 11 |
| 0 | 11 | IMF:RAS | 0 | err_flows | 11 |
| 9 | 1 | PTN | 1 | err_flows | 0 |

## Log
- `21:42:32` mode=refresh flows=58 rearmed=None resynced=27
- `21:42:32` windows by status: {'empty': 182, 'done': 309, 'err:HTTP502': 1, 'slow_month': 28, 'err:TimeoutE': 13, 'err:HTTP400': 187, 'err:HTTP504': 1}
- `21:42:32` error codes: {'err:HTTP502': 1, 'err:TimeoutError': 13, 'err:HTTP400': 187, 'err:HTTP504': 1}
- `21:42:32` tries distribution among err windows: {3: 202}
- `21:42:32` banked raw bytes across done windows: 66.5 GB
## err windows by flow

- `21:42:32`    CSEC           err 14 (gap  1, tail 13)  done 36  last-ok 2025-04_2025-04  e.g. 1900_1979:HTTP502/t3, 2026-08_2026-08:TimeoutError/t3, 2026-09_2026-09:TimeoutError/t3, 2026-10_2026-10:TimeoutError/t3
- `21:42:32`    ECB.DISS:JDF_ICP_COICOP_ANR err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ECB.DISS:JDF_ICP_COICOP_INX err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ECB.DISS:JDF_PUB_BSI_CROSS_BORDER_POSITIONS err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ECB.DISS:JDF_PUB_BSI_MFI_BALANCE_SHEET err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ESTAT:E09      err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ESTAT:E11      err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ESTAT:GFS      err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ESTAT:IDCM     err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ESTAT:IDCS     err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ESTAT:MNA      err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    ESTAT:QSA      err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    EUROSTAT:IEAF  err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    EUROSTAT:IEAQ  err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    IMF:BP6        err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    IMF:BPS        err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    IMF:RA6        err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    IMF:RAS        err 11 (gap  0, tail 11)  done  0  last-ok None  e.g. 1900_1979:HTTP400/t3, 1980_1989:HTTP400/t3, 1990_1994:HTTP400/t3, 1995_1999:HTTP400/t3
- `21:42:32`    PTN            err  1 (gap  1, tail  0)  done  9  last-ok 2025_2035  e.g. 2020_2022:HTTP504/t3
## verdict

- `21:42:32`    2 err windows are real GAPS (data exists on both sides), 200 are tail windows (nothing later in the flow)
- `21:42:32`    engine has a one-shot healing switch: invoke with {"rearm_errs": true} -> tries reset, retried under the slow-window guard
- `21:42:32` ✅ ops 5169 complete -- read-only
