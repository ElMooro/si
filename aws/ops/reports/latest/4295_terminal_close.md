# ops 4295 -- decision terminal, shapes proven

**Status:** success  
**Duration:** 236.5s  
**Finished:** 2026-08-02T21:54:24+00:00  

## Data

| price | ticker | verdict | why | winrate |
|---|---|---|---|---|
| 464.72 | MSFT | STRONG BUY | MSFT screens as a strong buy because it's bought by a politician on a  | 66.5 |

## Log
- `21:54:24` nowcast: [('Consumer Sentiment (UMich)', -1.67), ('Nonfarm Payrolls', -1.07), ('2s10s Yield Curve', 0.55), ('Housing Starts', 0.52)]
- `21:54:24` us_cycle: WATCH 51.7 worst=[{'name': 'margin', 'z': 1.52}, {'name': 'semis', 'z': 1.49}, {'name': 'real_10y', 'z': -1.47}]
- `21:54:24` credit: [{'name': 'HYG/LQD', 'z': 2.04}, {'name': 'LQD/IEF', 'z': -0.13}, {'name': 'TIP/IEF', 'z': 0.84}, {'name': 'EMB/IEF', 'z': 1.66}, {'name': 'TLT/SHY', 'z': -1.88}] | bond_vol -0.13
- `21:54:24` ladder stress ERs: [('BONDS_LONG', 2.4), ('US_SMALL_VALUE', 1.6)]
- `21:54:24` ✅ stress coverage: 2 ladder classes (upstream stress-scenarios covers 2 mappable ladder assets today -- gate matches reality, growth is an upstream item)
## RESULT

- `21:54:24` ✅ OPS 4293 PASS -- one screen, whole decision, every shape real
