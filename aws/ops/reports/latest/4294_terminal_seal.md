# ops 4294 -- decision terminal, shapes proven

**Status:** success  
**Duration:** 12.5s  
**Finished:** 2026-08-02T21:42:54+00:00  

## Data

| price | ticker | verdict | why | winrate |
|---|---|---|---|---|
| None | MSFT | STRONG BUY | MSFT screens as a strong buy because it's bought by a politician on a  | None |

## Log
- `21:42:54` nowcast: [('Consumer Sentiment (UMich)', -1.67), ('Nonfarm Payrolls', -1.07), ('2s10s Yield Curve', 0.55), ('Housing Starts', 0.52)]
- `21:42:54` us_cycle: None None worst=[{'name': 'margin', 'z': 1.52}, {'name': 'semis', 'z': 1.49}, {'name': 'real_10y', 'z': -1.47}]
- `21:42:54` credit: [{'name': 'HYG/LQD', 'z': 2.04}, {'name': 'LQD/IEF', 'z': -0.13}, {'name': 'TIP/IEF', 'z': 0.84}, {'name': 'EMB/IEF', 'z': 1.66}, {'name': 'TLT/SHY', 'z': -1.88}] | bond_vol -0.13
- `21:42:54` ladder stress ERs: [('BONDS_LONG', 2.4), ('US_SMALL_VALUE', 1.6)]
## RESULT

- `21:42:54` ✗   us_cycle unread
- `21:42:54` ✗   ladder stress <3 classes ([('BONDS_LONG', 2.4), ('US_SMALL_VALUE', 1.6)])
