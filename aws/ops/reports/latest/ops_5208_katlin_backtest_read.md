# ops 5208 -- KATLIN walk-forward read

**Status:** success  
**Duration:** 426.3s  
**Finished:** 2026-09-04T19:44:20+00:00  

## Error

```
SystemExit: 0
```

## Log
- `19:37:14`    existing backtest: v1.0.0 as_of 2026-09-04T19:16:01Z n_obs 38383 n_dates 44 universe {'stocks': 800, 'etfs': 300}
- `19:37:14`    invoked backtest async (prior as_of=2026-09-04T19:16:01Z); polling data/katlin-backtest.json
## walk-forward cohorts (point-in-time price gates, excess vs SPY)

- `19:44:20`    v1.2.0 as_of 2026-09-04T19:44:08Z sessions 1260 (2021-08-27..2026-09-03) n_obs 38584 n_dates 44 step 21 universe {'stocks': 800, 'etfs': 300} budget_hit None
- `19:44:20`    cohort                         21s n/med_excess/hit/MAE           | 63s                                | 126s                              
- `19:44:20`    above200                       25234   -0.46  46.0   -3.01        | 23947   -1.32  45.0   -5.69        | 22196   -1.94  45.0   -7.86       
- `19:44:20`    below200                       13280   -0.34  48.0   -3.31        | 12615   -1.68  45.0   -6.02        | 11510   -4.95  40.0   -8.52       
- `19:44:20`    below200+oversold               9481    -0.2  49.0   -3.28        |  9052    -1.5  45.0   -5.88        |  8214   -5.07  40.0   -8.49       
- `19:44:20`    below200+accum                  1608   -1.12  43.0   -3.48        |  1565    -2.6  41.0   -6.07        |  1486   -6.03  36.0   -8.77       
- `19:44:20`    below200+structure              4866   -0.71  46.0   -3.62        |  4593   -2.31  43.0   -6.55        |  4296   -4.98  40.0   -9.04       
- `19:44:20`    KATLIN_price_3of3                483   -1.27  42.0   -3.81        |   469   -2.15  43.0   -7.03        |   455   -4.47  40.0   -9.86       
- `19:44:20`    confirmed_bottom+accum           534   -1.42  42.0   -3.99        |   526   -2.65  41.0    -7.8        |   511   -4.97  38.0  -10.53       
- `19:44:20`    knife                             68    4.47  63.0   -3.59        |    66   10.69  62.0    -7.6        |    48   24.25  67.0    -9.0       
- `19:44:20` ✅    walk-forward read
