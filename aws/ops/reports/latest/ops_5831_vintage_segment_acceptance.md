
**Status:** failure  
**Duration:** 619.8s  
**Finished:** 2026-09-19T05:04:39+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5831_vintage_segment_acceptance.py", line 64, in main
    assert set(index['series'])==set(model.SERIES) and index['n_series']==len(model.SERIES),index['errors']
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: {'T10Y2Y': 'TimeoutError'}

```

## Data

| active_aliases | runtimes |
|---|---|
| {'justhodl-backtest-engine': {'function': 'justhodl-backtest-engine', 'alias': 'live', 'version': '4', 'code_sha256': '5yBAepH0aIVwhkXV5O3bwKp8WFS7YqXNdUSjvga4g8E=', 'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442'}, 'justhodl-risk-gate': {'function': 'justhodl-risk-gate', 'alias': 'live', 'version': '9', 'code_sha256': '/PEbwhUv0yk7pz68n97LJlYfA473Tm8JvuY00ga0Ll4=', 'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442'}} | {'justhodl-ai-chat': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': 'PBdAOS1I9O5rS3fYyLJoR7usLFHABW6JUcyZfWETHp4='}, 'justhodl-backtest-engine': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': '5yBAepH0aIVwhkXV5O3bwKp8WFS7YqXNdUSjvga4g8E='}, 'justhodl-bond-warroom': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': '4u/jQ5yKBsq5LRMQOKPYlkvnDN/z4BgHMRk9WKlwRXQ='}, 'justhodl-euro-fragmentation': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': 'wYi423rco8j1Xh65lyUnNBZMoio9TEraxPTYZfDIaFM='}, 'justhodl-eurodollar-plumbing': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': '/Tgw2iovEDu8AIrSWMJpNsIegNrvReXbnLuSrGEV8KQ='}, 'justhodl-liquidity-capacity': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': 'bk8oDxH4GK096goSrJo0t6f9Z3QOhgbKw5HdDZQrsoU='}, 'justhodl-liquidity-credit-engine': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': 'N9MW1bUS40mkGqg/uxZ/0lLbGhhGWAZF3Myrs01eZSQ='}, 'justhodl-liquidity-inflection': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': 'DOJ4mvW76HrD+1EqonEMKwJqKjlyi6drGC41w6qAnAA='}, 'justhodl-repo': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': 'invZ8FoRTp7qyL08pWqsJmIOMIuAAyBbfsPF/L8IA5k='}, 'justhodl-risk-gate': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': '/PEbwhUv0yk7pz68n97LJlYfA473Tm8JvuY00ga0Ll4='}, 'justhodl-treasury-rehypo': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': 'E22EwetnyHJtnJhx8RRJ3EY/QjYaPq0FEDeS1+PGTMg='}, 'justhodl-vintage-fred': {'commit': '45f14f9087cb6e565b0cca21a75db0e3eede9442', 'code_sha256': '/7TrCsFiOeg4Szz+O/y8798Rc3e947+/cs6hYmIccoU='}} |

## Log

