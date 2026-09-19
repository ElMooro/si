
**Status:** failure  
**Duration:** 509.4s  
**Finished:** 2026-09-19T04:44:12+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5830_vintage_archive_verify.py", line 64, in main
    assert set(index['series'])==set(model.SERIES) and index['n_series']==len(model.SERIES),index['errors']
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: {'NFCI': 'ValueError: source exceeds bounded page budget; not a complete archive', 'STLFSI4': 'ValueError: source exceeds bounded page budget; not a complete archive', 'T10Y2Y': 'TimeoutError'}

```

## Data

| active_aliases | runtimes |
|---|---|
| {'justhodl-backtest-engine': {'function': 'justhodl-backtest-engine', 'alias': 'live', 'version': '3', 'code_sha256': '7MoJH3ukKccw1pqJ1LYjjCpJdAs8eafSCG18XfcNHMw=', 'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7'}, 'justhodl-risk-gate': {'function': 'justhodl-risk-gate', 'alias': 'live', 'version': '8', 'code_sha256': 'tXOuVijM9XHcySwfY5j6GZSVsUVD3aR0KIfyy+D5Sv4=', 'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7'}} | {'justhodl-ai-chat': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'bzFErh8cWO4THrlVTOosyv2/IU30eFxRK1nCml9fl44='}, 'justhodl-backtest-engine': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': '7MoJH3ukKccw1pqJ1LYjjCpJdAs8eafSCG18XfcNHMw='}, 'justhodl-bond-warroom': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'ajKDUmV7DETwYYYGnfkdsHMHEi7S3cwhzxmJ8FJa6qk='}, 'justhodl-euro-fragmentation': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'zjznZw9AXJ86yGlH4Nr6i1T+Xk2XNDgpKWREVX5TT24='}, 'justhodl-eurodollar-plumbing': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'FDo7qauaJ2b8XS7m5nLRXTAkmliOisKcW1+bUaZeTY0='}, 'justhodl-liquidity-capacity': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'fsOEMEtSxRT3KhU/CxvW6eZ7+KnHBPXjTg8HDQsbf30='}, 'justhodl-liquidity-credit-engine': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': '+kikjdo2gPhY9XFQ8HLGAlWjv6KNMwbCb8dsFiwWKyk='}, 'justhodl-liquidity-inflection': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'Z4f8bxxpvhJoR/CuB9pnqESJ9ob9yX31/m3jEtJDOA0='}, 'justhodl-repo': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'tqjR8NlACnHQu8zAN4HXOzOjFNTkJ7lxTxXTZv4d9Gc='}, 'justhodl-risk-gate': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'tXOuVijM9XHcySwfY5j6GZSVsUVD3aR0KIfyy+D5Sv4='}, 'justhodl-treasury-rehypo': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'Uj+v03JAIbwtXiPRtgetnlUPty8wb7vdLv/8zqrp5lk='}, 'justhodl-vintage-fred': {'commit': 'a0e4e493f5169d643f7915f540853b79167d54a7', 'code_sha256': 'gAYA7iZJSFVvL92zgOi0ncp5ribA//9ycIyHQy1Lwc8='}} |

## Log

