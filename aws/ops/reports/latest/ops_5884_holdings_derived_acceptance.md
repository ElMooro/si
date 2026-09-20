
**Status:** failure  
**Duration:** 19.0s  
**Finished:** 2026-09-20T01:17:56+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5884_holdings_derived_acceptance.py", line 138, in main
    assert public(name)[0] == (ROOT/name).read_bytes(), 'Public asset differs: '+name
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: Public asset differs: jh-holdings-boundary.css

```

## Data

| acceptance | consumer | notification_suppression | private_account_reads | releases |
|---|---|---|---|---|
|  |  | True | 0 | {'justhodl-compound-aggregator': {'commit': 'a7b3519062e970a1b2ea28aa184157c049322828', 'code_sha256': 'ywvjVKA0KBGB651vQFiE/1/I/ZixodR+KXxfi36PF0w='}, 'justhodl-attention-confluence': {'commit': 'a7b3519062e970a1b2ea28aa184157c049322828', 'code_sha256': 'IrKxAgJR07band1HMsmeh/iNB6ZUsLU1wLLt3SPgtX4='}, 'justhodl-flow-confluence': {'commit': 'a7b3519062e970a1b2ea28aa184157c049322828', 'code_sha256': 'O2fUVP/TTQ/VoysGGOl79ds/pysvQq0/cZx0X6u/vfw='}, 'justhodl-master-ranker': {'commit': 'a7b3519062e970a1b2ea28aa184157c049322828', 'code_sha256': 'xnD8MChDNgIB7sH23oj6wiLt0i62ZjnZbMdAFxxaTLE='}} |
| {'generated_at': '2026-09-20T01:17:44+00:00', 'packet_sha256': 'c8d90fac4384130a5ee7fb128653816ac57e30d5f8657059cb00ee598553e705', 'throttle_rejections_before_execution': 0, 'public_page': 'compound-signals.html', 'known_direct_and_cluster_paths_excluded': True} | justhodl-compound-aggregator |  |  |  |
| {'generated_at': '2026-09-20T01:17:47.487230+00:00', 'packet_sha256': '2f1eab069801de46cd0dd32ab1cac15191c0df86a2c6f75919208b653623826c', 'throttle_rejections_before_execution': 0, 'public_page': 'attention.html', 'known_direct_and_cluster_paths_excluded': True} | justhodl-attention-confluence |  |  |  |
| {'generated_at': '2026-09-20T01:17:49.762394+00:00', 'packet_sha256': 'b34e30fc069b59662f67a2c4def013070cd2c871e46c089e139a1a7e67f076f0', 'throttle_rejections_before_execution': 0, 'public_page': 'flow-confluence.html', 'known_direct_and_cluster_paths_excluded': True} | justhodl-flow-confluence |  |  |  |
| {'generated_at': '2026-09-20T01:17:55+00:00', 'packet_sha256': '4d90a09128c2dff2db2f21010c73084a5fd4747e81e14f3340ecca778240cc52', 'throttle_rejections_before_execution': 0, 'public_page': 'master-rank.html', 'known_direct_and_cluster_paths_excluded': True} | justhodl-master-ranker |  |  |  |

## Log

