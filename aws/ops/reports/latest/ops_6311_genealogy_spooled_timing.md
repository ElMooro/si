
**Status:** success  
**Duration:** 15.8s  
**Finished:** 2026-09-28T20:28:25+00:00  

## Data

| actual_aws_writes | downstream_output_reads | elapsed_seconds | learning_ledger_reads | native_invocations | original_bytes | peak_runner_rss_kib | private_account_reads | provider_requests | result | schedule_changes | scope | whole_predecessor_output_equal |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 15.767 | 0 | 0 | 120357077 | 86476 | 0 | 0 | {'complete_output_sha256': 'a1b1500fa1490c3f671890bbe327b7882ef87855770f1bebfea8e7026a1e3a1d', 'output_bytes': 892773, 'input_contexts_sha256': 'f2f0bc9dc3b70d6c1a959cff15ae949068e1c1f2d9465e9b709a9310bf72ec06', 'capture_count': 251, 'source_count': 247, 'first_observed_groups': 333, 'possible_comparisons': 55, 'comparison_status_counts': {'a_observed_presence_before_b_bounded_entry': 29, 'unresolved_prior_observation_missing': 25, 'b_observed_presence_before_a_bounded_entry': 1}, 'database_bytes': 60923904, 'scope': 'Complete validated input iterator, disk-backed ordering and full canonical output. Original-body validation and durable incremental caching are separate required boundaries.'} | 0 | All fixed approved public captures. Temporary SQLite ordering and streaming canonical output reproduce every predecessor value. No native publication, incremental cache, private ledger, account data or portfolio authority. | True |

## Log

