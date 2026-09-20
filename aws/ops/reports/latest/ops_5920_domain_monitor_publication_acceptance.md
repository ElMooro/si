
**Status:** failure  
**Duration:** 6.8s  
**Finished:** 2026-09-20T12:27:50+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5920_domain_monitor_publication_acceptance.py", line 83, in main
    subprocess.run(['git','merge-base','--is-ancestor',commit,actual],cwd=ROOT,check=True)
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/subprocess.py", line 571, in run
    raise CalledProcessError(retcode, process.args,
subprocess.CalledProcessError: Command '['git', 'merge-base', '--is-ancestor', '369692d28ea19955b287d04ec407da4ea765eb95', '5ef029ebb70c5f6e820cccdbc5169f8814201136']' returned non-zero exit status 128.

```

## Data

| code_sha256 | engine_invocations | exact_runtime_commit | notifications_sent | paid_ai_calls | portfolio_writes | private_account_reads | public_input_rehearsal | schedule |
|---|---|---|---|---|---|---|---|---|
| ri+bscjs7BNAT3Y4itvjX4gpEWfDKuWIB6Z+suZfR9Q= | 0 | 369692d28ea19955b287d04ec407da4ea765eb95 | 0 | 0 | 0 | 0 |  | {'Name': 'domain-barometers-daily', 'ScheduleExpression': 'cron(20 12 * * ? *)', 'ScheduleExpressionTimezone': 'UTC', 'State': 'ENABLED'} |
|  |  |  |  |  |  |  | {'input_references': {'domain-barometers': {'sha256': 'ecceb7271eb704047468eada682c3380338648b1c5bb54cd4ece81a1e7033248', 'bytes': 6337012, 'retention': 'private_audit_copy_of_existing_public_data'}, 'tradingview': {'sha256': 'c06cabcf0c14208ad2daa5c797b8596d732e3262f3321777ed6bed5c4fc5f299', 'bytes': 7474812, 'retention': 'private_audit_copy_of_existing_public_data'}, 'risk-gate': {'sha256': 'be9e6a1f034d748dd78b84b52ccd7c30a99d741c25eb60cd94ac99d0dfa990c3', 'bytes': 424400, 'retention': 'private_audit_copy_of_existing_public_data'}, 'rotation-dashboard': {'sha256': 'e2b95d8a0a2e975408cd1fb2fd51fe19034cf33a12caa64d6a240a2d5178735e', 'bytes': 52861, 'retention': 'private_audit_copy_of_existing_public_data'}}, 'rows': 10483, 'classification_generated_at': '2026-09-20T12:20:33.989922+00:00', 'result_sha256': 'bab58b0c96dbd1cfb9d1461fe33d60bd07540c38363f32718be815afc68eb359', 'counts': {'MACRO': {'n_favourable': 54, 'n_adverse': 62, 'n_unchanged': 5, 'n_comparison_unavailable': 2360, 'n_excluded_comparisons': 37, 'excluded_provider_roots': {'duplicate_provider_series': 21, 'unidentified_provider_series': 5, 'conflicting_alias_vintage_or_rule': 11}}, 'LIQUIDITY': {'n_favourable': 25, 'n_adverse': 8, 'n_unchanged': 3, 'n_comparison_unavailable': 14, 'n_excluded_comparisons': 6, 'excluded_provider_roots': {'unidentified_provider_series': 1, 'duplicate_provider_series': 4, 'conflicting_alias_vintage_or_rule': 1}}, 'RISK': {'n_favourable': 36, 'n_adverse': 18, 'n_unchanged': 5, 'n_comparison_unavailable': 6, 'n_excluded_comparisons': 3, 'excluded_provider_roots': {'conflicting_alias_vintage_or_rule': 2, 'duplicate_provider_series': 1}}}, 'scope': 'Public-input calculation rehearsal using retained public classification labels. The private classifier and scheduled handler were not invoked.'} |  |

## Log

