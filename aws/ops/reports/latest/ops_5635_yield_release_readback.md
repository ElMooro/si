
**Status:** failure  
**Duration:** 0.8s  
**Finished:** 2026-09-17T19:02:25+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5635_yield_release_readback.py", line 54, in main
    assert not differences,'Deployed source differs from pinned commit; no receipt written or invoke performed'
           ^^^^^^^^^^^^^^^
AssertionError: Deployed source differs from pinned commit; no receipt written or invoke performed

```

## Data

| actual_file_count | code_sha256 | differing_files | expected_commit | expected_file_count | function | last_modified |
|---|---|---|---|---|---|---|
| 38 | Uz33UypML8FjnrD2PJRbaG5y3CB0O9qFPKKZvVkp0gA= | ['bottom_context.py', 'brief_compiler.py', 'brief_contract.py', 'capital_contract.py', 'chain_guard.py', 'ciss_vintage.py', 'consume_brain.py', 'deterministic_desk.py', 'donor_contract.py', 'equity_donor_inputs.py', 'equity_enrich.py', 'factory_coder.py', 'factory_core.py', 'factory_discipline.py', 'factory_doctrine.py', 'factory_evidence.py', 'factory_repair.py', 'factory_sources.py', 'factory_store.py', 'fmp_book.py', 'fmp_stable.py', 'governed_targets.py', 'jh_adapters.py', 'jh_brief_adapters.py', 'lambda_function.py', 'llm_cost.py', 'llm_router.py', 'macro_donor_inputs.py', 'private_artifact.py', 'proposed_book_risk.py', 'public_brain_projection.py', 'public_provider_json.py', 'research_capital_ledger.py', 'test_brief_contract.py', 'xai_voice.py'] | 5d0c09d4de5aaee9895aed269ce0004f5202522d | 68 | justhodl-us10y-sentinel | 2026-09-09T00:07:57.000+0000 |

## Log

