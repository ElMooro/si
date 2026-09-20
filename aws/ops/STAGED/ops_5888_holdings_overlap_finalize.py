"""Finish 5887's accepted publication using the original-content archive reader."""
import sys
from ops_5887_holdings_overlap_acceptance import main

if __name__ == '__main__':
    try:
        main(report_name='ops_5888_holdings_overlap_finalize',
             accepted_run='a6585d36194e79aa65b648871726c369aad2de3da8edfebf30898a4a00a0f1e3',
             prior_invocation={'run': 35483697031,
                               'report': 'aws/ops/reports/latest/ops_5887_holdings_overlap_acceptance.md',
                               'started_at': '2026-09-20T02:19:57.215265+00:00'},
             prior_runtime={'request_id': '65e187ae-2222-462d-ab8b-7defca86d7ce',
                            'duration_ms': 16844.29, 'memory_size_mb': 1024, 'max_memory_used_mb': 320})
    except Exception:
        print('Overlap finalization failed. No engine was invoked; inspect the committed report.')
        sys.exit(1)
