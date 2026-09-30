"""Offline actual-handler integration regression (shared fixture, no AWS)."""
import importlib.util
import unittest
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location("integration_suite",HERE.parents[1]/"justhodl-portfolio-admin/tests/run_tests.py")
suite=importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)
if __name__ == "__main__":
    # Load before integration fixtures substitute boto3 in sys.modules.
    import test_watchlist_sync, test_accounting, test_book_read, test_quote_read, test_research_enrichment, test_publication_compatibility, test_quote_collection, test_sector_accounting
    cases=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromModule(module) for module in (test_watchlist_sync,test_accounting,test_book_read,test_quote_read,test_research_enrichment,test_publication_compatibility,test_quote_collection,test_sector_accounting))
    result=unittest.TextTestRunner(verbosity=1).run(cases)
    if not result.wasSuccessful():raise SystemExit(1)
    suite.test_actual_handler_handles_mixed_priced_unpriced_book()
    suite.test_private_publication_failure_prevents_snapshot_write()
    suite.test_anonymous_snapshot_http_denied_before_account_reads()
    suite.test_validate_only_snapshot_skips_sync_and_all_writes()
    print("justhodl-portfolio-snapshot integration tests passed")
