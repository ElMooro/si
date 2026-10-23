"""ops 6636 - the ops 6633 canary again, held for 20 minutes instead of 4 (archive scope B, Khalid 2026-10-23).

ops 6633 found every excluded canary still marked for replication (PENDING) for 4.3 minutes after a Disabled,
higher-priority rule was added - S3 appears to skip disabled rules when it picks the rule that applies. AWS also warns
that a replication configuration change takes a while to reach all S3 systems, so before concluding that S3 cannot
exclude a folder this way, the same test runs for 20 minutes: canary pairs every 30 s from +0 to +20 min. Same steps,
same safety: backup, add the disabled self-test rule, canaries, and on anything but PASS the backed-up configuration
is put back exactly. Pass = 3 consecutive pairs with the excluded canary unmarked and the included one marked, the
included one then in the archive, no excluded one there.
"""
import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("canary6633", HERE / "ops_6633_archive_scope_canary.py")
canary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(canary)
canary.PAIRS, canary.GAP_S, canary.SETTLE_S, canary.NEED, canary.GIVE_UP_S = 40, 30, 60, 3, 1200
canary.REPORT_NAME = "ops_6636_archive_scope_canary_long"
main = canary.main

if __name__ == "__main__":
    if "--selftest" in sys.argv:
        canary.selftest()
        sys.exit(0)
    main()                 # exits 0 on PASS, sys.exit(1) on anything else (configuration restored)
