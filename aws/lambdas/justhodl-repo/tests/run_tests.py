"""Network-free production macro donor builders and capacity handler regressions; write-only-on-change of the history files;
the board's added scope, derived spreads and FRED rhythm; its own source readers (the dealer survey rate, the tri-party
haircut workbooks, DTCC's moved file); the whole board build end to end on a stub bucket and wire."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/"shared/tests"))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from macro_donor_test_support import run
import test_history_writes
import test_repo_scope
import test_repo_sources
import test_board_build
if __name__=="__main__":
    test_history_writes.main()
    test_repo_scope.main()
    test_repo_sources.main()
    test_board_build.main()
    run()
