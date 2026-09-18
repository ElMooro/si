from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/"tests"))
from tenor_consumer_test_support import run
if __name__=="__main__":run("morning-intelligence")

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(ROOT/"tests"))
    from research_brief_consumer_test_support import run as run_research
    run_research("morning-intelligence")
