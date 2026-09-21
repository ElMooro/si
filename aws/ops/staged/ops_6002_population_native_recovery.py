"""Resume uninvoked population acceptance after observed ARM64 config repair.

Ops6000 stopped during consumer package checks, before invocation or head write.
The same durable producer request identity still prevents duplicate recovery.
"""
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
from ops_6000_population_native_acceptance import main

if __name__=='__main__':
    try:main('ops_6002_population_native_recovery')
    except Exception:sys.exit(1)
