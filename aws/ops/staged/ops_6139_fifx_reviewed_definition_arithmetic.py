"""New qualification identity after correcting the exact retained GBP unit.

Reuses all captured originals. The failed 6138 claim and report stay preserved.
No provider request, native invocation, public packet or schedule mutation.
"""
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
from ops_6138_fifx_retained_arithmetic import main

if __name__=='__main__':
    try:main(request_id='chatgpt-fifx-reviewed-definition-arithmetic-6139',
             report_name='ops_6139_fifx_reviewed_definition_arithmetic')
    except Exception:sys.exit(1)
