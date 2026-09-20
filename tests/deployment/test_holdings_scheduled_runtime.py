"""Accept real Lambda REPORT units; refuse failures and exhausted capacity."""
import ast
from pathlib import Path
import re


def _parser():
    path = Path(__file__).resolve().parents[2]/'aws/ops/STAGED/ops_5883_scheduled_canonical_holdings.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    function = next(v for v in tree.body if isinstance(v, ast.FunctionDef) and v.name == 'runtime_report')
    scope = {'re': re}
    exec(compile(ast.Module(body=[function], type_ignores=[]), '<actual-runtime-report-parser>', 'exec'), scope)
    return scope['runtime_report']


def test_scheduled_holdings_runtime_reads_duration_and_peak_memory():
    message = 'REPORT RequestId: 01234567-abcd-0123-abcd-0123456789ab\tDuration: 345678.9 ms\tBilled Duration: 345679 ms\tMemory Size: 1024 MB\tMax Memory Used: 801 MB\tInit Duration: 540.2 ms'
    result = _parser()(message)
    assert result['duration_ms'] == 345678.9
    assert result['memory_size_mb'] == 1024 and result['max_memory_used_mb'] == 801


def test_scheduled_holdings_runtime_refuses_failure_and_capacity_exhaustion():
    base = 'REPORT RequestId: 01234567-abcd-0123-abcd-0123456789ab Duration: 250000 ms Memory Size: 1024 MB Max Memory Used: 801 MB'
    for message in (base+' Status: timeout', base+' Status: error Error Type: Runtime.OutOfMemory',
                    base.replace('250000', '600000'), base.replace('801 MB', '1024 MB')):
        try: _parser()(message)
        except ValueError: pass
        else: raise AssertionError('Failed execution accepted')


def test_scheduled_holdings_runtime_refuses_incomplete_report():
    try: _parser()('REPORT RequestId: 01234567-abcd-0123-abcd-0123456789ab Duration: 1000 ms')
    except ValueError: pass
    else: raise AssertionError('Missing memory evidence accepted')
