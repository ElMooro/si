"""Legacy admin baseline absence cannot relax post-deploy receipt proof."""
from pathlib import Path
import ast,copy,runpy
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6360_portfolio_accounting_runtime_acceptance.py'
if not PATH.exists():PATH=ROOT/'aws/ops/STAGED/ops_6360_portfolio_accounting_runtime_acceptance.py'


def test_legacy_admin_absence_is_only_a_prechange_baseline_exception():
    scope=runpy.run_path(str(PATH));original=runpy.run_path(str(ROOT/'tests/deployment/test_portfolio_accounting_runtime.py'))
    admin=original['evidence']('justhodl-portfolio-admin');admin['source_files_checked']=1;admin['receipt']={'status':'missing_predecessor_receipt'}
    scope['validate'](admin)
    for expected,baseline in [('a'*40,None),(None,admin),('a'*40,admin)]:
        try:scope['validate'](admin,expected,baseline)
        except ValueError:pass
        else:raise AssertionError('Missing post-deploy receipt accepted')
    snapshot=original['evidence']('justhodl-portfolio-snapshot');snapshot['receipt']={'status':'missing_predecessor_receipt'}
    try:scope['validate'](snapshot)
    except ValueError:pass
    else:raise AssertionError('Unreviewed legacy exception accepted')


def test_legacy_baseline_still_requires_complete_source_and_original_resources():
    scope=runpy.run_path(str(PATH));original=runpy.run_path(str(ROOT/'tests/deployment/test_portfolio_accounting_runtime.py'));base=original['evidence']('justhodl-portfolio-admin');base['receipt']={'status':'missing_predecessor_receipt'}
    for key,value in [('source_files_checked',0),('source_files_checked',True),('memory_mb',512),('timeout',180),('receipt',{'status':'unknown'})]:
        bad=copy.deepcopy(base);bad[key]=value
        try:scope['validate'](bad)
        except ValueError:pass
        else:raise AssertionError('Invalid legacy baseline accepted')


def test_corrected_accounting_baseline_never_invokes_or_mutates_resources():
    source=PATH.read_text(encoding='utf-8');calls={node.func.attr for node in ast.walk(ast.parse(source)) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)}
    assert not calls & {'invoke','put_object','transact_write_items','update_function_code','put_rule','update_schedule','start_execution'}
    assert 'sys.exit(1)' in source and 'native_invocations=0' in source and 'private_reads=0' in source
