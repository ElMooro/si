from pathlib import Path
import ast,importlib.util
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6329_auction_source_runtime_baseline.py'


def loaded():
    spec=importlib.util.spec_from_file_location('auction_source_baseline',PATH)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def test_auction_baseline_storage_is_exactly_two_receipts():
    m=loaded()
    class Client:
        def __init__(self):self.reads=[]
        def get_object(self,**request):self.reads.append(request);return {}
    raw=Client();guard=m.ReceiptsOnly(raw)
    for fn in m.FUNCTIONS:guard.get_object(Bucket=m.BUCKET,Key='data/ops/releases/'+fn+'.json')
    for key in ('data/auction-crisis.json','data/auction-desk.json','data/warm/treasury-auctions/history.json.gz',
                'data/prospective-outcomes.json','private/account.json','data/ops/releases/unrelated.json'):
        try:guard.get_object(Bucket=m.BUCKET,Key=key)
        except ValueError:pass
        else:raise AssertionError(key)
    assert len(raw.reads)==2 and not hasattr(guard,'put_object')


def test_auction_baseline_has_no_native_execution_or_mutation():
    tree=ast.parse(PATH.read_text(encoding='utf-8'))
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','put_rule','update_schedule','get_secret_value','get_parameter'}
    assert 'sys.exit(1)' in PATH.read_text(encoding='utf-8')


def test_auction_baseline_requires_whole_code_resources_and_existing_schedule():
    m=loaded();fn=m.FUNCTIONS[0]
    value={'function_name':fn,'source_files_checked':4,'memory_mb':512,'timeout':300,
           'schedules':[{'state':'ENABLED','expression':'cron(5 13 * * ? *)','native_targets':1}]}
    m.validate(value,fn)
    for changed in ({'function_name':'another'},{'source_files_checked':True},{'source_files_checked':1},
                    {'memory_mb':0},{'timeout':None},{'schedules':[]}):
        try:m.validate({**value,**changed},fn)
        except ValueError:pass
        else:raise AssertionError(changed)
