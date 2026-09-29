from pathlib import Path
from copy import deepcopy
import ast,importlib.util,json
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6337_auction_fred_originals_runtime_acceptance.py'


def loaded():
    spec=importlib.util.spec_from_file_location('auction_observation_acceptance',PATH)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def test_auction_fred_originals_acceptance_is_receipt_only():
    m=loaded();reads=[]
    class Client:
        def get_object(self,**request):reads.append(request);return {}
    guard=m.ReceiptsOnly(Client())
    for fn in m.FUNCTIONS:guard.get_object(Bucket=m.BUCKET,Key='data/ops/releases/'+fn+'.json')
    for key in ('data/auction-crisis.json','data/auction-desk.json','data/prospective-outcomes.json','private/account.json'):
        try:guard.get_object(Bucket=m.BUCKET,Key=key)
        except ValueError:pass
        else:raise AssertionError(key)
    calls={n.func.attr for n in ast.walk(ast.parse(PATH.read_text(encoding='utf-8'))) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert len(reads)==1 and not calls&{'invoke','put_object','get_secret_value','get_parameter','put_rule','update_schedule'}
    assert 'sys.exit(1)' in PATH.read_text(encoding='utf-8')


def test_auction_fred_originals_acceptance_preserves_whole_original_runtime_and_every_binding():
    m=loaded();baselines=json.loads(m.BASELINE_PATH.read_bytes())['actual_runtimes'];sha='a'*40
    for fn in m.FUNCTIONS:
        base=baselines[fn]
        actual=deepcopy(base);actual['receipt']={'status':'matched','commit':sha};m.validate(actual,fn,sha,base)
        for label in ('memory','timeout','cadence','timezone','extra','missing','receipt','identity','closure'):
            bad=deepcopy(actual)
            if label=='memory':bad['memory_mb']+=128
            elif label=='timeout':bad['timeout']+=1
            elif label=='cadence':bad['schedules'][0]['expression']='rate(1 minute)'
            elif label=='timezone':bad['schedules'][0]['timezone']='UTC'
            elif label=='extra':bad['schedules']+=deepcopy(bad['schedules'])
            elif label=='missing':bad['schedules'].pop()
            elif label=='receipt':bad['receipt']['commit']='b'*40
            elif label=='identity':bad['function_name']='other'
            else:bad['source_files_checked']=True
            try:m.validate(bad,fn,sha,base)
            except ValueError:pass
            else:raise AssertionError((fn,label))


def test_fred_originals_detector_resources_equal_verified_native_predecessor():
    m=loaded();fn=m.FUNCTIONS[0];base=json.loads(m.BASELINE_PATH.read_bytes())['actual_runtimes'][fn]
    config=json.loads((ROOT/'aws/lambdas'/fn/'config.json').read_bytes())
    assert (config['memory'],config['timeout'],config['runtime'],config['architectures'])==(base['memory_mb'],base['timeout'],base['runtime'],base['architectures'])
    assert 'schedule' not in config and 'eventbridge_scheduler' not in config
