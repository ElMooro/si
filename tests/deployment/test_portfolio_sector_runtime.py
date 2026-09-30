"""Sector source acceptance remains read-only and complete for both runtimes."""
from pathlib import Path
import ast,copy,gzip,hashlib,json,runpy
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6368_portfolio_sector_runtime_acceptance.py'


def retained_source(path):
    if path=='aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py':
        return ROOT/'tests/fixtures/pre-snapshot-byte-publication/lambda_function.py.txt'
    return ROOT/path


def evidence(function):
    return {'function_name':function,'receipt':{'status':'matched','commit':'a'*40},
        'source_files_checked':4 if function.endswith('snapshot') else 8,
        'memory_mb':512 if function.endswith('snapshot') else 1024,'timeout':180 if function.endswith('snapshot') else 300,
        'runtime':'python3.12','handler':'lambda_function.lambda_handler','architectures':['x86_64'],
        'role':'invented-original-role','ephemeral_storage_mb':512,
        'schedules':[{'kind':'EventBridge rule','name':'invented-original','state':'ENABLED','expression':'cron(40 * * * ? *)'}],
        'code_sha256':'invented-hash','active_alias':{'alias':'live','version':'13','code_sha256':'invented-hash'}}


def test_both_sector_runtimes_require_exact_receipts_and_complete_closures():
    s=runpy.run_path(str(PATH))
    for fn in s['FUNCTIONS']:
        base=evidence(fn);s['validate'](base,'a'*40,base,fn)
        for key,value in [('receipt',{'status':'matched','commit':'b'*40}),('source_files_checked',True),('source_files_checked',3),('function_name','other')]:
            bad=copy.deepcopy(base);bad[key]=value
            try:s['validate'](bad,'a'*40,base,fn)
            except ValueError:pass
            else:raise AssertionError('Unproven source closure accepted')


def test_original_native_resources_and_complete_schedules_remain_unchanged():
    s=runpy.run_path(str(PATH))
    for fn in s['FUNCTIONS']:
        base=evidence(fn)
        for key,value in [('timeout',600),('memory_mb',2048),('runtime','other'),('handler','other'),('role','other'),('architectures',['arm64']),('ephemeral_storage_mb',1024),('schedules',[]),('schedules',None)]:
            bad=copy.deepcopy(base);bad[key]=value
            try:s['validate'](bad,'a'*40,base,fn)
            except ValueError:pass
            else:raise AssertionError('Changed resource accepted: '+key)


def test_only_selected_receipt_can_be_read_and_producers_never_invoked():
    s=runpy.run_path(str(PATH));calls=[]
    class Client:
        def get_object(self,**kw):calls.append(kw);return {}
    for fn in s['FUNCTIONS']:
        reader=s['ReceiptOnly'](Client(),fn);request={'Bucket':s['BUCKET'],'Key':'data/ops/releases/'+fn+'.json'};reader.get_object(**request)
        for key in ('portfolio/snapshot.json','portfolio/risk.json','history/archive/other.json','data/ops/releases/other.json'):
            try:reader.get_object(**{**request,'Key':key})
            except ValueError:pass
            else:raise AssertionError('Unreviewed data read allowed')
    assert len(calls)==2
    source=PATH.read_text(encoding='utf-8');attributes={n.func.attr for n in ast.walk(ast.parse(source)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not attributes & {'invoke','put_object','put_item','update_item','transact_write_items','update_function_code','put_rule','update_schedule'}
    assert 'sys.exit(1)' in source and 'native_invocations=0' in source


def test_whole_inert_predecessors_and_current_synthetic_sector_frames_are_bound():
    audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-sector-coverage.json').read_bytes())
    for row in audit['fixtures'].values():
        raw=(ROOT/row['path']).read_bytes();assert len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256']
    frames=json.loads(gzip.decompress((ROOT/'tests/fixtures/portfolio-sector-coverage-synthetic.json.gz').read_bytes()));assert len(frames['cases'])==16
    for path,digest in frames['source_files'].items():assert hashlib.sha256(retained_source(path).read_bytes()).hexdigest()==digest,path
    for row in frames['cases'].values():
        assert row['output']['permissions']['sizing_eligible'] is False
        assert len(row['bundle']['inputs']['snapshot']['positions'])==len(row['output']['sector_exposure']['records'])


def test_complete_current_browser_frames_preserve_unknown_known_empty_and_all_sources():
    frames=json.loads(gzip.decompress((ROOT/'tests/fixtures/portfolio-sector-browser-synthetic.json.gz').read_bytes()))
    for path,digest in frames['source_files'].items():assert hashlib.sha256(retained_source(path).read_bytes()).hexdigest()==digest,path
    assert len(frames['cases'])==9
    for name,unknown,breach in [('complete',100,None),('mixed',65,None),('known',35,True)]:
        row=frames['cases'][name];assert len(row['writes'])==2
        risk=row['frame']['risk'];assert risk['sector_exposure']['unclassified_weight_pct']==unknown
        assert risk['alerts_summary']['sector_concentration_breach'] is breach and risk['concentration_hhi'] is None
        assert row['complete_inputs']['watchlist']==frames['cases']['complete']['complete_inputs']['watchlist']
    assert frames['cases']['empty']['frame']['risk']['sector_exposure']['status']=='NO_GROSS_EXPOSURE'
