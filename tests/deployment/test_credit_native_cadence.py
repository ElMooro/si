from pathlib import Path
from datetime import date,datetime,timedelta,timezone
import ast,copy,hashlib,importlib.machinery,importlib.util,json,runpy,sys

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/lambdas/justhodl-credit-stress/source','aws/lambdas/justhodl-credit-stress/tests','aws/lambdas/justhodl-bond-desk/source')]
import credit_collection_clock as clock,credit_research_store as store,credit_research_model as model
from credit_fixtures import fixture
from native_credit_tests import Memory


def original(name):
    p=ROOT/'tests/fixtures'/('pre-credit-cadence-'+name.replace('/','--')+'.txt')
    return p,p.read_bytes()


def retained_legacy(inputs,m,output):
    refs={}
    for category,doc in (('input',inputs),('output',output)):
        raw=model.encoded(doc);digest=model.sha(raw);key=store.PREFIX+category+'s/'+digest+'.json'
        m.objects[key]=raw;refs[category]={'key':key,'sha256':digest,'bytes':len(raw)}
    compilers={}
    for name,digest in store.LEGACY_COMPILERS.items():
        if name=='dealer_research_context':raw=(ROOT/'aws/shared/dealer_research_context.py').read_bytes()
        else:_,raw=original('aws/lambdas/justhodl-credit-stress/source/'+name+'.py')
        assert model.sha(raw)==digest
        key=store.PREFIX+'compilers/'+digest+'.py';m.objects[key]=raw;compilers[name]={'key':key,'sha256':digest}
    manifest={'contract':'credit-native-replay.v1','generated_at':output['generated_at'],**refs,'compilers':compilers,'output_sha256':refs['output']['sha256']}
    raw=model.encoded(manifest);key=store.PREFIX+'runs/'+model.sha(raw)+'.json';m.objects[key]=raw
    return {'manifest_key':key,'output_sha256':refs['output']['sha256']}


def test_promoted_clock_logic_is_exactly_the_retained_source_candidate():
    paths=[ROOT/'aws/ops/checks/credit_collection_clock.py',ROOT/'aws/shared/credit_collection_clock.py']
    trees=[ast.parse(p.read_text(encoding='utf-8')) for p in paths]
    for tree in trees:tree.body=tree.body[1:] # deployment-scope docstrings differ; all executable statements must match
    assert ast.dump(trees[0])==ast.dump(trees[1])
    assert hashlib.sha256(paths[0].read_bytes()).hexdigest()=='2fff74a2ec913299ff08cbb76d06b93b2cc395b2149e4ed3b7281e7fc25f2ba8'


def test_complete_legacy_credit_compiler_replays_without_changing_one_output_field():
    inputs,bodies=fixture();m=Memory();m.objects.update(bodies);read=store.reader(m,'test')
    path,raw=original('aws/lambdas/justhodl-credit-stress/source/credit_research_store.py')
    assert model.sha(raw)==store.LEGACY_COMPILERS['credit_research_store']
    name='whole_predecessor_credit_store';loader=importlib.machinery.SourceFileLoader(name,str(path))
    spec=importlib.util.spec_from_loader(name,loader);old=importlib.util.module_from_spec(spec);sys.modules[name]=old
    try:loader.exec_module(old);expected=old.compile_output(inputs,read)
    finally:sys.modules.pop(name,None)
    assert model.encoded(store.compile_output(inputs,read))==model.encoded(expected)
    ref=retained_legacy(inputs,m,expected)
    assert model.encoded(store.replay(ref,read))==model.encoded(expected)
    assert expected['version']=='2.0.0' and 'collection_policy' not in expected['freshness']


def test_new_native_input_has_its_own_four_file_replay_and_cannot_change_measurements():
    inputs,bodies=fixture();m=Memory();m.objects.update(bodies);read=store.reader(m,'test')
    old=store.compile_output(inputs,read);new_inputs={**inputs,'collection_policy':clock.POLICY}
    output=store.compile_output(new_inputs,read)
    assert output['version']=='2.1.0'
    assert {k:v for k,v in output.items() if k not in ('version','freshness')}=={k:v for k,v in old.items() if k not in ('version','freshness')}
    ref=store.retain(m,'test',new_inputs,output);manifest=json.loads(m.objects[ref['manifest_key']])
    assert len(manifest['compilers'])==4 and 'credit_collection_clock' in manifest['compilers']
    assert store.replay(ref,read)==output
    assert not clock.collection_current(output,output['freshness']['pipeline_check_due_at'])
    for bad in (None,True,'unknown'):
        try:store.compile_output({**inputs,'collection_policy':bad},read)
        except ValueError:pass
        else:raise AssertionError('Unknown native policy accepted')


def test_legacy_or_mixed_compilers_cannot_borrow_the_new_policy():
    inputs,bodies=fixture();m=Memory();m.objects.update(bodies);read=store.reader(m,'test')
    inputs={**inputs,'collection_policy':clock.POLICY};out=store.compile_output(inputs,read)
    ref=retained_legacy(inputs,m,out)
    try:store.replay(ref,read)
    except ValueError as error:assert 'Legacy compiler' in str(error)
    else:raise AssertionError('Old compiler closure asserted new policy')
    ref=store.retain(m,'test',inputs,out);manifest=json.loads(m.objects[ref['manifest_key']])
    digest=store.LEGACY_COMPILERS['credit_research_store']
    manifest['compilers']['credit_research_store']={'key':store.PREFIX+'compilers/'+digest+'.py','sha256':digest}
    raw=model.encoded(manifest);key=store.PREFIX+'runs/'+model.sha(raw)+'.json';m.objects[key]=raw
    try:store.replay({**ref,'manifest_key':key},read)
    except ValueError as error:assert 'complete reviewed' in str(error)
    else:raise AssertionError('Mixed compiler closure accepted')


def test_independent_credit_check_refuses_expired_admitted_or_missing_comparisons():
    import bond_credit,verify_bond_credit
    sys.path.insert(0,str(ROOT/'tests'));from test_bond_credit_candidate import fixture as bond_fixture,AT
    packet=bond_fixture();raw=bond_credit.encode(packet);out=bond_credit.project(raw,AT)
    assert verify_bond_credit.verify(raw,out)['comparisons_checked']==4
    out['evaluated_at']='2026-09-29T20:00:00Z'
    try:verify_bond_credit.verify(raw,out)
    except ValueError as error:assert 'not current' in str(error)
    else:raise AssertionError('Correct arithmetic with an expired source was admitted')
    out=bond_credit.project(raw,AT);out['comparisons'].pop('hy_minus_ig')
    try:verify_bond_credit.verify(raw,out)
    except ValueError as error:assert 'inventory' in str(error)
    else:raise AssertionError('Incomplete comparison inventory accepted')


def test_cadence_acceptance_requires_original_resources_and_has_no_invocation_or_output_write():
    path=ROOT/'aws/ops/staged/ops_6301_credit_cadence_acceptance.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6301_credit_cadence_acceptance.py'
    scope=runpy.run_path(str(path))
    original=json.loads((ROOT/'docs/audit/2026-09-28/bond-desk-normal-publication-acceptance.json').read_text(encoding='utf-8'))['runtime_acceptance']
    scope['original_operating_settings'](original,original)
    for field,value in [('timeout',181),('memory_mb',512),('schedules',[]),('handler','different')]:
        bad=copy.deepcopy(original);bad[field]=value
        try:scope['original_operating_settings'](bad,original)
        except ValueError:pass
        else:raise AssertionError('Operating settings drift accepted')
    assert len(scope['FUNCTIONS'])==10 and len(set(scope['FUNCTIONS']))==10
    source=path.read_text(encoding='utf-8')
    calls={n.func.attr for n in ast.walk(ast.parse(source)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','update_function_code','put_rule','update_schedule','start_execution','run','collect','acquire'}
    assert 'sys.exit(1)' in source and 'downstream_output_reads=0' in source


def test_acceptance_imports_production_clock_instead_of_same_named_candidate():
    from types import SimpleNamespace
    path=ROOT/'aws/ops/staged/ops_6301_credit_cadence_acceptance.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6301_credit_cadence_acceptance.py'
    tree=ast.parse(path.read_text(encoding='utf-8'));main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
    assignment=next(n for n in main.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Subscript))
    state={'ROOT':ROOT,'sys':SimpleNamespace(path=[])}
    exec(compile(ast.Module(body=[assignment],type_ignores=[]),'reviewed import path assignment','exec'),state)
    spec=importlib.machinery.PathFinder.find_spec('credit_collection_clock',state['sys'].path)
    assert Path(spec.origin).resolve()==(ROOT/'aws/shared/credit_collection_clock.py').resolve()
    before=ast.parse((ROOT/'tests/fixtures/pre-credit-cadence-acceptance.py.txt').read_text(encoding='utf-8'))
    before_main=next(n for n in before.body if isinstance(n,ast.FunctionDef) and n.name=='main')
    before_assignment=next(n for n in before_main.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Subscript))
    state={'ROOT':ROOT,'sys':SimpleNamespace(path=[])}
    exec(compile(ast.Module(body=[before_assignment],type_ignores=[]),'whole predecessor import assignment','exec'),state)
    predecessor=importlib.machinery.PathFinder.find_spec('credit_collection_clock',state['sys'].path)
    assert Path(predecessor.origin).resolve()==(ROOT/'aws/ops/checks/credit_collection_clock.py').resolve()
    assert hashlib.sha256(Path(spec.origin).read_bytes()).digest()!=hashlib.sha256(Path(predecessor.origin).read_bytes()).digest()


def test_real_native_compile_shared_consumer_and_independent_bond_check_agree_across_weekend():
    import credit_research,bond_credit,verify_bond_credit
    # Shift synthetic originals, including every response-vintage/request field.
    # No provider body is downloaded or a clock on an existing packet relabeled.
    inputs,bodies=fixture();inputs.update(started_at='2026-09-25T22:10:00Z',generated_at='2026-09-25T22:11:00Z',evaluation_date='2026-09-25')
    m=Memory()
    for sid,pair in inputs['sources'].items():
        for kind,item in pair.items():
            doc=json.loads(bodies[item['evidence']['key']])
            if kind=='definition':doc['seriess'][0].update(realtime_start='2026-09-25',realtime_end='2026-09-25')
            else:
                doc.update(realtime_start='2026-09-25',realtime_end='2026-09-25',observation_end='2026-09-25',
                    observation_start=str(date(2026,9,25)-timedelta(days=model.HISTORY_DAYS)))
                for row in doc['observations']:
                    row.update(realtime_start='2026-09-25',realtime_end='2026-09-25',date=str(date.fromisoformat(row['date'])+timedelta(days=7)))
            raw=model.encoded(doc);digest=model.sha(raw);key=model.PRIVATE+digest+'.bin';m.objects[key]=raw
            item['acquired_at']='2026-09-25T22:10:30Z';item['evidence'].update(key=key,sha256=digest,bytes=len(raw),request_url=model.source_url(sid,kind,'2026-09-25'))
    read=store.reader(m,'test');legacy=store.compile_output(inputs,read)
    old={**legacy,'replay':retained_legacy(inputs,m,legacy)}
    inputs['collection_policy']=clock.POLICY;new=store.compile_output(inputs,read)
    packet={**new,'replay':store.retain(m,'test',inputs,new)};raw=model.encoded(packet)
    at=datetime(2026,9,28,17,tzinfo=timezone.utc)
    assert credit_research.context(old,at)['available'] is False
    assert len(credit_research.context(packet,at)['measurements'])==28
    assert credit_research.qualified_signal(packet) is None
    projected=bond_credit.project(raw,at.isoformat());assert verify_bond_credit.verify(raw,projected)['comparisons_checked']==4
    expired=datetime(2026,9,28,20,5,tzinfo=timezone.utc)
    assert credit_research.context(packet,expired)['available'] is False
    projected=bond_credit.project(raw,expired.isoformat());assert verify_bond_credit.verify(raw,projected)['unavailable_comparisons']==4
