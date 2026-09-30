"""Configuration-only cutover and exact native receipt acceptance boundaries."""
from pathlib import Path
import ast,copy,hashlib,json,runpy,sys,tempfile
ROOT=Path(__file__).resolve().parents[2]
P='aws/ops/staged/ops_6374_snapshot_conditional_storage.py'
S=runpy.run_path(str(ROOT/P))
N=runpy.run_path(str(ROOT/'aws/ops/staged/ops_6373_snapshot_ordering_runtime_acceptance.py'))


def fixture():
    return {'Version':'2012-10-17','Id':'complete invented bucket','Statement':[
        {'Sid':'Invented'+str(i),'Effect':'Allow','Principal':'*','Action':['s3:GetObject'],
         'Resource':['arn:aws:s3:::invented-'+str(i)+'/public/*']} for i in range(31)]}


class Client:
    def __init__(self,value,drift_at=None,fail=None):self.value=copy.deepcopy(value);self.calls=[];self.reads=0;self.drift_at=drift_at;self.fail=fail
    def get_bucket_policy(self,**kw):
        assert kw=={'Bucket':S['BUCKET'],'ExpectedBucketOwner':S['OWNER']}
        self.calls.append('get');self.reads+=1
        if self.reads==self.drift_at:self.value['Statement'][0]['Resource'].append('arn:aws:s3:::invented-concurrent/public/*')
        return {'Policy':S['encoded'](self.value).decode(),'ResponseMetadata':{'HTTPStatusCode':200,'RequestId':'invented-'+str(self.reads)}}
    def put_bucket_policy(self,**kw):
        assert set(kw)=={'Bucket','ExpectedBucketOwner','Policy'}
        assert kw['Bucket']==S['BUCKET'] and kw['ExpectedBucketOwner']==S['OWNER'];self.calls.append('put')
        if self.fail!='before':self.value=S['document'](kw['Policy'])
        if self.fail:raise RuntimeError('invented lost acknowledgement')
    def __getattr__(self,name):raise AssertionError('Unreviewed data/mutation method: '+name)


def refuses(fn,cls=None):
    try:fn()
    except (S['PolicyUnavailable'] if cls is None else cls):pass
    else:raise AssertionError('Unreviewed storage change accepted')


def apply(client,baseline):
    # Pure configuration mutation cases isolate the separately-tested native
    # guard. No account access is supplied or authorized by this test client.
    old=S['apply'].__globals__['storage_guard'];rows=[]
    try:
        S['apply'].__globals__['storage_guard']=lambda c:hashlib.sha256(S['encoded'](c.value)).hexdigest()
        return S['apply'](client,baseline,lambda *row:rows.append(row)),rows
    finally:S['apply'].__globals__['storage_guard']=old


def test_scoped_guard_additions_preserve_every_predecessor_statement_and_field():
    before=fixture();saved=copy.deepcopy(before);after=S['prepare'](before)
    assert before==saved and after['Statement'][:31]==before['Statement']
    assert {k:v for k,v in before.items() if k!='Statement'}=={k:v for k,v in after.items() if k!='Statement'}
    assert after['Statement'][31:]==S['guard_statements']() and len(after['Statement'])==34
    # No general bucket deny, no multipart exemption, no widening existing ACLs.
    assert after['Statement'][31]['Condition']=={'Null':{'s3:if-match':'true','s3:if-none-match':'true'}}
    assert all(row['Effect']=='Deny' for row in after['Statement'][31:])


def test_conditional_policy_readbacks_idempotence_and_lost_ack_resolution():
    baseline=fixture();client=Client(baseline);result,rows=apply(client,baseline)
    assert result['policy_changed'] and client.calls==['get','get','put','get','get']
    assert [phase for phase,_ in rows]==['before','after'] and all(len(responses)==2 for _,responses in rows)
    assert not result['effective_write_authorization_tested'] and not result['pre_admitted_requests_cancelled']
    client.calls=[];assert apply(client,baseline)[0]['policy_changed'] is False and client.calls==['get']*4
    client=Client(baseline,fail='after');assert apply(client,baseline)[0]['policy_changed'] and client.calls.count('put')==1
    client=Client(baseline,fail='before');refuses(lambda:apply(client,baseline));assert client.value==baseline and client.calls.count('put')==1


def test_unknown_or_changing_policy_is_never_overwritten_or_rolled_back():
    baseline=fixture()
    for at in (1,2):
        client=Client(baseline,drift_at=at);refuses(lambda:apply(client,baseline));assert 'put' not in client.calls
    client=Client(baseline,drift_at=3);refuses(lambda:apply(client,baseline));assert client.calls.count('put')==1
    assert client.value['Statement'][0]['Resource'][-1]=='arn:aws:s3:::invented-concurrent/public/*'


def test_whole_retained_control_plane_predecessor_hash_and_policy_byte_bound():
    before=S['original_policy']();after=S['prepare'](before)
    assert len(S['encoded'](before))==19583 and len(S['encoded'](after))==20366
    assert after['Statement'][:31]==before['Statement']
    with tempfile.TemporaryDirectory() as directory:
        root=Path(directory);p=root/S['REPORT'];p.parent.mkdir(parents=True);p.write_bytes((ROOT/S['REPORT']).read_bytes()+b'\n')
        refuses(lambda:S['original_policy'](root))


def test_incomplete_duplicate_identifier_or_oversized_policy_refuses_without_write():
    for mutate in (lambda b:b['Statement'].pop(),lambda b:b['Statement'][0].update(Sid='SnapshotCASv1'),lambda b:b.update(Id='x'*20480)):
        value=fixture();mutate(value);refuses(lambda:S['prepare'](value))
    for raw in ('{}','{"Version":"2012-10-17","Statement":[],"Statement":[]}','{"Version":"2012-10-17","Statement":[],"bad":NaN}'):
        refuses(lambda:S['document'](raw))


def runtime():
    return {'function_name':'justhodl-portfolio-snapshot','receipt':{'status':'matched','commit':'a'*40},'source_files_checked':6,
        'memory_mb':512,'timeout':180,'runtime':'python3.12','handler':'lambda_function.lambda_handler','architectures':['x86_64'],
        'role':'invented-original-role','ephemeral_storage_mb':512,'schedules':[], 'code_sha256':'invented-whole-package',
        'active_alias':{'alias':'live','version':'15','code_sha256':'invented-whole-package'}}


def test_exact_new_six_file_native_closure_and_promoted_alias_are_required():
    fn='justhodl-portfolio-snapshot';base=runtime();N['validate'](base,'a'*40,base,fn)
    for key,val in [('source_files_checked',5),('source_files_checked',True),('receipt',{'status':'matched','commit':'b'*40}),('active_alias',{'alias':'live','version':'15','code_sha256':'different'})]:
        bad=copy.deepcopy(base);bad[key]=val;refuses(lambda:N['validate'](bad,'a'*40,base,fn),ValueError)
    for key,val in [('memory_mb',1024),('timeout',900),('runtime','other'),('role','other'),('schedules',[{'kind':'new','name':'new'}])]:
        bad=copy.deepcopy(base);bad[key]=val;refuses(lambda:N['validate'](bad,'a'*40,base,fn),ValueError)


def test_native_acceptance_receipt_only_and_policy_write_preceded_by_exact_package_check():
    calls=[]
    class Read:
        def get_object(self,**kw):calls.append(kw);return {}
    reader=N['ReceiptOnly'](Read(),'justhodl-portfolio-snapshot')
    reader.get_object(Bucket=N['BUCKET'],Key='data/ops/releases/justhodl-portfolio-snapshot.json')
    refuses(lambda:reader.get_object(Bucket=N['BUCKET'],Key='portfolio/snapshot.json'),ValueError)
    text=(ROOT/P).read_text();tree=ast.parse(text)
    methods={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not methods&{'invoke','put_object','put_item','head_object','delete_object','put_rule','update_schedule'}
    assert 'sys.exit(1)' in text and text.index('native.verify_runtime(result)')<text.index('result.kv(**apply(client')
    assert len(calls)==1


if __name__=='__main__':
    tests=[v for k,v in globals().copy().items() if k.startswith('test_') and callable(v)]
    for test in tests:test()
    print('Snapshot conditional storage tests:',len(tests),'passed')
