"""Complete synthetic control-plane responses; never access Cloudflare or users."""
from pathlib import Path
from email.message import Message
from unittest.mock import patch
import ast
import base64
import copy
import hashlib
import io
import json
import runpy
import sys
import urllib.error

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'aws/ops/checks'))
import worker_source_evidence as w

DID = '11111111-1111-1111-1111-111111111111'
VID = '22222222-2222-2222-2222-222222222222'
CODE = b'export default { fetch() { return new Response("synthetic"); } };\n'


def refuses(fn):
    try:
        fn()
    except w.EvidenceError:
        return
    raise AssertionError('Invalid evidence accepted')


class Response:
    def __init__(self, raw=b'', headers=(), status=200, chunk=7):
        self.data = io.BytesIO(raw); self.headers = Message(); self.status = status
        self.closed = False; self.read_sizes = []; self.chunk = chunk
        for k, v in headers:
            self.headers[k] = v
    def read1(self, n):
        self.read_sizes.append(n)
        return self.data.read(min(n, self.chunk))
    def close(self): self.closed = True
    def __enter__(self): return self
    def __exit__(self, *args): self.close()


def multipart(boundary='synthetic-boundary', bodies=None):
    bodies = bodies or [('index.js', CODE, 'application/javascript+module'), ('opaque.bin', b'\x00\xff\r\n\x80\n', 'application/octet-stream')]
    raw = b''
    for name, body, kind in bodies:
        raw += ('--'+boundary+'\r\nContent-Disposition: form-data; name="'+name+'"; filename="'+name+'"\r\nContent-Type: '+kind+'\r\n\r\n').encode()+body+b'\r\n'
    raw += ('--'+boundary+'--\r\n').encode()
    return raw, {'Content-Type':'multipart/form-data; boundary='+boundary, 'cf-entrypoint':'index.js', 'ETag':'opaque-synthetic-tag'}


def fixture():
    return {'/deployments': {'deployments': [{'id':DID, 'versions':[{'version_id':VID, 'percentage':100}]}]},
        '/versions/'+VID: {'id':VID, 'resources':{'script':{'etag':'opaque-synthetic-tag'}}},
        '/settings': {'bindings':[{'name':'PRIVATE_DATA', 'type':'kv_namespace', 'namespace_id':'synthetic'},
            {'name':'SERVICE', 'type':'secret_text'}], 'compatibility_date':'2025-10-01'},
        '/schedules': {'schedules':[]}, '/content/v2': multipart()}


class FakeControl:
    def __init__(self, mutations=None): self.values=fixture(); self.counts={}; self.read_count=0; self.mutations=mutations or {}
    def get(self, path):
        self.read_count += 1; self.counts[path] = self.counts.get(path, 0)+1
        result=copy.deepcopy(self.values[path])
        if (path, self.counts[path]) in self.mutations:
            result=self.mutations[path, self.counts[path]](result)
        return result


def test_worker_capture_retains_all_original_member_bytes_and_reports_limits():
    client=FakeControl(); result=w.capture(client)
    raw=base64.b64decode(result['transport']['raw_body_base64'], validate=True)
    assert raw==fixture()['/content/v2'][0]
    assert result['transport']['sha256']==hashlib.sha256(raw).hexdigest()
    assert w.source_bodies(raw, result['transport']['headers'])['opaque.bin'][0]==b'\x00\xff\r\n\x80\n'
    assert result['source_members']['index.js']['sha256']==hashlib.sha256(CODE).hexdigest()
    assert result['control_plane_gets']==9
    assert result['private_reads']==result['worker_invocations']==result['mutations']==0
    assert result['source_active_version_binding_verified'] is False
    assert result['intended_repo_build_verified'] is False


def test_worker_capture_transport_boundaries_can_change_without_source_drift():
    result=w.capture(FakeControl({('/content/v2', 2):lambda _:multipart('different-boundary')}))
    assert len(result['source_members'])==2


def test_worker_capture_refuses_code_binding_schedule_or_deployment_races():
    changes={
        ('/content/v2',2): lambda _:multipart(bodies=[('index.js',CODE+b'// changed', 'application/javascript+module')]),
        ('/settings',2): lambda v:{**v, 'bindings':v['bindings']+[{'name':'EXTRA', 'type':'plain_text', 'text':'synthetic'}]},
        ('/schedules',2): lambda _:{'schedules':[{'cron':'* * * * *'}]},
        ('/deployments',2): lambda _:{'deployments':[{'id':VID,'versions':[{'version_id':VID,'percentage':100}]}]},
    }
    for key, value in changes.items(): refuses(lambda:w.capture(FakeControl({key:value})))


def test_worker_capture_requires_exact_version_and_full_traffic():
    for value in [None,{}, {'deployments':[]}, {'deployments':[{'id':DID,'versions':[]}]},
        {'deployments':[{'id':DID,'versions':[{'version_id':VID,'percentage':True}]}]},
        {'deployments':[{'id':DID,'versions':[{'version_id':VID,'percentage':99}]}]}]:
        refuses(lambda:w.active_deployment(value))
    client=FakeControl();client.values['/versions/'+VID]['id']=DID
    refuses(lambda:w.capture(client))
    client=FakeControl();client.values['/versions/'+VID]['resources']=None
    assert w.capture(client)['version_script_etag'] is None


def test_worker_source_multipart_rejects_truncation_duplicates_and_nested_parts():
    raw,headers=multipart()
    refuses(lambda:w.source_members(raw[:-8],headers))
    raw,headers=multipart(bodies=[('same.js',CODE,'application/javascript'),('same.js',CODE,'application/javascript')])
    refuses(lambda:w.source_members(raw,headers))
    raw,headers=multipart(bodies=[('nested',b'--x--\r\n','multipart/mixed; boundary=x')])
    refuses(lambda:w.source_members(raw,headers))
    for kind in ['', 'text/html', 'text/\u00e9', 'text/javascript\r\nOther: x']:
        refuses(lambda:w.source_members(CODE,{'Content-Type':kind}))


def test_worker_source_direct_script_has_complete_exact_bytes():
    result=w.source_members(CODE,{'Content-Type':'application/javascript; charset=utf-8','cf-entrypoint':'worker.js'})
    assert result['worker.js']['bytes']==len(CODE)
    assert result['worker.js']['sha256']==hashlib.sha256(CODE).hexdigest()


def test_worker_complete_acquisition_is_bounded_and_requires_eof():
    response=Response(CODE,[('Content-Length',str(len(CODE)))])
    assert w.complete(response,2,lambda:1)==CODE
    assert all(0<n<=65536 for n in response.read_sizes)
    for headers in [[('Content-Length','999')],[('Content-Length','+2')],[('Content-Length','2'),('Content-Length','2')],
        [('Content-Length','\u0662')],[('Content-Encoding','gzip')]]:
        refuses(lambda:w.complete(Response(CODE,headers),2,lambda:1))
    for status in [204,206,301,403,500]: refuses(lambda:w.complete(Response(CODE,status=status),2,lambda:1))
    assert w.complete(Response(CODE,[('Content-Length',' \t'+str(len(CODE))+'\t ')]),2,lambda:1)==CODE
    with patch.object(w,'LIMIT',8): refuses(lambda:w.complete(Response(CODE),2,lambda:1))


def test_worker_complete_acquisition_rejects_slow_final_read():
    clock=iter([1,3])
    refuses(lambda:w.complete(Response(CODE),2,lambda:next(clock)))


def test_worker_control_plane_closes_success_failure_and_never_mutates():
    observed=[]; response=Response(json.dumps({'success':True,'errors':[],'result':{'synthetic':1}}).encode())
    def open_(request,timeout): observed.append((request,timeout)); return response
    client=w.ControlPlane('a'*32,'synthetic-token-for-tests',opener=open_)
    assert client.get('/settings')=={'synthetic':1} and response.closed
    request,timeout=observed[0]
    assert request.method=='GET' and request.data is None and timeout==30
    assert request.full_url==client.base+'/settings'
    for path in ['/storage/kv','/../kv','/versions/'+VID+'?x=y','/settings?x=y','https://example.org']:
        refuses(lambda:client.get(path))
    assert len(observed)==1
    response=Response(b'bad',status=500)
    refuses(lambda:client.get('/settings')); assert response.closed


def test_worker_control_plane_never_retains_echoed_credentials_or_error_body():
    token='synthetic-only-credential-value'
    for raw in [token.encode(),json.dumps({'success':True,'result':token}).encode()]:
        response=Response(raw)
        client=w.ControlPlane('a'*32,token,opener=lambda *a,**k:response)
        refuses(lambda:client.get('/content/v2')); assert response.closed
    body=io.BytesIO(token.encode())
    error=urllib.error.HTTPError('https://synthetic.invalid',403,token,{},body)
    def broken(*a,**k):raise error
    client=w.ControlPlane('a'*32,token,opener=broken)
    try:client.get('/settings')
    except w.EvidenceError as exc:assert token not in str(exc) and 'HTTP_403' in str(exc)
    else:raise AssertionError('HTTP failure accepted')
    assert body.closed


def test_worker_control_plane_refuses_redirects_and_invalid_json():
    refuses(lambda:w.NoRedirect().redirect_request(None,io.BytesIO(),302,'',{},'https://other.invalid'))
    for raw in [b'{"a":1,"a":2}',b'{"a":NaN}',b'{"a":1e999}',b'{"a":"\\ud800"}',b'\xff',b'{} trailing',b'['*130+b'0'+b']'*130]:
        refuses(lambda:w.document(raw))
    assert w.document(b'{"integer":9007199254740993}')['integer']==9007199254740993


def test_worker_configuration_hashes_values_without_exposing_them():
    settings=fixture()['/settings'];settings['bindings'][0]['namespace_id']='sensitive-test-value'
    config=w.configuration(settings,{'schedules':[]})
    assert 'sensitive-test-value' not in json.dumps(config)
    reordered=copy.deepcopy(settings);reordered['bindings'].reverse()
    assert w.configuration(reordered,{'schedules':[]})==config
    reordered['bindings'][0]['other']='change'
    assert w.configuration(reordered,{'schedules':[]})!=config
    for bad in [{}, {'bindings':[None]}, {'bindings':[{'name':'same','type':'x'}]*2}]:
        refuses(lambda:w.configuration(bad,{'schedules':[]}))


def test_worker_source_retention_scans_decoded_code_before_writing():
    path=ROOT/'aws/ops/staged/ops_6354_worker_source_predecessor.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6354_worker_source_predecessor.py'
    scope=runpy.run_path(str(path));result=w.capture(FakeControl())
    assert json.loads(scope['prepare'](result,set()))['source_members']==result['source_members']
    text='synthetic_retired_token_with_32_characters'
    raw,headers=multipart(bodies=[('index.js',('const token="'+text+'";').encode(),'application/javascript')])
    result['transport'].update(raw_body_base64=base64.b64encode(raw).decode(),headers=headers)
    refuses(lambda:scope['prepare'](result,{hashlib.sha256(text.encode()).hexdigest()}))


def test_worker_capture_workflow_is_fixed_and_does_not_inherit_aws_access():
    workflow=(ROOT/'.github/workflows/worker-source-evidence.yml').read_text(encoding='utf-8')
    direct=(ROOT/'.github/workflows/run-ops-direct.yml').read_text(encoding='utf-8')
    assert 'workflow_dispatch:' in workflow and 'expected_commit:' in workflow
    assert 'CLOUDFLARE_API_TOKEN' in workflow and 'CLOUDFLARE_API_TOKEN' not in direct
    assert 'aws-actions/' not in workflow and 'AWS_ACCESS_KEY' not in workflow and 'wrangler' not in workflow
    assert 'ops_6354_worker_source_predecessor.py' in workflow and 'test "$pushed" = 1' in workflow
    path=ROOT/'aws/ops/staged/ops_6354_worker_source_predecessor.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6354_worker_source_predecessor.py'
    source=path.read_text(encoding='utf-8');attrs={n.func.attr for n in ast.walk(ast.parse(source)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not attrs & {'invoke','put_object','get_object','update_function_code','put_rule','update_schedule'}
    assert 'sys.exit(1)' in source and "target.open('xb')" in source


if __name__=='__main__':
    tests=[value for name,value in sorted(globals().items()) if name.startswith('test_') and callable(value)]
    for test in tests:test()
    print('Worker source evidence tests passed:',len(tests))
