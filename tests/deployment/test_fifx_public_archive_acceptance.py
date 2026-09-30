"""Whole invented native sources and canonical originals; no live clients."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import ast
import gzip
import importlib.util
import io
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT/p) for p in ('aws/lambdas/justhodl-fifx-vol-migration/source','aws/ops/checks','aws/shared','tests')]
import fifx_archive_acceptance as archive
import fifx_store as store
import fifx_model as model
import test_fifx_candidate as fixture
import report_observations as macro


def rejected(fn):
    try: fn()
    except (ValueError, TypeError, KeyError, EOFError, gzip.BadGzipFile): return
    raise AssertionError('Invalid evidence received acceptance')


def native():
    path = ROOT/'aws/lambdas/justhodl-fifx-vol-migration/tests/run_tests.py'
    spec = importlib.util.spec_from_file_location('native_fifx_archive_fixture', path)
    result = importlib.util.module_from_spec(spec); spec.loader.exec_module(result)
    return result


def whole_case(missing=(), wrong_move=False):
    case = native(); client = case.Store(); inputs, _ = case.inputs(client)
    originals = {}
    for sid in store.catalog.FRED:
        definition = fixture.definition(sid)
        definition['seriess'][0].update(title='Invented '+sid, seasonal_adjustment_short='NSA')
        rows = [dict(zip(('date','value'), line.split(','))) for line in fixture.csv(sid,n=42).decode().splitlines()[1:]][::-1]
        observations = {'observations': rows, 'units':'lin', 'output_type':1, 'count':len(rows), 'limit':4000, 'offset':0}
        evidence = {}
        for kind, doc in (('definition',definition), ('observations',observations)):
            raw = model.encoded(doc); digest = store.sha(raw)
            url = 'https://api.stlouisfed.org/fred/series'+('/observations' if kind=='observations' else '')+'?series_id='+sid
            if kind=='observations': url += '&units=lin&limit=4000&sort_order=desc'
            key = 'data/evidence/fred/'+store.sha(url.encode())+'/'+digest+'.bin.gz'
            client.objects[key] = gzip.compress(raw, mtime=0)
            evidence[kind] = {'contract':'source-evidence.v1','provider':'fred','captured':True,'source_url':url,
                'key':key,'sha256':digest,'bytes':len(raw),'first_received_at':fixture.NOW}
        originals[sid] = {'definition':definition,'observations':observations,'evidence':evidence,'acquired_at':fixture.NOW}
    packet = macro.build({sid:{'category':'invented','display_name':sid} for sid in store.catalog.FRED}, originals, fixture.NOW)
    compiler = Path(macro.__file__).read_bytes(); compiler_key = 'data/report-research/compilers/'+store.sha(compiler)+'.py'
    run = {'contract':'report-research-replay.v1','generated_at':fixture.NOW,'catalog':packet['catalog'],
           'inputs':{sid:{'evidence':row['evidence'],'acquired_at':row['acquired_at']} for sid,row in originals.items()},
           'compiler':{'key':compiler_key,'sha256':store.sha(compiler)},'output_sha256':macro.digest(packet)}
    raw = model.encoded(run); key = 'data/report-research/runs/'+store.sha(raw)+'.json'
    client.objects[key] = raw; client.objects[compiler_key] = compiler
    packet['replay'] = {'manifest_key':key,'output_sha256':run['output_sha256'],'compiler_sha256':store.sha(compiler)}
    inputs['macro'] = store.retain_bytes(client,'bucket',model.encoded(packet),'snapshots')
    for sid in missing: inputs['sources'][sid] = {'original':None,'receipt':None}
    if wrong_move:
        quote = fixture.quote('^MOVE',n=42); meta = quote['chart']['result'][0]['meta']
        meta.update(shortName='Invented wrong instrument',longName='Invented wrong instrument')
        raw = model.encoded(quote)
        inputs['sources']['^MOVE'] = {'original':store.retain_bytes(client,'bucket',raw,'originals','bin'),
            'receipt':store.retain_bytes(client,'bucket',model.encoded(fixture.receipt('^MOVE',raw)),'receipts')}
    inputs['acquisition']['sources'] = {sid:{'status':'unavailable' if sid in missing else 'received'} for sid in store.catalog.SOURCES}
    output = store.retain(client,'bucket',inputs)
    return client.objects, output, inputs


class PublicClient:
    def __init__(self,objects):
        self.objects=objects; self.reads=[]; self.listings=0; self.pages=None; self.change=False
    def get_object(self,**request):
        assert request['Bucket']=='invented' and set(request)=={'Bucket','Key'}
        key=request['Key']; self.reads.append(key)
        return {'Body':io.BytesIO(self.objects[key]),'ContentLength':len(self.objects[key])}
    def get_paginator(self,name):
        assert name=='list_objects_v2'; return self
    def paginate(self,**request):
        assert request=={'Bucket':'invented','Prefix':model.PREFIX+'runs/'}
        self.listings+=1
        if self.pages is not None: yield from self.pages; return
        rows=[{'Key':key,'Size':len(raw),'ETag':'"invented"','LastModified':datetime(2026,9,26,tzinfo=timezone.utc)}
              for key,raw in sorted(self.objects.items()) if key.startswith(request['Prefix'])]
        if self.change and self.listings==2: rows[0]['ETag']='"changed"'
        yield {'Name':'invented','Prefix':request['Prefix'],'IsTruncated':False,'KeyCount':len(rows),'Contents':rows,
               'ResponseMetadata':{'RequestId':'invented-'+str(self.listings)}}


def inspect(client, cutoff='2026-09-01T00:00:00Z'):
    return archive.inspect(client,'invented',store,model,cutoff=cutoff,checked_at='2026-09-30T00:00:00Z')


def test_whole_canonical_and_native_originals_replay_every_source_without_live_heads():
    objects, packet, _ = whole_case(); before=deepcopy(objects); client=PublicClient(objects)
    result=inspect(client)
    assert result['replayed'] and len(result['source_recovery'])==18
    assert result['quality']==packet['quality'] and result['quality']['original_rows']==18*42
    assert len(result['complete_arithmetic_proofs'])==18
    assert sum(p['original_rows'] for p in result['complete_arithmetic_proofs'].values())==18*42
    assert result['inventory_before']['entries']==result['inventory_after']['entries']
    assert result['inventory_before']['pages']!=result['inventory_after']['pages']
    assert not result['current_head_read'] and not result['current_head_publication_verified'] and not result['investment_authority']
    assert objects==before and all('/'+store.sha(archive.PublicArchiveReader(client,'invented')(key))+'.' in key for key in client.reads[:])


def test_missing_sources_and_wrong_move_identity_do_not_become_recovery():
    objects, packet, _=whole_case(missing=('DGS10','DEXJPUS'),wrong_move=True)
    result=inspect(PublicClient(objects)); assert result['replayed'] and not result['complete_source_recovery']
    for sid in ('DGS10','DEXJPUS','^MOVE'):
        assert not result['source_recovery'][sid]['current_available_at_run']
    assert result['source_recovery']['^MOVE']['quality']['status']=='identity_mismatch'
    assert result['source_recovery']['^MOVE']['retained_original_rows']==42
    assert result['quality']==packet['quality']


def test_empty_and_pre_repair_archives_remain_pending():
    assert not inspect(PublicClient({}))['replayed']
    objects, _, _=whole_case(); client=PublicClient(objects)
    result=inspect(client,'2026-09-29T00:00:00Z')
    assert result['status']=='pending_original_post_repair_archive' and len(client.reads)==1


def test_corrupted_whole_original_compiler_and_gzip_dependencies_cannot_pass():
    objects, packet, inputs=whole_case(); manifest=store.strict(objects[packet['replay']['manifest_key']])
    keys=(packet['replay']['manifest_key'],inputs['sources']['DGS10']['original']['key'],manifest['compilers']['fifx_candidate']['key'],
          next(key for key in objects if key.endswith('.gz')))
    for key in keys:
        broken=deepcopy(objects); broken[key]=b'whole invented corrupt object'
        rejected(lambda:inspect(PublicClient(broken)))


def test_conflicting_or_future_runs_and_inventory_changes_refuse_acceptance():
    objects, packet, _=whole_case(); original=store.strict(objects[packet['replay']['manifest_key']])
    for change in ({'generated_at':'2099-01-01T00:00:00Z'},{'whole_invented_conflict':True}):
        changed=deepcopy(objects); raw=model.encoded({**original,**change}); changed[model.PREFIX+'runs/'+store.sha(raw)+'.json']=raw
        rejected(lambda:inspect(PublicClient(changed)))
    client=PublicClient(objects); client.change=True; rejected(lambda:inspect(client))


def test_private_current_provider_and_unreviewed_paths_fail_before_storage_access():
    client=PublicClient({}); read=archive.PublicArchiveReader(client,'invented')
    for key in (model.CURRENT,model.HISTORY,store.SOURCE,store.BOND,model.PRIVATE+'requests/'+64*'a'+'.json',
                'data/prospective-outcomes.json','data/scorecard.json',model.PREFIX+'runs/../current.json',
                'data/bond-vol-research/views/'+64*'a'+'.json','https://example.invalid/original'):
        rejected(lambda key=key:read(key))
    assert client.reads==[]


def test_fragmented_complete_streams_are_read_to_eof_and_every_failure_closes():
    raw=b'{"whole_invented_object":[1,2,3,null,false]}\n'
    class Fragments(io.BytesIO):
        def read(self,n=-1): return super().read(min(7,n))
    stream=Fragments(raw); assert archive.complete(stream,len(raw))==raw and stream.closed
    for expected,limit in ((len(raw)+1,1000),(True,1000),(len(raw),len(raw)-1),(None,5)):
        stream=Fragments(raw); rejected(lambda:archive.complete(stream,expected,limit)); assert stream.closed
    stream=io.BytesIO(b''); rejected(lambda:archive.complete(stream)); assert stream.closed


def test_declared_stored_bytes_and_decoded_identity_are_separately_verified():
    raw=b'{"whole_invented_original":true}'; digest=store.sha(raw)
    key='data/evidence/fred/'+64*'a'+'/'+digest+'.bin.gz'
    client=PublicClient({key:gzip.compress(raw,mtime=0)}); read=archive.PublicArchiveReader(client,'invented')
    assert read(key)==raw and read.reads[key]['bytes']==len(raw)
    assert read.reads[key]['stored_bytes']==len(client.objects[key])
    response=client.get_object(Bucket='invented',Key=key); response['ContentLength']=True
    client.get_object=lambda **kw:response
    rejected(lambda:read(key)); assert response['Body'].closed


def test_complete_listing_rejects_wrong_scope_duplicates_and_missing_terminal_page():
    good={'Name':'invented','Prefix':model.PREFIX+'runs/','IsTruncated':False,'KeyCount':0,'Contents':[]}
    for pages in ([],[good,good],[{**good,'Name':'wrong'}],[{**good,'KeyCount':True}],
                  [{**good,'IsTruncated':True,'NextContinuationToken':'invented'}]):
        client=PublicClient({});client.pages=pages;rejected(lambda:inspect(client))
    objects, _, _=whole_case();client=PublicClient(objects)
    page=next(client.paginate(Bucket='invented',Prefix=model.PREFIX+'runs/'))
    client.pages=[{**page,'Contents':page['Contents']*2,'KeyCount':2}]; rejected(lambda:inspect(client))


def operation():
    path=ROOT/'aws/ops/staged/ops_6378_fifx_original_archive_acceptance.py'
    spec=importlib.util.spec_from_file_location('fifx_archive_op',path)
    op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op);return op


def test_exact_native_settings_receipt_and_sdk_types_are_required():
    op=operation(); base=json.loads(op.BASELINE.read_bytes())['evidence']['actual_runtime'];op.validate(base)
    for change in ({'timeout':900.0},{'source_files_checked':14.0},{'memory_mb':1024},{'code_sha256':'wrong'},
                   {'receipt':{'status':'matched','commit':'a'*40}},{'schedules':[]}):
        rejected(lambda change=change:op.validate({**base,**change}))
    stamp=datetime(2026,9,26,tzinfo=timezone.utc)
    assert op.jsonable([stamp,False,1,1.0,None])==[{'sdk_type':'datetime','iso8601':stamp.isoformat()},False,1,1.0,None]


def test_operation_cannot_expand_into_provider_invocation_or_private_reads():
    op=operation()
    class Client:
        def get_object(self,**request):return request
        def get_function_configuration(self,**request):
            return {'FunctionName':op.FN,'CodeSha256':'invented','Environment':{'Variables':{'S3_BUCKET':op.BUCKET,'UNRELATED':'invented-secret'}}}
    client=Client();read=op.ReceiptOnly(client)
    assert read.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json')['Bucket']==op.BUCKET
    rejected(lambda:read.get_object(Bucket=op.BUCKET,Key=model.CURRENT))
    assert 'invented-secret' not in json.dumps(op.origin(client,{'code_sha256':'invented'}))
    rejected(lambda:op.origin(client,{'code_sha256':'wrong'}))
    source=Path(op.__file__).read_text(encoding='utf-8'); tree=ast.parse(source)
    calls={node.func.attr for node in ast.walk(tree) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','update_schedule','put_rule','get_secret_value','get_parameter','acquire'}
    assert 'sys.exit(1)' in source
