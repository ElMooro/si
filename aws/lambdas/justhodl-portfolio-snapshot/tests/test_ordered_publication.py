"""Current native handler/protocol with whole invented stores and failure schedules.

External networking is forbidden. No private account/artifact, provider or live
producer is consulted. The Worker protocol also has its own local workerd suite.
"""
from pathlib import Path
from email.message import Message
from unittest.mock import patch
import contextlib,copy,hashlib,io,json,socket,sys,types,unittest

ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-portfolio-snapshot/source')]
import portfolio_snapshot_ordering as ordering
import portfolio_snapshot_publication as wire
from test_watchlist_sync import load_current

TOKEN='00000000-0000-4000-8000-000000000001'
STAMP='2026-09-30T15:00:00Z'


class Fault(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


def ok(**values):return {**values,'ResponseMetadata':{'HTTPStatusCode':200}}


def policy():
    return {'Version':'2012-10-17','Statement':[
        {'Sid':'Audit20260909PrivatePersonalArtifacts','Effect':'Deny','Principal':'*',
         'Action':['s3:GetObject','s3:GetObjectVersion'],
         'Condition':{'StringNotEquals':{'aws:PrincipalAccount':ordering.ACCOUNT}},
         'Resource':ordering.guard_statements()[-1]['Resource']},*ordering.guard_statements()]}


class Store:
    def __init__(self):
        self.objects={};self.calls=[];self.version=0;self.policy=policy();self.rules=[]
        self.before_put=None;self.after_put=None;self.mutate_read=None;self.bodies=[]
    def get_bucket_policy(self,**kw):self.calls.append(('policy',kw));return ok(Policy=json.dumps(self.policy))
    def get_bucket_versioning(self,**kw):self.calls.append(('versioning',kw));return ok(Status='Enabled')
    def get_bucket_lifecycle_configuration(self,**kw):self.calls.append(('lifecycle',kw));return ok(Rules=copy.deepcopy(self.rules))
    def seed(self,key,raw,metadata=None):
        self.version+=1;self.objects[key]={'raw':raw,'etag':'"invented-version-'+str(self.version)+'"','metadata':copy.deepcopy(metadata or {})}
    def get_object(self,**kw):
        self.calls.append(('read',kw));key=kw['Key']
        if key not in self.objects:raise Fault('NoSuchKey')
        row=self.objects[key];body=io.BytesIO(row['raw']);self.bodies.append(body)
        response=ok(Body=body,ETag=row['etag'],ContentLength=len(row['raw']),Metadata=copy.deepcopy(row['metadata']))
        if self.mutate_read:self.mutate_read(key,response)
        return response
    def put_object(self,**kw):
        self.calls.append(('write',kw));key=kw['Key']
        if self.before_put:self.before_put(kw)
        current=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and current:raise Fault('PreconditionFailed')
        if 'IfMatch' in kw and (not current or current['etag']!=kw['IfMatch']):raise Fault('PreconditionFailed')
        assert 'IfMatch' in kw or kw.get('IfNoneMatch')=='*'
        assert kw['ExpectedBucketOwner']==ordering.ACCOUNT and kw['CacheControl']=='private, no-store'
        self.seed(key,kw['Body'],kw.get('Metadata'))
        if self.after_put:self.after_put(kw)
        return ok(ETag=self.objects[key]['etag'])


class Mirror:
    def __init__(self,validate):self.counter=0;self.tokens={};self.current=None;self.calls=[];self.validate=validate;self.failure=None;self.after=None
    def __call__(self,method,raw,token=None,context=None):
        self.calls.append((method,raw,token));doc=json.loads(raw)
        if self.failure:self.failure(method,raw)
        if method=='POST':
            self.counter=max(self.counter,doc['minimum_revision'])+1
            issued='00000000-0000-4000-8000-'+str(self.counter).zfill(12);self.tokens[self.counter]=issued
            return {'ok':True,'protocol':ordering.PROTOCOL,'revision':self.counter,'token':issued}
        revision=doc['publication']['revision']
        if self.current and revision<self.current['revision']:raise ordering.MirrorConflict('superseded_publication')
        if self.current and revision==self.current['revision']:
            if raw!=self.current['raw'] or token!=self.current['token']:raise ordering.OrderingUnavailable('conflicting invented mirror body')
            status='unchanged'
        else:
            if self.tokens.get(revision)!=token:raise ordering.MirrorConflict('unknown_publication_reservation')
            status='published';self.current={'raw':raw,'revision':revision,'token':token}
        if self.after:self.after(method,raw)
        return {'ok':True,'protocol':ordering.PROTOCOL,'revision':revision,'body_bytes':len(raw),
                'body_sha256':hashlib.sha256(raw).hexdigest(),'identity_bytes':self.validate(doc),'status':status}


class Ordered(unittest.TestCase):
    def setUp(self):
        self.mod=load_current();self.validate=self.mod.validate_snapshot_publication;self.store=Store();self.mirror=Mirror(self.validate)
        p=patch.object(ordering,'request',self.mirror);p.start();self.addCleanup(p.stop)
        p=patch.object(socket.socket,'connect',side_effect=AssertionError('External networking forbidden'));p.start();self.addCleanup(p.stop)
    def attempt(self):return ordering.begin_snapshot_publication(self.store,self.validate)
    def candidate(self,attempt,n=1):
        doc={'positions':[{'symbol':'INVENTED','qty':n,'unknown':{'negative_zero':-0.0,'unicode':'π😀'}}],
             'capital_book':{'status':'BLOCKED','allows_new_entries':False},'publication':dict(attempt['frame'])}
        raw=wire.encode_snapshot(doc);return raw,self.validate(doc)
    def publish(self,attempt,n=1):
        raw,identity=self.candidate(attempt,n);return ordering.finish_snapshot_publication(self.store,self.validate,attempt,raw,identity)
    def legacy(self):
        raw=b'{ "positions": [], "complete_original": "invented old body", "n": -0.0 }\n'
        self.store.seed(ordering.KEY,raw);return raw
    def test_complete_same_bytes_and_private_outbox_then_both_representations(self):
        old=self.legacy();attempt=self.attempt();raw,identity=self.candidate(attempt)
        out=ordering.finish_snapshot_publication(self.store,self.validate,attempt,raw,identity)
        self.assertTrue(out['published']);self.assertEqual(self.store.objects[ordering.KEY]['raw'],raw);self.assertEqual(self.mirror.current['raw'],raw)
        key=ordering.ARCHIVE+hashlib.sha256(old).hexdigest()+'.json';self.assertEqual(self.store.objects[key]['raw'],old)
        self.assertEqual(self.store.objects[ordering.KEY]['metadata'],{ordering.META:attempt['token']});self.assertNotIn(attempt['token'].encode(),raw)
        writes=[kw for kind,kw in self.store.calls if kind=='write'];self.assertEqual([kw['Key'] for kw in writes],[key,ordering.KEY]);self.assertIn('IfMatch',writes[-1])
        self.assertTrue(all(body.closed for body in self.store.bodies))
    def test_absent_current_uses_if_none_match(self):
        attempt=self.attempt();self.assertTrue(self.publish(attempt)['published'])
        writes=[kw for kind,kw in self.store.calls if kind=='write'];self.assertEqual(len(writes),1);self.assertEqual(writes[0]['IfNoneMatch'],'*')
    def test_slow_older_attempt_cannot_replace_newer_completion(self):
        older,newer=self.attempt(),self.attempt();self.assertLess(older['frame']['revision'],newer['frame']['revision'])
        self.assertTrue(self.publish(newer,2)['published']);raw=self.store.objects[ordering.KEY]['raw']
        self.assertEqual(self.publish(older,1)['status'],'superseded_before_s3');self.assertEqual(self.store.objects[ordering.KEY]['raw'],raw);self.assertEqual(self.mirror.current['raw'],raw)
    def test_cas_conflict_reloads_and_retains_concurrent_whole_predecessor(self):
        old=self.legacy();first,second=self.attempt(),self.attempt();trigger=[]
        def race(kw):
            if kw['Key']==ordering.KEY and not trigger:
                trigger.append(True);self.store.before_put=None;self.assertTrue(self.publish(first)['published'])
        self.store.before_put=race;self.assertTrue(self.publish(second,2)['published'])
        archives=[row['raw'] for key,row in self.store.objects.items() if key.startswith(ordering.ARCHIVE)]
        self.assertIn(old,archives);self.assertIn(self.candidate(first)[0],archives);self.assertEqual(self.mirror.current['raw'],self.candidate(second,2)[0])
    def test_same_revision_different_body_or_token_is_never_replaced(self):
        attempt=self.attempt();self.publish(attempt);before=copy.deepcopy(self.store.objects)
        with self.assertRaisesRegex(ordering.OrderingUnavailable,'body conflict'):self.publish(attempt,2)
        changed=copy.deepcopy(attempt);changed['token']=TOKEN
        if changed['token']==attempt['token']:changed['token']='10000000-0000-4000-8000-000000000001'
        with self.assertRaisesRegex(ordering.OrderingUnavailable,'body conflict'):self.publish(changed)
        self.assertEqual(self.store.objects,before)
    def test_exact_retry_only_reacknowledges_without_replacing_s3(self):
        attempt=self.attempt();self.publish(attempt);before=self.store.version
        self.assertTrue(self.publish(attempt)['published']);self.assertEqual(self.store.version,before)
    def test_archive_mismatch_prevents_current_and_mirror_replacement(self):
        raw=self.legacy();key=ordering.ARCHIVE+hashlib.sha256(raw).hexdigest()+'.json';self.store.seed(key,b'{"invented":"conflicting"}')
        attempt=self.attempt()
        with self.assertRaisesRegex(ordering.OrderingUnavailable,'not retained'):self.publish(attempt)
        self.assertEqual(self.store.objects[ordering.KEY]['raw'],raw);self.assertIsNone(self.mirror.current)
    def test_archive_lost_ack_resolves_only_from_complete_bytes(self):
        self.legacy();attempt=self.attempt()
        def lost(kw):
            if kw['Key'].startswith(ordering.ARCHIVE):raise RuntimeError('INVENTED_PRIVATE')
        self.store.after_put=lost;self.assertTrue(self.publish(attempt)['published'])
    def test_archive_failure_with_no_bytes_prevents_current_write(self):
        self.legacy();attempt=self.attempt()
        def fail(kw):
            if kw['Key'].startswith(ordering.ARCHIVE):raise Fault('AccessDenied')
        self.store.before_put=fail
        with self.assertRaisesRegex(ordering.OrderingUnavailable,'not retained'):self.publish(attempt)
        self.assertIsNone(self.mirror.current)
    def test_current_lost_ack_is_resolved_by_exact_body_and_token(self):
        attempt=self.attempt();self.store.after_put=lambda kw:(_ for _ in ()).throw(RuntimeError('lost ack'))
        self.assertTrue(self.publish(attempt)['published']);self.assertEqual(self.store.version,1)
    def test_unconfirmed_s3_failure_never_publishes_mirror(self):
        attempt=self.attempt();self.store.before_put=lambda kw:(_ for _ in ()).throw(Fault('AccessDenied'))
        with self.assertRaisesRegex(ordering.OrderingUnavailable,'unconfirmed'):self.publish(attempt)
        self.assertIsNone(self.mirror.current);self.assertEqual(self.store.objects,{})
    def test_mirror_failure_leaves_exact_recoverable_outbox_for_next_original_attempt(self):
        attempt=self.attempt();raw=self.candidate(attempt)[0]
        def fail(method,_):
            if method=='PUT':raise wire.SnapshotPublicationUnavailable('unavailable')
        self.mirror.failure=fail;result=self.publish(attempt)
        self.assertEqual(result['status'],'s3_committed_mirror_unconfirmed');self.assertFalse(result['published']);self.assertEqual(self.store.objects[ordering.KEY]['raw'],raw)
        self.mirror.failure=None;next_attempt=self.attempt()
        self.assertEqual(self.mirror.current['raw'],raw);self.assertEqual(next_attempt['prior_mirror_recovery'],'acknowledged');self.assertEqual(next_attempt['frame']['revision'],2)
        self.assertEqual([row[0] for row in self.mirror.calls],['POST','PUT','PUT','POST'])
    def test_lost_mirror_ack_next_attempt_retries_the_exact_bytes(self):
        attempt=self.attempt();self.mirror.after=lambda *args:(_ for _ in ()).throw(wire.SnapshotPublicationUnavailable('lost'))
        self.assertFalse(self.publish(attempt)['published']);raw=self.mirror.current['raw'];self.mirror.after=None
        self.attempt();self.assertEqual(self.mirror.current['raw'],raw)
    def test_failed_recovery_stops_before_new_reservation(self):
        attempt=self.attempt();self.publish(attempt);before=len(self.mirror.calls)
        self.mirror.failure=lambda *args:(_ for _ in ()).throw(wire.SnapshotPublicationUnavailable('unavailable'))
        with self.assertRaises(wire.SnapshotPublicationUnavailable):self.attempt()
        self.assertEqual([row[0] for row in self.mirror.calls[before:]],['PUT'])
    def test_expired_reservation_is_never_relabelled_as_new_observations(self):
        attempt=self.attempt();self.mirror.failure=lambda method,raw:(_ for _ in ()).throw(wire.SnapshotPublicationUnavailable('unavailable')) if method=='PUT' else None
        self.publish(attempt);raw=self.store.objects[ordering.KEY]['raw'];self.mirror.failure=None;self.mirror.tokens={}
        fresh=self.attempt();self.assertEqual(fresh['prior_mirror_recovery'],'unknown_publication_reservation');self.assertGreater(fresh['frame']['revision'],attempt['frame']['revision']);self.assertEqual(self.store.objects[ordering.KEY]['raw'],raw)
    def test_superseded_after_s3_never_sends_candidate_to_mirror(self):
        first,second=self.attempt(),self.attempt();newraw=self.candidate(second,2)[0]
        def advance(kw):
            if kw['Key']==ordering.KEY:self.store.seed(ordering.KEY,newraw,{ordering.META:second['token']})
        self.store.after_put=advance;self.assertEqual(self.publish(first)['status'],'superseded_after_s3');self.assertIsNone(self.mirror.current)
    def test_superseded_after_mirror_ack_cannot_claim_current_success(self):
        first,second=self.attempt(),self.attempt();raw=self.candidate(second,2)[0]
        self.mirror.after=lambda *args:self.store.seed(ordering.KEY,raw,{ordering.META:second['token']})
        self.assertEqual(self.publish(first)['status'],'superseded_after_mirror')
    def test_four_conflicts_are_bounded_and_do_not_publish(self):
        attempt=self.attempt();self.store.before_put=lambda kw:(_ for _ in ()).throw(Fault('ConditionalRequestConflict'))
        with self.assertRaisesRegex(ordering.OrderingUnavailable,'contention'):self.publish(attempt)
        self.assertEqual(sum(kind=='write' for kind,_ in self.store.calls),4);self.assertIsNone(self.mirror.current)
    def test_policy_missing_or_weakened_refuses_before_current_read_or_reservation(self):
        for mutate in (lambda p:p['Statement'].pop(),lambda p:p['Statement'][-2]['Condition']['Null'].update({'s3:if-none-match':'false'}),lambda p:p['Statement'].append(copy.deepcopy(p['Statement'][1])),lambda p:p['Statement'][0]['Resource'].pop()):
            self.store=Store();mutate(self.store.policy)
            with self.assertRaises(ordering.OrderingUnavailable):self.attempt()
            self.assertFalse(any(k in ('read','write') for k,_ in self.store.calls));self.assertEqual(self.mirror.calls,[])
    def test_policy_rechecked_after_collection_before_any_write(self):
        attempt=self.attempt();self.store.policy['Statement'].pop()
        with self.assertRaises(ordering.OrderingUnavailable):self.publish(attempt)
        self.assertEqual(self.store.objects,{});self.assertIsNone(self.mirror.current)
    def test_lifecycle_disjoint_prefix_is_required_for_all_expiration_rules(self):
        for filt in ({},{'Tag':{'Key':'invented','Value':'x'}},{'Prefix':'portfolio/'},{'And':{'Prefix':ordering.ARCHIVE+'2026/','Tags':[]}}):
            self.store.rules=[{'Status':'Enabled','Filter':filt,'Expiration':{'Days':1}}]
            with self.assertRaisesRegex(ordering.OrderingUnavailable,'lifecycle expiry'):self.attempt()
        self.store.rules=[{'Status':'Enabled','Filter':{'Prefix':'data/'},'NoncurrentVersionExpiration':{'NoncurrentDays':1}}]
        self.assertEqual(self.attempt()['frame']['revision'],1)
    def test_disabled_expiry_and_multipart_cleanup_do_not_delete_snapshot(self):
        self.store.rules=[{'Status':'Disabled','Filter':{},'Expiration':{'Days':1}},{'Status':'Enabled','Filter':{},'AbortIncompleteMultipartUpload':{'DaysAfterInitiation':1}}]
        self.assertEqual(self.attempt()['frame']['revision'],1)
    def test_guard_missing_permission_or_suspended_versioning_fails_closed(self):
        for value in ('Suspended',None):
            self.store.get_bucket_versioning=lambda **kw:ok(Status=value)
            with self.assertRaisesRegex(ordering.OrderingUnavailable,'version retention'):self.attempt()
        self.store.get_bucket_policy=lambda **kw:(_ for _ in ()).throw(Fault('AccessDenied'))
        with self.assertRaisesRegex(ordering.OrderingUnavailable,'cannot be verified'):self.attempt()
    def test_unknown_read_failure_is_not_absence(self):
        for code in ('AccessDenied','404','NoSuchBucket','SlowDown'):
            self.store.get_object=lambda **kw:(_ for _ in ()).throw(Fault(code))
            with self.assertRaisesRegex(ordering.OrderingUnavailable,'read unavailable'):self.attempt()
        self.assertEqual(self.mirror.calls,[])
    def test_current_body_shape_framing_types_and_metadata_are_strict(self):
        for change in (lambda r:r.update(ContentLength=True),lambda r:r.update(ContentLength=0),lambda r:r.update(ContentLength=r['ContentLength']+1),lambda r:r.update(ETag='unquoted'),lambda r:r.update(ContentEncoding='gzip'),lambda r:r.update(Metadata=[])):
            self.legacy();self.store.mutate_read=lambda key,r:change(r)
            with self.assertRaises(ordering.OrderingUnavailable):self.attempt()
            self.assertTrue(self.store.bodies[-1].closed)
    def test_duplicate_unsafe_nonfinite_or_invalid_current_refuses_without_writes(self):
        for raw in (b'{"x":1,"x":2}',b'{"x":NaN}',b'{"x":1e999}',b'{"x":9007199254740992}',b'{"x":"\\ud800"}',b'\xef\xbb\xbf{}',b'[]',b'\xff'):
            self.store.seed(ordering.KEY,raw)
            with self.assertRaises(ordering.OrderingUnavailable):self.attempt()
        self.assertEqual(self.mirror.calls,[])
    def test_missing_outbox_token_or_invalid_ordered_frame_refuses_recovery(self):
        for change in (lambda d:d['publication'].update(revision=True),lambda d:d['publication'].update(started_at='2026-02-30T00:00:00Z'),lambda d:d['publication'].update(schema_version='legacy'),lambda d:None):
            attempt={'frame':{'schema_version':ordering.PROTOCOL,'revision':1,'started_at':STAMP},'token':TOKEN}
            doc=json.loads(self.candidate(attempt)[0]);change(doc);self.store.seed(ordering.KEY,wire.encode_snapshot(doc))
            with self.assertRaises(ordering.OrderingUnavailable):self.attempt()
        self.assertEqual(self.mirror.calls,[])
    def test_whole_large_unabridged_candidate_survives_both_stores_and_recovery(self):
        attempt=self.attempt();doc=json.loads(self.candidate(attempt)[0]);doc['complete_invented_source']=[{'index':i,'text':'π😀 full source '+str(i)*8} for i in range(24000)]
        raw=wire.encode_snapshot(doc);self.assertGreater(len(raw),2_000_000)
        self.assertTrue(ordering.finish_snapshot_publication(self.store,self.validate,attempt,raw,self.validate(doc))['published'])
        self.assertEqual(self.mirror.current['raw'],raw);self.attempt();self.assertEqual(self.mirror.current['raw'],raw)
    def test_invalid_reservation_ack_does_not_receive_a_new_acquisition_rank(self):
        for extra in ({'ok':1},{'revision':True},{'revision':0},{'revision':2**53},{'token':'invented-invalid'},{'protocol':'legacy'}):
            ack={'ok':True,'revision':1,'token':TOKEN,'protocol':ordering.PROTOCOL,**extra}
            with patch.object(ordering,'request',return_value=ack),self.assertRaises(ordering.OrderingUnavailable):self.attempt()
    def test_wrong_ordered_ack_is_partial_not_success(self):
        for extra in ({'ok':1},{'revision':True},{'body_bytes':True},{'identity_bytes':True},{'body_sha256':'0'*64},{'status':'queued'},{'protocol':wire.PROTOCOL}):
            self.store=Store();self.mirror=Mirror(self.validate)
            with patch.object(ordering,'request',self.mirror):attempt=self.attempt()
            raw,identity=self.candidate(attempt)
            ack={'ok':True,'protocol':ordering.PROTOCOL,'revision':attempt['frame']['revision'],'body_bytes':len(raw),'identity_bytes':identity,'body_sha256':hashlib.sha256(raw).hexdigest(),'status':'published',**extra}
            with patch.object(ordering,'request',return_value=ack):result=ordering.finish_snapshot_publication(self.store,self.validate,attempt,raw,identity)
            self.assertEqual(result['status'],'s3_committed_mirror_unconfirmed');self.assertFalse(result['published'])
    def actual_handler(self,event=None):
        mod=self.mod;mod.begin_snapshot_publication=ordering.begin_snapshot_publication;mod.finish_snapshot_publication=ordering.finish_snapshot_publication;mod.publication_s3=self.store
        self.acquisition=[]
        def record(name,value):
            self.acquisition.append(name);self.assertTrue(self.mirror.calls and self.mirror.calls[-1][0]=='POST');return value
        mod.load_s3_json=lambda key,default:record('research',{})
        mod.query_pk=lambda key:record('book',[{'symbol':'INVENTED','qty':1,'cost_basis_per_share':2}] if key=='POSITION' else [])
        mod.batch_fetch_prices=lambda *args,**kw:record('prices',{})
        mod.sync_auto_watchlist=lambda *args:record('sync',{'added_S':[],'added_A':[],'removed_S':[],'removed_A':[]})
        with contextlib.redirect_stdout(io.StringIO()):return mod.lambda_handler({} if event is None else event,None)
    def test_current_handler_reserves_before_all_acquisition_and_reports_acknowledged_bytes(self):
        response=self.actual_handler();self.assertEqual(response['statusCode'],200);self.assertTrue(self.acquisition)
        raw=self.store.objects[ordering.KEY]['raw'];self.assertEqual(self.mirror.current['raw'],raw)
        doc=json.loads(raw);self.assertEqual(len(doc['accounting']['source_positions']),1);self.assertFalse(doc['capital_book']['allows_new_entries']);self.assertEqual(doc['publication']['revision'],1)
    def test_current_handler_uninstalled_guard_performs_no_collection_or_sync(self):
        self.store.policy['Statement'].pop()
        with self.assertRaises(ordering.OrderingUnavailable):self.actual_handler()
        self.assertEqual(self.acquisition,[]);self.assertEqual(self.mirror.calls,[]);self.assertEqual(self.store.objects,{})
    def test_current_handler_partial_delivery_raises_actual_async_error_with_fixed_state(self):
        self.mirror.failure=lambda method,raw:(_ for _ in ()).throw(wire.SnapshotPublicationUnavailable('INVENTED_PRIVATE')) if method=='PUT' else None
        with self.assertRaises(wire.SnapshotPublicationUnavailable) as error:self.actual_handler()
        self.assertIn('s3_committed_mirror_unconfirmed',str(error.exception));self.assertNotIn('INVENTED_PRIVATE',str(error.exception))
        self.assertIn(ordering.KEY,self.store.objects);self.assertIsNone(self.mirror.current)


class Transport(unittest.TestCase):
    def setUp(self):
        self.requests=[];self.auth=[]
        p=patch.object(wire,'service_headers',lambda:self.auth.append(True) or {'X-JH-Service-Token':'invented-only-470'});p.start();self.addCleanup(p.stop)
        p=patch.dict(wire.os.environ,{'PRIVATE_ARTIFACT_PROXY':wire.ORIGIN});p.start();self.addCleanup(p.stop)
        p=patch.object(socket.socket,'connect',side_effect=AssertionError('External networking forbidden'));p.start();self.addCleanup(p.stop)
    def send(self,method='POST',raw=b'{}',token=None,answer=None,status=200,url=None,headers=None,context=None):
        from test_byte_publication import Response
        response=Response(json.dumps({'ok':True} if answer is None else answer).encode(),headers,status,
                          url or (ordering.RESERVE_URL if method=='POST' else wire.URL));self.response=response
        def open_(req,**kwargs):self.requests.append((req,kwargs));return response
        with patch.object(ordering.urllib.request,'build_opener',return_value=types.SimpleNamespace(open=open_)):
            return ordering.request(method,raw,token,context)
    def test_reservation_exact_origin_ack_and_bounded_deadline(self):
        self.assertEqual(self.send(),{'ok':True});req,kw=self.requests[0]
        self.assertEqual(req.full_url,ordering.RESERVE_URL);self.assertEqual(req.method,'POST');self.assertEqual(req.data,b'{}')
        self.assertEqual(req.get_header('Accept-encoding'),'identity');self.assertGreater(kw['timeout'],0);self.assertLessEqual(kw['timeout'],8);self.assertTrue(self.response.closed)
    def test_ordered_write_carries_exact_bytes_token_and_digest_only_to_reviewed_origin(self):
        raw=b'{"complete":"invented"}';self.send('PUT',raw,TOKEN);req,_=self.requests[0]
        self.assertIs(req.data,raw);self.assertEqual(req.full_url,wire.URL);self.assertEqual(req.get_header('X-jh-publication-token'),TOKEN)
        self.assertEqual(req.get_header('X-jh-body-sha256'),hashlib.sha256(raw).hexdigest());self.assertTrue(self.response.closed)
    def test_only_validated_known_conflicts_are_classified(self):
        for code in ('unknown_publication_reservation','superseded_publication'):
            with self.assertRaises(ordering.MirrorConflict) as exc:self.send('PUT',token=TOKEN,status=409,answer={'ok':False,'error':code})
            self.assertEqual(exc.exception.code,code);self.assertTrue(self.response.closed)
        for answer in ({'ok':True,'error':'superseded_publication'},{'ok':False,'error':'INVENTED_PRIVATE'}):
            with self.assertRaises(ordering.OrderingUnavailable) as exc:self.send('PUT',token=TOKEN,status=409,answer=answer)
            self.assertNotIn('INVENTED_PRIVATE',str(exc.exception));self.assertTrue(self.response.closed)
    def test_reservation_response_cannot_change_url_status_or_framing(self):
        for args in ({'url':wire.URL},{'status':201},{'headers':[('Content-Type','application/json'),('Content-Length','1')]},{'headers':[('Content-Type','text/html')]}):
            with self.assertRaises(wire.SnapshotPublicationUnavailable):self.send(**args)
            self.assertTrue(self.response.closed)
    def test_invalid_write_proof_or_runtime_stops_before_credentials(self):
        for token in (None,'invalid',True):
            with self.assertRaises(ordering.OrderingUnavailable):ordering.request('PUT',b'{}',token)
        with self.assertRaises(wire.SnapshotPublicationUnavailable):self.send(context=types.SimpleNamespace(get_remaining_time_in_millis=lambda:5000))
        self.assertEqual(self.auth,[]);self.assertEqual(self.requests,[])
    def test_http_errors_are_closed_without_echoing_response_or_service_details(self):
        import urllib.error
        from test_byte_publication import Response
        response=Response(b'INVENTED_PRIVATE');error=urllib.error.HTTPError(ordering.RESERVE_URL,503,'private',response.headers,response)
        def open_(*a,**kw):raise error
        with patch.object(ordering.urllib.request,'build_opener',return_value=types.SimpleNamespace(open=open_)):
            with self.assertRaises(wire.SnapshotPublicationUnavailable) as exc:ordering.request('POST',b'{}')
        self.assertTrue(response.closed);self.assertNotIn('INVENTED_PRIVATE',str(exc.exception))


if __name__=='__main__':unittest.main()
