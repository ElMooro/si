"""Replay a completed accounting campaign before conditional native publication.

The source collector runs on the existing AWS-authorized Actions runner.
This native publisher never calls a provider, another Lambda, an account,
an AI service, an order endpoint or a notification service.
"""
from datetime import datetime, timezone
import gc, re, time
import statement_research_source as source
import statement_research_v2 as model
import statement_research_store_v2 as store

CURRENT = 'data/forensic-screen.json'
READY = model.PRIVATE + 'ready.json'


def now():
    return datetime.now(timezone.utc).isoformat()


def missing(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('404','NoSuchKey')


def conflict(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('409','412','ConditionalRequestConflict','PreconditionFailed')


def request_key(identity):
    if not isinstance(identity,str) or not 1 <= len(identity) <= 200:
        raise ValueError('Bounded durable accounting request identity required')
    return model.PRIVATE + 'requests/' + source.sha(identity.encode()) + '.json'


def raw(client,bucket,key):
    return store.bounded(client.get_object(Bucket=bucket,Key=key)['Body'])


def journal(client,bucket,key,value,claim=False):
    if not re.fullmatch(re.escape(model.PRIVATE)+r'requests/[a-f0-9]{64}\.json',key):
        raise ValueError('Reviewed accounting journal required')
    body=source.encoded(value)
    if len(body)>source.MAX:raise ValueError('Bounded request journal required')
    client.put_object(Bucket=bucket,Key=key,Body=body,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if raw(client,bucket,key)!=body:raise ValueError('Accounting journal readback differs')


def protect(client,bucket,body):
    if not isinstance(body,bytes) or not 0<len(body)<=source.MAX:
        raise ValueError('Whole bounded accounting predecessor required')
    ref={'key':model.PRIVATE+source.sha(body)+'.bin','sha256':source.sha(body),'bytes':len(body)}
    try:
        client.put_object(Bucket=bucket,Key=ref['key'],Body=body,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
    if raw(client,bucket,ref['key'])!=body:raise ValueError('Whole predecessor readback differs')
    return ref


def current_matches(packet,reference):
    if not isinstance(packet,dict) or packet.get('contract')!=model.CONTRACT or packet.get('replay')!=reference:
        return False
    return source.sha(source.encoded({k:v for k,v in packet.items() if k!='replay'}))==reference.get('output_sha256')


def run(client,bucket,request_id,execution_id,remaining_seconds=840,clock=now):
    deadline=time.monotonic()+remaining_seconds-60
    if not isinstance(execution_id,str) or not execution_id:
        raise ValueError('Actual AWS execution identity required')
    status=request_key(request_id)
    try:previous_request=source.strict(raw(client,bucket,status))
    except Exception as exc:
        if not missing(exc):raise
        previous_request=None
    if previous_request:
        if previous_request.get('status')!='complete':raise ValueError('Request already attempted; inspect its retained failure or ambiguous state')
        return previous_request['result']
    progress={'request_id':request_id,'execution_id':execution_id,'status':'claimed','started_at':clock()}
    journal(client,bucket,status,progress,True)
    try:
        response=client.get_object(Bucket=bucket,Key=READY)
        ready_bytes=store.bounded(response['Body']);ready=source.strict(ready_bytes);ready_etag=response['ETag']
        if (ready.get('contract')!='financial-statement-qualified-ready.v2' or ready.get('status')!='qualified'
                or ready.get('qualification',{}).get('all_original_rows_conserved') is not True
                or ready.get('qualification',{}).get('production_measurement_formulas_imported') is not False):
            raise ValueError('Whole independently qualified accounting snapshot required')
        def deadline_check():
            if time.monotonic()>=deadline:raise TimeoutError('Source replay budget exhausted; current packet retained')
        reference=ready['replay'];read=store.reader(client,bucket,before_read=deadline_check)
        recorded=store.verified_run(reference,read)
        packet=store.checked(recorded['output'],'outputs',read)
        age=(source.clock(clock())-source.clock(packet['generated_at'])).total_seconds()
        if not 0<=age<=48*3600:raise ValueError('Ready accounting acquisition is future-dated or older than 48 hours')
        for stamp in (packet['financial_statement_acquisition_started_at'],packet['identity_index']['requested_at']):
            if not 0<=(source.clock(clock())-source.clock(stamp)).total_seconds()<=48*3600:
                raise ValueError('Underlying accounting or SEC identity acquisition is future-dated or older than 48 hours')
        proof=ready['qualification']
        if (proof.get('original_rows_checked')!=packet['provider_rows'] or ready.get('source_manifest_sha256')!=packet['source_manifest_sha256']
                or proof.get('metric_comparisons')!=sum(packet['metric_statuses'].values())
                or proof.get('identity_metadata_rows_checked')!=packet['provider_rows']
                or proof.get('current_sec_pairs_checked') is not True
                or proof.get('current_identity_statuses')!=packet['current_identity_statuses']
                or proof.get('clock_issues')!=packet['clock_issues']
                or proof.get('forecast_qualified') is not False or proof.get('sizing_qualified') is not False):
            raise ValueError('Independent qualification differs from the recorded population')
        head=client.get_object(Bucket=bucket,Key=CURRENT);old=store.bounded(head['Body']);previous=source.strict(old)
        progress.update(replay=reference,ready_sha256=source.sha(ready_bytes))
        if current_matches(previous,reference):
            result={'published':False,'reason':'unchanged_qualified_snapshot','replay':reference}
        else:
            if remaining_seconds<300:raise ValueError('Insufficient source replay budget')
            progress['predecessor']=protect(client,bucket,old);journal(client,bucket,status,progress)
            compiled=store.replay(reference,read)
            if compiled['packet']!=packet:raise ValueError('Native source reconstruction differs from ready output')
            del compiled;gc.collect()
            previous_time=source.clock(previous['generated_at']) if previous.get('generated_at') else None
            stamp=source.clock(packet['generated_at'])
            if previous_time and (stamp<previous_time or (stamp==previous_time and previous.get('replay')!=reference)):
                result={'published':False,'reason':'observation_rollback_or_equal_time_conflict','replay':reference}
            elif client.head_object(Bucket=bucket,Key=READY)['ETag']!=ready_etag:
                result={'published':False,'reason':'new_ready_snapshot_available','replay':reference}
            else:
                if time.monotonic()>=deadline:raise TimeoutError('Publication reserve reached; current packet retained')
                body=source.encoded({**packet,'replay':reference})
                if len(body)>source.MAX:raise ValueError('Whole native accounting packet exceeds bound')
                try:
                    client.put_object(Bucket=bucket,Key=CURRENT,Body=body,ContentType='application/json',CacheControl='no-store',IfMatch=head['ETag'])
                except Exception as exc:
                    if not conflict(exc):raise
                    result={'published':False,'reason':'concurrent_publication','replay':reference}
                else:
                    if raw(client,bucket,CURRENT)!=body:raise ValueError('Accounting publication readback differs')
                    result={'published':True,'generated_at':packet['generated_at'],'replay':reference,
                        'reported_names':packet['reported_names'],'provider_rows':packet['provider_rows']}
        journal(client,bucket,status,{**progress,'status':'complete','finished_at':clock(),'result':result})
        return result
    except Exception as exc:
        journal(client,bucket,status,{**progress,'status':'failed','error_type':type(exc).__name__})
        raise
