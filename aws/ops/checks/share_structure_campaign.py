"""Complete population plan and disjoint, resumable source batches.

Each batch retains whole responses. A partial batch never represents a complete
population and cannot publish an engine head. Successful originals are reusable;
failed/ambiguous requests require explicit review, never an automatic retry.
"""
from pathlib import Path
import json,time
import share_structure_sources as source
import financial_statement_source, financial_statement_campaign, market_runtime_evidence
from financial_statement_campaign import collect
from market_runtime_evidence import bounded

CHUNK=2000
CONTRACT='capital-structure-source-plan.v1'


def plan(labels,baseline,probe,accounting,clock):
    specs=source.specifications(labels)
    if len(specs)>35000:raise ValueError('Reviewed population bound exceeded')
    return {'contract':CONTRACT,'generated_at':clock(),'reported_symbols':sorted(source.population(labels)),
        'baseline':baseline,'probe':probe,'accounting':accounting,'planned_sources':len(specs),
        'requests_per_batch':CHUNK,'batches':(len(specs)+CHUNK-1)//CHUNK,
        'source_module_sha256':source.sha(Path(source.__file__).read_bytes()),
        'campaign_module_sha256':source.sha(Path(__file__).read_bytes()),
        'dependency_module_sha256':{module.__name__:source.sha(Path(module.__file__).read_bytes())
            for module in (financial_statement_source,financial_statement_campaign,market_runtime_evidence)},
        'max_request_starts_per_second':2.5,'workers':3,'snapshot_atomic':False,
        'historical_availability_verified':False,'population_qualified':False,
        'forecast_qualified':False,'sizing_qualified':False}


def specifications(document,part):
    expected=plan(document['reported_symbols'],document['baseline'],document['probe'],document['accounting'],lambda:document['generated_at'])
    if document!=expected:raise ValueError('Exact original source plan and collector bytes required')
    if type(part) is not int or not 1<=part<=document['batches']:raise ValueError('Reviewed source-batch index required')
    return source.specifications(document['reported_symbols'])[(part-1)*CHUNK:part*CHUNK]


def read_journal(client,key):
    return json.loads(bounded(client.get_object(Bucket=source.BUCKET,Key=key)['Body'],source.MAX))


def run_batch(client,request_id,plan_ref,part,credential,clock,adoption,remaining_seconds=2700,transport=None):
    document=json.loads(source.read(client,plan_ref));specs=specifications(document,part)
    key=source.request_key(request_id,'batch:'+str(part))
    try:previous=read_journal(client,key)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
        previous=None
    if previous:
        if previous.get('status')!='complete':raise ValueError('Source batch already attempted; review and adopt retained work explicitly')
        manifest=json.loads(source.read(client,previous['manifest']))
        if manifest.get('plan')!=plan_ref or manifest.get('part')!=part:raise ValueError('Completed source-batch identity differs')
        verify_batch(client,document,manifest)
        return previous['manifest'],False
    if remaining_seconds<1200:raise ValueError('Complete bounded source-batch budget required')
    deadline=time.monotonic()+remaining_seconds-120
    progress={'contract':'capital-structure-source-batch.v1','request_id':request_id,'part':part,'plan':plan_ref,
        'generated_at':clock(),'planned_sources':len(specs),'status':'claimed','captures':{}}
    source.journal(client,key,progress,True)
    try:
        allowed=frozenset(document['reported_symbols']);rate=source.Rate()
        def fetch(spec):
            if time.monotonic()>=deadline:raise TimeoutError('Source budget reached; no new request')
            prior=adoption(spec)
            if prior is not None:
                capsule,body,origin=prior
                return source.adopt(client,request_id,spec,allowed,capsule,body,origin,clock)
            return source.capture(client,request_id,spec,allowed,credential,rate,clock,transport)
        def checkpoint(complete,errors):
            progress.update(status='source_failure' if errors else 'capturing',
                captures={url:value['retained_capture'] for url,value in complete.items()},source_errors=errors)
            source.journal(client,key,progress)
            print(json.dumps({'source_batch':part,'completed':len(complete),'planned':len(specs),'errors':len(errors)}),flush=True)
        results=collect(specs,{},fetch,checkpoint,workers=3)
        counts={'complete_sources':len(results),'provider_requests':sum(not v.get('reused_original',False) for v in results.values()),
            'reused_sources':sum(bool(v.get('reused_original',False)) for v in results.values()),
            'provider_rows':sum(v['inventory']['rows'] for v in results.values()),
            'provider_bytes':sum(v['original']['bytes'] for v in results.values()),
            'empty_arrays':sum(v['inventory']['rows']==0 for v in results.values())}
        result={**progress,'status':'complete','completed_at':clock(),'counts':counts,
            'population_qualified':False,'forecast_qualified':False,'sizing_qualified':False}
        verify_batch(client,document,result)
        ref=source.retain(client,source.encode(result))
        source.journal(client,key,{**result,'manifest':ref})
        return ref,True
    except Exception as exc:
        source.journal(client,key,{**progress,'status':'failed','error_type':type(exc).__name__});raise


def verify_batch(client,document,manifest):
    expected=specifications(document,manifest['part'])
    if manifest.get('status')!='complete' or set(manifest['captures'])!={v['url'] for v in expected}:
        raise ValueError('Every original source for this disjoint batch is required')
    counts={'complete_sources':0,'provider_requests':0,'reused_sources':0,'provider_rows':0,'provider_bytes':0,'empty_arrays':0}
    for request in expected:
        capsule=json.loads(source.read(client,manifest['captures'][request['url']]))
        if capsule['spec']!=request or capsule['http_status']!=200:raise ValueError('Source request or response differs')
        raw=source.read(client,capsule['original']);inventory=source.inspect(raw,request)
        source.response_integrity(raw,capsule['headers'])
        if inventory!=capsule['inventory']:raise ValueError('Reparsed source inventory differs')
        counts['complete_sources']+=1;counts['provider_rows']+=inventory['rows'];counts['provider_bytes']+=len(raw)
        counts['empty_arrays']+=int(inventory['rows']==0)
        counts['reused_sources' if capsule.get('reused_original') else 'provider_requests']+=1
    if counts!=manifest['counts']:raise ValueError('Whole source-batch counts differ')
    return counts


def complete_population(client,plan_ref,batch_refs):
    document=json.loads(source.read(client,plan_ref))
    if len(batch_refs)!=document['batches']:raise ValueError('Every disjoint source batch is required')
    seen=set();captures={};counts={'complete_sources':0,'provider_requests':0,'reused_sources':0,'provider_rows':0,'provider_bytes':0,'empty_arrays':0}
    for ref in batch_refs:
        batch=json.loads(source.read(client,ref));part=batch['part']
        if part in seen or batch['plan']!=plan_ref:raise ValueError('Overlapping or unrelated source batches')
        seen.add(part);checked=verify_batch(client,document,batch)
        if set(captures)&set(batch['captures']):raise ValueError('Source-request overlap')
        captures.update(batch['captures'])
        for key in counts:counts[key]+=checked[key]
    if seen!=set(range(1,document['batches']+1)) or set(captures)!={v['url'] for v in source.specifications(document['reported_symbols'])}:
        raise ValueError('Whole retained population differs from its plan')
    return {'contract':'capital-structure-complete-sources.v1','plan':plan_ref,'status':'complete',
        'reported_symbols':document['reported_symbols'],'captures':captures,'counts':counts,
        'batch_manifests':batch_refs,'snapshot_atomic':False,'all_original_responses_reparsed':True,
        'identity_qualified':False,'statement_duration_verified':False,'share_split_comparability_verified':False,
        'forecast_qualified':False,'sizing_qualified':False}
