"""Complete, reproducible page-source inventory with no model/provider requests.

The earlier model functions remain in the pinned predecessor for inspection.
This entry path publishes descriptive evidence only; it does not inherit their
performance, forecast or sizing claims. All configured inputs remain listed.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, math, re, time

CONTRACT='commentary-research.v1'
POLICY='commentary-sources.json'
PRIVATE='audit-private/20260909-originals/commentary-research/'
LIMIT=64*1024*1024
BUDGET=240
DENY=re.compile(r'(^|[/_.-])(private|secret|credentials?|tokens?|passwords?|users?|brain|journal|portfolio|orders?|accounts?|subscriptions?|auth|learning|validation|calibration|tickets?)([/_.-]|$)',re.I)
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')


class EvidenceError(ValueError):pass


class DeadlineClient:
    """Check the fixed wave deadline before every S3 operation."""
    def __init__(self,client,deadline):self.client,self.deadline=client,deadline
    def check(self):
        if time.monotonic()+35>=self.deadline:raise EvidenceError('Wave acquisition budget exhausted; no partial head')
    def get_object(self,**kwargs):
        self.check();return self.client.get_object(**kwargs)
    def put_object(self,**kwargs):
        self.check();return self.client.put_object(**kwargs)


def strict(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise EvidenceError('Duplicate JSON key')
            out[k]=v
        return out
    def invalid(x):raise EvidenceError('Nonfinite JSON number')
    def number(x):
        n=float(x)
        if not math.isfinite(n):invalid(x)
        return n
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=number,parse_constant=invalid)


def whole(stream,length):
    parts=[];size=0
    try:
        while True:
            raw=stream.read(min(65536,LIMIT+1-size))
            if not raw:break
            if not isinstance(raw,bytes):raise EvidenceError('Byte stream required')
            parts.append(raw);size+=len(raw)
            if size>LIMIT:raise EvidenceError('Complete body exceeds bound')
    finally:stream.close()
    if type(length) is not int or size!=length:raise EvidenceError('Incomplete body')
    return b''.join(parts)


def get(client,bucket,key):
    try:obj=client.get_object(Bucket=bucket,Key=key)
    except Exception as e:
        code=str(getattr(e,'response',{}).get('Error',{}).get('Code'))
        if code in ('NoSuchKey','404','NotFound'):return None
        raise EvidenceError('Object read failed') from None
    raw=whole(obj['Body'],obj.get('ContentLength'))
    etag=obj.get('ETag')
    if not isinstance(etag,str) or not etag:raise EvidenceError('Conditional identity absent')
    return {'raw':raw,'etag':etag}


def retain(client,bucket,raw):
    if not isinstance(raw,bytes) or len(raw)>LIMIT:raise EvidenceError('Bounded whole original required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:client.put_object(Bucket=bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as e:
        if str(getattr(e,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if retained(client,bucket,ref)!=raw:raise EvidenceError('Original readback differs')
    return ref


def retained(client,bucket,ref):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))
        or ref.get('key')!=PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int or not 0<=ref['bytes']<=LIMIT):raise EvidenceError('Protected identity required')
    got=get(client,bucket,ref['key'])
    if got is None or len(got['raw'])!=ref['bytes'] or sha(got['raw'])!=ref['sha256']:raise EvidenceError('Retained body differs')
    return got['raw']


def stamp(value):
    if not isinstance(value,str) or 'T' not in value:raise EvidenceError('Aware publication clock required')
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None:raise EvidenceError('Aware publication clock required')
    return d.astimezone(timezone.utc)


FIELDS={
    'signal-board':('regime_analysis','key_divergences','positioning_view','regime_score'),
    'risk-desk':('primary_risks','hedge_recommendation','leading_indicators','risk_score'),
    'liquidity':('liquidity_flow','implications','policy_outlook','liquidity_score'),
    'fundamentals':('best_value','warning_flags','watch_list','value_score'),
    'crisis':('crisis_assessment','contagion_risks','hedge_actions','crisis_score'),
    '13f':('smart_money_moves','top_buys_sells','divergence_signals','conviction_score'),
}


def policy_read():
    raw=(Path(__file__).parent/POLICY).read_bytes();p=strict(raw)
    if p.get('contract')!='commentary-source-policy.v1' or set(p.get('pages',{}))!=set(FIELDS):raise EvidenceError('Exact public commentary policy required')
    for page,cfg in p['pages'].items():
        if DENY.search(page) or not isinstance(cfg.get('data_files'),list) or len(cfg['data_files'])>200:raise EvidenceError('Invalid declared population')
        if not isinstance(cfg.get('allowed'),list) or len(cfg['allowed'])!=len(set(cfg['allowed'])):raise EvidenceError('Unique allowed keys required')
        for key in cfg['data_files']:
            if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_/-]+\.json',key):raise EvidenceError('Exact source path required')
        for key in cfg['allowed']:
            binding=cfg['bindings'].get(key,{})
            if key not in cfg['data_files'] or DENY.search(key) or binding.get('key')!=key or binding.get('access')!='public' or binding.get('required_projection'):raise EvidenceError('Reviewed public source binding required')
    return raw,p


def compiler_hashes():
    return {name:sha((Path(__file__).parent/name).read_bytes()) for name in ('lambda_function.py','commentary_store.py',POLICY)}


def summarize(key,raw,at):
    out={'key':key,'bytes':len(raw),'sha256':sha(raw),'status':'complete_json','published_at':None,'publication_clock_status':'absent','observation_clock_status':'not_inferred'}
    try:doc=strict(raw)
    except (ValueError,RecursionError):return {**out,'status':'invalid_json'}
    if not isinstance(doc,(list,dict)):return {**out,'status':'unsupported_json_shape'}
    if not doc:return {**out,'status':'empty_json'}
    out.update(json_type='object' if isinstance(doc,dict) else 'array',top_level_members=len(doc))
    if isinstance(doc,dict) and doc.get('generated_at') is not None:
        try:
            out['publication_clock_status']='valid' if stamp(doc['generated_at'])<=stamp(at) else 'future'
            out['published_at']=doc['generated_at']
        except (TypeError,ValueError):out['publication_clock_status']='invalid'
    return out


def build(page,entries,at,compilers):
    n=len(entries);parsed=sum(e['status']=='complete_json' for e in entries)
    excluded=sum(e['status']=='unreviewed_binding_not_read' for e in entries)
    clocks=sum(e.get('publication_clock_status')=='valid' for e in entries)
    details=[]
    for e in entries:
        if e['status']=='complete_json':
            clock='published '+e['published_at'] if e['publication_clock_status']=='valid' else 'publication clock '+e['publication_clock_status']
            details.append(f"{e['key']}: {e['bytes']} complete bytes; {clock}.")
        else:details.append(f"{e['key']}: {e['status']}.")
    text=' '.join(details)
    qualification='These are stored engine reports. Their observation dates, definitions, dependencies and predictive performance require separate validation. Publication time is not observation freshness.'
    action='WAIT means abstain. This inventory does not establish an independently validated trade, hedge, expected return or portfolio size.'
    a,b,c,score=FIELDS[page]
    note={'mode':'research_inventory','headline':f'{parsed} of {n} declared inputs parsed as complete JSON; {excluded} unreviewed bindings were not read.',
          a:text,b:qualification,c:action,score:None,'narrative':text+' '+qualification+' '+action,
          'posture':'WAIT','regime':None,'confidence_score':None,'model_api_calls':0,
          'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'forecast_qualified':False}
    return {'contract':CONTRACT,'version':'1.0.0','page':page,'generated_at':at,
            'model':'deterministic-source-inventory','model_api_calls':0,'commentary':note,
            'preserved_from':None,'llm_attempt_failed':False,'source_inventory':entries,
            'source_coverage':{'declared':n,'complete_json':parsed,'unreviewed_not_read':excluded,'valid_publication_clocks':clocks,'complete_for_declared_inputs':parsed==n and n>0},
            'compiler_sha256':compilers,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'forecast_qualified':False,'portfolio_action':'WAIT'}


def publish(client,bucket,page,policy,policy_ref,at):
    # Only the six reviewed public heads are eligible; no account or old private
    # commentary page is read even to preserve its predecessor.
    if page not in FIELDS or page not in policy['pages']:raise EvidenceError('Unreviewed commentary page; no reads')
    stamp(at);cfg=policy['pages'][page]
    head='data/ai-commentary/'+page+'.json'
    archive='data/ai-commentary/history/'+page+'/'+stamp(at).strftime('%Y-%m-%d')+'.json'
    before=get(client,bucket,head)
    original=retain(client,bucket,before['raw']) if before else None
    if before:
        old=strict(before['raw'])
        if not isinstance(old,dict) or old.get('page')!=page or stamp(old.get('generated_at'))>=stamp(at):raise EvidenceError('Predecessor page/clock invalid')
    prior_archive=get(client,bucket,archive)
    archive_ref=retain(client,bucket,prior_archive['raw']) if prior_archive else None
    entries=[];inputs=[];total=0
    for key in cfg['data_files']:
        if key not in cfg['allowed'] or DENY.search(key):
            entries.append({'key':key,'status':'unreviewed_binding_not_read'});inputs.append(None);continue
        got=get(client,bucket,key)
        if got is None:entries.append({'key':key,'status':'missing'});inputs.append(None);continue
        total+=len(got['raw'])
        if total>128*1024*1024:raise EvidenceError('Whole input population exceeds bound; not truncated')
        inputs.append({'original':retain(client,bucket,got['raw']),'etag':got['etag']})
        entries.append(summarize(key,got['raw'],at))
    compilers=compiler_hashes();out=build(page,entries,at,compilers)
    plan={'contract':CONTRACT,'page':page,'generated_at':at,'policy':policy_ref,'predecessor':original,
          'predecessor_etag':before['etag'] if before else None,'dated_predecessor':archive_ref,'archive_key':archive,
          'inputs':inputs,'compiler_sha256':compilers,'projection':retain(client,bucket,encode(out))}
    ref=retain(client,bucket,encode(plan))
    out['publication_context']={'manifest':ref,'original_vintage_verified':False,'snapshot_atomic':False,
        'dated_history':'prior_daily_file_retained' if prior_archive else 'first_daily_file_created_before_head',
        'replay_scope':'Complete stored inputs and deterministic inventory; source methodology and forecasts unqualified.'}
    raw=encode(out);retain(client,bucket,raw)
    # Never overwrite the old one-file-per-day history. Subsequent publications
    # remain in the complete protected predecessor chain instead.
    if not prior_archive:
        client.put_object(Bucket=bucket,Key=archive,Body=raw,ContentType='application/json',CacheControl='public, max-age=86400',IfNoneMatch='*')
        check=get(client,bucket,archive)
        if check is None or check['raw']!=raw:raise EvidenceError('Daily archive changed; no head replacement')
    client.put_object(Bucket=bucket,Key=head,Body=raw,ContentType='application/json',CacheControl='public, max-age=600',**({'IfMatch':before['etag']} if before else {'IfNoneMatch':'*'}))
    check=get(client,bucket,head)
    if check is None or check['raw']!=raw:raise EvidenceError('Public head changed; no rollback')
    return out


def replay(client,bucket,packet):
    ctx=packet['publication_context'];plan=strict(retained(client,bucket,ctx['manifest']))
    if plan.get('contract')!=CONTRACT or plan.get('compiler_sha256')!=compiler_hashes():raise EvidenceError('Exact compiler required')
    policy=strict(retained(client,bucket,plan['policy']))
    if policy!=policy_read()[1]:raise EvidenceError('Exact source policy required')
    page=plan['page']
    if page not in FIELDS:raise EvidenceError('Public page required')
    cfg=policy['pages'][page]
    if len(cfg['data_files'])!=len(plan['inputs']):raise EvidenceError('Complete input population required')
    entries=[];bodies=0
    for key,item in zip(cfg['data_files'],plan['inputs']):
        if key not in cfg['allowed'] or DENY.search(key):
            if item is not None:raise EvidenceError('Excluded input was read')
            entries.append({'key':key,'status':'unreviewed_binding_not_read'})
        elif item is None:entries.append({'key':key,'status':'missing'})
        else:
            if not isinstance(item.get('etag'),str) or not item['etag']:raise EvidenceError('Original object identity absent')
            raw=retained(client,bucket,item['original']);entries.append(summarize(key,raw,plan['generated_at']));bodies+=1
    expected=build(page,entries,plan['generated_at'],plan['compiler_sha256']);raw=encode(expected)
    if retained(client,bucket,plan['projection'])!=raw or encode({k:v for k,v in packet.items() if k!='publication_context'})!=raw:raise EvidenceError('Complete inventory replay differs')
    for key in ('predecessor','dated_predecessor'):
        if plan[key] is not None:retained(client,bucket,plan[key])
    expected_ctx={'manifest':ctx['manifest'],'original_vintage_verified':False,'snapshot_atomic':False,
        'dated_history':'prior_daily_file_retained' if plan['dated_predecessor'] else 'first_daily_file_created_before_head',
        'replay_scope':'Complete stored inputs and deterministic inventory; source methodology and forecasts unqualified.'}
    if ctx!=expected_ctx:raise EvidenceError('Publication qualification differs')
    return {'status':'matched','whole_input_bodies':bodies,'declared_inputs':len(entries),'projection_sha256':sha(raw),'model_api_calls':0}


def run(client,bucket,event,context):
    event=event or {}
    if not isinstance(event,dict):raise EvidenceError('Event object required')
    if any(k in event for k in ('requestContext','httpMethod','headers','queryStringParameters')):
        return {'statusCode':409,'body':json.dumps({'error':'scheduled_publication_only'})}
    raw,policy=policy_read();page=event.get('page')
    if page is not None and (not isinstance(page,str) or page not in FIELDS):
        return {'statusCode':422,'body':json.dumps({'error':'unreviewed_page_not_read','model_api_calls':0})}
    remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,'get_remaining_time_in_millis') else 300
    client=DeadlineClient(client,time.monotonic()+min(BUDGET,remaining-35))
    ref=retain(client,bucket,raw);at=datetime.now(timezone.utc).isoformat();results=[]
    for page in ([page] if page else list(policy['pages'])):
        try:
            out=publish(client,bucket,page,policy,ref,at)
            results.append({'page':page,'status':'published','complete_inputs':out['source_coverage']['complete_for_declared_inputs']})
        except Exception as e:results.append({'page':page,'status':'not_published','error_type':type(e).__name__})
    published=sum(r['status']=='published' for r in results)
    out={'contract':CONTRACT,'results':results,'pages_published':published,'pages_failed':len(results)-published,
         'excluded_pages':policy['excluded_pages'],'model_api_calls':0}
    print(json.dumps(out))
    return {'statusCode':200 if published==len(results) else 207 if published else 503,'body':json.dumps(out)}
