"""Complete, reproducible page-source inventory with no model/provider requests.

The earlier model functions remain in the pinned predecessor for inspection.
This entry path publishes descriptive evidence only; it does not inherit their
performance, forecast or sizing claims. All configured inputs remain listed.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, math, re, time

CONTRACT='page-explanation-research.v1'
POLICY='page-explanation-sources.json'
MANIFEST='data/page-ai-manifest.json'
CURSOR='data/_cache/page-ai-research-cursor.json'
PRIVATE='audit-private/20260909-originals/page-explanation-research/'
LIMIT=64*1024*1024
WAVE=42
BUDGET=240
DENY=re.compile(r'(^|[/_.-])(private|secret|credentials?|tokens?|passwords?|users?|brain|journal|portfolio|orders?|accounts?|subscriptions?|auth|learning|validation|tickets?)([/_.-]|$)',re.I)
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


def source_key(value):
    if not isinstance(value,str) or not re.fullmatch(r'(?:data/)?[a-zA-Z0-9][a-zA-Z0-9_/-]*(?:\.json)?',value):return None
    key=value if value.startswith('data/') else 'data/'+value
    return key if key.endswith('.json') else key+'.json'


def permitted(page,key,policy):
    return bool(key and not DENY.search(key) and key in policy['pages'].get(page,[])
                and not key.startswith(('data/page-ai/','data/ai-commentary/','data/_cache/')))


def policy_read():
    raw=(Path(__file__).parent/POLICY).read_bytes();p=strict(raw)
    if not isinstance(p,dict) or p.get('contract')!='page-explanation-source-policy.v1' or not isinstance(p.get('pages'),dict):raise EvidenceError('Bundled source policy required')
    for page,keys in p['pages'].items():
        if not re.fullmatch('[a-z0-9][a-z0-9_-]*',page) or DENY.search(page) or not isinstance(keys,list) or len(keys)!=len(set(keys)) or any(source_key(k)!=k or DENY.search(k) for k in keys):raise EvidenceError('Invalid public page policy')
    return raw,p


def compiler_hashes():
    root=Path(__file__).parent
    return {name:sha((root/name).read_bytes()) for name in ('lambda_function.py','page_explanation_store.py',POLICY)}


def source_summary(key,raw,at):
    identity={'key':key,'bytes':len(raw),'sha256':sha(raw),'status':'complete_json','published_at':None,'publication_clock_status':'absent','observation_clock_status':'not_inferred'}
    try:data=strict(raw)
    except (ValueError,RecursionError):return {**identity,'status':'invalid_json'}
    if not isinstance(data,(dict,list)):return {**identity,'status':'unsupported_json_shape'}
    if not data:return {**identity,'status':'empty_json'}
    identity['json_type']='object' if isinstance(data,dict) else 'array' if isinstance(data,list) else 'scalar'
    identity['top_level_members']=len(data) if isinstance(data,(dict,list)) else None
    if isinstance(data,dict):
        value=data.get('generated_at')
        if value is not None:
            try:
                d=stamp(value)
                identity['publication_clock_status']='valid' if d<=stamp(at) else 'future'
                identity['published_at']=value
            except (ValueError,TypeError):identity['publication_clock_status']='invalid'
    return identity


def build(page,meta,entries,at,compilers):
    title=meta.get('title') or page
    if not isinstance(title,str) or len(title)>4096:raise EvidenceError('Valid complete title required')
    n=len(entries);valid=sum(e['status']=='complete_json' for e in entries)
    clocks=sum(e.get('publication_clock_status')=='valid' for e in entries)
    sentences=[]
    for e in entries:
        if e['status']=='complete_json':
            clock=('publication '+e['published_at']) if e['publication_clock_status']=='valid' else 'publication clock '+e['publication_clock_status']
            sentences.append(f"{e['declared_source']}: complete {e['bytes']}-byte JSON; {clock}. Observation age and measurement comparability need their own checks.")
        else:sentences.append(f"{e['declared_source']}: {e['status']}. No data claim is made for this input.")
    return {'contract':CONTRACT,'version':'1.2.0','page':page,'title':title,'generated_at':at,
            'mode':'deterministic_source_inventory','model':None,'model_api_calls':0,
            'what_it_is':f'{title} has {n} declared source entries in the retained page manifest.',
            'what_it_does':'This explanation inventories every declared input, binds whole retained bytes to exact source paths and separates publication clocks from observation dates. It does not infer an investment edge from a source name or a scorecard label.',
            'analysis':f'{valid} of {n} declared entries parsed as complete JSON; {clocks} have valid nonfuture publication clocks. '+(' '.join(sentences) if sentences else 'No source entries were declared; this is an empty inventory, not market evidence.'),
            'pick_read':'No independently qualified single-name setup or portfolio instruction is established by this inventory.',
            'has_data':valid>0,'source_inventory':entries,'source_coverage':{'declared':n,'complete_json':valid,'valid_publication_clocks':clocks,'complete_for_declared_inputs':valid==n and n>0},
            'outlook':{'alpha_status':'UNQUALIFIED','n_graded':None,'confidence':None,'note':'Legacy scorecard name matching is not a performance or cost-adjusted return validation.'},
            'compiler_sha256':compilers,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'forecast_qualified':False,'portfolio_action':'WAIT'}


def publish(client,bucket,page,meta,manifest_ref,policy,policy_ref,at):
    if page not in policy['pages'] or DENY.search(page):raise EvidenceError('Unreviewed page; no reads')
    if not isinstance(meta,dict) or not isinstance(meta.get('data_files',[]),list):raise EvidenceError('Complete source list required')
    declared=meta.get('data_files',[])
    if len(declared)>200:raise EvidenceError('Source population exceeds bound; not truncated')
    if any(not isinstance(d,str) or len(d)>2048 for d in declared):raise EvidenceError('Invalid declared source identity')
    # A legacy explanation of unknown/private inputs may itself contain private
    # text. Refuse that whole page before reading even its old published head.
    if any(not permitted(page,source_key(d),policy) for d in declared):raise EvidenceError('Unreviewed source binding; page predecessor not read')
    head='data/page-ai/'+page+'.json';before=get(client,bucket,head)
    original=retain(client,bucket,before['raw']) if before is not None else None
    if before is not None:
        old=strict(before['raw'])
        if not isinstance(old,dict) or old.get('page')!=page:raise EvidenceError('Predecessor page identity differs')
        if old.get('generated_at') is not None and stamp(old['generated_at'])>=stamp(at):raise EvidenceError('Predecessor is not older')
    entries=[];inputs=[];total=0
    for item in declared:
        key=source_key(item)
        if not permitted(page,key,policy):
            entries.append({'declared_source':item,'key':key,'status':'not_in_public_source_policy'});inputs.append(None);continue
        got=get(client,bucket,key)
        if got is None:
            entries.append({'declared_source':item,'key':key,'status':'missing'});inputs.append(None);continue
        total+=len(got['raw'])
        if total>128*1024*1024:raise EvidenceError('Complete page inputs exceed memory budget; not truncated')
        ref=retain(client,bucket,got['raw']);inputs.append(ref)
        entries.append({'declared_source':item,**source_summary(key,got['raw'],at)})
    compilers=compiler_hashes();out=build(page,meta,entries,at,compilers)
    plan={'contract':CONTRACT,'page':page,'generated_at':at,'manifest':manifest_ref,'policy':policy_ref,'predecessor':original,'predecessor_etag':before['etag'] if before else None,'inputs':inputs,'compiler_sha256':compilers,'projection':retain(client,bucket,encode(out)),'capture_atomic':False}
    ref=retain(client,bucket,encode(plan))
    out['publication_context']={'manifest':ref,'original_vintage_verified':False,'snapshot_atomic':False,'replay_scope':'Complete stored inputs and deterministic explanation; no original-provider or performance qualification.'}
    raw=encode(out);retain(client,bucket,raw)
    client.put_object(Bucket=bucket,Key=head,Body=raw,ContentType='application/json',CacheControl='public, max-age=900',**({'IfMatch':before['etag']} if before else {'IfNoneMatch':'*'}))
    check=get(client,bucket,head)
    if check is None or check['raw']!=raw:raise EvidenceError('Published head changed or differs; no rollback')
    return out


def replay(client,bucket,packet):
    plan=strict(retained(client,bucket,packet['publication_context']['manifest']))
    if plan.get('contract')!=CONTRACT or plan.get('compiler_sha256')!=compiler_hashes():raise EvidenceError('Exact reviewed compiler required')
    manifest=strict(retained(client,bucket,plan['manifest']));policy=strict(retained(client,bucket,plan['policy']))
    if encode(policy)!=encode(policy_read()[1]):raise EvidenceError('Retained source policy differs')
    page=plan['page'];meta=manifest[page];declared=meta.get('data_files',[])
    if page not in policy['pages'] or len(declared)!=len(plan['inputs']):raise EvidenceError('Complete page population required')
    entries=[];bodies=0
    for item,ref in zip(declared,plan['inputs']):
        key=source_key(item)
        if not permitted(page,key,policy):
            if ref is not None:raise EvidenceError('Excluded source was read')
            entries.append({'declared_source':item,'key':key,'status':'not_in_public_source_policy'})
        elif ref is None:entries.append({'declared_source':item,'key':key,'status':'missing'})
        else:
            raw=retained(client,bucket,ref);bodies+=1
            entries.append({'declared_source':item,**source_summary(key,raw,plan['generated_at'])})
    expected=build(page,meta,entries,plan['generated_at'],plan['compiler_sha256'])
    raw=encode(expected)
    if retained(client,bucket,plan['projection'])!=raw or encode({k:v for k,v in packet.items() if k!='publication_context'})!=raw:raise EvidenceError('Complete explanation replay differs')
    if plan.get('predecessor') is not None:retained(client,bucket,plan['predecessor'])
    expected_context={'manifest':packet['publication_context']['manifest'],'original_vintage_verified':False,'snapshot_atomic':False,'replay_scope':'Complete stored inputs and deterministic explanation; no original-provider or performance qualification.'}
    if packet['publication_context']!=expected_context:raise EvidenceError('Publication qualification differs')
    return {'status':'matched','declared_entries':len(entries),'whole_input_bodies':bodies,'projection_sha256':sha(raw),'provider_requests':0,'model_requests':0}


def run(client,bucket,event,context):
    event=event or {}
    if not isinstance(event,dict):raise EvidenceError('Event object required')
    # Function-URL callers cannot turn source inventory into a generation API.
    if any(k in event for k in ('requestContext','httpMethod','headers','queryStringParameters')):
        method=(event.get('requestContext') or {}).get('http',{}).get('method') or event.get('httpMethod')
        return {'statusCode':204 if method=='OPTIONS' else 409,'headers':{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'},'body':json.dumps({'error':'scheduled_publication_only','note':'Read the existing same-origin page explanation. This endpoint does not generate on click.'})}
    start=time.monotonic();at=datetime.now(timezone.utc).isoformat()
    remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,'get_remaining_time_in_millis') else 300
    underlying=client
    client=DeadlineClient(client,min(start+BUDGET,start+remaining-35))
    policy_raw,policy=policy_read();policy_ref=retain(client,bucket,policy_raw)
    got=get(client,bucket,MANIFEST)
    if got is None:raise EvidenceError('Page manifest absent')
    manifest_ref=retain(client,bucket,got['raw']);manifest=strict(got['raw'])
    if not isinstance(manifest,dict) or not manifest:raise EvidenceError('Complete nonempty manifest required')
    pages=list(manifest)
    if any(not re.fullmatch('[a-z0-9][a-z0-9_-]*',p) for p in pages):raise EvidenceError('Exact root page identities required')
    before=get(client,bucket,CURSOR);cur=0
    if before:
        retain(client,bucket,before['raw']);state=strict(before['raw'])
        if not isinstance(state,dict) or type(state.get('i')) is not int or state['i']<0:raise EvidenceError('Invalid research cursor')
        if state.get('manifest_sha256')==manifest_ref['sha256']:cur=state['i']%len(pages)
    results=[]
    for k in range(min(WAVE,len(pages))):
        remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,'get_remaining_time_in_millis') else 300
        if time.monotonic()+35>=client.deadline or remaining<70:break
        page=pages[(cur+k)%len(pages)]
        if page not in policy['pages'] or DENY.search(page):results.append({'page':page,'status':'unreviewed_page_not_read'});continue
        try:
            out=publish(client,bucket,page,manifest[page],manifest_ref,policy,policy_ref,at)
            results.append({'page':page,'status':'published','complete_inputs':out['source_coverage']['complete_for_declared_inputs']})
        except Exception as e:results.append({'page':page,'status':'not_published','error_type':type(e).__name__})
    if not results:raise EvidenceError('No page attempts completed')
    new={'i':(cur+len(results))%len(pages),'manifest_sha256':manifest_ref['sha256'],'updated':at,'results':results}
    # Leave the original cursor unchanged if the reserved closing budget is
    # unavailable. Published heads already have complete retained plans.
    if context and hasattr(context,'get_remaining_time_in_millis') and context.get_remaining_time_in_millis()<35000:raise EvidenceError('Cursor closing budget unavailable')
    raw=encode(new);retain(underlying,bucket,raw)
    underlying.put_object(Bucket=bucket,Key=CURSOR,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfMatch':before['etag']} if before else {'IfNoneMatch':'*'}))
    published=sum(r['status']=='published' for r in results)
    summary={'contract':CONTRACT,'pages_attempted':len(results),'pages_published':published,'pages_unreviewed':sum(r['status']=='unreviewed_page_not_read' for r in results),'pages_failed':sum(r['status']=='not_published' for r in results),'cursor':new['i'],'model_api_calls':0}
    print(json.dumps(summary))
    return {'statusCode':200 if published==len(results) else 207 if published else 503,'body':json.dumps({**summary,'results':results})}
