"""Bind descriptive cohorts to complete retained issuer packets and reviewed code.

Downstream replay verifies packet binding and cohort arithmetic. The separately
accepted upstream compiler retains issuer originals; this does not pretend to
rerun every issuer workbook inside the 256 MB Bond Desk Lambda.
"""
from pathlib import Path
import hashlib,json,re
import bond_flow as model,verify_bond_flow as independent
from bond_flow_qualification import QUALIFIED,UPSTREAM

ROOT=Path(__file__).parent
PREFIX='data/bond-desk-research/flows/'
SOURCE='data/etf-true-flows.json'
MAX=16*1024*1024
encode=model.encoded
sha=lambda raw:hashlib.sha256(raw).hexdigest()


def strict(raw):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate source key')
            result[key]=value
        return result
    def invalid(value):raise ValueError('Nonfinite JSON value')
    doc=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid);encode(doc)
    return doc


def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if not 0<len(raw)<=MAX:raise ValueError('Whole bounded artifact required')
    return raw


def reader(client,bucket):
    def read(key):
        own=re.escape(PREFIX)+r'(?:inputs|outputs|proofs|runs|compilers)/[a-f0-9]{64}\.(?:json|py)'
        upstream=r'data/etf-research/(?:runs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py)'
        if key!=SOURCE and (not isinstance(key,str) or not re.fullmatch('(?:'+own+'|'+upstream+')',key)):raise ValueError('Unapproved cohort artifact')
        return bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    return read


def paths():return {n:ROOT/n for n in ('bond_flow.py','verify_bond_flow.py','bond_flow_qualification.py','bond_flow_store.py','lambda_function.py')}


def qualified():
    for name,digest in QUALIFIED.items():
        if sha((ROOT/name).read_bytes())!=digest:raise ValueError('Accepted cohort arithmetic changed')


def upstream_binding(raw,read):
    packet=strict(raw);ref=packet['replay'];key=ref['manifest_key']
    if not re.fullmatch(r'data/etf-research/runs/[a-f0-9]{64}\.json',key):raise ValueError('Exact upstream run required')
    manifest_raw=read(key);manifest=strict(manifest_raw)
    if key!='data/etf-research/runs/'+sha(manifest_raw)+'.json' or manifest['contract']!='etf-original-replay.v1':raise ValueError('Upstream manifest differs')
    if set(manifest['compilers'])!=set(UPSTREAM):raise ValueError('Complete reviewed upstream compiler required')
    for name,digest in UPSTREAM.items():
        c=manifest['compilers'][name]
        if c!={'key':'data/etf-research/compilers/'+digest+'.py','sha256':digest} or sha(read(c['key']))!=digest:raise ValueError('Reviewed upstream compiler differs')
    output=manifest['output'];out_raw=read(output['key'])
    if output['key']!='data/etf-research/outputs/'+output['sha256']+'.json' or len(out_raw)!=output['bytes'] or sha(out_raw)!=output['sha256']:raise ValueError('Whole upstream output differs')
    if ref['output_sha256']!=output['sha256'] or manifest['output_sha256']!=output['sha256'] or manifest['generated_at']!=packet['generated_at']:raise ValueError('Upstream publication differs')
    if encode(strict(out_raw))!=encode({k:v for k,v in packet.items() if k!='replay'}):raise ValueError('Source head differs from retained output')
    return {'manifest_key':key,'output_sha256':output['sha256'],'reviewed_compilers':UPSTREAM,
        'whole_packet_bound':True,'issuer_originals_replayed_here':False,
        'scope':'Reviewed upstream original-research publisher and exact retained output; cohort arithmetic is replayed here.'}


def retain(client,bucket,raw,category,ext='json'):
    if category not in ('inputs','outputs','proofs','runs','compilers') or ext not in ('json','py') or not 0<len(raw)<=MAX:raise ValueError('Artifact retention bound')
    ref={'key':PREFIX+category+'/'+sha(raw)+'.'+ext,'sha256':sha(raw),'bytes':len(raw)}
    try:client.put_object(Bucket=bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='text/plain' if ext=='py' else 'application/json',CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if reader(client,bucket)(ref['key'])!=raw:raise ValueError('Retained cohort bytes differ')
    return ref


def checked(ref,read,category,ext='json'):
    if not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256'))):raise ValueError('Exact reference required')
    if ref.get('key')!=PREFIX+category+'/'+ref['sha256']+'.'+ext or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX:raise ValueError('Whole reference differs')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Referenced artifact differs')
    return raw


def compile_raw(raw,at,read):
    qualified();binding=upstream_binding(raw,read)
    output=model.compile_cohorts(raw,at);proof=independent.verify(raw,output,model.BUCKETS,at)
    output['upstream_binding']=binding
    output['arithmetic_checks']=proof
    return output,proof


def replay(packet,read):
    ref=packet.get('replay',{});key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Exact cohort run required')
    raw=read(key);manifest=strict(raw)
    if key!=PREFIX+'runs/'+sha(raw)+'.json' or manifest['contract']!='bond-flow-replay.v1':raise ValueError('Cohort manifest differs')
    own=paths()
    if set(manifest['compilers'])!=set(own):raise ValueError('Complete cohort compiler required')
    for name,path in own.items():
        if checked(manifest['compilers'][name],read,'compilers','py')!=path.read_bytes():raise ValueError('Use matching cohort compiler checkout')
    output,proof=compile_raw(checked(manifest['input'],read,'inputs'),manifest['evaluated_at'],read)
    out_raw=checked(manifest['output'],read,'outputs');proof_raw=checked(manifest['proof'],read,'proofs')
    if encode(output)!=out_raw or encode(proof)!=proof_raw or encode({k:v for k,v in packet.items() if k!='replay'})!=out_raw or ref['output_sha256']!=sha(out_raw):raise ValueError('Cohort reconstruction differs')
    return output


def collect(client,bucket,at):
    read=reader(client,bucket);raw=read(SOURCE);output,proof=compile_raw(raw,at,read)
    put=lambda value,category:retain(client,bucket,encode(value),category)
    manifest={'contract':'bond-flow-replay.v1','evaluated_at':at,'input':retain(client,bucket,raw,'inputs'),
        'output':put(output,'outputs'),'proof':put(proof,'proofs'),
        'compilers':{name:retain(client,bucket,path.read_bytes(),'compilers','py') for name,path in paths().items()}}
    ref=put(manifest,'runs');packet={**output,'replay':{'manifest_key':ref['key'],'output_sha256':manifest['output']['sha256']}}
    replay(packet,read)
    return packet


def compatibility(packet):
    """Preserve field names; never put a partial subtotal into a total field."""
    result={}
    for name,tickers in model.BUCKETS.items():
        row=packet['cohorts'][name]['windows']['5_observations'] if packet else None
        result[name]={'flow_5d_usd':float(row['complete_cohort_decimal']) if row and row['complete_cohort_decimal'] is not None else None,
            'flow_21d_usd':None,'avg_z90':None,'n':row['included_count'] if row else 0,'configured_n':len(tickers),
            'status':row['status'] if row else 'unavailable','coverage_subtotal_decimal':row['coverage_subtotal_decimal'] if row else None,
            'unit':'USD','whole_market_total':False,'calls_eligible':False}
    return result
