"""Bind originals one series at a time, preserve legacy bytes and replay before publication."""
from datetime import datetime, timezone
import gzip, hashlib, io, json, re
from pathlib import Path
import canonical_macro_sources
import fr2004_research_context
import report_observations
import research_brief_model
import reversal_research as model
import reversal_research_catalog

MAX_BYTES=32*1024*1024
PRIVATE='audit-private/20260909-originals/reversal-research/'
COMPILERS=(model,reversal_research_catalog,fr2004_research_context,canonical_macro_sources,
           report_observations,research_brief_model)


def code(exc): return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc): return code(exc) in ('404','NoSuchKey')
def conflict(exc): return code(exc) in ('412','409','PreconditionFailed','ConditionalRequestConflict')
def now(): return datetime.now(timezone.utc).isoformat()


def bounded(body):
    try: raw=body.read(MAX_BYTES+1)
    finally: body.close()
    if len(raw)>MAX_BYTES: raise ValueError('reversal artifact exceeds bound')
    return raw


def raw_reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:
            raise ValueError('registered public evidence path required')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read


def immutable(client,bucket,key,raw,kind='application/json'):
    if not (key.startswith(model.PREFIX) or key.startswith(PRIVATE)): raise ValueError('retention prefix')
    try:
        client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
            CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc): raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw: raise ValueError('immutable bytes differ')


def preserve(client,bucket,read):
    key=model.PREFIX+'migration.json'
    try:
        marker=json.loads(read(key))
        if marker.get('contract')!='reversal-legacy-preservation.v1': raise ValueError('migration contract differs')
    except Exception as exc:
        if not missing(exc): raise
        raw=read(model.CURRENT);packet=json.loads(raw)
        if packet.get('contract')==model.CONTRACT: raise ValueError('migration record missing after publication')
        inventory=model.project_inventory(packet);sha=hashlib.sha256(raw).hexdigest()
        immutable(client,bucket,PRIVATE+sha+'.bin',raw,'application/octet-stream')
        body=model.encoded(inventory);target=model.PREFIX+'inventories/'+model.digest(inventory)+'.json'
        immutable(client,bucket,target,body)
        marker={'contract':'reversal-legacy-preservation.v1','source':model.CURRENT,'sha256':sha,'bytes':len(raw),
            'protected_backup':True,'inventory':{'key':target,'sha256':model.digest(inventory),'bytes':len(body)},
            'inventory_entries':len(inventory['rows'])}
        immutable(client,bucket,key,model.encoded(marker))
    ref=marker['inventory'];body=read(ref['key'])
    if ref['key']!=model.PREFIX+'inventories/'+ref['sha256']+'.json' or len(body)!=ref['bytes'] or hashlib.sha256(body).hexdigest()!=ref['sha256']:
        raise ValueError('retained inventory differs')
    inventory=json.loads(body);model.validate_inventory(inventory)
    if len(inventory['rows'])!=marker['inventory_entries']: raise ValueError('inventory count differs')
    return inventory,marker


class NativeInputs:
    """Validate the common snapshot once; original response graphs are released per row."""
    def __init__(self,source,read):
        canonical_macro_sources.originals(source,read,())
        self.source=source;self.read=read
        self.manifest=json.loads(read(source['replay']['manifest_key']))

    def get(self,sid):
        if sid not in self.source.get('measurements',{}): return None
        entry=self.manifest['inputs'][sid];item={'evidence':entry['evidence'],'acquired_at':entry['acquired_at']}
        if set(item['evidence'])!={'definition','observations'}: raise ValueError('native original pair required')
        for part,receipt in item['evidence'].items():
            raw=self.read(receipt['key'])
            if len(raw)!=receipt['bytes'] or hashlib.sha256(raw).hexdigest()!=receipt['sha256']:
                raise ValueError('native original bytes differ')
            item[part]=json.loads(raw)
        return item


def publish(client,bucket,packet):
    stamp=model.clock(packet['generated_at']);source_stamp=model.clock(packet['source_generated_at'])
    for _ in range(5):
        try:
            obj=client.get_object(Bucket=bucket,Key=model.CURRENT);old=json.loads(bounded(obj['Body']))
            if old.get('contract')==model.CONTRACT:
                previous=model.clock(old['generated_at'])
                if previous>stamp or model.clock(old['source_generated_at'])>source_stamp: return False
                if previous==stamp and old!=packet: raise ValueError('same-clock conflicting research')
            condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',**condition)
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('research publication conflict bound')


def compile_input(inputs,read,stamp):
    context=fr2004_research_context.project(inputs['fails'],model.clock(stamp))
    return model.build(inputs['source'],NativeInputs(inputs['source'],read),inputs['inventory'],stamp,context,inputs['legacy_ref'])


def replay(manifest,read):
    if manifest.get('contract')!='reversal-replay.v1' or set(manifest['compilers'])!={m.__name__ for m in COMPILERS}:
        raise ValueError('reversal replay identity')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();ref=manifest['compilers'][module.__name__]
        if ref!={'key':model.PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=raw:
            raise ValueError('reviewed compiler differs; use matching checkout')
    def load(name):
        ref=manifest[name];raw=read(ref['key'])
        if ref['key']!=model.PREFIX+name+'s/'+ref['sha256']+'.json' or len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:
            raise ValueError('retained '+name+' differs')
        return json.loads(raw)
    output=compile_input(load('input'),read,manifest['generated_at'])
    if output!=load('output') or model.digest(output)!=manifest['output_sha256']: raise ValueError('original reproduction differs')
    return output


def run(client,bucket):
    read=raw_reader(client,bucket);inventory,legacy=preserve(client,bucket,read)
    source=json.loads(read('data/report-measurements.json'))
    try: fails=json.loads(read('data/settlement-fails.json'))
    except Exception: fails={}
    inputs={'source':source,'inventory':inventory,'legacy_ref':legacy,'fails':fails};stamp=now()
    output=compile_input(inputs,read,stamp);refs={}
    for name,value in (('input',inputs),('output',output)):
        raw=model.encoded(value);sha=model.digest(value);key=model.PREFIX+name+'s/'+sha+'.json'
        immutable(client,bucket,key,raw);refs[name]={'key':key,'sha256':sha,'bytes':len(raw)}
    compilers={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,raw,'text/x-python');compilers[module.__name__]={'key':key,'sha256':sha}
    manifest={'contract':'reversal-replay.v1','generated_at':stamp,'compilers':compilers,**refs,'output_sha256':refs['output']['sha256']}
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    if replay(manifest,read)!=output: raise ValueError('pre-publication replay differs')
    ref={'manifest_key':key,'output_sha256':manifest['output_sha256']}
    published=publish(client,bucket,{**output,'replay':ref})
    return {'published':published,'generated_at':stamp,'replay':ref,'quality':output['quality'],
            'inventory_entries':len(inventory['rows']),'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0}
