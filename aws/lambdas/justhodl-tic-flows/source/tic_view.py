"""Verified holdings view of the canonical foreign-flow snapshot; no second vote."""
from datetime import date,datetime,timezone
from decimal import Decimal
import hashlib,json,re
from pathlib import Path

PREFIX='data/tic-view/'
FOREIGN='data/foreign-research/'
CURRENT='data/tic-flows.json'
CONTRACT='tic-holdings-view.v1'
PRIVATE='audit-private/20260909-originals/tic-view/'
MAX_BYTES=4*1024*1024

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(doc):return hashlib.sha256(encoded(doc)).hexdigest()
def clock(stamp):
    value=datetime.fromisoformat(stamp.replace('Z','+00:00'))
    if value.tzinfo is None:raise ValueError('timezone required')
    return value.astimezone(timezone.utc)
def month_before(day,n):
    d=date.fromisoformat(day);year,month=divmod(d.year*12+d.month-1-n,12)
    return f'{year:04d}-{month+1:02d}-01'
def dec(value):return Decimal(value) if value is not None else None
def bn(value):return float(value/1000) if value is not None else None
def load(ref,read,prefix,suffix='.json'):
    sha=ref['sha256'];key=ref['key']
    if not re.fullmatch('[a-f0-9]{64}',sha) or key!=prefix+sha+suffix:raise ValueError('retained artifact identity')
    raw=read(key)
    if len(raw)>MAX_BYTES or hashlib.sha256(raw).hexdigest()!=sha or 'bytes' in ref and ref['bytes']!=len(raw):raise ValueError('retained artifact bytes')
    return json.loads(raw)

def build(inputs,read,at):
    if inputs.get('contract')!='tic-view-inputs.v1':raise ValueError('view input contract')
    manifest=load(inputs['foreign_manifest'],read,FOREIGN+'runs/')
    if manifest.get('contract')!='foreign-original-replay.v1':raise ValueError('foreign replay contract')
    source=load(manifest['output'],read,FOREIGN+'outputs/')
    if source.get('contract')!='foreign-original-research.v1' or digest(source)!=manifest['output_sha256']:raise ValueError('foreign output contract')
    if source['generated_at']!=manifest['generated_at'] or clock(source['generated_at'])>clock(at):raise ValueError('foreign output clock')
    asof=source['latest_month'];groups=source['groups'];individual={};history_refs={}
    aliases={alias:row['code'] for alias,row in source['country_lt_treasury'].items() if not row.get('composite')}
    def bundle(code):
        group=groups[code];doc=load(group['history'],read,FOREIGN+'histories/')
        if doc.get('contract')!='foreign-history.v1' or doc.get('kind')!='reporting_group' or doc.get('code')!=code or doc.get('name')!=group['name']:raise ValueError('group identity differs')
        history_refs[code]=group['history'];return doc
    def summary(doc):
        series=doc['series']['treas:pos'];mapping={r['date']:r for r in series['rows']}
        if len(mapping)!=len(series['rows']) or series['unit']!='usd_million' or series['family']!='treas' or series['measure']!='pos':raise ValueError('holdings definition differs')
        current=dec(mapping.get(asof,{}).get('value_decimal'));prior=dec(mapping.get(month_before(asof,12),{}).get('value_decimal'))
        prior3=dec(mapping.get(month_before(asof,3),{}).get('value_decimal'));delta=current-prior if current is not None and prior is not None else None
        return {'current_b':bn(current),'yoy_change_b':bn(delta),'3mo_change_b':bn(current-prior3) if current is not None and prior3 is not None else None,
            'yoy_pct':float(delta/prior*100) if delta is not None and prior is not None and prior>0 else None,
            'status':'REPORTED_HOLDINGS' if current is not None else 'MISSING','as_of':asof,'series_id':series['id'],
            'source_rows':{str(n):mapping.get(month_before(asof,n),{}).get('row_index') for n in (0,3,12)},
            'unit':'usd_bn','scope':'Long-term and short-term Treasury holdings; reported custody/residence, not ultimate ownership.',
            'change_is_transaction':False}
    for alias,code in aliases.items():
        if groups[code]['scope']!='reported_country_or_territory':continue
        doc=bundle(code);individual[alias]={'country':doc['name'],'code':code,**summary(doc),'history':history_refs[code]}
    grand=bundle('99996');total=summary(grand);series=grand['series']['treas:net']
    mapping={r['date']:r for r in series['rows']}
    if len(mapping)!=len(series['rows']) or series['unit']!='usd_million' or series['family']!='treas' or series['measure']!='net':raise ValueError('transaction definition differs')
    def rolling(n):
        values=[dec(mapping.get(month_before(asof,i),{}).get('value_decimal')) for i in range(n)]
        return sum(values,Decimal(0)) if all(v is not None for v in values) else None
    one,three,twelve=(rolling(n) for n in (1,3,12))
    net={'latest_month_m':float(one) if one is not None else None,'trailing_3mo_m':float(three) if three is not None else None,
        'trailing_12mo_m':float(twelve) if twelve is not None else None,'latest_date':asof,
        '12mo_avg_monthly_m':float(twelve/12) if twelve is not None else None,'unit':'usd_million','series_id':series['id'],
        'definition':'Grand Total foreign net transactions in long-term and short-term Treasuries; includes international/regional organizations.',
        'exact_usd_million_decimal':{str(n):str(v) if v is not None else None for n,v in ((1,one),(3,three),(12,twelve))}}
    quality=dict(source['quality']);age=(clock(at)-clock(source['generated_at'])).total_seconds()/3600
    quality.update(source_generated_at=source['generated_at'],view_generated_at=at,source_packet_age_hours=age)
    quality['observation_age_days']=(clock(at).date()-date.fromisoformat(quality['observation_period_end'])).days
    if quality['observation_age_days']>quality['max_observation_age_days']:quality['status']='stale'
    acquisitions=[quality.get('acquired_at'),quality.get('calendar_acquired_at')]
    if age>26 or any(stamp and (clock(at)-clock(stamp)).total_seconds()>26*3600 for stamp in acquisitions):quality['status']='stale_source'
    if quality.get('calendar_expires_at') and clock(at)>=clock(quality['calendar_expires_at']):quality['status']='release_due_unverified'
    if any(v['current_b'] is None for v in individual.values()) or total['current_b'] is None:quality['status']='partial'
    return {'contract':CONTRACT,'version':'2.0.0','schema_version':'2.0','method':'verified_foreign_snapshot_view',
        'generated_at':at,'observation_date':asof,'source_generated_at':source['generated_at'],'quality':quality,
        'composite_tic_stress':None,'regime':'MONITOR_ONLY','call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'independent_evidence_root':'US_TREASURY:TIC:CSLT','additional_independent_votes':0,'top_reasons':[],
        'top_holders':sorted(individual.values(),key=lambda r:(r['current_b'] is None,-(r['current_b'] or 0),r['country'])),
        'individual':individual,'n_holders':len(individual),'holder_coverage':'Selected named jurisdictions; this is not an exhaustive global ranking.',
        'total_foreign_holdings':total,'net_purchases':net,'holder_splits':source['holder_splits'],
        'foreign_manifest':inputs['foreign_manifest'],'foreign_output':manifest['output'],'source_histories':history_refs,
        'holdings_table':source['holdings_table'],'legacy_preservation':inputs['legacy'],
        'interpretation':'Reported holdings and transactions are distinct. Holdings changes include valuation and other adjustments. Custody does not identify ultimate owners; these measurements do not establish de-dollarization, auction demand or future returns.'}

def bounded(body):
    try:raw=body.read(MAX_BYTES+1)
    finally:body.close()
    if len(raw)>MAX_BYTES:raise ValueError('view artifact bound')
    return raw
def error_code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def reader(client,bucket):
    def read(key):
        if not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('public path required')
        return bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    return read
def retain(client,bucket,key,raw,kind='application/json'):
    if not key.startswith((PREFIX,PRIVATE)):raise ValueError('view retention prefix')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if error_code(exc) not in ('412','409','PreconditionFailed','ConditionalRequestConflict'):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('retained view bytes differ')
def preserve(client,bucket,read):
    key=PREFIX+'migration.json'
    try:marker=json.loads(read(key))
    except Exception as exc:
        if error_code(exc) not in ('404','NoSuchKey'):raise
        raw=read(CURRENT);old=json.loads(raw)
        if old.get('contract')==CONTRACT:raise ValueError('prior view retention missing')
        sha=hashlib.sha256(raw).hexdigest();retain(client,bucket,PRIVATE+sha+'.bin',raw,'application/octet-stream')
        marker={'contract':'tic-view-legacy.v1','sha256':sha,'bytes':len(raw),'protected_backup':True,'legacy_generated_at':old.get('generated_at')}
        retain(client,bucket,key,encoded(marker))
    if marker.get('contract')!='tic-view-legacy.v1' or not re.fullmatch('[a-f0-9]{64}',marker.get('sha256','')):raise ValueError('legacy marker')
    return marker
def publish(client,bucket,packet):
    for _ in range(5):
        obj=client.get_object(Bucket=bucket,Key=CURRENT);old=json.loads(bounded(obj['Body']))
        if old.get('contract')==CONTRACT:
            if clock(old['generated_at'])>clock(packet['generated_at']) or clock(old['source_generated_at'])>clock(packet['source_generated_at']):return False
            if old['generated_at']==packet['generated_at'] and old!=packet:raise ValueError('same-clock view conflict')
        try:
            client.put_object(Bucket=bucket,Key=CURRENT,Body=encoded(packet),ContentType='application/json',CacheControl='no-store',IfMatch=obj['ETag']);return True
        except Exception as exc:
            if error_code(exc) not in ('412','409','PreconditionFailed','ConditionalRequestConflict'):raise
    raise RuntimeError('view publication contention')
def compiler():
    raw=Path(__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest()
    return raw,{'key':PREFIX+'compilers/'+sha+'.py','sha256':sha,'bytes':len(raw)}
def replay(manifest,read):
    raw,ref=compiler()
    if manifest.get('contract')!='tic-view-replay.v1' or manifest['compiler']!=ref or read(ref['key'])!=raw:raise ValueError('view reviewed compiler differs')
    inputs=load(manifest['input'],read,PREFIX+'inputs/');output=build(inputs,read,manifest['generated_at'])
    if output!=load(manifest['output'],read,PREFIX+'outputs/'):raise ValueError('view output replay differs')
    return output
def run(client,bucket,at=None):
    at=at or datetime.now(timezone.utc).isoformat();read=reader(client,bucket);legacy=preserve(client,bucket,read)
    source=json.loads(read('data/foreign-flows.json'));key=source['replay']['manifest_key']
    if not re.fullmatch(re.escape(FOREIGN)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('foreign run path')
    inputs={'contract':'tic-view-inputs.v1','foreign_manifest':{'key':key,'sha256':key.rsplit('/',1)[-1][:-5]},'legacy':legacy}
    output=build(inputs,read,at)
    # The current source must be the exact immutable output, not merely point at it.
    ref=output['foreign_output']
    if {k:v for k,v in source.items() if k!='replay'}!=load(ref,read,FOREIGN+'outputs/') or source['replay']['output_sha256']!=ref['sha256']:raise ValueError('current foreign packet differs')
    refs={}
    for name,doc in (('input',inputs),('output',output)):
        body=encoded(doc);sha=digest(doc);key=PREFIX+name+'s/'+sha+'.json';retain(client,bucket,key,body);refs[name]={'key':key,'sha256':sha,'bytes':len(body)}
    raw,ref=compiler();retain(client,bucket,ref['key'],raw,'text/x-python')
    manifest={'contract':'tic-view-replay.v1','generated_at':at,'compiler':ref,**refs};key=PREFIX+'runs/'+digest(manifest)+'.json';retain(client,bucket,key,encoded(manifest))
    if replay(manifest,read)!=output:raise ValueError('pre-publication view replay')
    proof={'manifest_key':key,'output_sha256':refs['output']['sha256']};published=publish(client,bucket,{**output,'replay':proof})
    return {'published':published,'replay':proof,'quality':output['quality'],'n_holders':output['n_holders'],
        'signals_emitted':0,'notifications_sent':0,'paid_ai_calls':0,'private_account_reads':0,'portfolio_writes':0}
