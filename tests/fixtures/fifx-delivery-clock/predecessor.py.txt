"""Pure qualification of a completely replayed normal FI/FX original archive.

This never reads a current packet, invokes a producer or requests a provider.
The caller supplies the complete read-only archive proof and current compilers.
"""
from datetime import datetime,timezone,timedelta
import re,urllib.parse

FRED=('VIXCLS','DGS10','DEXUSEU','DEXJPUS','DEXUSUK','DTWEXBGS')
COMPILER_NAMES=frozenset(('canonical_fred_replay','evidence_store','fifx_acquire','fifx_candidate','fifx_catalog','fifx_fred','fifx_model','fifx_originals','fifx_qualification','fifx_store','fifx_timezones','report_observations','research_brief_model','verify_fifx_arithmetic'))

def clock(value):
    if type(value) is not str:raise ValueError('Typed original publication clock required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Zoned original publication clock required')
    return result.astimezone(timezone.utc)

def api_identity(sid,receipt,population,run_clock,lower):
    try:
        if type(receipt) is not dict or type(population) is not dict:return False
        url=receipt.get('source_url')
        if type(url) is not str or len(url)>2048:return False
        p=urllib.parse.urlsplit(url)
        if p.scheme!='https' or p.netloc!='api.stlouisfed.org' or p.path!='/fred/series/observations' or p.fragment or p.username or p.password or p.port:return False
        pairs=urllib.parse.parse_qsl(p.query,strict_parsing=True,keep_blank_values=True);query=dict(pairs)
        if len(query)!=len(pairs):return False
        as_of=query.get('realtime_start');day=datetime.strptime(as_of,'%Y-%m-%d').date()
        if day.isoformat()!=as_of or as_of<'1988-01-01':return False
        expected={'series_id':sid,'file_type':'json','realtime_start':as_of,'realtime_end':as_of,'observation_start':'1988-01-01','observation_end':as_of,'units':'lin','sort_order':'asc','limit':'50000','offset':'0','output_type':'1'}
        if query!=expected:return False
        acquired=clock(receipt['acquired_at'])
        if not lower<=acquired or not run_clock<=acquired<=run_clock+timedelta(seconds=900) or not 0<=(acquired.date()-day).days<=1:return False
        controls={'requested_start':'1988-01-01','requested_end':as_of,'realtime_start':as_of,'realtime_end':as_of,'offset':0,'limit':50000,'complete_requested_window':True,'full_series_history':False,'point_in_time_backtest':False}
        if any(type(population.get(k)) is not type(v) or population[k]!=v for k,v in controls.items()):return False
        if type(receipt.get('bytes')) is not int or not 0<receipt['bytes']<=8*1024*1024:return False
        return type(receipt.get('sha256')) is str and re.fullmatch('[a-f0-9]{64}',receipt['sha256']) is not None
    except (ValueError,TypeError,KeyError,OverflowError):return False


def qualify(archive,expected_compilers,cutoff):
    lower=clock(cutoff)
    if type(expected_compilers) is not dict or set(expected_compilers)!=COMPILER_NAMES or any(type(v) is not str or not re.fullmatch('[a-f0-9]{64}',v) for v in expected_compilers.values()):
        raise ValueError('All fourteen exact reviewed compiler hashes required')
    result={'status':'pending_new_normal_originals','api_originals_verified':False,'current_fred_sources':None,
            'current_pointer_delivery_verified':False,'schedule_causation_verified':False,'point_in_time_qualified':False,'investment_authority':False}
    if archive.get('replayed') is not True:return result
    if archive.get('status')!='complete_original_archive_replayed':raise ValueError('Complete independent archive replay required')
    selected=[row['manifest'] for row in archive['manifests'] if row['key']==archive['selected_manifest']]
    if len(selected)!=1:raise ValueError('One exact selected original run required')
    manifest=selected[0]
    if clock(manifest['generated_at'])!=clock(archive['generated_at']):raise ValueError('Selected run clock differs')
    if clock(archive['generated_at'])<lower:return result
    actual={name:ref['sha256'] for name,ref in manifest['compilers'].items()}
    if actual!=expected_compilers:
        result['status']='selected_originals_not_from_exact_new_compilers';return result
    rows={}
    for sid in FRED:
        source=archive['source_recovery'][sid];identity=source.get('source_identity') or {};population=identity.get('population') or {}
        receipt=source.get('receipt') or {};proof=archive['complete_arithmetic_proofs'][sid]
        count=population.get('returned_rows')
        complete=(identity.get('identity_reviewed') is True and identity.get('status')=='reviewed_source_identity'
          and type(receipt.get('http_status')) is int and receipt['http_status']==200
          and api_identity(sid,receipt,population,clock(archive['generated_at']),lower)
          and population.get('complete_requested_window') is True and type(count) is int and 0<count<=50000
          and type(population.get('reported_rows')) is int and population['reported_rows']==count
          and type(source.get('retained_original_rows')) is int and source['retained_original_rows']==count
          and type(proof.get('original_rows')) is int and proof['original_rows']==count
          and proof.get('source_id')==sid and type(source.get('retained_history_rows')) is int
          and type(proof.get('history_rows')) is int and 0<=proof['history_rows']==source['retained_history_rows']<=count
          and type(proof.get('current_available')) is bool and type(source.get('current_available_at_run')) is bool
          and proof['current_available']==source['current_available_at_run'])
        rows[sid]={'complete_api_original_verified':complete,'current_available_at_run':complete and source.get('current_available_at_run') is True,
                   'returned_rows':count,'quality':source.get('quality'),'original_receipt':receipt if complete else None,'population':population if complete else None}
    result.update(status='post_release_originals_replayed',generated_at=archive['generated_at'],selected_manifest=archive['selected_manifest'],
                  api_originals_verified=all(row['complete_api_original_verified'] for row in rows.values()),
                  current_fred_sources=sum(row['current_available_at_run'] for row in rows.values()),fred_sources=rows)
    return result
