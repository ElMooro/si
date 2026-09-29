"""Unpublished TIC Calls ancestry from complete public Treasury/FRED originals."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import zipfile

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-capital-inflows/source'),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
import tic_store as store
import verify_tic_arithmetic as arithmetic

CONTRACT='calls-tic-lineage-candidate.v1'
encoded=store.model.encoded
digest=store.model.digest
IMMUTABLE=(r'data/tic-research/(?:runs|inputs|outputs|histories)/[a-f0-9]{64}\.json',
           r'data/tic-research/compilers/[a-f0-9]{64}\.py',
           r'data/evidence/tic/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz')
MAX_TOTAL=256*1024*1024
MAX_ARTIFACTS=128


def strict(raw, source=False):
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def bad(_):raise ValueError('Nonfinite JSON value')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=bad,parse_float=str if source else float)


class ImmutableReader:
    def __init__(self,read):self.read=read;self.cache={};self.bytes=0
    def __call__(self,key):
        if not isinstance(key,str) or not any(re.fullmatch(p,key) for p in IMMUTABLE):
            raise ValueError('Reviewed immutable public TIC artifact required before transport')
        if key in self.cache:return self.cache[key]
        if len(self.cache)>=MAX_ARTIFACTS:raise ValueError('Complete archive count exceeds bound')
        raw=self.read(key)
        if not isinstance(raw,bytes) or not 0<len(raw)<=store.MAX_BYTES:raise ValueError('Complete bounded original required')
        if self.bytes+len(raw)>MAX_TOTAL:raise ValueError('Complete archive bytes exceed bound')
        sha=re.search(r'/([a-f0-9]{64})\.(?:json|py|bin\.gz)$',key)[1]
        if hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('Immutable content hash differs')
        self.cache[key]=raw;self.bytes+=len(raw);return raw


def inspect(raw_packet,read,as_of):
    if not isinstance(raw_packet,bytes) or not 0<len(raw_packet)<=store.MAX_BYTES:raise ValueError('Complete bounded public packet required')
    packet=strict(raw_packet);now=store.native.clock(as_of)
    if not isinstance(packet,dict) or packet.get('contract')!=store.model.CONTRACT:raise ValueError('Reviewed native TIC packet required')
    ref=packet.get('replay')
    if (not isinstance(ref,dict) or set(ref)!={'manifest_key','output_sha256'} or
        not isinstance(ref['manifest_key'],str) or not re.fullmatch(r'data/tic-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']) or
        not isinstance(ref['output_sha256'],str) or not re.fullmatch('[a-f0-9]{64}',ref['output_sha256'])):
        raise ValueError('Exact immutable TIC run reference required')
    retained=ImmutableReader(read);manifest=strict(retained(ref['manifest_key']))
    if manifest.get('output_sha256')!=ref['output_sha256']:raise ValueError('Referenced TIC output differs')
    output=store.replay(manifest,retained)
    if encoded(output)!=encoded({k:v for k,v in packet.items() if k!='replay'}):raise ValueError('Whole supplied packet differs from retained replay')
    generated=store.native.clock(output['generated_at'])
    if now<generated:raise ValueError('Future source packet')
    inputs=strict(retained(manifest['input']['key']));bulk=inputs['originals']['bulk'];receipt=bulk['evidence']
    with zipfile.ZipFile(io.BytesIO(retained(receipt['key']))) as archive:
        members=archive.infolist()
        if len(members)!=1 or members[0].filename!='cslt.json' or not 0<members[0].file_size<=store.native.MAX_DOCUMENT:
            raise ValueError('Complete CSLT member required')
        with archive.open(members[0]) as stream:body=stream.read(store.native.MAX_DOCUMENT+1)
    if len(body)!=output['archive']['member_bytes'] or hashlib.sha256(body).hexdigest()!=output['archive']['member_sha256']:
        raise ValueError('Complete native member differs')
    document=strict(body,source=True)
    histories={key:strict(raw) for key,raw in retained.cache.items() if key.startswith('data/tic-research/histories/')}
    proof=arithmetic.verify(output,document,histories)
    core,_=arithmetic.source_series(document,generated.date());asof=output['data_asof']
    rolling=histories[output['rolling_history']['key']]['rows'][-1]
    selected={};windows={};measurements={m['role']:m for m in output['measurements'].values()}
    for role,source in core.items():
        rows=source['rows'];window=rolling['totals'][role];coordinates={}
        dates=set(window['months'])
        if role=='total':dates.update(output['exact_headline']['prior_nonoverlapping_twelve_months']['months'])
        for period in sorted(dates):
            row=rows.get(period)
            if row is None:coordinates[period]=None;continue
            definition={'provider':'US_TREASURY:TIC:CSLT','native_series_id':source['source_id'],
                        'observation_month':period,'unit':'usd_million','measurement':'monthly_net_securities_transactions'}
            period_id='tic-period-'+digest(definition)
            original=document['series'][source['series_index']]['observations'][row['original_row']]
            occurrence={**definition,'period_id':period_id,'archive_sha256':receipt['sha256'],
                        'member_sha256':output['archive']['member_sha256'],'original_series_index':source['series_index'],
                        'original_row':row['original_row'],'reported_native_value':original[1]}
            identity='tic-occurrence-'+digest(occurrence);selected[identity]=occurrence;coordinates[period]=identity
        calculation={'role':role,'series_id':measurements[role]['id'],'window_end':asof,'months':list(window['months']),
                     'original_observations':{period:coordinates[period] for period in window['months']},
                     'total_usd_million_decimal':window['usd_million_decimal'],'total_usd_bn_decimal':window['usd_bn_decimal'],
                     'missing_months':list(window['missing_months'])}
        windows[role]={'calculation_id':'tic-window-'+digest(calculation),**calculation}
        if role=='total':
            prior=output['exact_headline']['prior_nonoverlapping_twelve_months']
            previous={'role':'total_prior_nonoverlapping_12m','series_id':measurements[role]['id'],
                      'months':list(prior['months']),'original_observations':{period:coordinates[period] for period in prior['months']},
                      'total_usd_million_decimal':prior['usd_million_decimal'],'total_usd_bn_decimal':prior['usd_bn_decimal'],
                      'missing_months':list(prior['missing_months'])}
            windows['total_prior_nonoverlapping_12m']={'calculation_id':'tic-window-'+digest(previous),**previous}
    issues=[]
    if now-generated>timedelta(hours=26):issues.append('native_packet:publication_age')
    if output['quality']['status']!='fresh':issues.append('native_packet:not_fresh')
    for role,m in measurements.items():
        quality=store.native.quality(m['as_of'],m['original']['acquired_at'],output['release_calendar'],as_of,
                                    m['value_decimal'] is None,output['cross_source_checks'][m['id']]['status']=='source_disagreement')
        if quality['status']!='fresh':issues.append(role+':'+quality['status'])
    for name,stamp in output['source_clocks'].items():
        age=now-store.native.clock(stamp)
        if age<timedelta(0):raise ValueError('Future original acquisition')
        if age>timedelta(hours=26):issues.append(name+':acquisition_age')
    approved={'bulk','series_release','release_dates'}|{sid+':'+part for sid in store.native.SERIES for part in ('definition','observations')}
    sources={name:{'url':item['url'],'acquired_at':item['acquired_at'],
                   'evidence':{key:item['evidence'][key] for key in ('contract','captured','provider','source_url','key','sha256','bytes','first_received_at')}}
             for name,item in inputs['originals'].items() if name in approved}
    return {'contract':CONTRACT,'candidate_only':True,'as_of':now.isoformat(),'source_generated_at':output['generated_at'],
            'packet_sha256':hashlib.sha256(raw_packet).hexdigest(),'source_replay':dict(ref),'original_sources':sources,
            'observations':selected,'windows':windows,'reported_headline':deepcopy(output['headline']),
            'reported_holder_splits':deepcopy(output['holder_splits']['lt_total']),
            'net_cross_border_definition':{'into_us':windows['total']['calculation_id'],'us_purchases_abroad':windows['us_abroad']['calculation_id'],
                                           'operation':'into_us_minus_us_purchases_abroad','unit':'usd_bn','value':output['headline']['net_cross_border_lt_12mo_b']},
            'current_use':{'eligible':not issues,'issues':sorted(set(issues))},
            'current_research':deepcopy(output['headline']) if not issues else None,
            'independent_arithmetic':proof,'coverage':{'archive_series_retained':len(document['series']),'core_series':9,
                'original_core_rows':proof['native_rows'],'retained_artifacts':len(retained.cache),'retained_uncompressed_bytes':retained.bytes,
                'selected_original_occurrences':len(selected),'complete_history_shards':len(histories)},
            'retained_artifact_inventory':[{'key':key,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)} for key,raw in sorted(retained.cache.items())],
            'complete_source_output_key':manifest['output']['key'],'independent_evidence_count':None,
            'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'publication_eligible':False,
            'limitations':['The nine core series share the CSLT source. FRED republishes the same series and does not add an independent economic observation.',
                           'The complete Treasury archive is retained. Other series remain unqualified; no claim covers them merely because their bytes were retained.',
                           'Amounts are reported and estimated securities transactions, not valuation changes, investable cash or a forecast of future asset buying.',
                           'Release dates are nominal calendar evidence, not verified initial availability. Current retrieved vintages do not establish historical information sets.',
                           'Independent arithmetic covers every core row/window/decomposition. FRED parity and release calendar semantics are separately replayed by the native compiler.',
                           'This candidate is not connected to native Calls and has no allocation authority.']}
