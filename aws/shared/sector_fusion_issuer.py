"""Reconstruct selected issuer histories; a public headline is never the evidence.

The parser is an exact reviewed copy of the ETF producer's pure source parser.
Compiler pins bind the upstream generation; downloaded source is not executed.
Unselected funds remain outside this deliberately bounded replay.
"""
from datetime import timedelta
from decimal import Decimal
import hashlib,json,re
import sector_issuer_native as native
from sector_fusion_pins import ETF_COMPILERS
from sector_research_catalog import SECTORS
PREFIX='data/etf-research/'


def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def artifact(key):
    return isinstance(key,str) and bool(re.fullmatch(r'data/(?:etf-research/(?:runs|inputs|outputs|compilers|histories)/[a-f0-9]{64}\.(?:json|py)|evidence/etf_original/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz)',key))


def checked(ref,kind,read):
    digest=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PREFIX+kind+'/'+digest+'.json':raise ValueError('Issuer artifact identity differs')
    raw=read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw)!=ref['bytes'] or sha(raw)!=digest:raise ValueError('Issuer artifact bytes differ')
    return json.loads(raw)


def restore(packet,read):
    if packet.get('contract')!='etf-original-research.v1' or packet.get('calls_eligible') is not False or packet.get('sizing_eligible') is not False:raise ValueError('Native issuer packet required')
    ref=packet['replay'];key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Issuer run path differs')
    raw=read(key);run=json.loads(raw)
    if key!=PREFIX+'runs/'+sha(raw)+'.json' or run.get('contract')!='etf-original-replay.v1':raise ValueError('Issuer run identity differs')
    if set(run['compilers'])!=set(ETF_COMPILERS):raise ValueError('Issuer compiler inventory differs')
    for name,digest in ETF_COMPILERS.items():
        expected={'key':PREFIX+'compilers/'+digest+'.py','sha256':digest}
        if run['compilers'][name]!=expected or sha(read(expected['key']))!=digest:raise ValueError('Reviewed issuer compiler differs')
    inputs=checked(run['input'],'inputs',read);output=checked(run['output'],'outputs',read)
    if output!={k:v for k,v in packet.items() if k!='replay'} or sha(encoded(output))!=ref['output_sha256'] or ref['output_sha256']!=run['output_sha256']:raise ValueError('Issuer public body differs')
    at=run['generated_at']
    if inputs.get('contract')!='etf-original-inputs.v1' or packet['generated_at']!=at:raise ValueError('Issuer source contract or clock differs')
    originals=inputs['originals'];symbols=tuple(SECTORS)
    sc=native.ssga_catalog(native.original(originals['ssga_catalog'],read,at,native.SSGA_URL),symbols)
    ic=native.ishares_catalog(native.original(originals['ishares_catalog'],read,at,native.ISHARES_URL),('IVV',))
    if set(sc)!=set(symbols) or set(ic)!={'IVV'}:raise ValueError('Declared sector issuer identities differ')
    ivv=native.ishares_history(native.original(originals['ishares_IVV'],read,at,native.DOWNLOAD.format(pid=ic['IVV']['portfolio_id'])),{**ic['IVV'],'currency':'USD'},at)
    dates=sorted({row['date'] for row in ivv['rows']});calendar=checked(packet['reference_calendar'],'histories',read)
    if calendar.get('rows')!=[{'date':d} for d in dates] or calendar.get('source')!=originals['ishares_IVV']:raise ValueError('Issuer reference calendar differs')
    end=packet['aggregation_period']['end_date'];result={}
    for ticker in symbols:
        row=packet['by_etf'][ticker];identity=sc[ticker]
        if not row.get('history'):
            result[ticker]={'status':'reported_unavailable','source_status':row.get('source_status'),'identity':identity,'windows':{},'history':[]}
            continue
        source=originals['ssga_'+ticker]
        doc=native.ssga_history(native.original(source,read,at,identity['history_url']),identity,at)
        history=native.flow_history(doc['rows'],dates,[]);retained=checked(row['history'],'histories',read)
        if (retained.get('contract')!='etf-native-history.v1' or retained.get('ticker')!=ticker or retained.get('identity')!=identity
            or retained.get('source')!=source or retained.get('rows')!=history or retained.get('corporate_actions')!=[]):raise ValueError('Original issuer history reconstruction differs')
        aligned=[x for x in history if x['date']<=end];windows={str(n)+'d':native.window(aligned,dates,n) for n in (1,5,20)}
        if windows!=row['flow_windows']:raise ValueError('Original issuer window reconstruction differs')
        observation=next((v for v in history if v['date']==identity['nav_date']),None)
        residual=native.dec(observation['nav_decimal'])-native.dec(identity['nav_decimal']) if observation and observation['nav_decimal'] is not None else None
        comparison={'status':'within_published_precision' if residual is not None and abs(residual)<=Decimal('0.01') else 'conflict_or_missing',
            'date':identity['nav_date'],'catalog_nav_decimal':identity['nav_decimal'],'native_nav_decimal':observation['nav_decimal'] if observation else None,
            'residual_decimal':native.ds(residual),'display_precision_allowance_decimal':'0.01',
            'independent_confirmation':False,'reason':'Catalogue and workbook come from the same issuer.'}
        if comparison!=row['source_comparison']:raise ValueError('Issuer catalogue reconciliation differs')
        due=min(native.clock(source['acquired_at'])+timedelta(hours=48),native.clock(history[-1]['date']+'T00:00:00+00:00')+timedelta(days=5),
            native.clock(originals['ssga_catalog']['acquired_at'])+timedelta(hours=48),native.clock(originals['ishares_IVV']['acquired_at'])+timedelta(hours=48))
        result[ticker]={'status':'original_replayed','identity':identity,'source':source,'history_ref':row['history'],
            'history':history,'windows':windows,'comparison':comparison,'source_valid_until':due.isoformat(),
            'latest_observation_date':history[-1]['date'],'acquired_at':source['acquired_at']}
    return {'generated_at':at,'source_generated_at':packet['source_generated_at'],'replay':ref,'sectors':result,
        'reference_dates':dates,'reference_source':originals['ishares_IVV'],'reference_calendar':packet['reference_calendar'],
        'scope':'Eleven State Street histories and IVV reference dates; other funds retained without selected-source replay.'}
