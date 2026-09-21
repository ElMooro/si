"""Independently reconcile futures calculations with retained original responses."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone,timedelta
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from fractions import Fraction
import json,re,subprocess,sys,time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
import ops_6009_futures_original_source_preflight as baseline
import futures_source_capture as capture
import option_snapshot_capture as primitives
import futures_research_model as model
BUCKET=baseline.BUCKET
AUDIT={'key':model.PRIVATE+'c238fe5ca27ad2ac25f61414243feea8a762c81633fbbf60284acf12bdbda950.bin',
    'sha256':'c238fe5ca27ad2ac25f61414243feea8a762c81633fbbf60284acf12bdbda950','bytes':120966}
STATUS=model.PRIVATE+'requests/'+model.sha(b'chatgpt-futures-calculation-candidate-6010')+'.json'


def read_ref(ref,read):
    assert isinstance(ref,dict) and isinstance(ref.get('sha256'),str) and re.fullmatch('[a-f0-9]{64}',ref['sha256'])
    assert type(ref.get('bytes')) is int and 0<ref['bytes']<=model.MAX
    assert ref['key'] in (model.PRIVATE+ref['sha256']+'.bin',model.PREFIX+'records/'+ref['sha256']+'.json')
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256'];return raw
def decimal_display(value):
    with localcontext() as ctx:
        ctx.prec=160;ctx.rounding=ROUND_HALF_EVEN
        value=(Decimal(value.numerator)/Decimal(value.denominator)).quantize(Decimal('0.000000000001'))
        return format(value,'f').rstrip('0').rstrip('.') if value else '0'
def independent(output,sources,read):
    assert output['contract']==model.CONTRACT and set(output['products'])==set(capture.PRODUCTS)
    assert output['call'] is None and output['score'] is None and output['independent_investment_votes']==0
    assert all(output[k] is False for k in model.FLAGS) and output['portfolio_action']=='WAIT'
    assert output['source_capture_completed_at']==max(datetime.fromisoformat(s['completed_at']) for s in sources.values()).isoformat()
    assert set(output['datasets'])==set(sources)
    raw_rows={};locations={};numeric_fields=0;total_rows=0;comparisons=0;curves=0;schedule_diagnostics={}
    def same_typed(actual,original):
        nonlocal numeric_fields
        if type(original) is int or isinstance(original,Decimal):
            v=Decimal(original);state='zero' if v==0 else 'positive' if v>0 else 'negative';decimal=format(v,'f')
            if abs(v.adjusted())>32 or len(v.as_tuple().digits)>80:state='outside_numeric_bound';decimal=None
            assert actual=={'number':{'state':state,'decimal':decimal}};numeric_fields+=1
        elif isinstance(original,dict):
            assert isinstance(actual,dict) and set(actual)==set(original)
            for k,v in original.items():same_typed(actual[k],v)
        elif isinstance(original,list):
            assert isinstance(actual,list) and len(actual)==len(original)
            for a,b in zip(actual,original):same_typed(a,b)
        else:assert type(actual) is type(original) and actual==original
    for name,source in sources.items():
        original=[];points=[];dataset=output['datasets'][name]
        assert source['pagination_complete'] is True and dataset['pagination_complete'] is True
        assert dataset['scope']==source['scope'] and dataset['source_capture_completed_at']==source['completed_at']
        for page in source['pages']:
            doc=json.loads(read_ref(page['original'],read),parse_float=Decimal)
            assert doc['status'] in ('OK','DELAYED') and page['http_status']==200
            for i,row in enumerate(doc['results']):
                original.append(row);points.append({'original':page['original'],'pointer':'/results/'+str(i),'page':page['page'],
                    'request_sha256':page['request_sha256'],'acquired_at':page['acquired_at']})
        actual=[]
        for ref in dataset['records']:
            block=json.loads(read_ref(ref,read));assert block['contract']=='futures-original-records.v1' and block['dataset']==name and block['offset']==len(actual)
            actual.extend(block['rows'])
        assert dataset['returned_rows']==len(actual)==len(original)
        for index,(a,b) in enumerate(zip(actual,original)):
            assert a['ordinal']==index and a['source']==points[index];same_typed(a['values'],b)
        raw_rows[name]=original;locations[name]=points;total_rows+=len(original)
        if source['scope']['kind']=='schedules':
            from collections import Counter
            schedule_diagnostics[name]={'product_codes':dict(Counter(str(r.get('product_code')) for r in original)),
                'venues':dict(Counter(str(r.get('trading_venue')) for r in original)),
                'names':dict(Counter(str(r.get('name')) for r in original)),
                'first_events':[{k:r.get(k) for k in ('product_code','trading_venue','name','event','session_end_date','timestamp')} for r in original[:6]]}
    def endpoint(name,index,field):
        row=raw_rows[name][index];value=row.get(field)
        return {'ordinal':index,'ticker':row['ticker'],'session_end_date':row['session_end_date'],
            'window_start_ns':str(row['window_start']),'field':field,'reported_decimal':format(Decimal(value),'f') if value is not None else None,'source':locations[name][index]}
    def exact(value,prefix,expected):
        assert value[prefix+'_exact']=={'numerator':str(expected.numerator),'denominator':str(expected.denominator)}
        assert value[prefix+'_decimal']==decimal_display(expected)
    for product,out in output['products'].items():
        assert all(out[k] is False for k in model.FLAGS) and out['venue']==capture.PRODUCTS[product]
        specs=raw_rows[product+':products'];assert len(specs)==1;spec=specs[0];meta=out['specification']
        assert meta['available'] is True and meta['quantity_conversion_qualified'] is True
        same_typed(meta['provider_reported'],spec);assert meta['source']==locations[product+':products'][0]
        assert spec['trade_currency_code']==spec['settlement_currency_code']=='USD'
        assert meta['usd_value_per_price_unit_per_contract_decimal']==format(Decimal(spec['unit_of_measure_qty']),'f')
        contracts=raw_rows[product+':contracts'];order=sorted(range(len(contracts)),key=lambda i:(contracts[i]['last_trade_date'],contracts[i]['settlement_date'],contracts[i]['ticker']))
        assert len(out['contracts'])==min(3,len(contracts))
        assert [r['ticker'] for r in out['contracts']]==[contracts[i]['ticker'] for i in order[:3]]
        for contract_index,record in zip(order,out['contracts']):
            same_typed(record['definition'],contracts[contract_index]);assert record['definition_source']==locations[product+':contracts'][contract_index]
            name=product+':bars:'+record['ticker'];rows=raw_rows[name];indices=sorted(range(len(rows)),key=lambda i:rows[i]['session_end_date'])
            assert record['dataset']==name and record['coverage']['returned_rows']==len(rows)
            assert record['coverage']['missing_settlement_rows']==sum('settlement_price' not in r for r in rows)
            assert record['coverage']['full_calendar_coverage_verified'] is False and record['coverage']['bar_finality_independently_verified'] is False
            assert record['chronology_unambiguous'] is True
            assert len({r['session_end_date'] for r in rows})==len(rows) and len({r['window_start'] for r in rows})==len(rows)
            assert all(rows[b]['window_start']>rows[a]['window_start'] for a,b in zip(indices,indices[1:]))
            for r in rows:
                assert r['ticker']==record['ticker'] and type(r['window_start']) is int
                assert r['low']<=min(r['open'],r['close'])<=max(r['open'],r['close'])<=r['high']
            latest=record['latest_reported_row'];assert latest['ordinal']==indices[-1] and latest['source']==locations[name][indices[-1]]
            for field,windows in record['comparisons'].items():
                assert field in ('close','settlement_price') and set(windows)=={'1','5','20'}
                for n in (1,5,20):
                    value=windows[str(n)];assert all(value[k] is False for k in model.FLAGS)
                    assert value['requested_row_offset']==n and value['price_field']==field
                    if len(rows)<=n:
                        assert value['available'] is False and value['reason']=='insufficient_returned_rows';continue
                    ai,bi=indices[-1-n],indices[-1];a,b=rows[ai].get(field),rows[bi].get(field)
                    assert value['from']==endpoint(name,ai,field) and value['to']==endpoint(name,bi,field)
                    assert value['source_row_ordinals']==indices[-1-n:]
                    assert value['elapsed_calendar_days']==(datetime.fromisoformat(rows[bi]['session_end_date'])-datetime.fromisoformat(rows[ai]['session_end_date'])).days
                    if a is None or b is None:
                        assert value['available'] is False and value['reason']=='reported_endpoint_unavailable_or_inconsistent';continue
                    assert value['available'] is True
                    av,bv=Fraction(Decimal(a)),Fraction(Decimal(b));exact(value,'absolute_change',bv-av);comparisons+=1
                    if av>0:exact(value,'percent_change',100*(bv-av)/av)
                    else:assert value['percent_change_decimal'] is None
        assert len(out['matched_curves'])==max(0,len(out['contracts'])-1)*2
        for curve in out['matched_curves']:
            assert all(curve[k] is False for k in model.FLAGS);field=curve['price_field']
            aname=product+':bars:'+curve['near_ticker'];bname=product+':bars:'+curve['far_ticker']
            a={r['session_end_date']:i for i,r in enumerate(raw_rows[aname])};b={r['session_end_date']:i for i,r in enumerate(raw_rows[bname])}
            common=sorted(set(a)&set(b))
            if not common:assert curve['available'] is False;continue
            session=common[-1];ai,bi=a[session],b[session]
            assert curve['session_end_date']==session and curve['near']==endpoint(aname,ai,field) and curve['far']==endpoint(bname,bi,field)
            assert curve['common_returned_sessions']==len(common)
            av,bv=raw_rows[aname][ai].get(field),raw_rows[bname][bi].get(field)
            if av is None or bv is None:assert curve['available'] is False;continue
            assert curve['available'] is True;exact(curve,'far_minus_near',Fraction(Decimal(bv))-Fraction(Decimal(av)));curves+=1
    return {'products':len(output['products']),'datasets':len(sources),'original_rows':total_rows,'numeric_field_values_checked':numeric_fields,
        'exact_price_comparisons':comparisons,'exact_common_session_spreads':curves,'schedule_diagnostics':schedule_diagnostics}


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    def read(key):
        assert re.fullmatch(re.escape(model.PRIVATE)+r'[a-f0-9]{64}\.bin|'+re.escape(model.PREFIX)+r'records/[a-f0-9]{64}\.json',key)
        return baseline.get(s3,key)
    artifacts={}
    original_read=read;cache={};cached_bytes=0
    def read(key):
        nonlocal cached_bytes
        if key in cache:return cache[key]
        raw=original_read(key)
        if cached_bytes+len(raw)<=64*1024*1024:cache[key]=raw;cached_bytes+=len(raw)
        return raw
    def emit(kind,doc):
        assert kind=='records';raw=model.encoded(doc);ref=model.ref(raw,kind)
        try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,ContentType='application/json',CacheControl='public, max-age=31536000, immutable',IfNoneMatch='*')
        except Exception as exc:
            if baseline.code(exc) not in ('PreconditionFailed','ConditionalRequestConflict','409','412'):raise
        assert read_ref(ref,read)==raw;artifacts[ref['key']]=ref;return ref
    with report('ops_6010_futures_calculation_candidate') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_futures_research_model.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/test_futures_calculation_candidate.py')],cwd=ROOT,check=True)
        source=json.loads(read_ref(AUDIT,read))
        for module in (capture,primitives):assert read_ref(source['compilers'][module.__name__],read)==Path(module.__file__).read_bytes()
        runtime=baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION);assert runtime==source['runtime']
        compilers={m.__name__:baseline.retain(s3,Path(m.__file__).read_bytes()) for m in (capture,primitives,model)}
        try:prior=json.loads(baseline.get(s3,STATUS))
        except Exception as exc:
            if baseline.code(exc) not in ('404','NoSuchKey'):raise
            prior=None
        if prior:
            assert prior['status']=='complete' and prior['compilers']==compilers,'Inspect retained incomplete calculation; do not discard its evidence'
            candidate=json.loads(read_ref(prior['candidate'],read));output=json.loads(read_ref(candidate['output'],read))
            generated=output['generated_at'];r.kv(adopted_completed_candidate=prior['candidate'])
        else:
            generated=capture.now();baseline.write_status(s3,STATUS,{'status':'claimed','source_audit':AUDIT,'compilers':compilers,'generated_at':generated},IfNoneMatch='*')
        start=time.monotonic();compiled=model.compile_output(source['sources'],generated,read,emit)
        if prior:assert compiled==output
        output=compiled;assert model.compile_output(source['sources'],generated,read,emit)==output
        counts=independent(output,source['sources'],read);elapsed=round(time.monotonic()-start,3)
        assert baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION)==runtime
        assert baseline.get(s3,model.LEGACY)==read_ref(source['predecessors'][model.LEGACY]['original'],read)
        candidate={'contract':'futures-calculation-candidate.v1','generated_at':capture.now(),'source_audit':AUDIT,
            'output':baseline.retain(s3,model.encoded(output)),'row_artifacts':list(artifacts.values()),'compilers':compilers,
            'counts':counts,'candidate_seconds':elapsed,'predecessor_runtime':runtime,
            'original_source_replay_verified':True,'independent_rational_reconciliation':True,'provider_requests':0,
            'engine_invocations':0,'public_head_writes':0,'private_account_reads':0,'paid_ai_calls':0,
            'notifications_sent':0,'portfolio_writes':0,'schedules_changed':0}
        ref=baseline.retain(s3,model.encoded(candidate));r.kv(retained_candidate=ref,counts=counts,candidate_seconds=elapsed)
        baseline.write_status(s3,STATUS,{'status':'complete','candidate':ref,'compilers':compilers,'source_audit':AUDIT})
        protected={STATUS,AUDIT['key'],ref['key'],candidate['output']['key'],*(x['key'] for x in compilers.values()),
            *(p['original']['key'] for s in source['sources'].values() for p in s['pages'] if p.get('original'))}
        def deny(key):assert baseline.denied_with_retry('https://justhodl.ai/'+key) and baseline.denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        candidate.update(privacy_verified=True,protected_artifacts_checked=len(protected),retained_candidate=ref)
        accepted=baseline.retain(s3,model.encoded(candidate));deny(accepted['key'])
        r.kv(accepted_candidate=accepted,counts=counts,candidate_seconds=elapsed,protected_artifacts_checked=len(protected)+1,
            originals_anonymously_denied=True,provider_requests=0,engine_invocations=0,public_head_writes=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
