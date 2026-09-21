"""Independently reconcile FX calculations with retained original responses."""
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
import ops_6005_fx_original_source_preflight as baseline
import fx_quote_capture as capture
import option_snapshot_capture as primitives
import fx_research_model as model
BUCKET=baseline.BUCKET
AUDIT={'key':model.PRIVATE+'f4e4f42d6ab7afb61cb58b3a589e4d297b1c2f0362defd5287c771cc57efe9f4.bin',
    'sha256':'f4e4f42d6ab7afb61cb58b3a589e4d297b1c2f0362defd5287c771cc57efe9f4','bytes':53088}
STATUS=model.PRIVATE+'requests/'+model.sha(b'chatgpt-fx-calculation-candidate-6006')+'.json'


def read_ref(ref,read):
    assert isinstance(ref,dict) and isinstance(ref.get('sha256'),str) and re.fullmatch('[a-f0-9]{64}',ref['sha256'])
    assert type(ref.get('bytes')) is int and 0<ref['bytes']<=model.MAX
    assert ref['key'] in (model.PRIVATE+ref['sha256']+'.bin',model.PREFIX+'bars/'+ref['sha256']+'.json')
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256'];return raw
def decimal_display(value):
    with localcontext() as ctx:
        ctx.prec=160;ctx.rounding=ROUND_HALF_EVEN
        value=(Decimal(value.numerator)/Decimal(value.denominator)).quantize(Decimal('0.000000000001'))
        return format(value,'f').rstrip('0').rstrip('.') if value else '0'
def independent(output,sources,read):
    assert output['contract']==model.CONTRACT and set(output['pairs'])==set(capture.PAIRS)
    assert output['configured_pairs']==19 and output['call'] is None and output['score'] is None and output['independent_investment_votes']==0
    assert all(output[k] is False for k in model.FLAGS) and output['portfolio_action']=='WAIT'
    assert output['quality']=={'status':'descriptive','complete_returned_captures':19,'unambiguous_chronologies':19}
    completed=max(datetime.fromisoformat(s['completed_at']) for s in sources.values())
    assert output['source_capture_completed_at']==completed.isoformat()
    assert output['source_review_due_at']==(completed+timedelta(hours=26)).isoformat()
    total=0;fields=0;comparisons=0
    for pair,ticker in capture.PAIRS.items():
        source=sources[pair];out=output['pairs'][pair];raw_rows=[];expected_sources=[]
        assert source['pagination_complete'] is True and source['stop']=='complete_returned_pagination'
        assert all(out[k] is False for k in model.FLAGS)
        for page in source['pages']:
            doc=json.loads(read_ref(page['original'],read),parse_float=Decimal)
            assert doc['ticker']==ticker and doc['status'] in ('OK','DELAYED') and page['http_status']==200
            for index,row in enumerate(doc['results']):
                assert isinstance(row,dict);raw_rows.append(row)
                expected_sources.append({'original':page['original'],'page':page['page'],'pointer':'/results/'+str(index),
                    'request_sha256':page['request_sha256'],'acquired_at':page['acquired_at']})
        rows=[]
        for block in out['bar_blocks']:
            body=json.loads(read_ref(block,read));assert body['contract']=='fx-original-bars.v1' and body['pair']==pair and body['offset']==len(rows)
            rows.extend(body['rows'])
        assert len(rows)==len(raw_rows)>20 and out['coverage']['returned_rows']==len(rows)
        assert out['coverage']['pagination_complete'] is True and out['coverage']['full_calendar_coverage_verified'] is False
        assert out['provider_ticker']==ticker and out['source_capture_completed_at']==source['completed_at']
        base,quote=pair.split('_');metal=base in ('XAU','XAG')
        assert (out['pair'],out['base_code'],out['quote_code'])==(pair,base,quote)
        assert out['price_unit']==quote+'_per_'+('provider_'+base+'_unit' if metal else base)
        assert out['metal_base_quantity_unit_verified'] is (False if metal else None)
        assert out['source_review_due_at']==(datetime.fromisoformat(source['completed_at'])+timedelta(hours=26)).isoformat()
        assert out['request_window']=={'from':source['from'],'to':source['to'],'calendar':'provider-described Eastern Time request dates'}
        expected_endpoints=[]
        for index,(actual,original) in enumerate(zip(rows,raw_rows)):
            assert actual['ordinal']==index and actual['source']==expected_sources[index]
            assert actual['quote_observed_at'] is None and actual['close_observed_at'] is None and actual['bar_finality_verified'] is False
            for field in ('o','h','l','c','v','vw','n','t'):
                value=original.get(field);state='missing' if field not in original else 'null' if value is None else 'invalid_type'
                decimal=None
                if type(value) is int or isinstance(value,Decimal):
                    number=Decimal(value)
                    state='zero' if number==0 else 'positive' if number>0 else 'negative'
                    if abs(number.adjusted())>32 or len(number.as_tuple().digits)>80:state='outside_numeric_bound'
                    else:decimal=format(number,'f')
                assert actual['values'][field]=={'state':state,'decimal':decimal},(pair,index,field)
                fields+=1
            assert type(original['t']) is int
            stamp=(datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(milliseconds=original['t'])).isoformat()
            assert actual['window_start_utc']==stamp and Decimal(original['c'])>0
            prices={k:Decimal(original[k]) for k in ('o','h','l','c')}
            assert all(v>0 for v in prices.values())
            assert prices['l']<=min(prices['o'],prices['c'])<=max(prices['o'],prices['c'])<=prices['h']
            assert actual['original_object_row'] is True and actual['positive_close_usable_as_reported'] is True
            assert actual['ohlc_bounds_consistent'] is True and actual['issues']==[]
            assert actual['extra_field_names']==sorted(set(original)-{'o','h','l','c','v','vw','n','t'})
            expected_endpoints.append({'ordinal':index,'window_start_utc':stamp,'reported_close_decimal':format(Decimal(original['c']),'f'),
                'source':expected_sources[index],'close_state':'positive','positive_close_usable_as_reported':True,
                'ohlc_bounds_consistent':True,'quote_observed_at':None,'close_observed_at':None,'bar_finality_verified':False})
        order=sorted(range(len(raw_rows)),key=lambda i:raw_rows[i]['t'])
        assert len({row['t'] for row in raw_rows})==len(raw_rows) and out['chronology_unambiguous'] is True
        assert out['chronological_source_ordinals']==order
        assert out['latest_reported_row']==expected_endpoints[order[-1]]
        assert out['coverage']=={'returned_rows':len(rows),'object_rows':len(rows),'positive_close_rows':len(rows),
            'close_states':{'positive':len(rows)},'invalid_window_start_rows':0,'duplicate_window_start_rows':0,
            'pagination_complete':True,'full_calendar_coverage_verified':False,'stop':'complete_returned_pagination'}
        assert set(out['comparisons'])=={'1','5','20'}
        for n in (1,5,20):
            value=out['comparisons'][str(n)];a_index,b_index=order[-1-n],order[-1]
            a,b=Fraction(Decimal(raw_rows[a_index]['c'])),Fraction(Decimal(raw_rows[b_index]['c']))
            assert value['available'] is True and value['requested_row_offset']==n
            assert all(value[k] is False for k in model.FLAGS)
            assert value['from']['ordinal']==a_index and value['to']['ordinal']==b_index
            assert value['from']['source']==expected_sources[a_index] and value['to']['source']==expected_sources[b_index]
            assert value['from']==expected_endpoints[a_index] and value['to']==expected_endpoints[b_index]
            assert value['unit']=='percent_change_in_reported_quote_close' and value['reason'] is None
            assert value['positive_close_rows_in_window']==n+1
            assert value['decimal_display_places']==12 and value['decimal_rounding']=='ROUND_HALF_EVEN'
            assert value['source_row_ordinals']==order[-1-n:] and value['returned_rows_in_window']==n+1
            span=Fraction(raw_rows[b_index]['t']-raw_rows[a_index]['t'],86400000)
            assert value['elapsed_calendar_days_decimal']==decimal_display(span)
            for prefix,expected in (('quoted_rate_change',(b-a)*100/a),('inverse_rate_change',(a-b)*100/b)):
                exact=value[prefix+'_exact'];assert Fraction(int(exact['numerator']),int(exact['denominator']))==expected
                assert value[prefix+'_decimal']==decimal_display(expected);comparisons+=1
        total+=len(rows)
    assert output['returned_rows']==total
    return {'pairs':19,'original_rows':total,'reported_numeric_field_slots':fields,'exact_rate_comparisons':comparisons}


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    def read(key):
        assert re.fullmatch(re.escape(model.PRIVATE)+r'[a-f0-9]{64}\.bin|'+re.escape(model.PREFIX)+r'bars/[a-f0-9]{64}\.json',key)
        return baseline.get(s3,key)
    artifacts={}
    def emit(kind,doc):
        assert kind=='bars';raw=model.encoded(doc);ref=model.ref(raw,kind)
        try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,ContentType='application/json',CacheControl='public, max-age=31536000, immutable',IfNoneMatch='*')
        except Exception as exc:
            if baseline.code(exc) not in ('PreconditionFailed','ConditionalRequestConflict','409','412'):raise
        assert read_ref(ref,read)==raw;artifacts[ref['key']]=ref;return ref
    with report('ops_6006_fx_calculation_candidate') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_fx_research_model.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/test_fx_calculation_candidate.py')],cwd=ROOT,check=True)
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
        candidate={'contract':'fx-calculation-candidate.v1','generated_at':capture.now(),'source_audit':AUDIT,
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
