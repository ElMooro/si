"""Qualify exact option fields/aggregations from retained originals; no recollection."""
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib, json, subprocess, sys, time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
import ops_5987_options_dependency_preflight as baseline
import ops_5988_option_originals_qualification as qualification
import option_contract_research as model

PREFIX=baseline.PREFIX;BUCKET=baseline.BUCKET
MANIFEST={'key':PREFIX+'b81c4ad3ffc2872243bc4bde1b5994663c4c7137cd5a8a13c31cea56b4323d1d.bin',
    'sha256':'b81c4ad3ffc2872243bc4bde1b5994663c4c7137cd5a8a13c31cea56b4323d1d','bytes':58138}


def verify_fields(rows,pages,output):
    """Independent original-field arithmetic, not a second call to the compiler."""
    assert len(rows)==len(output['rows']) and output['total_session_volume'] is None
    assert output['dealer_position'] is None and output['independent_investment_votes']==0
    for k in model.PERMISSIONS:assert output[k] is False
    counts=Counter();ois={'call':[],'put':[]};groups=defaultdict(lambda:{'call':[],'put':[]})
    expected_quality=defaultdict(Counter)
    position=0
    for page in pages:
        source=model.capture.decode(page['raw'])['results']
        for index,raw in enumerate(source):
            item=output['rows'][position];position+=1
            assert item['evidence']=={'original':page['original'],'page':page['page'],'row_index':index,'row_pointer':'/results/'+str(index)}
            assert item['source_received_at']==model.clock(page['acquired_at']).isoformat()
            assert item['contract_id']==raw['details']['ticker']
            # All three qualified probes independently report the standard deliverable.
            assert raw['details']['shares_per_contract']==100 and raw['details']['exercise_style']=='american'
            assert item['identity_eligible'] and not item['identity_reasons'], 'Unexpected source identity requires review'
            assert item['open_interest_date'] is None and item['iv_observation_date'] is None and item['greek_observation_date'] is None
            kind=raw['details']['contract_type'];metrics=item['metrics'];counts['original_rows']+=1
            for name,parent,key in (('open_interest',raw,'open_interest'),('daily_volume',raw.get('day',{}),'volume')):
                cell=metrics[name];v=parent.get(key)
                if key not in parent:assert cell['state']=='missing' and cell['value'] is None
                elif v is None:assert cell['state']=='null' and cell['value'] is None
                else:
                    assert not isinstance(v,bool) and isinstance(v,(int,Decimal)) and 0<=v<=10**18 and Decimal(v)==Decimal(v).to_integral_value()
                    assert Decimal(cell['value'])==v and cell['state']==('reported_zero' if v==0 else 'reported')
                    counts['quantity_fields']+=1;counts['true_zero_quantities']+=int(v==0)
                if name=='open_interest':ois[kind].append(v)
            for name,parent,key in (('strike',raw['details'],'strike_price'),('shares_per_contract',raw['details'],'shares_per_contract'),
                    ('vendor_iv',raw,'implied_volatility'),('vendor_gamma',raw.get('greeks',{}),'gamma'),('vendor_delta',raw.get('greeks',{}),'delta')):
                cell=metrics[name];v=parent.get(key)
                if key not in parent:assert cell['state']=='missing' and cell['value'] is None
                elif v is None:assert cell['state']=='null' and cell['value'] is None
                else:
                    assert Decimal(cell['reported_value'])==v
                    if name=='vendor_gamma' and v<0:
                        assert cell['state']=='outside_domain' and cell['value'] is None;counts['negative_gamma_retained_and_excluded']+=1
                    elif name=='vendor_delta' and not (-1<=v<=0 if kind=='put' else 0<=v<=1):
                        assert cell['state']=='outside_domain' and cell['value'] is None;counts['delta_domain_exclusions']+=1
                    elif name=='vendor_iv' and v<=0:assert cell['value'] is None
                    else:assert Decimal(cell['value'])==v
                    counts['numeric_fields']+=1
            bar=raw.get('day',{});stamp=bar.get('last_updated');parsed=item['clocks']['daily_bar_updated']
            if stamp is None:assert parsed['value'] is None
            else:
                assert parsed['raw_nanoseconds']==str(stamp)
                # Calendar grouping is an update-date label, never a session-volume claim.
                when=datetime.fromtimestamp(stamp//1000000000,timezone.utc).astimezone(model.EASTERN).date().isoformat()
                assert parsed['date_new_york']==when
                groups[when][kind].append(bar.get('volume'));counts['exact_timestamp_fields']+=1
            for name,cell in metrics.items():expected_quality[name][cell['state']]+=1
    assert output['coverage']['field_quality']=={k:dict(v) for k,v in expected_quality.items()}
    def check_total(cell,values):
        observed=[Decimal(v) for v in values if v is not None]
        assert cell['included_rows']==len(observed) and cell['population_rows']==len(values)
        if observed:assert Decimal(cell['value'])==sum(observed,Decimal(0))
        else:assert cell['value'] is None
        assert cell['missing_or_invalid_rows']==len(values)-len(observed)
        assert cell['complete_field_coverage']==(bool(values) and len(observed)==len(values))
    check_total(output['reported_open_interest']['calls'],ois['call'])
    check_total(output['reported_open_interest']['puts'],ois['put'])
    def check_ratio(calls,puts,ratio):
        assert ratio['numerator']==calls['value'] and ratio['denominator']==puts['value']
        if not calls['complete_field_coverage'] or not puts['complete_field_coverage']:
            assert ratio['value'] is None and ratio['status']=='incomplete_field_coverage'
        elif Decimal(puts['value'])==0:
            assert ratio['value'] is None and ratio['status']=='zero_denominator'
        else:
            assert ratio['status']=='descriptive_reported_ratio'
            with localcontext() as ctx:
                ctx.prec=180
                exact=Decimal(calls['value'])/Decimal(puts['value'])
                assert abs(Decimal(ratio['value'])-exact)<=Decimal('0.0000000000005')
    interest=output['reported_open_interest']
    check_ratio(interest['calls'],interest['puts'],interest['call_put_ratio'])
    assert len(output['daily_bar_update_groups'])==len(groups)
    for group in output['daily_bar_update_groups']:
        src=groups[group['daily_bar_update_date_new_york']]
        check_total(group['calls'],src['call']);check_total(group['puts'],src['put'])
        assert group['market_session_completeness_verified'] is False
        check_ratio(group['calls'],group['puts'],group['call_put_ratio'])
        counts['dated_volume_groups']+=1
    return dict(counts)


def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1');started=time.monotonic()
    with report('ops_5989_option_contract_original_replay') as r:
        for test in ('test_option_contract_research.py','test_option_contract_acceptance.py'):
            subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
        original=json.loads(qualification.checked(s3,MANIFEST))
        assert qualification.checked(s3,original['compiler'])==Path(model.capture.__file__).read_bytes()
        outputs={};counts={};protected={MANIFEST['key'],original['compiler']['key']}
        for symbol,chain in original['option_snapshots'].items():
            assert chain['pagination_complete'];qualification.summarize_chain(s3,chain)
            pages=[];rows=[]
            for meta in chain['pages']:
                assert meta['status']=='received' and meta['http_status']==200
                raw=qualification.checked(s3,meta['original']);protected.add(meta['original']['key'])
                pages.append({**meta,'raw':raw});rows.extend(model.capture.decode(raw)['results'])
            output=model.compile_rows(symbol,pages,True)
            counts[symbol]=verify_fields(rows,pages,output)
            raw=baseline.evidence.encoded(output)
            outputs[symbol]={'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'coverage':output['coverage'],
                'reported_open_interest':output['reported_open_interest'],'dated_volume_group_count':len(output['daily_bar_update_groups']),
                'oldest_daily_bar_update_date':output['daily_bar_update_groups'][0]['daily_bar_update_date_new_york'] if output['daily_bar_update_groups'] else None,
                'newest_daily_bar_update_date':output['daily_bar_update_groups'][-1]['daily_bar_update_date_new_york'] if output['daily_bar_update_groups'] else None}
            del output,raw,pages,rows
        compiler=baseline.retain(s3,Path(model.__file__).read_bytes());protected.add(compiler['key'])
        manifest={'contract':'option-contract-original-qualification.v1','generated_at':model.capture.now(),
            'source_manifest':MANIFEST,'compilers':{'option_contract_research':compiler,'option_snapshot_capture':original['compiler']},
            'outputs':outputs,'independent_checks':counts,'elapsed_s':round(time.monotonic()-started,3),
            'peak_runner_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'scope':'Complete retained original snapshot row replay and descriptive arithmetic. No native producer/publication migration or forecast authority.'}
        ref=baseline.retain(s3,baseline.evidence.encoded(manifest));protected.add(ref['key'])
        from concurrent.futures import ThreadPoolExecutor
        def check(key):
            assert baseline.evidence.denied('https://justhodl.ai/'+key) and baseline.evidence.denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=6) as pool:list(pool.map(check,sorted(protected)))
        r.kv(retained_manifest=ref,outputs=outputs,independent_checks=counts,
            replay_elapsed_s=manifest['elapsed_s'],peak_runner_rss_kib=manifest['peak_runner_rss_kib'],
            protected_artifacts_checked=len(protected),originals_anonymously_denied=True,
            provider_requests=0,engine_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
