"""Replay retained ETF profiles with independent field arithmetic; no collection.

Read-only AWS audit. No producer, account, notification or paid AI invocation.
"""
from pathlib import Path
from decimal import Decimal, localcontext
from concurrent.futures import ThreadPoolExecutor
import json, re, subprocess, sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
import etf_profile_native as native
from ops_5975_etf_constituent_source_preflight import denied
BUCKET='justhodl-dashboard-live'
MANIFEST={'key':native.PRIVATE+'cfddf78961116702fc2d7db4bb8f3fcd5e673fd9961b782d0dfa337b05b743a0.bin',
    'sha256':'cfddf78961116702fc2d7db4bb8f3fcd5e673fd9961b782d0dfa337b05b743a0','bytes':159154}


def independent(collection, result, read):
    """Compare every published field against its exact original row/field."""
    counts={'profiles':0,'numeric_fields':0,'text_fields':0,'exposure_entries':0,'exposure_sums':0}
    expected_rows=[]
    for page in collection['pages']:
        raw=read(page['original']['key'])
        assert len(raw)==page['original']['bytes'] and native.sha(raw)==page['original']['sha256']
        expected_rows.append(json.loads(raw,parse_float=Decimal)['results'])
    assert len(result['profiles'])==sum(map(len,expected_rows))
    for profile in result['profiles']:
        loc=profile['source'];raw=expected_rows[loc['page']][loc['row_index']]
        assert loc['sha256']==collection['pages'][loc['page']]['original']['sha256']
        assert profile['ticker']==raw['composite_ticker']==collection['ticker']
        assert profile['effective_date']==raw['effective_date'] and profile['processed_date']==raw['processed_date']
        assert profile['dates_apply_to']=='profile_only' and profile['holdings_effective_date_inferred'] is False
        counts['profiles']+=1
        for field,cell in profile['numeric'].items():
            assert cell['source']=={**loc,'field':field} and cell['present']==(field in raw)
            value=raw.get(field)
            if value is None:
                assert cell['value_decimal'] is None and cell['raw_decimal'] is None
                assert cell['status']==('null' if field in raw else 'missing')
            else:
                assert not isinstance(value,bool) and isinstance(value,(int,Decimal))
                assert Decimal(cell['raw_decimal'])==Decimal(value)
                assert cell['status']=='reported' and Decimal(cell['value_decimal'])==Decimal(value)
            if field in ('aum','net_expenses','management_fee','total_expenses','fee_waivers','bid_ask_spread'):
                assert cell['unit_certified'] is False
            counts['numeric_fields']+=1
        for field,cell in profile['text'].items():
            assert cell['source']=={**loc,'field':field}
            assert cell['value']==raw.get(field)
            counts['text_fields']+=1
        for field,view in profile['exposures'].items():
            assert view['source']=={**loc,'field':field}
            assert view['normalized'] is False and view['unit_certified'] is False and view['portfolio_weight_eligible'] is False
            value=raw.get(field)
            if isinstance(value,dict):
                assert len(view['entries'])==len(value) and {x['label'] for x in view['entries']}==set(value)
                numbers=[]
                for cell in view['entries']:
                    source=value[cell['label']];assert not isinstance(source,bool) and isinstance(source,(int,Decimal))
                    assert cell['source']=={**loc,'field':field,'member_key':cell['label']}
                    assert Decimal(cell['raw_decimal'])==Decimal(source) and cell['status']=='reported'
                    numbers.append(Decimal(source));counts['exposure_entries']+=1
                with localcontext() as ctx:
                    ctx.prec=180
                    expected=sum(numbers,Decimal(0)) if numbers else None
                assert (Decimal(view['raw_observed_sum_decimal']) if view['raw_observed_sum_decimal'] is not None else None)==expected
                counts['exposure_sums']+=1
            elif value is None:
                assert view['raw_observed_sum_decimal'] is None and not view['entries']
            else:assert isinstance(value,list) and view['status']=='reported_array_unqualified_schema' and len(view['raw_structure']['items'])==len(value)
    assert result['quality']['independent_investment_votes']==0 and result['quality']['current_holdings_confirmed'] is False
    return counts


def main():
    s3=boto3.client('s3',region_name='us-east-1');cache={}
    def read(key):
        assert re.fullmatch(re.escape(native.PRIVATE)+r'[a-f0-9]{64}\.bin',key)
        if key not in cache:
            response=s3.get_object(Bucket=BUCKET,Key=key)['Body']
            try:raw=response.read(native.MAX_SOURCE_BYTES+1)
            finally:response.close()
            assert 0<len(raw)<=native.MAX_SOURCE_BYTES and key==native.PRIVATE+native.sha(raw)+'.bin'
            assert sum(map(len,cache.values()))+len(raw)<=128*1024*1024
            cache[key]=raw
        return cache[key]
    with report('ops_5984_etf_profile_original_replay') as r:
        subprocess.run([sys.executable,str(ROOT/'aws/shared/tests/test_etf_profile_native.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/test_etf_profile_original_replay.py')],cwd=ROOT,check=True)
        raw=read(MANIFEST['key']);assert len(raw)==MANIFEST['bytes'];audit=json.loads(raw)
        assert audit['contract']=='etf-profile-source-preflight.v1'
        results={};total={}
        for role in ('current','prior'):
            for ticker,collection in audit[role+'_probes'].items():
                result=native.reconstruct(collection,read,audit['generated_at'])
                assert result['quality']['status']=='complete_returned_profile_snapshot'
                counts=independent(collection,result,read)
                for name,n in counts.items():total[name]=total.get(name,0)+n
                results[role+'_'+ticker]={'processed_date':result['processed_date'],'effective_date':result['effective_date'],
                    'source_acquired_at':result['source_acquired_at'],'returned_profiles':len(result['profiles']),
                    'single_profile_unambiguous':result['quality']['single_profile_unambiguous'],
                    'current_profile_eligible':result['quality']['current_profile_eligible'],
                    'independent_field_arithmetic':counts,'original_source_replayed':True}
        assert len(results)==12 and total['profiles']==12
        def check(key):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(check,sorted(cache)):pass
        compilers={m.__name__:native.sha(Path(m.__file__).read_bytes()) for m in (native,native.base)}
        r.kv(audit_manifest=MANIFEST,profile_original_replays=results,independent_counts=total,compiler_sha256=compilers,
            protected_artifacts_checked=len(cache),originals_anonymously_denied=True,provider_requests=0,producer_invocations=0,
            private_account_reads=0,paid_ai_calls=0,portfolio_writes=0,signals_emitted=0,notifications_sent=0,
            scope='Profile parser validated from retained originals. Live ETF desk migration remains pending.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
