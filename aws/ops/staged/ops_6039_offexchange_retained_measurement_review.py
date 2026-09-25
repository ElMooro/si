"""Reconstruct retained FINRA observations without new provider calls or writes."""
from pathlib import Path
from fractions import Fraction
from decimal import Decimal
import json,sys,subprocess
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
import offexchange_measurements as model
import ops_6036_offexchange_whole_source_preflight as original
REF={'key':original.PRIVATE+'a87413d775af2e211170e707bc3869fbb143d89d5bf5fc3ffc5cf8c381dc75b6.bin','sha256':'a87413d775af2e211170e707bc3869fbb143d89d5bf5fc3ffc5cf8c381dc75b6','bytes':26448}

def independent_ratio(actual,a,b):
    if not b:assert actual is None;return
    assert abs(Fraction(actual)-Fraction(a)/Fraction(b))<=Fraction(1,2*10**12)

def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_6039_offexchange_retained_measurement_review') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_offexchange_measurements.py')],cwd=ROOT,check=True)
        manifest=original.strict(original.original(s3,REF));assert manifest['request_id']=='chatgpt-offexchange-journalled-inventory-6038'
        daily=manifest['captures']['daily:CNMS'];assert daily['http_status']==200
        raw=original.original(s3,daily['original']);date=manifest['parents']['data/finra-short.json']['data_date']
        failures={};daily_rows=[];weekly=[];partitions={};totals={}
        try:
            daily_rows=model.cnms(raw,date)
            lines=raw.decode('utf-8-sig').splitlines()[1:-1];assert len(lines)==len(daily_rows)
            sums=[Fraction(0),Fraction(0),Fraction(0)]
            for line,compiled in zip(lines,daily_rows):
                fields=line.split('|');assert compiled['symbol']==fields[1]
                for i,key in enumerate(('short_volume_shares','short_exempt_volume_shares','total_volume_shares')):
                    assert Fraction(compiled[key])==Fraction(fields[i+2]);sums[i]+=Fraction(fields[i+2])
                independent_ratio(compiled['short_volume_pct'],Fraction(fields[2])*100,Fraction(fields[4]))
                independent_ratio(compiled['short_exempt_pct_of_short'],Fraction(fields[3])*100,Fraction(fields[2]))
            totals={key:str(value) for key,value in zip(('short_shares_exact_fraction','exempt_shares_exact_fraction','total_shares_exact_fraction'),sums)}
        except Exception as exc:failures['daily']=str(exc)
        for code in model.WEEKLY_CODES:
            capture=manifest['captures']['probe:'+code];raw=original.original(s3,capture['original'])
            try:
                page=model.page(raw,capture['headers'],capture['body']['offset'],capture['body']['limit'])
                doc=json.loads(raw,parse_float=Decimal);assert len(doc)>0
                week=doc[0]['weekStartDate'];tier=doc[0]['tierIdentifier'];compiled=model.weekly(raw,code,week,tier)
                for row,derived in zip(doc,compiled):
                    assert Fraction(str(row['totalWeeklyShareQuantity']))==Fraction(derived['shares'])
                    assert Fraction(str(row['totalWeeklyTradeCount']))==Fraction(derived['trades'])
                    independent_ratio(derived['average_shares_per_reported_trade'],Fraction(derived['shares']),Fraction(derived['trades']))
                weekly.extend(compiled);partitions[code]={'week':week,'tier':tier,'pagination':page}
            except Exception as exc:failures[code]=str(exc)
        joined=model.join_weekly(weekly)
        for row in joined:
            if row['ats'] and row['non_ats']:
                total=Fraction(row['ats']['shares'])+Fraction(row['non_ats']['shares']);assert Fraction(row['reported_offexchange_shares'])==total
                independent_ratio(row['ats_pct_of_reported_offexchange'],Fraction(row['ats']['shares'])*100,total)
            else:assert row['reported_offexchange_shares'] is None
        r.kv(manifest=REF,parser_sha256=original.sha(Path(model.__file__).read_bytes()),failures=failures,
            daily_date=date,daily_rows=len(daily_rows),daily_totals=totals,weekly_rows=len(weekly),partitions=partitions,
            joined_rows=len(joined),matched_legs=sum(bool(v['ats'] and v['non_ats']) for v in joined),
            missing_legs=sum(not(v['ats'] and v['non_ats']) for v in joined),coverage_complete=False,
            provider_requests=0,engine_invocations=0,public_head_writes=0,private_account_reads=0,
            paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
        assert not failures,failures
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
