"""Inspect 6040's one failed partition and retain all completed work as evidence.

No recollection, no current publication, no duplicate-row removal. Complete
partitions are checked against the original response bodies; failed June stays
explicitly incomplete and cannot contribute a concentration calculation.
"""
from pathlib import Path
from fractions import Fraction
from decimal import Decimal
import json,sys,subprocess
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
import offexchange_measurements as model
import ops_6040_offexchange_complete_partitions as capture
from ops_6039_offexchange_retained_measurement_review import independent_ratio
prior=capture.prior

def clean(value):
    if isinstance(value,Decimal):return format(value,'f')
    if isinstance(value,list):return [clean(v) for v in value]
    if isinstance(value,dict):return {k:clean(v) for k,v in value.items()}
    return value

def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_6041_offexchange_partition_diagnosis') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_offexchange_measurements.py')],cwd=ROOT,check=True)
        state=prior.strict(prior.get(s3,capture.STATUS));assert state['request_id']==capture.REQUEST
        failed_name='monthlySummary/OTC_M_SMBL_FIRM/2026-06-01/NMS'
        assert set(state['errors'])=={failed_name} and len(state['results'])==5
        failed=prior.strict(prior.get(s3,capture.key_for('partition:'+failed_name)));assert failed['status']=='failed'
        seen={};duplicates=[]
        for page_number,page in enumerate(failed['pages']):
            rows=model.strict(prior.original(s3,page['original']))
            for row_number,row in enumerate(rows):
                key=capture.grain(row,failed['partition']);ref={'page':page_number,'row':row_number,'original':page['original']}
                if key in seen:
                    previous=seen[key];duplicates.append({'grain':key,'first':previous,'next':{'locator':ref,'row':clean(row)},'exact_row_duplicate':previous['row']==clean(row)})
                else:seen[key]={'locator':ref,'row':clean(row)}
        assert duplicates,'Expected recorded duplicate grain must be reproducible'
        checks={};errors={};weekly=[];monthly=[]
        for name,result in state['results'].items():
            try:
                part=result['partition'];rows=[];total=None
                for page in result['pages'][:-1]:
                    raw=prior.original(s3,page['original']);boundary=model.page(raw,page['headers'],page['body']['offset'],page['body']['limit'])
                    if total is None:total=boundary['reported_total']
                    assert total==boundary['reported_total']
                    parsed=model.weekly(raw,part['code'],part['period'],part['tier']) if part['dataset']=='weeklySummary' else model.monthly(raw,part['period'],part['tier'])
                    for row in parsed:row['source_original']=page['original'];rows.append(row)
                assert len(rows)==total==result['rows']
                for row in rows:independent_ratio(row['average_shares_per_reported_trade'],Fraction(row['shares']),Fraction(row['trades']))
                if part['dataset']=='weeklySummary':weekly.extend(rows)
                else:monthly.extend(rows)
                checks[name]={'rows':len(rows),'provider_requests_already_recorded':result['provider_requests'],'records_reconciled':True,'snapshot_atomic':False}
            except Exception as exc:errors[name]=str(exc)
        joined=model.join_weekly(weekly);concentration=model.concentration(monthly,records_reconciled=True)
        for row in joined:
            if row['ats'] and row['non_ats']:
                total=Fraction(row['ats']['shares'])+Fraction(row['non_ats']['shares']);assert Fraction(row['reported_offexchange_shares'])==total
                independent_ratio(row['ats_pct_of_reported_offexchange'],Fraction(row['ats']['shares'])*100,total)
        for row in concentration:
            raw=row['source_rows'];total=sum((Fraction(v['shares']) for v in raw),Fraction(0));assert Fraction(row['reported_non_ats_shares'])==total
            named=sum((Fraction(v['shares'])**2 for v in raw if v['firm_crd']!='0'),Fraction(0));unknown=sum((Fraction(v['shares']) for v in raw if v['firm_crd']=='0'),Fraction(0))
            independent_ratio(row['reported_activity_hhi_lower_bound'],named*10000,total**2)
            independent_ratio(row['reported_activity_hhi_upper_bound'],(named+unknown**2)*10000,total**2)
        r.kv(failed_request=capture.REQUEST,complete_partition_checks=checks,measurement_failures=errors,
            duplicate_rows=duplicates,failed_partition_pages=len(failed['pages']),failed_partition_unique_rows_seen=len(seen),
            joined_weekly_rows=len(joined),matched_weekly_legs=sum(bool(v['ats'] and v['non_ats']) for v in joined),
            monthly_symbol_period_rows=len(concentration),monthly_rows=len(monthly),
            de_minimis_symbol_buckets=sum(v['de_minimis_bucket_present'] for v in concentration),
            parser_sha256=prior.sha(Path(model.__file__).read_bytes()),provider_requests=0,engine_invocations=0,
            public_head_writes=0,original_recollection=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
        assert not errors,errors
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
