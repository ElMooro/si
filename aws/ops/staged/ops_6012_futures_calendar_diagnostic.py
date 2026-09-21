"""Inspect rejected retained market-calendar rows; no collection or mutable writes."""
from pathlib import Path
import json,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
import ops_6009_futures_original_source_preflight as baseline
from ops_6010_futures_calculation_candidate import AUDIT
import futures_source_capture as capture
import futures_session_calendar as calendar


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_6012_futures_calendar_diagnostic') as r:
        source=json.loads(baseline.checked(s3,AUDIT));results={}
        for product in capture.PRODUCTS:
            packet=source['sources'][product+':schedules'];rows=baseline.rows(s3,packet);scope=packet['scope']
            derived=calendar.reconcile(rows,product,capture.PRODUCTS[product],scope['from'],scope['to'],packet['pagination_complete'])
            invalid=derived['invalid_source_row_ordinals'];samples=[]
            for i in invalid[:30]:
                row=rows[i];samples.append({'ordinal':i,'field_names':sorted(row) if isinstance(row,dict) else [],
                    'values':{key:row.get(key) for key in ('product_code','trading_venue','session_end_date','event','timestamp')} if isinstance(row,dict) else None})
            dates=sorted({row['session_end_date'] for row in rows if isinstance(row,dict) and isinstance(row.get('session_end_date'),str)})
            results[product]={'scope':scope,'returned_rows':len(rows),'invalid_rows':len(invalid),
                'first_returned_session':dates[0] if dates else None,'last_returned_session':dates[-1] if dates else None,
                'qualified_session_closes':derived['qualified_session_closes'],'invalid_samples':samples}
        r.kv(source_audit=AUDIT,calendar_diagnostics=results,provider_requests=0,engine_invocations=0,
            public_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
