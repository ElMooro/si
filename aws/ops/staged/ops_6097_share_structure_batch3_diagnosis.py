"""Inspect the retained batch-3 source failure, without a provider retry."""
from pathlib import Path
import json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import share_structure_sources as source
import share_structure_campaign as campaign
import share_structure_batch_runner as batches


def message(value):
    if not isinstance(value,str):return {'type':type(value).__name__}
    value=re.sub(r'https?://\S+','[provider-url]',value)
    value=re.sub(r'(?i)(api[_-]?key|token|authorization)\s*[=:]\s*\S+',r'\1=[redacted]',value)
    return value.replace('|',' / ')[:600]


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_6097_share_structure_batch3_diagnosis') as r:
        key=source.request_key(batches.PARENT,'batch:3')
        state=campaign.read_journal(s3,key)
        assert state['status']=='failed' and state['part']==3 and state['source_errors']
        document=json.loads(source.read(s3,state['plan']))
        expected={spec['url']:spec for spec in campaign.specifications(document,3)}
        original=source.retain(s3,source.encode(state))
        report_raw=(ROOT/'aws/ops/reports/latest/ops_6085_share_structure_sources_part_3.md').read_bytes()
        assert b'Full source capture incomplete' in report_raw
        report_ref=source.retain(s3,report_raw)
        findings=[];protected={key,original['key'],report_ref['key']}
        for url in sorted(state['source_errors']):
            assert url in expected
            request_key=source.request_key(batches.PARENT,url);retained=campaign.read_journal(s3,request_key)
            assert retained['spec']==expected[url] and retained['status']=='failed'
            protected.add(request_key)
            finding={k:retained.get(k) for k in ('spec','error_type','http_status','headers','requested_at','received_at','original','transport_attempted')}
            ref=retained.get('original')
            if ref:
                raw=source.read(s3,ref);protected.add(ref['key'])
                try:parsed=source.strict(raw)
                except Exception:finding['body_shape']='unparseable_json'
                else:
                    finding['body_shape']=type(parsed).__name__
                    if isinstance(parsed,list):
                        finding['rows']=len(parsed);finding['row_types']=sorted({type(v).__name__ for v in parsed})
                        finding['first_row_fields']=sorted(parsed[0]) if parsed and isinstance(parsed[0],dict) else []
                    elif isinstance(parsed,dict):
                        finding['top_level_fields']=sorted(parsed)
                        finding['provider_diagnostic']={k:message(parsed[k]) for k in ('Error Message','message','error') if k in parsed}
            findings.append(finding)
        for path in protected:
            assert denied_with_retry('https://justhodl.ai/'+path)
            assert denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+path)
        r.kv(failed_request=batches.PARENT,failed_part=3,whole_failed_journal=original,whole_failed_report=report_ref,
            retained_successes=len(state['captures']),failures=findings,protected_artifacts_checked=len(protected),
            provider_requests=0,producer_invocations=0,consumer_invocations=0,public_writes=0,
            private_account_reads=0,signal_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
