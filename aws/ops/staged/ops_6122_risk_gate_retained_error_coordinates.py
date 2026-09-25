"""Read already-retained failure coordinates, without re-running the engine."""
from pathlib import Path
import hashlib,json,re,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops'))
from ops_report import report
BUCKET='justhodl-dashboard-live'
REF={'key':'audit-private/20260909-originals/risk-gate-runtime/292eca6fc182c51b4926a9fd8f39a159af216221245dbf9c9c2d1d7b6a760631.bin','sha256':'292eca6fc182c51b4926a9fd8f39a159af216221245dbf9c9c2d1d7b6a760631','bytes':43556}


def coordinates(rows):
    errors={}
    for row in rows:
        text=row.get('message','')
        if '[ERROR]' not in text and 'Traceback' not in text:continue
        digest=hashlib.sha256(text.encode()).hexdigest()
        if digest in errors:errors[digest]['count']+=1;continue
        kind=re.search(r'\[ERROR\]\s+([A-Za-z][A-Za-z0-9_.]{0,80}):',text)
        frames=[{'module':module,'line':int(line),'function':function} for module,line,function in re.findall(
            r'File "/var/task/([A-Za-z0-9_/]{1,160}\.py)", line ([0-9]{1,7}), in ([A-Za-z_][A-Za-z0-9_]{0,100})',text)]
        errors[digest]={'message_sha256':digest,'error_class':kind.group(1) if kind else 'unclassified','frames':frames,'count':1}
    return list(errors.values())


def main():
    with report('ops_6122_risk_gate_retained_error_coordinates') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6120_risk_gate_refresh_diagnostic.md').read_text(encoding='utf-8')
        s3=boto3.client('s3',region_name='us-east-1')
        body=s3.get_object(Bucket=BUCKET,Key=REF['key'])['Body']
        try:raw=body.read(REF['bytes']+1)
        finally:body.close()
        assert len(raw)==REF['bytes'] and hashlib.sha256(raw).hexdigest()==REF['sha256']
        result=coordinates(json.loads(raw));assert result and any(e['frames'] for e in result),'Retained stack coordinates required'
        r.kv(retained_log_reference=REF,errors=result,raw_messages_published=False,native_invocations=0,
            provider_requests=0,private_account_reads=0,public_data_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
