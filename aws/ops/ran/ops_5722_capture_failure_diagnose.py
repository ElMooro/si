"""Diagnose bounded capture failure without exposing source payloads or credentials."""
import json
from pathlib import Path
import re
import sys
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops')]
from ops_report import report


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=920,retries={'max_attempts':0}))
    with report('ops_5722_capture_failure_diagnose') as r:
        response=lam.invoke(FunctionName='justhodl-signal-harvester',InvocationType='RequestResponse',Payload=b'{"capture_only":true,"suppress_alerts":true}')
        doc=json.loads(response['Payload'].read())
        frames=[]
        for row in doc.get('stackTrace',[]):
            match=re.search(r'File "([^"\n]+)", line (\d+), in ([a-zA-Z0-9_<>]+)',str(row))
            if match:frames.append({'file':Path(match[1]).name,'line':int(match[2]),'function':match[3]})
        known={'prospective clocks inconsistent','source byte identity invalid','immutable journal conflict',
               'unsupported journal or prospective protocol','source projection schema mismatch',
               'explicit direction required','unsupported instrument','source quality cannot register forecast',
               'forecast identity mismatch','collector identity missing','protocol was not registered before the forecast',
               'maximum recursion depth exceeded while calling a Python object'}
        message=doc.get('errorMessage')
        r.kv(function_error=response.get('FunctionError'),error_type=doc.get('errorType'),
             known_reason=message if message in known else 'unclassified message withheld',stack=frames,
             source_payloads_reported=False,legacy_ledger_writes=0)
        r.ok('Runtime diagnostic captured; this report is not a successful capture/deployment acceptance claim')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
