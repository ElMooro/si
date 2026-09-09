"""Read-only deployment verification for the accounting audit release.

No Lambda invokes, database writes or provider requests. Run after scheduled jobs
(or separately authorized invocations) have produced new outputs. A BLOCKED
capital/performance contract is an expected safe state, not an operational failure.
"""
import argparse
import hashlib
import io
import json
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ENGINES = (
    'portfolio-admin', 'portfolio-snapshot', 'calibration-snapshotter',
    'backtest-engine', 'research-backtest', 'outcome-checker', 'fleet-freshness-monitor',
)


def inspect_payload(key, doc):
    errors=[]
    if key == 'portfolio/snapshot.json':
        if doc.get('audit_version') != '2026-09-09.1': errors.append('old implementation output')
        for row in doc.get('positions', []):
            if row.get('valuation_status') != 'PRICED':
                if any(row.get(k) is not None for k in ('market_value','pnl_dollars','stop_hit')):
                    errors.append('unpriced valuation contract violation')
            elif row.get('mark_age_h') is None or not -0.0833 <= row['mark_age_h'] <= 120:
                errors.append('invalid priced mark age')
        book=doc.get('capital_book') or {}
        if book.get('status') != 'BLOCKED' or book.get('equity_nav') is not None or book.get('allows_new_entries') is not False:
            errors.append('unreconciled capital must be blocked')
    elif key == 'backtest/results.json':
        for name in ('summary','realistic_summary','honest_summary','walkforward_summary'):
            section=doc.get(name) or {}
            if section.get('headline_eligible') is not False or section.get('tradable_portfolio_nav') is not False:
                errors.append(name+' incorrectly publication eligible')
        if (doc.get('publication') or {}).get('status') != 'BLOCKED': errors.append('missing publication gate')
        if not isinstance(doc.get('historical_inputs'),dict): errors.append('missing vintage readiness contract')
        ledger=doc.get('portfolio_performance') or {}
        if ledger.get('schema_version') != 'research-capital-ledger-1.0' or ledger.get('adapter_implemented') is not True:
            errors.append('daily research ledger adapter contract absent')
        if ledger.get('publication_eligible') is not False:
            errors.append('research ledger cannot unlock performance publication')
        if ledger.get('status') == 'BLOCKED':
            if ledger.get('nav_curve') or ledger.get('daily_returns'):
                errors.append('blocked ledger exposes partial performance')
        elif ledger.get('status') == 'READY':
            if not (ledger.get('input_contract') or {}).get('hash_verified'):
                errors.append('ready ledger lacks verified immutable input')
            if not ledger.get('nav_curve') or any(row.get('reconciled') is not True for row in ledger['nav_curve']):
                errors.append('ready ledger contains unreconciled sessions')
        else:
            errors.append('unknown ledger adapter state')
    elif key == 'analytics/backtest_results.json':
        if doc.get('audit_version') != '2026-09-09.1': errors.append('old implementation output')
        calls=doc.get('per_call')
        if not isinstance(calls,list): errors.append('attribution builder did not produce an array')
    elif key == 'calibration/model-latest.json':
        if doc.get('audit_version') != '2026-09-09.1' or not doc.get('available_at') or not doc.get('snapshot_id'):
            errors.append('missing immutable model provenance')
    elif key == 'data/_freshness-monitor.json':
        if doc.get('version') != '2.1.0': errors.append('old freshness implementation')
        if doc.get('status') not in ('HEALTHY','DEGRADED','UNKNOWN'): errors.append('missing explicit health state')
        if doc.get('status') == 'HEALTHY' and (doc.get('n_unknown',0) or not doc.get('full_expected_coverage')):
            errors.append('healthy without verified expected coverage')
    return sorted(set(errors))


def run(repo, region='us-east-1', bucket='justhodl-dashboard-live'):
    import boto3
    lamb=boto3.client('lambda',region_name=region)
    s3=boto3.client('s3',region_name=region)
    report={'read_only':True,'checked_at':datetime.now(timezone.utc).isoformat(),'code':{},'outputs':{}}
    for engine in ENGINES:
        name='justhodl-'+engine
        source=repo/'aws/lambdas'/name/'source/lambda_function.py'
        config_path=source.parent.parent/'config.json'
        config=json.loads(config_path.read_text()) if config_path.exists() else {}
        try:
            deployed=lamb.get_function(FunctionName=name)
            with urllib.request.urlopen(deployed['Code']['Location'],timeout=30) as response:
                archive=zipfile.ZipFile(io.BytesIO(response.read()))
            actual=hashlib.sha256(archive.read('lambda_function.py')).hexdigest()
            expected=hashlib.sha256(source.read_bytes()).hexdigest()
            current=deployed['Configuration']
            mismatches=[]
            for local,remote in (('runtime','Runtime'),('handler','Handler'),('memory','MemorySize'),('timeout','Timeout'),('architectures','Architectures')):
                if local in config and config[local] != current.get(remote): mismatches.append(remote)
            report['code'][name]={'source_sha256_match':actual==expected,'expected_sha256':expected,'actual_sha256':actual,
                                  'configuration_mismatches':mismatches,'state':current.get('State'),'update_status':current.get('LastUpdateStatus')}
        except Exception as error:
            # Signed download URLs and environment values must not appear in reports.
            report['code'][name]={'error_type':type(error).__name__}
    for key,max_age in (('portfolio/snapshot.json',3),('backtest/results.json',8),('analytics/backtest_results.json',30),('calibration/model-latest.json',192),('data/_freshness-monitor.json',2)):
        try:
            response=s3.get_object(Bucket=bucket,Key=key)
            doc=json.loads(response['Body'].read())
            age=(datetime.now(timezone.utc)-response['LastModified']).total_seconds()/3600
            errors=inspect_payload(key,doc)
            if age<0 or age>max_age: errors.append('output outside expected publication interval')
            report['outputs'][key]={'age_hours':round(age,2),'contract_errors':errors,'pass':not errors}
        except Exception as error:
            report['outputs'][key]={'pass':False,'error_type':type(error).__name__}
    report['pass']=all(row.get('source_sha256_match') and not row.get('configuration_mismatches') and row.get('state')=='Active' and row.get('update_status')=='Successful' for row in report['code'].values()) and all(row.get('pass') for row in report['outputs'].values())
    report['remaining_prerequisites']=['Reconciled broker capital ledger with cash/liabilities/reserved orders and NAV history',
        'Immutable fills and daily marks/corporate actions/financing for genuine portfolio performance',
        'Baseline adjustment provenance for unresolved historical outcome grades']
    return report


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--repo',type=Path,default=Path(__file__).resolve().parents[3])
    args=parser.parse_args()
    result=run(args.repo)
    print(json.dumps(result,indent=2))
    raise SystemExit(0 if result['pass'] else 1)
