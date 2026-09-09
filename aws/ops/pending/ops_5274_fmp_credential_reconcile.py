"""Recover FMP only from its existing Lambda or canonical managed credentials.

No new credential is created, no provider account is changed, and secret values
and provider bodies never appear in reports. Reject source/config drift.
"""
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from audit_20260909_fmp_credentials import validate
from audit_20260909_privacy_migration import ACCOUNT, REGION, error_code, require, update_environment
from release_package_evidence import check_packages


def main():
    require(os.environ.get('GITHUB_ACTIONS')=='true','runner_only')
    import boto3
    from botocore.config import Config
    clients={name:boto3.client(name,region_name=REGION,config=Config(connect_timeout=15,read_timeout=45,
             retries={'total_max_attempts':2})) for name in ('sts','lambda','ssm')}
    report={'ops':5274,'ok':False,'function':'fmp-fundamentals-agent','secret_values_reported':0,
            'provider_response_bodies_reported':0,'configuration_changed':False,
            'checkout_sha':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()}
    try:
        require(clients['sts'].get_caller_identity()['Account']==ACCOUNT,'wrong_account')
        package=check_packages(clients['lambda'],ROOT,[report['function']])[0]
        report['source_package']=package
        config=clients['lambda'].get_function_configuration(FunctionName=report['function'])
        env=config.get('Environment',{}).get('Variables',{})
        candidates=[('environment_FMP_API_KEY',env.get('FMP_API_KEY','')),('environment_FMP_KEY',env.get('FMP_KEY',''))]
        try:
            parameter=clients['ssm'].get_parameter(Name='/justhodl/fmp/api-key',WithDecryption=True)['Parameter']
            require(parameter.get('Type')=='SecureString','managed_parameter_not_secure')
            candidates.append(('managed_parameter',parameter.get('Value','')))
        except Exception as exc:
            if error_code(exc)!='ParameterNotFound':raise
            candidates.append(('managed_parameter',''))
        fetch=runpy.run_path(str(ROOT/'aws/lambdas/fmp-fundamentals-agent/source/lambda_function.py'))['fetch_rows']
        seen={};report['candidates']=[];selected=None
        for label,value in candidates:
            # Duplicate known values do not consume another provider request.
            result=seen.setdefault(value,validate(fetch,value)) if value not in seen else seen[value]
            report['candidates'].append({'source':label,**result})
            if selected is None and result['valid']:selected=(label,value)
        if selected:
            report['selected_source']=selected[0]
            # Public credential diagnostics may run despite configuration drift;
            # any mutation still requires exact source AND configuration parity.
            require(package['pass'],'source_package_mismatch')
            if env.get('FMP_API_KEY')!=selected[1]:
                report['configuration']=update_environment(clients['lambda'],report['function'],
                    {'FMP_API_KEY':selected[1]},package['code_sha256'])
                report['configuration_changed']=True
            report.update(ok=True,status='EXISTING_CREDENTIAL_VALIDATED')
        else:report['status']='PROVIDER_CREDENTIAL_REPLACEMENT_REQUIRED'
    except Exception as exc:
        report.update(status='RECONCILIATION_FAILED',error_type=type(exc).__name__)
    path=ROOT/'aws/ops/reports/5274_fmp_credential_reconciliation.json'
    path.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'ops':5274,'ok':report['ok'],'status':report['status'],
                      'configuration_changed':report['configuration_changed']}))
    return 0 if report['ok'] else 1


if __name__=='__main__':raise SystemExit(main())
