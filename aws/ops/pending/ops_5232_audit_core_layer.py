"""Publish sanitized core layer and move only its existing consumers, with managed FRED config.
Runs on the credentialed Actions runner. Never logs credentials or environment values.
"""
import base64
import ast
import re
import subprocess
import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
from ops_report import report


def run(rep):
    lam = boto3.client('lambda', region_name='us-east-1')
    ssm = boto3.client('ssm', region_name='us-east-1')
    # Preserve service for the final two independently found source credentials.
    for function, parameter, aliases in (
        ('fmp-fundamentals-agent', '/justhodl/fmp/api-key', ('FMP_API_KEY', 'FMP_KEY')),
        ('nasdaq-datalink-agent', '/justhodl/nasdaq-datalink/api-key', ('NASDAQ_API_KEY', 'NASDAQ_DATALINK_API_KEY', 'NASDAQ_DATALINK_KEY', 'QUANDL_API_KEY')),
    ):
        try:
            value = ssm.get_parameter(Name=parameter, WithDecryption=True)['Parameter']['Value']
        except ssm.exceptions.ParameterNotFound:
            baseline = (ROOT / 'aws/lambdas' / function / 'source/lambda_function.py').read_text()
            candidates = []
            for node in ast.walk(ast.parse(baseline)):
                if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'API_KEY' for t in node.targets):
                    candidates.extend(c.value for c in ast.walk(node.value) if isinstance(c, ast.Constant) and isinstance(c.value, str) and re.fullmatch('[A-Za-z0-9_-]{16,}', c.value) and not c.value.isupper())
            if len(candidates) != 1:
                raise RuntimeError('No unambiguous legacy provider configuration for ' + function)
            value = candidates[0]
            ssm.put_parameter(Name=parameter, Value=value, Type='SecureString', Overwrite=False,
                              Description='Managed provider credential; external rotation still required')
        current = lam.get_function_configuration(FunctionName=function)
        env = dict((current.get('Environment') or {}).get('Variables') or {})
        if not any(env.get(alias) for alias in aliases):
            env[aliases[0]] = value
            lam.update_function_configuration(FunctionName=function, RevisionId=current['RevisionId'], Environment={'Variables': env})
            lam.get_waiter('function_updated_v2').wait(FunctionName=function)
        rep.kv(function=function, managed_configuration=True, external_rotation='still required')
    live = []
    for page in lam.get_paginator('list_layers').paginate():
        live.extend(x for x in page.get('Layers', []) if x['LayerName'] == 'justhodl-core')
    if not live:
        rep.ok('No live justhodl-core layer exists; sanitized repository layer is ready for future use')
        return
    content = io.BytesIO()
    layer_root = ROOT / 'aws/layers/justhodl-core'
    with zipfile.ZipFile(content, 'w', zipfile.ZIP_DEFLATED) as archive:
        files = [(p.relative_to(layer_root).as_posix(), p) for p in sorted((layer_root / 'python').rglob('*.py')) if '__pycache__' not in p.parts]
        files.append(('python/managed_secret.py', ROOT / 'aws/shared/managed_secret.py'))
        for name, path in files:
            info = zipfile.ZipInfo(name, (2026, 9, 9, 0, 0, 0))
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    body = content.getvalue()
    expected = base64.b64encode(hashlib.sha256(body).digest()).decode()
    latest = live[0]['LatestMatchingVersion']
    current = lam.get_layer_version(LayerName='justhodl-core', VersionNumber=latest['Version'])
    if current['Content']['CodeSha256'] == expected:
        version_arn = current['LayerVersionArn']
    else:
        arguments = {'LayerName': 'justhodl-core', 'Description': 'Audit: managed FRED credential only; no source credential fallback', 'Content': {'ZipFile': body}}
        for key in ('CompatibleRuntimes', 'CompatibleArchitectures', 'LicenseInfo'):
            if current.get(key): arguments[key] = current[key]
        published = lam.publish_layer_version(**arguments)
        version_arn = published['LayerVersionArn']
        if published['Content']['CodeSha256'] != expected:
            raise RuntimeError('Layer package hash does not match reviewed bytes')
    fred = ssm.get_parameter(Name='/justhodl/fred/api-key', WithDecryption=True)['Parameter']['Value']
    targets = []
    prefix = live[0]['LayerArn'] + ':'
    for page in lam.get_paginator('list_functions').paginate():
        targets.extend(f['FunctionName'] for f in page.get('Functions', []) if any(x['Arn'].startswith(prefix) for x in f.get('Layers', [])))
    for function in targets:
        current = lam.get_function_configuration(FunctionName=function)
        layers = [version_arn if x['Arn'].startswith(prefix) else x['Arn'] for x in current.get('Layers', [])]
        env = dict((current.get('Environment') or {}).get('Variables') or {})
        if not (env.get('FRED_API_KEY') or env.get('FRED_KEY')):
            env['FRED_API_KEY'] = fred
        if layers != [x['Arn'] for x in current.get('Layers', [])] or env != ((current.get('Environment') or {}).get('Variables') or {}):
            lam.update_function_configuration(FunctionName=function, RevisionId=current['RevisionId'], Layers=layers, Environment={'Variables': env})
            lam.get_waiter('function_updated_v2').wait(FunctionName=function)
        final = lam.get_function_configuration(FunctionName=function)
        if version_arn not in [x['Arn'] for x in final.get('Layers', [])]:
            raise RuntimeError('Layer reference verification failed for ' + function)
        # Keep previously qualified scheduled runtimes on the same verified code/config.
        try:
            alias = lam.get_alias(FunctionName=function, Name='live')
        except lam.exceptions.ResourceNotFoundException:
            alias = None
        if alias:
            protected = lam.get_function_configuration(FunctionName=function, Qualifier='live')
            if protected['CodeSha256'] != final['CodeSha256']:
                raise RuntimeError('A different reviewed code version is live; layer promotion requires its release workflow: ' + function)
            version = lam.publish_version(FunctionName=function, RevisionId=final['RevisionId'], CodeSha256=final['CodeSha256'])
            lam.update_alias(FunctionName=function, Name='live', FunctionVersion=version['Version'], RevisionId=alias['RevisionId'], RoutingConfig={'AdditionalVersionWeights': {}})
        rep.kv(function=function, layer=version_arn, configured=True)
    rep.ok('Verified sanitized layer package and ' + str(len(targets)) + ' existing consumers; previous layer versions retained for rollback')


with report('ops_5232_audit_core_layer') as rep:
    rep.heading('Sanitized managed-credential core layer')
    try:
        run(rep)
    except Exception as exc:
        rep.fail('Layer migration did not complete: ' + type(exc).__name__)
        sys.exit(1)
