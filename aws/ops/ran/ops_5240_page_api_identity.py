"""Runner-only, read-only metadata proof of the two reviewed page API bindings.

No API payload requests, Lambda invokes, environment values, secrets, or AWS
mutations. Each metadata read and each function records its own result.
"""
from __future__ import annotations

import ast
import base64
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[3]
# Recheck the corrected Census page against the actual source-defined URL.
TARGETS = frozenset(('fmp-fundamentals-agent', 'fedliquidityapi'))
REPORT = ROOT / 'aws/ops/reports/latest/ops_5240_page_api_identity.json'
HOST = re.compile(r'^[a-z0-9]+\.lambda-url\.us-east-1\.on\.aws$')
ARN = re.compile(r'^arn:aws:lambda:us-east-1:\d{12}:function:([A-Za-z0-9_-]+)(?::[A-Za-z0-9_-]+)?$')


def reviewed_bindings(root=ROOT):
    """Read literal source mapping without importing the release verifier."""
    path = root / 'aws/ops/checks/audit_20260909_release.py'
    tree = ast.parse(path.read_text())
    values = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'FUNCTION_URL_BINDINGS' for t in node.targets):
            values = ast.literal_eval(node.value)
            break
    rows = [tuple(row) for row in values if len(row) == 3 and row[1] in TARGETS]
    if len(rows) != 2 or {row[1] for row in rows} != TARGETS:
        raise ValueError('The two reviewed API identity bindings must exist exactly once')
    for page, function, hostname in rows:
        if page not in ('fmp.html', 'census.html') or not HOST.fullmatch(hostname):
            raise ValueError('Page API binding is outside the reviewed metadata scope')
    return rows


def error_type(exc):
    value = type(exc).__name__
    return value if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', value) else 'MetadataReadError'


def safe_arn(value):
    return value if isinstance(value, str) and ARN.fullmatch(value) else None


def safe_hash(value):
    if not isinstance(value, str):
        return None
    try:
        return value if len(base64.b64decode(value, validate=True)) == 32 else None
    except Exception:
        return None


def inspect_identities(client, bindings, root=ROOT):
    rows = []
    for page, function, expected_host in bindings:
        row = {'page': page, 'function': function, 'expected_page_hostname': expected_host,
               'page_contains_expected_hostname': expected_host in (root / page).read_text(),
               'actual_function_url': None, 'actual_hostname': None, 'url_function_arn': None,
               'configuration_function_arn': None, 'code_sha256': None, 'state': None,
               'url_hostname_matches': None, 'metadata_calls_attempted': 0, 'errors': []}
        row['metadata_calls_attempted'] += 1
        try:
            url_config = client.get_function_url_config(FunctionName=function)
            parsed = urlsplit(url_config.get('FunctionUrl', ''))
            if parsed.scheme == 'https' and HOST.fullmatch(parsed.netloc) and parsed.path in ('', '/') and not parsed.query and not parsed.fragment:
                row['actual_function_url'] = 'https://' + parsed.netloc + '/'
                row['actual_hostname'] = parsed.netloc
                row['url_hostname_matches'] = parsed.netloc == expected_host
            else:
                row['errors'].append({'call': 'get_function_url_config', 'reason': 'INVALID_OR_MISSING_FUNCTION_URL'})
            row['url_function_arn'] = safe_arn(url_config.get('FunctionArn'))
        except Exception as exc:
            row['errors'].append({'call': 'get_function_url_config', 'error_type': error_type(exc)})
        row['metadata_calls_attempted'] += 1
        try:
            configuration = client.get_function_configuration(FunctionName=function)
            row['configuration_function_arn'] = safe_arn(configuration.get('FunctionArn'))
            row['code_sha256'] = safe_hash(configuration.get('CodeSha256'))
            state = configuration.get('State')
            row['state'] = state if state in ('Pending', 'Active', 'Inactive', 'Failed') else None
            # Deliberately project only these fields. Configuration can contain
            # environment secrets, descriptions, error details, and other text.
        except Exception as exc:
            row['errors'].append({'call': 'get_function_configuration', 'error_type': error_type(exc)})
        arns = (row['url_function_arn'], row['configuration_function_arn'])
        row['function_arn_matches'] = bool(all(arns) and arns[0] == arns[1] and ARN.fullmatch(arns[0]).group(1) == function)
        if row['url_hostname_matches'] is False:
            row['status'] = 'MISMATCH'
        elif (row['url_hostname_matches'] is True and row['page_contains_expected_hostname'] and row['function_arn_matches'] and row['code_sha256'] and row['state'] == 'Active' and not row['errors']):
            row['status'] = 'VERIFIED'
        else:
            row['status'] = 'UNPROVEN'
        rows.append(row)
    return rows


def main():
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise RuntimeError('This read-only metadata operation runs only in GitHub Actions')
    import boto3
    bindings = reviewed_bindings()
    rows = inspect_identities(boto3.client('lambda', region_name='us-east-1'), bindings)
    report = {'schema_version': 'page-api-identity-metadata.v1', 'operation': '5240',
              'observed_at': datetime.now(timezone.utc).isoformat(),
              'checkout_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'read_only': True, 'payload_requests': 0, 'lambda_invokes': 0,
              'binding_source': 'aws/ops/checks/audit_20260909_release.py:FUNCTION_URL_BINDINGS',
              'bindings': rows, 'all_verified': all(row['status'] == 'VERIFIED' for row in rows)}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, sort_keys=True))
    return report


if __name__ == '__main__':
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report as ops_report
    with ops_report('ops_5240_page_api_identity') as rep:
        try:
            result = main()
            for row in result['bindings']:
                rep.kv(page=row['page'], function=row['function'], status=row['status'], expected=row['expected_page_hostname'], actual=row['actual_hostname'])
            if not result['all_verified']:
                rep.fail('Both cases were recorded; at least one API identity is mismatched or unproven')
                sys.exit(1)
            rep.ok('Both page API bindings match active Lambda metadata; no payloads requested')
        except Exception as exc:
            rep.fail('Metadata operation failed: ' + error_type(exc))
            sys.exit(1)
