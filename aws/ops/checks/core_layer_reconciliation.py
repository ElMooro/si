"""Conservative current-state reconciliation of a sanitized core layer.

Clients are injected. Reports contain identities, hashes and field names only,
never environment values, code archives or secrets. Historical safety cannot be
inferred from current parity without a pre-migration configuration snapshot.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import io
import zipfile

FRED_KEYS = {'FRED_API_KEY', 'FRED_KEY'}
RESPONSE_METADATA = {'ResponseMetadata', 'FunctionName', 'FunctionArn', 'Version', 'RevisionId', 'LastModified',
                     'LastUpdateStatus', 'LastUpdateStatusReason', 'LastUpdateStatusReasonCode',
                     'State', 'StateReason', 'StateReasonCode', 'CodeSha256', 'CodeSize'}


def layer_package(root):
    folder = root / 'aws/layers/justhodl-core'
    files = [(path.relative_to(folder).as_posix(), path) for path in sorted((folder / 'python').rglob('*.py'))
             if '__pycache__' not in path.parts]
    files.append(('python/managed_secret.py', root / 'aws/shared/managed_secret.py'))
    if len(files) < 2 or len({name for name, _ in files}) != len(files):
        raise ValueError('Core source package unavailable or duplicate member')
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, path in files:
            info = zipfile.ZipInfo(name, (2026, 9, 9, 0, 0, 0))
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    body = output.getvalue()
    return body, base64.b64encode(hashlib.sha256(body).digest()).decode()


def core_layers(config, prefix):
    return [row['Arn'] for row in config.get('Layers', []) if row['Arn'].startswith(prefix)]


def controlled_config(config):
    """Compare all response fields except explicit response metadata/intended deltas."""
    result = {key: copy.deepcopy(value) for key, value in config.items()
              if key not in RESPONSE_METADATA | {'Layers'}}
    environment = result.setdefault('Environment', {})
    variables = environment.setdefault('Variables', {})
    for key in FRED_KEYS:
        variables.pop(key, None)
    return result


def drift_fields(current, protected, prefix):
    left, right = controlled_config(current), controlled_config(protected)
    result = sorted(key for key in set(left) | set(right) if left.get(key) != right.get(key))
    if current.get('CodeSha256') != protected.get('CodeSha256'):
        result.append('CodeSha256')
    a = [x['Arn'] for x in current.get('Layers', []) if not x['Arn'].startswith(prefix)]
    b = [x['Arn'] for x in protected.get('Layers', []) if not x['Arn'].startswith(prefix)]
    if a != b:
        result.append('Layers.non_core')
    return result


def discover(lam, prefix):
    """Inspect live traffic as well as $LATEST; old-layer live-only states remain visible."""
    for page in lam.get_paginator('list_functions').paginate():
        for summary in page.get('Functions', []):
            name = summary['FunctionName']
            try:
                alias = lam.get_alias(FunctionName=name, Name='live')
            except lam.exceptions.ResourceNotFoundException:
                alias = None
            live_versions = {}
            if alias:
                versions = {alias['FunctionVersion']} | set(alias.get('RoutingConfig', {}).get('AdditionalVersionWeights', {}))
                for version in versions:
                    live_versions[version] = lam.get_function_configuration(FunctionName=name, Qualifier=version)
            if not core_layers(summary, prefix) and not any(core_layers(config, prefix) for config in live_versions.values()):
                continue
            current = lam.get_function_configuration(FunctionName=name)
            yield name, current, alias, live_versions


def reconcile_consumer(lam, name, current, alias, live_versions, prefix, desired_layer, fred):
    result = {'function': name, 'latest_layers_before': core_layers(current, prefix),
              'live_layers_before': {version: core_layers(config, prefix) for version, config in live_versions.items()},
              'status': 'BLOCKED', 'alias_promoted': False, 'configuration_changed': False}
    if not isinstance(fred, str) or not fred:
        result['reason'] = 'MANAGED_FRED_CONFIGURATION_UNAVAILABLE'; return result
    if current.get('State') != 'Active' or current.get('LastUpdateStatus') != 'Successful':
        result['reason'] = 'LATEST_NOT_STABLE'; return result
    protected = live_versions.get(alias['FunctionVersion']) if alias else None
    if alias and protected is None:
        result['reason'] = 'LIVE_CONFIGURATION_UNAVAILABLE'; return result
    if alias and alias.get('RoutingConfig', {}).get('AdditionalVersionWeights'):
        result['reason'] = 'WEIGHTED_LIVE_ALIAS_REQUIRES_REVIEW'; return result
    if protected is not None:
        differences = drift_fields(current, protected, prefix)
        if differences:
            result.update(reason='UNEXPLAINED_CONFIGURATION_DRIFT', differing_fields=differences); return result
        if not core_layers(current, prefix):
            # Restoring a removed layer onto $LATEST would guess at the purpose
            # of that staged configuration. Preserve it and report the live gap.
            result['reason'] = 'ALIAS_ONLY_LAYER_REQUIRES_EXPLICIT_RELEASE'; return result
    reference = protected if protected is not None else current
    if len(core_layers(reference, prefix)) != 1 or len(core_layers(current, prefix)) != 1:
        result['reason'] = 'AMBIGUOUS_CORE_LAYER_BINDING'; return result
    layers = [desired_layer if row['Arn'].startswith(prefix) else row['Arn'] for row in reference.get('Layers', [])]
    environment = copy.deepcopy(current.get('Environment', {}))
    if environment.get('Error'):
        result['reason'] = 'ENVIRONMENT_UNAVAILABLE'; return result
    variables = dict(environment.get('Variables') or {})
    variables['FRED_API_KEY'] = fred
    if 'FRED_KEY' in variables:
        variables['FRED_KEY'] = fred
    desired_env = {'Variables': variables}
    if layers != [row['Arn'] for row in current.get('Layers', [])] or desired_env != environment:
        lam.update_function_configuration(FunctionName=name, RevisionId=current['RevisionId'], Layers=layers, Environment=desired_env)
        result['configuration_changed'] = True
        lam.get_waiter('function_updated_v2').wait(FunctionName=name, WaiterConfig={'Delay': 1, 'MaxAttempts': 300})
    final = lam.get_function_configuration(FunctionName=name)
    if final.get('State') != 'Active' or final.get('LastUpdateStatus') != 'Successful':
        result['reason'] = 'POST_UPDATE_CONFIGURATION_NOT_STABLE'; return result
    if drift_fields(final, reference, prefix) or [row['Arn'] for row in final.get('Layers', [])] != layers or final.get('Environment') != desired_env:
        result['reason'] = 'POST_UPDATE_CONFIGURATION_PARITY_FAILED'; return result
    if alias:
        already_current = ([row['Arn'] for row in protected.get('Layers', [])] == layers and protected.get('Environment') == desired_env)
        if not already_current:
            published = lam.publish_version(FunctionName=name, RevisionId=final['RevisionId'], CodeSha256=final['CodeSha256'])
            version = published['Version']
            if not str(version).isdigit() or int(version) < 1:
                result['reason'] = 'UNNUMBERED_CANDIDATE'; return result
            candidate = lam.get_function_configuration(FunctionName=name, Qualifier=version)
            if candidate.get('State') != 'Active' or candidate.get('LastUpdateStatus') not in (None, 'Successful'):
                result['reason'] = 'PUBLISHED_CONFIGURATION_NOT_STABLE'; return result
            if drift_fields(candidate, reference, prefix) or [row['Arn'] for row in candidate.get('Layers', [])] != layers or candidate.get('Environment') != desired_env:
                result['reason'] = 'PUBLISHED_CONFIGURATION_PARITY_FAILED'; return result
            lam.update_alias(FunctionName=name, Name='live', FunctionVersion=version, RevisionId=alias['RevisionId'])
            result.update(alias_promoted=True, live_version=version)
        else:
            result['live_version'] = alias['FunctionVersion']
        observed_alias = lam.get_alias(FunctionName=name, Name='live')
        if observed_alias['FunctionVersion'] != result['live_version'] or observed_alias.get('RoutingConfig', {}).get('AdditionalVersionWeights'):
            result['reason'] = 'LIVE_ALIAS_CHANGED_DURING_RECONCILIATION'; return result
    result.update(status='VERIFIED_CURRENT_STATE', desired_layer=desired_layer, code_sha256=final['CodeSha256'],
                  non_intended_configuration_parity=True)
    return result


def reconcile(lam, ssm, root, checkpoint=lambda report: None):
    report = {'ops': 5234, 'ok': False, 'scope': 'CURRENT_LATEST_AND_LIVE_CORE_LAYER_BINDINGS', 'consumers': [],
              'prior_5232_configuration_safety': 'UNVERIFIABLE_WITHOUT_PREMIGRATION_SNAPSHOT',
              'historical_limitation_is_observed_error': False, 'secret_values_reported': 0}
    layers = [row for page in lam.get_paginator('list_layers').paginate() for row in page.get('Layers', []) if row['LayerName'] == 'justhodl-core']
    if len(layers) != 1:
        report.update(status='BLOCKED_LAYER_INVENTORY_UNAVAILABLE'); checkpoint(report); return report
    body, expected = layer_package(root)
    latest = lam.get_layer_version(LayerName='justhodl-core', VersionNumber=layers[0]['LatestMatchingVersion']['Version'])
    # This follow-up never guesses which layer to publish. Require ops5232's
    # available package to match the current reviewed sanitized source exactly.
    if latest['Content']['CodeSha256'] != expected:
        report.update(status='BLOCKED_REVIEWED_LAYER_PACKAGE_UNAVAILABLE', expected_layer_sha256=expected); checkpoint(report); return report
    desired = latest['LayerVersionArn']; prefix = layers[0]['LayerArn'] + ':'
    if not desired.startswith(prefix):
        report.update(status='BLOCKED_LAYER_IDENTITY_MISMATCH'); checkpoint(report); return report
    fred = ssm.get_parameter(Name='/justhodl/fred/api-key', WithDecryption=True)['Parameter']['Value']
    report.update(desired_layer=desired, expected_layer_sha256=expected)
    # Complete discovery before the first mutation. Failure to inspect a live
    # alias is an incomplete inventory, never evidence of zero consumers.
    try:
        consumers = list(discover(lam, prefix))
    except Exception as error:
        report.update(status='BLOCKED_INCOMPLETE_DISCOVERY', error_type=type(error).__name__)
        checkpoint(report)
        return report
    for name, current, alias, live_versions in consumers:
        try:
            row = reconcile_consumer(lam, name, current, alias, live_versions, prefix, desired, fred)
        except Exception as error:
            row = {'function': name, 'status': 'ERROR', 'error_type': type(error).__name__,
                   'reason': 'CURRENT_STATE_REQUIRES_RECONCILIATION'}
        report['consumers'].append(row); checkpoint(report)
    report['unresolved_count'] = sum(row['status'] != 'VERIFIED_CURRENT_STATE' for row in report['consumers'])
    report['consumer_count'] = len(report['consumers'])
    report['ok'] = report['unresolved_count'] == 0
    report['status'] = 'VERIFIED_CURRENT_STATE' if report['ok'] else 'PARTIAL_CURRENT_STATE'
    checkpoint(report)
    return report
