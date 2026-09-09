#!/usr/bin/env python3
"""Protect batch dependencies before any caller or producer code is replaced."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/shared"))
from governed_targets import GOVERNED_FUNCTIONS

CALLERS = {"justhodl-scheduler", "justhodl-event-coordinator", "justhodl-schedule-liveness", "justhodl-backend-agent"}
# This source-reviewed metadata service is introduced by the current release.
# Established production dependencies cannot opt themselves out of priming.
BOOTSTRAP_CONTRACTS = {"justhodl-public-archive-index": "public-engine-archive-index.v1"}
ESTABLISHED_DEPENDENCIES = GOVERNED_FUNCTIONS - BOOTSTRAP_CONTRACTS.keys()


def targets_to_prime(selected, configs):
    targets = set()
    names = set()
    for folder in selected:
        config = configs.get(folder, {})
        name = config.get("function_name", folder)
        names.add(name)
        if name in GOVERNED_FUNCTIONS or (config.get("release_validation") or {}).get("schema_version"):
            targets.add(name)
    if names & CALLERS:
        # A router update can immediately invoke any governed dependency, even
        # when that producer has no code change in this particular push.
        targets.update(GOVERNED_FUNCTIONS)
    if "justhodl-portfolio-admin" in names:
        targets.add("justhodl-portfolio-snapshot")
    return sorted(targets)


def bootstrap_eligible(function, selected, configs, root=ROOT):
    if function in ESTABLISHED_DEPENDENCIES or function not in BOOTSTRAP_CONTRACTS or function not in selected:
        return False
    config=configs.get(function,{})
    validation=config.get('release_validation') or {}
    source=root/'aws/lambdas'/function/'source/lambda_function.py'
    return (config.get('function_name',function)==function and config.get('handler')=='lambda_function.lambda_handler'
            and validation.get('bootstrap_if_absent') is True
            and validation.get('schema_version')==BOOTSTRAP_CONTRACTS[function] and source.is_file())


def prime_batch(selected, configs, clients, protect, root=ROOT):
    """Inventory all dependencies before mutation; only approved new code may wait."""
    lam=clients[0];targets=targets_to_prime(selected,configs);missing=set()
    for function in targets:
        try:
            lam.get_function_configuration(FunctionName=function)
        except Exception as exc:
            code=getattr(exc,'response',{}).get('Error',{}).get('Code')
            if code!='ResourceNotFoundException':
                raise RuntimeError('Production dependency existence could not be verified') from None
            if not bootstrap_eligible(function,selected,configs,root):
                raise RuntimeError('Required existing or unapproved production dependency is absent') from None
            missing.add(function)
    rows=[]
    for function in targets:
        if function in missing:
            rows.append({'function':function,'phase':'validated_bootstrap_pending','absence_verified':True,
                         'schema_version':BOOTSTRAP_CONTRACTS[function],
                         'requires':'create_then_pinned_validation_before_live_alias_and_schedule'})
        else:
            print(json.dumps({'phase':'protecting_production','function':function}),file=sys.stderr,flush=True)
            rows.append(protect(*clients,function))
    return rows


if __name__ == "__main__":
    import boto3
    from protect_lambda_alias import protect
    region, *selected = sys.argv[1:]
    configs = {}
    for target in selected:
        path = ROOT / "aws/lambdas" / target / "config.json"
        if path.exists():
            configs[target] = json.loads(path.read_text())
    clients = [boto3.client(service, region_name=region) for service in ("lambda", "scheduler", "events")]
    for row in prime_batch(selected, configs, clients, protect):
        print(json.dumps(row, sort_keys=True))
