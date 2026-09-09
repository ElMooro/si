#!/usr/bin/env python3
"""Verify an explicitly anchored source release, then refresh reviewed quiet engines.

Default is the reviewed final source anchor. Success requires the actual parity and publication gates.
For a later release set AUDIT_RELEASE_SHA and AUDIT_RELEASE_STAGE=final (or use
--release / --stage). Code mismatch aborts before any AWS mutation. Notification
engines await their ordinary schedules; no messages or orders are submitted.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from audit_20260909_release import BASE, REGION, ReleaseVerifier


def command(*args):
    return subprocess.check_output(['git',*args],cwd=ROOT,text=True,stderr=subprocess.DEVNULL).strip()


def require_commit(value):
    # Only a hexadecimal commit identifier, never a refspec/option or shell text.
    import re
    if not re.fullmatch(r'[0-9a-fA-F]{7,40}',value):raise ValueError('explicit_commit_sha_required')
    try:return command('rev-parse','--verify',value+'^{commit}')
    except subprocess.CalledProcessError:
        command('fetch','--no-tags','--depth=1','origin',value)
        return command('rev-parse','--verify',value+'^{commit}')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release',default=os.environ.get('AUDIT_RELEASE_SHA','cd425e4705c0b284b19f7732dd0e4d44099ac4fd'))
    parser.add_argument('--stage',choices=('preliminary','final'),default=os.environ.get('AUDIT_RELEASE_STAGE','final'))
    parser.add_argument('--parity-minutes',type=int,default=5)
    parser.add_argument('--publication-minutes',type=int,default=35)
    args=parser.parse_args()
    report={'ops':5231,'ok':False,'status':'INITIALIZATION_FAILED','verification_stage':args.stage,
            'completion_scope':'PRELIMINARY_SOURCE_RELEASE' if args.stage=='preliminary' else 'EXPLICIT_FINAL_RELEASE',
            'private_payloads_reported':0,'raw_lambda_responses_reported':0}
    stage_path=None;verifier=None
    try:
        release=require_commit(args.release);require_commit(BASE)
        report.update(expected_release_sha=release,checker_checkout_sha=command('rev-parse','HEAD'),ops_target_sha=os.environ.get('OPS_TARGET_SHA'))
        with tempfile.TemporaryDirectory(prefix='justhodl-5231-') as temp:
            stage_path=Path(temp)/'release'
            command('worktree','add','--detach',str(stage_path),release)
            # Expected member bytes/config always come from this fixed commit,
            # even though run-ops refreshes its executable checkout to latest main.
            import boto3
            from botocore.config import Config
            clients={name:boto3.client(name,region_name=REGION,config=Config(connect_timeout=15,
                read_timeout=950 if name=='lambda' else 45,tcp_keepalive=True,retries={'total_max_attempts':1} if name=='lambda' else {'max_attempts':2})) for name in ('lambda','s3','scheduler','events')}
            privacy_path=ROOT/'aws/ops/reports/5230_audit_privacy_migration.json'
            privacy=json.loads(privacy_path.read_text()) if privacy_path.exists() else None
            verifier=ReleaseVerifier(stage_path,clients,parity_seconds=max(0,args.parity_minutes)*60,
                                     publication_seconds=max(0,args.publication_minutes)*60,privacy_receipt=privacy)
            # Checkpoints must survive temporary-worktree cleanup and workflow
            # auto-commit. Never persist captured private payloads.
            destination=ROOT/'aws/ops/reports/5231_audit_release_verify.json'
            def checkpoint():
                destination.parent.mkdir(parents=True,exist_ok=True)
                destination.write_text(json.dumps({**report,**verifier.report},indent=2,allow_nan=False)+'\n')
            verifier.checkpoint=checkpoint
            try:report.update(verifier.run())
            finally:command('worktree','remove','--force',str(stage_path));stage_path=None
    except Exception as exc:
        if verifier is not None:report.update(verifier.report)
        report.update(ok=False,status='OPERATION_FAILED',error_type=type(exc).__name__)
        # Error strings may contain signed URLs, request payloads, or private data.
    finally:
        if stage_path is not None:
            try:command('worktree','remove','--force',str(stage_path))
            except Exception:pass
        destination=ROOT/'aws/ops/reports/5231_audit_release_verify.json'
        destination.parent.mkdir(parents=True,exist_ok=True)
        destination.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    print(json.dumps({'ops':5231,'ok':report['ok'],'status':report['status'],
                      'stage':args.stage,'code_engines':len(report.get('code',{})),
                      'pending_outputs':len(report.get('pending_outputs',[])),
                      'contract_failures':len(report.get('contract_failures',[])),
                      'report':'aws/ops/reports/5231_audit_release_verify.json'}))
    return 0 if report['ok'] else 1


if __name__=='__main__':raise SystemExit(main())
