#!/usr/bin/env python3
"""Validate the exact snapshot ZIP with reviewed, invented offline regressions.

Only justhodl-portfolio-snapshot may use this path. No native invocation, private
source read or provider request occurs. Alias/code pins remain in the caller.
"""
from pathlib import Path
import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
FUNCTION = 'justhodl-portfolio-snapshot'
MODE = 'offline_snapshot_v1'
SCHEMA = 'audit-accounting-1.0'


def package_sources(root):
    source = root / 'aws/lambdas' / FUNCTION / 'source'
    expected = {p.name: p for p in (root/'aws/shared').glob('*.py')}
    # A clean runner has only tracked source. Include new reviewed source in
    # local candidate checks too, while excluding all ignored build products.
    paths = subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','--',source.relative_to(root).as_posix()], cwd=root, text=True).splitlines()
    for relative in paths:
        path = root / relative
        if path.is_file():expected[path.relative_to(source).as_posix()] = path
    if not {'lambda_function.py'} <= set(expected):
        raise ValueError('Complete reviewed snapshot source required')
    return expected


def verify_package(root, package):
    expected = package_sources(root)
    raw = package.read_bytes()
    if not raw or len(raw) > 64*1024*1024:
        raise ValueError('Candidate ZIP byte bound exceeded')
    with zipfile.ZipFile(package) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError('Duplicate candidate ZIP members')
        files = {n for n in names if not n.endswith('/')}
        if files != set(expected):
            raise ValueError('Candidate ZIP membership differs from reviewed source')
        for name, path in expected.items():
            if archive.read(name) != path.read_bytes():
                raise ValueError('Candidate source bytes differ: '+name)
    return {'bytes':len(raw), 'code_sha256':base64.b64encode(hashlib.sha256(raw).digest()).decode(), 'source_files_checked':len(expected)}


def validate(function, schema, config, package, root=ROOT):
    if function != FUNCTION or schema != SCHEMA or config.get('function_name') != FUNCTION or config.get('release_validation') != {'schema_version':SCHEMA, 'mode':MODE}:
        raise ValueError('Unreviewed offline validation configuration')
    proof = verify_package(root, package)
    bootstrap = '''import runpy,sys
def deny_network(event,args):
    if event in {"socket.connect","socket.getaddrinfo","socket.bind","subprocess.Popen","os.system"}:
        raise RuntimeError("Offline candidate tests cannot use networking or subprocesses")
sys.addaudithook(deny_network)
sys.path.insert(0,sys.argv[1])
runpy.run_path(sys.argv[1]+"/run_tests.py",run_name="__main__")
'''
    # No runner credentials, tokens or application/provider environment enter
    # the child. PYTHONPATH is retained only for installed offline dependencies.
    env = {k:v for k,v in os.environ.items() if k.upper() in {'PATH','SYSTEMROOT','WINDIR','TEMP','TMP','PYTHONPATH'}}
    env.update(PYTHONUTF8='1', PYTHONIOENCODING='utf-8', AWS_EC2_METADATA_DISABLED='true')
    result = subprocess.run([sys.executable,'-c',bootstrap,str(root/'aws/lambdas'/FUNCTION/'tests')],cwd=root,env=env,capture_output=True,text=True,encoding='utf-8',timeout=120)
    if result.returncode != 0:
        raise ValueError('Offline candidate regressions failed; no promotion permitted')
    if 'justhodl-portfolio-snapshot integration tests passed' not in result.stdout or 'Ran 52 tests' not in result.stderr or '\nOK\n' not in result.stderr:
        raise ValueError('Complete offline regression acknowledgement required')
    # Tests do not get to alter the artifact or source they just validated.
    if verify_package(root, package) != proof:
        raise ValueError('Candidate changed during offline validation')
    return {'ok':True,'validation_only':True,'schema_version':SCHEMA,'status':'OFFLINE_SYNTHETIC_TESTS_PASSED',
            'artifact_size_bytes':proof['bytes'],'validation_mode':MODE,'native_invocations':0,
            'actual_private_reads':0,'provider_requests':0,'normal_private_publication_verified':False,
            'tests':{'watchlist_and_reader':21,'holdings_accounting':15,'complete_book_read':16,'actual_handler_integration':4}, **proof}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--function',required=True);parser.add_argument('--schema',required=True);parser.add_argument('--config',type=Path,required=True);parser.add_argument('--zip',type=Path,required=True)
    args=parser.parse_args()
    try:print(json.dumps(validate(args.function,args.schema,json.loads(args.config.read_bytes()),args.zip),sort_keys=True))
    except Exception as error:raise SystemExit('Snapshot offline validation failed: '+str(error)) from None
