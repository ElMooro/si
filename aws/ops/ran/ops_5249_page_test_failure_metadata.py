"""Read filtered GitHub test-failure locations; no cloud or application calls."""
import ast
import json
import os
from pathlib import Path
import re
import urllib.request
import urllib.error

ROOT=Path(__file__).resolve().parents[3]
if os.environ.get('GITHUB_REPOSITORY')!='ElMooro/si':raise RuntimeError('Unexpected repository')
os.environ['GH_TOKEN']=os.environ['GH_API_TOKEN']
source=ROOT/'scripts/audit_release_progress.py'
functions=[node for node in ast.parse(source.read_text()).body
           if isinstance(node,ast.FunctionDef) and node.name in ('failure_sections','diagnostics')]
assert len(functions)==2
namespace={**globals(),'repo':'ElMooro/si'}
exec(compile(ast.Module(body=functions,type_ignores=[]),str(source),'exec'),namespace)
result={'scope':'ALLOWLISTED_GITHUB_FAILURE_LOCATIONS_ONLY','raw_logs_reported':0,
        'run_id':34353027745,'job_id':102470700134,
        'diagnostics':namespace['diagnostics'](102470700134)}
path=ROOT/'aws/ops/reports/5249_page_test_failure_metadata.json'
path.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'report':str(path.relative_to(ROOT)),'available':result['diagnostics'].get('available')}))
raise SystemExit(0 if result['diagnostics'].get('available') else 1)
