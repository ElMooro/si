"""Audit every source-bound public route without fetching engine/customer bodies."""
import json
from pathlib import Path
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
from audit_20260909_public_pages import sweep
EXPECTED='fee895b944849ac6ae0d7d47359caaba1d653236'
result=sweep(ROOT,EXPECTED)
result.update(operation=5246,checker_sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip())
path=ROOT/'aws/ops/reports/5246_public_page_sweep.json';path.parent.mkdir(parents=True,exist_ok=True)
path.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:result[k] for k in ('operation','ok','routes_checked','verified_routes','failed_routes','scope')}))
raise SystemExit(0 if result['ok'] else 1)
