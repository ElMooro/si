"""Verify public HTML against the exact successful Pages deployment commit."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
from audit_20260909_public_pages import sweep


def main():
    expected=os.environ.get('AUDIT_PAGES_DEPLOYED_SHA','')
    run=os.environ.get('AUDIT_PAGES_DEPLOYMENT_RUN','')
    if os.environ.get('GITHUB_ACTIONS')!='true' or not re.fullmatch('[a-f0-9]{40}',expected) or not run.isdigit():
        raise RuntimeError('explicit_successful_pages_deployment_required')
    report=sweep(ROOT,expected)
    report.update(operation=5275,deployment_run_id=int(run),aws_sdk_calls=0,
                  check_finished_at=datetime.now(timezone.utc).isoformat())
    path=ROOT/'aws/ops/reports/5275_public_page_sweep.json'
    path.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:report[key] for key in ('ok','routes_checked','verified_routes','failed_routes','expected_product_release')}))


if __name__=='__main__':main()
