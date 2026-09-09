"""Public FMP HTTP diagnostics; no AWS SDK, credentials or response-body output."""
import json
import os
from pathlib import Path
import runpy
import subprocess
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]


def main():
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise RuntimeError('runner_only')
    filename = 'ops_5271_audit_page_api_probe.py'
    source = next(path for folder in ('pending', 'ran')
                  if (path := ROOT/'aws/ops'/folder/filename).is_file())
    # Loading definitions does not call main(), create clients or read credentials.
    probe = runpy.run_path(str(source))['probe']
    requests = [probe('/health'), probe('/')]
    report = {'operation':5272, 'read_only':True, 'aws_sdk_calls':0,
              'scope':'PUBLIC_HTTP_RESPONSE_AND_BROWSER_CORS_ONLY',
              'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
              'observed_at':datetime.now(timezone.utc).isoformat(),
              'requests':requests, 'raw_response_bodies_reported':0,
              'ok':all(row.get('status') == 200 and row.get('agent_matches') and row.get('cors_valid')
                       and not row.get('handler_error_present') and not row.get('trace_field_present')
                       for row in requests)}
    path = ROOT/'aws/ops/reports/5272_public_api_runtime.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
