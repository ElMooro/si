"""Read retained 6057 public-source bytes; no provider request or engine invoke."""
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6057_squeeze_settlement_source_baseline as base
import sec_ftd_inventory as sec
ORIGINAL = {'key': base.PRIVATE + '0c90519d0d0e959b86331c5fe546cefa027c8a17998111270001b0ee61bbf203.bin',
            'sha256': '0c90519d0d0e959b86331c5fe546cefa027c8a17998111270001b0ee61bbf203', 'bytes': 1393881}


def inspect(archive):
    member, body = sec.text_member(archive)
    lines = body.decode('utf-8-sig', errors='strict').splitlines()
    widths = Counter()
    anomalies, last_rows = [], []
    for n, line in enumerate(lines, 1):
        fields = line.split('|')
        widths[len(fields)] += 1
        sample = {'source_line': n, 'field_count': len(fields), 'line_characters': len(line)}
        if len(line) <= 500:
            sample['fields'] = fields
        if n > 1 and line.strip() and len(fields) != len(sec.FIELDS):
            if len(anomalies) < 8:
                anomalies.append(sample)
        last_rows.append(sample)
        last_rows = last_rows[-5:]
    return {'member': member, 'line_count': len(lines), 'field_count_distribution': dict(sorted(widths.items())),
            'first_header': lines[0].split('|'), 'sampled_non_schema_lines': anomalies, 'last_five_lines': last_rows,
            'archive_bytes_retained_whole': True, 'source_schema_qualified': False}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    with report('ops_6059_sec_archive_row_diagnosis') as r:
        failed = base.raw.strict(base.read(s3, base.STATUS))
        assert failed['status'] == 'failed' and failed['request_id'] == base.REQUEST
        assert failed['captures']['1']['original'] == ORIGINAL
        archive = base.checked(s3, ORIGINAL)
        result = inspect(archive)
        paths = [base.STATUS, ORIGINAL['key']]
        def denied(key):
            assert denied_with_retry('https://justhodl.ai/' + key)
            assert denied_with_retry('https://' + base.BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=2) as pool:
            for _ in pool.map(denied, paths):
                pass
        r.kv(original=ORIGINAL, diagnosis=result, protected_artifacts_checked=len(paths),
             provider_requests=0, engine_invocations=0, public_head_writes=0, private_account_reads=0,
             paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
