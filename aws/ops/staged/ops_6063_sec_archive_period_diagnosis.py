"""Inspect the retained July archive that failed 6062; no source recollection."""
from pathlib import Path
from datetime import date
from collections import Counter
import sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6062_sec_advertised_archive_history as campaign
import sec_ftd_inventory as sec
base, raw = campaign.base, campaign.raw
URL = 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202607b.zip'


def inspect(archive, url):
    member, body = sec.text_member(archive)
    lines, controls = sec.source_lines(body)
    year, month, half = sec.archive_period(url)
    dates, outside, examples, quantities, rows = Counter(), Counter(), [], 0, 0
    first = last = None
    for line_number, line in enumerate(lines[1:-2], 2):
        if not line.strip():
            continue
        fields = line.split('|')
        assert len(fields) == 6, 'Unexpected row schema remains unqualified'
        stamp = fields[0]
        assert len(stamp) == 8 and stamp.isascii() and stamp.isdigit()
        day = date.fromisoformat(stamp[:4] + '-' + stamp[4:6] + '-' + stamp[6:])
        assert fields[3].isascii() and fields[3].isdigit()
        quantities += int(fields[3])
        rows += 1
        dates[day.isoformat()] += 1
        sample = {'source_line': line_number, 'fields': fields}
        first = sample if first is None else first
        last = sample
        if day.year != year or day.month != month or ('a' if day.day <= 15 else 'b') != half:
            outside[day.isoformat()] += 1
            if len(examples) < 5:
                examples.append(sample)
    return {'member': member, 'archive_url': url, 'advertised_period': [year, month, half], 'rows': rows,
            'dates': dict(sorted(dates.items())), 'outside_advertised_period': dict(sorted(outside.items())),
            'first_data_row': first, 'last_data_row': last, 'outside_examples': examples, 'control_totals': controls,
            'record_count_matches': rows == controls['reported_record_count'],
            'quantity_checksum_matches': quantities == int(controls['reported_quantity_sum']),
            'source_period_qualified': False, 'source_rows_relabelled': False}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    with report('ops_6063_sec_archive_period_diagnosis') as r:
        state_key, request_key = campaign.key('campaign'), campaign.key('source:' + URL)
        state = raw.strict(base.read(s3, state_key))
        failed = raw.strict(base.read(s3, request_key))
        assert state['status'] == failed['status'] == 'failed'
        assert state['request_id'] == failed['request_id'] == campaign.REQUEST
        cap = failed['capture']
        assert cap['url'] == URL and cap['http_status'] == 200 and cap['status'] == 'response_retained'
        archive = base.checked(s3, cap['original'])
        result = inspect(archive, URL)
        for key in (state_key, request_key, cap['original']['key']):
            assert denied_with_retry('https://justhodl.ai/' + key)
            assert denied_with_retry('https://' + base.BUCKET + '.s3.amazonaws.com/' + key)
        r.kv(original=cap['original'], captured_at=cap['received_at'], diagnosis=result, protected_artifacts_checked=3,
             provider_requests=0, engine_invocations=0, public_head_writes=0, private_account_reads=0,
             paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
