"""Profile every retained July field without inventing replacements or fetching."""
from pathlib import Path
from collections import Counter
from datetime import date
import re, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6064_sec_reported_settlement_history as campaign
import sec_ftd_inventory as sec
base = campaign.base


def inspect(archive):
    member, body = sec.text_member(archive)
    lines, controls = sec.source_lines(body)
    counts, examples, rows, quantity = Counter(), {}, 0, 0
    widths, dates = Counter(), Counter()
    for number, line in enumerate(lines[1:-2], 2):
        if not line.strip():
            continue
        fields = line.split('|')
        widths[len(fields)] += 1
        rows += 1
        problems = []
        if len(fields) != 6:
            problems.append('column_count')
        else:
            stamp, cusip, symbol, amount, description, price = fields
            try:
                assert re.fullmatch('[0-9]{8}', stamp)
                dates[date.fromisoformat(stamp[:4] + '-' + stamp[4:6] + '-' + stamp[6:]).isoformat()] += 1
            except (AssertionError, ValueError):
                problems.append('settlement_format')
            if not re.fullmatch('[A-Z0-9*@#]{9}', cusip):
                problems.append('cusip_format')
            if not symbol:
                problems.append('symbol_empty')
            if len(symbol) > 10:
                problems.append('symbol_longer_than_ten')
            if not description:
                problems.append('description_empty')
            if len(description) > 30:
                problems.append('description_longer_than_thirty')
            if re.fullmatch('[0-9]+', amount):
                quantity += int(amount)
            else:
                problems.append('quantity_format')
            if price != '.' and not re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', price):
                problems.append('price_format')
        for label in problems:
            counts[label] += 1
            if len(examples.setdefault(label, [])) < 5:
                examples[label].append({'source_line': number, 'fields': fields if len(line) <= 500 else None,
                                        'line_characters': len(line)})
    return {'member': member, 'rows': rows, 'dates': dict(sorted(dates.items())),
            'column_widths': dict(widths), 'diagnostic_counts': dict(counts), 'diagnostic_examples': examples,
            'control_totals': controls, 'record_count_matches': rows == controls['reported_record_count'],
            'quantity_checksum_matches': quantity == int(controls['reported_quantity_sum']),
            'every_source_row_inspected': True, 'rows_discarded': False, 'source_fields_repaired': False,
            'source_schema_qualified': False}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    with report('ops_6065_sec_complete_row_diagnosis') as r:
        body = base.checked(s3, campaign.JULY_ORIGINAL)
        result = inspect(body)
        key = campaign.JULY_ORIGINAL['key']
        assert denied_with_retry('https://justhodl.ai/' + key)
        assert denied_with_retry('https://' + base.BUCKET + '.s3.amazonaws.com/' + key)
        r.kv(original=campaign.JULY_ORIGINAL, diagnosis=result, protected_artifacts_checked=1,
             provider_requests=0, engine_invocations=0, public_head_writes=0, private_account_reads=0,
             paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
