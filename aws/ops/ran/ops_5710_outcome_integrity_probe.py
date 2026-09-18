"""Read-only, bounded investigation of public-engine outcome price lineage."""
import json
import math
import sys
from collections import Counter
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Attr

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

TARGETS = ('eng:crypto-emergence', 'eng:compass-decisive-call', 'eng:dollar-decisive-call')


def number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def mark_projection(value):
    if not isinstance(value, dict):
        return None
    return {key: value.get(key) for key in ('price', 'as_of', 'provider', 'adjustment_basis', 'symbol', 'currency', 'instrument_id')}


def main():
    ddb = boto3.resource('dynamodb', region_name='us-east-1')
    table = ddb.Table('justhodl-outcomes')
    with report('ops_5710_outcome_integrity_probe') as r:
        r.heading('Outcome integrity: read-only price and baseline lineage')
        rows, scanned, pages, more = [], 0, 0, None
        while pages < 150:
            args = {'FilterExpression': Attr('signal_type').is_in(list(TARGETS)), 'Limit': 1500}
            if more:
                args['ExclusiveStartKey'] = more
            result = table.scan(**args)
            rows.extend(result.get('Items', []))
            scanned += result.get('ScannedCount', 0)
            pages += 1
            more = result.get('LastEvaluatedKey')
            if not more:
                break
        r.kv(scanned=scanned, matching=len(rows), pages=pages, complete_scan=not bool(more))
        assert rows, 'No affected outcomes found; diagnosis incomplete'
        signals = ddb.Table('justhodl-signals')
        for target in TARGETS:
            subset = [x for x in rows if x.get('signal_type') == target]
            reasons = Counter()
            for row in subset:
                oc = row.get('outcome') or {}
                reasons['endpoint_marks_present' if oc.get('marks') else 'endpoint_marks_absent'] += 1
                reasons['entry_mark_present' if oc.get('baseline_mark') else 'entry_mark_absent'] += 1
            r.log(target + ' ' + json.dumps(dict(reasons), sort_keys=True))
            def magnitude(row):
                oc = row.get('outcome') or {}
                a, b = number(oc.get('price_at_signal')), number(oc.get('price_at_check'))
                return abs(b / a - 1) if a and a > 0 and b is not None else -1
            for row in sorted(subset, key=magnitude, reverse=True)[:5]:
                oc = row.get('outcome') or {}
                signal = signals.get_item(Key={'signal_id': row['signal_id']}).get('Item') or {}
                sample = {k: row.get(k) for k in ('signal_type', 'window_key', 'logged_at', 'checked_at', 'predicted_dir')}
                sample['outcome'] = {k: oc.get(k) for k in ('price_at_signal', 'price_at_check', 'return_pct', 'excess_return', 'actual_direction', 'graded_at_session')}
                sample['outcome']['marks'] = {key: mark_projection(value) for key, value in (oc.get('marks') or {}).items() if key in ('asset', 'benchmark')}
                sample['signal_found'] = bool(signal)
                sample['signal'] = {k: signal.get(k) for k in ('ticker', 'measure_against', 'baseline_price', 'baseline_price_basis', 'baseline_benchmark_price', 'benchmark', 'signal_value')}
                sample['signal']['baseline_mark'] = mark_projection(signal.get('baseline_mark'))
                # Deliberately omit notes, metadata, personal portfolios and credentials.
                r.log(json.dumps(sample, default=str, sort_keys=True))
        r.ok('Read-only sample captured; no ledger records or production artifacts changed')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
