"""Grader projects desk grades only; deterministic and cloud-free."""
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import runpy

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / 'source/lambda_function.py'


class Store:
    def __init__(self):
        self.docs, self.puts = {}, []
    def get_object(self, Bucket, Key):
        if Key not in self.docs:
            raise KeyError(Key)
        import io
        return {'Body': io.BytesIO(json.dumps(self.docs[Key]).encode())}
    def put_object(self, **kw):
        self.puts.append(kw)
        self.docs[kw['Key']] = json.loads(kw['Body'])


def load(store):
    with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **k: store)}):
        return runpy.run_path(str(SRC))


def desk_row(day, grade='B', complete=True, **extra):
    trace = {'contract': 'auction-participation-inputs.v1', 'status': 'complete' if complete else 'unavailable', 'grade': grade if complete else None,
             'problems': [] if complete else ['cohort_size_outside_4_to_12']}
    return {'cusip': 'X%s' % day.replace('-', ''), 'auction_date': day, 'type': 'Note', 'term': '9-Year 10-Month', 'reopening': True,
            'instrument_kind': 'NOMINAL_COUPON', 'original_term': '10-Year', 'quality': {'cohort_term': '10-Year'},
            'grade': grade if complete else 'n/a', 'demand_score': 0.5 if complete else None, 'grading_inputs': trace,
            'btc': 2.5, 'indirect_pct': 70.0, 'pd_pct': 10.0, 'direct_pct': 20.0, 'total_accepted': 39e9, 'high_yield': 4.1,
            'z': {'btc': 0.5, 'indirect': 0.2, 'pd': -0.3}, 'trailing12': {'n': 12, 'btc': 2.4, 'indirect_pct': 68.0, 'pd_pct': 11.0},
            'behaviour': {'hit_ratio_pct': {'pd': 5.0}}, 'high_minus_median_bp': 4.5, 'read': {'headline': 'h', 'what_it_means': 'm'}, **extra}


def test_grader_mirrors_desk_letters_and_withholds_incomplete():
    store = Store()
    today = datetime.now(timezone.utc).date().isoformat()
    store.docs['data/auction-desk.json'] = {'generated_at': 'now', 'version': '1.6.0', 'auctions': [desk_row(today, 'A'), desk_row(today, 'F', complete=False)],
                                            'alerts': {'items': [{'id': 'dealer_share_plus_2sigma', 'severity': 'watch', 'cusip': 'X1', 'date': today, 'text': 'dealers <b>', 'type': 'Note'}]},
                                            'curve_map': {'rows': []}}
    env = load(store)
    out = env['lambda_handler']({}, None)
    body = json.loads(out['body'])
    doc = store.docs['data/auction-grades.json']
    letters = [g['overall_grade'] for g in doc['graded_auctions']]
    assert letters == ['A', 'n/a'], letters
    assert doc['graded_auctions'][0]['grade_letter'] == 'A' and doc['graded_auctions'][0]['grade_numeric'] == 4.0
    assert doc['graded_auctions'][1]['grade_numeric'] is None and 'withheld' in doc['graded_auctions'][1]['narrative']
    assert doc['summary']['n_withheld'] == 1 and doc['summary']['average_score'] == 4.0 and doc['summary']['overall_gpa_letter'] == 'A'
    assert doc['graded_auctions'][0]['tail_bps'] is None and doc['call'] is None and doc['sizing_eligible'] is False
    assert body['new_alerts'] == 1 and body['telegram_sent'] == 0
    # second run: same alert is not new
    env2 = load(store)
    body2 = json.loads(env2['lambda_handler']({}, None)['body'])
    assert body2['new_alerts'] == 0


def test_tampered_letter_without_matching_trace_is_withheld():
    store = Store()
    env = load(store)
    row = desk_row('2026-09-30', 'A')
    row['grade'] = 'F'  # letter disagrees with the retained trace
    assert env['desk_grade'](row) is None
    row = desk_row('2026-09-30', 'A'); row['grading_inputs']['contract'] = 'other'
    assert env['desk_grade'](row) is None


def test_desk_unavailable_leaves_prior_output():
    store = Store()
    store.docs['data/auction-grades.json'] = {'marker': 1}
    env = load(store)
    out = json.loads(env['lambda_handler']({}, None)['body'])
    assert out['ok'] is False and store.docs['data/auction-grades.json'] == {'marker': 1}


def test_alert_messages_escape_html():
    env = load(Store())
    output = {'alerts': {'items': [{'id': 'wide_stop_dispersion', 'severity': 'watch', 'cusip': 'c', 'date': '2026-10-01', 'text': '<script>x</script>'}]}}
    messages, keys = env['new_alert_messages'](output, {})
    assert '&lt;script&gt;' in messages[0] and '<script>' not in messages[0] and keys == ['wide_stop_dispersion|c|2026-10-01']


if __name__ == '__main__':
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn(); n += 1
    print('Auction grader tests passed: %d' % n)
