"""Native execution evidence cannot be confused with a runner qualification."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import Mock, patch
import importlib.util, sys, unittest

ROOT = Path(__file__).resolve().parents[1]
path = ROOT/'aws/ops/staged/ops_6154_share_flows_complete_normal_acceptance.py'
spec = importlib.util.spec_from_file_location('normal_share_evidence', path)
ops = importlib.util.module_from_spec(spec); spec.loader.exec_module(ops)
REF = {'manifest_key': 'retained-reference', 'output_sha256': 'same-output'}
WHEN = datetime(2026, 9, 26, 13, 40, tzinfo=timezone.utc)


def record():
    return {'execution_id': 'actual-lambda-request', 'status': 'complete',
        'started_at': '2026-09-26T13:35:01+00:00', 'finished_at': '2026-09-26T13:38:01+00:00',
        'result': {'published': True, 'replay': deepcopy(REF)}}


def check(records):
    client = Mock(); entries = [{'Key': ops.source.PRIVATE+'requests/'+str(i)+'.json', 'LastModified': WHEN}
                              for i in range(len(records))]
    values = {row['Key']: value for row, value in zip(entries, records)}
    client.get_paginator.return_value.paginate.return_value = [{'Contents': entries}]
    with patch.object(ops.producer, 'raw', side_effect=lambda s3, bucket, key: ops.source.encoded(values[key])):
        result = ops.native_execution(client, REF)
    client.get_paginator.assert_called_once_with('list_objects_v2')
    client.get_paginator.return_value.paginate.assert_called_once_with(Bucket=ops.BUCKET, Prefix=ops.source.PRIVATE+'requests/')
    client.put_object.assert_not_called()
    return result


class Tests(unittest.TestCase):
    def test_one_complete_normal_native_execution_supplies_measured_duration(self):
        self.assertEqual(check([record()])['elapsed_seconds'], 180)

    def test_runner_pending_failed_unchanged_or_wrong_reference_is_not_native_publication(self):
        changes = (lambda r:r.pop('execution_id'), lambda r:r.update(status='claimed'),
            lambda r:r.update(status='failed'), lambda r:r['result'].update(published=False),
            lambda r:r['result'].update(replay={}), lambda r:r.update(execution_id=''))
        for change in changes:
            value = record(); change(value)
            with self.assertRaisesRegex(ValueError, 'Exactly one'):check([value])

    def test_duplicate_old_or_impossible_runtime_cannot_receive_acceptance(self):
        with self.assertRaisesRegex(ValueError, 'Exactly one'):check([record(), record()])
        for start, finish in (('2026-09-26T13:34:00Z', '2026-09-26T13:38:00Z'),
                              ('2026-09-26T13:35:00Z', '2026-09-26T13:51:00Z'),
                              ('2026-09-26T13:35:00Z', '2026-09-26T13:34:00Z')):
            value = record(); value.update(started_at=start, finished_at=finish)
            with self.assertRaisesRegex(ValueError, 'clock or runtime'):check([value])


    def test_full_source_claim_population_does_not_hide_native_execution(self):
        old=datetime(2026,9,25,21,53,tzinfo=timezone.utc)
        entries=[{'Key':ops.source.PRIVATE+'requests/source-'+str(i)+'.json','LastModified':old} for i in range(11305)]
        target=ops.source.PRIVATE+'requests/native.json';entries.append({'Key':target,'LastModified':WHEN})
        client=Mock();client.get_paginator.return_value.paginate.return_value=[{'Contents':entries[i:i+1000]} for i in range(0,len(entries),1000)]
        with patch.object(ops.producer,'raw',return_value=ops.source.encoded(record())) as read:
            result=ops.native_execution(client,REF)
        read.assert_called_once_with(client,ops.BUCKET,target)
        self.assertEqual(result['elapsed_seconds'],180);client.put_object.assert_not_called()

    def test_inventory_and_recent_read_bounds_remain_explicit(self):
        client=Mock();old=datetime(2026,9,25,tzinfo=timezone.utc)
        client.get_paginator.return_value.paginate.return_value=[{'Contents':[{'Key':'old','LastModified':old}]*(ops.MAX_REQUESTS+1)}]
        with patch.object(ops.producer,'raw') as read,self.assertRaisesRegex(ValueError,'inventory bound'):
            ops.native_execution(client,REF)
        read.assert_not_called()
        client.get_paginator.return_value.paginate.return_value=[{'Contents':[{'Key':'new','LastModified':WHEN}]*(ops.MAX_RECENT_REQUESTS+1)}]
        with patch.object(ops.producer,'raw',return_value=b'{}') as read,self.assertRaisesRegex(ValueError,'Recent request'):
            ops.native_execution(client,REF)
        self.assertEqual(read.call_count,ops.MAX_RECENT_REQUESTS)

if __name__ == '__main__': unittest.main(verbosity=2)
