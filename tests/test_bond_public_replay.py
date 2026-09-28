"""Publication replay regressions use only synthetic, isolated storage."""
import copy
import io
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import replay_bond_research as replay
pub = replay.pub


class Memory:
    def __init__(self):
        self.data = {}
        self.writes = []

    def get_object(self, *, Bucket, Key):
        return {'Body': io.BytesIO(self.data[Key]), 'ETag': pub.sha(self.data[Key])}

    def put_object(self, *, Bucket, Key, Body, **kwargs):
        self.writes.append(Key)
        self.data[Key] = Body


def fixture():
    client = Memory()
    client.data[pub.CURRENT] = b'{ "generated_at":"2026-09-25T15:16:00Z", "legacy":true }'
    client.data[pub.HISTORY] = b'{"2026-09-25":{"anxiety":0,"appetite":0,"eqbond":null,"additional":"preserved"}}'
    snap = pub.begin(client, 'public', '2026-09-28T15:15:00Z')
    rows = copy.deepcopy(snap['history']['doc'])
    rows['2026-09-28'] = {'anxiety': None, 'appetite': None, 'eqbond': None}
    packet = {'generated_at': '2026-09-28T15:16:00Z', 'version': 'synthetic',
              'world_anxiety': None, 'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
              'decision': {'verb': 'WAIT', 'meaning': 'abstain'},
              'anxiety_history': [{'date': k, 'value': v['anxiety']} for k, v in sorted(rows.items())]}
    packet = pub.publish(client, 'public', snap, packet, rows)
    client.writes.clear()
    return client, packet


def reseal(client, packet, change):
    """A hash-consistent but semantically false attempt must still fail."""
    key = packet['publication_record']['manifest_key']
    manifest = pub.strict(client.data[key])
    change(manifest)
    raw = pub.encode(manifest)
    key = pub.PREFIX + 'attempts/' + pub.sha(raw) + '.json'
    client.data[key] = raw
    packet['publication_record']['manifest_key'] = key


class Tests(unittest.TestCase):
    def test_original_bytes_history_and_permissions_pass_without_writes(self):
        client, packet = fixture()
        result = replay.publication(packet, client)
        self.assertEqual(result['history_rows'], 2)
        self.assertEqual(result['previous_dates_unchanged'], 1)
        self.assertIs(result['snapshot_atomic'], False)
        self.assertIs(result['original_source_replayed_here'], False)
        self.assertEqual(client.writes, [])

    def test_body_corruption_legacy_and_concurrent_history_fail(self):
        for kind in ('manifest', 'history', 'head', 'legacy', 'length'):
            with self.subTest(kind=kind):
                client, packet = fixture()
                key = packet['publication_record']['manifest_key']
                if kind == 'manifest': client.data[key] += b' '
                if kind == 'history': client.data[pub.HISTORY] = b'{}'
                if kind == 'head': packet['anxiety_history'][0]['value'] = False
                if kind == 'legacy': packet.pop('publication_record')
                if kind == 'length': reseal(client, packet, lambda m: m['output'].update(bytes=True))
                with self.assertRaises(ValueError): replay.publication(packet, client)
                self.assertEqual(client.writes, [])

    def test_valid_hashes_cannot_hide_lost_history_or_false_qualification(self):
        for kind in ('lost_date', 'typed_change', 'manifest_authority', 'atomic', 'clock', 'inventory'):
            with self.subTest(kind=kind):
                client, packet = fixture()
                def change(m):
                    if kind == 'manifest_authority': m['calls_eligible'] = 0
                    if kind == 'atomic': m['snapshot_atomic'] = True
                    if kind == 'clock': m['started_at'] = '2026-09-28T15:00:00Z'
                    if kind == 'inventory': m['predecessors'].pop('head')
                    if kind in ('lost_date', 'typed_change'):
                        ref = m['predecessors']['history']
                        prior = pub.strict(client.data[ref['key']])
                        if kind == 'lost_date': prior['2026-09-24'] = {'anxiety': 7}
                        else: prior['2026-09-25']['anxiety'] = False
                        raw = pub.encode(prior); digest = pub.sha(raw)
                        key = pub.PREFIX + 'predecessors/' + digest + '.json'
                        client.data[key] = raw
                        m['predecessors']['history'] = {'key': key, 'sha256': digest, 'bytes': len(raw)}
                reseal(client, packet, change)
                with self.assertRaises(ValueError): replay.publication(packet, client)
                self.assertEqual(client.writes, [])

    def test_unapproved_transport_paths_never_reach_network(self):
        with patch.object(replay.urllib.request, 'urlopen') as network:
            for key in ('data/ai-brief.json', '../data/bond-desk.json', 'data/bond-desk.json?secret=x', 'https://example.com/x'):
                with self.assertRaises(ValueError): replay.PublicReader().get_object(Bucket='public', Key=key)
            network.assert_not_called()

    def test_component_failure_cannot_be_reported_as_success(self):
        client, packet = fixture()
        packet['regions'] = {'us': {'flows': {'flow_research': {}}, 'credit': {'credit_research': {}}}}
        client.data[pub.CURRENT] = pub.encode(packet)
        with patch.object(replay, 'publication', return_value={}), patch.object(replay.flows, 'replay', side_effect=ValueError('compiler differs')), patch.object(replay.credit, 'replay') as second:
            with self.assertRaisesRegex(ValueError, 'compiler differs'): replay.verify(client)
            second.assert_not_called()


if __name__ == '__main__':
    unittest.main()
