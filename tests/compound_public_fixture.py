"""Emit the actual handler's public packet from invented inputs, offline only."""
from pathlib import Path
import contextlib
import io
import json
import socket
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'aws/shared/tests')]
from ciss_vintage_test_support import load


class Memory:
    def __init__(self):
        self.values = {
            'data/nobrainers.json': {'summary': {'top_25_overall': [{'ticker': 'QAONLY', 'score': 40}]}},
            'data/insider-clusters.json': {'clusters': [{'ticker': 'QAONLY', 'score': 60}]},
        }
        self.writes = {}

    def get_object(self, **kwargs):
        return {'Body': io.BytesIO(json.dumps(self.values.get(kwargs['Key'], {})).encode('utf-8'))}

    def put_object(self, **kwargs):
        self.writes[kwargs['Key']] = json.loads(kwargs['Body'])


def fixture():
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline fixture only')):
        module = load('justhodl-compound-aggregator')
        storage = Memory()
        with patch.object(module, 'S3', storage), patch.object(module, 'emit_alerts', side_effect=AssertionError('No messages')), contextlib.redirect_stdout(io.StringIO()):
            result = module.lambda_handler({'suppress_alerts': True})
        assert result['statusCode'] == 200
        assert module.STATE_KEY not in storage.writes
        packet = storage.writes[module.S3_KEY]
        assert packet['compound'][0]['symbol'] == 'QAONLY'
        assert packet['compound'][0]['compound_score'] == 150
        assert packet['new_alerts'] == []
        return packet


if __name__ == '__main__':
    print(json.dumps(fixture(), allow_nan=False))
