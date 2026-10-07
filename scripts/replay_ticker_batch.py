"""Compare complete Ticker 360 publications offline, then run the real network.

Retained complete public captures only; no network/provider calls. The baseline
is recovered through the exact source transition, not reimplemented by this test.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
import types
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'tests')]
from replay_research_network import LocalStore
from ticker_batch_preservation import preceding_source
import ticker_360
import research_network_store


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshots', type=Path, required=True)
    parser.add_argument('--extra-inventory', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(ROOT):
        raise ValueError('Keep captured bodies and replay outputs outside the repository')
    args.output.mkdir(parents=True, exist_ok=True)
    path = ROOT / 'aws/lambdas/justhodl-ticker-360/source/lambda_function.py'
    current = path.read_text(encoding='utf-8')
    previous = preceding_source(path, current)
    now = datetime.now(timezone.utc)

    class Clock:
        @staticmethod
        def now(*unused):
            return now

    results = {}
    bodies = {}
    for label, text in [('previous', previous), ('candidate', current)]:
        output = args.output / label
        storage = LocalStore(args.snapshots, output)
        extras = json.loads(args.extra_inventory.read_text(encoding='utf-8'))
        for row in extras:
            if row['status'] != 'captured':
                continue
            file = args.snapshots / row['file']
            if hashlib.sha256(file.read_bytes()).hexdigest() != row['sha256']:
                raise ValueError('Extra original digest mismatch')
            storage.sources[row['key']] = file
        boto = types.ModuleType('boto3')
        boto.client = lambda *a, **kw: storage
        producer = types.ModuleType(label)
        with patch.dict(sys.modules, boto3=boto):
            exec(compile(text, str(path), 'exec'), producer.__dict__)
        with patch.object(producer, 'datetime', Clock), patch.object(producer, 'publish_network', return_value={'publication_id': 'offline', 'entity_count': 0}):
            started = time.perf_counter()
            result = producer.lambda_handler({}, None)
            seconds = time.perf_counter() - started
        raw = (output / 'data/ticker-360.json').read_bytes()
        body = json.loads(raw)
        body.pop('elapsed_s')  # Intentionally changed duration, same generation clock.
        canonical = json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        bodies[label] = canonical
        results[label] = {'seconds': round(seconds, 3), 'bytes': len(raw),
                          'canonical_sha256': hashlib.sha256(canonical).hexdigest(),
                          'universe': result['universe'], 'indexed': result['indexed']}
        print(json.dumps({label: results[label]}), flush=True)
        if label == 'candidate':
            started = time.perf_counter()
            manifest = research_network_store.publish_network(storage, 'offline', now=now)
            results['network'] = {'seconds': round(time.perf_counter()-started, 3),
                                  'publication_id': manifest['publication_id'],
                                  'entities': manifest['entity_count'], 'available_sources': manifest['available_sources']}
    results['complete_legacy_output_equal_except_elapsed_s'] = bodies['previous'] == bodies['candidate']
    results['scope'] = 'offline complete captured public inputs; not AWS runtime or private-account acceptance'
    (args.output / 'results.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
    print(json.dumps(results, indent=2), flush=True)
    if not results['complete_legacy_output_equal_except_elapsed_s']:
        raise ValueError('Complete legacy output differs')


if __name__ == '__main__':
    main()
