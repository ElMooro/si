"""Build synthetic browser evidence with the actual frozen compiler closure."""
from pathlib import Path
import json, sys
sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1] / 'aws/shared')]
from test_statement_research_v2 import Fixture
from test_statement_research_store import S3
import statement_research_store_v2 as store
import statement_research_source as source
import statement_measurements as measurements


def main():
    def change(symbol, period, endpoint, rows):
        if symbol == 'EMPTY': return []
        if symbol == 'PART':
            if endpoint == measurements.C: return []
            if endpoint == measurements.I: rows[0]['revenue'] = 0
        if symbol == 'BADCIK': rows[0]['cik'] = '456'
        if symbol == 'BADDATE': rows[0]['acceptedDate'] = '2025-01-01 12:00:00'
        return rows
    fixture = Fixture(('ABC', 'BADCIK', 'BADDATE', 'EMPTY', 'PART', 'XYZ'), modify=change)
    compiled = fixture.compile(); fixture.verify(compiled)
    client = S3(fixture.files)
    ref = store.retain(client, 'synthetic', fixture.ref, fixture.identity_ref, compiled)
    assert store.replay(ref, store.reader(client, 'synthetic')) == compiled
    objects = {key: body.decode('utf-8') for key, body in sorted(client.files.items())
        if key.startswith('data/statement-research/') and key.endswith('.json')}
    objects['data/forensic-screen.json'] = source.encoded({**compiled['packet'], 'replay': ref}).decode('utf-8')
    value = {'fixture_notice': 'SYNTHETIC TEST DATA ONLY. Not a live issuer, filing or qualification.',
        'run': ref['manifest_key'], 'objects': objects}
    path = Path(__file__).resolve().parent / 'fixtures/statement-research-public.json'
    path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')), encoding='utf-8', newline='\n')
    print(json.dumps({'fixture': str(path), 'bytes': path.stat().st_size, 'run': ref['manifest_key']}))


if __name__ == '__main__': main()
