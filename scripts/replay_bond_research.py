"""Read-only Bond Desk publication binding and descriptive component replay.

Uses reviewed local compilers, never downloaded executable source. Upstream issuer
and credit originals have separate acceptance; this command does not replay them.
"""
import argparse
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import re
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/lambdas/justhodl-bond-desk/source'), str(ROOT / 'aws/shared')]
import bond_publication as pub
import bond_flow_store as flows
import bond_credit_store as credit


class PublicReader:
    """Bounded, same-origin, GET-only transport for public research artifacts."""
    def __init__(self):
        self.cache = {}
        self.total = 0
        self.evidence = []

    def get_object(self, *, Bucket, Key):
        allowed = (Key in (pub.CURRENT, pub.HISTORY)
                   or re.fullmatch(r'data/bond-desk-research/(?:publications/(?:predecessors|histories|outputs|attempts)|(?:flows|credit)/(?:inputs|outputs|proofs|runs|compilers))/[a-f0-9]{64}\.(?:json|py)', Key)
                   or re.fullmatch(r'data/(?:etf|credit)-research/(?:runs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py)', Key))
        if not allowed:
            raise ValueError('Unapproved public research path')
        if Key not in self.cache:
            if len(self.cache) >= 100 or self.total >= 128 * 1024 * 1024:
                raise ValueError('Public replay acquisition budget exceeded')
            url = 'https://justhodl.ai/' + Key + '?exact=1&nogen=1'
            req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-release-verify/1.0', 'Cache-Control': 'no-cache'})
            with urllib.request.urlopen(req, timeout=45) as response:
                if response.geturl() != url:
                    raise ValueError('Unexpected public artifact redirect')
                raw = response.read(pub.MAX + 1)
                etag = response.headers.get('ETag')
            if not 0 < len(raw) <= pub.MAX or self.total + len(raw) > 128 * 1024 * 1024:
                raise ValueError('Whole bounded artifact required')
            if not isinstance(etag, str) or not etag:
                raise ValueError('Public conditional identity unavailable')
            self.total += len(raw)
            self.cache[Key] = (raw, etag)
            self.evidence.append({'key': Key, 'bytes': len(raw), 'sha256': pub.sha(raw)})
        raw, etag = self.cache[Key]
        return {'Body': io.BytesIO(raw), 'ContentLength': len(raw), 'ETag': etag}


def publication(packet, client):
    ref = packet.get('publication_record', {})
    key = ref.get('manifest_key')
    if not isinstance(key, str) or not re.fullmatch(re.escape(pub.PREFIX) + r'attempts/[a-f0-9]{64}\.json', key):
        raise ValueError('Native publication record unavailable')
    record = pub.read(client, 'public', key)
    if record is None or key != pub.PREFIX + 'attempts/' + pub.sha(record['raw']) + '.json':
        raise ValueError('Publication manifest identity differs')
    manifest = record['doc']
    if manifest.get('contract') != 'bond-desk-publication.v1':
        raise ValueError('Publication contract unavailable')

    def checked(reference, kind):
        if (not isinstance(reference, dict)
                or not re.fullmatch('[a-f0-9]{64}', str(reference.get('sha256')))
                or reference.get('key') != pub.PREFIX + kind + '/' + reference['sha256'] + '.json'
                or type(reference.get('bytes')) is not int
                or not 0 < reference['bytes'] <= pub.MAX):
            raise ValueError('Whole publication reference required')
        value = pub.read(client, 'public', reference['key'])
        if value is None or len(value['raw']) != reference['bytes'] or pub.sha(value['raw']) != reference['sha256']:
            raise ValueError('Retained publication bytes differ')
        return value

    out = checked(manifest['output'], 'outputs')
    history = checked(manifest['history'], 'histories')
    if (pub.encode({k: v for k, v in packet.items() if k != 'publication_record'}) != out['raw']
            or ref.get('output_sha256') != pub.sha(out['raw'])
            or manifest.get('generated_at') != packet.get('generated_at')):
        raise ValueError('Whole public head differs from retained output')
    for value in (manifest, packet):
        if any(value.get(k) is not False for k in ('calls_eligible', 'sizing_eligible', 'execution_eligible')):
            raise ValueError('Unqualified publication asserts authority')
    if (manifest.get('snapshot_atomic') is not False or manifest.get('original_source_replayed_here') is not False
            or packet.get('world_anxiety') is not None or pub.encode(packet.get('decision')) != pub.encode({'verb': 'WAIT', 'meaning': 'abstain'})):
        raise ValueError('Publication qualification differs')
    at = pub.clock(packet['generated_at'])
    start = pub.clock(manifest['started_at'])
    if not 0 <= (at - start).total_seconds() <= 180:
        raise ValueError('Publication outside original runtime')
    predecessors = manifest['predecessors']
    if set(predecessors) != {'head', 'history'}:
        raise ValueError('Complete predecessor inventory required')
    old = {name: checked(value, 'predecessors') if value is not None else None for name, value in predecessors.items()}
    if old['head'] and not (pub.clock(old['head']['doc']['generated_at']) <= start and pub.clock(old['head']['doc']['generated_at']) < at):
        raise ValueError('Publication does not advance predecessor')
    rows = history['doc']
    prior = old['history']['doc'] if old['history'] else {}
    day = at.date().isoformat()
    for date, row in {**prior, **rows}.items():
        if (not re.fullmatch(r'\d{4}-\d{2}-\d{2}', date)
                or datetime.strptime(date, '%Y-%m-%d').date() > at.date()
                or not isinstance(row, dict) or 'anxiety' not in row):
            raise ValueError('Invalid history observation')
    if set(rows) != set(prior) | {day} or any(pub.encode(rows[k]) != pub.encode(v) for k, v in prior.items() if k != day):
        raise ValueError('Prior history changed or lost')
    if pub.encode(rows[day]) != pub.encode({'anxiety': None, 'appetite': None, 'eqbond': None}):
        raise ValueError('Unqualified history invented a signal')
    expected = [{'date': k, 'value': v['anxiety']} for k, v in sorted(rows.items())]
    if pub.encode(packet.get('anxiety_history')) != pub.encode(expected):
        raise ValueError('Whole history projection differs')
    live_history = pub.read(client, 'public', pub.HISTORY)
    if live_history is None or live_history['raw'] != history['raw']:
        raise ValueError('Current history differs; no atomic snapshot or retry is assumed')
    return {'manifest_key': key, 'history_rows': len(rows), 'predecessor_history_rows': len(prior),
            'previous_dates_unchanged': sum(k != day for k in prior), 'snapshot_atomic': False,
            'original_source_replayed_here': False}


def verify(client):
    head = pub.read(client, 'public', pub.CURRENT)
    if head is None:
        raise ValueError('Public Bond Desk head unavailable')
    packet = head['doc']
    binding = publication(packet, client)
    us = packet['regions']['us']
    flow = flows.replay(us['flows']['flow_research'], flows.reader(client, 'public'))
    comparisons = credit.replay(us['credit']['credit_research'], credit.reader(client, 'public'))
    return {'checked_at': datetime.now(timezone.utc).isoformat(), 'generated_at': packet['generated_at'],
            'version': packet['version'], 'public_head': {'bytes': len(head['raw']), 'sha256': pub.sha(head['raw'])},
            'publication': binding, 'flow_arithmetic': flow['arithmetic_checks'],
            'credit_arithmetic': comparisons['arithmetic_checks'],
            'component_replay_verified': True, 'upstream_originals_replayed_here': False,
            'other_regions_replayed_here': False, 'calls_eligible': False, 'sizing_eligible': False,
            'scope': 'Whole derived publication and history binding; flow/credit arithmetic and reviewed compiler closure. No forecast, portfolio or upstream-original qualification.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    reader = PublicReader()
    result = verify(reader)
    result['artifacts'] = reader.evidence
    result['read_bytes'] = reader.total
    text = json.dumps(result, indent=2, allow_nan=False) + '\n'
    if args.output:
        args.output.write_text(text, encoding='utf-8', newline='\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'artifacts'}, sort_keys=True))
