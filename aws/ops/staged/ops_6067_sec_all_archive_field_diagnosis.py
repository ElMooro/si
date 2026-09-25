"""Inspect every retained archive and row from failed 6066; no downloads or repairs."""
from pathlib import Path
from collections import Counter
from datetime import date
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import hashlib, re, sys, zipfile
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6066_sec_complete_observed_history as campaign
import sec_ftd_inventory as sec
base = campaign.base
ORIGINALS = {'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608b.zip': {'bytes': 1393881, 'key': 'audit-private/20260909-originals/sec-ftd-research/0c90519d0d0e959b86331c5fe546cefa027c8a17998111270001b0ee61bbf203.bin', 'sha256': '0c90519d0d0e959b86331c5fe546cefa027c8a17998111270001b0ee61bbf203'}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608a.zip': {'bytes': 1352929, 'key': 'audit-private/20260909-originals/sec-ftd-research/7eac7eb87278af473d8ba63387da2bb1ce22432261589d732c54e2c54640fefa.bin', 'sha256': '7eac7eb87278af473d8ba63387da2bb1ce22432261589d732c54e2c54640fefa'}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202607b.zip': {'bytes': 1653974, 'key': 'audit-private/20260909-originals/sec-ftd-research/dc7d52c6eaf5c3cd18cbd8f6700ed1c8f9311a2a5d2c710b74f2a697384fbdea.bin', 'sha256': 'dc7d52c6eaf5c3cd18cbd8f6700ed1c8f9311a2a5d2c710b74f2a697384fbdea'}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202607a.zip': {'key': 'audit-private/20260909-originals/sec-ftd-research/a0f4a8d3c88f8a81a3207a999008c656be251daac3df93acd04e71bb55b17f24.bin', 'sha256': 'a0f4a8d3c88f8a81a3207a999008c656be251daac3df93acd04e71bb55b17f24', 'bytes': 1242622}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202606b.zip': {'key': 'audit-private/20260909-originals/sec-ftd-research/eacc947fdf3661a33bbcbcf112ae92dd05740083ef8fa82a6d4a4498b4622a2c.bin', 'sha256': 'eacc947fdf3661a33bbcbcf112ae92dd05740083ef8fa82a6d4a4498b4622a2c', 'bytes': 1594245}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202606a.zip': {'key': 'audit-private/20260909-originals/sec-ftd-research/7620134f9d67533f0f532a4da505b24a775d8dd277445e15d939df4f396e60b2.bin', 'sha256': '7620134f9d67533f0f532a4da505b24a775d8dd277445e15d939df4f396e60b2', 'bytes': 1335662}, 'https://www.sec.gov/files/data/other/fails-deliver-data/cnsfails202605b.zip': {'key': 'audit-private/20260909-originals/sec-ftd-research/53f95c33fe5b1e427e02ea5a6c8206d06e9f781a25838b47cca6545f5c2b0364.bin', 'sha256': '53f95c33fe5b1e427e02ea5a6c8206d06e9f781a25838b47cca6545f5c2b0364', 'bytes': 1269388}, 'https://www.sec.gov/files/data/other/fails-deliver-data/cnsfails202605a.zip': {'key': 'audit-private/20260909-originals/sec-ftd-research/2aefd16e5c39206a108aac53a4076572930ba84581a0e0981dcd136f1958a72e.bin', 'sha256': '2aefd16e5c39206a108aac53a4076572930ba84581a0e0981dcd136f1958a72e', 'bytes': 1283728}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202604b.zip': {'key': 'audit-private/20260909-originals/sec-ftd-research/75b7d61b1bc08c332736aa852b6684d0406adf17e0f5bafb5b3cee1da35e44dc.bin', 'sha256': '75b7d61b1bc08c332736aa852b6684d0406adf17e0f5bafb5b3cee1da35e44dc', 'bytes': 1484637}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202604a.zip': {'key': 'audit-private/20260909-originals/sec-ftd-research/772f0aaa553de505500f7071260753a94a8dfe1826d6d0eca44a7b02e6e71b29.bin', 'sha256': '772f0aaa553de505500f7071260753a94a8dfe1826d6d0eca44a7b02e6e71b29', 'bytes': 1137701}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202603b.zip': {'key': 'audit-private/20260909-originals/sec-ftd-research/bf85fc144dd29637e87f37ba08de05f18b4e76a72254276bb31978f4b5e80116.bin', 'sha256': 'bf85fc144dd29637e87f37ba08de05f18b4e76a72254276bb31978f4b5e80116', 'bytes': 1565163}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202603a.zip': {'key': 'audit-private/20260909-originals/sec-ftd-research/867b4c74fb5e16bf1d44216b266e3ac6e200592d736ff6b7ab40a6efb32e7650.bin', 'sha256': '867b4c74fb5e16bf1d44216b266e3ac6e200592d736ff6b7ab40a6efb32e7650', 'bytes': 1289454}}


def inspect_body(member, body):
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


def inspect(archive):
    assert isinstance(archive, bytes) and 0 < len(archive) <= sec.MAX_ZIP
    with zipfile.ZipFile(BytesIO(archive)) as packed:
        members = packed.infolist()
        metadata = [{'name': m.filename, 'bytes': m.file_size, 'compressed_bytes': m.compress_size,
                     'directory': m.is_dir(), 'flags': m.flag_bits, 'crc32': m.CRC} for m in members]
        # No extraction to a filesystem: a member's name cannot select a path.
        assert len(members) == 1
        member = members[0]
        assert not member.is_dir() and not member.flag_bits & 1 and 0 < member.file_size <= sec.MAX_TEXT
        with packed.open(member) as stream:
            body = stream.read(sec.MAX_TEXT + 1)
        assert len(body) == member.file_size
    identity = {'name': member.filename, 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
    result = inspect_body(identity, body)
    result['archive_members'] = metadata
    return result


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    with report('ops_6067_sec_all_archive_field_diagnosis') as r:
        state = base.raw.strict(base.read(s3, campaign.key('campaign')))
        assert state['request_id'] == campaign.REQUEST and state['status'] == 'failed'
        assert {u: v['original'] for u, v in state['captures'].items()} == ORIGINALS
        results = {}
        for url, ref in ORIGINALS.items():
            results[url] = inspect(base.checked(s3, ref))
        protected = {campaign.key('campaign'), *(v['key'] for v in ORIGINALS.values())}
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/' + key)
            assert denied_with_retry('https://' + base.BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=3) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        r.kv(diagnoses=results, originals=ORIGINALS, protected_artifacts_checked=len(protected),
             provider_requests=0, engine_invocations=0, public_head_writes=0, private_account_reads=0,
             paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
