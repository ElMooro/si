"""Read the complete, fixed approved public archive; no native engine invocation."""
from pathlib import Path
import hashlib
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
INVENTORY_HASHES = {
    'data/research-forecasts/captures/': 'be88c4246fb155165ae7c8a778cc734d1552060d987d0b910e0937f14d61e0cd',
    'data/research-forecasts/records/': '98f21bcc0afe16801b06648c02c15c08c77a01f0c1ef7a4401e4b694083552f2',
}


def main():
    sys.path[:0] = [str(ROOT/p) for p in ('aws/ops/checks', 'aws/shared', 'aws/ops')]
    from genealogy_public_archive import audit, load_inventory, canonical
    from ops_report import report
    inventories = load_inventory(ROOT/'aws/ops/reports/latest/ops_6303_public_research_archive_inventory.md')
    if {p: i['inventory_sha256'] for p, i in inventories.items()} != INVENTORY_HASHES:
        raise ValueError('The reviewed fixed archive inventory differs')
    import boto3
    from botocore.config import Config
    client = boto3.client('s3', region_name='us-east-1', config=Config(max_pool_connections=8,
        connect_timeout=10, read_timeout=30, retries={'mode': 'standard', 'total_max_attempts': 2}))
    with report('ops_6304_genealogy_public_archive_reconcile') as r:
        started = time.monotonic()
        result = audit(client, inventories)
        records, evidence = result.pop('records'), result.pop('evidence')
        # Keep report size bounded by omitting the successful bodies already
        # retained at their public immutable paths, never by sampling input.
        record_digest = hashlib.sha256(canonical(records)).hexdigest()
        evidence_digest = hashlib.sha256(canonical(evidence)).hexdigest()
        r.kv(archive=result, complete_record_projection_sha256=record_digest,
             complete_read_evidence_sha256=evidence_digest,
             identity_issues=[{'forecast_id': v['forecast_id'], 'reason': v['identity_issue']} for v in records if v['identity_issue']],
             elapsed_seconds=round(time.monotonic()-started, 3),
             candidate_source_sha256=hashlib.sha256((ROOT/'aws/ops/checks/genealogy_public_archive.py').read_bytes()).hexdigest(),
             native_invocations=0, learning_ledger_reads=0, downstream_output_reads=0,
             private_account_reads=0, provider_requests=0, credential_reads=0,
             public_writes=0, history_writes=0, schedule_changes=0,
             scope='Every object in the fixed approved public archive, plus its retained protocol. Content checks and registration-clock reconciliation only; no upstream model replay, price-outcome validation or live native repair.')
        if not result['record_body_and_storage_checks_complete']:
            raise ValueError('Public archive reconciliation found unresolved content or reference failures; full diagnostics retained')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
