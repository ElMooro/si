"""Inspect completed part-4 originals and record every anonymous-access outcome.

6086 failed after collection. Never acquire again or overwrite its control.
Retain the whole journal, reparse the exact source plan and preserve complete
HEAD observations before declaring whether source/privacy verification passed.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json, sys
import boto3
from botocore.config import Config
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
import share_structure_sources as source
import share_structure_campaign as campaign
import share_structure_batch_runner as batches
import retained_access_evidence as access
import ops_6081_share_structure_retained_baseline as baseline
import ops_6082_share_structure_original_probe as probe

REQUEST = 'chatgpt-share-structure-part4-retained-6114'
STATUS = source.request_key(REQUEST, 'verification')


def main():
    s3 = boto3.client('s3', region_name='us-east-1', config=Config(max_pool_connections=12, retries={'max_attempts': 2}))
    lam, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 'events', 'scheduler'))
    with report('ops_6114_share_structure_part4_retained_verification') as r:
        failed_report = (ROOT / 'aws/ops/reports/latest/ops_6086_share_structure_sources_part_4.md').read_text(encoding='utf-8')
        assert '**Status:** failure' in failed_report and 'denied_with_retry' in failed_report
        previous = json.loads(source.read(s3, probe.BASELINE))
        before = runtime(lam, s3, events, scheduler, baseline.FUNCTION)
        assert before == previous['runtime']
        control = source.request_key(batches.PARENT, 'batch:4')
        response = s3.get_object(Bucket=source.BUCKET, Key=control)
        raw = bounded(response['Body'], source.MAX); state = json.loads(raw)
        whole = source.retain(s3, raw)
        r.kv(whole_original_journal=whole, observed_status=state.get('status'),
             retained_capture_count=len(state.get('captures', {})), source_errors=state.get('source_errors', {}),
             batch_manifest=state.get('manifest'), original_failed_run=36167201844, provider_requests=0)
        assert state['request_id'] == batches.PARENT and state['part'] == 4 and state['status'] == 'complete'
        assert not state['source_errors'] and len(state['captures']) == 2000
        plan_state = campaign.read_journal(s3, batches.PLAN_STATUS)
        assert plan_state['status'] == 'complete' and plan_state['plan'] == state['plan']
        plan = json.loads(source.read(s3, state['plan']))
        assert plan['baseline'] == probe.BASELINE and plan['probe'] == batches.PROBE and plan['accounting'] == probe.ACCOUNTING
        compiler = source.retain(s3, Path(access.__file__).read_bytes())
        progress = {'request_id': REQUEST, 'status': 'claimed', 'whole_original_journal': whole,
                    'manifest': state['manifest'], 'access_checker': compiler, 'started_at': baseline.now()}
        source.journal(s3, STATUS, progress, True)
        try:
            manifest = json.loads(source.read(s3, state['manifest']))
            assert manifest['plan'] == state['plan'] and manifest['captures'] == state['captures']
            counts = campaign.verify_batch(s3, plan, manifest)
            assert counts == state['counts'] and counts['complete_sources'] == 2000
            protected = {STATUS, control, whole['key'], compiler['key'], state['manifest']['key'], state['plan']['key'],
                         batches.PLAN_STATUS, probe.BASELINE['key'], batches.PROBE['key'], probe.ACCOUNTING['key']}
            for ref in manifest['captures'].values():
                capsule = json.loads(source.read(s3, ref))
                protected.update((ref['key'], capsule['original']['key'], capsule['request_status_key']))
            r.kv(counts=counts, original_sources_reparsed=True, protected_paths_pending=len(protected))
            with ThreadPoolExecutor(max_workers=4) as pool:
                outcomes = list(pool.map(access.check, sorted(protected)))
            full = source.retain(s3, source.encode({'contract': 'retained-anonymous-access-evidence.v1',
                'whole_journal': whole, 'manifest': state['manifest'], 'outcomes': outcomes}))
            outcomes.append(access.check(full['key']))
            result = access.summarize(outcomes)
            r.kv(access_evidence=full, **result)
            assert result['all_denied'], 'Inspect exact failed HEAD outcomes before another operation'
            assert runtime(lam, s3, events, scheduler, baseline.FUNCTION) == before
            assert bounded(s3.get_object(Bucket=source.BUCKET, Key=control)['Body'], source.MAX) == raw
            final = {'part': 4, 'batch_manifest': state['manifest'], 'whole_original_journal': whole,
                'counts': counts, 'access_evidence': full, 'original_sources_reparsed': True,
                'native_package_unchanged': True, 'parent_control_unchanged': True,
                'complete_population_claimed': False, 'forecast_qualified': False, 'sizing_qualified': False,
                'provider_requests': 0, 'producer_invocations': 0, 'consumer_invocations': 0,
                'public_writes': 0, 'private_account_reads': 0, 'paid_ai_calls': 0,
                'notifications_sent': 0, 'signal_writes': 0, 'schedules_changed': 0, **result}
            source.journal(s3, STATUS, {**progress, 'status': 'complete', 'result': final})
            r.kv(**final)
        except Exception as exc:
            source.journal(s3, STATUS, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
            raise


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
