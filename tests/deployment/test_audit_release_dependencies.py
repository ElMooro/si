"""A queued operation cannot race or substitute its exact required releases."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'aws/ops/checks'))
import audit_release_dependencies as dependencies


def receipt(run_id, **changes):
    return {'id':run_id, 'head_sha':dependencies.RELEASES[run_id],
            'path':'.github/workflows/deploy-lambdas.yml',
            'repository':{'full_name':'ElMooro/si'}, 'status':'completed',
            'conclusion':'success', **changes}


def test_all_exact_success_receipts_required_before_return():
    rows=dependencies.await_releases(read=receipt, pause=lambda _:None)
    assert len(rows)==len(dependencies.RELEASES) and all(row['conclusion']=='success' for row in rows)


def test_failed_cancelled_or_wrong_source_receipt_blocks():
    for change in ({'conclusion':'failure'}, {'conclusion':'cancelled'}, {'head_sha':'0'*40},
                   {'repository':{'full_name':'other/repo'}}):
        try:dependencies.await_releases(read=lambda run_id:receipt(run_id,**change))
        except dependencies.DependencyError:pass
        else:raise AssertionError('invalid release accepted')


def test_waiting_release_must_complete_or_reach_bounded_deadline():
    ticks=[0]
    def pause(delay):ticks[0]+=delay
    def read(run_id):
        return receipt(run_id, status='in_progress', conclusion=None) if ticks[0]<30 else receipt(run_id)
    rows=dependencies.await_releases(read=read,clock=lambda:ticks[0],pause=pause,timeout=60)
    assert len(rows)==len(dependencies.RELEASES) and ticks[0]==30
    try:dependencies.await_releases(read=lambda run_id:receipt(run_id,status='queued',conclusion=None),
                                   clock=lambda:ticks[0],pause=pause,timeout=30)
    except dependencies.DependencyError as error:assert str(error)=='required_release_wait_expired'
    else:raise AssertionError('unfinished release accepted')
