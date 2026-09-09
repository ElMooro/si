"""Interrupted migrations retain progress without publishing application data."""
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'aws/ops/checks'))
from audit_20260909_migration_progress import ProgressCheckpoint


def test_progress_never_contains_row_payload_or_claims_completion():
    with tempfile.TemporaryDirectory() as folder:
        path=Path(folder)/'progress.json'
        writer=ProgressCheckpoint(path,'a'*40)
        writer(SimpleNamespace(step='sanitize_current_public_objects', rows=[
            {'check':'public_object_sanitized','payload':'SYNTHETIC_PRIVATE_TEXT','ok':True}]))
        raw=path.read_text(); result=json.loads(raw)
        assert 'SYNTHETIC_PRIVATE_TEXT' not in raw
        assert result['ok'] is False and result['final_receipt'] is False
        assert result['check_counts']=={'public_object_sanitized':1}
        assert not path.with_suffix('.tmp').exists()


def test_progress_throttles_same_phase_but_flushes_phase_changes():
    with tempfile.TemporaryDirectory() as folder:
        path=Path(folder)/'progress.json'; now=[0]
        writer=ProgressCheckpoint(path,'a'*40,clock=lambda:now[0])
        state=SimpleNamespace(step='sanitize_current_public_objects',rows=[])
        writer(state); state.rows.append({'check':'public_object_sanitized'})
        now[0]=10; writer(state)
        assert json.loads(path.read_text())['recorded_checks']==0
        state.step='rebuild_search_and_clear_warm_cache'; writer(state)
        assert json.loads(path.read_text())['recorded_checks']==1
        state.rows.append({'check':'provider_catalog_rebuilt'})
        now[0]=40; writer(state)
        assert json.loads(path.read_text())['recorded_checks']==2
