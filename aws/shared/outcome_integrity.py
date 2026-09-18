"""Recompute eligible outcomes from dated, identified, comparable price marks.

Legacy ledger rows remain intact. Absence of proof is reported, never repaired
by capping returns, inventing a baseline or treating a ticker as an identity.
"""
import math
import re
from datetime import datetime
from instrument_identity import same_instrument

CONTRACT = 'outcome-lineage.v1'
UP = frozenset(('UP', 'OUTPERFORM', 'LONG', 'BULLISH'))
DOWN = frozenset(('DOWN', 'UNDERPERFORM', 'SHORT', 'BEARISH'))


def finite(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError, OverflowError):
        return None


def stamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return result if result.tzinfo is not None else None
    except (TypeError, ValueError):
        return None


def pair_return(entry, exit_mark):
    errors = []
    if not isinstance(entry, dict) or not isinstance(exit_mark, dict):
        return None, ['missing_entry_or_exit_mark']
    if not same_instrument(entry, exit_mark):
        errors.append('instrument_or_currency_mismatch')
    prices = [finite(m.get('price')) for m in (entry, exit_mark)]
    if any(p is None or p <= 0 for p in prices):
        errors.append('invalid_price')
    for mark in (entry, exit_mark):
        if not mark.get('provider') or not re.fullmatch(r'[0-9a-f]{64}', str(mark.get('evidence_sha256') or '')):
            errors.append('missing_price_evidence')
    dates = [stamp(m.get('observed_at')) for m in (entry, exit_mark)]
    if any(d is None for d in dates) or (all(d is not None for d in dates) and dates[0] >= dates[1]):
        errors.append('invalid_mark_window')
    basis = entry.get('adjustment_basis')
    if basis not in ('split_adjusted_price', 'total_return_adjusted', 'unadjusted_spot') or basis != exit_mark.get('adjustment_basis'):
        errors.append('incompatible_adjustment_basis')
    if basis in ('split_adjusted_price', 'total_return_adjusted'):
        if not entry.get('adjustment_vintage') or entry.get('adjustment_vintage') != exit_mark.get('adjustment_vintage'):
            errors.append('unreconciled_corporate_action_vintage')
    elif basis == 'unadjusted_spot' and not str(entry.get('instrument_id', '')).startswith('crypto:'):
        errors.append('unadjusted_security_corporate_actions_unverified')
    if errors:
        return None, sorted(set(errors))
    return (prices[1] / prices[0] - 1.0) * 100.0, []


def assess_outcome(row, evidence_verifier=None):
    row = row if isinstance(row, dict) else {}
    outcome = row.get('outcome') if isinstance(row.get('outcome'), dict) else {}
    reasons = []
    if outcome.get('lineage_contract') != CONTRACT:
        reasons.append('legacy_lineage_unverified')
    entry_marks = outcome.get('entry_marks') if isinstance(outcome.get('entry_marks'), dict) else {}
    end_marks = outcome.get('marks') if isinstance(outcome.get('marks'), dict) else {}
    entry = entry_marks.get('asset') if isinstance(entry_marks.get('asset'), dict) else None
    end = end_marks.get('asset') if isinstance(end_marks.get('asset'), dict) else None
    result, problems = pair_return(entry, end)
    reasons.extend(problems)
    logged = stamp(row.get('logged_at'))
    start = stamp((entry or {}).get('observed_at'))
    if logged is None or start is None or start < logged:
        reasons.append('entry_precedes_recorded_signal_or_time_unknown')
    # A harvested ranked list is not an explicit directional forecast.
    if row.get('prediction_origin') != 'explicit_direction':
        reasons.append('direction_not_explicitly_recorded')
    direction = str(row.get('predicted_dir') or row.get('predicted_direction') or '').upper()
    if direction not in UP | DOWN:
        reasons.append('no_directional_prediction')
    excess = None
    relative = direction in ('OUTPERFORM', 'UNDERPERFORM') or outcome.get('excess_return') is not None
    bm_entry = entry_marks.get('benchmark') if isinstance(entry_marks.get('benchmark'), dict) else None
    bm_end = end_marks.get('benchmark') if isinstance(end_marks.get('benchmark'), dict) else None
    if relative or bm_entry or bm_end:
        benchmark_return, errors = pair_return(bm_entry, bm_end)
        reasons.extend('benchmark_' + e for e in errors)
        if entry and end and bm_entry and bm_end:
            if any(a.get('observed_at') != b.get('observed_at') for a, b in ((entry, bm_entry), (end, bm_end))):
                reasons.append('asset_benchmark_window_mismatch')
            if entry.get('currency') != bm_entry.get('currency') or entry.get('adjustment_basis') != bm_entry.get('adjustment_basis'):
                reasons.append('asset_benchmark_basis_mismatch')
        if benchmark_return is not None and result is not None:
            excess = result - benchmark_return
    stored = outcome.get('excess_return') if relative else outcome.get('return_pct')
    calculated = excess if relative else result
    if stored is not None and (finite(stored) is None or calculated is None or not math.isclose(finite(stored), calculated, rel_tol=1e-7, abs_tol=1e-4)):
        reasons.append('stored_return_disagrees_with_marks')
    lineage_valid = not reasons
    if lineage_valid:
        if evidence_verifier is None:
            reasons.append('price_archive_not_verified')
        else:
            for name, mark in (('entry_asset',entry),('exit_asset',end),('entry_benchmark',bm_entry),('exit_benchmark',bm_end)):
                if mark is not None:
                    reasons.extend(name+'_'+problem for problem in evidence_verifier(mark))
    return {'contract': CONTRACT, 'lineage_valid': lineage_valid, 'verified': not reasons, 'reasons': sorted(set(reasons)),
            'return_pct': result if not reasons else None, 'excess_return': excess if not reasons else None,
            'relative': relative, 'sizing_eligible': False,
            'validation_scope': 'MEASUREMENT_ONLY' if not reasons else 'QUARANTINED'}
