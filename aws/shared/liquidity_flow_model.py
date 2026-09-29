"""Descriptive native liquidity publication; no calibrated return or sizing model."""
from copy import deepcopy
from datetime import date
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
import math
import liquidity_flow_arithmetic as arithmetic
from report_observations import encoded, digest
from research_brief_model import clock

CONTRACT = 'liquidity-flow-research.v1'
PREFIX = 'data/liquidity-flow-research/'
CURRENT = 'data/liquidity-flow.json'


def settlement_context(packet, generated_at, reference):
    """Strict snapshot projection. Does not independently requalify FR2004."""
    packet = packet if isinstance(packet, dict) else {}
    now = clock(generated_at)
    valid_contract = packet.get('contract') == 'fr2004-fails-research.v1'
    def scope(name, identity, combined):
        row = packet.get(name) or {}; issues = []
        if not isinstance(row, dict): row = {}
        if not valid_contract or row.get('scope_id') != identity: issues.append('scope_contract_missing')
        if row.get('complete') is not True: issues.append('incomplete_scope')
        try:
            observed = date.fromisoformat(row['as_of'])
            if str(observed) != row['as_of'] or observed > now.date(): raise ValueError('date')
            age = (now.date()-observed).days
        except (ValueError, KeyError, TypeError): observed = None; age = None; issues.append('invalid_date')
        values = {}
        for field, source in (('ftd_bn', 'ftd_bn'), ('ftr_bn', 'ftr_bn'), ('combined_bn', combined)):
            value = row.get(source)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (row.get('field_units') or {}).get(source) != 'usd_bn':
                issues.append('invalid_'+source); value = None
            values[field] = value
        if all(value is not None for value in values.values()):
            with localcontext() as context:
                context.prec = 34; context.rounding = ROUND_HALF_EVEN
                if Decimal(str(values['ftd_bn']))+Decimal(str(values['ftr_bn'])) != Decimal(str(values['combined_bn'])):
                    issues.append('gross_reconciliation_failed')
        if issues: values = {key: None for key in values}
        return {'scope_id': identity, 'as_of': str(observed) if observed else None, 'unit': 'usd_bn', **values,
            'quality': {'status': 'unavailable' if issues else 'stale' if age > 15 else 'unverified',
                'observation_date': str(observed) if observed else None, 'publication_date': None,
                'age_days': age, 'frequency': 'weekly', 'missing': issues,
                'basis': 'Strict scoped public snapshot; original FR2004 replay belongs to the source desk.'},
            'reported_source_quality': deepcopy(row.get('quality')), 'calls_eligible': False}
    gross = scope('treasury', 'treasury_incl_tips', 'gross_bn')
    return {**gross, 'ust_ex_tips': scope('headline', 'ust_ex_tips', 'combined_bn'),
        'source': 'data/settlement-fails.json', 'source_generated_at': packet.get('generated_at'),
        'source_snapshot': deepcopy(reference), 'source_replay': deepcopy(packet.get('replay')),
        'role': 'context_only', 'original_source_replayed_here': False,
        'note': 'Two-sided gross reported fails, not unique securities, defaults, flows or a WALCL term. Including-TIPS and ex-TIPS overlap; never add the scopes.'}


def build(source, originals, settlement, generated_at, legacy_context, settlement_reference=None):
    audit = arithmetic.build(source, originals, generated_at)
    out = deepcopy(audit)
    out.pop('candidate_only'); out.pop('publication_eligible')
    out['contract'] = CONTRACT
    snapshot = audit['current']
    legs = snapshot['legs'] if snapshot else {}
    dates = {sid: row['selected']['observation_date'] for sid, row in legs.items()}
    out['current'] = {'fed_balance_sheet_b': legs.get('WALCL', {}).get('value', {}).get('value'),
        'tga_b': legs.get('WTREGEN', {}).get('value', {}).get('value'),
        'rrp_b': legs.get('RRPONTSYD', {}).get('value', {}).get('value'),
        'net_liquidity_b': snapshot['net']['value'] if snapshot else None,
        'net_liquidity_exact_decimal': snapshot['net']['exact_decimal'] if snapshot else None,
        'observation_dates': dates, 'valuation_date': snapshot['valuation_date'] if snapshot else None}
    out['as_of'] = min(dates.values()) if dates else None
    out['as_of_basis'] = 'Oldest selected input date; the individual periods and measurement bases remain different.'
    out['quality'].update(status='fresh' if snapshot else 'unavailable',
        observation_date=out['as_of'], frequency='mixed',
        freshness_basis='Per-original acquisition <=26h and canonical observation ceilings; release calendar not verified.',
        source_eligibility=audit['quality']['status'])
    out['regime'] = 'descriptive_proxy' if snapshot else 'unavailable'
    out['interpretation'] = ('WALCL minus weekly-average WTREGEN minus daily RRP, in USD billions. '
        'Selected dates, carry ages and signed level changes are inspectable. This mixed-basis proxy is not investable cash, '
        'a causal liquidity flow, an easing signal or a validated asset-return forecast.')
    out['units'] = 'usd_bn'; out['formula_note'] = audit['measurement_basis']
    out['deltas'] = {label: ({'walcl': row['legs']['WALCL']['reported_level_change']['value'],
        'tga': row['legs']['WTREGEN']['reported_level_change']['value'],
        'rrp': row['legs']['RRPONTSYD']['reported_level_change']['value'], 'net': row['change']['value'],
        'current_valuation_date': row['current_valuation_date'], 'baseline_valuation_date': row['baseline_valuation_date'],
        'basis': 'Current-vintage calendar endpoints; reported level changes, not causal cash flows.'}
        if snapshot and row['change']['value'] is not None else None) for label, row in audit['comparisons'].items()}
    out['history_180d'] = [{'date': row['valuation_date'],
        **{label: row['legs'][sid]['value']['value'] for sid, label in (('WALCL', 'walcl'), ('WTREGEN', 'tga'), ('RRPONTSYD', 'rrp'))},
        'net': row['net']['value']} for row in audit['calendar_history_180d']]
    out['pd_settlement_fails'] = settlement_context(settlement, generated_at, settlement_reference)
    out['legacy_context'] = deepcopy(legacy_context)
    out['migration'] = {'previous_measurements_preserved': True,
        'changes': ['declared_units', 'calendar_endpoints', 'retained_nulls', 'explicit_source_dates',
                    'separate_fail_scopes', 'removed_unsupported_directional_inference'],
        'legacy_regime_thresholds_qualified': False}
    return out
