"""FR2004 measurement contracts: levels, complete sums, dated observations.

The SI issue ladder is not the entire Treasury inventory. FR2004B transactions
are daily averages for the reporting week, not weekly totals. Net positions
are not gross inventory and cannot identify turnover or collateral reuse.
"""
import copy
import math
from datetime import datetime, timezone
from donor_contract import inspect_donor

METHOD = 'fr2004-measurement.v2'


def complete_sum(series):
    if not series or any(not s for s in series):
        return {}
    dates = set.intersection(*(set(s) for s in series))
    return {d: sum(s[d] for s in series) for d in sorted(dates)}


def observation_quality(as_of, complete=True, now=None):
    now = now or datetime.now(timezone.utc)
    try:
        age = (now.date() - datetime.strptime(as_of, '%Y-%m-%d').date()).days
    except (ValueError, TypeError):
        age = None
    status = ('incomplete' if not complete or age is None else
              'invalid' if age < 0 else 'stale' if age > 21 else 'fresh')
    return {'status': status, 'observation_date': as_of, 'age_days': age,
            'max_age_days': 21, 'frequency': 'weekly', 'complete': complete,
            'freshness_basis': 'observation_date; publication timestamp is not a new observation'}


def canonical_fails(doc, now=None):
    c = inspect_donor(doc, 'data/settlement-fails.json', 72,
                      observed_paths=('treasury.as_of',), required_paths=('treasury',),
                      max_observation_age_hours=21*24, now=now)
    t = doc.get('treasury') or {}
    vals = [t.get(k) for k in ('ftd_bn', 'ftr_bn', 'gross_bn')]
    valid = (t.get('complete') is True and t.get('unit') == 'USD_bn_par'
             and t.get('quality', {}).get('status') == 'fresh'
             and all(isinstance(v, (int, float)) and not isinstance(v, bool)
                     and math.isfinite(v) and v >= 0 for v in vals))
    if valid:
        valid = abs(vals[0] + vals[1] - vals[2]) <= .03
    usable = c['usable'] and valid
    return {'source_artifact': 'data/settlement-fails.json',
            'producer_generated_at': doc.get('generated_at'),
            'status': 'fresh' if usable else 'unavailable', 'usable': usable,
            'treasury': copy.deepcopy(t) if usable else None,
            'contract': c, 'reason': None if usable else 'Canonical Treasury fails missing, stale, incomplete or inconsistent'}


def finalize(doc, fails, class_quality):
    doc.update(version='3.2.0', methodology_version=METHOD, execution_eligible=False,
               call=None, settlement_fails=canonical_fails(fails), class_quality=class_quality)
    doc['specific_issue_net_settled_b'] = doc['net_treasury_total_b']
    doc['net_treasury_total_b'] = None
    doc['field_definitions'] = {
        'net_treasury_total_b': 'Unavailable: SI specific issues do not cover the whole Treasury inventory.',
        'specific_issue_net_settled_b': 'Common-week SI issue ladder sum, USD bn; excludes other issues and bills.',
        'by_tenor_usd_b': 'Specific issues by original tenor, not maturity buckets of the entire dealer book.',
        'corporate.net_bonds_b': 'Eight IG/HY bond maturity levels, complete same-week sum; excludes commercial paper.',
        'transactions': 'FR2004B daily average transactions for the reporting week, USD bn/day.',
        'financing': 'Reported collateral financing balances; two-sided totals are not unique collateral.',
        'turnover_velocity': 'Unavailable: net positions are not gross inventory.',
    }
    q = observation_quality(doc.get('as_of'), bool(class_quality) and all(
        v['status'] == 'fresh' for v in class_quality.values()))
    doc['quality'] = q
    corp = doc.get('corporate')
    if corp:
        corp['quality'] = observation_quality(corp.get('as_of'), corp.get('components_aligned') is True)
        corp['turnover_velocity'] = None
        corp['squeeze_setup'] = None
        corp['read'] = ('Net corporate bond position %+.2fB; 5y+ %+.2fB, under 5y %+.2fB. '
                        'Cash bonds only; hedges and gross inventory are not measured. '
                        'Net short does not establish market-making capacity or a directional trade.' %
                        (corp['net_bonds_b'], corp['net_5yplus_b'], corp['net_under5y_b']))
        if corp['quality']['status'] != 'fresh':
            corp['regime'] = 'UNKNOWN'
    for row in doc.get('positions_ledger', {}).values():
        row['quality'] = observation_quality(row.get('as_of'))
    for row in doc.get('transactions', {}).values():
        row['quality'] = observation_quality(row.get('as_of'))
        row['daily_average_b'] = row.get('weekly_b')
        row['daily_average_4w_b'] = row.get('avg_4w_b')
        row['weekly_b'] = None
        row['avg_4w_b'] = None
        row['unit'] = 'USD_bn_per_day'
    if corp:
        corp['weekly_volume_b'] = None
    if doc.get('financing'):
        doc['financing']['quality'] = observation_quality(doc['financing'].get('as_of'), doc['financing'].get('components_aligned') is True)
        doc['financing']['scope'] = 'all_reported_collateral_classes'
        doc['financing']['read'] = 'Repo and reverse-repo balances on the same reporting date; not unique collateral or measured reuse.'
    return doc
