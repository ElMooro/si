"""Add-only weekly FR2004 context: Treasury incl-TIPS and ex-TIPS never mix."""
import json
import math
from datetime import datetime, timezone

KEY = 'data/settlement-fails.json'
NOTE = ('FTD + FTR is two-sided gross reported fails, not unique securities, '
        'defaults, capital flows or central-bank injection. The same failed '
        'settlement can appear on both sides and on successive days.')


def number(value):
    if isinstance(value, bool) or value is None: return None
    try:
        x = float(value)
        return x if math.isfinite(x) and x >= 0 else None
    except (ValueError, TypeError, OverflowError): return None


def project_scope(row, scope, combined_key, now):
    row = row if isinstance(row, dict) else {}
    as_of = row.get('as_of')
    try:
        observed = datetime.strptime(as_of, '%Y-%m-%d').date()
        age = (now.date() - observed).days
    except (TypeError, ValueError): age = None
    values = {key: number(row.get(src)) for key, src in
              [('ftd_bn','ftd_bn'), ('ftr_bn','ftr_bn'), ('combined_bn', combined_key)]}
    missing = [key for key, value in values.items() if value is None]
    source_quality = row.get('quality') if isinstance(row.get('quality'), dict) else {}
    declared_scope = row.get('scope_id') or row.get('scope')
    if declared_scope and declared_scope != scope: missing.append('scope_mismatch')
    if age is None or age < 0: missing.append('observation_date')
    if row.get('complete') is False: missing.append('incomplete_scope')
    if missing:
        status = 'unavailable'
    elif age > 14 or source_quality.get('status') == 'stale':
        status = 'stale'
    elif source_quality.get('status') not in ('fresh',):
        status = 'unverified'
    else: status = 'fresh'
    # Never substitute fields from the other scope. Keep partial observations
    # inspectable while explicitly withholding any eligibility claim.
    return {'scope_id': scope, 'as_of': as_of, 'unit': 'usd_bn', **values,
            'quality': {'status': status, 'observation_date': as_of,
                        'publication_date': source_quality.get('publication_date'),
                        'frequency': 'weekly', 'freshness_basis': 'weekly_observation',
                        'max_age_days': 14, 'age_days': age, 'missing': missing}}


def project(document, now=None):
    now = now or datetime.now(timezone.utc)
    document = document if isinstance(document, dict) else {}
    gross = project_scope(document.get('treasury'), 'treasury_incl_tips', 'gross_bn', now)
    headline = project_scope(document.get('headline'), 'ust_ex_tips', 'combined_bn', now)
    return {**gross, 'label': 'U.S. Treasury including TIPS — two-sided gross',
            'source': KEY, 'source_generated_at': document.get('generated_at'),
            'ust_ex_tips': {**headline, 'label': 'U.S. Treasury excluding TIPS — headline scope'},
            'note': NOTE, 'role': 'context_only', 'calls_eligible': False,
            'scope_note': 'Top-level values use Treasury including TIPS only. ust_ex_tips is a separate scope; do not add the scopes together.'}


def load(s3, bucket, now=None):
    try:
        doc = json.loads(s3.get_object(Bucket=bucket, Key=KEY)['Body'].read())
        return project(doc, now)
    except Exception as exc:
        out = project({}, now)
        out['quality']['reason'] = 'source_unavailable'
        out['quality']['error_type'] = type(exc).__name__
        return out


def input_quality(feeds, now):
    """Composite coverage audit; fresh wrapper timestamps do not qualify inputs."""
    details = {}
    for name, data in feeds.items():
        data = data if isinstance(data, dict) else {}
        quality = data.get('quality') if isinstance(data.get('quality'), dict) else {}
        stamp = data.get('generated_at') or data.get('as_of')
        try:
            at = datetime.fromisoformat(str(stamp).replace('Z','+00:00'))
            age_h = (now - at).total_seconds() / 3600 if at.tzinfo else None
        except ValueError: age_h = None
        status = quality.get('status', 'unverified') if data else 'unavailable'
        if age_h is None or age_h < -0.0833 or age_h > 48: status = 'stale' if age_h is not None and age_h > 48 else 'unverified'
        if status == 'fresh':
            try:
                observed = datetime.strptime(str(quality['observation_date'])[:10], '%Y-%m-%d').date()
                age_days = (now.date() - observed).days
                frequency = str(quality.get('frequency','daily')).lower()
                limit = 14 if frequency in ('w','weekly') else 62 if frequency in ('m','monthly') else 5
                if age_days < 0 or age_days > limit: status = 'stale'
            except (KeyError, ValueError, TypeError): status = 'unverified'
        details[name] = {'status': status, 'publication_date': stamp,
                         'observation_date': quality.get('observation_date'),
                         'publication_age_hours': round(age_h,2) if age_h is not None else None}
    missing = [name for name, row in details.items() if row['status'] != 'fresh']
    return {'status': 'fresh' if details and not missing else 'partial',
            'publication_date': now.isoformat(), 'observation_date': None,
            'frequency': 'mixed', 'freshness_basis': 'per_source_contract',
            'missing': missing, 'sources': details,
            'note': 'Coverage audit only. A fresh computation or an available legacy score does not prove predictive validity.'}
