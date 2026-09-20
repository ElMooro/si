"""Typed arithmetic and explicit limits for the legacy domain research model."""
from datetime import datetime, timezone
import math
import json
import re

CONTRACT = 'domain-rule-monitor.v1'


def finite(value):
    # Provider strings, booleans, infinities and NaNs do not become observations.
    if isinstance(value, bool) or not isinstance(value, (int, float)): return None
    return value if math.isfinite(value) else None


def change(row, previous):
    """Prefer a signed level difference; no percentage division across negative/zero levels."""
    current, prior = finite(row.get('value')), finite(row.get('prev'))
    if current is not None and prior is not None:
        result = current-prior
        return (result, 'row_level_difference') if finite(result) is not None else (None, 'nonfinite_difference')
    # A reported change is retained as such, not reconstructed from a guessed ledger period.
    reported = finite(row.get('chg_pct'))
    if current is not None and reported is not None: return reported, 'provider_reported_percent'
    # Old ledger entries have no per-series unit/vintage/observation identity. Never
    # silently compare a current revised/rescaled level with that unknown baseline.
    return None, 'comparison_unavailable'


def change_unit(row, basis):
    if basis == 'provider_reported_percent': return 'percent (provider reported)'
    unit = row.get('unit')
    if unit == '% YoY': return 'percentage_points'
    return unit if isinstance(unit, str) and unit.strip() else 'source_units_unverified'


def clock(value):
    if not isinstance(value, str): return None
    try:
        dt = datetime.fromisoformat(value.replace('Z','+00:00'))
        return dt if dt.tzinfo is not None else None
    except ValueError: return None


def publication_quality(sources, now):
    clocks = {}
    for name, doc in sources.items():
        stamp = doc.get('generated_at') if isinstance(doc, dict) else None
        parsed = clock(stamp)
        age = (now-parsed).total_seconds() if parsed else None
        clocks[name] = {'generated_at': stamp, 'age_seconds': None if age is None else round(age,3),
                        'status': 'missing_clock' if age is None else 'future_clock' if age < 0 else 'reported_collection_clock',
                        'observation_freshness_verified': False}
    return {'status':'unvalidated_monitor','source_collection_clocks':clocks,
            'observation_freshness_verified':False,'independence_validated':False,
            'reason':'Note-derived polarity rules, mixed observation periods and inherited provider changes are descriptive. No forecast or allocation qualification.'}


def permissions():
    return {'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'may_recommend_trades':False}


def source_root(row):
    source=row.get('source')
    if not isinstance(source,str):return None
    match=re.fullmatch(r'(fred(?:_alias|_yoy)?|yahoo|polygon|fmp):([A-Za-z0-9_^./=-]+)',source)
    if not match:return None
    provider='fred' if match[1].startswith('fred') else match[1]
    return provider+':'+match[2]


def root_admission(rows, domains, categories, polarity, asset_categories, asset_symbols):
    """Deduplicate named provider series; never choose among conflicting vintages."""
    groups, result = {}, {}
    for row in rows:
        sym=row['symbol'];category=categories.get(sym)
        if row.get('status')!='LIVE' or category in asset_categories or sym in asset_symbols:continue
        pol=polarity(sym,category)[0]
        if not pol:continue
        root=source_root(row)
        if root is None:
            result[id(row)]={'source_root':None,'vote_eligible':False,'exclusion':'unidentified_provider_series'};continue
        delta,basis=change(row,{})
        signature=json.dumps({'domain':domains.get(sym),'polarity':pol,'current':finite(row.get('value')),
            'prior':finite(row.get('prev')),'change':delta,'basis':basis,'unit':row.get('unit'),
            'observation':row.get('observation_date') or row.get('asof'),'comparison':row.get('previous_observation_date'),
            'transform':row.get('contract_version'),'frequency':row.get('frequency')},sort_keys=True,default=str)
        groups.setdefault(root,[]).append((row,signature))
    for root,items in groups.items():
        conflict=len({signature for _,signature in items})>1
        chosen=min((row for row,_ in items),key=lambda row:row['symbol'])
        for row,_ in items:
            reason='conflicting_alias_vintage_or_rule' if conflict else 'duplicate_provider_series' if row is not chosen else None
            result[id(row)]={'source_root':root,'vote_eligible':reason is None,'exclusion':reason}
    return result
