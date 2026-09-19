"""Typed engine context only; an engine packet is not an original FR2004 response."""
from datetime import date, datetime, timezone
from decimal import Decimal
from report_observations import decimal


def project(document, at):
    document = document if isinstance(document, dict) else {}
    scopes = {}
    for name, identity, total in (('treasury','treasury_incl_tips','gross_bn'),
                                  ('headline','ust_ex_tips','combined_bn')):
        source=document.get(name) or {}; source=source if isinstance(source,dict) else {}
        declared=source.get('scope_id') if source.get('scope_id') is not None else source.get('scope')
        fields=source.get('field_units') or {}; fields=fields if isinstance(fields,dict) else {}
        values={key:decimal(source.get(key)) for key in ('ftd_bn','ftr_bn',total)}
        units={key:fields.get(key) for key in values}; reason=[]
        if declared!=identity: reason.append('scope_unverified')
        if not (all(v=='usd_bn' for v in units.values()) or
                source.get('unit')=='usd_bn' and all(v in (None,'usd_bn') for v in units.values())):
            reason.append('units_unverified')
        if any(v is None or v<0 for v in values.values()): reason.append('values_missing_or_invalid')
        elif abs(values['ftd_bn']+values['ftr_bn']-values[total])>Decimal('0.02'):
            reason.append('gross_reconciliation_failed')
        try: age=(at.date()-date.fromisoformat(source.get('as_of'))).days
        except (TypeError,ValueError): age=None
        if age is None or not 0<=age<=14: reason.append('observation_stale_or_missing')
        if source.get('complete') is False: reason.append('incomplete_scope')
        quality=source.get('quality') if isinstance(source.get('quality'),dict) else {}
        if quality.get('status')!='fresh': reason.append('source_not_fresh')
        scopes[name]={'scope_id':identity,'as_of':source.get('as_of'),'unit':source.get('unit'),
            'field_units':units,'ftd_bn':float(values['ftd_bn']) if values['ftd_bn'] is not None else None,
            'ftr_bn':float(values['ftr_bn']) if values['ftr_bn'] is not None else None,
            'combined_bn':float(values[total]) if values[total] is not None else None,
            'display_values':None if reason else [float(values[k]) for k in ('ftd_bn','ftr_bn',total)],
            'status':'withheld' if reason else 'context_only','reasons':reason,'original_provider_verified':False,
            'quality':{'status':'unverified','age_days':age,'max_age_days':14},'calls_eligible':False,'sizing_eligible':False}
    return {**scopes['treasury'],'ust_ex_tips':scopes['headline'],'source':'data/settlement-fails.json',
        'source_generated_at':document.get('generated_at'),'role':'context_only',
        'note':'Two-sided gross reported fails, not unique securities or defaults. Treasury including TIPS and excluding TIPS are separate scopes; never add them.'}
