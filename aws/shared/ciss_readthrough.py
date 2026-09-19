"""Source-bound ECB context. A measurement never grants trading authority."""
from datetime import datetime, timezone
import math
import re

from ciss_source_model import CONTRACT, HEAD, clock, digest, row_is_current

KEY = 'data/ciss-stress.json'
SERIES = {'ciss': HEAD,
          'ciss_mm': 'CISS.D.U2.Z0Z.4F.EC.SS_MMN.CON',
          'ciss_bond': 'CISS.D.U2.Z0Z.4F.EC.SS_BMN.CON',
          'ciss_equity': 'CISS.D.U2.Z0Z.4F.EC.SS_EMN.CON'}


def context(doc, now=None, series_key=HEAD):
    """Recheck packet binding, series identity and both source clocks.

    The immutable source run permits reproduction of the displayed value.
    Distribution percentiles describe this exact series' retrieved history;
    they do not validate a short exposure, risk-on vote or composite floor.
    """
    now = now or datetime.now(timezone.utc)
    result = {'contract': 'ciss-readthrough.v1', 'source_key': KEY,
              'series_id': series_key, 'status': 'unavailable', 'value': None,
              'value_decimal': None, 'observation_date': None,
              'source_publication_at': None, 'acquired_at': None,
              'warehouse_generated_at': None, 'evidence': None,
              'source_replay': None, 'unit': None, 'percentile': None,
              'regime': None, 'call': None, 'calls_eligible': False,
              'sizing_eligible': False, 'vote_eligible': False,
              'score_contribution': 0, 'evidence_family': 'ecb_ciss',
              'independent_votes': 0, 'errors': [],
              'read': 'A current, source-bound ECB CISS measurement is unavailable.',
              'use': 'Dated research context only. No calibrated link to returns, short exposure, risk votes or position size.'}
    if not isinstance(doc, dict):
        result['errors'].append('source packet missing'); return result
    try:
        ref = doc['replay']
        if doc.get('contract') != CONTRACT or not re.fullmatch(r'data/ciss-research/runs/[a-f0-9]{64}\.json', ref['manifest_key']):
            raise ValueError('canonical source identity missing')
        if digest({k:v for k,v in doc.items() if k != 'replay'}) != ref['output_sha256']:
            raise ValueError('source packet digest differs')
        rows = doc['series']
        matches = [r for r in rows if isinstance(r, dict) and r.get('key') == series_key]
        if len(matches) != 1: raise ValueError('exact series must occur once')
        row = matches[0]
        unit = 'dimensionless_contribution' if series_key.endswith('.CON') else 'dimensionless_index'
        if row.get('unit') != unit: raise ValueError('series unit differs')
        meta = row['source_metadata']
        dimensions = ('FREQ','REF_AREA','CURRENCY','PROVIDER_FM','INSTRUMENT_FM','PROVIDER_FM_ID','DATA_TYPE_FM')
        if 'CISS.'+'.'.join(meta[k] for k in dimensions) != series_key or meta['UNIT'] != 'PURE_NUMB' or meta['UNIT_MULT'] != '0':
            raise ValueError('source dimensions differ')
        evidence=row['evidence']
        source_path='data/evidence/ecb/[a-f0-9]{64}/'+str(evidence.get('sha256',''))+r'\.bin\.gz'
        if (evidence.get('captured') is not True or evidence.get('provider')!='ecb'
                or not re.fullmatch(r'[a-f0-9]{64}',str(evidence.get('sha256','')))
                or not re.fullmatch(source_path,str(evidence.get('key','')))
                or not isinstance(row.get('source_row'),int) or row['source_row']<0):
            raise ValueError('retained original observation evidence required')
        q = row['quality']
        result.update(warehouse_generated_at=doc['generated_at'], source_replay=ref,
                      observation_date=row['latest_date'], period_end=row['observation_period_end'],
                      acquired_at=row['acquired_at'], unit=unit, evidence=row.get('evidence'),
                      source_row=row.get('source_row'), source_definition=meta,
                      maximum_observation_age_days=q['maximum_observation_age_days'],
                      maximum_acquisition_age_seconds=q['maximum_acquisition_age_seconds'],
                      distribution_definition=row.get('distribution_definition'))
        age = (now-clock(doc['generated_at'])).total_seconds()
        if not 0 <= age <= 72*3600:
            result['status'] = 'stale' if age > 0 else 'invalid'
            result['errors'].append('warehouse clock expired or future'); return result
        if q.get('status') != 'fresh' or not row_is_current(row, now.isoformat()):
            result['status'] = q.get('status') if q.get('status') != 'fresh' else 'stale'
            result['errors'].append('source observation or acquisition unavailable'); return result
        reconciliation = doc.get('headline_reconciliation') or {}
        if reconciliation.get('status') != 'matched' or reconciliation.get('date') != row['latest_date']:
            raise ValueError('same-date headline contribution reconciliation required')
        value = row['latest']
        if isinstance(value, bool) or not isinstance(value, (int,float)) or not math.isfinite(value):
            raise ValueError('finite source value required')
        if unit == 'dimensionless_index' and not 0 <= value <= 1:
            raise ValueError('index outside defined range')
        if float(row['latest_decimal']) != value: raise ValueError('source decimal differs')
        pct = row.get('pctile')
        if pct is not None and (isinstance(pct,bool) or not isinstance(pct,(int,float)) or not math.isfinite(pct) or not 0 <= pct <= 100):
            raise ValueError('invalid descriptive percentile')
        label = 'CISS contribution' if unit == 'dimensionless_contribution' else 'CISS index'
        result.update(status='fresh', value=value, value_decimal=row['latest_decimal'], percentile=pct,
                      read=f"ECB {label} {row['latest_decimal']} on {row['latest_date']}; research context only.")
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        result.update(status='unverified', value=None, value_decimal=None, percentile=None)
        result['errors'].append('canonical packet, exact series or source evidence did not qualify')
    return result
