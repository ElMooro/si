"""Native ACM model observations, without predictive or sizing permission."""
from copy import deepcopy
import term_premium_candidate as arithmetic
import verify_term_premium_arithmetic as independent

CONTRACT='term-premium-research.v1'
PREFIX='data/term-premium-research/'
CURRENT='data/term-premium.json'
encoded=arithmetic.encoded
digest=lambda value:arithmetic.digest(encoded(value))
clock=arithmetic.clock


def continuity(packet):
    values=deepcopy(packet['source_watermarks'])
    clock(values['acquired_at'])
    if set(values['observations'])!=set(arithmetic.SHEETS):raise ValueError('Complete observation watermark inventory required')
    for day in values['observations'].values():
        if day is not None:arithmetic.date.fromisoformat(day)
    return values


def build(raw,source,stamp,previous,predecessors,acquisition):
    if acquisition.get('status') not in ('acquired','failed'):raise ValueError('Explicit acquisition result required')
    output=arithmetic.build(raw,source,stamp);proof=independent.verify(output,raw)
    output.pop('candidate_only');output.pop('publication_eligible');output['contract']=CONTRACT
    output.update(predecessors=deepcopy(predecessors),acquisition=deepcopy(acquisition),original_arithmetic_checks=proof)
    latest={name:max((row['observation_date'] for row in table['rows'] if row['observation_date']<=str(clock(source['acquired_at']).date())),default=None)
        for name,table in output['tables'].items()}
    previous=previous or {'acquired_at':source['acquired_at'],'observations':latest}
    if set(previous['observations'])!=set(arithmetic.SHEETS) or clock(previous['acquired_at'])>clock(stamp):
        raise ValueError('Complete past source watermarks required')
    output['source_watermarks']={'acquired_at':max((previous['acquired_at'],source['acquired_at']),key=clock),
        'observations':{name:max((day for day in (latest[name],previous['observations'][name]) if day is not None),default=None) for name in latest}}
    for row in output['series'].values():
        old=previous['observations'][row['table']];now=latest[row['table']]
        reason=('acquisition_failed' if acquisition['status']=='failed' else
            'source_regression' if clock(source['acquired_at'])<clock(previous['acquired_at']) or old and (not now or now<old) else None)
        if reason:row.update(current=None,current_comparisons=None);row['quality']['status']=reason
    current=sum(row['current'] is not None for row in output['series'].values())
    output['quality'].update(current_series=current,status='within_age_ceiling' if current==60 else 'degraded' if current else 'unavailable')
    return output


def compact(output,tables):
    if set(tables)!=set(arithmetic.SHEETS):raise ValueError('Both complete worksheet artifacts required')
    result={key:deepcopy(value) for key,value in output.items() if key!='tables'}
    result['tables']={name:{**{key:deepcopy(value) for key,value in table.items() if key!='rows'},
        'complete_table_artifact':tables[name],'retained_data_rows':len(table['rows']),
        'first_date':min(row['observation_date'] for row in table['rows']),
        'last_date':max(row['observation_date'] for row in table['rows'])} for name,table in output['tables'].items()}
    result['view']={'contract':'term-premium-summary.v1','complete_worksheets':2,
        'complete_original_data_rows':sum(len(table['rows']) for table in output['tables'].values()),
        'scope':'Every measurement and comparison; both whole worksheets load by immutable reference.'}
    return result
