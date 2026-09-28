"""Independent rational check of admitted credit projection measurements."""
from fractions import Fraction
from datetime import datetime,timedelta,timezone,date
import hashlib,json


PAIRS={'ccc_minus_bb':('BAMLH0A3HYC','BAMLH0A1HYBB'),'hy_minus_ig':('BAMLH0A0HYM2','BAMLC0A0CM'),
    'bbb_minus_aaa':('BAMLC0A4CBBB','BAMLC0A1CAAA'),'em_hy_minus_us_hy':('BAMLEMHBHYCRPIOAS','BAMLH0A0HYM2')}


def stamp(value):
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Explicit verification timezone required')
    return result.astimezone(timezone.utc)


def current_source(packet,evaluated_at):
    """Separate calendar enumeration; do not call the projection clock helper."""
    generated=stamp(packet['generated_at']);at=stamp(evaluated_at);freshness=packet['freshness']
    if packet['version']=='2.0.0' and 'collection_policy' not in freshness:
        limit=min(generated+timedelta(hours=36),stamp(freshness['pipeline_check_due_at']))
    elif packet['version']=='2.1.0':
        start=generated.replace(hour=0,minute=0,second=0,microsecond=0)
        slots=[start+timedelta(days=d,minutes=m) for d in range(8) for m in (1200,1330)
               if (start+timedelta(days=d)).isoweekday()<=5]
        next_slot=min(t for t in slots if t>generated);limit=next_slot+timedelta(minutes=5)
        policy=freshness['collection_policy']
        if (policy['contract']!='credit-collection-cadence.v1' or policy['timezone']!='UTC'
                or policy['collection_times']!=['20:00','22:10'] or type(policy['completion_allowance_seconds']) is not int
                or policy['completion_allowance_seconds']!=300 or policy['weekdays']!=[0,1,2,3,4]
                or any(type(day) is not int for day in policy['weekdays'])
                or stamp(policy['next_collection_at'])!=next_slot or stamp(policy['pipeline_check_due_at'])!=limit
                or stamp(freshness['pipeline_check_due_at'])!=limit):raise ValueError('Independent collection schedule differs')
    else:raise ValueError('Unreviewed current-source version')
    return generated<=at<min(limit,stamp(freshness['valid_until']))


def verify(raw,view):
    packet=json.loads(raw);count=0
    if view['contract']!='bond-credit-comparisons.v1' or view['source']['sha256']!=hashlib.sha256(raw).hexdigest() or view['source']['bytes']!=len(raw):raise ValueError('Whole source binding differs')
    for flag in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'):
        if view[flag] is not False:raise ValueError('Unqualified authority')
    if type(view['independent_votes']) is not int or view['independent_votes']!=0 or view['decision']!={'verb':'WAIT','meaning':'abstain'}:raise ValueError('Credit vote differs')
    if set(view['comparisons'])!=set(PAIRS):raise ValueError('Complete credit comparison inventory required')
    for name,row in view['comparisons'].items():
        if (row['left_series_id'],row['right_series_id'])!=PAIRS[name]:raise ValueError('Comparison source identity differs')
        if any(row.get(k) is not False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')):raise ValueError('Comparison asserts unqualified authority')
        if row['status']=='unavailable':
            if row['value_bps'] is not None or row['value_decimal'] is not None or row['components'] or not row['reason'] or view['legacy_fields'][name+'_bps'] is not None:raise ValueError('Missing comparison manufactured a number')
            continue
        if row['status']!='dated_descriptive_comparison' or not current_source(packet,view['evaluated_at']):raise ValueError('Admitted comparison source is not current')
        a=packet['measurements'][row['left_series_id']];b=packet['measurements'][row['right_series_id']]
        at=stamp(view['evaluated_at'])
        if any(not 0<=(at.date()-date.fromisoformat(m['observation_date'])).days<=5 or at>=stamp(m['source_valid_until']) for m in (a,b)):
            raise ValueError('Admitted comparison observation expired')
        if row['unit']!='basis_points' or a['unit']!='percent' or b['unit']!='percent' or not a['observation_date']==b['observation_date']==row['observation_date']:raise ValueError('Same-date typed percent inputs required')
        expected=100*(Fraction(a['exact']['value_pct'])-Fraction(b['exact']['value_pct']))
        if Fraction(row['value_decimal'])!=expected or Fraction(str(row['value_bps']))!=expected or Fraction(str(packet['comparisons'][name]['value_bps']))!=expected:raise ValueError('Rational credit difference differs')
        if view['legacy_fields'][name+'_bps']!=row['value_bps']:raise ValueError('Legacy projection differs')
        count+=1
    return {'contract':'bond-credit-rational-check.v1','comparisons_checked':count,'unavailable_comparisons':len(view['comparisons'])-count,
        'checks_passed':True,'original_provider_replayed_here':False,'forecast_qualified':False}
