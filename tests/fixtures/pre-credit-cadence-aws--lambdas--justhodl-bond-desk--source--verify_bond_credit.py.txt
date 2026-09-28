"""Independent rational check of admitted credit projection measurements."""
from fractions import Fraction
import hashlib,json


def verify(raw,view):
    packet=json.loads(raw);count=0
    if view['contract']!='bond-credit-comparisons.v1' or view['source']['sha256']!=hashlib.sha256(raw).hexdigest() or view['source']['bytes']!=len(raw):raise ValueError('Whole source binding differs')
    for flag in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'):
        if view[flag] is not False:raise ValueError('Unqualified authority')
    if type(view['independent_votes']) is not int or view['independent_votes']!=0 or view['decision']!={'verb':'WAIT','meaning':'abstain'}:raise ValueError('Credit vote differs')
    for name,row in view['comparisons'].items():
        if row['status']=='unavailable':
            if row['value_bps'] is not None or row['value_decimal'] is not None or row['components'] or not row['reason']:raise ValueError('Missing comparison manufactured a number')
            continue
        a=packet['measurements'][row['left_series_id']];b=packet['measurements'][row['right_series_id']]
        if row['unit']!='basis_points' or a['unit']!='percent' or b['unit']!='percent' or not a['observation_date']==b['observation_date']==row['observation_date']:raise ValueError('Same-date typed percent inputs required')
        expected=100*(Fraction(a['exact']['value_pct'])-Fraction(b['exact']['value_pct']))
        if Fraction(row['value_decimal'])!=expected or Fraction(str(row['value_bps']))!=expected or Fraction(str(packet['comparisons'][name]['value_bps']))!=expected:raise ValueError('Rational credit difference differs')
        if view['legacy_fields'][name+'_bps']!=row['value_bps']:raise ValueError('Legacy projection differs')
        count+=1
    return {'contract':'bond-credit-rational-check.v1','comparisons_checked':count,'unavailable_comparisons':len(view['comparisons'])-count,
        'checks_passed':True,'original_provider_replayed_here':False,'forecast_qualified':False}
