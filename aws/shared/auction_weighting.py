"""Complete-weight auction heuristic aggregation; never a forecasting validation."""
import math


def weighted_summary(rows):
    """Unknown size withholds the window; a measured zero contributes no weight."""
    def number(value):
        if type(value) not in (int,float):return None
        try:return float(value) if math.isfinite(value) else None
        except (ValueError,OverflowError):return None
    missing_weights=missing_scores=zero_weights=0
    weights=[];products=[]
    for row in rows:
        weight=number(row.get('accepted_billions'))
        if weight is None or weight<0:
            missing_weights+=1;continue
        weights.append(weight)
        if weight==0:
            zero_weights+=1;continue
        score=number(row.get('composite_score'))
        if score is None or not 0<=score<=100:
            missing_scores+=1;continue
        products.append(weight*score)
    try:total=math.fsum(weights);numerator=math.fsum(products)
    except (OverflowError,ValueError):total=numerator=None
    finite=total is not None and math.isfinite(total) and math.isfinite(numerator)
    status=('empty' if not rows else 'missing_weight' if missing_weights else 'missing_score' if missing_scores
            else 'nonfinite_aggregate' if not finite else 'no_positive_weight' if total<=0 else 'complete')
    return {'composite':numerator/total if status=='complete' else None,
        'status':status,'n_observations':len(rows),'n_missing_weights':missing_weights,
        'n_missing_scores':missing_scores,'n_zero_weights':zero_weights,
        'known_weight_total_usd_bn':total if finite else None,
        'weight_scope':'accepted auction amount; no missing-size imputation',
        'calls_eligible':False,'sizing_eligible':False}
