"""A required missing vote cannot renormalize into a complete world signal."""
import math

def required_region_composite(regions,weights):
    if set(regions)!=set(weights):raise ValueError('Complete required regional inventory')
    if any(not row.get('fresh') or type(row.get('score')) not in (int,float) or not math.isfinite(row['score'])
           or row.get('calls_eligible') is False for row in regions.values()):return None,'UNAVAILABLE',None
    world=round(sum(weights[k]*regions[k]['score'] for k in weights)/sum(weights.values()),1)
    regime='STRESS' if world>=75 else 'ANXIOUS' if world>=60 else 'UNEASY' if world>=45 else 'CALM'
    return world,regime,max(regions.items(),key=lambda kv:kv[1]['score'])
