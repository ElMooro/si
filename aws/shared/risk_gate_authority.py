"""Risk Gate source context is not a qualified capital-allocation model."""
from datetime import datetime,timezone
import math


def context(doc,now=None,max_age_h=36):
    """Fail closed for missing, old and unqualified models, including forged flags.

    No Risk Gate model currently has an approved, content-bound qualification
    protocol. A future protocol needs a reviewed verifier here, not a JSON flag.
    This is an abstention from new sizing, never an order to liquidate a book.
    """
    now=now or datetime.now(timezone.utc)
    out={'posture':'UNAVAILABLE','composite':None,'sizing_multiplier':None,
         'calls_eligible':False,'sizing_eligible':False,'allows_new_entries':False,
         'forced_liquidation':False,'qualification_status':'UNQUALIFIED',
         'note':'Risk Gate has no qualified portfolio-sizing model; WAIT means abstain.'}
    if not isinstance(doc,dict):out['source_status']='unavailable';return out
    try:
        stamp=datetime.fromisoformat(doc['generated_at'].replace('Z','+00:00'))
        if stamp.tzinfo is None:raise ValueError('timezone required')
        age=(now-stamp).total_seconds()/3600
        if not math.isfinite(age) or age<0 or age>max_age_h:raise ValueError('outside age ceiling')
    except (KeyError,TypeError,ValueError,AttributeError):
        out['source_status']='invalid_or_expired';return out
    declared=doc.get('sizing_multiplier')
    valid=isinstance(declared,(int,float)) and not isinstance(declared,bool) and math.isfinite(declared) and 0<=declared<=1
    out.update(source_status='current_packet',generated_at=doc['generated_at'],source_replay=doc.get('replay'),
               declared_posture=doc.get('posture') if isinstance(doc.get('posture'),str) else None,
               declared_sizing_multiplier=declared if valid else None)
    return out


def read(client,bucket):
    """Read once per caller invocation; never cache authority across warm runs."""
    import json
    try:
        raw=client.get_object(Bucket=bucket,Key='data/risk-gate.json')['Body'].read(4*1024*1024+1)
        if len(raw)>4*1024*1024:raise ValueError('packet exceeds bound')
        return context(json.loads(raw))
    except Exception:
        return context(None)
