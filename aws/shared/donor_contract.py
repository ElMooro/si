"""Read-only contracts for cross-engine donors; never infer freshness from a reader run.

Receivers choose frequency-appropriate SLAs and the exact fields needed by a
calculation. Units document the inspected producer schema; no numeric conversion
or calibration weight is silently applied here.
"""
import calendar
import math
import re
from datetime import datetime, timezone


_GENERATED_PATHS = ('generated_at','updated_at','updated','as_of','timestamp','asof')


def get_path(doc, path, default=None):
    """Dotted dict keys/list indices; '*' maps the remaining path over a collection."""
    parts = str(path).split('.') if path else []
    def descend(value, index):
        if index == len(parts):
            return value
        part = parts[index]
        if part == '*':
            values = list(value.values()) if isinstance(value, dict) else value if isinstance(value, list) else []
            return [descend(item, index + 1) for item in values]
        if isinstance(value, dict):
            return descend(value[part], index + 1) if part in value else default
        if isinstance(value, list):
            try:
                return descend(value[int(part)], index + 1)
            except (ValueError, IndexError):
                return default
        return default
    return descend(doc, 0)


def numeric(doc, path=None):
    value = get_path(doc, path) if path is not None else doc
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError, OverflowError):
        return None


def parse_timestamp(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, (float, int)):
            if not math.isfinite(value):
                return None
            return datetime.fromtimestamp(value / 1000 if value > 1e11 else value, tz=timezone.utc)
        text = str(value).strip()
        quarter = re.fullmatch(r'(\d{4})[-:]?Q([1-4])', text)
        if quarter:
            year, month = int(quarter[1]), int(quarter[2]) * 3
            return datetime(year, month, calendar.monthrange(year,month)[1], tzinfo=timezone.utc)
        if re.fullmatch(r'\d{4}-\d{2}', text):
            year, month = map(int,text.split('-'))
            return datetime(year,month,calendar.monthrange(year,month)[1],tzinfo=timezone.utc)
        result = datetime.fromisoformat(text.replace('Z','+00:00'))
        if result.tzinfo is None:
            # Calendar observation dates are date-only; undated local clock times are ambiguous.
            if len(text) != 10:
                return None
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def inspect_donor(doc, artifact, max_age_hours, observed_paths=(), required_paths=(), units=None,
                  now=None, max_observation_age_hours=None):
    """Inspect availability and observation ages without returning synthetic defaults.

    observed_paths are ordered aliases for the relevant observation time. Required
    fields must exist and be nonempty (numeric zero remains valid). The returned
    fields preserve original values; callers still validate domain ranges/types.
    """
    now = now or datetime.now(timezone.utc)
    result = {'schema_version':'1.0','source_artifact':artifact,'status':'MISSING','usable':False,
              'generated_at':None,'observed_at':None,'age_hours':None,'observation_age_hours':None,
              'fields':{},'units':units,'errors':[]}
    if not isinstance(doc,dict) or not doc:
        result['errors']=['donor missing or not an object']
        return result
    errors=[]
    if doc.get('ok') is False or doc.get('status') in ('ERROR','INVALID','MISSING','BLOCKED'):
        errors.append('producer reports unavailable data')
    generated=None
    for path in _GENERATED_PATHS:
        value=get_path(doc,path)
        if value is not None:
            generated=parse_timestamp(value)
            if generated is None:
                errors.append('invalid generation timestamp: '+path)
            else:
                result['generation_timestamp_path']=path
            break
    if generated is None:
        errors.append('generation timestamp unavailable')
    observed=None
    for path in observed_paths:
        value=get_path(doc,path)
        if value is not None:
            observed=parse_timestamp(value)
            if observed is None:
                errors.append('invalid observation timestamp: '+path)
            else:
                result['observation_timestamp_path']=path
            break
    if observed_paths and observed is None:
        errors.append('observation timestamp unavailable')
    for path in required_paths:
        value=get_path(doc,path)
        result['fields'][path]=value
        if value is None or value == '' or value == [] or value == {}:
            errors.append('required field missing/empty: '+path)
        elif isinstance(value,(int,float)) and not isinstance(value,bool) and not math.isfinite(value):
            errors.append('invalid numeric field: '+path)
    if isinstance(units,str) and doc.get('units') is not None and doc['units'] != units:
        errors.append('producer units mismatch')
    sla=numeric(max_age_hours)
    obs_sla=numeric(max_observation_age_hours if max_observation_age_hours is not None else max_age_hours)
    if sla is None or sla <= 0 or obs_sla is None or obs_sla <= 0:
        errors.append('invalid freshness SLA')
    if generated is not None:
        result['generated_at']=generated.isoformat()
        result['age_hours']=round((now-generated).total_seconds()/3600,4)
        if result['age_hours'] < -5/60:
            errors.append('future generation timestamp')
    if observed is not None:
        result['observed_at']=observed.isoformat()
        result['observation_age_hours']=round((now-observed).total_seconds()/3600,4)
        if result['observation_age_hours'] < -5/60:
            errors.append('future observation timestamp')
        if generated is not None and observed > generated:
            errors.append('observation timestamp after publication')
    if errors:
        result.update(status='INVALID',errors=errors)
    elif result['age_hours'] > sla or (observed is not None and result['observation_age_hours'] > obs_sla):
        result.update(status='STALE',errors=['donor publication or observation exceeds SLA'])
    else:
        result.update(status='FRESH',usable=True)
    return result
