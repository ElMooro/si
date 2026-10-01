"""Bounded manifest checks for each resident search process, without freshness claims."""
import math

from directory_index import IndexIntegrityError, clock

CHECK_SECONDS = 300
RETRY_SECONDS = 30


def identity(cache):
    binding = cache.get('index_integrity') or {}
    return (cache.get('built_at'), binding.get('generation'), binding.get('docs_sha256'))


def elapsed(now, stamp, mono, previous):
    if type(mono) not in (int, float) or not math.isfinite(mono):
        raise IndexIntegrityError('Finite resident-cache clock required')
    if type(previous) not in (int, float) or not math.isfinite(previous) or stamp is None:
        return None
    wall = (clock(now) - clock(stamp)).total_seconds()
    steady = mono - previous
    if wall < 0 or steady < 0:
        return None
    # Either clock advancing expires a check, including host/process freeze.
    return max(wall, steady)


def load(cache, state, refresh, read_head, needs_refresh, iso, monotonic, *, force=False, check=False):
    now, tick = iso(), monotonic()
    clock(now)
    if type(tick) not in (int, float) or not math.isfinite(tick):
        raise IndexIntegrityError('Finite resident-cache clock required')
    ready = cache.get('docs') is not None
    if ready and not force and not check and state.get('identity') == identity(cache):
        age = elapsed(now, state.get('attempted_at'), tick, state.get('attempted_tick'))
        bound = RETRY_SECONDS if state.get('error_type') else CHECK_SECONDS
        if age is not None and age < bound:
            return cache
    try:
        reload = force or not ready
        if not reload:
            head = read_head()
            if clock(head.get('built_at')) > clock(now):
                raise IndexIntegrityError('Directory head clock is in the future')
            reload = needs_refresh(head, cache)
        if reload:
            refresh()
        if cache.get('docs') is None:
            raise IndexIntegrityError('Directory refresh returned no resident population')
    except Exception as exc:
        # Cache bytes/metadata are managed by refresh's existing rollback.
        # Record the failed check separately; do not relabel the old graph.
        state.update(attempted_at=iso(), attempted_tick=monotonic(),
                     error_type=type(exc).__name__, identity=identity(cache), reloaded=False)
        if force or check or not ready or cache.get('docs') is None:
            raise
        return cache
    state.clear()
    state.update(attempted_at=now, attempted_tick=tick, checked_at=now,
                 checked_tick=tick, identity=identity(cache), reloaded=reload)
    return cache


def evidence(cache, state, iso, monotonic):
    """Storage-selection evidence only; source observation freshness stays separate."""
    now, tick = iso(), monotonic()
    result = dict(cache.get('index_integrity') or {})
    checked = elapsed(now, state.get('checked_at'), tick, state.get('checked_tick'))
    attempted = elapsed(now, state.get('attempted_at'), tick, state.get('attempted_tick'))
    same = state.get('identity') == identity(cache)
    valid = same and not state.get('error_type') and checked is not None and checked < CHECK_SECONDS
    failed = same and bool(state.get('error_type'))
    result['head_check'] = {
        'status': 'checked_within_interval' if valid else ('unavailable' if failed else 'check_due'),
        'checked_at': state.get('checked_at'),
        'attempted_at': state.get('attempted_at'),
        'check_interval_s': CHECK_SECONDS,
        'retry_interval_s': RETRY_SECONDS,
        'age_s': round(checked, 3) if checked is not None else None,
        'error_type': state.get('error_type'),
        'serving_cached_generation': not valid,
        'source_freshness_verified': False,
    }
    # Do not let HTTP caching extend the process's successful-check window.
    ttl = max(0, min(120, int(CHECK_SECONDS - checked))) if valid else 0
    result['head_check']['http_max_age_s'] = ttl
    result['head_check']['retry_after_s'] = max(0, math.ceil(RETRY_SECONDS-attempted)) if failed and attempted is not None else 0
    return result
