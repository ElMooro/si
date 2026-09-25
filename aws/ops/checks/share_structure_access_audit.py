"""Bounded, recorded anonymous-access verification for the unstarted final batch."""
from concurrent.futures import ThreadPoolExecutor
import retained_access_evidence as access
import share_structure_sources as source


def verify(client, protected, part, report):
    if part != 6: raise ValueError('Only the unstarted final batch uses this verifier')
    keys = sorted(set(protected))
    if not 1 <= len(keys) <= 8000: raise ValueError('Reviewed access-check population required')
    for key in keys: access.urls(key)  # Validate the entire set before any request.
    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(access.check, keys))
    full = source.retain(client, source.encode({'contract': 'share-structure-access-evidence.v1',
        'part': part, 'method': 'HEAD', 'outcomes': outcomes}))
    outcomes.append(access.check(full['key']))
    summary = access.summarize(outcomes)
    report.kv(access_evidence=full, anonymous_access=summary)
    if not summary['all_denied']: raise ValueError('Inspect retained access outcomes; unknown responses are not denials')
    return {'access_evidence': full, 'anonymous_access': summary}
