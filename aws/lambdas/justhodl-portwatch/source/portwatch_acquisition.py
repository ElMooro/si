"""Whole ArcGIS query membership and calendar-aware cohort acquisition.

Membership is enumerated, not inferred from a short page. Separate requests
are not an atomic provider snapshot, and old revisions remain unqualified.
"""
from datetime import timedelta
from hashlib import sha256
import json
from portwatch_measurements import source_date

CONTRACT = 'portwatch-query-membership.v1'
OBJECT_FIELD = 'ObjectId'  # All five retained, reviewed provider layer schemas.
CHUNK = 1000
ID_LIMIT = 1000000  # Esri's documented ID-array ceiling; count reconciliation required.


class AcquisitionError(ValueError):
    pass


def identifier(value):
    return type(value) is int and 0 <= value <= 9007199254740991


def all_features(query, url, where, remaining, reviews):
    """Retain via the caller, reconcile count -> IDs -> every complete feature."""
    def ask(params):
        if remaining() <= 0:
            raise AcquisitionError('Request budget cannot complete this query; no partial publication')
        result = query(url, params)
        if not isinstance(result, dict) or result.get('_err') or result.get('error'):
            raise AcquisitionError('Whole provider query failed')
        if result.get('exceededTransferLimit') is not None and result.get('exceededTransferLimit') is not False:
            raise AcquisitionError('Provider declares incomplete query membership')
        return result

    count = ask({'where': where, 'returnCountOnly': 'true'}).get('count')
    if not identifier(count) or count > ID_LIMIT:
        raise AcquisitionError('Declared count outside reviewed ID-array capacity')
    # Do not start feature-page acquisition unless all declared pages fit.
    if 1 + (count + CHUNK - 1) // CHUNK > remaining():
        raise AcquisitionError('Complete query exceeds remaining request budget')
    listing = ask({'where': where, 'returnIdsOnly': 'true'})
    ids = listing.get('objectIds')
    if (listing.get('objectIdFieldName') != OBJECT_FIELD or not isinstance(ids, list)
            or any(not identifier(i) for i in ids) or len(ids) != count or len(set(ids)) != count):
        raise AcquisitionError('Declared count and unique object identities differ')
    ids = sorted(ids)
    rows = []
    for start in range(0, len(ids), CHUNK):
        chunk = ids[start:start + CHUNK]
        packet = ask({'where': where, 'objectIds': ','.join(map(str, chunk)),
                      'outFields': '*', 'returnGeometry': 'false'})
        features = packet.get('features')
        if (packet.get('objectIdFieldName', OBJECT_FIELD) != OBJECT_FIELD
                or not isinstance(features, list) or len(features) != len(chunk)):
            raise AcquisitionError('Complete feature batch required')
        found = {}; expected = set(chunk)
        for feature in features:
            row = feature.get('attributes') if isinstance(feature, dict) else None
            ident = row.get(OBJECT_FIELD) if isinstance(row, dict) else None
            if not identifier(ident) or ident in found or ident not in expected:
                raise AcquisitionError('Duplicate, missing or foreign feature identity')
            found[ident] = row
        rows.extend(found[ident] for ident in chunk)
    reviews.append({'url': url, 'where': where, 'declared_count': count,
                    'enumerated_ids': len(ids), 'returned_rows': len(rows),
                    'object_id_field': OBJECT_FIELD,
                    'object_ids_sha256': sha256(json.dumps(ids, separators=(',', ':')).encode()).hexdigest(),
                    'membership_reconciled': True, 'provider_snapshot_atomic': False})
    return rows


def cohort_start(store, entity_ids, first, today):
    """Earliest missing date or three-day revision overlap for EACH queried ID.

An observed null still proves a row exists; it does not become zero. Existing
future/malformed rows cannot advance the acquisition watermark. Complete
original rows remain in the retained ledger regardless of this query window.
"""
    entity_ids = sorted(set(entity_ids))
    if not entity_ids or first > today:
        raise AcquisitionError('Identified calendar cohort required')
    per = {ident: set() for ident in entity_ids}
    for key, row in store.items():
        ident, sep, day = key.partition('|')
        if ident not in per:
            continue
        observed = source_date(row.get('date'))
        if (sep and source_date(day) == observed and observed is not None
                and row.get('portid', row.get('chokepoint_id')) == ident and first <= observed <= today):
            per[ident].add(observed)
    starts = []
    for dates in per.values():
        if not dates:
            starts.append(first)
            continue
        candidate = first
        while candidate <= today and candidate in dates:
            candidate += timedelta(days=1)
        starts.append(max(first, min(candidate, max(dates) - timedelta(days=3))))
    return min(starts)


def quoted_ids(ids):
    if not ids or any(not isinstance(i, str) or not i for i in ids):
        raise AcquisitionError('Nonempty provider string identities required')
    return ','.join("'" + i.replace("'", "''") + "'" for i in sorted(set(ids)))


def merge_rows(store, rows):
    """Validate all identities before mutation; duplicate day rows cannot overwrite."""
    additions = {}
    for row in rows:
        if not isinstance(row, dict):
            raise AcquisitionError('Complete observation attributes required')
        ident = row.get('portid', row.get('chokepoint_id'))
        day = source_date(row.get('date'))
        if not isinstance(ident, str) or not ident or '|' in ident or day is None:
            raise AcquisitionError('Observation identity or date is malformed')
        key = ident + '|' + day.isoformat()
        if key in additions:
            raise AcquisitionError('Multiple provider rows for the same entity and date')
        additions[key] = row
    store.update(additions)
    return len(additions)
