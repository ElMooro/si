"""Complete source membership and exact matched-calendar shipment comparisons.

AIS-derived shipment weights are estimates. No customs-value, holiday-adjusted,
revenue, forecast or portfolio interpretation is qualified by this arithmetic.
"""
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json, math, re

BASE = 'https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/'
DAILY = BASE + 'Daily_Ports_Data/FeatureServer/0'
CATALOG = BASE + 'PortWatch_ports_database/FeatureServer/0'
CONTRACT = 'port-cargo-calendar-measurements.v1'
MAX_EXACT = 9007199254740991
CHUNK = 1000
METHOD = 'https://www.imf.org/en/publications/wp/issues/2025/05/16/nowcasting-global-trade-from-space-566957'


def source_date(value):
    try:
        if type(value) in (int, float):
            if not math.isfinite(value) or value % 86400000: return None
            return datetime.fromtimestamp(value / 1000, timezone.utc).date()
        if isinstance(value, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
            return date.fromisoformat(value)
        if isinstance(value, str) and 'T' in value:
            stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if stamp.tzinfo is None: return None
            stamp = stamp.astimezone(timezone.utc)
            if any((stamp.hour, stamp.minute, stamp.second, stamp.microsecond)): return None
            return stamp.date()
    except (ValueError, OverflowError, OSError): pass
    return None


def integer(value):
    return type(value) is int and 0 <= value <= MAX_EXACT


def amount(value):
    return type(value) in (int, float) and math.isfinite(value) and 0 <= value <= MAX_EXACT and value == int(value)


def schema(packet, name, daily=False):
    required = {'ObjectId': 'esriFieldTypeOID', 'portid': 'esriFieldTypeString',
                'portname': 'esriFieldTypeString', 'country': 'esriFieldTypeString', 'ISO3': 'esriFieldTypeString'}
    if daily: required.update(date='esriFieldTypeDateOnly', **{'import': 'esriFieldTypeInteger', 'export': 'esriFieldTypeInteger'})
    fields = packet.get('fields')
    if (packet.get('name') != name or packet.get('objectIdField') != 'ObjectId'
            or type(packet.get('maxRecordCount')) is not int or packet['maxRecordCount'] < CHUNK
            or not isinstance(fields, list) or any(not isinstance(f, dict) or not isinstance(f.get('name'), str) for f in fields)):
        raise ValueError('Unreviewed complete source layer schema')
    observed = {f['name']: f.get('type') for f in fields}
    if len(observed) != len(fields) or any(observed.get(k) != v for k, v in required.items()):
        raise ValueError('Source field identity or type differs from reviewed layer')


def acquire(query, layer, at, remaining):
    if layer != DAILY + '/query':
        raise ValueError('Only reviewed Daily_Ports_Data source is qualified')
    clock = datetime.fromisoformat(at.replace('Z', '+00:00'))
    if clock.tzinfo is None: raise ValueError('Aware acquisition clock required')
    reviews = []
    def ask(url, params, limited=False):
        if remaining() <= 0: raise ValueError('No request budget for complete source query')
        packet, error = query(url, {**params, 'f': 'pjson'})
        if error or not isinstance(packet, dict) or packet.get('error'):
            raise ValueError('Complete provider query failed')
        flag = packet.get('exceededTransferLimit')
        if not limited and flag is not None and flag is not False:
            raise ValueError('Provider declares incomplete query')
        return packet
    schema(ask(DAILY, {}), 'Daily_Ports_Data', True)
    schema(ask(CATALOG, {}), 'PortWatch_ports_database')
    latest = ask(DAILY + '/query', {'where': '1=1', 'outFields': '*', 'orderByFields': 'date DESC',
                                   'resultRecordCount': 1, 'returnGeometry': 'false'}, True).get('features')
    if not isinstance(latest, list) or len(latest) != 1 or not isinstance(latest[0], dict):
        raise ValueError('Explicit most recent provider row required')
    anchor = source_date((latest[0].get('attributes') or {}).get('date'))
    if anchor is None or anchor > clock.astimezone(timezone.utc).date():
        raise ValueError('Invalid or future source calendar anchor')
    start = anchor - timedelta(days=41)
    def complete(url, where):
        count = ask(url, {'where': where, 'returnCountOnly': 'true'}).get('count')
        if not integer(count) or count > 1000000 or 1 + (count + CHUNK - 1)//CHUNK > remaining():
            raise ValueError('Declared complete membership exceeds bounded acquisition')
        listing = ask(url, {'where': where, 'returnIdsOnly': 'true'})
        ids = listing.get('objectIds')
        if (listing.get('objectIdFieldName') != 'ObjectId' or not isinstance(ids, list)
                or len(ids) != count or any(not integer(i) for i in ids) or len(set(ids)) != count):
            raise ValueError('Declared count and unique source IDs differ')
        ids = sorted(ids); result = []
        for offset in range(0, count, CHUNK):
            chunk = ids[offset:offset+CHUNK]
            packet = ask(url, {'where': where, 'objectIds': ','.join(map(str, chunk)), 'outFields': '*', 'returnGeometry': 'false'})
            features = packet.get('features')
            if not isinstance(features, list) or len(features) != len(chunk) or packet.get('objectIdFieldName', 'ObjectId') != 'ObjectId':
                raise ValueError('Every requested feature must be returned')
            found = {}; expected = set(chunk)
            for feature in features:
                row = feature.get('attributes') if isinstance(feature, dict) else None
                ident = row.get('ObjectId') if isinstance(row, dict) else None
                if not integer(ident) or ident not in expected or ident in found:
                    raise ValueError('Missing, duplicate or foreign feature identity')
                found[ident] = row
            result.extend(found[i] for i in chunk)
        reviews.append({'layer': url, 'where': where, 'declared_count': count, 'enumerated_ids': len(ids),
                        'returned_rows': len(result), 'object_ids_sha256': sha256(json.dumps(ids, separators=(',', ':')).encode()).hexdigest(),
                        'membership_reconciled': True, 'provider_snapshot_atomic': False})
        return result
    catalog = complete(CATALOG + '/query', '1=1')
    rows = complete(DAILY + '/query', "date >= timestamp '%s' AND date <= timestamp '%s'" % (start, anchor))
    if not catalog or not rows: raise ValueError('Nonempty source catalog and observation window required')
    for row in rows:
        day = source_date(row.get('date'))
        if day is None or not start <= day <= anchor: raise ValueError('Returned row outside exact queried calendar')
    if max(source_date(r['date']) for r in rows) != anchor:
        raise ValueError('Enumerated observations do not confirm latest source anchor')
    return rows, catalog, {'contract': 'port-cargo-query-membership.v1', 'queries': reviews,
                          'start': start.isoformat(), 'end': anchor.isoformat(), 'window_days': 42,
                          'provider_snapshot_atomic': False, 'source_catalog_is_world_census': False}


def window(daily, end, days, leg):
    dates = [end - timedelta(days=days-1-i) for i in range(days)]
    values = [daily[d].get(leg) for d in dates if d in daily and amount(daily[d].get(leg))]
    missing = [d.isoformat() for d in dates if d not in daily or not amount(daily[d].get(leg))]
    total = sum(int(v) for v in values) if not missing else None
    overflow = total is not None and total > MAX_EXACT
    if overflow: total = None
    return {'start': dates[0].isoformat(), 'end': end.isoformat(), 'expected_days': days, 'available_days': len(values),
            'missing_or_invalid_dates': missing, 'sum_metric_tons': total,
            'mean_metric_tons_per_day': total/days if total is not None else None,
            'status': 'outside_exact_json_range' if overflow else 'complete' if not missing else 'incomplete'}


def comparison(current, previous):
    a, b = current['mean_metric_tons_per_day'], previous['mean_metric_tons_per_day']
    return {'delta_metric_tons_per_day': a-b if a is not None and b is not None else None,
            'percent': 100*(a/b-1) if a is not None and b is not None and b > 0 else None,
            'status': 'incomplete_windows' if a is None or b is None else 'zero_denominator' if b == 0 else 'defined'}


def build(rows, catalog, acquisition, at):
    clock = datetime.fromisoformat(at.replace('Z', '+00:00'))
    if clock.tzinfo is None: raise ValueError('Aware calculation clock required')
    anchor = source_date(acquisition.get('end')); start = source_date(acquisition.get('start'))
    if anchor is None or start != anchor-timedelta(days=41) or anchor > clock.astimezone(timezone.utc).date():
        raise ValueError('Exact reviewed source window required')
    registered = {}; observed = defaultdict(dict); catalog_oids = set(); observation_oids = set()
    for row in catalog:
        ident = row.get('portid')
        oid = row.get('ObjectId')
        if not isinstance(ident, str) or not ident or ident in registered or not integer(oid) or oid in catalog_oids:
            raise ValueError('Unique stable catalog IDs required')
        catalog_oids.add(oid)
        registered[ident] = row
    for row in rows:
        ident, day = row.get('portid'), source_date(row.get('date'))
        oid = row.get('ObjectId')
        if not isinstance(ident, str) or not ident or day is None or not start <= day <= anchor or not integer(oid) or oid in observation_oids:
            raise ValueError('Whole observation identity required')
        observation_oids.add(oid)
        if day in observed[ident]: raise ValueError('Duplicate port/date observations cannot overwrite each other')
        observed[ident][day] = row
    ports = []
    for ident in sorted(set(registered) | set(observed)):
        meta, daily = registered.get(ident, {}), observed.get(ident, {})
        country = meta.get('ISO3')
        valid_country = isinstance(country, str) and bool(re.fullmatch('[A-Z]{3}', country))
        mismatch = any(row.get('ISO3') != country for row in daily.values()) if valid_country else True
        names = sorted({row['portname'] for row in [meta, *daily.values()] if isinstance(row.get('portname'), str) and row['portname']})
        observations = [{'source_object_id': row.get('ObjectId'), 'date': day.isoformat(),
                         'import_source_value': row.get('import'), 'export_source_value': row.get('export'),
                         'import_valid': amount(row.get('import')), 'export_valid': amount(row.get('export'))}
                        for day, row in sorted(daily.items())]
        legs = {}
        for leg in ('import', 'export'):
            current, previous = window(daily, anchor, 7, leg), window(daily, anchor-timedelta(days=7), 28, leg)
            legs[leg] = {'current_7d': current, 'previous_28d': previous, 'comparison': comparison(current, previous)}
        ports.append({'port_id': ident, 'names': names, 'catalog_status': 'registered' if ident in registered else 'unregistered',
                      'country_code': country if valid_country else None, 'country_name': meta.get('country'),
                      'country_identity_consistent': not mismatch, 'observation_rows': len(daily),
                      'last_observation_date': max(daily).isoformat() if daily else None,
                      'observations': observations, 'legs': legs})
    def aggregate(population):
        out = {'population_port_ids': [p['port_id'] for p in population], 'population_ports': len(population), 'legs': {}}
        for leg in ('import', 'export'):
            cohort = [p for p in population if p['catalog_status'] == 'registered' and p['country_identity_consistent']
                      and all(p['legs'][leg][w]['status'] == 'complete' for w in ('current_7d', 'previous_28d'))]
            cohort_ids = {p['port_id'] for p in cohort}
            values = {}
            for name, days in (('current_7d', 7), ('previous_28d', 28)):
                total = sum(p['legs'][leg][name]['sum_metric_tons'] for p in cohort) if cohort else None
                values[name] = {'sum_metric_tons': total if total is not None and total <= MAX_EXACT else None,
                                'mean_metric_tons_per_day': total/days if total is not None and total <= MAX_EXACT else None}
            out['legs'][leg] = {**values, 'matched_port_ids': [p['port_id'] for p in cohort], 'matched_ports': len(cohort),
                                'excluded_port_ids': [p['port_id'] for p in population if p['port_id'] not in cohort_ids],
                                'comparison': comparison(values['current_7d'], values['previous_28d'])}
        return out
    groups = defaultdict(list)
    for port in ports: groups[port['country_code'] or 'unassigned'].append(port)
    return {'contract': CONTRACT, 'calculation_at': at, 'source_latest_date': anchor.isoformat(),
            'source_observation_lag_days': (clock.astimezone(timezone.utc).date()-anchor).days,
            'unit': 'estimated_metric_tons', 'rate_unit': 'estimated_metric_tons_per_day', 'methodology_url': METHOD,
            'observation_rows': len(rows), 'catalog_ports': len(catalog), 'port_rows': len(ports), 'ports': ports,
            'countries': [{'country_code': code, **aggregate(population)} for code, population in sorted(groups.items())],
            'covered_port_cohort': aggregate(ports), 'forecast_qualified': False, 'calls_eligible': False,
            'sizing_eligible': False, 'execution_eligible': False,
            'definitions': {'window': 'Exact seven calendar dates and immediately preceding 28 dates, ending at the explicit latest provider date. Every date and value required; no ragged-date trimming or missing-as-zero substitution.',
                            'cohort': 'Each direction uses the identical registered, country-consistent ports with both windows complete. Imports and exports can have different cohorts; no combined world total is calculated.',
                            'coverage': 'Includes every provider catalog port and all unregistered observation IDs. Missing catalog ports remain visible. This catalog and matching query membership are not a world-port census or atomic provider snapshot.',
                            'interpretation': 'Descriptive estimated shipment weight. Not customs value, deflated trade volume, unique global cargo, a seasonally/holiday-adjusted change, a company earnings forecast or a portfolio instruction.'}}
