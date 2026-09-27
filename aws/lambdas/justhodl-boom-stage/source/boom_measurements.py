"""Dated, unit-specific FRED research; no inventory, country or trading inference.

Retain whole metadata/observation responses before parsing. The requested two-year
window is explicit, not a claim of complete history or original release vintages.
"""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, InvalidOperation
import time
import urllib.error
import urllib.parse
import urllib.request

import boom_history as h

CONTRACT = 'boom-fred-calendar.v1'
# Reviewed against the publisher's series pages on 2026-09-27. US definitions
# cannot qualify the inherited mappings to other countries or industries.
PROFILES = {
    'CAPUTLG3344S': ('Percent', 'M', 'SA', 'percentage_points', 'US semiconductor and electronic-component utilization'),
    'CAPUTLG21S': ('Percent', 'M', 'SA', 'percentage_points', 'US mining utilization'),
    'TCU': ('Percent', 'M', 'SA', 'percentage_points', 'US total industrial utilization'),
    'MNFCTRIRSA': ('Ratio', 'M', 'SA', 'ratio_points', 'US manufacturers inventories-to-sales ratio'),
    'ISRATIO': ('Ratio', 'M', 'SA', 'ratio_points', 'US total business inventories-to-sales ratio'),
    'RETAILIRSA': ('Ratio', 'M', 'SA', 'ratio_points', 'US retailers inventories-to-sales ratio'),
}
UNREVIEWED = {
    'WGTSTUS1': 'Inherited natural-gas definition is rejected: EIA WGTSTUS1 is total gasoline stocks, not natural gas. FRED identity remains unverified.',
    'PCU2122302122300': 'Inherited copper-price proxy is not an inventory measurement; this series identity remains unverified.',
}
ALLOWED = set(PROFILES) | set(UNREVIEWED)
FIELDS = ('id', 'title', 'units', 'frequency', 'frequency_short', 'seasonal_adjustment',
          'seasonal_adjustment_short', 'observation_start', 'observation_end', 'last_updated',
          'realtime_start', 'realtime_end')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('FRED redirects refused')


def number(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    try:
        result = Decimal(str(value))
        if not result.is_finite() or abs(result) > Decimal('1e100') or result < 0:
            return None
        return result
    except InvalidOperation:
        return None


def empty(sid, status):
    return {'contract': CONTRACT, 'series': sid, 'name': PROFILES[sid][4] if sid in PROFILES else sid,
            'date': None, 'level': None, 'prior_date': None, 'prior_level': None,
            'yoy_chg': None, 'change_unit': None, 'unit': None, 'comparison': 'same_month_previous_calendar_year',
            'status': status, 'read': 'RESEARCH_ONLY', 'means': 'No qualified inventory, supply, demand or investment inference.',
            'country': 'US' if sid in PROFILES else None, 'country_industry_mapping_qualified': False,
            'point_in_time_verified': False, **dict.fromkeys(h.FLAGS, False)}


def measure(sid, metadata, packet, generated_at):
    """Pure interpretation; preserve row identity, including unusable observations."""
    if sid not in ALLOWED:
        raise h.IntegrityError('Unreviewed FRED request identity')
    at = h.stamp(generated_at).date()
    out = empty(sid, 'unavailable')
    records = metadata.get('seriess') if isinstance(metadata, dict) else None
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], dict) or records[0].get('id') != sid:
        out['status'] = 'metadata_identity_mismatch'
        return out
    meta = records[0]
    out['metadata'] = {k: meta.get(k) for k in FIELDS}
    if sid not in PROFILES:
        out.update(status='definition_unverified', definition_issue=UNREVIEWED[sid])
        return out
    units, frequency, seasonal, delta_unit, _ = PROFILES[sid]
    if (meta.get('units'), meta.get('frequency_short'), meta.get('seasonal_adjustment_short')) != (units, frequency, seasonal):
        out['status'] = 'metadata_definition_changed'
        return out
    out.update(unit=units, change_unit=delta_unit, seasonal_adjustment=seasonal, frequency='monthly')
    if not isinstance(packet, dict) or not isinstance(packet.get('observations'), list):
        out['status'] = 'observations_unavailable'
        return out
    rows = packet['observations']
    if (type(packet.get('count')) is not int or packet['count'] != len(rows)
            or type(packet.get('offset')) is not int or packet['offset'] != 0
            or packet.get('units') != 'lin' or type(packet.get('output_type')) is not int or packet['output_type'] != 1):
        out['status'] = 'incomplete_or_transformed_response'
        return out
    out['observations'] = []
    dates, ambiguous = {}, False
    for index, row in enumerate(rows):
        item = {'row': index, 'date': row.get('date') if isinstance(row, dict) else None,
                'value': None, 'status': 'invalid_date'}
        if isinstance(row, dict):
            item['provider_value'] = deepcopy(row.get('value'))
        try:
            day = h.date(item['date'])
            if day.day != 1 or not at-timedelta(days=730) <= day <= at:
                raise ValueError()
            item['status'] = 'missing_or_invalid_value'
            val = number(row.get('value'))
            if val is not None:
                item.update(value=float(val), status='observed')
            if day in dates:
                item['status'] = 'duplicate_date'
                ambiguous = True
            dates[day] = (val, item)
        except (h.IntegrityError, ValueError, TypeError):
            ambiguous = True
        out['observations'].append(item)
    out['returned_rows'] = len(rows)
    if ambiguous:
        out['status'] = 'ambiguous_observation_identity'
        return out
    if not dates:
        out['status'] = 'empty_observations'
        return out
    latest = max(dates)
    prior_day = latest.replace(year=latest.year-1)
    current = dates[latest][0]
    prior = dates.get(prior_day, (None,))[0]
    out.update(date=latest.isoformat(), level=float(current) if current is not None else None,
               prior_date=prior_day.isoformat(), prior_level=float(prior) if prior is not None else None,
               observation_age_days=(at-latest).days,
               observation_date_kind='month_start_label_not_publication_date', observation_freshness_verified=False,
               status='latest_missing' if current is None else 'prior_month_missing' if prior is None else 'measured')
    if current is not None and prior is not None:
        out['yoy_chg'] = float(current-prior)
        out['means'] = ('Same-month change in '+delta_unit+'. US source only; no foreign-country, inventory-build or trading inference.')
    return out


class Session:
    """Invocation-local identity cache, original bytes and bounded request budget."""
    def __init__(self, client, bucket, generated_at, api_key, opener=None):
        self.client, self.bucket, self.at, self.api_key = client, bucket, generated_at, api_key
        self.open = opener or urllib.request.build_opener(NoRedirect()).open
        self.remaining = 40
        self.results, self.records = {}, {}

    def request(self, sid, endpoint):
        # Other native Yahoo/S3 work must not consume this source's budget.
        started = time.monotonic()
        try:
            return self._request(sid, endpoint)
        finally:
            self.remaining -= max(0, time.monotonic()-started)

    def _request(self, sid, endpoint):
        remaining = self.remaining
        if remaining <= 0:
            return None, {'status': 'request_budget_exhausted'}
        day = h.stamp(self.at).date()
        params = {'series_id': sid, 'api_key': self.api_key, 'file_type': 'json',
                  'realtime_start': day.isoformat(), 'realtime_end': day.isoformat()}
        public_params = {k: v for k, v in params.items() if k != 'api_key'}
        if endpoint == 'series/observations':
            window = {'observation_start': (day-timedelta(days=730)).isoformat(), 'observation_end': day.isoformat(),
                      'sort_order': 'desc', 'limit': 100000, 'offset': 0, 'units': 'lin', 'output_type': 1}
            params.update(window); public_params.update(window)
        url = 'https://api.stlouisfed.org/fred/'+endpoint
        evidence = {'endpoint': url, 'parameters': public_params}
        try:
            response = self.open(urllib.request.Request(url+'?'+urllib.parse.urlencode(params),
                headers={'User-Agent': 'JustHodl-research/1.0', 'Accept-Encoding': 'identity'}), timeout=min(5, remaining))
        except urllib.error.HTTPError as exc:
            response = exc
        except Exception:
            evidence['status'] = 'transport_failed'
            return None, evidence
        status = getattr(response, 'status', getattr(response, 'code', None))
        length = response.headers.get('Content-Length')
        try:
            raw = h.bounded(response)
        except Exception:
            raise h.IntegrityError('Whole FRED response unavailable; no partial publication') from None
        # Readback is required even for a non-200 or malformed body. URLs/keys
        # and raw error messages are never put in the public output/log.
        evidence.update(original=h.retain(self.client, self.bucket, raw), http_status=status)
        if length is not None and (not str(length).isdigit() or int(length) != len(raw)):
            raise h.IntegrityError('FRED response length differs')
        if status != 200:
            evidence['status'] = 'http_error'
            return None, evidence
        try:
            parsed = h.decode(raw)
        except h.IntegrityError:
            evidence['status'] = 'malformed_complete_json'
            return None, evidence
        evidence['status'] = 'complete_response_retained'
        return parsed, evidence

    def get(self, sid):
        if sid not in ALLOWED:
            raise h.IntegrityError('Unreviewed FRED identity')
        if sid in self.results:
            return deepcopy(self.results[sid])
        if not self.api_key:
            result = empty(sid, 'credential_unavailable')
            evidence = {'metadata': {'status': 'credential_unavailable'}, 'observations': {'status': 'not_requested'}}
        else:
            metadata, m = self.request(sid, 'series')
            observations, o = (self.request(sid, 'series/observations') if metadata is not None else (None, {'status': 'not_requested'}))
            result = measure(sid, metadata, observations, self.at)
            evidence = {'metadata': m, 'observations': o}
        result['original_responses'] = evidence
        self.records[sid] = deepcopy(evidence)
        self.results[sid] = deepcopy(result)
        return result

    def review(self):
        return {'contract': CONTRACT, 'calculated_at': self.at, 'series': deepcopy(self.results),
                'requested_lookback_days': 730, 'complete_series_history': False,
                'source_originals_retained': all(v.get('original_responses', {}).get('metadata', {}).get('original')
                                               and v.get('original_responses', {}).get('observations', {}).get('original')
                                               for v in self.results.values()) if self.results else False,
                'point_in_time_verified': False, 'forecast_qualified': False,
                'scope': 'FRED metadata and dated monthly arithmetic only. Other value, shipping, stage and trade models remain unqualified.'}


def replay(review, read_original):
    """Replay every acquired FRED response; never contact a provider."""
    if not isinstance(review, dict) or review.get('contract') != CONTRACT or not isinstance(review.get('series'), dict):
        raise h.IntegrityError('Complete FRED review required')
    for sid, row in review['series'].items():
        evidence = row['original_responses']
        parsed = {}
        for name in ('metadata', 'observations'):
            ref = evidence[name].get('original')
            raw = read_original(ref) if ref else None
            if ref and (not isinstance(raw, bytes) or h.sha(raw) != ref.get('sha256') or len(raw) != ref.get('bytes')):
                raise h.IntegrityError('Complete FRED original differs')
            parsed[name] = h.decode(raw) if raw is not None and evidence[name].get('status') == 'complete_response_retained' else None
        expected = (empty(sid, 'credential_unavailable') if evidence['metadata'].get('status') == 'credential_unavailable'
                    else measure(sid, parsed['metadata'], parsed['observations'], review['calculated_at']))
        expected['original_responses'] = evidence
        if expected != row:
            raise h.IntegrityError('FRED replay differs')
    return {'series': len(review['series']), 'complete_dated_arithmetic_matches': True,
            'other_native_models_replayed': False, 'point_in_time_verified': False}
