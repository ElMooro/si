"""Lossless numeric-token decoding and explicit source-identity checks; no IO."""
from datetime import date, datetime, timezone
from decimal import Decimal
import csv, hashlib, io, json, re, urllib.parse
import fifx_catalog as catalog
import fifx_timezones as timezones


class Number(str):
    """Distinguish an original JSON number from a quoted numeric string."""


def clock(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Explicit timezone required')
    return result.astimezone(timezone.utc)


def decimal(value):
    if value is None or value in ('', '.'):
        return None
    if not isinstance(value, str):
        raise ValueError('Original numeric text required')
    out = Decimal(value)
    if not out.is_finite() or len(out.as_tuple().digits) > 40 or not -40 <= out.as_tuple().exponent <= 15:
        raise ValueError('Original value outside reviewed finite precision')
    return out


def unique_object(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError('Duplicate original JSON member')
        out[key] = value
    return out


def receipt_check(sid, raw, receipt, generated_at):
    if sid not in catalog.SOURCES or not isinstance(raw, bytes) or not 0 < len(raw) <= 8 * 1024 * 1024:
        raise ValueError('Whole reviewed source required')
    if receipt.get('bytes') != len(raw) or receipt.get('sha256') != hashlib.sha256(raw).hexdigest():
        raise ValueError('Original acquisition receipt differs')
    if type(receipt.get('http_status')) is not int:
        raise ValueError('HTTP outcome required')
    now, acquired = clock(generated_at), clock(receipt['acquired_at'])
    if acquired > now:
        raise ValueError('Future source acquisition')
    url = urllib.parse.urlsplit(receipt['source_url'])
    params = urllib.parse.parse_qs(url.query, strict_parsing=True)
    if url.scheme != 'https' or url.fragment or url.username or url.password or url.port:
        raise ValueError('Unreviewed source URL')
    if sid in catalog.FRED:
        expected = {'id': [sid], 'cosd': ['1988-01-01'], 'coed': [str(acquired.date())]}
        valid = url.netloc == 'fred.stlouisfed.org' and url.path == '/graph/fredgraph.csv' and params == expected
    else:
        valid = url.netloc == 'query1.finance.yahoo.com' and urllib.parse.unquote(url.path) == '/v8/finance/chart/' + sid
        if sid in ('^MOVE', '^VHSI'):
            valid = valid and params == {'range': ['2y'], 'interval': ['1d']}
        else:
            valid = valid and set(params) == {'period1', 'period2', 'interval'} and params['period1'] == ['315532800'] and params['interval'] == ['1d']
            valid = valid and len(params['period2']) == 1 and params['period2'][0].isdigit() and 0 <= acquired.timestamp() - int(params['period2'][0]) <= 3600
    if not valid:
        raise ValueError('Source request identity differs')
    return now, acquired


def parse_csv(raw, sid, definition):
    table = list(csv.reader(io.StringIO(raw.decode('utf-8-sig'), newline='')))
    if not table or table[0] not in (['observation_date', sid], ['DATE', sid]) or not 1 <= len(table) - 1 <= 50000:
        raise ValueError('Complete reviewed CSV columns and population required')
    rows = []
    for i, cells in enumerate(table[1:]):
        if len(cells) != 2 or date.fromisoformat(cells[0]).isoformat() != cells[0]:
            raise ValueError('Canonical source row required')
        decimal(cells[1])
        rows.append({'original_row': i, 'date': cells[0], 'value': cells[1]})
    dates = [r['date'] for r in rows]
    if dates != sorted(dates) or len(set(dates)) != len(dates):
        raise ValueError('Monotonic unique original dates required')
    meta = definition.get('seriess', []) if isinstance(definition, dict) else []
    expected = (sid, catalog.FRED_UNITS[sid], 'D', 'Daily, Close' if sid == 'VIXCLS' else 'Daily', 'Not Seasonally Adjusted')
    identity = len(meta) == 1 and tuple(meta[0].get(k) for k in ('id', 'units', 'frequency_short', 'frequency', 'seasonal_adjustment')) == expected
    return rows, {'status': 'reviewed_source_identity' if identity else 'definition_mismatch',
                  'definition': definition, 'identity_reviewed': identity, 'timezone': None}


def parse_quote(raw, sid):
    doc = json.loads(raw, parse_int=Number, parse_float=Number, object_pairs_hook=unique_object,
                     parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))
    chart = doc.get('chart') if isinstance(doc, dict) else None
    results = chart.get('result') if isinstance(chart, dict) else None
    if not isinstance(chart, dict) or chart.get('error') is not None or not isinstance(results, list) or len(results) != 1:
        raise ValueError('One complete original quote result required')
    row = results[0]
    meta, stamps = row['meta'], row['timestamp']
    quotes = row['indicators']['quote']
    if not isinstance(meta, dict) or not isinstance(stamps, list) or not 1 <= len(stamps) <= 50000 or not isinstance(quotes, list) or len(quotes) != 1 or not isinstance(quotes[0], dict):
        raise ValueError('Complete quote schema required')
    arrays = quotes[0]
    if 'close' not in arrays or any(not isinstance(v, list) or len(v) != len(stamps) for v in arrays.values()):
        raise ValueError('Aligned full original arrays required')
    for values in arrays.values():
        for value in values:
            if value is not None and (not isinstance(value, Number) or decimal(value) is None):
                raise ValueError('Finite original quote numbers required')
    if any(not isinstance(t, Number) or not re.fullmatch(r'\d+', t) or not 0 <= int(t) <= 253402214400 for t in stamps):
        raise ValueError('Bounded integer quote timestamps required')
    stamps = [int(t) for t in stamps]
    if stamps != sorted(stamps) or len(set(stamps)) != len(stamps):
        raise ValueError('Unique ordered quote timestamps required')
    currency, exchange, zone_name, names = catalog.QUOTE_IDENTITIES[sid]
    identity = (meta.get('symbol') == sid and meta.get('instrumentType') == 'INDEX' and
        meta.get('currency') == currency and meta.get('exchangeName') == exchange and
        meta.get('exchangeTimezoneName') == zone_name and meta.get('dataGranularity') == '1d' and
        all(' '.join(str(meta.get(k, '')).split()) in names for k in ('shortName', 'longName')))
    # Use the reviewed instrument timezone even when the provider metadata disagrees.
    # Such rows remain evidence only and cannot authorize a calculation.
    zone = timezones.zone(zone_name)
    rows = [{'original_row': i, 'timestamp': t,
             'date': str(datetime.fromtimestamp(t, timezone.utc).astimezone(zone).date()),
             'value': str(arrays['close'][i]) if arrays['close'][i] is not None else None}
            for i, t in enumerate(stamps)]
    if len({r['date'] for r in rows}) != len(rows):
        raise ValueError('More than one returned daily row per local session')
    return rows, {'status': 'reviewed_source_identity' if identity else 'identity_mismatch',
        'metadata': meta, 'identity_reviewed': identity, 'original_arrays': sorted(arrays),
        'official_feed_parity_verified': False,
        'timezone': {'name': zone_name, 'iana_version': timezones.VERSION, 'tzif_sha256': timezones.ZONES[zone_name]['sha256']}}
