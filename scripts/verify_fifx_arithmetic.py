"""Independent original parsing, 100-digit logs and rational second-moment checks.

Does not import the candidate or its parser. Each shard is checked separately,
so complete multi-decade histories do not require one giant in-memory object.
"""
from bisect import bisect_left, bisect_right, insort
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from fractions import Fraction
from zoneinfo import ZoneInfo
import base64, csv, hashlib, io, json, math
import fifx_catalog as catalog
import fifx_timezones as pinned


def clock(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    assert result.tzinfo is not None
    return result.astimezone(timezone.utc)


def dec(value):
    return Decimal(value.numerator) / Decimal(value.denominator)


def check(actual, expected):
    assert set(actual) == {'value', 'calculated_decimal'}
    if expected is None:
        assert actual == {'value': None, 'calculated_decimal': None}
        return
    if isinstance(expected, Fraction):
        expected = dec(expected)
    assert abs(Decimal(actual['calculated_decimal'])-expected) <= max(Decimal('1e-57'), abs(expected)*Decimal('1e-57'))
    assert type(actual['value']) in (int, float) and math.isfinite(actual['value'])
    assert actual['value'] == float(Decimal(actual['calculated_decimal']))


def second_moment(values):
    n = len(values)
    return (sum(x*x for x in values)-sum(values)**2/n)/(n-1)


def zone(name):
    raw = base64.b64decode(pinned.ZONES[name]['base64'], validate=True)
    assert hashlib.sha256(raw).hexdigest() == pinned.ZONES[name]['sha256']
    return ZoneInfo.from_file(io.BytesIO(raw), key=name)


def verify(output, raw, receipt, definition=None):
    sid = output['source_id'];spec = catalog.SPECS[sid]
    assert output['contract'] == 'fifx-source-candidate.v1' and output['candidate_only'] is True
    assert output['specification'] == spec and output['methodology'] == catalog.METHOD
    assert all(output[k] is False for k in catalog.AUTHORITY) and output['independent_votes'] == 0
    assert output['receipt'] == receipt
    proof = {'source_id': sid, 'original_rows': 0, 'history_rows': 0, 'independent_scalar_checks': 0,
             'current_available': output['current'] is not None, 'status': output['quality']['status'],
             'forecast_qualified': False, 'sizing_qualified': False}
    if raw is None or receipt['http_status'] != 200:
        assert output['current'] is None and output['history'] == [] and output['original_rows'] == []
        assert output['quality']['status'] == ('unavailable' if raw is None else 'http_error')
        if raw is None:
            assert receipt is None and output['original_sha256'] is None and output['original_bytes'] is None
        else:
            assert output['original_sha256'] == receipt['sha256'] == hashlib.sha256(raw).hexdigest()
            assert output['original_bytes'] == receipt['bytes'] == len(raw)
        return proof
    assert output['original_sha256'] == receipt['sha256'] == hashlib.sha256(raw).hexdigest()
    assert output['original_bytes'] == receipt['bytes'] == len(raw)
    # Malformed originals are explicit retained failures. They never authorize numbers.
    if output['quality']['status'] == 'invalid_original_schema':
        assert output['current'] is None and output['history'] == [] and output['original_rows'] == []
        proof['schema_rejection_only'] = True
        return proof
    now, acquired = clock(output['generated_at']), clock(receipt['acquired_at'])
    assert now >= acquired
    if sid in catalog.FRED:
        table = list(csv.reader(io.StringIO(raw.decode('utf-8-sig'), newline='')))
        expected_rows = [{'original_row': i, 'date': r[0], 'value': r[1]} for i, r in enumerate(table[1:])]
        assert table[0] in (['observation_date', sid], ['DATE', sid])
        assert output['source_identity']['definition'] == definition
        meta = definition.get('seriess', []) if isinstance(definition, dict) else []
        reviewed = len(meta) == 1 and meta[0].get('id') == sid and meta[0].get('units') == catalog.FRED_UNITS[sid] and meta[0].get('frequency_short') == 'D' and meta[0].get('frequency') == ('Daily, Close' if sid == 'VIXCLS' else 'Daily') and meta[0].get('seasonal_adjustment') == 'Not Seasonally Adjusted'
        local_zone = None
    else:
        class Token(str):
            pass
        doc = json.loads(raw, parse_int=Token, parse_float=Token)
        res = doc['chart']['result'][0];meta = res['meta'];stamps = [int(t) for t in res['timestamp']]
        currency, exchange, zone_name, names = catalog.QUOTE_IDENTITIES[sid]
        local_zone = zone(zone_name)
        expected_rows = [{'original_row': i, 'timestamp': t, 'date': str(datetime.fromtimestamp(t, timezone.utc).astimezone(local_zone).date()),
                          'value': res['indicators']['quote'][0]['close'][i]} for i, t in enumerate(stamps)]
        reviewed = (meta.get('symbol') == sid and meta.get('currency') == currency and meta.get('exchangeName') == exchange and
            meta.get('instrumentType') == 'INDEX' and meta.get('exchangeTimezoneName') == zone_name and meta.get('dataGranularity') == '1d' and
            all(' '.join(str(meta.get(k, '')).split()) in names for k in ('shortName', 'longName')))
        assert output['source_identity']['metadata'] == meta
        assert output['source_identity']['timezone'] == {'name': zone_name, 'iana_version': pinned.VERSION, 'tzif_sha256': pinned.ZONES[zone_name]['sha256']}
        assert output['source_identity']['official_feed_parity_verified'] is False
    assert output['original_rows'] == expected_rows
    assert output['source_identity']['identity_reviewed'] == reviewed
    proof['original_rows'] = len(expected_rows)
    cutoff = str(acquired.astimezone(local_zone).date() if local_zone else acquired.date())
    eligible = [r for r in expected_rows if r['date'] <= cutoff]
    latest = expected_rows[-1]
    age = ((now.astimezone(local_zone).date() if local_zone else now.date())-date.fromisoformat(latest['date'])).days
    numeric = [r for r in eligible if r['value'] not in (None, '', '.')]
    n = spec['window_changes']
    history = output['history']
    assert len(history) == (max(0, len(numeric)-n) if reviewed else 0)
    with localcontext() as precision:
        precision.prec = 100;precision.rounding = ROUND_HALF_EVEN
        # Independent log evaluation: ln(b/a), at higher precision.
        changes = []
        if n:
            for a, b in zip(numeric, numeric[1:]):
                first, last = Decimal(a['value']), Decimal(b['value'])
                change = Fraction(last-first)*100 if sid == 'DGS10' else Fraction((last/first).ln()*100) if first > 0 and last > 0 else None
                changes.append(change)
        prefix = [Fraction(0)];squares = [Fraction(0)];invalid_prefix = [0];ordered = [];previous = []
        for i, row in enumerate(history):
            last = numeric[i+n]
            assert row['date'] == last['date'] and row['original_row'] == last['original_row']
            if n:
                first = numeric[i];batch = numeric[i:i+n+1];delta = changes[i:i+n]
                gaps = [(date.fromisoformat(b['date'])-date.fromisoformat(a['date'])).days for a, b in zip(batch, batch[1:])]
                variance = second_moment(delta) if all(x is not None for x in delta) else None
                estimate = dec(variance*252).sqrt() if variance is not None else None
                valid = max(gaps) <= 7 and variance is not None
                assert row['start_date'] == first['date'] and row['start_original_row'] == first['original_row']
                assert row['numeric_observations'] == n+1 and row['change_count'] == n
                assert row['elapsed_calendar_days'] == (date.fromisoformat(last['date'])-date.fromisoformat(first['date'])).days
                assert row['missing_rows_inside_window'] == last['original_row']-first['original_row']-n
                assert row['max_interval_days'] == max(gaps) and row['invalid_log_changes'] == sum(x is None for x in delta)
                check(row['sample_variance'], variance)
                check(row['step_dispersion'], dec(variance).sqrt() if variance is not None else None)
                proof['independent_scalar_checks'] += 2
            else:
                estimate = Decimal(last['value']);valid = estimate >= 0
            check(row['estimate'], estimate)
            assert row['valid_intervals'] == valid
            # Baseline is explicitly defined on the checked 72-digit estimates.
            value = Fraction(row['estimate']['calculated_decimal']) if estimate is not None else None
            start = max(0, i-504);count = i-start;bad = invalid_prefix[i]-invalid_prefix[start]
            actual = row['baseline'];state = 'insufficient_prior_windows' if count != 504 else 'invalid_prior_or_current' if bad or not valid else 'available'
            mean = sd = z = rank = None
            if state == 'available':
                total = prefix[i]-prefix[start];ss = squares[i]-squares[start]
                mean = dec(total/count);sd = dec((ss-total*total/count)/(count-1)).sqrt()
                z = (dec(value)-mean)/sd if sd else None
                rank = Decimal(100)*(bisect_left(ordered, value)+Decimal(bisect_right(ordered, value)-bisect_left(ordered, value))/2)/count
                if not sd:state = 'flat_baseline'
            assert actual['status'] == state and actual['prior_count'] == count and actual['invalid_prior_count'] == bad
            assert actual['first_prior_date'] == (history[start]['date'] if count else None)
            assert actual['last_prior_date'] == (history[i-1]['date'] if count else None)
            for key, expected in zip(('mean', 'sample_sd', 'z_score', 'midrank_percentile'), (mean, sd, z, rank)):
                check(actual[key], expected)
            previous.append((value, valid))
            prefix.append(prefix[-1]+(value if valid else 0));squares.append(squares[-1]+(value*value if valid else 0));invalid_prefix.append(invalid_prefix[-1]+(not valid))
            if valid:insort(ordered, value)
            if i >= 504 and previous[i-504][1]:ordered.pop(bisect_left(ordered, previous[i-504][0]))
            proof['independent_scalar_checks'] += 5
    last = history[-1] if history else None
    status = ('identity_mismatch' if not reviewed else 'future_original_rows' if len(eligible) != len(expected_rows) else
        'provisional_session' if local_zone and latest['date'] >= cutoff else 'missing_latest_value' if latest['value'] in (None, '', '.') else
        'stale_acquisition' if (now-acquired).total_seconds() > 26*3600 else 'stale_observation' if not 0 <= age <= spec['max_observation_age_days'] else
        'insufficient_history' if last is None else 'invalid_current_window' if not last['valid_intervals'] or last['date'] != latest['date'] else 'within_age_ceiling')
    assert output['latest_reported'] == latest and output['last_calculated'] == last
    assert output['current'] == (last if status == 'within_age_ceiling' else None)
    assert output['quality'] == {'status': status, 'observation_age_days': age, 'acquisition_age_seconds': (now-acquired).total_seconds(),
        'acquisition_cutoff_date': cutoff, 'excluded_future_rows': len(expected_rows)-len(eligible),
        'max_observation_age_days': spec['max_observation_age_days'], 'max_acquisition_age_seconds': 26*3600, 'release_calendar_verified': False}
    proof['history_rows'] = len(history)
    return proof
