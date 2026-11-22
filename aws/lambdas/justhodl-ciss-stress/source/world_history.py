"""Histories for the Sovereign desk's World tab: data/ciss-sovereign-world.json.

The World tab's table is one day's figures for 44 governments (data/global-sovereign.json, as reported by World
Government Bonds).  A chart needs a history per country.  This module gathers the histories the site already keeps and
publishes them as four sets, each from one kind of source so that the lines of a set can be compared:

  yield            10-year government yields, monthly averages, long history: the ECB's long-term interest rate
                   statistic (read from the desk's own document) or the OECD's (the site's warehouse, through its
                   series resolver), whichever reaches further back and is still current
  cds              5-year CDS measured by the site's CDS desk from DTCC public trade prints (since September 2024)
  yield_reported   the table's own 10-year yield, kept day by day from the world document's past publications
  cds_reported     the table's own CDS figure, kept the same way

Nothing is estimated or filled in.  A country without a stored history in a set is listed as missing in that set.
None of it is a forecast, a probability of default or a position size.

The world document's engine keeps every publication it replaces; each publication names the receipt of the attempt
that wrote it, and the receipt names the publication before it.  walk() follows that chain back to the last
publication already folded into the state, so a missed day is recovered on the next run.

compile_world() is pure: the same inputs and the same clock give the same bytes.
"""
import calendar
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
import time
import urllib.parse
import urllib.request

from ciss_source_model import encoded, digest, clock, PREFIX
import sovereign_desk as desk

VERSION = '1.0.0'
CONTRACT = 'sovereign-world.v1'
STATE_CONTRACT = 'sovereign-world-state.v1'
CURRENT = 'data/ciss-sovereign-world.json'
STATE = PREFIX + 'sovereign/world-state.json'
TABLE = 'data/global-sovereign.json'
CDS_PACKET = 'data/cds-desk.json'
CDS_HISTORY = 'data/cds-desk-history.json'
RETAINED = 'audit-private/20260909-originals/global-sovereign-research/'      # where the world engine keeps what it replaces
RESOLVER = 'https://justhodl-data-proxy.raafouis.workers.dev/series?id='
CLIENT = {'User-Agent': 'JustHodl-SovereignDesk/1.0', 'Accept': 'application/json'}

# OECD long-term government bond yield, 10-year, monthly: FRED id IRLTLT01<code>M156N (held in the site's warehouse)
OECD = {'United States': 'US', 'Japan': 'JP', 'United Kingdom': 'GB', 'Canada': 'CA', 'Australia': 'AU', 'New Zealand': 'NZ', 'Switzerland': 'CH', 'Norway': 'NO',
        'South Korea': 'KR', 'Mexico': 'MX', 'South Africa': 'ZA', 'Israel': 'IL', 'Chile': 'CL', 'Russia': 'RU', 'Germany': 'DE', 'France': 'FR', 'Italy': 'IT',
        'Spain': 'ES', 'Netherlands': 'NL', 'Belgium': 'BE', 'Austria': 'AT', 'Portugal': 'PT', 'Ireland': 'IE', 'Greece': 'GR', 'Finland': 'FI', 'Sweden': 'SE',
        'Denmark': 'DK', 'Poland': 'PL', 'Hungary': 'HU', 'Czech Republic': 'CZ'}
SAME = {'czech republic': 'czechia', 'korea': 'south korea', 'republic of korea': 'south korea', 'united states of america': 'united states', 'usa': 'united states',
        'uk': 'united kingdom', 'turkiye': 'turkey'}
MONTHS = {m: i + 1 for i, m in enumerate('January February March April May June July August September October November December'.split())}
SETS = (
    ('yield', 'WY_', '10-year yields', 'monthly averages over the longest history the site holds, from the ECB or the OECD (named on each line)', 'pct', 'M'),
    ('cds', 'WC_', '5-year CDS, measured', 'measured by the site\'s CDS desk from DTCC public trade prints since September 2024, one point for each day the name traded', 'bp', 'D'),
    ('yield_reported', 'RY_', '10-year yields, as reported', 'the table\'s own figure, kept day by day', 'pct', 'D'),
    ('cds_reported', 'RC_', 'CDS, as reported', 'the table\'s own figure, kept day by day', 'bp', 'D'),
)
SOURCE = {
    'ecb': 'ECB long-term interest rate statistics, 10-year government yield, monthly average',
    'oecd': 'OECD Main Economic Indicators, long-term government bond yield, monthly average (FRED %s, the site\'s warehouse)',
    'dtcc': 'DTCC public price dissemination, 5-year senior CDS as measured by the site\'s CDS desk',
    'reported': 'World Government Bonds, as reported on the day; kept by the site since %s',
}
MIN_POINTS = 2


def norm(name):
    key = ' '.join(str(name or '').lower().split())
    return SAME.get(key, key)


def slug(name):
    return re.sub(r'[^A-Z0-9]+', '_', norm(name).upper()).strip('_')


def provider_day(text):
    """'21 November 2026' -> '2026-11-21'; anything else -> None."""
    parts = str(text or '').split()
    if len(parts) == 3 and parts[0].isdigit() and parts[1] in MONTHS and parts[2].isdigit():
        try:
            return date(int(parts[2]), MONTHS[parts[1]], int(parts[0])).isoformat()
        except ValueError:
            return None
    return None


def spoken(day):
    """'2026-09-26' -> '26 September 2026'."""
    d = date.fromisoformat(day)
    return '%d %s %d' % (d.day, next(name for name, number in MONTHS.items() if number == d.month), d.year)


def month_end(day):
    """The last day of the month an ISO date falls in."""
    d = date.fromisoformat(str(day)[:10])
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1]).isoformat()


def decimal(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return out if out.is_finite() else None


def pairs(rows, monthly=False, today=None):
    """Ascending (iso date, Decimal) from [[date, value], ...] or [{'date', 'value'}, ...]; one reading per date, the last given wins."""
    out = {}
    for row in rows or []:
        if isinstance(row, dict):
            day, value = row.get('date') or row.get('d'), row.get('value') if 'value' in row else row.get('v')
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            day, value = row[0], row[1]
        else:
            continue
        value = decimal(value)
        if value is None or not isinstance(day, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', day[:10]):
            continue
        try:
            day = month_end(day) if monthly else date.fromisoformat(day[:10]).isoformat()
        except ValueError:
            continue
        if today and day > today:
            continue                                        # a month that has not ended, or a date ahead of the clock
        out[day] = value
    return sorted(out.items())


def expected_month_end(today):
    """The newest month a monthly statistic can be expected to carry on this day."""
    back = 1 if today.day >= 12 else 2
    year, month0 = divmod(today.year * 12 + today.month - 1 - back, 12)
    return date(year, month0 + 1, calendar.monthrange(year, month0 + 1)[1]).isoformat()


def due(cached, now):
    """Whether a cached monthly series should be asked for again."""
    if not cached or not cached.get('obs') or not cached.get('read_at'):
        return True
    last = month_end(cached['obs'][-1][0])
    hours = (now - clock(cached['read_at'])).total_seconds() / 3600
    if hours < 20:
        return False                                        # asked within the day
    if (now.date() - date.fromisoformat(last)).days > desk.ENDED:
        return hours >= 7 * 24                              # a series its source stopped: look once a week
    return last < expected_month_end(now.date())


def fold(state, head):
    """Add one publication of the world document to the day-by-day record.  Returns how many figures it carried."""
    stamp, count = head['generated_at'], 0
    book = state.setdefault('reported', {})
    for row in head.get('countries') or []:
        if not isinstance(row, dict) or not isinstance(row.get('country'), str) or not row['country'].strip():
            continue
        day = provider_day(row.get('as_of')) or stamp[:10]
        slot = book.setdefault(row['country'], {'y': {}, 'c': {}})
        for field, name in (('yield_10y_pct', 'y'), ('cds_bp', 'c')):
            value = row.get(field)
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
                slot[name][day] = value
                count += 1
    if stamp > (state.get('last_publication') or ''):
        state['last_publication'] = stamp
    return count


def walk(read_json, head, known, limit=500, deadline=None, problems=None):
    """Publications before `head`, newest first, back to one already folded (generated_at <= known).

    The chain ends cleanly where a publication names no receipt or a receipt names no predecessor.  An object that
    cannot be read also ends it; that is noted in `problems`, because the days before it are then not recovered."""
    out, doc, newer = [], head, head.get('generated_at') or ''
    while len(out) < limit and (deadline is None or time.monotonic() < deadline):
        receipt = (doc.get('publication_record') or {}).get('manifest_key')
        if not isinstance(receipt, str) or not receipt.startswith(RETAINED):
            break
        try:
            prior = ((read_json(receipt).get('predecessors') or {}).get(TABLE) or {}).get('key')
            if not isinstance(prior, str) or not prior.startswith(RETAINED):
                break
            doc = read_json(prior)
        except Exception as exc:                            # noqa: BLE001 - the chain ends where an object cannot be read
            if problems is not None:
                problems.append(str(getattr(exc, 'response', {}).get('Error', {}).get('Code') or type(exc).__name__))
            break
        if not isinstance(doc, dict) or not isinstance(doc.get('countries'), list) or not isinstance(doc.get('generated_at'), str):
            break
        if (known and doc['generated_at'] <= known) or doc['generated_at'] >= newer:
            break                                           # already folded, or a chain that does not run backwards in time
        newer = doc['generated_at']
        out.append(doc)
    return out


def cds_names(packet):
    """{normalised country name: the CDS desk's key for that sovereign} from the desk's packet."""
    out = {}
    group = ((packet or {}).get('groups') or {}).get('sovereign') or {}
    for part in ('rows', 'unpriced', 'dormant', 'universe'):
        for row in group.get(part) or []:
            if isinstance(row, dict) and row.get('name') and row.get('key'):
                out.setdefault(norm(row['name']), row['key'])
    return out


def choose_monthly(candidates, today):
    """Of the monthly sources a country has, the one that reaches furthest back among those still current; else the newest."""
    live = [c for c in candidates if (date.fromisoformat(today) - date.fromisoformat(c['obs'][-1][0])).days <= desk.ALLOW['M']]
    if live:
        return min(live, key=lambda c: (c['obs'][0][0], 0 if c['source'] == 'ecb' else 1))
    return max(candidates, key=lambda c: (c['obs'][-1][0], c['source'] == 'ecb')) if candidates else None


def compile_world(table, desk_doc, desk_history, cds_packet, cds_history, state, generated_at, inputs=None, errors=None):
    """The world-history document from what was read.  Pure."""
    today = clock(generated_at).date().isoformat()
    issues = dict(errors or {})
    measures, series, countries = {}, {}, []
    ecb_ids = {norm(c['country']): c['ids']['yield'] for c in (desk_doc or {}).get('countries') or [] if (c.get('ids') or {}).get('yield')}
    ecb_series = (desk_history or {}).get('series') or {}
    names = cds_names(cds_packet)
    prints = (cds_history or {}).get('names') or {}
    cache = (state or {}).get('oecd') or {}
    book = (state or {}).get('reported') or {}
    missing = {s[0]: [] for s in SETS}
    counts = {}

    def add(set_id, prefix, country, unit, freq, obs, source, label, detail):
        if len(obs) < MIN_POINTS:
            return None
        mid = prefix + slug(country)
        words = {'yield': ' 10-year government yield, monthly', 'cds': ' 5-year CDS, measured', 'yield_reported': ' 10-year yield, as reported', 'cds_reported': ' CDS, as reported'}[set_id]
        m = desk.measure(mid, country + words, unit, freq, obs, today, group=set_id)
        if m is None or mid in measures:
            return None
        for unused in ('area', 'evidence', 'source_row', 'exact', 'blank_after', 'left_out', 'derived', 'period'):
            m.pop(unused, None)
        m.update({'country': country, 'set': set_id, 'source': source, 'source_label': label, 'key': detail})
        measures[mid] = m
        series[mid] = {'d': [d for d, _ in obs], 'v': [desk.number(v, unit) for _, v in obs]}
        counts[source] = counts.get(source, 0) + 1
        return mid

    for row in table.get('countries') or []:
        country = row.get('country') if isinstance(row, dict) else None
        if not isinstance(country, str) or not country.strip():
            continue
        key, ids = norm(country), {}
        # 10-year yield, monthly: the ECB's statistic from the desk, the OECD's from the warehouse
        candidates = []
        if key in ecb_ids and ecb_ids[key] in ecb_series:
            s = ecb_series[ecb_ids[key]]
            obs = pairs(zip(s.get('d') or [], s.get('v') or []), monthly=True, today=today)
            if len(obs) >= MIN_POINTS:
                candidates.append({'source': 'ecb', 'obs': obs, 'label': SOURCE['ecb'], 'detail': ((desk_doc['measures'].get(ecb_ids[key]) or {}).get('key'))})
        kept = cache.get(country)
        if kept and kept.get('obs'):
            obs = pairs(kept['obs'], monthly=True, today=today)
            if len(obs) >= MIN_POINTS:
                candidates.append({'source': 'oecd', 'obs': obs, 'label': SOURCE['oecd'] % kept.get('id', '').split(':')[-1], 'detail': kept.get('id')})
        pick = choose_monthly(candidates, today)
        if pick:
            ids['yield'] = add('yield', 'WY_', country, 'pct', 'M', pick['obs'], pick['source'], pick['label'], pick['detail'])
        # 5-year CDS as the CDS desk measured it
        record = prints.get(names.get(key)) if names.get(key) else None
        if isinstance(record, dict):
            ids['cds'] = add('cds', 'WC_', country, 'bp', 'D', pairs(record.get('points'), today=today), 'dtcc', SOURCE['dtcc'], names[key])
        # the table's own figures, day by day
        slot = book.get(country) or {}
        for set_id, prefix, name, unit in (('yield_reported', 'RY_', 'y', 'pct'), ('cds_reported', 'RC_', 'c', 'bp')):
            obs = pairs(sorted((slot.get(name) or {}).items()), today=today)
            if obs:
                ids[set_id] = add(set_id, prefix, country, unit, 'D', obs, 'reported', SOURCE['reported'] % spoken(obs[0][0]), TABLE)
        ids = {k: v for k, v in ids.items() if v}
        for s in SETS:
            if s[0] not in ids:
                missing[s[0]].append(country)
        countries.append({'country': country, 'region': row.get('region'), 'ids': ids})

    sets = [{'id': sid, 'name': name, 'detail': detail, 'unit': unit, 'freq': freq, 'n': sum(1 for c in countries if sid in c['ids'])} for sid, _, name, detail, unit, freq in SETS]
    doc = {'engine': 'ciss-stress', 'document': 'sovereign-world', 'version': VERSION, 'contract': CONTRACT, 'generated_at': generated_at,
           'table': {'key': TABLE, 'generated_at': table.get('generated_at'), 'version': table.get('version'), 'countries': len(countries)},
           'sets': sets, 'countries': countries, 'measures': measures, 'series': series, 'missing': missing,
           'reported_since': min((m['start'] for m in measures.values() if m['source'] == 'reported'), default=None),
           'inputs': inputs or [], 'errors': issues,
           'coverage': {'countries': len(countries), 'measures': len(measures), 'by_source': counts, 'errors': len(issues), 'status': 'degraded' if issues else 'measured'},
           'definitions': {'yield': 'monthly average of the 10-year government benchmark yield, percent; the source is named on each line',
                           'cds': 'the CDS desk\'s 5-year spread for the sovereign from the day\'s public prints, basis points, only on days it traded; nothing is filled in between',
                           'reported': 'the figure World Government Bonds showed on that day, unchecked; where two publications fell on one day the later is kept',
                           'missing': 'countries for which the site holds no stored history in that set; nothing is estimated for them'},
           'calls_eligible': False, 'sizing_eligible': False,
           'decision': {'verb': 'WAIT', 'meaning': 'abstain', 'reason': 'Dated measurements do not establish a validated forecast or portfolio size.'}}
    return doc


# ----------------------------------------------------------------------------------------------- reading, state, publication

def http_json(url, timeout=30):
    with urllib.request.urlopen(urllib.request.Request(url, headers=CLIENT), timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw), raw


def refresh_oecd(state, wanted, now, fetch, deadline, pause=0.5, sleep=time.sleep):
    """Ask the site's resolver for each monthly series that is due; keep the cached one when it is not, or when the request fails."""
    cache, errors, asked = state.setdefault('oecd', {}), {}, 0
    for country in wanted:
        code = OECD.get(country)
        if not code:
            continue
        sid = 'fred:IRLTLT01%sM156N' % code
        if not due(cache.get(country), now):
            continue
        if time.monotonic() >= deadline:
            errors.setdefault('oecd', 'OUT_OF_TIME: some monthly series were not asked for in this run')
            break
        try:
            if asked:
                sleep(pause)
            asked += 1
            payload, raw = fetch(RESOLVER + urllib.parse.quote(sid, safe=''))
            obs = pairs((payload or {}).get('obs'), monthly=True)
            if len(obs) < MIN_POINTS:
                raise ValueError('no observations')
            cache[country] = {'id': sid, 'read_at': now.isoformat(), 'via': str((payload or {}).get('source') or '')[:120], 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
                              'obs': [[d, str(v)] for d, v in obs]}
        except Exception as exc:                            # noqa: BLE001 - one refused series must not cost the others
            errors[sid] = type(exc).__name__ + (' - the series read on %s is used' % cache[country]['read_at'][:10] if cache.get(country) else '')
    return errors, asked


def run(client, bucket, budget_seconds=150, now=None, fetch=http_json, sleep=time.sleep):
    """Read what the site keeps, bring the day-by-day record up to date, compile and publish.  Returns a small receipt."""
    t0 = time.monotonic()
    deadline = t0 + budget_seconds
    started = now or datetime.now(timezone.utc)
    generated = started.isoformat()
    inputs, errors = [], {}

    def get(key, note=True):
        obj = client.get_object(Bucket=bucket, Key=key)
        raw = obj['Body'].read()
        if note:
            inputs.append({'key': key, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw), 'written': obj['LastModified'].astimezone(timezone.utc).isoformat()})
        return json.loads(raw)

    def optional(name, *keys):
        try:
            return [get(k) for k in keys]
        except Exception as exc:                            # noqa: BLE001 - a set without its source is left out, the others stand
            errors[name] = 'UNAVAILABLE:' + type(exc).__name__
            return [None] * len(keys)

    table = get(TABLE)
    if not isinstance(table.get('countries'), list) or not table.get('generated_at'):
        raise ValueError('world table required')
    desk_doc, desk_history = optional('ecb', desk.CURRENT, desk.HISTORY)
    cds_packet, cds_history = optional('dtcc', CDS_PACKET, CDS_HISTORY)
    try:
        state = get(STATE, note=False)
        if state.get('contract') != STATE_CONTRACT:
            raise ValueError('unexpected state')
    except Exception as exc:                                # noqa: BLE001
        code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code'))
        if code not in ('NoSuchKey', '404'):
            errors['state'] = 'UNREADABLE:' + type(exc).__name__ + ' - the day-by-day record starts again from the publications still kept'
        state = {'contract': STATE_CONTRACT, 'reported': {}, 'oecd': {}, 'last_publication': None}

    # the table's own figures: every publication since the last one folded, oldest first, then today's
    problems = []
    earlier = walk(lambda key: get(key, note=False), table, state.get('last_publication'), deadline=t0 + budget_seconds * 0.4, problems=problems)
    if problems:
        errors['publications'] = 'CHAIN_UNREADABLE:%s - earlier days of the table\'s own figures were not recovered in this run' % problems[0]
    folded = sum(1 for head in reversed(earlier) if fold(state, head) is not None)
    fold(state, table)
    wanted = [row['country'] for row in table['countries'] if isinstance(row, dict) and isinstance(row.get('country'), str)]
    oecd_errors, asked = refresh_oecd(state, wanted, started, fetch, t0 + budget_seconds * 0.85, sleep=sleep)
    errors.update(oecd_errors)
    state['updated_at'] = generated
    client.put_object(Bucket=bucket, Key=STATE, Body=encoded(state), ContentType='application/json', CacheControl='no-store')

    for country, kept in sorted((state.get('oecd') or {}).items()):
        if country in wanted:
            inputs.append({'series': kept['id'], 'via': kept.get('via'), 'sha256': kept.get('sha256'), 'bytes': kept.get('bytes'), 'read_at': kept['read_at']})
    doc = compile_world(table, desk_doc, desk_history, cds_packet, cds_history, state, generated, inputs, errors)
    if digest(compile_world(table, desk_doc, desk_history, cds_packet, cds_history, state, generated, inputs, errors)) != digest(doc):
        raise ValueError('world history does not compile twice to the same bytes')
    doc['sha256'] = digest(doc)
    client.put_object(Bucket=bucket, Key=CURRENT, Body=encoded(doc), ContentType='application/json', CacheControl='no-cache')
    return {'published': True, 'generated_at': generated, 'coverage': doc['coverage'], 'sets': {s['id']: s['n'] for s in doc['sets']},
            'publications_folded': folded + 1, 'series_asked': asked, 'seconds': round(time.monotonic() - t0, 1)}
