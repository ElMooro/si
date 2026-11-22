"""The dollar-shortage document: data/ciss-dollar-shortage.json.

Whether the world outside the United States is short of dollars shows in a handful of dated measurements.  This
module gathers them, in five parts, each with its whole history:

  backstop   dollars lent by central banks: the Federal Reserve's swap lines and its repos with foreign central
             banks (H.4.1), and the Eurosystem's foreign-currency lending to euro-area banks (ECB)
  price      the price of the dollar: DXY rebuilt from the Federal Reserve's H.10 rates, the Fed's broad, advanced-
             economy and emerging-market dollar indexes, and the ECB's euro reference rate
  official   what foreign central banks keep at the Federal Reserve: Treasuries in custody and their 13-week change,
             the foreign repo pool
  funding    banks' dollar funding: SOFR against the rate the Federal Reserve pays on reserves, the TED spread until
             it ended, and what foreign banks' US offices owe their offices abroad (H.8)
  reserves   the Eurosystem's own foreign reserves other than gold (not a gauge of dollars lent)

Two kinds of source, kept apart:

  * The three ECB measurements are the sovereign-stress desk document's own (data/ciss-sovereign.json).  They are read
    again from the retained ECB responses that document names, with every observation, and every figure must equal
    the desk document's (date, value, count, source row) - otherwise nothing is published.  No request to the ECB.
  * The Federal Reserve's series are read from the site's own warehouse through its series resolver, one at a time
    with a pause, at most once a day each, and kept in a state object.  A series that cannot be read today is taken
    from that state; one that was never read is left out and named in `errors`.  This part can never stop the ECB
    part from being published.

Nothing here is a forecast, a probability or a position size.  compile_dollar() is pure.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import time
import urllib.parse

from ciss_source_model import encoded, digest, clock, PREFIX
import sovereign_desk as desk
import world_history as world

VERSION = '1.1.0'
CONTRACT = 'ciss-dollar-shortage.v1'
CURRENT = 'data/ciss-dollar-shortage.json'
STATE = PREFIX + 'sovereign/dollar-state.json'
STATE_CONTRACT = 'ciss-dollar-state.v1'
FROM = '1999-01-01'                      # the euro's first day: nothing here is kept from before it

# measure id, the desk's name for the ECB response, whether the ECB's unit multiplier is brought to billions
SERIES = (('AUX_EURUSD', 'eurusd', False), ('AUX_FX_EA', 'fx_claims_ea', True), ('AUX_FX_NONEA', 'fx_claims_nonea', True))
ABOUT = {
    'AUX_EURUSD': {'part': 'price', 'short': 'EUR/USD', 'worse': 'lower',
                   'what': 'The ECB\'s euro foreign exchange reference rate: US dollars for one euro, set each working day. A falling rate is a rising dollar.'},
    'AUX_FX_EA': {'part': 'backstop', 'short': 'Eurosystem foreign-currency lending to euro-area banks', 'worse': 'higher',
                  'what': 'Asset item 3 of the Eurosystem\'s weekly financial statement, "claims on euro area residents denominated in foreign currency": '
                          'what the Eurosystem has lent in foreign currency, mostly US dollars, to euro-area banks. Its large rises were the ECB\'s dollar operations of 2008, 2011-12 and 2020.'},
    'AUX_FX_NONEA': {'part': 'reserves', 'short': 'Eurosystem foreign reserves other than gold', 'worse': None,
                     'what': 'Asset item 2 of the Eurosystem\'s weekly financial statement, "claims on non-euro area residents denominated in foreign currency": its position at the IMF '
                             '(item 2.1) and its deposits, securities and loans abroad (item 2.2). It does not measure dollars lent. It steps at each quarter-end revaluation, and it rose by about '
                             'EUR 120 bn in the week to 27 August 2021, when the IMF\'s allocation of special drawing rights was booked.'},
}

PARTS = (
    ('backstop', 'Dollars lent by central banks', 'What the Federal Reserve has lent to foreign central banks, and the Eurosystem to its own banks. These rise in a dollar shortage.'),
    ('price', 'The price of the dollar', 'A dollar shortage bids the dollar up against other currencies.'),
    ('official', 'Foreign official dollars at the Federal Reserve', 'What foreign central banks keep at the Federal Reserve. Their Treasuries in custody fall when they sell them to raise dollars.'),
    ('funding', 'Banks\' dollar funding', 'What dollars cost overnight against what the Federal Reserve pays on reserves, and what foreign banks\' US offices owe their offices abroad. '
                                           'The TED spread ended in 2022 and is kept for its history.'),
    ('reserves', 'The Eurosystem\'s own foreign reserves', 'Shown because it sits beside the lending item on the same statement; it is not a gauge of a dollar shortage.'),
)
# ICE's dollar index formula on the Federal Reserve's H.10 noon rates (the same arithmetic as the Dollar desk's calc:dxy)
DXY_K = 50.14348112
DXY_LEGS = (('DEXUSEU', -0.576), ('DEXJPUS', 0.136), ('DEXUSUK', -0.119), ('DEXCAUS', 0.091), ('DEXSDUS', 0.042), ('DEXSZUS', 0.036))
H41 = 'Federal Reserve H.4.1, Wednesday level'
H10 = 'Federal Reserve H.10'
IORB_FROM = '2021-07-29'                 # the day the rate on reserve balances replaced the rate on excess reserves
# `places` names a unit of the desk whose rounding is used (usd: 4 places, eur_bn: 3, bp: 2); `allow` is how many days
# after its date a reading still counts as current (H.10 is published a week in arrears).
# `lead`: FRED carries every H.4.1 line back to 18 December 2002 and fills the weeks before the Federal Reserve began
# printing the line with zeros (custody holdings until mid-2007, the foreign repo line until 2020).  Those zeros are not
# readings, so such a line's history begins at its first reading above zero; zeros after that are kept - they are real.
US = (
    {'id': 'FED_SWAPS', 'lead': True, 'part': 'backstop', 'fred': ('SWPT',), 'scale': Decimal('0.001'), 'unit': 'usd_bn', 'places': 'eur_bn', 'freq': 'W', 'allow': 21, 'worse': 'higher', 'release': H41,
     'label': 'Federal Reserve central bank liquidity swaps outstanding', 'short': 'Fed swap lines drawn',
     'what': 'Dollars the Federal Reserve has lent to foreign central banks through its swap lines and not yet been repaid. Foreign central banks draw on them to lend dollars to their own banks.'},
    {'id': 'FED_FIMA', 'lead': True, 'part': 'backstop', 'fred': ('H41RESPPALGTRFNWW',), 'scale': Decimal('0.001'), 'unit': 'usd_bn', 'places': 'eur_bn', 'freq': 'W', 'allow': 21, 'worse': 'higher', 'release': H41,
     'label': 'Federal Reserve repurchase agreements with foreign official accounts', 'short': 'Fed repos with foreign central banks (FIMA)',
     'what': 'Dollars the Federal Reserve has lent overnight to foreign central banks against their Treasuries, the FIMA repo facility. It lets them raise dollars without selling.'},
    {'id': 'USD_DXY', 'part': 'price', 'fred': tuple(sid for sid, _ in DXY_LEGS), 'calc': 'dxy', 'unit': 'idx', 'places': 'usd', 'freq': 'D', 'allow': 16, 'worse': 'higher', 'release': H10,
     'label': 'US dollar index (DXY), rebuilt from the Federal Reserve\'s H.10 rates', 'short': 'DXY, rebuilt from the Fed\'s H.10 rates',
     'what': 'ICE\'s dollar index formula on the Federal Reserve\'s daily noon rates for the euro, yen, pound, Canadian dollar, krona and franc; one value for each day all six were published. '
             'H.10 is released a week in arrears, so this runs behind the market\'s own print.'},
    {'id': 'USD_BROAD', 'part': 'price', 'fred': ('DTWEXBGS',), 'unit': 'idx', 'places': 'usd', 'freq': 'D', 'allow': 16, 'worse': 'higher', 'release': H10,
     'label': 'Nominal broad US dollar index', 'short': 'Broad dollar index',
     'what': 'The Federal Reserve\'s trade-weighted dollar against the currencies of its main trading partners, goods and services (January 2006 = 100).'},
    {'id': 'USD_AFE', 'part': 'price', 'fred': ('DTWEXAFEGS',), 'unit': 'idx', 'places': 'usd', 'freq': 'D', 'allow': 16, 'worse': 'higher', 'release': H10,
     'label': 'Nominal US dollar index against advanced foreign economies', 'short': 'Dollar against advanced economies',
     'what': 'The Federal Reserve\'s trade-weighted dollar against the euro area, Canada, Japan, the United Kingdom, Switzerland, Australia and Sweden.'},
    {'id': 'USD_EME', 'part': 'price', 'fred': ('DTWEXEMEGS',), 'unit': 'idx', 'places': 'usd', 'freq': 'D', 'allow': 16, 'worse': 'higher', 'release': H10,
     'label': 'Nominal US dollar index against emerging market economies', 'short': 'Dollar against emerging markets',
     'what': 'The Federal Reserve\'s trade-weighted dollar against emerging-market trading partners; where a dollar shortage usually bites first.'},
    {'id': 'FED_CUSTODY_13W', 'lead': True, 'part': 'official', 'fred': ('WMTSECL1',), 'scale': Decimal('0.001'), 'calc': 'change13', 'unit': 'usd_bn', 'places': 'eur_bn', 'freq': 'W', 'allow': 21,
     'worse': 'lower', 'release': H41,
     'label': 'US Treasury securities held in custody at the Federal Reserve for foreign official and international accounts: change over 13 weeks', 'short': 'Treasuries in custody: 13-week change',
     'what': 'How much the Treasuries that foreign central banks keep at the Federal Reserve have risen or fallen over the last 13 weeks. A large fall is foreign central banks '
             'selling Treasuries, which they do to raise dollars; the level itself has long trends of its own.'},
    {'id': 'FED_CUSTODY_UST', 'lead': True, 'part': 'official', 'fred': ('WMTSECL1',), 'scale': Decimal('0.001'), 'unit': 'usd_bn', 'places': 'eur_bn', 'freq': 'W', 'allow': 21, 'worse': None, 'release': H41,
     'label': 'US Treasury securities held in custody at the Federal Reserve for foreign official and international accounts', 'short': 'Treasuries in custody for foreign central banks',
     'what': 'Treasuries that foreign central banks keep at the Federal Reserve. The level rose for years and has been falling since 2021 for reasons of its own, '
             'so the page takes no side on it; the 13-week change beside it is the line to read.'},
    {'id': 'FED_FOREIGN_RRP', 'part': 'official', 'fred': ('WLRRAFOIAL',), 'scale': Decimal('0.001'), 'unit': 'usd_bn', 'places': 'eur_bn', 'freq': 'W', 'allow': 21, 'worse': None, 'release': H41,
     'label': 'Federal Reserve reverse repurchase agreements with foreign official and international accounts', 'short': 'Foreign repo pool at the Fed',
     'what': 'Cash that foreign central banks leave overnight at the Federal Reserve. In March 2020 it rose while their Treasuries in custody fell: they sold securities and held the dollars.'},
    {'id': 'USD_SOFR_IOR', 'part': 'funding', 'fred': ('SOFR', 'IORB', 'IOER'), 'calc': 'spread', 'unit': 'bp', 'places': 'bp', 'freq': 'D', 'allow': 10, 'worse': 'higher',
     'release': 'New York Fed and Federal Reserve Board',
     'label': 'SOFR minus the interest rate the Federal Reserve pays on reserves', 'short': 'SOFR minus interest on reserves',
     'what': 'The secured overnight financing rate less the rate the Federal Reserve pays banks on reserves (IOER until 28 July 2021, IORB since), in basis points. '
             'Above zero, borrowing dollars overnight against Treasuries costs more than the Fed pays to hold them.'},
    {'id': 'USD_TED', 'part': 'funding', 'fred': ('TEDRATE',), 'scale': Decimal(100), 'unit': 'bp', 'places': 'bp', 'freq': 'D', 'allow': 10, 'worse': 'higher', 'release': 'Federal Reserve Bank of St. Louis',
     'label': 'TED spread: three-month dollar LIBOR minus the three-month Treasury bill', 'short': 'TED spread (ended 2022)',
     'what': 'What banks paid for three-month dollars over the Treasury bill, in basis points. It ended in January 2022 with LIBOR and is kept for the shortages it recorded.'},
    {'id': 'H8_NET_DUE', 'part': 'funding', 'fred': ('NDFFRIW027NBOG',), 'unit': 'usd_bn', 'places': 'eur_bn', 'freq': 'W', 'allow': 24, 'worse': None, 'release': 'Federal Reserve H.8, Wednesday level, not seasonally adjusted',
     'label': 'Net due to related foreign offices, foreign-related institutions in the United States', 'short': 'Foreign banks\' US offices: net owed to offices abroad',
     'what': 'What the US branches and agencies of foreign banks owe their own offices abroad, net. Below zero the US offices are sending dollars to their parents, '
             'as they did on a large scale before and during 2008.'},
)
FRED_IDS = tuple(dict.fromkeys(sid for spec in US for sid in spec['fred']))


def dxy_obs(legs):
    """One DXY value for each day all six H.10 rates were published."""
    maps = [dict(legs[sid]) for sid, _ in DXY_LEGS]
    out = []
    for day in sorted(set.intersection(*(set(m) for m in maps))):
        value = DXY_K
        for (_, power), m in zip(DXY_LEGS, maps):
            if m[day] <= 0:
                break
            value *= float(m[day]) ** power
        else:
            out.append((day, Decimal(repr(round(value, 4)))))
    return out


def spread_obs(sofr, iorb, ioer):
    """SOFR minus the rate on reserves in basis points, on the days both exist: IOER before IORB began, IORB since."""
    paid = {day: value for day, value in ioer if day < IORB_FROM}
    paid.update((day, value) for day, value in iorb if day >= IORB_FROM)
    return [(day, (value - paid[day]) * 100) for day, value in sofr if day in paid]


def change_obs(obs, weeks):
    """Each reading less the reading exactly that many weeks earlier; a week with no reading that far back has no figure."""
    index = dict(obs)
    out = []
    for day, value in obs:
        earlier = index.get((date.fromisoformat(day) - timedelta(days=7 * weeks)).isoformat())
        if earlier is not None:
            out.append((day, value - earlier))
    return out


def us_obs(spec, kept, today):
    """A US measure's observations from the kept series, or None when a series it needs is not there."""
    parts = []
    for sid in spec['fred']:
        entry = kept.get(sid)
        if not entry or not entry.get('obs'):
            return None
        parts.append(world.pairs(entry['obs'], today=today))
    if spec.get('calc') == 'dxy':
        obs = dxy_obs(dict(zip(spec['fred'], parts)))
    elif spec.get('calc') == 'spread':
        obs = spread_obs(*parts)
    else:
        obs = [(day, value * spec['scale']) for day, value in parts[0]] if spec.get('scale') else parts[0]
    obs = [p for p in obs if p[0] >= FROM]
    if spec.get('lead'):
        first = next((i for i, (_, value) in enumerate(obs) if value > 0), len(obs))
        obs = obs[first:]
    if spec.get('calc') == 'change13':
        obs = change_obs(obs, 13)
    return obs


def us_measure(spec, obs, kept, today):
    m = desk.measure(spec['id'], spec['label'], spec['places'], spec['freq'], obs, today, group='dollar')
    for unused in ('area', 'evidence', 'source_row', 'exact', 'blank_after', 'left_out', 'derived', 'period'):
        m.pop(unused, None)
    age = (date.fromisoformat(today) - date.fromisoformat(m['date'])).days
    ids = ['fred:' + sid for sid in spec['fred']]
    m.update({'unit': spec['unit'], 'status': 'current' if age <= spec['allow'] else 'ended' if age > desk.ENDED else 'late', 'part': spec['part'], 'short': spec['short'], 'worse': spec['worse'],
              'what': spec['what'], 'source': 'fred', 'key': ', '.join(ids), 'series': list(spec['fred']),
              'source_label': '%s; FRED %s, from the site\'s warehouse' % (spec['release'], ', '.join(spec['fred'])),
              'read_at': max(kept[sid]['read_at'] for sid in spec['fred'])})
    return m


def compile_dollar(desk_doc, read, generated_at, kept=None, errors=None):
    """The dollar-shortage document.  Pure.

    desk_doc      the published sovereign-stress desk document
    read          receipt -> the exact original bytes
    kept          {FRED id: {'obs': [[date, value], ...], 'read_at', 'via', 'sha256', 'bytes'}} - the Federal Reserve's
                  series as last read from the warehouse; None or {} leaves the US measures out
    Raises ValueError when an ECB series does not reproduce the desk document's own figure.
    """
    today = clock(generated_at).date().isoformat()
    cut = clock(desk_doc['generated_at']).date().isoformat()       # the desk's own day: observations after it were not in its figures
    by_name = {s['name']: s for s in desk_doc.get('sources') or []}
    measures, series, sources, issues = {}, {}, [], dict(errors or {})
    for mid, name, billions in SERIES:
        m = (desk_doc.get('measures') or {}).get(mid)
        if not m or not m.get('evidence') or not m.get('key'):
            raise ValueError('the desk document does not carry ' + mid)
        entry = desk.flow_series(read(m['evidence'])).get(m['key'])
        if entry is None:
            raise ValueError('the retained response does not hold ' + m['key'])
        scale = Decimal(10) ** (int(entry['meta']['unit_mult']) - 9) if billions else None
        obs, info = desk.observations(entry, cut, scale)
        if not obs or obs[-1][0] != m['date'] or desk.number(obs[-1][1], m['unit']) != m['value'] or len(obs) != m['n'] or info.get('last_row') != m.get('source_row'):
            raise ValueError('%s does not reproduce the desk document: %s' % (mid, (obs[-1][0], str(obs[-1][1]), len(obs)) if obs else 'no observations'))
        measures[mid] = {**m, **ABOUT[mid], 'source': 'ecb'}
        series[mid] = {'d': [d for d, _ in obs], 'v': [desk.number(v, m['unit']) for _, v in obs]}
        src = by_name.get(name) or {}
        sources.append({'id': mid, 'name': name, 'label': src.get('label'), 'path': src.get('path'), 'key': m['key'], 'evidence': m['evidence'], 'acquired_at': src.get('acquired_at'),
                        'carried': bool(src.get('carried')), 'newest': src.get('newest'), 'observations': len(obs)})
    kept = kept or {}
    for spec in US:
        obs = us_obs(spec, kept, today)
        if obs is None or len(obs) < 2:
            issues.setdefault(spec['id'], 'NOT_HELD: ' + ', '.join(sid for sid in spec['fred'] if not (kept.get(sid) or {}).get('obs')) if obs is None else 'TOO_SHORT')
            continue
        measures[spec['id']] = us_measure(spec, obs, kept, today)
        series[spec['id']] = {'d': [d for d, _ in obs], 'v': [desk.number(v, spec['places']) for _, v in obs]}
        sources.append({'id': spec['id'], 'name': 'fred', 'label': spec['release'], 'series': list(spec['fred']), 'observations': len(obs), 'newest': obs[-1][0],
                        'read': [{'series': sid, 'read_at': kept[sid].get('read_at'), 'via': kept[sid].get('via'), 'sha256': kept[sid].get('sha256'), 'bytes': kept[sid].get('bytes')} for sid in spec['fred']]})
    parts = [{'id': pid, 'name': name, 'note': note, 'ids': [mid for mid in [s['id'] for s in US if s['part'] == pid] + [k for k, _, _ in SERIES if ABOUT[k]['part'] == pid] if mid in measures]}
             for pid, name, note in PARTS]
    for part in parts:
        if part['id'] == 'price':                                   # the euro's own rate closes the list of dollar indexes
            part['ids'].sort(key=lambda mid: mid == 'AUX_EURUSD')
    parts = [p for p in parts if p['ids']]
    return {'engine': 'ciss-stress', 'document': 'dollar-shortage', 'version': VERSION, 'contract': CONTRACT, 'generated_at': generated_at,
            'desk': {'key': desk.CURRENT, 'generated_at': desk_doc['generated_at'], 'version': desk_doc.get('version'), 'replay': desk_doc.get('replay')},
            'parts': parts, 'order': [mid for p in parts for mid in p['ids']], 'ecb': [mid for mid, _, _ in SERIES],
            'measures': measures, 'series': series, 'episodes': desk_doc.get('episodes') or [], 'sources': sources, 'errors': issues,
            'aggregation': 'every usable observation its source publishes: daily for exchange rates, indexes and money-market rates, weekly for the ECB\'s and the Federal Reserve\'s balance-sheet items',
            'units': {'usd': 'US dollars per euro', 'eur_bn': 'EUR billions', 'usd_bn': 'USD billions', 'idx': 'index level', 'bp': 'basis points'},
            'definitions': {'peaks': 'highest and lowest reading inside each named window',
                            'pctile': 'share of the series\' own observations at or below the latest, percent; not a probability',
                            'worse': 'the direction a dollar shortage moves the series in; none where the page takes no side',
                            'USD_DXY': '50.14348112 x EURUSD^-0.576 x USDJPY^0.136 x GBPUSD^-0.119 x USDCAD^0.091 x USDSEK^0.042 x USDCHF^0.036 on H.10 rates',
                            'start': 'an H.4.1 line begins at its first reading above zero: FRED fills the weeks before the line was printed with zeros',
                            'FED_CUSTODY_13W': 'the Wednesday level less the Wednesday level 13 weeks (91 days) earlier',
                            'absent': 'a cross-currency basis is not here: no free official source publishes one'},
            'coverage': {'measures': len(measures), 'observations': sum(len(s['d']) for s in series.values()), 'ecb': len(SERIES), 'fed': len(measures) - len(SERIES),
                         'errors': len(issues), 'status': 'degraded' if issues else 'measured'},
            'calls_eligible': False, 'sizing_eligible': False,
            'decision': {'verb': 'WAIT', 'meaning': 'abstain', 'reason': 'Dated measurements do not establish a validated forecast or portfolio size.'}}


# ------------------------------------------------------------------------------------- the warehouse's series, state, publication

def due(cached, now):
    """Whether a kept series should be asked for again: once a day; a series its source ended, once a week."""
    if not cached or not cached.get('obs') or not cached.get('read_at'):
        return True
    hours = (now - clock(cached['read_at'])).total_seconds() / 3600
    if hours < 20:
        return False
    if (now.date() - date.fromisoformat(cached['obs'][-1][0])).days > desk.ENDED:
        return hours >= 7 * 24
    return True


def refresh(state, now, fetch, deadline, pause=0.5, sleep=time.sleep):
    """Ask the site's resolver for each series that is due; keep what was read before when it is not, or when the request fails."""
    kept, errors, asked = state.setdefault('fred', {}), {}, 0
    for sid in FRED_IDS:
        if not due(kept.get(sid), now):
            continue
        if time.monotonic() >= deadline:
            errors.setdefault('fred', 'OUT_OF_TIME: some series were not asked for in this run; what was read before is used')
            break
        try:
            if asked:
                sleep(pause)
            asked += 1
            payload, raw = fetch(world.RESOLVER + urllib.parse.quote('fred:' + sid, safe=''))
            obs = [p for p in world.pairs((payload or {}).get('obs')) if p[0] >= FROM]
            if len(obs) < 2:
                raise ValueError('no observations')
            kept[sid] = {'read_at': now.isoformat(), 'via': str((payload or {}).get('source') or '')[:120], 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
                         'obs': [[d, str(v)] for d, v in obs]}
        except Exception as exc:                            # noqa: BLE001 - one refused series must not cost the others
            errors['fred:' + sid] = type(exc).__name__ + (' - the series read on %s is used' % kept[sid]['read_at'][:10] if kept.get(sid) else '')
    return errors, asked


def ask(url):
    return world.http_json(url, timeout=50)


def run(client, bucket, store, now=None, budget_seconds=120, fetch=ask, sleep=time.sleep):
    """Compile and publish.  The ECB part reads only what is already kept; the US part asks the site's own resolver."""
    t0 = time.monotonic()
    desk_doc = json.loads(client.get_object(Bucket=bucket, Key=desk.CURRENT)['Body'].read())
    if desk_doc.get('contract') != desk.CONTRACT:
        raise ValueError('sovereign desk document required')
    started = now or datetime.now(timezone.utc)
    generated = started.isoformat()
    errors, asked = {}, 0
    try:
        state = json.loads(client.get_object(Bucket=bucket, Key=STATE)['Body'].read())
        if state.get('contract') != STATE_CONTRACT or not isinstance(state.get('fred'), dict):
            raise ValueError('unexpected state')
    except Exception as exc:                                # noqa: BLE001
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('NoSuchKey', '404'):
            errors['state'] = 'UNREADABLE:' + type(exc).__name__ + ' - every series is asked for again'
        state = {'contract': STATE_CONTRACT, 'fred': {}}
    try:
        more, asked = refresh(state, started, fetch, t0 + budget_seconds * 0.8, sleep=sleep)
        errors.update(more)
        if asked:
            state['updated_at'] = generated
            client.put_object(Bucket=bucket, Key=STATE, Body=encoded(state), ContentType='application/json', CacheControl='no-store')
    except Exception as exc:                                # noqa: BLE001 - the ECB part is published whatever happens to this one
        errors['fed'] = 'UNAVAILABLE:' + type(exc).__name__

    def read(receipt):
        return store.original(client, bucket, receipt)

    kept = state.get('fred') or {}
    doc = compile_dollar(desk_doc, read, generated, kept, errors)
    if digest(compile_dollar(desk_doc, read, generated, kept, errors)) != digest(doc):       # a second, separate read of every original
        raise ValueError('dollar-shortage document does not compile twice to the same bytes')
    doc['sha256'] = digest(doc)
    client.put_object(Bucket=bucket, Key=CURRENT, Body=encoded(doc), ContentType='application/json', CacheControl='no-cache')
    return {'published': True, 'generated_at': generated, 'desk_generated_at': desk_doc['generated_at'], 'coverage': doc['coverage'], 'series_asked': asked,
            'latest': {mid: [doc['measures'][mid]['value'], doc['measures'][mid]['date']] for mid in doc['order']}, 'seconds': round(time.monotonic() - t0, 1)}
