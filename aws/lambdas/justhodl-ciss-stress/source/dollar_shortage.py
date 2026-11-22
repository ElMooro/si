"""The dollar-shortage document: data/ciss-dollar-shortage.json.

Three ECB measurements read together say whether the euro area is short of dollars:

  AUX_EURUSD     the euro's reference rate against the dollar (daily)
  AUX_FX_EA      the Eurosystem's claims on euro-area residents in foreign currency (weekly): what it has lent in
                 foreign currency, mostly dollars, to euro-area banks
  AUX_FX_NONEA   the Eurosystem's claims on non-euro-area residents in foreign currency (weekly): the foreign-currency
                 balances, securities and loans it holds abroad

The sovereign-stress desk document (data/ciss-sovereign.json) already carries the three as measures, each with the
receipt of the ECB response it was read from.  This module makes no request to the ECB: it reads those same retained
responses again and publishes the three with their complete histories - the reference rate for every day, where the
desk's history keeps one reading a week - in one small document that the Sovereign desk's "Dollar shortage" tab and
the Dollar desk both read.

Every figure must equal the desk document's own (same date, same value, same number of observations); if one does
not, nothing is published and the last document stays.  Nothing here is a forecast or a position size.
compile_dollar() is pure: the same desk document and the same originals give the same bytes.
"""
from datetime import datetime, timezone
from decimal import Decimal
import json
import time

from ciss_source_model import encoded, digest, clock
import sovereign_desk as desk

VERSION = '1.0.0'
CONTRACT = 'ciss-dollar-shortage.v1'
CURRENT = 'data/ciss-dollar-shortage.json'

# measure id, the desk's name for the ECB response, whether the ECB's unit multiplier is brought to billions
SERIES = (('AUX_EURUSD', 'eurusd', False), ('AUX_FX_EA', 'fx_claims_ea', True), ('AUX_FX_NONEA', 'fx_claims_nonea', True))
ABOUT = {
    'AUX_EURUSD': {'short': 'EUR/USD', 'worse': 'lower',
                   'what': 'The ECB\'s euro foreign exchange reference rate: US dollars for one euro, set each working day. A falling rate is a rising dollar.'},
    'AUX_FX_EA': {'short': 'Foreign-currency lending to euro-area banks', 'worse': 'higher',
                  'what': 'Asset item 3 of the Eurosystem\'s weekly financial statement, "claims on euro area residents denominated in foreign currency": '
                          'what the Eurosystem has lent in foreign currency, mostly US dollars, to euro-area banks. Its large rises were the ECB\'s dollar operations of 2008, 2011-12 and 2020.'},
    'AUX_FX_NONEA': {'short': 'Foreign-currency claims abroad', 'worse': None,
                     'what': 'Asset item 2 of the Eurosystem\'s weekly financial statement, "claims on non-euro area residents denominated in foreign currency": '
                             'balances with banks, security investments and loans abroad in foreign currency, and receivables from the IMF.'},
}


def compile_dollar(desk_doc, read, generated_at):
    """The dollar-shortage document from the desk document and the originals its measures name.

    desk_doc      the published sovereign-stress desk document
    read          receipt -> the exact original bytes
    Raises ValueError when a series does not reproduce the desk document's own figure.
    """
    clock(generated_at)
    cut = clock(desk_doc['generated_at']).date().isoformat()       # the desk's own day: observations after it were not in its figures
    by_name = {s['name']: s for s in desk_doc.get('sources') or []}
    measures, series, sources = {}, {}, []
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
        measures[mid] = {**m, **ABOUT[mid]}
        series[mid] = {'d': [d for d, _ in obs], 'v': [desk.number(v, m['unit']) for _, v in obs]}
        src = by_name.get(name) or {}
        sources.append({'id': mid, 'name': name, 'label': src.get('label'), 'path': src.get('path'), 'key': m['key'], 'evidence': m['evidence'], 'acquired_at': src.get('acquired_at'),
                        'carried': bool(src.get('carried')), 'newest': src.get('newest'), 'observations': len(obs)})
    return {'engine': 'ciss-stress', 'document': 'dollar-shortage', 'version': VERSION, 'contract': CONTRACT, 'generated_at': generated_at,
            'desk': {'key': desk.CURRENT, 'generated_at': desk_doc['generated_at'], 'version': desk_doc.get('version'), 'replay': desk_doc.get('replay')},
            'order': [mid for mid, _, _ in SERIES], 'measures': measures, 'series': series, 'episodes': desk_doc.get('episodes') or [], 'sources': sources,
            'aggregation': 'every usable observation the ECB publishes: daily for the reference rate, weekly for the two balance-sheet items',
            'units': {'usd': 'US dollars per euro', 'eur_bn': 'EUR billions'},
            'definitions': {'peaks': 'highest and lowest reading inside each named window',
                            'pctile': 'share of the series\' own observations at or below the latest, percent; not a probability',
                            'worse': 'the direction a dollar shortage moves the series in: a lower euro, more foreign-currency lending to banks; none where the page takes no side'},
            'coverage': {'measures': len(measures), 'observations': sum(len(s['d']) for s in series.values()), 'status': 'measured'},
            'calls_eligible': False, 'sizing_eligible': False,
            'decision': {'verb': 'WAIT', 'meaning': 'abstain', 'reason': 'Dated measurements do not establish a validated forecast or portfolio size.'}}


def run(client, bucket, store, now=None):
    """Compile from the desk document that is published and the retained originals it names; publish.  No ECB request."""
    t0 = time.monotonic()
    desk_doc = json.loads(client.get_object(Bucket=bucket, Key=desk.CURRENT)['Body'].read())
    if desk_doc.get('contract') != desk.CONTRACT:
        raise ValueError('sovereign desk document required')
    generated = (now or datetime.now(timezone.utc)).isoformat()

    def read(receipt):
        return store.original(client, bucket, receipt)

    doc = compile_dollar(desk_doc, read, generated)
    if digest(compile_dollar(desk_doc, read, generated)) != digest(doc):          # a second, separate read of every original
        raise ValueError('dollar-shortage document does not compile twice to the same bytes')
    doc['sha256'] = digest(doc)
    client.put_object(Bucket=bucket, Key=CURRENT, Body=encoded(doc), ContentType='application/json', CacheControl='no-cache')
    return {'published': True, 'generated_at': generated, 'desk_generated_at': desk_doc['generated_at'], 'coverage': doc['coverage'],
            'latest': {mid: [doc['measures'][mid]['value'], doc['measures'][mid]['date']] for mid in doc['order']}, 'seconds': round(time.monotonic() - t0, 1)}
