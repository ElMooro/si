"""Exact exchange responses. TWD trading values are not cross-border cash flows."""
from datetime import datetime, timezone, timedelta
import hashlib
import json
import re

TWSE = 'https://www.twse.com.tw/rwd/en/fund/BFI82U'
TPEX = 'https://www.tpex.org.tw/www/zh-tw/insti/summary'
OPENAPI = 'https://www.tpex.org.tw/openapi/v1/tpex_3insti_summary'
TZ = timezone(timedelta(hours=8))
SCOPES = {'twse': 'TWSE listed-board foreign investors including Mainland investors plus foreign dealers',
          'tpex': 'TPEx mainboard stock foreign and Mainland investors including foreign dealers'}


def clock(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None: raise ValueError('timezone required')
    return result.astimezone(timezone.utc)


def day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{8}', value): raise ValueError('exchange date required')
    return datetime.strptime(value, '%Y%m%d').date()


def integer(value):
    if not isinstance(value, str) or not re.fullmatch(r'-?(?:\d+|\d{1,3}(?:,\d{3})+)', value):
        raise ValueError('exact integer TWD field required')
    number = int(value.replace(',', ''))
    if abs(number) > 10**16: raise ValueError('exchange amount exceeds bound')
    return number


def amounts(row):
    values = [integer(v) for v in row]
    if len(values) != 3 or min(values[:2]) < 0 or values[0] - values[1] != values[2]:
        raise ValueError('buy minus sell must equal net exactly')
    return dict(zip(('buy_twd', 'sell_twd', 'net_twd'), map(str, values)))


def requested_url(board, requested=None):
    if requested is not None: day(requested)
    if board == 'twse': return TWSE + '?response=json' + ('&dayDate=' + requested + '&type=day' if requested else '')
    if board == 'tpex_openapi' and requested is None: return OPENAPI
    if board == 'tpex' and requested:
        return TPEX + '?type=Daily&prod=1&date=' + day(requested).strftime('%Y%%2F%m%%2F%d') + '&response=json'
    raise ValueError('unknown exchange request')


def parse(board, raw, descriptor):
    ev = descriptor['evidence']; url = requested_url(board, descriptor.get('requested_day'))
    base = url.split('?')[0]
    if (descriptor['url'] != url or ev.get('contract') != 'source-evidence.v1' or ev.get('captured') is not True
            or ev.get('provider') != ('twse' if board == 'twse' else 'tpex')
            or ev.get('source_url') != base or len(raw) != ev['bytes'] or hashlib.sha256(raw).hexdigest() != ev['sha256']):
        raise ValueError('original exchange identity differs')
    # Public date/type/product parameters are bound separately because shared capture strips unknown query keys.
    expected_key = 'data/evidence/' + ev['provider'] + '/' + hashlib.sha256(base.encode()).hexdigest() + '/' + ev['sha256'] + '.bin.gz'
    if ev['key'] != expected_key: raise ValueError('original exchange key differs')
    acquired = clock(descriptor['acquired_at']); first = clock(ev['first_received_at'])
    if first > acquired: raise ValueError('receipt clock after acquisition')
    j = json.loads(raw); selected = {}; notes = []; definition = None
    if board == 'twse':
        if (j.get('stat') != 'OK' or j.get('fields') != ['Item', 'Total Buy', 'Total Sell', 'Difference']
                or j.get('hints') != '(NT$)' or j.get('params', {}).get('type') != 'day'):
            raise ValueError('TWSE daily definition or unit unavailable')
        date = j['date']; notes = j.get('notes', [])
        if j.get('params', {}).get('dayDate') != date: raise ValueError('TWSE response date identity differs')
        for index, row in enumerate(j['data']):
            if len(row) != 4: raise ValueError('TWSE row shape changed')
            label = str(row[0]).strip().lower()
            kind = 'investors' if label.startswith('foreign investors') and 'foreign dealers excluded' in label else 'dealers' if label == 'foreign dealers' else None
            if kind:
                if kind in selected: raise ValueError('duplicate foreign category')
                selected[kind] = {**amounts(row[1:]), 'original_row_index': index, 'label': row[0]}
        if set(selected) != {'investors', 'dealers'}: raise ValueError('disjoint foreign categories absent')
        total = {k: str(sum(int(v[k]) for v in selected.values())) for k in ('buy_twd', 'sell_twd', 'net_twd')}
        definition = {'unit': 'TWD', 'unit_source': 'response.hints', 'fields': j['fields'], 'category_rule': 'investors excluding foreign dealers plus foreign dealers'}
    else:
        if board == 'tpex':
            if j.get('stat') != 'ok' or j.get('template') != '/template/insti/summary' or len(j.get('tables', [])) != 1:
                raise ValueError('TPEx daily report unavailable')
            table = j['tables'][0]; date = j['date']
            if table.get('prod') != '1' or table.get('fields') != ['單位名稱', '買進金額(元)', '賣出金額(元)', '買賣超(元)']:
                raise ValueError('TPEx stock product or TWD units changed')
            roc = re.fullmatch(r'(\d{3})/(\d{2})/(\d{2})', table['date'])
            if not roc or str(int(roc[1]) + 1911) + roc[2] + roc[3] != date: raise ValueError('TPEx date identities disagree')
            rows = table['data']; notes = table.get('notes', [])
            definition = {'unit': 'TWD', 'unit_source': 'tables[0].fields', 'fields': table['fields'], 'product': '1', 'category_rule': 'foreign total equals investors excluding dealers plus foreign dealers'}
        else:
            if not isinstance(j, list) or not j: raise ValueError('TPEx OpenAPI rows absent')
            dates = {row.get('Date') for row in j}
            if len(dates) != 1: raise ValueError('mixed TPEx OpenAPI dates')
            roc = dates.pop()
            if not isinstance(roc, str) or not re.fullmatch(r'\d{7}', roc): raise ValueError('ROC date required')
            date = str(int(roc[:3]) + 1911) + roc[3:]
            rows = [[r['Investor'], r['PurchaseAmount'], r['SaleAmount'], r['Net']] for r in j]
            definition = {'unit': None, 'unit_source': 'requires matching dated official report'}
        names = {'外資及陸資合計': 'total', '外資及陸資(不含自營商)': 'investors', '外資自營商': 'dealers'}
        for index, row in enumerate(rows):
            if len(row) != 4: raise ValueError('TPEx row shape changed')
            kind = names.get(str(row[0]).strip())
            if kind:
                if kind in selected: raise ValueError('duplicate foreign category')
                selected[kind] = {**amounts(row[1:]), 'original_row_index': index, 'label': row[0]}
        if set(selected) != {'total', 'investors', 'dealers'}: raise ValueError('TPEx category decomposition absent')
        total = {k: selected['total'][k] for k in ('buy_twd', 'sell_twd', 'net_twd')}
        for k, value in total.items():
            if int(value) != int(selected['investors'][k]) + int(selected['dealers'][k]): raise ValueError('TPEx total differs from disjoint parts')
    if day(date) > acquired.astimezone(TZ).date(): raise ValueError('future exchange date')
    if descriptor.get('requested_day') and date != descriptor['requested_day']: raise ValueError('requested exchange date differs')
    return {'board': 'tpex' if board.startswith('tpex') else 'twse', 'date': date, **total,
            'categories': selected, 'definition': definition, 'notes': notes, 'acquired_at': descriptor['acquired_at'],
            'original': ev, 'request': {'url': url, 'requested_day': descriptor.get('requested_day')}}


def reproduce(descriptor, read):
    return parse(descriptor['board'], read(descriptor['evidence']['key']), descriptor)
