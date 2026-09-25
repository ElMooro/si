"""Inspect whole SEC CNS archives without inferring short sales or fail age."""
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from html.parser import HTMLParser
from io import BytesIO
import hashlib, re, zipfile
from urllib.parse import urljoin

INDEX = 'https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data'
FIELDS = ('SETTLEMENT DATE', 'CUSIP', 'SYMBOL', 'QUANTITY (FAILS)', 'DESCRIPTION', 'PRICE')
ZIP_PATTERN = r'https://www\.sec\.gov/files/data/fails-deliver-data/cnsfails(20[0-9]{2})(0[1-9]|1[0-2])([ab])\.zip'
MAX_ZIP, MAX_TEXT = 8 * 1024 * 1024, 32 * 1024 * 1024


def archive_period(url):
    match = re.fullmatch(ZIP_PATTERN, url)
    if not match:
        raise ValueError('Exact reviewed SEC archive URL required')
    year, month, half = match.groups()
    return int(year), int(month), half


def advertised_archives(raw, cutoff, count=2):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_ZIP:
        raise ValueError('Whole bounded index bytes required')
    cutoff = date.fromisoformat(cutoff)
    if count not in (2, 4, 6, 12):
        raise ValueError('Explicit reviewed archive window required')
    urls = set()
    class Links(HTMLParser):
        def handle_starttag(self, tag, attrs):
            if tag.lower() != 'a':
                return
            for key, value in attrs:
                if key.lower() != 'href' or not value:
                    continue
                candidate = urljoin(INDEX, value)
                if re.fullmatch(ZIP_PATTERN, candidate):
                    year, month, half = archive_period(candidate)
                    if date(year, month, 1 if half == 'a' else 16) <= cutoff:
                        urls.add(candidate)
    parser = Links(convert_charrefs=True)
    parser.feed(raw.decode('utf-8-sig', errors='strict'))
    ordered = sorted(urls, key=archive_period, reverse=True)
    if len(ordered) < count:
        raise ValueError('Requested complete archive window is not advertised')
    chosen = ordered[:count]
    ordinals = [y * 24 + (m - 1) * 2 + (h == 'b') for y, m, h in map(archive_period, chosen)]
    if any(a - b != 1 for a, b in zip(ordinals, ordinals[1:])):
        raise ValueError('Advertised half-month archive window has gaps')
    return chosen


def text_member(raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_ZIP:
        raise ValueError('Whole bounded ZIP required')
    with zipfile.ZipFile(BytesIO(raw)) as archive:
        members = archive.infolist()
        if len(members) != 1:
            raise ValueError('One complete text member required')
        member = members[0]
        if (member.is_dir() or not re.fullmatch(r'[A-Za-z0-9_.-]+\.txt', member.filename)
                or member.flag_bits & 1 or not 0 < member.file_size <= MAX_TEXT):
            raise ValueError('Invalid bounded plaintext archive member')
        with archive.open(member) as stream:
            body = stream.read(MAX_TEXT + 1)
        if len(body) != member.file_size:
            raise ValueError('Incomplete archive member')
    return {'name': member.filename, 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}, body


def rows(body, url, cutoff):
    cutoff = date.fromisoformat(cutoff)
    year, month, half = archive_period(url)
    lines = body.decode('utf-8-sig', errors='strict').splitlines()
    if not lines or tuple(lines[0].strip().split('|')) != FIELDS:
        raise ValueError('SEC source columns differ from reviewed schema')
    result, keys = [], set()
    for line_number, line in enumerate(lines[1:], 2):
        if not line.strip():
            continue
        fields = line.split('|')
        if len(fields) != len(FIELDS):
            raise ValueError(f'Incomplete SEC row at line {line_number}')
        stamp, cusip, symbol, quantity, description, price = fields
        if not re.fullmatch(r'\d{8}', stamp):
            raise ValueError(f'Invalid settlement at line {line_number}')
        day = date.fromisoformat(stamp[:4] + '-' + stamp[4:6] + '-' + stamp[6:])
        if day > cutoff:
            raise ValueError(f'Future settlement at line {line_number}')
        if day.year != year or day.month != month or ('a' if day.day <= 15 else 'b') != half:
            raise ValueError(f'Settlement outside advertised archive at line {line_number}')
        if not re.fullmatch(r'[A-Z0-9*@#]{9}', cusip) or not symbol or len(symbol) > 10 or not description:
            raise ValueError(f'Invalid exact reported identity at line {line_number}')
        if not re.fullmatch(r'\d+', quantity):
            raise ValueError(f'Invalid fail-balance quantity at line {line_number}')
        if price != '.':
            if not re.fullmatch(r'\d+(?:\.\d+)?', price) or not Decimal(price).is_finite():
                raise ValueError(f'Invalid previous-day source price at line {line_number}')
        key = (stamp, cusip)
        if key in keys:
            raise ValueError(f'Duplicate settlement/CUSIP at line {line_number}')
        keys.add(key)
        result.append({'source_line': line_number, 'settlement_date': day.isoformat(), 'cusip': cusip,
                       'symbol': symbol, 'description': description, 'fail_balance_shares': quantity,
                       'reported_price': price, 'previous_day_reported_price': None if price == '.' else price})
    if not result:
        raise ValueError('Nonempty whole SEC source required')
    return result


def inventory(raw, url, cutoff):
    member, body = text_member(raw)
    records = rows(body, url, cutoff)
    dates = Counter(r['settlement_date'] for r in records)
    symbols, cusips = defaultdict(set), defaultdict(set)
    for row in records:
        symbols[row['symbol']].add(row['cusip'])
        cusips[row['cusip']].add((row['symbol'], row['description']))
    return {'member': member, 'rows': len(records), 'settlements': dict(sorted(dates.items())),
            'cusips': len(cusips), 'symbols': len(symbols),
            'symbols_with_multiple_cusips': sum(len(v) > 1 for v in symbols.values()),
            'cusips_with_multiple_reported_labels': sum(len(v) > 1 for v in cusips.values()),
            'missing_previous_day_prices': sum(r['previous_day_reported_price'] is None for r in records),
            'reported_zero_balances': sum(int(r['fail_balance_shares']) == 0 for r in records),
            'quantity_definition': 'aggregate_net_outstanding_fail_balance_on_settlement_date',
            'price_currency_explicit_in_file': False, 'price_observation_date_explicit_in_file': False,
            'daily_new_fail_flow_measured': False, 'fail_age_measured': False,
            'short_sale_origin_verified': False, 'forced_buy_in_forecast_qualified': False,
            'historical_publication_time_verified': False, 'security_continuity_verified': False,
            'source_schema_valid': True}
