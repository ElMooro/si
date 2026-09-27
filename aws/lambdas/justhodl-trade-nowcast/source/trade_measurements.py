"""Calendar-specific transport prices and CPB merchandise-volume observations.

Definitions: FRED/BLS series pages and CPB's original monthly workbook. These
are descriptive measurements, not independent votes or investment forecasts.
"""
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from io import BytesIO
import hashlib, math, re, zipfile, xml.etree.ElementTree as ET
from urllib.parse import urlsplit, urljoin

CONTRACT = 'trade-calendar-measurements.v1'
FLAGS = dict.fromkeys(('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible'), False)
PROFILES = {
    'PCU4831114831115': ('ocean_ppi', 'Index Jun 1988=100', 'Producer Price Index by Industry: Deep Sea Freight Transportation: Deep Sea Freight Transportation Services'),
    'PCU484121484121': ('truck_ppi', 'Index Dec 2003=100', 'Producer Price Index by Industry: General Freight Trucking, Long-Distance Truckload'),
    'IR': ('import_prices', 'Index 2000=100', 'Import Price Index (End Use): All Commodities'),
    'IQ': ('export_prices', 'Index 2000=100', 'Export Price Index (End Use): All Commodities'),
}
MONTHS = 'januari februari maart april mei juni juli augustus september oktober november december'.split()
ENGLISH = 'january february march april may june july august september october november december'.split()
NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
RNS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
PNS = 'http://schemas.openxmlformats.org/package/2006/relationships'
REGIONS = ('w1', 'i1', 'e6', 'us', 'gb', 'jp', 'a3', 'r2', 'd1', 'cn', 'a5', 't1', 'l1', 'f3')
TRADE_CODES = {'tgz_w1_qnmi_sn', 'tgz_w1_pdmi_sn', 'hfl_w1_pdmi_nn', 'hpr_w1_pdmi_nn'} | {
    f'{flow}_{region}_{measure}' for flow in ('mgz', 'xgz') for region in REGIONS for measure in ('qnmi_sn', 'pdmi_sn')}
IP_CODES = {f'ipz_{region}_qnmi_{weight}' for region in REGIONS for weight in ('sm', 'sp')}


class MeasurementError(ValueError):
    pass


def clock(value):
    if not isinstance(value, str) or 'T' not in value:
        raise MeasurementError('Aware publication clock required')
    d = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if d.tzinfo is None:
        raise MeasurementError('Aware publication clock required')
    return d.astimezone(timezone.utc)


def month(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-(?:0[1-9]|1[0-2])', value):
        raise MeasurementError('Exact monthly identity required')
    return date.fromisoformat(value + '-01')


def shift(value, months):
    d = month(value); n = d.year * 12 + d.month - 1 + months
    return f'{n // 12:04d}-{n % 12 + 1:02d}'


def number(value):
    if value in (None, '', '.'):
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise MeasurementError('Published numeric value required')
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        raise MeasurementError('Invalid source number') from None
    if not d.is_finite() or d < 0 or d > Decimal('1e15') or (d != 0 and float(d) == 0):
        raise MeasurementError('Finite nonnegative representable source number required')
    return d


def change(current, previous, current_month, previous_month):
    status = 'latest_missing' if current is None else 'prior_month_missing' if previous is None else 'zero_denominator' if previous == 0 else 'measured'
    return {'status': status, 'percent': float((current / previous - 1) * 100) if status == 'measured' else None,
            'current_month': current_month, 'previous_month': previous_month, 'unit': 'percent'}


def summary(values):
    if not values:
        raise MeasurementError('Monthly population is empty')
    latest = max(values); current = values[latest]
    out = {'latest_month': latest, 'level': float(current) if current is not None else None,
           'status': 'measured' if current is not None else 'latest_missing'}
    for name, distance in (('mom', -1), ('three_month', -3), ('yoy', -12)):
        prior = shift(latest, distance)
        out[name] = change(current, values.get(prior), latest, prior)
    return out


def fred(sid, metadata, packet, at):
    key, unit, name = PROFILES[sid]; cutoff = clock(at).date()
    out = {'series_id': sid, 'key': key, 'name': name, 'source_url': 'https://fred.stlouisfed.org/series/' + sid,
           'status': 'unavailable', 'definition_status': 'unverified', 'unit': unit, 'seasonal_adjustment': 'NSA',
           'frequency': 'monthly', 'observation_date_kind': 'month_start_label', 'observations': [],
           'returned_rows': 0, 'original_vintage_verified': False, 'observation_freshness_verified': False, **FLAGS}
    records = metadata.get('seriess') if isinstance(metadata, dict) else None
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], dict) or records[0].get('id') != sid:
        out['status'] = 'metadata_identity_mismatch'; return out
    meta = records[0]; out['metadata'] = meta
    if (meta.get('units'), meta.get('frequency_short'), meta.get('seasonal_adjustment_short')) != (unit, 'M', 'NSA'):
        out['status'] = 'metadata_definition_changed'; return out
    out['definition_status'] = 'reviewed_source_definition'
    rows = packet.get('observations') if isinstance(packet, dict) else None
    if (not isinstance(rows, list) or type(packet.get('count')) is not int or packet['count'] != len(rows)
            or type(packet.get('offset')) is not int or packet['offset'] != 0 or packet.get('units') != 'lin'
            or type(packet.get('output_type')) is not int or packet['output_type'] != 1):
        out['status'] = 'incomplete_or_transformed_response'; return out
    values = {}; ambiguous = False
    for position, row in enumerate(rows):
        item = {'position': position, 'original': row, 'month': None, 'value': None, 'status': 'invalid_observation'}
        try:
            if not isinstance(row, dict):
                raise MeasurementError('Observation object required')
            label = row.get('date'); d = date.fromisoformat(label)
            if label != d.isoformat() or d.day != 1 or d > cutoff:
                raise MeasurementError('Monthly observation identity differs')
            ym = label[:7]; value = number(row.get('value'))
            if ym in values:
                raise MeasurementError('Duplicate month')
            values[ym] = value
            item.update(month=ym, value=float(value) if value is not None else None, status='observed' if value is not None else 'missing')
        except (ValueError, TypeError):
            ambiguous = True
        out['observations'].append(item)
    out['returned_rows'] = len(rows)
    if ambiguous:
        out['status'] = 'ambiguous_or_invalid_observations'; return out
    if not values:
        out['status'] = 'empty_observations'; return out
    out.update(summary(values), observation_age_days=(cutoff - month(max(values))).days,
               annualized_three_month_percent=None, annualization_status='not_seasonally_adjusted')
    return out


def xml(raw):
    if len(raw) > 64 * 1024 * 1024 or b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
        raise MeasurementError('Unreviewed XML declaration or size')
    return ET.fromstring(raw)


def cpb_candidates(raw, at):
    root = xml(raw); namespace = '{http://www.sitemaps.org/schemas/sitemap/0.9}'
    if root.tag != namespace + 'urlset':
        raise MeasurementError('Complete CPB URL sitemap required')
    candidates = []; seen = set(); cutoff = clock(at).date().strftime('%Y-%m')
    for node in root.findall(namespace + 'url'):
        url = node.findtext(namespace + 'loc')
        if not isinstance(url, str) or url in seen:
            raise MeasurementError('Unique sitemap URL required')
        seen.add(url)
        if not any(word in url.lower() for word in ('wereldhandelsmonitor', 'world-trade-monitor')):
            continue
        parsed = urlsplit(url)
        match = re.fullmatch(r'/wereldhandelsmonitor/cpb-wereldhandelsmonitor-(' + '|'.join(MONTHS) + r')-(\d{4})', parsed.path)
        period = f'{int(match[2]):04d}-{MONTHS.index(match[1]) + 1:02d}' if match else None
        eligible = (parsed.scheme == 'https' and parsed.netloc == 'www.cpb.nl' and not parsed.query and not parsed.fragment
                    and period is not None and period < cutoff)
        candidates.append({'url': url, 'last_modified': node.findtext(namespace + 'lastmod'), 'period': period,
                           'status': 'report_candidate' if eligible else 'agenda_or_unreviewed_route_excluded'})
    eligible = [r for r in candidates if r['status'] == 'report_candidate']
    if not eligible:
        raise MeasurementError('No exact CPB report candidate')
    latest = max(r['period'] for r in eligible); choices = [r for r in eligible if r['period'] == latest]
    if len(choices) != 1:
        raise MeasurementError('Latest report identity is ambiguous')
    return {'sitemap_url': 'https://www.cpb.nl/sitemap.xml', 'sitemap_entries': len(seen),
            'candidates': candidates, 'selected_url': choices[0]['url'], 'period': latest}


class ReportHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True); self.meta = {}; self.links = []; self.article_count = 0
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == 'meta':
            key = a.get('name') or a.get('property')
            if key in ('contenttype', 'publicationdatetime', 'og:url', 'og:description'):
                if key in self.meta:
                    raise MeasurementError('Duplicate report metadata')
                self.meta[key] = a.get('content')
        if tag == 'article' and 'node--type-world-trade-monitor' in a.get('class', '').split():
            self.article_count += 1
        if tag == 'a' and a.get('href'):
            self.links.append(a['href'])


def cpb_report(raw, discovery, at):
    parser = ReportHTML(); parser.feed(raw.decode('utf-8')); parser.close()
    url = discovery['selected_url']; ym = discovery['period']; meta = parser.meta
    if parser.article_count != 1 or meta.get('contenttype') != 'world_trade_monitor' or meta.get('og:url') != url:
        raise MeasurementError('Actual CPB report type/canonical identity required')
    published = clock(meta.get('publicationdatetime'))
    if published > clock(at) or published.date() < month(ym):
        raise MeasurementError('CPB publication clock differs')
    expected = 'https://www.cpb.nl/system/files/cpbmedia/CPB-world-trade-monitor-' + ENGLISH[int(ym[5:]) - 1] + '-' + ym[:4] + '.xlsx'
    matches = {urljoin(url, link) for link in parser.links if urljoin(url, link) == expected}
    if matches != {expected}:
        raise MeasurementError('Same-period official workbook link required')
    return {'report_url': url, 'period': ym, 'published_at': meta['publicationdatetime'], 'workbook_url': expected,
            'source_description': meta.get('og:description'), 'description_used_for_numbers': False}


def col_number(col):
    n = 0
    for c in col:
        n = n * 26 + ord(c) - 64
    return n


def workbook_sheets(raw):
    if len(raw) > 16 * 1024 * 1024:
        raise MeasurementError('Whole workbook exceeds bound')
    with zipfile.ZipFile(BytesIO(raw)) as z:
        members = z.infolist(); names = [i.filename for i in members]
        if (len(names) != len(set(names)) or len(names) > 1000 or sum(i.file_size for i in members) > 128 * 1024 * 1024
                or any(i.file_size > 64 * 1024 * 1024 or i.flag_bits & 1 for i in members)):
            raise MeasurementError('Ambiguous, encrypted or oversized workbook')
        book = xml(z.read('xl/workbook.xml')); sheets = book.findall('{' + NS + '}sheets/{' + NS + '}sheet')
        if [s.get('name') for s in sheets] != ['trade_out', 'inpro_out']:
            raise MeasurementError('Exact trade and industrial-production sheets required')
        rels = xml(z.read('xl/_rels/workbook.xml.rels'))
        shared = xml(z.read('xl/sharedStrings.xml')) if 'xl/sharedStrings.xml' in names else None
        strings = [''.join(t.text or '' for t in si.iter('{' + NS + '}t')) for si in shared.findall('{' + NS + '}si')] if shared is not None else []
        result = {}
        for i, sheet in enumerate(sheets, 1):
            rid = sheet.get('{' + RNS + '}id'); matches = [r for r in rels if r.get('Id') == rid]
            target = f'worksheets/sheet{i}.xml'
            if (len(matches) != 1 or matches[0].get('TargetMode') == 'External'
                    or matches[0].get('Type') != RNS + '/worksheet' or matches[0].get('Target') not in (target, '/xl/' + target)):
                raise MeasurementError('Exact internal sheet relationship required')
            root = xml(z.read('xl/' + target)); rows = {}
            for row in root.findall('{' + NS + '}sheetData/{' + NS + '}row'):
                r = row.get('r', '')
                if not r.isdigit() or int(r) < 1 or int(r) in rows:
                    raise MeasurementError('Unique positive row required')
                cells = {}; rows[int(r)] = cells
                for cell in row.findall('{' + NS + '}c'):
                    ref = cell.get('r', ''); match = re.fullmatch(r'([A-Z]+)([1-9][0-9]*)', ref)
                    if not match or match[2] != r or match[1] in cells:
                        raise MeasurementError('Exact unique cell identity required')
                    v = cell.find('{' + NS + '}v'); kind = cell.get('t', 'n'); formula = cell.find('{' + NS + '}f')
                    value = v.text if v is not None else None
                    if kind == 's' and value is not None:
                        if not value.isdigit() or int(value) >= len(strings):
                            raise MeasurementError('Shared string index differs')
                        value = strings[int(value)]
                    elif kind == 'inlineStr':
                        value = ''.join(t.text or '' for t in cell.iter('{' + NS + '}t'))
                    cells[match[1]] = {'cell': ref, 'value': value, 'type': kind, 'formula': formula.text if formula is not None else None,
                                         'has_formula': formula is not None}
            result[sheet.get('name')] = rows
    return result


def cpb_workbook(raw, report, at):
    try:
        sheets = workbook_sheets(raw)
    except (ET.ParseError, zipfile.BadZipFile, KeyError):
        raise MeasurementError('Invalid complete workbook') from None
    results = {}; common_calendar = None; sheet_metadata = {}
    for name, rows in sheets.items():
        def text(r, c):
            cell = rows.get(r, {}).get(c, {})
            if cell.get('has_formula'):
                raise MeasurementError('Direct source header required')
            return cell.get('value')
        title = 'Merchandise world trade, fixed base 2021=100' if name == 'trade_out' else 'Industrial production volume excluding construction, fixed base 2021=100'
        if text(1, 'B') != 'CPB WORLD TRADE MONITOR' or text(2, 'B') != title:
            raise MeasurementError('CPB workbook scope or index base changed')
        sections = {6: 'Volumes, seasonally adjusted', 40: 'Prices / unit values in usd', 74: 'Prices / unit values in usd'} if name == 'trade_out' else {6: 'Import weighted, seasonally adjusted', 24: 'Production weighted, seasonally adjusted'}
        if any(text(r, 'B') != label for r, label in sections.items()):
            raise MeasurementError('CPB section definition changed')
        calendar = []
        for col, cell in sorted(rows.get(4, {}).items(), key=lambda p: col_number(p[0])):
            if col_number(col) < 6 or cell['value'] in (None, ''):
                continue
            match = re.fullmatch(r'(\d{4})m(0[1-9]|1[0-2])', str(cell['value']))
            if cell['has_formula'] or not match:
                raise MeasurementError('Exact direct source month required')
            ym = match[1] + '-' + match[2]
            if ym >= clock(at).strftime('%Y-%m') or (calendar and ym != shift(calendar[-1]['month'], 1)) or col_number(col) != len(calendar) + 6:
                raise MeasurementError('Duplicate, missing or future workbook calendar')
            calendar.append({'column': col, 'cell': cell['cell'], 'month': ym})
        if not calendar or calendar[0]['month'] != '2000-01' or calendar[-1]['month'] != report['period']:
            raise MeasurementError('Report/workbook calendar differs')
        if common_calendar is not None and calendar != common_calendar:
            raise MeasurementError('Sheet calendars differ')
        common_calendar = calendar; expected = TRADE_CODES if name == 'trade_out' else IP_CODES
        found = set(); sheet_metadata[name] = {'title': title, 'build_clock_text': text(4, 'B'), 'build_clock_is_publication': False, 'sections': {str(k): v for k, v in sections.items()}}
        for r, cells in sorted(rows.items()):
            code = text(r, 'C')
            if code in (None, ''):
                # A new numeric data row must not disappear just because its ID is absent.
                if r > 4 and any(c['value'] not in (None, '') for col, c in cells.items() if col_number(col) >= 6):
                    raise MeasurementError('Monthly source row has no series identity')
                continue
            if code not in expected or code in found or code in results:
                raise MeasurementError('Unknown or duplicate CPB series identity')
            found.add(code); values = {}; observations = []
            for entry in calendar:
                cell = cells.get(entry['column']); value = None
                if cell is not None:
                    if cell['has_formula'] or cell['value'] not in (None, '') and cell['type'] != 'n':
                        raise MeasurementError('Direct published monthly numeric cell required')
                    value = number(cell['value'])
                values[entry['month']] = value
                observations.append({'month': entry['month'], 'cell': entry['column'] + str(r), 'provider_value': cell['value'] if cell else None,
                                     'value': float(value) if value is not None else None, 'status': 'observed' if value is not None else 'missing'})
            allowed_columns = {e['column'] for e in calendar} | {'A', 'B', 'C', 'D', 'E'}
            if any(c['value'] not in (None, '') for col, c in cells.items() if col not in allowed_columns):
                raise MeasurementError('Monthly values outside declared calendar')
            section_row = max(s for s in sections if s < r)
            # Section membership and source code must agree, not just the row label.
            required_section = 6 if '_qnmi_' in code and name == 'trade_out' or code.endswith('_sm') else 24 if code.endswith('_sp') else 74 if code.startswith(('hfl_', 'hpr_')) else 40
            if section_row != required_section:
                raise MeasurementError('Series moved across definitions')
            results[code] = {'series_id': code, 'sheet': name, 'row': r, 'name': text(r, 'B'), 'unit': 'Index 2021=100',
                             'section': sections[section_row], 'seasonal_adjustment': 'SA' if '_qnmi_' in code else 'not stated in section heading',
                             'base_year_weight_or_value_cell': cells.get('D'), 'monthly_observations': observations,
                             'monthly_count': len(observations), **summary(values), **FLAGS}
        if found != expected:
            raise MeasurementError('Complete declared CPB series population required')
    return {'status': 'measured', 'workbook_sha256': hashlib.sha256(raw).hexdigest(), 'report': report, 'sheets': sheet_metadata,
            'calendar': common_calendar, 'series': results, 'series_count': len(results), 'monthly_positions': sum(r['monthly_count'] for r in results.values()),
            'world_trade': results['tgz_w1_qnmi_sn'], 'original_vintage_verified': False, **FLAGS,
            'scope': 'Merchandise goods volumes, prices/unit values and separately weighted industrial production. Excludes services; gross trade is not national-accounts value added. Price indexes do not identify trade volume or demand.'}


def bdi(raw):
    text = raw.decode('utf-8'); candidates = []
    for pattern in (r'id="p"[^>]*>\s*([\d,.]+)', r'"last"\s*:\s*([\d.]+)', r'Baltic[^<]{0,60}?([\d,]{3,6}(?:\.\d+)?)\s*(?:points|index)'):
        for match in re.finditer(pattern, text, re.I):
            try:
                value = number(match[1].replace(',', ''))
            except MeasurementError:
                value = None
            candidates.append({'pattern': pattern, 'offset': match.start(), 'source_text': match[1], 'candidate_level': float(value) if value is not None else None})
    return {'status': 'unqualified_quote', 'level': None, 'quote_at': None, 'unit': 'index_points', 'candidates': candidates,
            'independent_sources': 1, 'read': 'A single scraped page has no verified quote timestamp or instrument binding. Repeated text matches are not independent confirmation.', **FLAGS}
