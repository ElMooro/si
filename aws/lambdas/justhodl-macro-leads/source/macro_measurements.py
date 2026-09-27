"""Exact monthly heavy-truck and complete source GPR workbook research."""
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import math, re, hashlib
import xlrd

FLAGS = dict.fromkeys(('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible'), False)
CONTRACT = 'macro-calendar-measurements.v1'
REQUIRED_GPR_SERIES = frozenset('GPR GPRT GPRA GPRH GPRHT GPRHA SHARE_GPR N10 SHARE_GPRH N3H GPRH_NOEW GPR_NOEW GPRH_AND GPR_AND GPRH_BASIC GPR_BASIC SHAREH_CAT_1 SHAREH_CAT_2 SHAREH_CAT_3 SHAREH_CAT_4 SHAREH_CAT_5 SHAREH_CAT_6 SHAREH_CAT_7 SHAREH_CAT_8 GPRC_ARG GPRC_AUS GPRC_BEL GPRC_BRA GPRC_CAN GPRC_CHE GPRC_CHL GPRC_CHN GPRC_COL GPRC_DEU GPRC_DNK GPRC_EGY GPRC_ESP GPRC_FIN GPRC_FRA GPRC_GBR GPRC_HKG GPRC_HUN GPRC_IDN GPRC_IND GPRC_ISR GPRC_ITA GPRC_JPN GPRC_KOR GPRC_MEX GPRC_MYS GPRC_NLD GPRC_NOR GPRC_PER GPRC_PHL GPRC_POL GPRC_PRT GPRC_RUS GPRC_SAU GPRC_SWE GPRC_THA GPRC_TUN GPRC_TUR GPRC_TWN GPRC_UKR GPRC_USA GPRC_VEN GPRC_VNM GPRC_ZAF GPRHC_ARG GPRHC_AUS GPRHC_BEL GPRHC_BRA GPRHC_CAN GPRHC_CHE GPRHC_CHL GPRHC_CHN GPRHC_COL GPRHC_DEU GPRHC_DNK GPRHC_EGY GPRHC_ESP GPRHC_FIN GPRHC_FRA GPRHC_GBR GPRHC_HKG GPRHC_HUN GPRHC_IDN GPRHC_IND GPRHC_ISR GPRHC_ITA GPRHC_JPN GPRHC_KOR GPRHC_MEX GPRHC_MYS GPRHC_NLD GPRHC_NOR GPRHC_PER GPRHC_PHL GPRHC_POL GPRHC_PRT GPRHC_RUS GPRHC_SAU GPRHC_SWE GPRHC_THA GPRHC_TUN GPRHC_TUR GPRHC_TWN GPRHC_UKR GPRHC_USA GPRHC_VEN GPRHC_VNM GPRHC_ZAF'.split())


def clock(value):
    if not isinstance(value, str) or 'T' not in value:
        raise ValueError('Aware calculation clock required')
    d = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if d.tzinfo is None:
        raise ValueError('Aware calculation clock required')
    return d.astimezone(timezone.utc)


def shift(value, offset):
    d = date.fromisoformat(value + '-01'); n = d.year * 12 + d.month - 1 + offset
    return f'{n // 12:04d}-{n % 12 + 1:02d}'


def number(value):
    if value in (None, '', '.'):
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)) or not re.fullmatch(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?', str(value).strip()):
        raise ValueError('Direct decimal number required')
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        raise ValueError('Invalid numeric value') from None
    if not d.is_finite() or d < 0 or d > Decimal('1e15') or d != 0 and float(d) == 0:
        raise ValueError('Finite nonnegative representable observation required')
    return d


def comparison(current, prior):
    state = 'latest_missing' if current is None else 'prior_month_missing' if prior is None else 'zero_denominator' if prior == 0 else 'measured'
    value = float((current / prior - 1) * 100) if state == 'measured' else None
    if value is not None and not math.isfinite(value):
        state = 'outside_numeric_range'; value = None
    return {'status': state, 'percent': value}


def baseline(values, latest, count):
    required = [shift(latest, i) for i in range(-count, 0)]
    missing = [d for d in required if values.get(d) is None]; current = values.get(latest)
    out = {'start': required[0], 'end': required[-1], 'expected_months': count, 'current_month_excluded': True,
           'missing_months': missing, 'mean': None, 'population_sd': None, 'z': None,
           'status': 'incomplete_baseline' if missing else 'latest_missing' if current is None else 'measured'}
    if missing:
        return out
    xs = [values[d] for d in required]; average = sum(xs) / count
    sd = (sum((v - average) ** 2 for v in xs) / count).sqrt()
    out.update(mean=float(average), population_sd=float(sd))
    if sd == 0:
        out['status'] = 'zero_variance'
    elif current is not None:
        z = float((current - average) / sd)
        if math.isfinite(z):
            out['z'] = z
        else:
            out['status'] = 'outside_numeric_range'
    return out


def truck(metadata, observations, at):
    out = {'series_id': 'HTRUCKSSAAR', 'source_url': 'https://fred.stlouisfed.org/series/HTRUCKSSAAR',
           'status': 'unavailable', 'unit': 'Millions of Units', 'seasonal_adjustment': 'SAAR', 'frequency': 'monthly',
           'observations': [], 'returned_rows': 0, 'latest_month': None, 'level': None,
           'original_vintage_verified': False, 'observation_freshness_verified': False, **FLAGS,
           'interpretation': 'An annualized monthly heavy-truck retail sales rate, not actual trucks sold that month. No demonstrated S&P 500 lead or position implication.'}
    records = metadata.get('seriess') if isinstance(metadata, dict) else None
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], dict) or records[0].get('id') != 'HTRUCKSSAAR':
        out['status'] = 'metadata_identity_mismatch'; return out
    meta = records[0]; out['metadata'] = meta
    if (meta.get('units'), meta.get('frequency_short'), meta.get('seasonal_adjustment_short')) != ('Millions of Units', 'M', 'SAAR'):
        out['status'] = 'metadata_definition_changed'; return out
    rows = observations.get('observations') if isinstance(observations, dict) else None
    if (not isinstance(rows, list) or type(observations.get('count')) is not int or observations['count'] != len(rows)
            or observations.get('offset') != 0 or type(observations.get('offset')) is not int or observations.get('units') != 'lin'
            or type(observations.get('output_type')) is not int or observations['output_type'] != 1):
        out['status'] = 'incomplete_or_transformed_response'; return out
    values = {}; bad = False; cutoff = clock(at).date()
    for position, original in enumerate(rows):
        row = {'position': position, 'original': original, 'month': None, 'value': None, 'status': 'invalid_observation'}
        try:
            if not isinstance(original, dict):
                raise ValueError('Observation object required')
            d = date.fromisoformat(original['date']); ym = d.isoformat()[:7]; n = number(original.get('value'))
            if original['date'] != d.isoformat() or d.day != 1 or d > cutoff or ym in values:
                raise ValueError('Unique exact monthly observation required')
            values[ym] = n; row.update(month=ym, value=float(n) if n is not None else None, status='observed' if n is not None else 'missing')
        except (ValueError, TypeError, KeyError):
            bad = True
        out['observations'].append(row)
    out['returned_rows'] = len(rows)
    if bad or not values:
        out['status'] = 'ambiguous_observations' if bad else 'empty_observations'; return out
    latest = max(values); n = values[latest]; prior = shift(latest, -12)
    out.update(status='measured' if n is not None else 'latest_missing', latest_month=latest, level=float(n) if n is not None else None,
               yoy={**comparison(n, values.get(prior)), 'current_month': latest, 'previous_month': prior, 'unit': 'percent'},
               prior_12_months=baseline(values, latest, 12), observation_age_days=(cutoff - date.fromisoformat(latest + '-01')).days)
    return out


def gpr(raw, at):
    if not isinstance(raw, bytes) or not 1 <= len(raw) <= 16 * 1024 * 1024:
        raise ValueError('Bounded complete GPR workbook required')
    book = xlrd.open_workbook(file_contents=raw, on_demand=True)
    try:
        if book.sheet_names() != ['Sheet1']:
            raise ValueError('Exact GPR source sheet required')
        sheet = book.sheet_by_index(0)
        if not 4 <= sheet.ncols <= 1000 or not 2 <= sheet.nrows <= 10000:
            raise ValueError('Complete workbook population outside bound')
        headers = sheet.row_values(0)
        if headers[0] != 'month' or headers[-2:] != ['var_name', 'var_label'] or len(headers) != len(set(headers)) or 'GPR' not in headers or 'GPRH' not in headers:
            raise ValueError('GPR source identities differ')
        ids = headers[1:-2]
        if not REQUIRED_GPR_SERIES.issubset(ids) or any(not isinstance(x, str) or not x for x in ids):
            raise ValueError('Every source column requires its identity')
        labels = {}; label_rows = []
        for r in range(1, sheet.nrows):
            key, label = sheet.cell_value(r, sheet.ncols - 2), sheet.cell_value(r, sheet.ncols - 1)
            if key or label:
                if not isinstance(key, str) or not key or key in labels or not isinstance(label, str) or not label:
                    raise ValueError('Complete unique source variable labels required')
                labels[key] = label; label_rows.append({'row': r + 1, 'variable': key, 'label': label})
        if set(labels) != {'month', *ids} or labels.get('GPR') != 'Recent GPR (Index: 1985:2019=100)' or labels.get('GPRH') != 'Historical GPR (Index: 1900:2019=100)':
            raise ValueError('GPR source labels or normalization base changed')
        dates = []; source_dates = []; columns = {key: [] for key in ids}; cutoff = clock(at).strftime('%Y-%m')
        for r in range(1, sheet.nrows):
            cell = sheet.cell(r, 0)
            if cell.ctype != xlrd.XL_CELL_DATE:
                raise ValueError('Typed Excel source month required')
            d = xlrd.xldate_as_datetime(cell.value, book.datemode); ym = d.strftime('%Y-%m')
            if d.day != 1 or d.hour or d.minute or d.second or ym >= cutoff or dates and ym != shift(dates[-1], 1):
                raise ValueError('Complete unique monthly GPR calendar required')
            dates.append(ym); source_dates.append({'row': r + 1, 'excel_serial': cell.value, 'month': ym})
            for c, key in enumerate(ids, 1):
                cell = sheet.cell(r, c)
                if cell.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK) or cell.ctype == xlrd.XL_CELL_TEXT and cell.value == '':
                    n = None
                elif cell.ctype == xlrd.XL_CELL_NUMBER:
                    n = number(cell.value)
                else:
                    raise ValueError('GPR numeric cell type differs')
                columns[key].append(float(n) if n is not None else None)
        if dates[0] != '1900-01':
            raise ValueError('Complete historical source calendar required')
        values = {d: number(v) for d, v in zip(dates, columns['GPR'])}; latest = dates[-1]; n = values[latest]
        return {'status': 'measured' if n is not None else 'latest_missing', 'source_url': 'https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls',
                'workbook_sha256': hashlib.sha256(raw).hexdigest(), 'workbook_bytes': len(raw), 'sheet': 'Sheet1', 'date_mode': book.datemode,
                'months': dates, 'source_dates': source_dates, 'source_labels': label_rows,
                'series': {key: {'column': i + 2, 'source_label': labels[key], 'values': columns[key], **FLAGS} for i, key in enumerate(ids)},
                'series_count': len(ids), 'monthly_rows': len(dates), 'monthly_positions': len(dates) * len(ids),
                'headline_series_id': 'GPR', 'headline_unit': 'Index 1985:2019=100', 'latest_month': latest, 'level': float(n) if n is not None else None,
                'prior_60_months': baseline(values, latest, 60), 'original_vintage_verified': False, 'workbook_contains_formulas_verified': False, **FLAGS,
                'interpretation': 'News-article index and source-specific article shares/counts. GPR and GPRH have different normalization bases. No market-pricing, war-probability or portfolio claim follows.'}
    finally:
        book.release_resources()
