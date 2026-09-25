"""Whole public-packet diagnostics. No source or forecast qualification."""
from collections import Counter
from datetime import date
import math


def records(packet, kind):
    if not isinstance(packet, dict):
        raise ValueError('Whole public object required')
    if kind == 'forensic':
        rows = packet.get('all_results')
    elif kind == 'share_flows':
        rows = packet.get('tickers')
        if isinstance(rows, dict):
            rows = [dict(value, reported_map_key=key) for key, value in rows.items() if isinstance(value, dict)]
            if len(rows) != len(packet['tickers']):
                raise ValueError('Share-flow rows must all be objects')
    elif kind == 'short_book':
        rows = packet.get('book')
    elif kind == 'universe':
        present = [packet[k] for k in ('rows', 'stocks', 'data') if isinstance(packet.get(k), list)]
        if len(present) != 1:
            raise ValueError('One explicit universe population required')
        rows = present[0]
    else:
        raise ValueError('Reviewed public packet kind required')
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError('Every source row must be retained and inventoried')
    return rows


def nonfinite(value):
    if isinstance(value, float):
        return int(not math.isfinite(value))
    if isinstance(value, dict):
        return sum(nonfinite(v) for v in value.values())
    if isinstance(value, list):
        return sum(nonfinite(v) for v in value)
    return 0


def dated(value):
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value[:10]).isoformat() == value[:10]
    except ValueError:
        return False


def inventory(packet, kind):
    rows = records(packet, kind)
    identities = Counter(str(r.get('symbol') or r.get('ticker') or r.get('reported_map_key') or '') for r in rows)
    fields = Counter(key for row in rows for key in row)
    missing = Counter(str(v) for row in rows for v in (row.get('components_missing') or []) if isinstance(v, str))
    clocks = ('as_of', 'date', 'observation_date', 'fiscal_period_end', 'period_end')
    currencies = ('currency', 'reportedCurrency', 'reported_currency')
    lineage = ('source', 'sources', 'evidence', 'replay', 'source_refs')
    def numeric(v):
        return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
    return {'contract': 'financial-statement-packet-inventory.v1', 'kind': kind,
        'generated_at': packet.get('generated_at'), 'version': packet.get('version'),
        'rows': len(rows), 'unique_reported_labels': len(identities) - int('' in identities),
        'missing_reported_labels': identities.get('', 0),
        'repeated_reported_labels': dict(sorted((k, v) for k, v in identities.items() if k and v > 1)),
        'row_field_population': dict(sorted(fields.items())),
        'rows_with_recognized_date_field': sum(any(dated(row.get(k)) for k in clocks) for row in rows),
        'rows_with_currency_field': sum(any(isinstance(row.get(k), str) and bool(row[k]) for k in currencies) for row in rows),
        'rows_with_nonempty_lineage_field': sum(any(row.get(k) is not None and row.get(k) not in ({}, [], '') for k in lineage) for row in rows),
        'rows_with_statement_arrays': sum(any(isinstance(row.get(k), list) and bool(row[k]) for k in ('income_statements', 'balance_sheets', 'cash_flows')) for row in rows),
        'rows_with_m_score': sum(numeric(row.get('m_score')) for row in rows),
        'rows_with_concern_score': sum(numeric(row.get('concern_score')) for row in rows),
        'rows_with_strength_score': sum(numeric(row.get('strength_score')) for row in rows),
        'rows_with_all_eight_reported_beneish_factors': sum(isinstance(row.get('factors'), dict) and all(numeric(row['factors'].get(k)) for k in ('DSRI', 'GMI', 'AQI', 'SGI', 'DEPI', 'SGAI', 'LVGI', 'TATA')) for row in rows),
        'missing_component_counts': dict(sorted(missing.items())), 'nonfinite_values_in_whole_packet': nonfinite(packet),
        'reported_attempted': packet.get('n_universe_attempted'), 'reported_scored': packet.get('n_scored_ok'),
        'reported_skipped': packet.get('n_skipped_missing_data'), 'reported_errors': packet.get('n_errors'),
        'reported_logged_signals': packet.get('logged'),
        'field_presence_is_not_source_verification': True, 'original_statement_responses_verified': False,
        'period_alignment_verified': False, 'currency_alignment_verified': False, 'index_membership_verified': False,
        'historical_availability_verified': False, 'forecast_qualified': False, 'sizing_qualified': False}
