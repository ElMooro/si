"""Explicit pricing identity; a display ticker is not a cross-provider join key."""
import re

AMBIGUOUS = frozenset(('BTC', 'ETH', 'SOL'))
CRYPTO = frozenset(('BTC', 'ETH', 'SOL', 'XRP', 'LTC', 'BCH', 'DOGE', 'ADA', 'AVAX', 'LINK'))


def resolve_instrument(symbol, asset_class=None):
    symbol = str(symbol or '').strip().upper()
    category = str(asset_class or '').strip().lower()
    explicit_crypto = symbol.endswith('-USD') or symbol.startswith('X:')
    if category in ('crypto', 'cryptocurrency', 'digital_asset') or explicit_crypto:
        base = symbol.removeprefix('X:').removesuffix('-USD')
        if base.endswith('USD'):
            base = base[:-3]
        if base not in CRYPTO:
            return None
        return {'instrument_id': 'crypto:' + base + '/USD', 'asset_class': 'crypto', 'currency': 'USD',
                'symbol': base + '-USD', 'provider_symbols': {'fmp': base + 'USD', 'polygon': 'X:' + base + 'USD', 'yahoo': base + '-USD'}}
    if symbol in AMBIGUOUS and category not in ('equity', 'etf', 'stock'):
        return None
    if category not in ('', 'equity', 'etf', 'stock') or not re.fullmatch(r'[A-Z][A-Z.\-]{0,6}', symbol):
        return None
    return {'instrument_id': 'equity:US:' + symbol, 'asset_class': 'equity', 'currency': 'USD',
            'symbol': symbol, 'provider_symbols': {'fmp': symbol, 'polygon': symbol, 'yahoo': symbol}}


def same_instrument(left, right):
    return bool(isinstance(left, dict) and isinstance(right, dict)
                and left.get('instrument_id') and left.get('instrument_id') == right.get('instrument_id')
                and left.get('currency') and left.get('currency') == right.get('currency'))
