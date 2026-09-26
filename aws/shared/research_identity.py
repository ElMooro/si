"""Collector identity boundaries; a crypto source cannot create equity outcomes.

The source binding below follows the checked-in producer's explicit coin universe
and Polygon X: route. It is not inferred from a substring or price magnitude.
"""
import hashlib
from pathlib import Path
import instrument_identity
from instrument_identity import CRYPTO,resolve_instrument

CRYPTO_SOURCES=frozenset(('data/crypto-emergence.json',))
CRYPTO_CLASSES=frozenset(('crypto','cryptocurrency','digital_asset'))
# DOT is in the source's coin universe but is unsupported by the older resolver.
# It must remain unavailable, never inherit the default US-equity route.
TOKEN_COLLISIONS=CRYPTO|{'DOT'}


def identity_policy():
    paths=(Path(__file__),Path(instrument_identity.__file__))
    return {'contract':'research-source-identity.v1',
            'files':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}}


def resolve_pick_identity(symbol,row=None,source_key=None):
    row=row if isinstance(row,dict) else {}
    category=row.get('asset_class') or row.get('asset_type')
    category=str(category or '').strip().lower()
    symbol=str(symbol or '').strip().upper()
    if source_key in CRYPTO_SOURCES:
        if category and category not in CRYPTO_CLASSES:return None
        return resolve_instrument(symbol,'crypto')
    if not category and symbol in TOKEN_COLLISIONS:return None
    return resolve_instrument(symbol,category)


def record_identity_issue(record):
    if record.get('source',{}).get('source_key') in CRYPTO_SOURCES:
        return 'crypto_source_not_supported_by_equity_measurement_protocol'
    return None
