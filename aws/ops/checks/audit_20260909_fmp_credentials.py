"""Validate an existing credential using one public quote; report metadata only."""
import math


def validate(fetch_rows, value):
    if not value:
        return {'valid':False, 'status':'ABSENT'}
    rows, diagnostic = fetch_rows('batch-quote', {'symbols':'AAPL'}, value)
    valid = any(row.get('symbol') == 'AAPL' and type(row.get('price')) in (int,float)
                and math.isfinite(row['price']) and row['price']>0
                for row in rows if isinstance(row,dict))
    return {'valid':valid, 'status':'VALIDATED_QUOTE' if valid else 'PROVIDER_REJECTED_OR_NO_VALID_QUOTE',
            **({'http_status':diagnostic['http_status']} if isinstance(diagnostic.get('http_status'),int) else {})}
