"""Public source-family rollup. Reads IAM-only browser attribution, writes only
its own data/source-map.json projection. Raw prose and diagnostics stay private.
"""
import json
import re
from collections import Counter
from datetime import datetime, timezone

import boto3
from public_brain_projection import source_map_public, SOURCE_MAP_FAMILIES, SOURCE_MAP_SYMBOL

MARKER = "source-map engine v3 public-source-map.v1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

# Junk = pure lowercase slugs. TV ships logoids ("django_model",
# "us_treasury_logo") in the same fields as real publisher names; a real
# publisher has capitals or spaces.
JUNK_RX = re.compile(r"^[a-z0-9_]{3,}$")
PFX_RX = re.compile(r"^(?:source|provider|country)/", re.I)

# Agency families. Keys mirror the justhodl-gov-sources registry so the
# two engines speak the same vocabulary and can be joined.
KNOWN = {
    "FRED": ("federal reserve", "fred", "st. louis"),
    "US-TREASURY": ("u.s. department of the treasury", "treasury"),
    "BLS": ("bureau of labor",),
    "BEA": ("bureau of economic analysis",),
    "CENSUS-US": ("u.s. census bureau", "united states census"),
    "ECB": ("european central bank",),
    "EUROSTAT": ("eurostat",),
    "BOJ": ("bank of japan",),
    "MOF-JAPAN": ("ministry of finance (japan", "japan ministry of finance",
                  "ministry of finance, japan"),
    "ESTAT-JAPAN": ("statistics bureau of japan", "e-stat"),
    "BOE": ("bank of england",),
    "SNB": ("swiss national bank",),
    "NORGES": ("norges bank",),
    "BCRP-PERU": ("banco central de reserva",),
    "BCB-BRAZIL": ("banco central do brasil", "central bank of brazil"),
    "PBOC": ("people's bank of china",),
    "MOEA-TAIWAN": ("ministry of economic affairs",),
    "CFTC": ("commodity futures",),
    "SEC-EDGAR": ("securities and exchange",),
    "OFR": ("office of financial research",),
    "IMF": ("international monetary fund",),
    "HKMA": ("hong kong monetary",),
    "OECD": ("oecd",),
    "WORLD-BANK": ("world bank",),
    "COINMETRICS": ("coin metrics", "coinmetrics"),
    "COINGECKO": ("coingecko",),
    "EIA": ("energy information administration",),
    "MARKET-VENUES": (
        "tvc", "sgx", "tpex", "xetr", "omx", "six", "bme", "tradegate",
        "cryptocap", "iceus", "ose", "comex", "gpw", "nymex", "ftse",
        "hose", "vie", "cselk", "pse", "sparks", "spcfd", "lse", "dj",
        "nasdaq", "nyse", "cboe", "cme", "ice ", "eurex", "tradingview",
        "arca", "amex", "otc", "lse ", "tsx", "borsa", "euronext", "xetra",
        "b3 ", "bmv", "hkex", "krx", "twse", "sse", "szse", "asx", "moex",
        "bist", "forex", "fx ", "binance", "coinbase", "kraken", "bitstamp",
        "bybit", "okx", "bitfinex"),
}

# Venues are attribution, but they are not the payoff — separating them
# keeps "coverage" from being inflated by knowing NVDA is on NASDAQ.
NON_AGENCY = {"MARKET-VENUES"}


def fam_of(src):
    t = str(src or "").lower()
    for fam, keys in KNOWN.items():
        if any(k in t for k in keys):
            return fam
    return None


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


# Public symbols are qualified market identifiers, never free-form browser labels.
PUBLIC_SYMBOL = SOURCE_MAP_SYMBOL
PUBLIC_FAMILIES = SOURCE_MAP_FAMILIES


def public_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    import math
    return value if math.isfinite(value) and value >= 0 else None


def public_timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


def lambda_handler(event, context):
    """Read the private landing artifact; publish a distinct public projection.

    No write back to ingest's store: that read/modify/write lost concurrent
    browser updates. No browser source prose or diagnostics enter this output.
    Family matching is a classification heuristic, not independent attestation.
    """
    now = datetime.now(timezone.utc).isoformat()
    raw = gj('data/tv-sources.json')
    available = isinstance(raw, dict) and isinstance(raw.get('sources'), dict)
    sr = raw if isinstance(raw, dict) else {}
    store = sr.get('sources') if available else {}
    diag = sr.get('last_harvest_diag')
    diag = diag if isinstance(diag, dict) else {}
    real, cleaned, families, by = {}, {}, Counter(), Counter()
    for symbol, value in store.items():
        if not isinstance(value, dict):
            continue
        source = PFX_RX.sub('', str(value.get('source') or '')).strip()
        if not source or JUNK_RX.match(source.replace('-', '_')):
            continue
        family = fam_of(source) or 'UNMAPPED'
        real[symbol] = family
        families[family] += 1
        by[source] += 1
        if isinstance(symbol, str) and PUBLIC_SYMBOL.fullmatch(symbol):
            cleaned[symbol] = {'source_family': family,
                               'updated': public_timestamp(value.get('updated'))}
    known = {f:n for f,n in families.items() if f != 'UNMAPPED'}
    agency = {f:n for f,n in known.items() if f not in NON_AGENCY}
    econ = Counter(f for symbol,f in real.items()
                   if isinstance(symbol,str) and symbol.startswith(('ECONOMICS:', 'FRED:')))
    ma = gj('data/macro-attribution.json')
    ma = ma if isinstance(ma,dict) else {}
    macro = ma.get('attribution')
    macro = macro if isinstance(macro,dict) else {}
    for value in macro.values():
        if not isinstance(value,dict):
            continue
        family = value.get('family')
        family = family if isinstance(family,str) and family in PUBLIC_FAMILIES else 'UNMAPPED'
        if family not in NON_AGENCY and family != 'UNMAPPED':
            agency[family] = agency.get(family,0)+1
        econ[family] += 1
    done, total, rate = (public_number(diag.get(k)) for k in ('done','total','rate_per_min'))
    progress = {'walked':done,'total':total,
                'pct':round(done/total*100,1) if done is not None and total else None,
                'tier1_done':public_number(diag.get('tier1_done')),
                'rate_per_min':rate,'elapsed_s':public_number(diag.get('elapsed_s')),
                'matched':public_number(diag.get('matched')),
                'eta_hours':round(max(total-done,0)/rate/60,1) if done is not None and total is not None and rate else None}
    out = {'schema_version':'public-source-map.v1','engine':'justhodl-source-map',
           'generated_at':now,'marker':MARKER,
           'publication':{'scope':'PUBLIC_MARKET_SOURCE_METADATA','contains_private_data':False,
                          'raw_source_text_private':True,'raw_diagnostics_private':True},
           'input_artifact':'data/tv-sources.json','input_generated_at':public_timestamp(sr.get('generated_at')),
           'input_status':'AVAILABLE' if available else 'UNAVAILABLE',
           'classification_method':'Fixed agency-family keyword classification; no independent publisher attestation. Unrecognized text is withheld.',
           'symbols_with_source':len(real),'distinct_sources':len(by),
           'junk_filtered':len(store)-len(real),'known_families':known,
           'agency_families':agency,'agency_rows':sum(agency.values()),
           'venue_rows':families.get('MARKET-VENUES',0),
           'economics_agencies':[{'source_family':family,'n_symbols':n} for family,n in econ.most_common()],
           'economics_symbols':sum(econ.values()),'macro_attributed':len(macro),
           'macro_unattributed':public_number(ma.get('unattributed')),
           'macro_coverage_pct':public_number(ma.get('coverage_pct')),
           'macro_input_generated_at':public_timestamp(ma.get('generated_at')),
           'harvest_progress':progress,'cleaned_sources':cleaned,
           'public_symbol_count':len(cleaned),'withheld_symbol_count':len(real)-len(cleaned),
           'unmapped_source_rows':families.get('UNMAPPED',0),
           'errors':[] if available else ['SOURCE_INPUT_UNAVAILABLE']}
    out = source_map_public(out)
    s3.put_object(Bucket=BUCKET,Key='data/source-map.json',Body=json.dumps(out,allow_nan=False),
                  ContentType='application/json',CacheControl='max-age=120')
    return {'statusCode':200,'body':json.dumps({'symbols_with_source':len(real),'public_symbol_count':len(cleaned),'input_status':out['input_status']})}
