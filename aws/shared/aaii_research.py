"""Typed AAII research context. No qualified AAII forecast is registered here."""
from datetime import datetime, timezone, date
from decimal import Decimal
import hashlib
import json
import math
import re


def context(packet, at=None):
    absent = {'available': False, 'reason': 'verified_current_survey_unavailable', 'forecast_qualified': False,
              'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False}
    try:
        if not isinstance(packet, dict) or packet.get('contract') != 'aaii-native-research.v1': return absent
        at = at or datetime.now(timezone.utc)
        generated = datetime.fromisoformat(packet['generated_at'].replace('Z', '+00:00'))
        quality = packet['quality']; freshness = quality['freshness']
        until = datetime.fromisoformat(freshness['valid_until'].replace('Z', '+00:00'))
        if generated.tzinfo is None or until.tzinfo is None or not generated <= at < until: return absent
        if (at-generated).total_seconds() > 36*3600 or quality['status'] != 'fresh' or quality['errors']: return absent
        if any(packet.get(k) is not False for k in ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')):
            return absent
        ref = packet['replay']; key = ref['manifest_key']
        if not re.fullmatch(r'data/aaii-research/runs/[a-f0-9]{64}\.json', key): return absent
        raw = json.dumps({k: v for k, v in packet.items() if k != 'replay'}, sort_keys=True,
                         separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        if hashlib.sha256(raw).hexdigest() != ref['output_sha256']: return absent
        row = packet['observation']; day = date.fromisoformat(row['week_ending'])
        if packet['as_of'] != day.isoformat() or day.weekday() != 2 or not 0 <= (at.date()-day).days <= 8: return absent
        vals = [row[k+'_pct'] for k in ('bullish', 'neutral', 'bearish')]
        if any(isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v) or not 0 <= v <= 100 for v in vals): return absent
        if Decimal(str(vals[0]))-Decimal(str(vals[2])) != Decimal(str(row['bull_bear_spread_pp'])): return absent
        for name, url in (('main', 'https://www.aaii.com/sentimentsurvey'), ('results', 'https://www.aaii.com/sentimentsurvey/sent_results')):
            evidence = packet['source_evidence'][name]['evidence']; digest = evidence['sha256']
            if (evidence['source_url'] != url or not re.fullmatch('[a-f0-9]{64}', digest)
                    or evidence['key'] != 'audit-private/20260909-originals/aaii-sentiment/'+digest+'.bin'): return absent
        return {**absent, 'available': True, 'reason': 'descriptive_survey_only', 'as_of': day.isoformat(),
                **{k: row[k] for k in ('bullish_pct', 'neutral_pct', 'bearish_pct', 'bull_bear_spread_pp')},
                'units': 'categories: percent of respondents; spread: percentage points',
                'population': 'Participating AAII members; six-month opinion, not portfolio exposure.',
                'source': 'https://www.aaii.com/sentimentsurvey', 'replay': ref}
    except (KeyError, ValueError, TypeError, OverflowError, ArithmeticError):
        return absent


def describe(doc):
    if not doc.get('available'): return 'AAII: verified current survey unavailable; research abstains.'
    return ('AAII week ending {as_of}: bullish {bullish_pct:g}%, neutral {neutral_pct:g}%, bearish {bearish_pct:g}%; '
            'bull-minus-bear {bull_bear_spread_pp:+g} percentage points. Participating AAII members, six-month opinions. '
            'Source https://www.aaii.com/sentimentsurvey. Descriptive research; no qualified return forecast or trade vote.').format(**doc)


def qualified_signal(packet):
    # There is no externally accepted point-in-time, after-cost AAII forecast.
    # Packet fields cannot self-authorize a vote or position size.
    return None
